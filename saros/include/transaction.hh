#pragma once

#include <stdio.h>
#include <array>
#include <atomic>
#include <cstdio>
#include <chrono>
#include <condition_variable>
#include <memory>
#include <mutex>
#include <unordered_map>
#include <thread>
#include <fstream>
#include <iostream>
#include <map>
#include <sstream>
#include <stack>
#include <queue>
#include <vector>

#include "common/backoff.hh"
#include "common/debug.hh"
#include "common/inline.hh"
#include "common/result.hh"
#include "common/status.hh"
#include "common/util.hh"
#include "oze.hh"
#include "common/engine/oze_op_element.hh"
#include "common.hh"
#include "common/engine/atomic_tool.hh"
#include "common/engine/scan.hh"
#include "tuple.hh"
#include "common/engine/txid.hh"
#include "common/engine/version.hh"

/**
 * @brief Deferred freeing of deleted rows (a committed delete or an aborted insert).
 * 
 * The row leaves the index and is logically deleted, so no transaction can obtain the
 * pointer any more. It is freed once min(min_live(), get_min_epoch() - 1) is above the
 * epoch of retirement: a transaction that still holds the pointer has a lower floor.
 */
class RowRetirementRegistry {
public:
    struct Counters {
        std::atomic<uint64_t> rows_retired{0};        // logically deleted + queued
        std::atomic<uint64_t> retired_aborted_insert{0}; // via writeSetClean (D1)
        std::atomic<uint64_t> retired_committed_delete{0}; // via commit_version (D2)
        std::atomic<uint64_t> unpublished_insert_frees{0}; // insert_record WARN_ALREADY_EXISTS (D3)
        std::atomic<uint64_t> rows_freed{0};
        std::atomic<uint64_t> versions_freed{0};
        std::atomic<uint64_t> reclaim_attempts{0};
        std::atomic<uint64_t> blocked_by_floor{0};
        std::atomic<uint64_t> peak_pending{0};
        std::atomic<uint64_t> immediate_frees{0};     // deferral disabled
        std::atomic<uint64_t> drained_at_shutdown{0};
        std::atomic<uint64_t> dropped_dead_targets{0};// propagation targets dropped
    };

    static RowRetirementRegistry& instance() {
        static RowRetirementRegistry registry;
        return registry;
    }
    static Counters& counters() { return instance().counters_; }

    // `owned_version` (an aborted insert's only version) is freed with the row.
    void retire(Tuple* row, Version* owned_version, uint64_t epoch) {
        if (row == nullptr) return;
        row->logically_delete();
        size_t pending;
        {
            std::lock_guard<std::mutex> guard(mutex_);
            queue_.push_back({row, owned_version, epoch});
            pending = queue_.size();
        }
        counters_.rows_retired.fetch_add(1, std::memory_order_relaxed);
        uint64_t peak = counters_.peak_pending.load(std::memory_order_relaxed);
        while (pending > peak) {
            if (counters_.peak_pending.compare_exchange_weak(
                    peak, pending, std::memory_order_relaxed)) break;
        }
    }

    // Frees up to `budget` entries whose epoch is below live_floor (0 frees none).
    size_t reclaim(uint64_t live_floor, size_t budget) {
        std::vector<Retired> to_free;
        {
            std::lock_guard<std::mutex> guard(mutex_);
            size_t kept = 0, scanned = 0;
            for (size_t i = 0; i < queue_.size(); ++i) {
                const Retired entry = queue_[i];
                if (scanned >= budget) { queue_[kept++] = entry; continue; }
                ++scanned;
                counters_.reclaim_attempts.fetch_add(1, std::memory_order_relaxed);
                if (live_floor == 0 || live_floor <= entry.epoch_) {
                    counters_.blocked_by_floor.fetch_add(1, std::memory_order_relaxed);
                    queue_[kept++] = entry;
                    continue;
                }
                to_free.push_back(entry);
            }
            queue_.resize(kept);
        }
        destroy(to_free);
        return to_free.size();
    }

    // Frees everything; only after every worker has joined.
    size_t drain_all() {
        std::vector<Retired> to_free;
        {
            std::lock_guard<std::mutex> guard(mutex_);
            to_free.swap(queue_);
        }
        counters_.drained_at_shutdown.fetch_add(to_free.size(),
                                                std::memory_order_relaxed);
        destroy(to_free);
        return to_free.size();
    }

    size_t pending() const {
        std::lock_guard<std::mutex> guard(mutex_);
        return queue_.size();
    }

private:
    struct Retired { Tuple* row_; Version* version_; uint64_t epoch_; };

    // The owned version first; ~Tuple() does not read its versions.
    void destroy(const std::vector<Retired>& entries) {
        for (const Retired& entry : entries) {
            if (entry.version_ != nullptr) {
                delete entry.version_;
                counters_.versions_freed.fetch_add(1, std::memory_order_relaxed);
            }
            delete entry.row_;
            counters_.rows_freed.fetch_add(1, std::memory_order_relaxed);
        }
    }

    mutable std::mutex mutex_;
    std::vector<Retired> queue_;
    Counters counters_;
};

/**
 * @brief Deferred freeing of the version of an aborted update or delete, whose row stays live.
 * 
 * retire() unlinks an installed version under the record latch and frees it once the
 * floor is above the epoch of retirement; a version never installed is freed at once.
 */
class VersionRetirementRegistry {
public:
    struct Counters {
        std::atomic<uint64_t> versions_retired{0};
        std::atomic<uint64_t> versions_retired_unlinked{0};  // found spliced in, unlinked
        std::atomic<uint64_t> versions_retired_unspliced{0}; // never reached the chain
        std::atomic<uint64_t> versions_freed{0};
        std::atomic<uint64_t> reclaim_attempts{0};
        std::atomic<uint64_t> blocked_by_floor{0};
        std::atomic<uint64_t> blocked_by_reference{0};
        std::atomic<uint64_t> peak_pending{0};
        std::atomic<uint64_t> immediate_frees{0}; // deferral disabled
        std::atomic<uint64_t> drained_at_shutdown{0};
    };

    static VersionRetirementRegistry& instance() {
        static VersionRetirementRegistry registry;
        return registry;
    }
    static Counters& counters() { return instance().counters_; }

    // Takes the record latch; the caller must not hold it.
    void retire(Tuple* tuple, Version* ver, uint64_t epoch) {
        if (ver == nullptr) return;
        const bool unlinked = unlink_from_chain(tuple, ver);
        counters_.versions_retired.fetch_add(1, std::memory_order_relaxed);
        if (unlinked) {
            counters_.versions_retired_unlinked.fetch_add(1, std::memory_order_relaxed);
        } else {
            counters_.versions_retired_unspliced.fetch_add(1, std::memory_order_relaxed);
        }
        if (!unlinked) {
            // Never installed, so no reader can have seen it.
            delete ver;
            counters_.versions_freed.fetch_add(1, std::memory_order_relaxed);
            return;
        }
        size_t pending;
        {
            std::lock_guard<std::mutex> guard(mutex_);
            queue_.push_back({ver, epoch});
            pending = queue_.size();
        }
        uint64_t peak = counters_.peak_pending.load(std::memory_order_relaxed);
        while (pending > peak) {
            if (counters_.peak_pending.compare_exchange_weak(
                    peak, pending, std::memory_order_relaxed)) break;
        }
    }

    // As RowRetirementRegistry::reclaim().
    size_t reclaim(uint64_t live_floor, size_t budget) {
        std::vector<Retired> to_free;
        {
            std::lock_guard<std::mutex> guard(mutex_);
            size_t kept = 0, scanned = 0;
            for (size_t i = 0; i < queue_.size(); ++i) {
                const Retired entry = queue_[i];
                if (scanned >= budget) { queue_[kept++] = entry; continue; }
                ++scanned;
                counters_.reclaim_attempts.fetch_add(1, std::memory_order_relaxed);
                if (live_floor == 0 || live_floor <= entry.epoch_) {
                    counters_.blocked_by_floor.fetch_add(1, std::memory_order_relaxed);
                    queue_[kept++] = entry;
                    continue;
                }
                to_free.push_back(entry);
            }
            queue_.resize(kept);
        }
        for (const Retired& entry : to_free) {
            delete entry.version_;
            counters_.versions_freed.fetch_add(1, std::memory_order_relaxed);
        }
        return to_free.size();
    }

    // Frees everything; only after every worker has joined.
    size_t drain_all() {
        std::vector<Retired> to_free;
        {
            std::lock_guard<std::mutex> guard(mutex_);
            to_free.swap(queue_);
        }
        counters_.drained_at_shutdown.fetch_add(to_free.size(),
                                                std::memory_order_relaxed);
        for (const Retired& entry : to_free) {
            delete entry.version_;
            counters_.versions_freed.fetch_add(1, std::memory_order_relaxed);
        }
        return to_free.size();
    }

    size_t pending() const {
        std::lock_guard<std::mutex> guard(mutex_);
        return queue_.size();
    }

    // Approximate bytes of the queued versions (for the retention sampler).
    uint64_t retained_bytes() const {
        std::lock_guard<std::mutex> guard(mutex_);
        uint64_t total = 0;
        for (const Retired& entry : queue_) {
            total += sizeof(Version) + entry.version_->body_.get_val_size();
        }
        return total;
    }

private:
    struct Retired { Version* version_; uint64_t epoch_; };

    // True if `ver` was in the chain (it is now removed). Takes the latch.
    bool unlink_from_chain(Tuple* tuple, Version* ver) {
        tuple->lock_.w_lock();
        Version* prev = nullptr;
        Version* cur = tuple->ldAcqLatest();
        bool found = false;
        while (cur != nullptr && cur != VERSION_TERMINATION) {
            if (cur == ver) {
                Version* next = cur->ldAcqNext();
                if (prev == nullptr) {
                    tuple->latest_.store(next, std::memory_order_release);
                } else {
                    prev->strRelNext(next);
                }
                found = true;
                break;
            }
            prev = cur;
            cur = cur->ldAcqNext();
        }
        tuple->lock_.w_unlock();
        return found;
    }

    mutable std::mutex mutex_;
    std::vector<Retired> queue_;
    Counters counters_;
};

// Copies the freeing counters into a Result, after the workers have joined.
inline void snapshot_history_chunk_counters(Result* result) {
    if (result == nullptr) return;
    auto load = [](const std::atomic<uint64_t>& v) {
        return v.load(std::memory_order_relaxed);
    };
    const auto& h = ChunkRetirementRegistry::counters();
    auto& lines = result->history_chunk_lines_;
    lines.clear();
    lines.emplace_back("history_chunk_free_enabled",
                       FLAGS_free_retired_history_chunks ? 1 : 0);
    lines.emplace_back("history_chunk_unlink_calls", load(h.unlink_calls));
    lines.emplace_back("history_chunks_unlinked", load(h.chunks_unlinked));
    lines.emplace_back("history_chunks_freed", load(h.chunks_freed));
    lines.emplace_back("history_chunk_dirs_retired", load(h.dirs_retired));
    lines.emplace_back("history_chunk_dirs_freed", load(h.dirs_freed));
    lines.emplace_back("history_chunk_pending_entries", load(h.pending_entries));
    lines.emplace_back("history_chunk_pending_bytes", load(h.pending_bytes));
    lines.emplace_back("history_chunk_peak_pending_entries",
                       load(h.peak_pending_entries));
    lines.emplace_back("history_chunk_peak_pending_bytes",
                       load(h.peak_pending_bytes));
    lines.emplace_back("history_chunk_reclaim_passes", load(h.reclaim_passes));
    lines.emplace_back("history_chunk_reclaim_blocked_by_floor",
                       load(h.blocked_by_floor));
    lines.emplace_back("history_chunk_reader_clamps", load(h.reader_clamps));
    lines.emplace_back("history_chunk_fallback_walks", load(h.fallback_walks));
    lines.emplace_back("history_chunk_cursor_relocations",
                       load(h.cursor_relocations));
}

/**
 * @brief One copy of each validating transaction's read keys, by TxID, used by discovery.
 * 
 * Sharded; entries more than one epoch below the publisher's floor are swept.
 */
struct ReadFootprintRegistry {
    using Keys = std::vector<RowKey>;
    static constexpr int kShards = 64;
    struct Shard {
        std::mutex m;
        std::unordered_map<uint64_t, std::shared_ptr<const Keys>> map;
        uint64_t inserts = 0;
    };
    Shard shards[kShards];
    static ReadFootprintRegistry& instance() {
        static ReadFootprintRegistry r;
        return r;
    }
    static int shard_of(const TxID& t) { return (int)((t.thid * 131u + t.tid) % kShards); }
    void publish(const TxID& t, std::shared_ptr<const Keys> keys, uint64_t floor_epoch) {
        Shard& sh = shards[shard_of(t)];
        std::lock_guard<std::mutex> g(sh.m);
        sh.map[t.obj_] = std::move(keys);
        if (++sh.inserts % 256 == 0 && floor_epoch > 1) {
            for (auto it = sh.map.begin(); it != sh.map.end();) {
                TxID id; id.obj_ = it->first;
                if ((uint64_t)id.epoch + 1 < floor_epoch) {
                    it = sh.map.erase(it);
                } else {
                    ++it;
                }
            }
        }
    }
    void clear_for_tests() {
        for (auto& sh : shards) {
            std::lock_guard<std::mutex> g(sh.m);
            sh.map.clear();
        }
    }
    std::shared_ptr<const Keys> get(const TxID& t) {
        Shard& sh = shards[shard_of(t)];
        std::lock_guard<std::mutex> g(sh.m);
        auto it = sh.map.find(t.obj_);
        return it == sh.map.end() ? nullptr : it->second;
    }
};

/**
 * @brief Runs one worker's transactions: reads, writes, validation and commit.
 * 
 * With --saros, validation imports per-record histories into graph_ and publishes facts
 * once; otherwise it uses the merge-based record graphs (Tuple::graph_).
 */
class TxExecutor {
public:
    TransactionStatus status_ = TransactionStatus::invalid;
    std::vector<ReadElement<Tuple>> read_set_;
    std::vector<WriteElement<Tuple>> write_set_;
    Result *result_ = nullptr;
    Backoff& backoff_;
    uint64_t epoch_timer_start_, epoch_timer_stop_;

    // Transaction-local read and write sets, keyed by stable record identity.
    ReadSet readSet_;
    WriteSet writeSet_;
    // Working graph: Saros imports histories into it; Oze copies record graphs into it.
    Graph graph_;
    // Per record, the history sequence this transaction has imported up to.
    std::unordered_map<RecordId, uint64_t, RecordIdHash> last_imported_seq_;
    // Per record this transaction appended to: its last sequence there, and the record's
    // prefix retirement bound at that moment (stamped on later citations).
    std::unordered_map<Tuple*, uint64_t> published_seq_;
    std::unordered_map<Tuple*, uint64_t> published_prefix_epoch_;
    std::set<std::pair<RecordId, uint64_t>> import_visited_;
    // Aborted transactions met during one import; removed from the graph at its end.
    TxSet import_aborted_;
    // Records pinned by the current import, each at most once.
    std::vector<PinnedRecord> import_pins_;
    FlatRecordWatermarks import_pinned_;

    // A borrowed state whose pin import_pins_ owns.
    RecordGraphState* pin_for_import(RecordId record) {
        if (!record.valid()) return nullptr;
        if (const uint64_t slot = import_pinned_.get(record); slot != 0) {
            return import_pins_[slot - 1].get();
        }
        PinnedRecord pinned(record);
        if (!pinned.valid()) return nullptr;
        RecordGraphState* state = pinned.get();
        import_pins_.push_back(std::move(pinned));
        import_pinned_.set_max(record, import_pins_.size());
        return state;
    }

    // A deleted row's history takes no more entries; earlier readers reach it by identity.
    static bool row_detached(const Tuple* t) {
        return t != nullptr && t->graph_state_ptr() == nullptr;
    }
    // Committed sequence of a row's history, deleted or not; 0 if reclaimed.
    static uint64_t row_committed_seq(const Tuple* t) {
        if (const RecordGraphState* st = t->graph_state_ptr()) return st->committed();
        PinnedRecord pin(t->detached_graph_id());
        return pin.valid() ? pin->committed() : 0;
    }

    void release_import_pins() {
        import_pins_.clear();
        import_pinned_ = FlatRecordWatermarks();
    }

    // The record validated last; the next validated record cites its prefix (reference chain).
    Tuple* last_published_tuple_ = nullptr;

    // History dump (--history_dump_path) for tools/check_history.py: for each written row,
    // the version below the new one and the global install order.
    struct HistoryWritePred {
        TxID pred;
        uint64_t install_seq;
    };
    std::unordered_map<Tuple*, HistoryWritePred> history_write_pred_;
    uint64_t history_commit_seq_ = 0;
    std::vector<std::string> history_lines_;
    uint64_t reclamation_epoch_;

    TxID txid_;
    // True from begin() to the end of writePhase() or abort().
    bool transaction_open_ = false;
    bool reconnoitering_ = false;
    bool is_forwarded_;
    bool has_write_ = false;
    bool has_insert_ = false;
    bool scan_conflict_found_ = false;  // a scan covers one of this transaction's inserts

    // --l1_cost_diag: one #l1cost line per commit of thread 0 (the BoMB L1 thread).
    uint64_t l1d_t0_ = 0, l1d_val_ = 0, l1d_facts_ = 0;
    uint64_t l1d_trav_ = 0, l1d_reads_ = 0;
    uint64_t l1d_imp_ = 0, l1d_fp_ = 0, l1d_hist_ = 0;
    bool l1d_on() const { return FLAGS_l1_cost_diag && thid_ == 0; }

    // Work of the current validation, for --l1_cost_diag.
    struct DiagVal {
        uint64_t hist = 0, deltas = 0, fp_keys = 0;
    } diag_;
    void diag_fan_record() {
        if (l1d_on()) { l1d_imp_ += diag_.deltas; l1d_fp_ += diag_.fp_keys; l1d_hist_ += diag_.hist; }
        diag_ = DiagVal{};
    }
    uint8_t thid_ = 0;

    const bool& quit_; // for thread termination control

    TxExecutor(uint8_t thid, Backoff& backoff, Result *res, const bool &quit)
        : result_(res), thid_(thid), backoff_(backoff), quit_(quit) {
        txid_.thid = thid_;
        txid_.tid = 0;

        is_forwarded_ = false;
    }

    ~TxExecutor() {
        read_set_.clear();
        write_set_.clear();
        if (!FLAGS_history_dump_path.empty()) {
            flush_history_log();
        }
    }

    static std::string history_hex_encode(const std::string& s) {
        static const char* kHexDigits = "0123456789abcdef";
        std::string out;
        out.reserve(s.size() * 2);
        for (unsigned char c : s) {
            out.push_back(kHexDigits[c >> 4]);
            out.push_back(kHexDigits[c & 0xf]);
        }
        return out;
    }

    // One JSON line per committed transaction; runs before writePhase() clears the sets.
    void append_history_record() {
        std::ostringstream line;
        line << "{\"txid\":\"" << this->txid_ << "\","
             << "\"order\":" << history_commit_seq_++ << ","
             << "\"reads\":[";
        for (size_t i = 0; i < read_set_.size(); ++i) {
            const auto& re = read_set_[i];
            if (i > 0) line << ",";
            line << "{\"s\":" << static_cast<uint32_t>(re.storage_)
                 << ",\"key\":\"" << history_hex_encode(re.key_)
                 << "\",\"vtxid\":\"" << re.txid_ << "\"}";
        }
        line << "],\"writes\":[";
        for (size_t i = 0; i < write_set_.size(); ++i) {
            const auto& we = write_set_[i];
            if (i > 0) line << ",";
            line << "{\"s\":" << static_cast<uint32_t>(we.storage_)
                 << ",\"key\":\"" << history_hex_encode(we.key_) << "\",\"pred\":";
            if (we.op_ == OpType::INSERT) {
                // An inserted row's first version has no predecessor.
                line << "null,\"install_seq\":null";
            } else if (auto it = history_write_pred_.find(we.rcdptr_);
                       it != history_write_pred_.end()) {
                line << "\"" << it->second.pred << "\",\"install_seq\":"
                     << it->second.install_seq;
            } else {
                line << "null,\"install_seq\":null";
            }
            line << "}";
        }
        line << "]}";
        history_lines_.push_back(line.str());
    }

    void flush_history_log() {
        if (history_lines_.empty()) return;
        const std::string path =
            FLAGS_history_dump_path + "/history_" + std::to_string(thid_) + ".log";
        std::ofstream out(path, std::ios::out | std::ios::trunc);
        for (const auto& line : history_lines_) {
            out << line << "\n";
        }
    }

    void abort();

    void mainte();  // maintenance

    void begin();

    Status read(Storage s, std::string_view key, TupleBody** body);
    Status read_internal(Storage s, std::string_view key, Tuple* tuple, Version** return_ver);

    Status write(Storage s, std::string_view key, TupleBody&& body);

    Status insert(Storage s, std::string_view key, TupleBody&& body);

    Status delete_record(Storage s, std::string_view key);

    Status scan(Storage s,
                std::string_view left_key, bool l_exclusive,
                std::string_view right_key, bool r_exclusive,
                std::vector<TupleBody *>&result);

    Status scan(Storage s,
                std::string_view left_key, bool l_exclusive,
                std::string_view right_key, bool r_exclusive,
                std::vector<TupleBody *>&result, int64_t limit);

    bool read_validation(KeySet &target_set, KeySet &finished_set);
    bool write_validation(WriteElement<Tuple> element, KeySet &finished_set, KeySet &propagate_set);
    bool insert_validation(WriteElement<Tuple> element);
    bool validation();

    void writePhase();

    bool commit();

    void reconnoiter_begin();
    void reconnoiter_end();

    bool holds_protected_state() const {
        return !import_pins_.empty() || !read_set_.empty() || !write_set_.empty() ||
               !published_seq_.empty() || !published_prefix_epoch_.empty() ||
               !history_write_pred_.empty() || last_published_tuple_ != nullptr ||
               !resolve_cache_.empty() || !graph_.empty() || !readSet_.empty() ||
               !writeSet_.empty() || !last_imported_seq_.empty() ||
               !import_visited_.empty() || transaction_open_ || reconnoitering_;
    }

    /**
     * @brief Leaves the epoch set while waiting for a request, so that an idle worker holds back no epoch or floor.
     * @return True if the transition happened; false if the worker holds protected state.
     */
    bool become_quiescent() {
        if (holds_protected_state()) return false;
        EpochMembershipChange membership_change;
        if (ThManagementTable[thid_]->atomicLoadParticipation() ==
            ParticipationState::Quiescent) {
            return false;
        }
        // Withdraw the floor before leaving the epoch set.
        ReclamationFloorAnnouncer::withdraw(thid_);
        ThManagementTable[thid_]->atomicStoreParticipation(
            ParticipationState::Quiescent);
        return true;
    }

    /**
     * @brief Rejoins the epoch set before a transaction begins (Quiescent, Entering, Active).
     * 
     * Entering counts in chkEpochLoaded(), so the global epoch cannot advance while the stamp is stale.
     */
    void resume_active() {
        ThreadManagementEntry* slot = ThManagementTable[thid_];
        if (slot->atomicLoadParticipation() == ParticipationState::Active) return;
        EpochMembershipChange membership_change;
        if (slot->atomicLoadParticipation() == ParticipationState::Active) return;
        slot->atomicStoreParticipation(ParticipationState::Entering);
        const uint64_t ge = atomicLoadGE();
        slot->atomicStoreEpoch(ge);
        slot->atomicStoreParticipation(ParticipationState::Active);
    }

    bool isLeader();

    void leaderWork();

    // Bounded reclamation passes run from mainte(), each every reclaim_interval calls.
    void reclaim_record_graph_states();
    uint64_t state_reclaim_tick_ = 0;
    void reclaim_retired_rows();
    uint64_t row_reclaim_tick_ = 0;
    void reclaim_retired_versions();
    uint64_t version_reclaim_tick_ = 0;
    void reclaim_retired_chunks();
    uint64_t chunk_reclaim_tick_ = 0;

    void backoff() {
        Backoff::backoff(FLAGS_clocks_per_us);
    }

    /**
     * @brief The condition of the single-writer insert-only rule.
     * @return True if every version in the read set has the same writer (an empty set counts).
     */
    bool read_set_single_writer() {
        auto itr = read_set_.begin();
        if (itr == read_set_.end()) return true;
        const TxID writer = itr->txid_;
        for (++itr; itr != read_set_.end(); ++itr) {
            if (itr->txid_ != writer) return false;
        }
        return true;
    }

    Version *newVersionGeneration([[maybe_unused]] Tuple *tuple) {
        return new Version(this->txid_);
    }

    Version *newVersionGeneration([[maybe_unused]] Tuple *tuple, TupleBody&& body) {
        return new Version(this->txid_, std::move(body));
    }

    // --read_set_hash_index: an index over read_set_, built lazily; it keeps the first
    // occurrence, like the linear scan.
    std::unordered_map<std::string, uint32_t> rs_index_;
    size_t rs_indexed_ = 0;
    static std::string rs_index_key(Storage s, std::string_view key) {
        std::string k;
        k.reserve(key.size() + 4);
        const uint32_t sv = static_cast<uint32_t>(s);
        k.append(reinterpret_cast<const char*>(&sv), sizeof(sv));
        k.append(key.data(), key.size());
        return k;
    }
    void reset_read_set_index() {
        rs_index_.clear();
        rs_indexed_ = 0;
    }

    inline ReadElement<Tuple> *searchReadSet(Storage s, std::string_view key) {
        if (FLAGS_read_set_hash_index) {
            if (read_set_.size() < rs_indexed_) reset_read_set_index();
            for (; rs_indexed_ < read_set_.size(); ++rs_indexed_) {
                const auto& re = read_set_[rs_indexed_];
                rs_index_.emplace(rs_index_key(re.storage_, re.key_),
                                  static_cast<uint32_t>(rs_indexed_));
            }
            auto it = rs_index_.find(rs_index_key(s, key));
            return it == rs_index_.end() ? nullptr : &read_set_[it->second];
        }
        for (auto &re : read_set_) {
            if (re.storage_ != s) continue;
            if (re.key_ == key) return &re;
        }
        return nullptr;
    }

    inline WriteElement<Tuple> *searchWriteSet(Storage s, std::string_view key) {
        for (auto &we : write_set_) {
            if (we.storage_ != s) continue;
            if (we.key_ == key) return &we;
        }

        return nullptr;
    }

    inline WriteElement<Tuple> *searchWriteSet(Tuple* tuple) {
        for (auto &we : write_set_) {
            if (we.rcdptr_ == tuple) return &we;
        }

        return nullptr;
    }

    /**
     * @brief Appends one entry to a record's history and updates the epoch bounds that protect it.
     * 
     * The history's bounds change before anyone can cite the new sequence, and this transaction's
     * citation of the record widens to it.
     * @param tuple The record; the caller holds its latch.
     * @param delta The entry.
     * @return The entry's sequence number, or 0 if the row has been deleted.
     */
    uint64_t append_delta(Tuple* tuple, GraphDelta delta) {
        RecordGraphState* const row_state = tuple->graph_state_ptr();
        if (row_state == nullptr) return 0;
        // The registry may free this history once this epoch is below every live floor.
        const uint64_t delta_epoch =
            (delta.kind_ == GraphDeltaKind::PublishRead ||
             delta.kind_ == GraphDeltaKind::AddEdge)
                ? delta_retirement_epoch(delta)
                : static_cast<uint64_t>(delta.txid_.epoch);
        const uint64_t retire_epoch = delta_retirement_epoch(delta);
        const uint64_t seq = row_state->append(std::move(delta));
        if (l1d_on()) ++l1d_facts_;
        row_state->observe_cite_epoch(delta_epoch);
        row_state->observe_prefix_epoch(retire_epoch);
        published_seq_.insert_or_assign(tuple, seq);
        published_prefix_epoch_[tuple] =
            row_state->max_prefix_epoch_.load(std::memory_order_relaxed);
        return seq;
    }

    /**
     * @brief Publishes a read fact at read time, so that a later writer of the record finds the reader.
     * @param tuple The record; the caller holds its latch.
     * @param txid The reader.
     * @param reads_from The writer of the version read.
     * @return The entry's sequence number.
     */
    uint64_t append_publish_read(Tuple* tuple, TxID txid, TxID reads_from) {
        GraphDelta delta;
        delta.kind_ = GraphDeltaKind::PublishRead;
        delta.txid_ = txid;
        delta.reads_from_txid_ = reads_from;
        delta.retire_epoch_ = static_cast<uint32_t>(
            std::max<uint64_t>(txid.epoch, reads_from.epoch));
        return append_delta(tuple, std::move(delta));
    }

    /**
     * @brief Publishes a dependency edge in a record's history.
     * @param tuple The record; the caller holds its latch.
     * @param src The edge's source.
     * @param dst The edge's destination.
     * @param kind Read-from or anti-dependency.
     * @return The entry's sequence number.
     */
    uint64_t append_edge(Tuple* tuple, TxID src, TxID dst, GraphEdgeKind kind) {
        GraphDelta delta;
        delta.kind_ = GraphDeltaKind::AddEdge;
        delta.txid_ = src;
        delta.other_txid_ = dst;
        delta.retire_epoch_ = static_cast<uint32_t>(
            std::max<uint64_t>(src.epoch, dst.epoch));
        delta.edge_kind_ = kind;
        return append_delta(tuple, std::move(delta));
    }

    /**
     * @brief The retirement bound of a reference to a record's prefix: the maximum over the prefix.
     * @param included_tuple The cited record.
     * @return The transitive retirement bound.
     */
    uint64_t citation_reach_epoch(Tuple* included_tuple) const {
        if (included_tuple == nullptr) return this->txid_.epoch;
        uint64_t reach = this->txid_.epoch;
        if (auto it = published_prefix_epoch_.find(included_tuple);
            it != published_prefix_epoch_.end()) {
            reach = std::max(reach, it->second);
        } else {
            PinnedRecord pin;
            const RecordGraphState* st = included_tuple->graph_state_ptr();
            if (st == nullptr) {
                pin = PinnedRecord(included_tuple->detached_graph_id());
                st = pin.get();
            }
            if (st != nullptr) reach = std::max(reach, st->prefix_epoch());
        }
        return reach;
    }

    /**
     * @brief Publishes a prefix reference instead of copying the facts of another record's history.
     * 
     * It names the cited history by stable identity and raises the cited history's bound, so that
     * the history outlives the citing transaction.
     * @param tuple The citing record; the caller holds its latch.
     * @param included_tuple The cited record.
     * @param included_seq The prefix's last sequence.
     * @return The entry's sequence number, or 0 if a row is gone.
     */
    uint64_t append_include(Tuple* tuple, Tuple* included_tuple, uint64_t included_seq) {
        RecordGraphState* const row_state = tuple->graph_state_ptr();
        if (row_state == nullptr) return 0;
        PinnedRecord included_pin;
        RecordGraphState* included_state = nullptr;
        if (included_tuple != nullptr) {
            included_state = included_tuple->graph_state_ptr();
            if (included_state == nullptr) {
                included_pin = PinnedRecord(included_tuple->detached_graph_id());
                included_state = included_pin.get();
                if (included_state == nullptr) return 0;
            }
        }
        GraphDelta delta;
        delta.kind_ = GraphDeltaKind::IncludeRecord;
        delta.txid_ = this->txid_;
        delta.retire_epoch_ =
            static_cast<uint32_t>(citation_reach_epoch(included_tuple));
        delta.include_record_id_ =
            included_state == nullptr ? RecordId() : included_state->id_;
        delta.include_seq_ = included_seq;
        if (included_tuple != nullptr) {
            included_state->observe_cite_epoch(this->txid_.epoch);
            included_state->cite_count_.fetch_add(
                1, std::memory_order_relaxed);
        }
        const uint64_t include_seqno = append_delta(tuple, std::move(delta));
        const uint64_t included_depth =
            included_tuple == nullptr
                ? 0
                : included_state->include_depth_hint_.load(std::memory_order_acquire);
        const uint64_t new_depth = included_depth + 1;
        const uint64_t prior_depth =
            row_state->include_depth_hint_.load(std::memory_order_relaxed);
        if (new_depth > prior_depth) {
            row_state->include_depth_hint_.store(
                new_depth, std::memory_order_release);
        }
        return include_seqno;
    }

    /**
     * @brief Publishes that a transaction aborted, so that its facts stop counting at this record.
     * @param tuple The record; the caller holds its latch.
     * @param txid The aborted transaction.
     * @return The entry's sequence number.
     */
    uint64_t append_abort(Tuple* tuple, TxID txid) {
        GraphDelta delta;
        delta.kind_ = GraphDeltaKind::MarkAborted;
        delta.txid_ = txid;
        return append_delta(tuple, std::move(delta));
    }

    /**
     * @brief Applies one imported history entry to the working graph.
     * @param work_graph The graph being reconstructed.
     * @param tuple The live row that owns the history, or nullptr if the row has been deleted.
     * @param delta The entry.
     */
    void apply_delta(Graph &work_graph, Tuple* tuple, const GraphDelta &delta) {
        auto ensure_node = [&](const TxID &txid) -> TxNode& {
            if (auto it = work_graph.find(txid); it != work_graph.end()) {
                return it->second;
            }
            work_graph.emplace(txid, TxNode(txid));
            return work_graph.at(txid);
        };

        switch (delta.kind_) {
            case GraphDeltaKind::PublishRead: {
                TxNode &node = ensure_node(delta.txid_);
                TxNode &source = ensure_node(delta.reads_from_txid_);
                // A deleted row adds no read-set entry; the edges below are recorded either way.
                if (tuple != nullptr) {
                    node.readSet_[row_key(tuple)] = delta.reads_from_txid_;
                }
                node.from_.insert(delta.reads_from_txid_);
                source.readBy_.insert(delta.txid_);
                break;
            }
            case GraphDeltaKind::AddEdge: {
                TxNode &src = ensure_node(delta.txid_);
                TxNode &dst = ensure_node(delta.other_txid_);
                if (delta.edge_kind_ == GraphEdgeKind::ReadBy) {
                    src.readBy_.insert(delta.other_txid_);
                } else {
                    src.writtenBy_.insert(delta.other_txid_);
                }
                dst.from_.insert(delta.txid_);
                break;
            }
            case GraphDeltaKind::MarkAborted: {
                ensure_node(delta.txid_);
                clean_up_node_edges(work_graph, delta.txid_);
                break;
            }
            case GraphDeltaKind::EnsureNode:
            case GraphDeltaKind::IncludeRecord:
                // References are followed by the importer itself.
                break;
        }
    }

    uint64_t imported_upto(RecordId record) const {
        if (auto it = last_imported_seq_.find(record);
            it != last_imported_seq_.end()) {
            return it->second;
        }
        return 0;
    }

    uint64_t imported_upto(Tuple* tuple) const {
        if (tuple == nullptr) return 0;
        return imported_upto(tuple->graph_id());
    }

    void remember_aborted_nodes(const Graph& graph) {
        for (const auto& [txid, node] : graph) {
            if (node.is_aborted) import_aborted_.insert(txid);
        }
    }

    void normalize_aborted_nodes(Graph& graph, const TxSet& aborted) {
        for (const auto& txid : aborted) {
            if (graph.find(txid) == graph.end()) {
                graph.emplace(txid, TxNode(txid));
            }
            clean_up_node_edges(graph, txid);
        }
    }

    // Memo of RecordId -> live row for one transaction; a deleted row resolves to nullptr.
    mutable std::vector<Tuple*> resolve_cache_;
    mutable FlatRecordWatermarks resolve_cache_index_;  // RecordId -> slot+1

    Tuple* resolve_live_record(RecordId record) const {
        if (!record.valid()) return nullptr;
        if (const uint64_t slot = resolve_cache_index_.get(record); slot != 0) {
            return resolve_cache_[slot - 1];
        }
        Tuple* owner = RecordGraphRegistry::instance().live_owner(record);
        resolve_cache_.push_back(owner);
        resolve_cache_index_.set_max(record, resolve_cache_.size());
        return owner;
    }

    // Records already considered by the current discovery: a stamp per record makes
    // clearing O(1).
    mutable FlatRecordWatermarks propagation_seen_;
    mutable uint64_t propagation_mark_epoch_ = 1;

    // True the first time this record is seen in the current discovery.
    INLINE bool mark_propagation_record(RecordId record) const {
        if (!record.valid()) return false;
        if (propagation_seen_.get(record) == propagation_mark_epoch_) {
            return false;
        }
        propagation_seen_.set_max(record, propagation_mark_epoch_);
        return true;
    }

    void clear_propagation_marks() const {
        ++propagation_mark_epoch_;
    }

    void clear_resolve_cache() {
        ++propagation_mark_epoch_;
        // Release the table only once it has grown.
        if (propagation_seen_.capacity() > 4096) {
            propagation_seen_ = FlatRecordWatermarks();
            propagation_mark_epoch_ = 1;
        }
        if (resolve_cache_.empty()) return;
        resolve_cache_.clear();
        resolve_cache_index_ = FlatRecordWatermarks();
    }

    // Clears the propagation marks on entry and on exit.
    struct PropagationMarkScope {
        explicit PropagationMarkScope(const TxExecutor& tx) : tx_(tx) {
            tx_.clear_propagation_marks();
        }
        ~PropagationMarkScope() { tx_.clear_propagation_marks(); }
        PropagationMarkScope(const PropagationMarkScope&) = delete;
        PropagationMarkScope& operator=(const PropagationMarkScope&) = delete;
        const TxExecutor& tx_;
    };

    // A reference whose target was reclaimed is skipped: reclamation requires every
    // citation of it to be retirable.
    void note_unresolved_include() const {
        RecordGraphRegistry::counters().unresolved_include_skips.fetch_add(
            1, std::memory_order_relaxed);
    }

    /**
     * @brief Reconstructs a history's facts in the working graph, following prefix references.
     * 
     * Entries at or below the watermark are skipped; with --import_skip_retirable so is the
     * leading run of entries that retirement would drop at this transaction's floor.
     * @param state The history, pinned by the caller or owned by a live row.
     * @param upto_seq Import up to this sequence, which gives the validator a stable view.
     * @param work_graph The graph being reconstructed.
     * @return False if a nested import failed.
     */
    bool import_record_recursive_impl(RecordGraphState* state, uint64_t upto_seq,
                                      Graph& work_graph) {
        if (state == nullptr) return false;
        const RecordId record = state->id_;
        Tuple* owner = state->owner_.load(std::memory_order_acquire);

        const uint64_t target_seq = std::min(upto_seq, state->committed());
        if (imported_upto(record) >= target_seq) return true;

        // Visited (record, cutoff) pairs keep cyclic and diamond-shaped reference graphs finite.
        const auto visit_key = std::make_pair(record, target_seq);
        if (!import_visited_.insert(visit_key).second) return true;

        const uint64_t retired_seq =
            state->retired_seq_.load(std::memory_order_acquire);
        const uint64_t start_seq = std::max(imported_upto(record), retired_seq);
        if (target_seq > start_seq) {
            ++diag_.hist;
            diag_.deltas += target_seq - start_seq;
        }

        bool import_ok = true;
        bool in_retirable_prefix = FLAGS_import_skip_retirable;
        state->for_each_delta(
            start_seq, target_seq, [&](const GraphDelta& delta) {
                if (in_retirable_prefix) {
                    if (delta_retirement_epoch(delta) < this->reclamation_epoch_) {
                        return true;
                    }
                    in_retirable_prefix = false;
                }
                if (delta.kind_ == GraphDeltaKind::IncludeRecord) {
                    // The pin is held for the whole import (import_pins_).
                    RecordGraphState* child =
                        pin_for_import(delta.include_record_id_);
                    if (child == nullptr) {
                        note_unresolved_include();
                        return true;
                    }
                    if (!import_record_recursive_impl(
                            child, delta.include_seq_, work_graph)) {
                        import_ok = false;
                        return false;
                    }
                } else if (delta.kind_ == GraphDeltaKind::MarkAborted) {
                    import_aborted_.insert(delta.txid_);
                } else {
                    apply_delta(work_graph, owner, delta);
                }
                return true;
            });
        if (!import_ok) return false;

        const uint64_t already_imported = imported_upto(record);
        if (target_seq > already_imported) {
            last_imported_seq_[record] = target_seq;
        }
        return true;
    }

    /**
     * @brief Imports a record's history and everything it references, as a merge would have provided.
     * 
     * A deleted row is reached by identity under a pin; a reclaimed one holds only retirable facts.
     * @param tuple The record.
     * @param upto_seq Import up to this sequence.
     * @param work_graph The graph being reconstructed.
     * @return False if the import failed.
     */
    bool import_record(Tuple* tuple, uint64_t upto_seq, Graph& work_graph) {
        import_visited_.clear();
        import_aborted_.clear();
        remember_aborted_nodes(work_graph);
        RecordGraphState* root_state =
            tuple == nullptr ? nullptr : tuple->graph_state_ptr();
        if (tuple != nullptr && root_state == nullptr) {
            root_state = pin_for_import(tuple->detached_graph_id());
            if (root_state == nullptr) {
                release_import_pins();
                return true;
            }
        }
        const bool ok = import_record_recursive_impl(
            root_state, upto_seq, work_graph);
        if (ok) {
            normalize_aborted_nodes(work_graph, import_aborted_);
        }
        import_visited_.clear();
        import_aborted_.clear();
        release_import_pins();
        return ok;
    }

    void reset_identity_caches() {
        release_import_pins();
        clear_resolve_cache();
    }

    /**
     * @brief Advances the record's retirement watermark over entries no live transaction can need.
     * 
     * An entry whose bound is strictly below this transaction's floor stamps only transactions
     * that finished before every live one began, so no cycle through a live transaction can use
     * it. The test is strict because begin() may label a transaction one epoch below its stamp.
     * A persisted cursor makes a call that finds nothing new O(1). With freeing, wholly retired
     * chunks are then unlinked.
     * @param tuple The record; the caller holds its latch.
     * @return False only for a null record.
     */
    bool retire_record_graph_log_facts(Tuple* tuple) {
        if (tuple == nullptr) return false;
        RecordGraphLog& log = tuple->graph_log();
        const uint64_t committed = log.committed_seq_.load(std::memory_order_relaxed);
        const uint64_t watermark = log.retired_seq_.load(std::memory_order_relaxed);
        uint64_t new_retired = watermark;

        uint64_t seq = log.retire_cursor_seq_;
        GraphDeltaChunk* chunk = log.retire_cursor_chunk_;
        if (chunk == nullptr || seq <= watermark) {
            seq = watermark + 1;
            if (FLAGS_free_retired_history_chunks) {
                // head_ may no longer be chunk 0; it never passes watermark + 1.
                chunk = log.chunk_containing(seq);
                ChunkRetirementRegistry::counters().cursor_relocations.fetch_add(
                    1, std::memory_order_relaxed);
            } else {
                chunk = log.head_.load(std::memory_order_acquire);
                for (uint64_t skip = (seq - 1) / kGraphDeltaChunkSlots;
                     skip > 0 && chunk != nullptr; --skip) {
                    chunk = chunk->next_.load(std::memory_order_acquire);
                }
            }
        }

        uint64_t slot = (seq - 1) % kGraphDeltaChunkSlots;
        while (seq <= committed) {
            const GraphDelta& delta = chunk->slots_[slot];
            if (!(delta_retirement_epoch(delta) < this->reclamation_epoch_)) {
                break;
            }
            // A retired reference no longer keeps its target cited.
            if (delta.kind_ == GraphDeltaKind::IncludeRecord &&
                delta.include_record_id_.valid()) {
                PinnedRecord cited(delta.include_record_id_);
                if (cited.valid()) {
                    uint64_t count =
                        cited->cite_count_.load(std::memory_order_relaxed);
                    while (count > 0 &&
                           !cited->cite_count_.compare_exchange_weak(
                               count, count - 1, std::memory_order_relaxed)) {
                    }
                }
            }
            new_retired = seq;
            ++seq;
            ++slot;
            if (slot == kGraphDeltaChunkSlots) {
                slot = 0;
                chunk = chunk->next_.load(std::memory_order_acquire);
            }
        }

        // The cursor stops at the first entry that is not yet retirable.
        log.retire_cursor_seq_ = seq;
        log.retire_cursor_chunk_ = chunk;

        if (new_retired > watermark) {
            log.retired_seq_.store(new_retired, std::memory_order_release);
        }
        // A wholly retired tail chunk becomes unlinkable once an append makes it a non-tail chunk.
        if (FLAGS_free_retired_history_chunks) {
            log.unlink_retired_prefix(std::max(new_retired, watermark));
        }
        return true;
    }

    /**
     * @brief Runs retirement once the history exceeds --retirement_threshold entries.
     * 
     * If this transaction published here, its citation of the record widens to the whole history.
     * @param tuple The record; the caller holds its latch.
     * @return False if retirement failed; the caller aborts.
     */
    bool maybe_retire_record_history(Tuple* tuple) {
        if (!FLAGS_saros) return true;
        if (tuple == nullptr) return false;
        if (row_detached(tuple)) return true;
        if (FLAGS_retirement_threshold == 0) return true;
        if (tuple->graph_log().committed() <= FLAGS_retirement_threshold) {
            return true;
        }
        const bool had_published_state =
            published_seq_.find(tuple) != published_seq_.end();
        const bool was_last_published_tuple = last_published_tuple_ == tuple;

        if (!retire_record_graph_log_facts(tuple)) {
            return false;
        }

        if (had_published_state) {
            published_seq_[tuple] = tuple->graph_log().committed();
            published_prefix_epoch_[tuple] = tuple->graph_log().prefix_epoch();
            if (was_last_published_tuple) {
                last_published_tuple_ = tuple;
            }
        }

        return true;
    }

    /**
     * @brief Widens this transaction's citation of a record to what it imported, so that a later reference covers it.
     * @param tuple The record; the caller holds its latch.
     * @param imported_seq The sequence imported up to.
     */
    void widen_citation_to_imported(Tuple* tuple, uint64_t imported_seq) {
        if (row_detached(tuple)) return;
        auto it = published_seq_.find(tuple);
        if (it == published_seq_.end() || it->second >= imported_seq) return;
        if (imported_seq != tuple->graph_log().committed()) return;
        it->second = imported_seq;
        published_prefix_epoch_[tuple] = tuple->graph_log().prefix_epoch();
    }

    // An aborted insert's row leaves the index and is retired with its version; an aborted
    // update or delete unlinks only its new version.
    void writeSetClean() {
        for (auto itr = write_set_.begin(); itr != write_set_.end(); ++itr) {
            (*itr).new_ver_->status_.store(VersionStatus::aborted, std::memory_order_release);
            if ((*itr).op_ == OpType::INSERT) {
                Masstrees[get_storage((*itr).storage_)].remove_value((*itr).key_);
                RowRetirementRegistry::counters()
                    .retired_aborted_insert.fetch_add(1, std::memory_order_relaxed);
                RowRetirementRegistry::instance().retire(
                    (*itr).rcdptr_, (*itr).new_ver_, atomicLoadGE());
            } else {
                VersionRetirementRegistry::instance().retire(
                    (*itr).rcdptr_, (*itr).new_ver_, atomicLoadGE());
            }
        }
        write_set_.clear();
    }

    void addScanEntry(const Storage s,
                      std::string_view left_key, bool l_exclusive,
                      std::string_view right_key, bool r_exclusive) {
        auto index = SCAN_HISTORY_INDEX(s, this->txid_.epoch);
        while(true) {
            uint64_t expected = ScanRange[index].load(std::memory_order_acquire);
            uint32_t min = expected >> 32;
            uint32_t max = expected & 0xffffffff;
            uint32_t left = *reinterpret_cast<const uint32_t*>(left_key.data());
            uint32_t right = *reinterpret_cast<const uint32_t*>(right_key.data());
            uint64_t updated;
            if (max < right) {
              updated |= static_cast<uint64_t>(right) & 0xffffffff;
            } else {
              updated |= static_cast<uint64_t>(max) & 0xffffffff;
            }
            if (left < min) {
              updated |= static_cast<uint64_t>(left) << 32;
            } else {
              updated |= static_cast<uint64_t>(min) << 32;
            }
            if (expected != updated) {
              if (ScanRange[index].compare_exchange_strong(
                  expected, updated, memory_order_acq_rel, memory_order_acquire))
                break;
            } else {
              break;
            }
        }
        while(true) {
          ScanEntry* expected = ScanHistory[index].load(std::memory_order_acquire);
          ScanEntry* new_entry = new ScanEntry(txid_, left_key, l_exclusive,
                                               right_key, r_exclusive, expected);
          if (ScanHistory[index].compare_exchange_strong(expected, new_entry,
                                                         memory_order_acq_rel, memory_order_acquire))
            break;
        }
    }

    // Transactions in g (other than this one) that read `key`.
    void find_readers(const Graph &g, const Key key, TxSet &readers) {
        for (const auto& [txid, node] : g) {
            if (txid == this->txid_) continue;  // a read-modify-write
            if (node.is_aborted) continue;
            if (auto it = node.readSet_.find(row_key(key));
                it != node.readSet_.end()) {
                readers.insert(txid);
            }
        }
    }

    // Writers of the versions `readers` read of `key`: order forwarding places the new version
    // before them when reader -> writer edges would close a cycle.
    void find_followers(const Graph &g, const Key key, TxSet &readers, TxSet &followers) {
        for (const auto& reader : readers) {
            if (auto node = g.find(reader); node != g.end()) {
                if (auto read = node->second.readSet_.find(row_key(key));
                    read != node->second.readSet_.end()) {
                    followers.insert(read->second);
                    continue;
                }
            }
        }
    }

    void find_reachable_to_dfs(const Graph &g, std::map<TxID,bool> &seen, const TxID txID) {
        if (l1d_on()) l1d_trav_ += 1 + g.at(txID).readBy_.size() + g.at(txID).writtenBy_.size();
        seen.emplace(txID, true);
        TxSet edges(g.at(txID).readBy_);
        edges.insert(g.at(txID).writtenBy_.begin(), g.at(txID).writtenBy_.end());

        for (const auto& next : edges) {
            if (auto it = seen.find(next); it != seen.end() && seen[next]) continue;
            if (auto it = g.find(next); it != g.end())
                find_reachable_to_dfs(g, seen, next);
        }
    }

    // Transactions reachable from `start` along readBy and writtenBy edges.
    void find_reachable_to(const TxID start, const Graph &g, TxSet &txns) {
        std::map<TxID,bool> seen;
        for (const auto& next : g.at(start).readBy_) {
            if (auto it = g.find(next); it != g.end())
                find_reachable_to_dfs(g, seen, next);
        }
        for (const auto& next : g.at(start).writtenBy_) {
            if (auto it = g.find(next); it != g.end())
                find_reachable_to_dfs(g, seen, next);
        }
        for (const auto& [id, _] : seen)
            txns.insert(id);
    }

    // Walks from `ver` to the newest committed or deleted version, collecting the pending ones skipped.
    Version* get_a_version(Version* ver, std::vector<Version*>& followers) {
        while (ver->status_.load(memory_order_acquire) != VersionStatus::committed
            && ver->status_.load(memory_order_acquire) != VersionStatus::deleted) {
            if (ver->status_.load(memory_order_acquire) == VersionStatus::aborted) {
                ver = ver->ldAcqNext();
            } else if (ver->status_.load(memory_order_acquire) == VersionStatus::pending
                || ver->status_.load(memory_order_acquire) == VersionStatus::deleting) {
                followers.push_back(ver);
                ver = ver->ldAcqNext();
            }
            if (ver == VERSION_TERMINATION) {
                return ver;
            }
            if (ver == nullptr) {
                return ver;
            }
        }
        return ver;
    }

    // Adds the candidate read-from edge ver -> this transaction to the record graph.
    void update_read_from(Tuple* tuple, Version* ver) {
        if (auto it = tuple->graph_.find(ver->txid_); it == tuple->graph_.end()) {
            tuple->graph_.emplace(ver->txid_, TxNode(ver->txid_));
        }
        tuple->graph_.at(ver->txid_).readBy_.emplace(this->txid_);
    }

    /**
     * @brief Chooses the version to read: the newest committed one that keeps the record graph acyclic.
     * 
     * Every version skipped gets an anti-dependency edge, which Saros also publishes. The follower-epoch
     * guard refuses to skip a pending version whose writer is more than one epoch older than this
     * transaction; one epoch of slack is needed because begin() may label a writer below its stamp.
     * @param tuple The record; the caller holds its latch.
     * @param return_ver Receives the version chosen.
     * @return OK, or the reason no version can be read.
     */
    Status get_visible_version(Tuple *tuple, Version **return_ver) {
        Status stat = Status::OK;
        Version* ver = nullptr;
        std::vector<Version*> followers;
        ver = tuple->ldAcqLatest();
RETRY:
        followers.clear();

        ver = get_a_version(ver, followers);
        if (ver == VERSION_TERMINATION) {
            // versions exist but no committed ones
            stat = Status::WARN_NOT_FOUND;
            goto OUT;
        }
        if (ver == nullptr) {
            // committed versions exist but no visible ones
            stat = Status::ERROR_NO_VISIBLE_VERSION;
            goto OUT;
        }

        if (reconnoitering_) {
          goto OUT;
        }

        // add anti-dependency edges found when skipping versions
        for (auto& follower : followers) {
            if (static_cast<uint64_t>(follower->txid_.epoch) + 1 < this->txid_.epoch) {
                stat = Status::ERROR_NO_VISIBLE_VERSION;
                goto OUT;
            }
            tuple->graph_.at(this->txid_).writtenBy_.emplace(follower->txid_);
            if (FLAGS_saros) {
                append_edge(tuple, this->txid_, follower->txid_,
                            GraphEdgeKind::WrittenBy);
            }
        }

        update_read_from(tuple, ver);

        if (has_cycle(tuple->graph_, this->txid_)) {
            // remove candidate edge and try next candidate
            tuple->graph_.at(ver->txid_).readBy_.erase(this->txid_);

            if (ver->txid_.epoch < this->txid_.epoch) {
                stat = Status::ERROR_NO_VISIBLE_VERSION;
                goto OUT;
            }

            // add anti-dependency edge before retry
            tuple->graph_.at(this->txid_).writtenBy_.emplace(ver->txid_);
            if (FLAGS_saros) {
                append_edge(tuple, this->txid_, ver->txid_,
                            GraphEdgeKind::WrittenBy);
            }
            ver = ver->ldAcqNext();
            if (ver == VERSION_TERMINATION) {
                stat = Status::WARN_NOT_FOUND;
                goto OUT;
            }
            if (ver == nullptr) {
                stat = Status::ERROR_NO_VISIBLE_VERSION;
                goto OUT;
            } else {
                goto RETRY;
            }
        }
OUT:
        *return_ver = ver;
        return stat;
    }

    // Adds the live row of `record` to keys, once per discovery; a dead row has nothing to validate.
    INLINE void insert_live_target_record(RecordId record, KeySet &keys) {
        if (!mark_propagation_record(record)) return;
        if (Tuple* row = resolve_live_record(record); row != nullptr) {
            keys.insert(row);
        } else {
            RowRetirementRegistry::counters().dropped_dead_targets.fetch_add(
                1, std::memory_order_relaxed);
        }
    }

    INLINE void insert_live_target(RowKey key, KeySet &keys) {
        insert_live_target_record(row_key_record(key), keys);
    }

    // The records read by `tx`, as live propagation targets.
    void add_related_read_keys(const TxNode *tx, KeySet &keys) {
        for (const auto& [key, id] : tx->readSet_) {
            insert_live_target(key, keys);
        }
    }

    // Adds to targets every key in keys that is neither queued nor validated.
    void push_candidate_keys(KeySet &targets, KeySet &done, KeySet &keys) {
        for (const auto& key : keys) {
            auto result1 = targets.find(key);
            auto result2 = done.find(key);
            if (result1 == targets.end() && result2 == done.end())
                targets.insert(key);
        }
    }

    /**
     * @brief Finds the records this transaction must still validate, without copied read sets.
     * 
     * The records read by every transaction reachable from this one in the working graph, plus
     * their read sets from the footprint registry, become targets unless already queued or validated.
     * @param g The working graph.
     * @param finished_set Records already validated.
     * @param targets Receives the new targets.
     */
    void discover_propagation_targets(const Graph &g, KeySet &finished_set,
                                      KeySet &targets) {
        KeySet keys;
        TxSet reachable_to;
        PropagationMarkScope mark_scope(*this);
        find_reachable_to(this->txid_, g, reachable_to);
        for (const auto& txid : reachable_to) {
            if (auto it = g.find(txid); it != g.end()) {
                add_related_read_keys(&it->second, keys);
            }
            if (FLAGS_saros && txid != this->txid_) {
                if (auto fp = ReadFootprintRegistry::instance().get(txid)) {
                    for (const RowKey& k : *fp) insert_live_target(k, keys);
                    diag_.fp_keys += fp->size();
                }
            }
        }
        push_candidate_keys(targets, finished_set, keys);
    }

    bool check_cycle_dfs(const Graph &g, std::map<TxID,bool> &seen, std::map<TxID,bool> &finished, const TxID txID) {
        if (l1d_on()) l1d_trav_ += 1 + g.at(txID).readBy_.size() + g.at(txID).writtenBy_.size();
        if (quit_) {
            dump(thid_, "WARN: Quit cycle check because benchmark has finished.");
            status_ = TransactionStatus::invalid;
            return true;
        }
        seen.emplace(txID, true);
        TxSet edges(g.at(txID).readBy_);
        edges.insert(g.at(txID).writtenBy_.begin(), g.at(txID).writtenBy_.end());

        for (const auto& next : edges) {
            auto f = finished.find(next);
            if (f != finished.end() && finished[next]) continue;
            auto s = seen.find(next);
            if (s != seen.end() && seen[next] && f == finished.end())
                return true;
            if (auto it = g.find(next); it != g.end()) {
                auto ret = check_cycle_dfs(g, seen, finished, next);
                if (ret)
                    return ret;
            }
        }
        finished[txID] = true;
        return false;
    }

    /**
     * @brief The serializability test of validation and of reads.
     * @param g The graph.
     * @param start The transaction checked.
     * @return True if a cycle through `start` exists; also true when the run is ending.
     */
    bool has_cycle(const Graph &g, const TxID start) {
        std::map<TxID,bool> seen, finished;
        TxSet edges(g.at(start).readBy_);
        edges.insert(g.at(start).writtenBy_.begin(), g.at(start).writtenBy_.end());

        for (const auto& next : edges) {
            if (auto it = g.find(next); it != g.end()) {
                auto ret = check_cycle_dfs(g, seen, finished, next);
                if (ret) return true;
            }
        }
        return false;
    }

    // Oze's merge of a working graph into a record graph, or back.
    void merge(Graph &target, Graph &source) {
        for (const auto& [tid, sourceTxNode] : source) {
            TxNode *targetTxNode;
            if (auto it = target.find(tid); it != target.end()) {
                targetTxNode = &target.at(tid);
            } else {
                target.emplace(tid, TxNode(tid));
                targetTxNode = &target.at(tid);
            }
            if (targetTxNode->is_aborted) {
                clean_up_node_edges(target, tid);
                continue;
            }
            targetTxNode->readSet_.insert(sourceTxNode.readSet_.begin(), sourceTxNode.readSet_.end());
            targetTxNode->writeSet_.insert(sourceTxNode.writeSet_.begin(), sourceTxNode.writeSet_.end());
            for (const auto& to : sourceTxNode.readBy_)
                targetTxNode->readBy_.insert(to);
            for (const auto& to : sourceTxNode.writtenBy_)
                targetTxNode->writtenBy_.insert(to);
            for (const auto& from : sourceTxNode.from_)
                targetTxNode->from_.insert(from);
            // Update status if the transaction in source graph is validating
            if (targetTxNode->status_ ==  TransactionStatus::inflight
                && sourceTxNode.status_ ==  TransactionStatus::validating)
                targetTxNode->status_ = TransactionStatus::validating;
        }
    }

    /**
     * @brief The minimum epoch over active workers, which labels new transactions and bounds floors.
     * @return The minimum, or the global epoch if every worker is idle.
     */
    uint64_t get_min_epoch() {
        // A mutex, not a pair of atomic loads, to see the latest membership change.
        std::lock_guard<std::mutex> membership_lock(EpochMembershipWriterMutex);
        uint64_t min = UINT64_MAX;
        for (unsigned int i = 0; i < TotalThreadNum; ++i) {
            if (!ThManagementTable[i]->countsTowardMinEpoch()) continue;
            auto e = ThManagementTable[i]->atomicLoadEpoch();
            if (e < min) min = e;
        }
        if (min == UINT64_MAX) return atomicLoadGE();
        return min;
    }

    void erase_following_edges(Graph &g, const TxID txid) {
        for (auto& reader : g.at(txid).readBy_) {
            if (auto it = g.find(reader); it != g.end())
                g.at(reader).from_.erase(txid);
        }
        for (auto& writer : g.at(txid).writtenBy_) {
            if (auto it = g.find(writer); it != g.end())
                g.at(writer).from_.erase(txid);
        }
    }

    // Oze's record-graph garbage collection: drops edges among transactions of epoch <= `epoch`
    // and nodes with no edge to or from a newer transaction.
    void gc(Graph &g, uint64_t epoch) {
        for (auto it = g.begin(); it != g.end();) {
            auto txid = (*it).first;
            auto txNode = &(*it).second;
            if (txid.epoch <= epoch) {
                bool keep = false;
                for (auto i = txNode->readBy_.begin(); i != txNode->readBy_.end();) {
                    auto reader = (*i);
                    if (reader.epoch <= epoch) {
                        if (auto it = g.find(reader); it != g.end())
                            g.at(reader).from_.erase(txid);
                        i = txNode->readBy_.erase(i);
                    } else {
                        keep = true;
                        ++i;
                    }
                }
                for (auto i = txNode->writtenBy_.begin(); i != txNode->writtenBy_.end();) {
                    auto writer = (*i);
                    if (writer.epoch <= epoch) {
                        if (auto it = g.find(writer); it != g.end())
                            g.at(writer).from_.erase(txid);
                        i = txNode->writtenBy_.erase(i);
                    } else {
                        keep = true;
                        ++i;
                    }
                }
                for (auto i = txNode->from_.begin(); i != txNode->from_.end();) {
                    auto from = (*i);
                    if (from.epoch > epoch) {
                        keep = true;
                        break;
                    }
                    ++i;
                }
                if (keep) {
                    ++it;
                    continue;
                }
                if (txNode->from_.size() > 0) {
                    ++it;
                    continue;
                }
                erase_following_edges(g, txid);
                it = g.erase(it);
            }
            else {
                ++it;
            }
        }
    }

    /**
     * @brief Links a new pending version into the chain: at the head, or below `followers` with order forwarding.
     * @param we The write.
     * @param followers Writers the new version must precede; emptied here.
     * @return OK, or an error if the record is being deleted.
     */
    Status insert_version(WriteElement<Tuple> we, TxSet& followers) {
        Tuple* tuple = we.rcdptr_;
        if (we.op_ == OpType::DELETE) {
            we.new_ver_->status_ = VersionStatus::deleting;
        } else {
            we.new_ver_->status_ = VersionStatus::pending;
        }
        Version* v = tuple->latest_;
        if (v->status_ == VersionStatus::deleting || v->status_ == VersionStatus::deleted) {
            return Status::ERROR_CONCURRENT_WRITE_OR_DELETE;
        }
        if (followers.size() == 0) {
            we.new_ver_->next_ = v;
            tuple->latest_ = we.new_ver_;
        } else {
            Version* prev = v;
            while (followers.size() > 0) {
                followers.erase(v->txid_);
                prev = v;
                v = v->next_;
            }
            prev->next_ = we.new_ver_;
            we.new_ver_->next_ = v;
        }
        if (!FLAGS_history_dump_path.empty()) {
            // The version below the new one and a global install order: forwarding can splice a write
            // below an installed one.
            const uint64_t install_seq =
                HistoryInstallSeq.fetch_add(1, std::memory_order_relaxed);
            history_write_pred_[tuple] = HistoryWritePred{v->txid_, install_seq};
        }
        return Status::OK;
    }

    // Marks txid aborted in g and removes its edges. The node stays, so that a later merge
    // from a validator that saw it in flight cannot revive it.
    void clean_up_node_edges(Graph &g, const TxID txid) {
        for (const auto& to : g.at(txid).writtenBy_) {
            if (auto it = g.find(to); it != g.end())
                g.at(to).from_.erase(txid);
        }
        for (const auto& from : g.at(txid).from_) {
            if (auto it = g.find(from); it != g.end()) {
                g.at(from).readBy_.erase(txid);
                g.at(from).writtenBy_.erase(txid);
            }
        }
        g.at(txid).readBy_.clear();
        g.at(txid).writtenBy_.clear();
        g.at(txid).from_.clear();
        g.at(txid).is_aborted = true;
    }

    // A committed delete removes the row from the index and retires it.
    void commit_version(WriteElement<Tuple> we) {
      if (we.op_ == OpType::DELETE) {
        Masstrees[get_storage(we.storage_)].remove_value(we.key_);
        we.new_ver_->status_.store(VersionStatus::deleted, std::memory_order_release);
        RowRetirementRegistry::counters()
            .retired_committed_delete.fetch_add(1, std::memory_order_relaxed);
        RowRetirementRegistry::instance().retire(we.rcdptr_, nullptr,
                                                 atomicLoadGE());
      } else {
        we.new_ver_->status_.store(VersionStatus::committed, std::memory_order_release);
      }
    }

    std::string to_string() {
        return std::to_string(txid_.epoch) + "_"
                + std::to_string(txid_.thid) + "_" + std::to_string(txid_.tid);
    }
};
