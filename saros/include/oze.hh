#pragma once

#include <algorithm>
#include <array>
#include <atomic>
#include <cstddef>
#include <cstdint>
#include <deque>
#include <limits>
#include <map>
#include <memory>
#include <mutex>
#include <set>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include <gflags/gflags.h>

#include "common/cache_line_size.hh"
#include "tuple.hh"
#include "transaction_status.hh"
#include "txid.hh"

// Defined in common.hh; RecordGraphState's readers and writers branch on it.
DECLARE_bool(free_retired_history_chunks);

class Tuple;
// A propagation target: a row that validation latches. A KeySet never outlives its transaction.
typedef Tuple* Key;

// How read and write sets name a row: its RecordId bits. A node can outlive its row,
// and a dead row's key then resolves to nullptr rather than to freed memory.
struct RowKey {
    uint64_t bits_ = 0;

    RowKey() = default;
    explicit RowKey(uint64_t bits) : bits_(bits) {}

    bool valid() const { return bits_ != 0; }
    bool operator==(const RowKey& r) const { return bits_ == r.bits_; }
    bool operator!=(const RowKey& r) const { return bits_ != r.bits_; }
    // A total order over the bits; nothing depends on which order.
    bool operator<(const RowKey& r) const { return bits_ < r.bits_; }
};

struct RowKeyHash {
    size_t operator()(const RowKey& k) const {
        uint64_t x = k.bits_ + UINT64_C(0x9e3779b97f4a7c15);
        x ^= x >> 30; x *= UINT64_C(0xbf58476d1ce4e5b9);
        x ^= x >> 27; x *= UINT64_C(0x94d049bb133111eb);
        return static_cast<size_t>(x ^ (x >> 31));
    }
};

typedef std::map<RowKey,TxID> ReadSet;
typedef std::set<RowKey> WriteSet;
typedef std::set<Key> KeySet;
typedef std::set<TxID> TxSet;

/**
 * @brief Identity of a record's history: a registry slot and the slot's generation.
 * 
 * Stored references, such as prefix references, use it instead of row addresses:
 * deleting a row detaches its history, and a reused slot gets a new generation,
 * so an old identity never resolves to a new record.
 */
struct RecordId {
    // slot_ == 0 means "no record"; slot indices are 1-based.
    uint32_t slot_ = 0;
    uint32_t generation_ = 0;

    RecordId() = default;
    RecordId(uint32_t slot, uint32_t generation)
        : slot_(slot), generation_(generation) {}

    bool valid() const { return slot_ != 0; }
    uint64_t bits() const {
        return (static_cast<uint64_t>(generation_) << 32) |
               static_cast<uint64_t>(slot_);
    }
    bool operator==(const RecordId& r) const { return bits() == r.bits(); }
    bool operator!=(const RecordId& r) const { return bits() != r.bits(); }
    bool operator<(const RecordId& r) const { return bits() < r.bits(); }

    static size_t hash(const RecordId& id) {
        uint64_t x = id.bits() + UINT64_C(0x9e3779b97f4a7c15);
        x ^= x >> 30; x *= UINT64_C(0xbf58476d1ce4e5b9);
        x ^= x >> 27; x *= UINT64_C(0x94d049bb133111eb);
        return static_cast<size_t>(x ^ (x >> 31));
    }
};

struct RecordIdHash {
    size_t operator()(const RecordId& id) const { return RecordId::hash(id); }
};

/**
 * @brief The reclamation floor of each thread's live transaction.
 * 
 * A transaction keeps the floor it took at begin() while get_min_epoch() moves on,
 * so every reclamation pass must also respect min_live().
 */
class ReclamationFloorAnnouncer {
public:
    // A thread id at or above this bound blocks all reclamation.
    static constexpr size_t kSlots = 512;

    static void announce(size_t thid, uint64_t floor) {
        if (thid >= kSlots) { overflow().store(true, std::memory_order_release); return; }
        slots()[thid].store(floor, std::memory_order_release);
    }

    static void withdraw(size_t thid) {
        if (thid >= kSlots) return;
        slots()[thid].store(std::numeric_limits<uint64_t>::max(),
                            std::memory_order_release);
    }

    /**
     * @brief Lower bound on the reclamation floor of every live transaction.
     * @return UINT64_MAX if none is live; 0 if a thread id exceeded kSlots, so that an
     *         unexpected thread count retains everything instead of reclaiming unsoundly.
     */
    static uint64_t min_live() {
        if (overflow().load(std::memory_order_acquire)) return 0;
        uint64_t min = std::numeric_limits<uint64_t>::max();
        for (size_t i = 0; i < kSlots; ++i) {
            const uint64_t v = slots()[i].load(std::memory_order_acquire);
            if (v < min) min = v;
        }
        return min;
    }

private:
    struct Table {
        std::array<std::atomic<uint64_t>, kSlots> v;
        Table() {
            for (auto& slot : v) {
                slot.store(std::numeric_limits<uint64_t>::max(),
                           std::memory_order_relaxed);
            }
        }
    };
    static std::array<std::atomic<uint64_t>, kSlots>& slots() {
        static Table table;
        return table.v;
    }
    static std::atomic<bool>& overflow() {
        static std::atomic<bool> flag{false};
        return flag;
    }
};

struct RecordGraphState;

/**
 * @brief Owns every history, so that a history can outlive its row.
 * 
 * Slots live in chunks that never move. A slot packs its generation and pin count
 * into one word, so that "bump the generation only if nobody pins the slot" is one
 * CAS. try_pin() is the only way from a RecordId to a state.
 */
class RecordGraphRegistry {
public:
    // Statistics (monotonic unless noted), read by tests.
    struct Counters {
        std::atomic<uint64_t> states_created{0};
        std::atomic<uint64_t> states_live{0};        // created - freed
        std::atomic<uint64_t> states_detached{0};    // currently detached, not yet freed
        std::atomic<uint64_t> detach_calls{0};
        std::atomic<uint64_t> pins_taken{0};
        std::atomic<uint64_t> pins_failed{0};        // prevented ABA / stale lookups
        std::atomic<uint64_t> generation_mismatch{0};
        std::atomic<uint64_t> reclaim_attempts{0};
        std::atomic<uint64_t> reclaim_generation_bumped{0};
        std::atomic<uint64_t> reclaim_blocked_by_pins{0};
        std::atomic<uint64_t> reclaim_blocked_by_epoch{0};
        std::atomic<uint64_t> states_freed{0};
        std::atomic<uint64_t> slots_allocated{0};
        std::atomic<uint64_t> slots_reused{0};
        std::atomic<uint64_t> unresolved_include_skips{0};
        std::atomic<uint64_t> reclaim_passes{0};
        std::atomic<uint64_t> reclaim_entries_traversed{0};
        std::atomic<uint64_t> retire_list_len{0};       // length at last pass
        std::atomic<uint64_t> free_after_grace_len{0};  // length at last pass
    };

    // Totals over all record states; byte counts come from sizes and counts.
    struct RetentionSample {
        uint64_t states_scanned = 0;      // slots that still resolve
        uint64_t states_with_log = 0;     // committed_seq_ > 0
        uint64_t struct_bytes = 0;
        uint64_t deltas_committed = 0;    // sum committed_seq_
        uint64_t deltas_retired = 0;      // sum min(retired_seq_, committed_seq_)
        uint64_t chunk_bytes = 0;         // chunks still linked
        uint64_t chunk_bytes_retired_prefix = 0;  // linked chunks wholly below retired_seq_
        uint64_t chunks_unlinked = 0;     // chunks freed from the front of live histories
        uint64_t dir_bytes = 0;
    };

    // O(slots): for a low-frequency sampler only.
    void sample_retention(RetentionSample& out);

    // Never destroyed: its storage stays reachable during static destruction.
    static RecordGraphRegistry& instance() {
        static RecordGraphRegistry* registry = new RecordGraphRegistry();
        return *registry;
    }

    static Counters& counters() { return instance().counters_; }

    // A new state owned by `owner`, unpinned: the caller holds the only reference.
    RecordGraphState* create(Tuple* owner);

    RecordGraphState* try_pin(RecordId id);
    void unpin(RecordId id);

    // Identity -> the live row, or nullptr once the row has been deleted.
    Tuple* live_owner(RecordId id);

    void detach(RecordGraphState* state, uint64_t epoch);

    size_t reclaim(uint64_t now_epoch, uint64_t min_future_floor,
                   size_t budget = 256);

    // Tests: frees everything that is reclaimable now.
    size_t drain_for_test(uint64_t now_epoch) {
        size_t total = 0;
        for (int i = 0; i < 64; ++i) {
            const size_t n = reclaim(now_epoch,
                                     std::numeric_limits<uint64_t>::max(),
                                     1u << 20);
            total += n;
            if (n == 0) break;
        }
        return total;
    }

private:
    static constexpr size_t kSlotChunkSlots = 4096;

    struct Slot {
        // (generation << 32) | pins; generations start at 1.
        std::atomic<uint64_t> gen_pins_{UINT64_C(1) << 32};
        // Written only while the slot is quiescent.
        RecordGraphState* state_ = nullptr;
    };

    struct SlotChunk {
        Slot slots_[kSlotChunkSlots];
    };

    struct Retired {
        RecordGraphState* state_;
        uint64_t epoch_;
    };

    Slot* slot_for(uint32_t index) const {
        // 1-based external index.
        const size_t i = static_cast<size_t>(index) - 1;
        const size_t chunk = i / kSlotChunkSlots;
        if (chunk >= chunk_count_.load(std::memory_order_acquire)) return nullptr;
        // chunks_ never moves, so this lock-free read cannot race with growth.
        return &chunks_[chunk].load(std::memory_order_acquire)
                    ->slots_[i % kSlotChunkSlots];
    }

    static uint32_t gen_of(uint64_t word) {
        return static_cast<uint32_t>(word >> 32);
    }

    mutable std::mutex mutex_;
    // Append-only chunk index, read lock-free; chunk_count_ publishes how many.
    static constexpr size_t kMaxSlotChunks = 1u << 18;
    static constexpr size_t kMaxRecordSlots = kMaxSlotChunks * kSlotChunkSlots;
    std::atomic<SlotChunk*>* chunks_ = nullptr;
    std::atomic<size_t> chunk_count_{0};
    uint32_t next_slot_ = 1;
    std::vector<uint32_t> free_slots_;
    std::vector<Retired> retire_list_;
    std::vector<Retired> free_after_grace_;
    Counters counters_;

    RecordGraphRegistry()
        : chunks_(new std::atomic<SlotChunk*>[kMaxSlotChunks]()) {}
};

enum class GraphEdgeKind : uint8_t {
    ReadBy,
    WrittenBy,
};

// Open-addressing map RecordId -> uint64_t that keeps the maximum value set per record.
class FlatRecordWatermarks {
public:
    struct Entry {
        RecordId record_;
        uint64_t upto_ = 0;
    };

    FlatRecordWatermarks() : slots_(16) {}

    uint64_t get(RecordId record) const {
        if (!record.valid() || slots_.empty()) return 0;
        const size_t mask = slots_.size() - 1;
        size_t index = RecordId::hash(record) & mask;
        for (;;) {
            const Entry& entry = slots_[index];
            if (!entry.record_.valid()) return 0;
            if (entry.record_ == record) return entry.upto_;
            index = (index + 1) & mask;
        }
    }

    void set_max(RecordId record, uint64_t upto) {
        if (!record.valid()) return;
        if ((size_ + 1) * 10 >= slots_.size() * 7) {
            rehash(slots_.size() * 2);
        }
        insert_without_grow(record, upto);
    }

    size_t size() const { return size_; }
    size_t capacity() const { return slots_.size(); }

    template <typename Fn>
    void for_each(Fn&& fn) const {
        for (const Entry& entry : slots_) {
            if (entry.record_.valid()) fn(entry.record_, entry.upto_);
        }
    }

private:
    std::vector<Entry> slots_;
    size_t size_ = 0;

    void insert_without_grow(RecordId record, uint64_t upto) {
        const size_t mask = slots_.size() - 1;
        size_t index = RecordId::hash(record) & mask;
        for (;;) {
            Entry& entry = slots_[index];
            if (!entry.record_.valid()) {
                entry = {record, upto};
                ++size_;
                return;
            }
            if (entry.record_ == record) {
                entry.upto_ = std::max(entry.upto_, upto);
                return;
            }
            index = (index + 1) & mask;
        }
    }

    void rehash(size_t capacity) {
        std::vector<Entry> old = std::move(slots_);
        slots_.assign(capacity, Entry{});
        size_ = 0;
        for (const Entry& entry : old) {
            if (entry.record_.valid()) {
                insert_without_grow(entry.record_, entry.upto_);
            }
        }
    }
};

class TxNode {
    public:
        TxID id_;
        TransactionStatus status_;
        bool is_aborted;
        ReadSet readSet_;
        WriteSet writeSet_;
        TxSet readBy_;     // outgoing edges
        TxSet writtenBy_;  // outgoing edges
        TxSet from_;       // incoming edges
        TxNode(TxID id) :
            id_(id),
            is_aborted(false),
            status_(TransactionStatus::inflight)
            {};
        TxNode(TxID id, TransactionStatus status) :
            id_(id),
            is_aborted(false),
            status_(status)
            {};
};

typedef std::map<TxID,TxNode> Graph;

enum class GraphDeltaKind : uint8_t {
    EnsureNode,
    PublishRead,
    AddEdge,
    IncludeRecord,
    MarkAborted,
};

/**
 * @brief One entry of a record's history: a read fact, an edge, an abort mark or a prefix reference.
 * 
 * Entries are written once under the record latch and read without a lock.
 */
struct GraphDelta {
    uint64_t seq_ = 0;
    GraphDeltaKind kind_ = GraphDeltaKind::EnsureNode;

    TxID txid_;
    TxID other_txid_;

    // PublishRead: txid_ read this record from reads_from_txid_.
    TxID reads_from_txid_;

    // AddEdge: the edge txid_ -> other_txid_.
    GraphEdgeKind edge_kind_ = GraphEdgeKind::ReadBy;

    // The retirement bound (see delta_retirement_epoch()). For IncludeRecord:
    // max(citer epoch, the cited prefix's transitive retirement bound).
    uint32_t retire_epoch_ = 0;

    // IncludeRecord (prefix reference): the history of include_record_id_ up
    // to and including include_seq_.
    RecordId include_record_id_;
    uint64_t include_seq_ = 0;
};

/**
 * @brief The retirement bound of a history entry.
 * 
 * A read fact or edge carries the epoch of its newer endpoint, as gc() drops an edge
 * only when both endpoints are old; a prefix reference carries the transitive bound
 * of the cited prefix.
 * @param delta The entry.
 * @return The epoch below whose floor the entry is retirable.
 */
inline uint64_t delta_retirement_epoch(const GraphDelta& delta) {
    if (delta.kind_ == GraphDeltaKind::IncludeRecord)
        return static_cast<uint64_t>(delta.retire_epoch_);
    if (delta.kind_ == GraphDeltaKind::PublishRead ||
        delta.kind_ == GraphDeltaKind::AddEdge) {
        return std::max<uint64_t>(delta.txid_.epoch, delta.retire_epoch_);
    }
    return static_cast<uint64_t>(delta.txid_.epoch);
}

// 64 entries of 64 bytes make one 4 KiB chunk.
static_assert(sizeof(GraphDelta) == 64,
              "GraphDelta must stay 64 bytes: retire_epoch_ is meant to "
              "occupy pre-existing padding, not to grow the log.");

inline constexpr size_t kGraphDeltaChunkSlots = 64;

/**
 * @brief A fixed-size block of a history.
 * 
 * Chunks never move. They are freed with the history or, once wholly retired and
 * unlinked, one epoch grace later (ChunkRetirementRegistry); a reader inside one is
 * protected by its transaction's epoch rather than by a pin.
 */
struct GraphDeltaChunk {
    GraphDelta slots_[kGraphDeltaChunkSlots];
    std::atomic<GraphDeltaChunk*> next_{nullptr};
    // The sequence number in slots_[0]; written before the chunk is published.
    uint64_t first_seq_ = 0;
};

/**
 * @brief Append-only index from chunk number to chunk, so that a reader seeks in O(1).
 * 
 * It doubles when full. With freeing, a new directory starts at the first linked chunk
 * and the superseded one is retired; writers hold the record latch.
 */
struct GraphChunkDirectory {
    uint32_t capacity_;
    uint32_t base_;
    GraphChunkDirectory* prev_;
    std::atomic<GraphDeltaChunk*>* entries_;

    GraphChunkDirectory(uint64_t capacity, GraphChunkDirectory* prev,
                        uint64_t base = 0)
        : capacity_(static_cast<uint32_t>(capacity)),
          base_(static_cast<uint32_t>(base)),
          prev_(prev),
          entries_(new std::atomic<GraphDeltaChunk*>[capacity]) {}

    ~GraphChunkDirectory() { delete[] entries_; }

    uint64_t bytes() const {
        return sizeof(GraphChunkDirectory) +
               uint64_t{capacity_} * sizeof(std::atomic<GraphDeltaChunk*>);
    }

    GraphChunkDirectory(const GraphChunkDirectory&) = delete;
    GraphChunkDirectory& operator=(const GraphChunkDirectory&) = delete;
};
static_assert(sizeof(GraphChunkDirectory) == 24, "keeps the measured layout");

inline constexpr uint64_t kGraphChunkDirectoryInitialCapacity = 16;

// Bytes of a history's directories, from the current capacity alone; `chained`:
// superseded directories are still owned (freeing off).
inline uint64_t graph_chunk_directory_bytes(uint32_t log2_capacity, bool chained,
                                            uint64_t* dirs = nullptr) {
    if (log2_capacity == 0) { if (dirs) *dirs = 0; return 0; }
    const uint64_t entry = sizeof(std::atomic<GraphDeltaChunk*>);
    const uint64_t cap = uint64_t{1} << log2_capacity;
    if (!chained) {
        if (dirs) *dirs = 1;
        return sizeof(GraphChunkDirectory) + cap * entry;
    }
    // 16 + 32 + ... + cap = 2*cap - 16 entries over log2(cap/16)+1 directories.
    const uint64_t n = log2_capacity - 4 + 1;
    if (dirs) *dirs = n;
    return n * sizeof(GraphChunkDirectory) + (2 * cap - 16) * entry;
}

// The epoch tag of a retired chunk or directory (defined in transaction.cc).
uint64_t history_chunk_retire_tag();

/**
 * @brief Deferred freeing of unlinked history chunks and superseded directories.
 * 
 * An entry is tagged with the global epoch t at unlinking and freed once the floor
 * min(min_live(), get_min_epoch() - 1) is above t. A transaction that began at epoch
 * t + 2 or later sees only the new front of the history; one that may still hold a
 * pointer into the unlinked chunks has a floor of at most t while it is live. Retired
 * chunks are not modified, so a reader inside one keeps walking forward.
 */
class ChunkRetirementRegistry {
public:
    struct Counters {
        std::atomic<uint64_t> unlink_calls{0};       // unlinks that removed >= 1 chunk
        std::atomic<uint64_t> chunks_unlinked{0};
        std::atomic<uint64_t> chunks_freed{0};
        std::atomic<uint64_t> dirs_retired{0};
        std::atomic<uint64_t> dirs_freed{0};
        std::atomic<uint64_t> pending_entries{0};     // chunks + dirs queued now
        std::atomic<uint64_t> pending_bytes{0};       // their bytes now
        std::atomic<uint64_t> peak_pending_bytes{0};
        std::atomic<uint64_t> peak_pending_entries{0};
        std::atomic<uint64_t> reclaim_passes{0};
        // A pass stopped at an entry whose tag is not below the floor.
        std::atomic<uint64_t> blocked_by_floor{0};
        std::atomic<uint64_t> drained_at_shutdown{0};
        std::atomic<uint64_t> reader_clamps{0};       // for_each_delta raised its start
        std::atomic<uint64_t> fallback_walks{0};      // directory miss -> walk from head_
        std::atomic<uint64_t> cursor_relocations{0};  // retirement cursor relocated from head_
    };

    // Never destroyed, like RecordGraphRegistry.
    static ChunkRetirementRegistry& instance() {
        static ChunkRetirementRegistry* registry = new ChunkRetirementRegistry();
        return *registry;
    }
    static Counters& counters() { return instance().counters_; }

    /**
     * @brief Queues chunks that the caller has just made unreachable, under the record latch.
     * @param first First of the consecutive chunks, linked through next_.
     * @param count Number of chunks.
     * @param tag Global epoch read after the unlinking stores.
     */
    void retire_chunks(GraphDeltaChunk* first, uint64_t count, uint64_t tag) {
        if (count == 0) return;
        GraphDeltaChunk* chunk = first;
        {
            std::lock_guard<std::mutex> guard(mutex_);
            for (uint64_t i = 0; i < count; ++i) {
                GraphDeltaChunk* next = chunk->next_.load(std::memory_order_relaxed);
                queue_.push_back({chunk, nullptr, tag});
                chunk = next;
            }
        }
        counters_.unlink_calls.fetch_add(1, std::memory_order_relaxed);
        counters_.chunks_unlinked.fetch_add(count, std::memory_order_relaxed);
        note_pending(count, count * sizeof(GraphDeltaChunk));
    }

    void retire_directory(GraphChunkDirectory* dir, uint64_t tag) {
        if (dir == nullptr) return;
        const uint64_t bytes = dir->bytes();
        {
            std::lock_guard<std::mutex> guard(mutex_);
            queue_.push_back({nullptr, dir, tag});
        }
        counters_.dirs_retired.fetch_add(1, std::memory_order_relaxed);
        note_pending(1, bytes);
    }

    // Frees up to `budget` entries whose tag is below live_floor (0 frees none).
    size_t reclaim(uint64_t live_floor, size_t budget) {
        std::vector<Retired> to_free;
        {
            std::lock_guard<std::mutex> guard(mutex_);
            if (queue_.empty()) return 0;
            counters_.reclaim_passes.fetch_add(1, std::memory_order_relaxed);
            while (!queue_.empty() && to_free.size() < budget) {
                const Retired& entry = queue_.front();
                if (live_floor == 0 || live_floor <= entry.tag_) {
                    counters_.blocked_by_floor.fetch_add(1, std::memory_order_relaxed);
                    break;
                }
                to_free.push_back(entry);
                queue_.pop_front();
            }
        }
        free_entries(to_free);
        return to_free.size();
    }

    // Frees everything; only when no reader can hold a pointer (shutdown, tests).
    size_t drain_all() {
        std::vector<Retired> to_free;
        {
            std::lock_guard<std::mutex> guard(mutex_);
            to_free.assign(queue_.begin(), queue_.end());
            queue_.clear();
        }
        counters_.drained_at_shutdown.fetch_add(to_free.size(),
                                                std::memory_order_relaxed);
        free_entries(to_free);
        return to_free.size();
    }

    size_t pending() const {
        std::lock_guard<std::mutex> guard(mutex_);
        return queue_.size();
    }

private:
    struct Retired {
        GraphDeltaChunk* chunk_;
        GraphChunkDirectory* dir_;
        uint64_t tag_;
    };

    void note_pending(uint64_t entries, uint64_t bytes) {
        const uint64_t e =
            counters_.pending_entries.fetch_add(entries, std::memory_order_relaxed) + entries;
        const uint64_t b =
            counters_.pending_bytes.fetch_add(bytes, std::memory_order_relaxed) + bytes;
        uint64_t peak = counters_.peak_pending_bytes.load(std::memory_order_relaxed);
        while (b > peak && !counters_.peak_pending_bytes.compare_exchange_weak(
                               peak, b, std::memory_order_relaxed)) {}
        peak = counters_.peak_pending_entries.load(std::memory_order_relaxed);
        while (e > peak && !counters_.peak_pending_entries.compare_exchange_weak(
                               peak, e, std::memory_order_relaxed)) {}
    }

    void free_entries(const std::vector<Retired>& to_free) {
        uint64_t chunks = 0, dirs = 0, bytes = 0;
        for (const Retired& entry : to_free) {
            if (entry.chunk_ != nullptr) {
                delete entry.chunk_;
                ++chunks;
                bytes += sizeof(GraphDeltaChunk);
            } else {
                bytes += entry.dir_->bytes();
                delete entry.dir_;
                ++dirs;
            }
        }
        if (to_free.empty()) return;
        counters_.chunks_freed.fetch_add(chunks, std::memory_order_relaxed);
        counters_.dirs_freed.fetch_add(dirs, std::memory_order_relaxed);
        counters_.pending_entries.fetch_sub(chunks + dirs, std::memory_order_relaxed);
        counters_.pending_bytes.fetch_sub(bytes, std::memory_order_relaxed);
    }

    mutable std::mutex mutex_;
    std::deque<Retired> queue_;
    Counters counters_;
};

/**
 * @brief A record's history: an append-only list of entries in chunks, owned by RecordGraphRegistry.
 * 
 * Sequence numbers are dense and 1-based. Writers hold the record latch and publish
 * committed_seq_ with a release store; readers take no lock, load committed_seq_ once
 * and never read past it, under a registry pin.
 */
struct RecordGraphState {
    RecordId id_;
    // The live row, or nullptr once the row has been deleted or reinitialized.
    std::atomic<Tuple*> owner_{nullptr};
    // Maximum epoch of any entry and of any reference to this history; the registry frees
    // a detached history only once the floor is above it.
    std::atomic<uint64_t> max_cite_epoch_{0};
    // Maximum retirement bound over the entries: what a reference to this history inherits.
    std::atomic<uint64_t> max_prefix_epoch_{0};
    // Number of prefix references to this history (statistics only).
    std::atomic<uint64_t> cite_count_{0};
    // Guarded by the registry mutex.
    bool detached_ = false;
    // log2 of the current directory's capacity (0 = none), for byte counts.
    std::atomic<uint8_t> dir_log2_cap_{0};
    // The chunk number head_ names; advanced under the latch, after head_.
    std::atomic<uint32_t> first_live_chunk_{0};
    uint64_t detach_epoch_ = 0;

    std::atomic<uint64_t> committed_seq_{0};
    // tail_ is used only under the latch.
    std::atomic<GraphDeltaChunk*> head_{nullptr};
    GraphDeltaChunk* tail_ = nullptr;
    std::atomic<GraphChunkDirectory*> chunk_dir_{nullptr};
    std::atomic<uint64_t> include_depth_hint_{0};
    // The retirement watermark: entries up to it are skipped by readers. Grows under the latch.
    std::atomic<uint64_t> retired_seq_{0};

    // Where the next retirement pass resumes; used only under the latch.
    uint64_t retire_cursor_seq_ = 0;
    GraphDeltaChunk* retire_cursor_chunk_ = nullptr;

    // Unused; keeps sizeof(RecordGraphState) at the measured 160 bytes.
    uint64_t reserved_[5] = {};

    RecordGraphState() = default;

    // Frees the chunks from head_ and the chained directories; unlinked ones belong to
    // ChunkRetirementRegistry.
    ~RecordGraphState() {
        GraphDeltaChunk* chunk = head_.load(std::memory_order_relaxed);
        while (chunk != nullptr) {
            GraphDeltaChunk* next = chunk->next_.load(std::memory_order_relaxed);
            delete chunk;
            chunk = next;
        }
        GraphChunkDirectory* dir = chunk_dir_.load(std::memory_order_relaxed);
        while (dir != nullptr) {
            GraphChunkDirectory* prev = dir->prev_;
            delete dir;
            dir = prev;
        }
    }

    RecordGraphState(const RecordGraphState&) = delete;
    RecordGraphState& operator=(const RecordGraphState&) = delete;

    // Atomic maximum: appends and references hold different latches.
    void observe_cite_epoch(uint64_t epoch) {
        uint64_t current = max_cite_epoch_.load(std::memory_order_relaxed);
        while (current < epoch) {
            if (max_cite_epoch_.compare_exchange_weak(
                    current, epoch, std::memory_order_release,
                    std::memory_order_relaxed)) {
                return;
            }
        }
    }

    // Called under this record's latch by append_delta().
    void observe_prefix_epoch(uint64_t epoch) {
        uint64_t current = max_prefix_epoch_.load(std::memory_order_relaxed);
        while (current < epoch) {
            if (max_prefix_epoch_.compare_exchange_weak(
                    current, epoch, std::memory_order_release,
                    std::memory_order_relaxed)) {
                return;
            }
        }
    }

    uint64_t prefix_epoch() const {
        return max_prefix_epoch_.load(std::memory_order_acquire);
    }

    // Chunks from head_ to the tail, computed without dereferencing a chunk.
    uint64_t live_chunks() const {
        const uint64_t committed = committed_seq_.load(std::memory_order_acquire);
        const uint64_t total =
            (committed + kGraphDeltaChunkSlots - 1) / kGraphDeltaChunkSlots;
        const uint64_t first =
            first_live_chunk_.load(std::memory_order_acquire);
        return total > first ? total - first : 0;
    }

    // Directory bytes this state still owns (see graph_chunk_directory_bytes()).
    uint64_t directory_bytes(uint64_t* dirs = nullptr) const {
        return graph_chunk_directory_bytes(
            dir_log2_cap_.load(std::memory_order_acquire),
            !FLAGS_free_retired_history_chunks, dirs);
    }

    // Bytes this history owns, computed from counters only.
    uint64_t retained_bytes() const {
        uint64_t bytes = sizeof(RecordGraphState);
        bytes += live_chunks() * sizeof(GraphDeltaChunk);
        bytes += directory_bytes();
        return bytes;
    }

    /**
     * @brief Appends one entry and publishes it to lock-free readers.
     * @param delta The entry; its sequence number is assigned here.
     * @return The entry's sequence number.
     */
    uint64_t append(GraphDelta&& delta) {
        const uint64_t n = committed_seq_.load(std::memory_order_relaxed);
        if (n % kGraphDeltaChunkSlots == 0) {
            GraphDeltaChunk* new_chunk = new GraphDeltaChunk();
            new_chunk->first_seq_ = n + 1;   // before any publication below
            if (n == 0) {
                head_.store(new_chunk, std::memory_order_release);
            } else {
                tail_->next_.store(new_chunk, std::memory_order_release);
            }
            tail_ = new_chunk;
            publish_chunk(n / kGraphDeltaChunkSlots, new_chunk);
        }
        const uint64_t seq = n + 1;
        delta.seq_ = seq;
        tail_->slots_[n % kGraphDeltaChunkSlots] = std::move(delta);
        committed_seq_.store(seq, std::memory_order_release);
        return seq;
    }

    // Enters a chunk in the directory, growing it if needed, before committed_seq_ is published.
    void publish_chunk(uint64_t chunk_index, GraphDeltaChunk* chunk) {
        if (FLAGS_free_retired_history_chunks) {
            publish_chunk_rebased(chunk_index, chunk);
            return;
        }
        GraphChunkDirectory* dir = chunk_dir_.load(std::memory_order_relaxed);
        if (dir == nullptr || chunk_index >= dir->capacity_) {
            uint64_t new_capacity =
                dir == nullptr ? kGraphChunkDirectoryInitialCapacity
                               : uint64_t{dir->capacity_} * 2;
            while (chunk_index >= new_capacity) new_capacity *= 2;
            auto* new_dir = new GraphChunkDirectory(new_capacity, dir);
            const uint64_t old_capacity = dir == nullptr ? 0 : dir->capacity_;
            for (uint64_t i = 0; i < old_capacity; ++i) {
                new_dir->entries_[i].store(
                    dir->entries_[i].load(std::memory_order_relaxed),
                    std::memory_order_relaxed);
            }
            for (uint64_t i = old_capacity; i < new_capacity; ++i) {
                new_dir->entries_[i].store(nullptr, std::memory_order_relaxed);
            }
            // The superseded directory stays alive (prev_) for concurrent readers.
            chunk_dir_.store(new_dir, std::memory_order_release);
            dir_log2_cap_.store(log2_of(new_capacity), std::memory_order_relaxed);
            dir = new_dir;
        }
        dir->entries_[chunk_index].store(chunk, std::memory_order_release);
    }

    static uint8_t log2_of(uint64_t power_of_two) {
        return static_cast<uint8_t>(__builtin_ctzll(power_of_two));
    }

    /**
     * @brief Enters a chunk in the directory when retired chunks may have been freed.
     * 
     * A new directory starts at the first linked chunk and holds at least twice the linked
     * chunks; the superseded one is retired after the new one is published.
     * @param chunk_index The chunk's number.
     * @param chunk The chunk.
     */
    void publish_chunk_rebased(uint64_t chunk_index, GraphDeltaChunk* chunk) {
        GraphChunkDirectory* dir = chunk_dir_.load(std::memory_order_relaxed);
        if (dir == nullptr || chunk_index < dir->base_ ||
            chunk_index - dir->base_ >= dir->capacity_) {
            const uint64_t new_base =
                first_live_chunk_.load(std::memory_order_relaxed);
            const uint64_t live = chunk_index - new_base + 1;
            uint64_t new_capacity = kGraphChunkDirectoryInitialCapacity;
            while (new_capacity < 2 * live) new_capacity *= 2;
            auto* new_dir = new GraphChunkDirectory(new_capacity, nullptr, new_base);
            for (uint64_t i = 0; i < new_capacity; ++i) {
                const uint64_t index = new_base + i;
                GraphDeltaChunk* entry = nullptr;
                if (dir != nullptr && index < chunk_index && index >= dir->base_ &&
                    index - dir->base_ < dir->capacity_) {
                    entry = dir->entries_[index - dir->base_].load(
                        std::memory_order_relaxed);
                }
                new_dir->entries_[i].store(entry, std::memory_order_relaxed);
            }
            chunk_dir_.store(new_dir, std::memory_order_release);
            dir_log2_cap_.store(log2_of(new_capacity), std::memory_order_relaxed);
            if (dir != nullptr) {
                ChunkRetirementRegistry::instance().retire_directory(
                    dir, history_chunk_retire_tag());
            }
            dir = new_dir;
        }
        dir->entries_[chunk_index - dir->base_].store(chunk,
                                                      std::memory_order_release);
    }

    // nullptr below base_ or past the directory's end (the reader then walks).
    GraphDeltaChunk* chunk_for_index_rebased(uint64_t index) const {
        const uint64_t chunk_index = index / kGraphDeltaChunkSlots;
        GraphChunkDirectory* dir = chunk_dir_.load(std::memory_order_acquire);
        if (dir == nullptr || chunk_index < dir->base_ ||
            chunk_index - dir->base_ >= dir->capacity_) {
            return nullptr;
        }
        return dir->entries_[chunk_index - dir->base_].load(std::memory_order_acquire);
    }

    /**
     * @brief Unlinks every chunk wholly at or below the watermark, except the tail, for freeing.
     * 
     * Stores head_, then first_live_chunk_ (both release), then reads the tag. Unlinked
     * chunks are not modified, so a reader inside one keeps walking forward.
     * @param retired The retirement watermark; the caller holds the record latch.
     * @return The number of chunks unlinked.
     */
    uint64_t unlink_retired_prefix(uint64_t retired) {
        const uint64_t committed = committed_seq_.load(std::memory_order_relaxed);
        if (committed == 0) return 0;
        const uint64_t first = first_live_chunk_.load(std::memory_order_relaxed);
        const uint64_t tail_index = (committed - 1) / kGraphDeltaChunkSlots;
        const uint64_t dead =
            std::min(retired, committed) / kGraphDeltaChunkSlots;
        const uint64_t new_first = std::min(dead, tail_index);
        if (new_first <= first) return 0;
        GraphDeltaChunk* const old_head = head_.load(std::memory_order_relaxed);
        GraphDeltaChunk* new_head = old_head;
        for (uint64_t i = first; i < new_first; ++i) {
            new_head = new_head->next_.load(std::memory_order_relaxed);
        }
        head_.store(new_head, std::memory_order_release);
        first_live_chunk_.store(static_cast<uint32_t>(new_first),
                                std::memory_order_release);
        ChunkRetirementRegistry::instance().retire_chunks(
            old_head, new_first - first, history_chunk_retire_tag());
        return new_first - first;
    }

    // The chunk holding `seq`, by a walk from head_; nullptr past the last chunk.
    GraphDeltaChunk* chunk_containing(uint64_t seq) const {
        GraphDeltaChunk* chunk = head_.load(std::memory_order_acquire);
        while (chunk != nullptr &&
               seq >= chunk->first_seq_ + kGraphDeltaChunkSlots) {
            chunk = chunk->next_.load(std::memory_order_acquire);
        }
        return chunk;
    }

    // The chunk holding 0-based entry `index`, or nullptr if this reader saw an older directory.
    GraphDeltaChunk* chunk_for_index(uint64_t index) const {
        const uint64_t chunk_index = index / kGraphDeltaChunkSlots;
        GraphChunkDirectory* dir = chunk_dir_.load(std::memory_order_acquire);
        if (dir == nullptr || chunk_index >= dir->capacity_) return nullptr;
        return dir->entries_[chunk_index].load(std::memory_order_acquire);
    }

    // Number of deltas currently published. Safe with no lock.
    uint64_t committed() const {
        return committed_seq_.load(std::memory_order_acquire);
    }

    bool empty() const { return committed() == 0; }
    uint64_t size() const { return committed(); }

    /**
     * @brief Visits a range of entries in order, without a lock.
     * @param start_seq_exclusive Entries after this sequence.
     * @param upto_seq Entries up to this sequence (at most the committed ones).
     * @param fn Called per entry; returning false stops the walk.
     */
    template <typename Fn>
    void for_each_delta(uint64_t start_seq_exclusive, uint64_t upto_seq, Fn&& fn) const {
        if (FLAGS_free_retired_history_chunks) {
            for_each_delta_freeing(start_seq_exclusive, upto_seq, fn, false);
            return;
        }
        const uint64_t n = committed_seq_.load(std::memory_order_acquire);
        const uint64_t target = std::min(upto_seq, n);
        if (start_seq_exclusive >= target) return;

        const uint64_t first_index = start_seq_exclusive;  // 0-based
        GraphDeltaChunk* chunk = chunk_for_index(first_index);
        if (chunk == nullptr) {
            chunk = head_.load(std::memory_order_acquire);
            for (uint64_t skipped = 0;
                 skipped < first_index / kGraphDeltaChunkSlots; ++skipped) {
                chunk = chunk->next_.load(std::memory_order_acquire);
            }
        }

        uint64_t slot = first_index % kGraphDeltaChunkSlots;
        const uint64_t remaining = target - start_seq_exclusive;
        for (uint64_t k = 0; k < remaining; ++k) {
            if (!fn(chunk->slots_[slot])) return;
            ++slot;
            if (slot == kGraphDeltaChunkSlots) {
                slot = 0;
                chunk = chunk->next_.load(std::memory_order_acquire);
            }
        }
    }

    /**
     * @brief for_each_delta() when retired chunks may have been unlinked and freed.
     * 
     * first_live_chunk_ is loaded before the directory or head_, and the start is raised to
     * its first sequence. The skipped entries are all retired; a reader that loaded an older
     * value is covered by the unlink's epoch grace until its transaction ends.
     * @param start_seq_exclusive Entries after this sequence.
     * @param upto_seq Entries up to this sequence.
     * @param fn Called per entry; returning false stops the walk.
     * @param force_fallback Walk from head_ instead of the directory (tests).
     */
    template <typename Fn>
    void for_each_delta_freeing(uint64_t start_seq_exclusive, uint64_t upto_seq,
                                Fn&& fn, bool force_fallback) const {
        const uint64_t n = committed_seq_.load(std::memory_order_acquire);
        const uint64_t target = std::min(upto_seq, n);
        uint64_t start = start_seq_exclusive;
        const uint64_t floor_seq =
            uint64_t{first_live_chunk_.load(std::memory_order_acquire)} *
            kGraphDeltaChunkSlots;
        if (start < floor_seq) {
            if (floor_seq < target) {
                ChunkRetirementRegistry::counters().reader_clamps.fetch_add(
                    1, std::memory_order_relaxed);
            }
            start = floor_seq;
        }
        if (start >= target) return;

        GraphDeltaChunk* chunk =
            force_fallback ? nullptr : chunk_for_index_rebased(start);
        if (chunk == nullptr) {
            if (!force_fallback) {
                ChunkRetirementRegistry::counters().fallback_walks.fetch_add(
                    1, std::memory_order_relaxed);
            }
            chunk = head_.load(std::memory_order_acquire);
            if (start + 1 < chunk->first_seq_) {
                start = chunk->first_seq_ - 1;
                if (start >= target) return;
            }
            while (start + 1 >= chunk->first_seq_ + kGraphDeltaChunkSlots) {
                chunk = chunk->next_.load(std::memory_order_acquire);
            }
        }

        uint64_t slot = start % kGraphDeltaChunkSlots;
        const uint64_t remaining = target - start;
        for (uint64_t k = 0; k < remaining; ++k) {
            if (!fn(chunk->slots_[slot])) return;
            ++slot;
            if (slot == kGraphDeltaChunkSlots) {
                slot = 0;
                chunk = chunk->next_.load(std::memory_order_acquire);
            }
        }
    }

    template <typename Fn>
    void for_each(Fn&& fn) const {
        for_each_delta(0, std::numeric_limits<uint64_t>::max(), fn);
    }
};

using RecordGraphLog = RecordGraphState;

// 160 bytes is a mimalloc size class; one more word would move every history into
// the 192-byte class.
static_assert(sizeof(RecordGraphState) == 160,
              "RecordGraphState must stay 160 bytes (allocator size class)");

inline RecordGraphState* RecordGraphRegistry::create(Tuple* owner) {
    RecordGraphState* state = new RecordGraphState();
    uint32_t index = 0;
    uint32_t generation = 0;
    {
        std::lock_guard<std::mutex> guard(mutex_);
        if (!free_slots_.empty()) {
            index = free_slots_.back();
            free_slots_.pop_back();
            counters_.slots_reused.fetch_add(1, std::memory_order_relaxed);
        } else {
            index = next_slot_++;
            const size_t needed =
                (static_cast<size_t>(index) + kSlotChunkSlots - 1) / kSlotChunkSlots;
            if (needed > kMaxSlotChunks) {
                std::cerr << "FATAL: RecordGraphRegistry slot ceiling reached ("
                          << kMaxRecordSlots
                          << " records). Raise kMaxSlotChunks or reuse slots."
                          << std::endl;
                std::abort();
            }
            size_t have = chunk_count_.load(std::memory_order_relaxed);
            while (have < needed) {
                chunks_[have].store(new SlotChunk(), std::memory_order_release);
                ++have;
                chunk_count_.store(have, std::memory_order_release);
            }
            counters_.slots_allocated.fetch_add(1, std::memory_order_relaxed);
        }
        Slot* slot = slot_for(index);
        // No reader can hold an id of this slot's new generation yet.
        slot->state_ = state;
        generation = gen_of(slot->gen_pins_.load(std::memory_order_relaxed)) + 1;
        state->id_ = RecordId(index, generation);
        state->owner_.store(owner, std::memory_order_release);
        // Publishing the generation makes the id resolvable (pairs with try_pin()).
        slot->gen_pins_.store(static_cast<uint64_t>(generation) << 32,
                              std::memory_order_release);
    }
    counters_.states_created.fetch_add(1, std::memory_order_relaxed);
    counters_.states_live.fetch_add(1, std::memory_order_relaxed);
    return state;
}

/**
 * @brief The only way from an identity to a history; a pinned history is not freed.
 * @param id The history's identity.
 * @return The pinned state, or nullptr when the identity is stale.
 */
inline RecordGraphState* RecordGraphRegistry::try_pin(RecordId id) {
    if (!id.valid()) return nullptr;
    Slot* slot = slot_for(id.slot_);
    if (slot == nullptr) {
        counters_.pins_failed.fetch_add(1, std::memory_order_relaxed);
        return nullptr;
    }
    uint64_t current = slot->gen_pins_.load(std::memory_order_acquire);
    for (;;) {
        if (gen_of(current) != id.generation_) {
            counters_.pins_failed.fetch_add(1, std::memory_order_relaxed);
            counters_.generation_mismatch.fetch_add(1, std::memory_order_relaxed);
            return nullptr;
        }
        if (slot->gen_pins_.compare_exchange_weak(current, current + 1,
                                                  std::memory_order_acq_rel,
                                                  std::memory_order_acquire)) {
            break;
        }
    }
    return slot->state_;
}

inline void RecordGraphRegistry::unpin(RecordId id) {
    if (!id.valid()) return;
    Slot* slot = slot_for(id.slot_);
    if (slot == nullptr) return;
    slot->gen_pins_.fetch_sub(1, std::memory_order_release);
}

inline Tuple* RecordGraphRegistry::live_owner(RecordId id) {
    RecordGraphState* state = try_pin(id);
    if (state == nullptr) return nullptr;
    Tuple* owner = state->owner_.load(std::memory_order_acquire);
    unpin(id);
    return owner;
}

/**
 * @brief Marks a history as ownerless: it stays resolvable for prefix references until reclaim().
 * @param state The history.
 * @param epoch Epoch of the detach.
 */
inline void RecordGraphRegistry::detach(RecordGraphState* state,
                                        uint64_t epoch) {
    if (state == nullptr) return;
    state->owner_.store(nullptr, std::memory_order_release);
    {
        std::lock_guard<std::mutex> guard(mutex_);
        if (state->detached_) return;
        state->detached_ = true;
        state->detach_epoch_ = epoch;
        retire_list_.push_back({state, epoch});
    }
    counters_.detach_calls.fetch_add(1, std::memory_order_relaxed);
    counters_.states_detached.fetch_add(1, std::memory_order_relaxed);
}

/**
 * @brief Frees detached histories once no reference can need them.
 * 
 * Once every citation is retirable by every live and future transaction, a generation
 * bump (only with no pin held) makes the identity stale; the state is freed one epoch later.
 * @param now_epoch The current global epoch.
 * @param min_future_floor Bounds the floor of transactions not yet begun.
 * @param budget Maximum detached states examined.
 * @return The number of states freed.
 */
inline size_t RecordGraphRegistry::reclaim(uint64_t now_epoch,
                                          uint64_t min_future_floor,
                                          size_t budget) {
    // Bounds the floor of every live and future transaction; 0 frees nothing.
    const uint64_t live_floor =
        std::min(ReclamationFloorAnnouncer::min_live(), min_future_floor);
    std::vector<RecordGraphState*> to_free;
    {
        std::lock_guard<std::mutex> guard(mutex_);

        counters_.reclaim_passes.fetch_add(1, std::memory_order_relaxed);
        counters_.reclaim_entries_traversed.fetch_add(
            retire_list_.size() + free_after_grace_.size(),
            std::memory_order_relaxed);
        counters_.retire_list_len.store(retire_list_.size(),
                                        std::memory_order_relaxed);
        counters_.free_after_grace_len.store(free_after_grace_.size(),
                                             std::memory_order_relaxed);

        // Free the states made unresolvable at an earlier epoch.
        size_t kept = 0;
        for (size_t i = 0; i < free_after_grace_.size(); ++i) {
            Retired entry = free_after_grace_[i];
            if (now_epoch > entry.epoch_) {
                to_free.push_back(entry.state_);
                free_slots_.push_back(entry.state_->id_.slot_);
            } else {
                free_after_grace_[kept++] = entry;
            }
        }
        free_after_grace_.resize(kept);

        // Make a detached state unresolvable once every reference to it is retirable.
        size_t scanned = 0;
        kept = 0;
        for (size_t i = 0; i < retire_list_.size(); ++i) {
            Retired entry = retire_list_[i];
            if (scanned >= budget) {
                retire_list_[kept++] = entry;
                continue;
            }
            ++scanned;
            counters_.reclaim_attempts.fetch_add(1, std::memory_order_relaxed);
            const uint64_t cite_epoch =
                entry.state_->max_cite_epoch_.load(std::memory_order_acquire);
            if (live_floor == 0 || cite_epoch >= live_floor) {
                counters_.reclaim_blocked_by_epoch.fetch_add(
                    1, std::memory_order_relaxed);
                retire_list_[kept++] = entry;
                continue;
            }
            Slot* slot = slot_for(entry.state_->id_.slot_);
            uint64_t expected =
                static_cast<uint64_t>(entry.state_->id_.generation_) << 32;
            if (!slot->gen_pins_.compare_exchange_strong(
                    expected, expected + (UINT64_C(1) << 32),
                    std::memory_order_acq_rel, std::memory_order_acquire)) {
                // Pinned: an import is reading this history now.
                counters_.reclaim_blocked_by_pins.fetch_add(
                    1, std::memory_order_relaxed);
                retire_list_[kept++] = entry;
                continue;
            }
            // From here the id is unresolvable and no new pin can be taken.
            slot->state_ = nullptr;
            counters_.reclaim_generation_bumped.fetch_add(
                1, std::memory_order_relaxed);
            free_after_grace_.push_back({entry.state_, now_epoch});
        }
        retire_list_.resize(kept);
    }

    for (RecordGraphState* state : to_free) {
        counters_.states_detached.fetch_sub(1, std::memory_order_relaxed);
        counters_.states_live.fetch_sub(1, std::memory_order_relaxed);
        counters_.states_freed.fetch_add(1, std::memory_order_relaxed);
        delete state;
    }
    return to_free.size();
}

inline void RecordGraphRegistry::sample_retention(RetentionSample& out) {
    out = RetentionSample{};
    const size_t chunk_total = chunk_count_.load(std::memory_order_acquire);
    for (size_t chunk = 0; chunk < chunk_total; ++chunk) {
        for (size_t i = 0; i < kSlotChunkSlots; ++i) {
            Slot& slot =
            chunks_[chunk].load(std::memory_order_acquire)->slots_[i];
            const uint64_t word = slot.gen_pins_.load(std::memory_order_acquire);
            const RecordId id(
                static_cast<uint32_t>(chunk * kSlotChunkSlots + i) + 1,
                gen_of(word));
            RecordGraphState* state = try_pin(id);
            if (state == nullptr) continue;
            ++out.states_scanned;
            out.struct_bytes += sizeof(RecordGraphState);

            const uint64_t committed =
                state->committed_seq_.load(std::memory_order_acquire);
            const uint64_t retired = std::min(
                state->retired_seq_.load(std::memory_order_acquire), committed);
            out.deltas_committed += committed;
            out.deltas_retired += retired;
            if (committed > 0) ++out.states_with_log;
            // Chunks still owned; first_live_chunk_ may be newer than committed_seq_.
            const uint64_t total_chunks =
                (committed + kGraphDeltaChunkSlots - 1) / kGraphDeltaChunkSlots;
            const uint64_t first_live = std::min<uint64_t>(
                state->first_live_chunk_.load(std::memory_order_acquire),
                total_chunks);
            const uint64_t chunks = total_chunks - first_live;
            out.chunk_bytes += chunks * sizeof(GraphDeltaChunk);
            out.chunks_unlinked += first_live;
            // A linked chunk is dead once its last slot is at or below retired_seq_.
            const uint64_t dead_chunks = retired / kGraphDeltaChunkSlots;
            out.chunk_bytes_retired_prefix +=
                (dead_chunks > first_live ? dead_chunks - first_live : 0) *
                sizeof(GraphDeltaChunk);
            out.dir_bytes += state->directory_bytes();
            unpin(id);
        }
    }
}

/**
 * @brief A registry pin held for a scope: the history is not freed while it is held.
 */
class PinnedRecord {
public:
    PinnedRecord() = default;
    explicit PinnedRecord(RecordId id)
        : id_(id), state_(RecordGraphRegistry::instance().try_pin(id)) {
        if (state_ == nullptr) id_ = RecordId();
    }
    ~PinnedRecord() { reset(); }

    PinnedRecord(const PinnedRecord&) = delete;
    PinnedRecord& operator=(const PinnedRecord&) = delete;
    PinnedRecord(PinnedRecord&& other) noexcept
        : id_(other.id_), state_(other.state_) {
        other.id_ = RecordId();
        other.state_ = nullptr;
    }
    PinnedRecord& operator=(PinnedRecord&& other) noexcept {
        if (this != &other) {
            reset();
            id_ = other.id_;
            state_ = other.state_;
            other.id_ = RecordId();
            other.state_ = nullptr;
        }
        return *this;
    }

    void reset() {
        if (state_ != nullptr) {
            RecordGraphRegistry::instance().unpin(id_);
            state_ = nullptr;
            id_ = RecordId();
        }
    }

    bool valid() const { return state_ != nullptr; }
    RecordGraphState* get() const { return state_; }
    RecordGraphState* operator->() const { return state_; }
    RecordGraphState& operator*() const { return *state_; }

private:
    RecordId id_;
    RecordGraphState* state_ = nullptr;
};
