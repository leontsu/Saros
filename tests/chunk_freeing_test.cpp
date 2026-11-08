// Tests of --free_retired_history_chunks: each makes freeing happen and frees
// by an explicit drain or by the production reclaim pass and floor.
#include <algorithm>
#include <atomic>
#include <chrono>
#include <cstdlib>
#include <map>
#include <memory>
#include <random>
#include <sstream>
#include <string>
#include <thread>
#include <vector>

#define GLOBAL_VALUE_DEFINE

#include "fixture_saros.hh"
#include "gtest/gtest.h"

namespace {

using Clock = std::chrono::steady_clock;

struct Snap {
    uint64_t unlinked, freed, dirs_retired, dirs_freed, clamps, fallbacks,
        relocations;
};

Snap snap() {
    const auto& c = ChunkRetirementRegistry::counters();
    auto ld = [](const std::atomic<uint64_t>& v) {
        return v.load(std::memory_order_relaxed);
    };
    return {ld(c.chunks_unlinked), ld(c.chunks_freed), ld(c.dirs_retired),
            ld(c.dirs_freed), ld(c.reader_clamps), ld(c.fallback_walks),
            ld(c.cursor_relocations)};
}

}  // namespace

class ChunkFreeingTest : public TestCase {
protected:
    void SetUp() override {
        std::call_once(logging_once_, [] {
            google::InitGoogleLogging("chunk_freeing_test");
            FLAGS_stderrthreshold = 0;
        });
        configure(true);
        TestCase::SetUp();
        // After TestCase::SetUp(), so that the environment does not override
        // the arm a test asks for.
        FLAGS_free_retired_history_chunks = true;
        FLAGS_retirement_threshold = 0;
        withdraw_all();
    }

    static void configure(bool free_chunks) {
        FLAGS_thread_num = NumVirtualThreads;
        FLAGS_saros = true;
        FLAGS_retirement_threshold = 0;
        FLAGS_import_skip_retirable = false;
        FLAGS_reclaim_interval = 1;
        FLAGS_free_retired_history_chunks_budget = 1u << 20;
        FLAGS_free_retired_history_chunks = free_chunks;
    }

    void reinit(bool free_chunks) {
        TestCase::TearDown();
        configure(free_chunks);
        TestCase::SetUp();
        FLAGS_free_retired_history_chunks = free_chunks;
        FLAGS_retirement_threshold = 0;
        withdraw_all();
    }

    // Announcements are process-global and survive fixture teardown; a stale
    // low floor from an earlier test would block every free in this one.
    static void withdraw_all() {
        for (size_t i = 0; i < ReclamationFloorAnnouncer::kSlots; ++i)
            ReclamationFloorAnnouncer::withdraw(i);
    }

    static void set_epoch(uint64_t e, int keep_thid = -1) {
        GlobalEpoch.obj_ = e;
        for (int i = 0; i < NumVirtualThreads; ++i)
            if (i != keep_thid) ThManagementTable[i]->atomicStoreEpoch(e);
    }

    // `n` PublishRead facts on `t` whose both endpoints carry `epoch`.
    void append_facts(Tuple* t, uint64_t n, uint32_t epoch, uint32_t tid_base) {
        t->lock_.w_lock();
        for (uint64_t i = 0; i < n; ++i) {
            TxID reader;
            reader.epoch = epoch;
            reader.thid = 14;
            reader.tid = tid_base + static_cast<uint32_t>(i) * 2 + 1;
            TxID writer = reader;
            writer.tid = reader.tid + 1;
            txmap[14]->append_publish_read(t, reader, writer);
        }
        t->lock_.w_unlock();
        txmap[14]->published_seq_.clear();
        txmap[14]->published_prefix_epoch_.clear();
    }

    void retire(TxExecutor* tx, Tuple* t) {
        t->lock_.w_lock();
        EXPECT_TRUE(tx->retire_record_graph_log_facts(t));
        t->lock_.w_unlock();
    }

    static std::vector<uint64_t> seqs(const RecordGraphState& log,
                                      uint64_t start, uint64_t upto,
                                      bool force_fallback = false) {
        std::vector<uint64_t> out;
        auto fn = [&](const GraphDelta& d) { out.push_back(d.seq_); return true; };
        if (force_fallback) log.for_each_delta_freeing(start, upto, fn, true);
        else log.for_each_delta(start, upto, fn);
        return out;
    }

    static uint64_t linked_chunks(const RecordGraphState& log) {
        uint64_t n = 0;
        for (GraphDeltaChunk* c = log.head_.load(); c != nullptr;
             c = c->next_.load())
            ++n;
        return n;
    }

    static std::string dump(const Graph& g) {
        std::ostringstream o;
        for (const auto& [id, node] : g) {
            o << id << (node.is_aborted ? "A" : "") << "{rb:";
            for (const auto& x : node.readBy_) o << x << ',';
            o << " wb:";
            for (const auto& x : node.writtenBy_) o << x << ',';
            o << " fr:";
            for (const auto& x : node.from_) o << x << ',';
            o << "}\n";
        }
        return o.str();
    }

    static inline std::once_flag logging_once_;
};

// A reference whose cutoff lies in a freed prefix imports what it imports without freeing.
TEST_F(ChunkFreeingTest, ReferenceIntoFreedPrefixImportsSameAsFlagOff) {
    std::map<bool, std::string> graphs;
    std::map<bool, std::vector<uint64_t>> delivered;
    for (bool on : {false, true}) {
        reinit(on);
        Tuple* r = table["r"];
        Tuple* c = table["c"];
        set_epoch(1);
        append_facts(r, 150, 1, 0);           // retirable
        set_epoch(5);
        txmap[2]->begin();                    // live writer at epoch 5
        append_facts(r, 150, 5, 100000);      // not retirable at floor 4
        txmap[3]->begin();                    // retirer, floor 4
        ASSERT_EQ(txmap[3]->reclamation_epoch_, 4U);
        const Snap before = snap();
        retire(txmap[3], r);
        ASSERT_EQ(r->graph_log().retired_seq_.load(), 150U);
        if (on) {
            EXPECT_EQ(r->graph_log().first_live_chunk_.load(), 2U);
            EXPECT_EQ(snap().unlinked - before.unlinked, 2U);
            EXPECT_EQ(ChunkRetirementRegistry::instance().drain_all(), 2U);
            EXPECT_EQ(snap().freed - before.freed, 2U);
        } else {
            EXPECT_EQ(r->graph_log().first_live_chunk_.load(), 0U);
            EXPECT_EQ(snap().unlinked, before.unlinked);
        }
        c->lock_.w_lock();
        txmap[2]->append_include(c, r, 100);
        txmap[2]->append_include(c, r, 200);
        c->lock_.w_unlock();
        txmap[4]->begin();
        Graph g;
        c->lock_.w_lock();
        ASSERT_TRUE(txmap[4]->import_record(c, c->graph_log().committed(), g));
        c->lock_.w_unlock();
        graphs[on] = dump(g);
        delivered[on] = seqs(r->graph_log(),
                             r->graph_log().retired_seq_.load(), 200);
        EXPECT_FALSE(g.empty());
    }
    EXPECT_EQ(graphs[false], graphs[true]);
    EXPECT_EQ(delivered[false], delivered[true]);
    ASSERT_EQ(delivered[true].size(), 50U);
    EXPECT_EQ(delivered[true].front(), 151U);
}

// A reader inside an unlinked chunk keeps reading it; reclaim frees it only after the reader's
// transaction has ended and the floor has passed the tag.
TEST_F(ChunkFreeingTest,
       ReaderMidIterationInUnlinkedChunkIsNotFreedUntilItsTransactionEnds) {
    Tuple* r = table["r"];
    set_epoch(1);
    append_facts(r, 200, 1, 0);               // chunks 0..3, tail 3
    set_epoch(3);
    TxExecutor* reader = txmap[2];
    reader->begin();                          // stamp 3, floor 2
    ASSERT_EQ(reader->reclamation_epoch_, 2U);
    TxExecutor* unlinker = txmap[3];
    const Snap before = snap();
    uint64_t expect = 0, freed_while_live = 0;
    bool unlinked = false;
    r->graph_log().for_each_delta(0, 200, [&](const GraphDelta& d) {
        ++expect;
        EXPECT_EQ(d.seq_, expect);
        if (d.seq_ == 10) {
            // Others move to epoch 4; the reader keeps its stamp 3.
            set_epoch(4, reader->thid_);
            unlinker->begin();                // floor min(3,4)-1 = 2 > 1
            retire(unlinker, r);
            unlinked = snap().unlinked > before.unlinked;
            unlinker->reclaim_retired_chunks();
            freed_while_live = snap().freed - before.freed;
        }
        return true;
    });
    EXPECT_EQ(expect, 200U);
    EXPECT_TRUE(unlinked);
    EXPECT_EQ(r->graph_log().first_live_chunk_.load(), 3U);
    EXPECT_EQ(freed_while_live, 0U) << "freed under a live reader";
    EXPECT_GT(ChunkRetirementRegistry::counters().blocked_by_floor.load(), 0U);
    // The reader's transaction ends; everyone moves on two epochs and
    // re-announces. The floor (6) now exceeds the tag (4).
    set_epoch(7);
    reader->begin();
    unlinker->begin();
    unlinker->reclaim_retired_chunks();
    EXPECT_EQ(snap().freed - before.freed, 3U);
    EXPECT_EQ(ChunkRetirementRegistry::instance().pending(), 0U);
    EXPECT_EQ(seqs(r->graph_log(), 0, 200).front(), 193U);
}

// A reader that loaded w_r before the advance and iterates after the
// unlink is clamped: it skips only entries at or below the (new) w_r.
TEST_F(ChunkFreeingTest,
       StaleWatermarkReaderClampSkipsOnlyRetiredEntries) {
    Tuple* r = table["r"];
    set_epoch(1);
    append_facts(r, 300, 1, 0);
    set_epoch(5);
    txmap[2]->begin();
    append_facts(r, 50, 5, 100000);           // 350 entries, tail chunk 5
    const uint64_t w0 = r->graph_log().retired_seq_.load();
    ASSERT_EQ(w0, 0U);
    txmap[3]->begin();
    const Snap before = snap();
    retire(txmap[3], r);
    const uint64_t w1 = r->graph_log().retired_seq_.load();
    ASSERT_EQ(w1, 300U);
    ASSERT_EQ(r->graph_log().first_live_chunk_.load(), 4U);   // 300/64 = 4
    const auto got = seqs(r->graph_log(), w0, 350);
    ASSERT_FALSE(got.empty());
    EXPECT_EQ(got.front(), 257U);
    EXPECT_EQ(got.back(), 350U);
    for (size_t i = 1; i < got.size(); ++i) EXPECT_EQ(got[i], got[i - 1] + 1);
    EXPECT_LE(got.front() - 1, w1) << "skipped an entry above w_r";
    EXPECT_EQ(snap().clamps - before.clamps, 1U);
    // A range wholly inside the freed prefix delivers nothing.
    EXPECT_TRUE(seqs(r->graph_log(), w0, 200).empty());
    ChunkRetirementRegistry::instance().drain_all();
    EXPECT_EQ(seqs(r->graph_log(), w0, 350), got);
}

// The head_ fallback walk and the retirement-cursor relocation after head_
// has moved locate chunks by first_seq_.
TEST_F(ChunkFreeingTest, FallbackWalkAndCursorRelocationAfterHeadMoved) {
    Tuple* r = table["r"];
    RecordGraphState& log = r->graph_log();
    set_epoch(1);
    append_facts(r, 128, 1, 0);               // exactly chunks 0 and 1
    set_epoch(5);
    txmap[3]->begin();
    const Snap s0 = snap();
    retire(txmap[3], r);
    ASSERT_EQ(log.retired_seq_.load(), 128U);
    // w_r on the tail's boundary: chunk 1 is wholly retired but is the tail.
    EXPECT_EQ(log.first_live_chunk_.load(), 1U);
    EXPECT_EQ(log.retire_cursor_chunk_, nullptr);
    EXPECT_EQ(log.retire_cursor_seq_, 129U);
    // Ten more old facts make chunk 1 a non-tail chunk. The next retirement
    // must relocate the cursor from the moved head_ (chunk 1) to chunk 2.
    append_facts(r, 10, 1, 1000);
    retire(txmap[3], r);
    EXPECT_EQ(snap().relocations - s0.relocations, 2U);   // first call, this call
    EXPECT_EQ(log.retired_seq_.load(), 138U);
    ASSERT_NE(log.retire_cursor_chunk_, nullptr);
    EXPECT_EQ(log.retire_cursor_chunk_->first_seq_, 129U);
    EXPECT_EQ(log.first_live_chunk_.load(), 2U);          // D4: chunk 1 unlinked
    EXPECT_EQ(log.head_.load()->first_seq_, 129U);
    // More chunks, some live, then compare the forced fallback walk with the
    // directory path over many ranges.
    txmap[2]->begin();
    append_facts(r, 500, 5, 100000);
    ChunkRetirementRegistry::instance().drain_all();
    const uint64_t n = log.committed();
    for (uint64_t start = 0; start <= n; start += 7) {
        for (uint64_t upto : {start + 1, start + 63, start + 64, start + 65, n}) {
            EXPECT_EQ(seqs(log, start, upto, true), seqs(log, start, upto))
                << "start=" << start << " upto=" << upto;
        }
    }
    const auto all = seqs(log, 0, n, true);
    ASSERT_FALSE(all.empty());
    EXPECT_EQ(all.front(), 129U);
    EXPECT_EQ(all.back(), n);
}

// retained_bytes() and the retention sampler count exactly the chunks
// and directory bytes the state still owns.
TEST_F(ChunkFreeingTest, RetainedBytesMatchesAllocatedChunks) {
    for (bool on : {false, true}) {
        reinit(on);
        Tuple* r = table["r"];
        RecordGraphState& log = r->graph_log();
        set_epoch(1);
        append_facts(r, 1500, 1, 0);          // 24 chunks: one directory growth
        set_epoch(5);
        txmap[2]->begin();
        append_facts(r, 40, 5, 100000);
        txmap[3]->begin();
        retire(txmap[3], r);
        uint64_t dir_bytes = 0, dirs = 0;
        for (GraphChunkDirectory* d = log.chunk_dir_.load(); d != nullptr;
             d = d->prev_) {
            dir_bytes += d->bytes();
            ++dirs;
        }
        const uint64_t chunks = linked_chunks(log);
        EXPECT_EQ(log.live_chunks(), chunks);
        uint64_t sampled_dirs = 0;
        EXPECT_EQ(log.directory_bytes(&sampled_dirs), dir_bytes);
        EXPECT_EQ(sampled_dirs, dirs);
        EXPECT_EQ(log.retained_bytes(),
                  sizeof(RecordGraphState) + chunks * sizeof(GraphDeltaChunk) +
                      dir_bytes);
        if (on) {
            EXPECT_EQ(chunks, 25U - 1500 / 64);   // 1540 entries = 25 chunks
            EXPECT_EQ(dirs, 1U);
        } else {
            EXPECT_EQ(chunks, 25U);
            EXPECT_EQ(dirs, 2U);
        }
        RecordGraphRegistry::RetentionSample rs;
        RecordGraphRegistry::instance().sample_retention(rs);
        EXPECT_EQ(ChunkRetirementRegistry::counters().pending_entries.load(),
                  ChunkRetirementRegistry::instance().pending());
        ChunkRetirementRegistry::instance().drain_all();
        EXPECT_EQ(ChunkRetirementRegistry::counters().pending_entries.load(), 0U);
        EXPECT_EQ(ChunkRetirementRegistry::counters().pending_bytes.load(), 0U);
    }
}

// Appenders, retirers, importers, lock-free readers and mainte() run concurrently while the
// epoch advances; readers check each slot's sequence.
TEST_F(ChunkFreeingTest, ConcurrentAppendRetireImportReclaimStress) {
    FLAGS_retirement_threshold = 16;
    const double seconds = std::getenv("OZE_TEST_CHUNK_STRESS_SECONDS")
        ? std::atof(std::getenv("OZE_TEST_CHUNK_STRESS_SECONDS")) : 3.0;
    constexpr int kAppenders = 3, kImporters = 3;
    std::vector<Tuple*> recs;
    for (const char* k : {"a", "b", "c", "d", "e", "f"}) recs.push_back(table[k]);
    // Only the worker slots take part in the epoch; the rest are quiescent.
    for (int i = kAppenders + kImporters; i < NumVirtualThreads; ++i)
        ThManagementTable[i]->atomicStoreParticipation(ParticipationState::Quiescent);
    set_epoch(1);
    const Snap before = snap();
    std::atomic<bool> stop{false};
    std::atomic<uint64_t> bad_slots{0}, imports{0}, reads{0};
    std::vector<std::thread> threads;
    for (int w = 0; w < kAppenders; ++w) {
        threads.emplace_back([&, w] {
            TxExecutor* tx = txmap[w];
            std::mt19937 rng(17 + w);
            while (!stop.load(std::memory_order_relaxed)) {
                tx->begin();
                Tuple* t = recs[rng() % recs.size()];
                Tuple* other = recs[rng() % recs.size()];
                const uint64_t cite = other->graph_log().committed();
                t->lock_.w_lock();
                for (int i = 0; i < 40; ++i) {
                    TxID from = tx->txid_;
                    from.tid = tx->txid_.tid + 1;
                    tx->append_publish_read(t, tx->txid_, from);
                }
                if (other != t && cite > 0 && rng() % 2 == 0)
                    tx->append_include(t, other, cite);
                tx->maybe_retire_record_history(t);
                t->lock_.w_unlock();
                tx->published_seq_.clear();
                tx->published_prefix_epoch_.clear();
                tx->last_published_tuple_ = nullptr;
                tx->transaction_open_ = false;
                tx->mainte();
            }
        });
    }
    for (int w = 0; w < kImporters; ++w) {
        threads.emplace_back([&, w] {
            TxExecutor* tx = txmap[kAppenders + w];
            std::mt19937 rng(91 + w);
            while (!stop.load(std::memory_order_relaxed)) {
                tx->begin();
                Tuple* t = recs[rng() % recs.size()];
                // A stale watermark, used after the import, so that unlinks
                // in between exercise the reader clamp.
                Tuple* u = recs[rng() % recs.size()];
                const RecordGraphState& log = u->graph_log();
                const uint64_t w_r = log.retired_seq_.load(std::memory_order_acquire);
                Graph g;
                t->lock_.w_lock();   // root latched as in validation
                tx->import_record(t, t->graph_log().committed(), g);
                t->lock_.w_unlock();
                imports.fetch_add(1, std::memory_order_relaxed);
                tx->last_imported_seq_.clear();
                // The lock-free reader, from the stale watermark.
                uint64_t prev = 0;
                log.for_each_delta(w_r, log.committed(), [&](const GraphDelta& d) {
                    // Never a zeroed/foreign slot, never out of order.
                    if (d.seq_ == 0) bad_slots.fetch_add(1);
                    if (prev != 0 && d.seq_ != prev + 1) bad_slots.fetch_add(1);
                    prev = d.seq_;
                    return true;
                });
                reads.fetch_add(1, std::memory_order_relaxed);
                tx->transaction_open_ = false;
                tx->mainte();
            }
        });
    }
    std::thread leader([&] {
        while (!stop.load(std::memory_order_relaxed)) {
            {
                std::lock_guard<std::mutex> guard(EpochMembershipWriterMutex);
                const uint64_t ge = atomicLoadGE();
                bool all = true;
                for (int i = 0; i < NumVirtualThreads; ++i) {
                    if (!ThManagementTable[i]->countsTowardEpochAdvance()) continue;
                    if (ThManagementTable[i]->atomicLoadEpoch() != ge) all = false;
                }
                if (all) atomicAddGE();
            }
            std::this_thread::sleep_for(std::chrono::microseconds(200));
        }
    });
    const auto deadline = Clock::now() +
        std::chrono::milliseconds(static_cast<int64_t>(seconds * 1000));
    while (Clock::now() < deadline)
        std::this_thread::sleep_for(std::chrono::milliseconds(20));
    stop.store(true);
    for (auto& t : threads) t.join();
    leader.join();
    const Snap after = snap();
    std::cout << "[ stress   ] seconds=" << seconds << " epochs=" << atomicLoadGE()
              << " imports=" << imports.load() << " reads=" << reads.load()
              << " unlinked=" << after.unlinked - before.unlinked
              << " freed_during_run=" << after.freed - before.freed
              << " dirs_retired=" << after.dirs_retired - before.dirs_retired
              << " dirs_freed=" << after.dirs_freed - before.dirs_freed
              << " clamps=" << after.clamps - before.clamps
              << " fallbacks=" << after.fallbacks - before.fallbacks
              << " pending=" << ChunkRetirementRegistry::instance().pending()
              << std::endl;
    EXPECT_EQ(bad_slots.load(), 0U);
    EXPECT_GT(after.unlinked - before.unlinked, 0U);
    EXPECT_GT(after.freed - before.freed, 0U) << "nothing was freed during the run";
    EXPECT_GT(imports.load(), 0U);
    for (Tuple* t : recs) {
        const RecordGraphState& log = t->graph_log();
        EXPECT_LE(log.first_live_chunk_.load() * kGraphDeltaChunkSlots,
                  log.retired_seq_.load());
        EXPECT_EQ(log.live_chunks(), linked_chunks(log));
    }
    for (int i = kAppenders + kImporters; i < NumVirtualThreads; ++i)
        ThManagementTable[i]->atomicStoreParticipation(ParticipationState::Active);
}

// A deleted record with unlinked chunks and a retired directory: ~RecordGraphState and the
// registry each free their part once, in either order.
TEST_F(ChunkFreeingTest,
       DeletedRecordDestructorAndRegistryNeverDoubleFreeOrLeak) {
    for (int order = 0; order < 2; ++order) {
        reinit(true);
        auto* row = new Tuple();
        const Snap before = snap();
        set_epoch(1);
        append_facts(row, 1500, 1, 0);        // 24 chunks, one directory growth
        set_epoch(5);
        txmap[2]->begin();
        append_facts(row, 40, 5, 100000);
        txmap[3]->begin();
        retire(txmap[3], row);
        const Snap mid = snap();
        ASSERT_EQ(mid.unlinked - before.unlinked, 1500U / 64);
        ASSERT_EQ(mid.dirs_retired - before.dirs_retired, 1U);
        const uint64_t states_freed_before =
            RecordGraphRegistry::counters().states_freed.load();
        auto delete_record = [&] {
            row->logically_delete();          // detaches the state
            delete row;
            row = nullptr;
            withdraw_all();
            RecordGraphRegistry::instance().drain_for_test(1000);
            RecordGraphRegistry::instance().drain_for_test(1001);
        };
        if (order == 0) {
            ChunkRetirementRegistry::instance().drain_all();
            delete_record();
        } else {
            delete_record();
            ChunkRetirementRegistry::instance().drain_all();
        }
        const Snap after = snap();
        EXPECT_GE(RecordGraphRegistry::counters().states_freed.load(),
                  states_freed_before + 1);
        EXPECT_EQ(after.freed - before.freed, after.unlinked - before.unlinked);
        EXPECT_EQ(after.dirs_freed - before.dirs_freed,
                  after.dirs_retired - before.dirs_retired);
        EXPECT_EQ(ChunkRetirementRegistry::instance().pending(), 0U);
    }
}
