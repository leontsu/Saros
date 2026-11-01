// Tests of retirement of prefix references, epoch membership transitions, the
// transaction-id counter and the record-state registry.

#include <algorithm>
#include <atomic>
#include <map>
#include <tuple>
#include <mutex>
#include <set>
#include <string>
#include <thread>
#include <vector>

#define GLOBAL_VALUE_DEFINE

#include "../saros/include/test.hh"

extern bool chkEpochLoaded();

namespace {

class RetirementTest : public TestCase {
protected:
    void SetUp() override {
        std::call_once(logging_once_, [] {
            google::InitGoogleLogging("retirement_test");
            FLAGS_stderrthreshold = 0;
        });
        configure(true);
        TestCase::SetUp();
    }

    void configure(bool incremental) {
        FLAGS_thread_num = NumVirtualThreads;
        FLAGS_saros = incremental;
        FLAGS_validate_insert_only_reads = true;
        FLAGS_retirement_threshold = 0;
    }

    void reinitialize(bool incremental) {
        TestCase::TearDown();
        configure(incremental);
        TestCase::SetUp();
    }

    size_t count_kind(const Tuple* tuple, GraphDeltaKind kind) {
        size_t n = 0;
        tuple->graph_log().for_each([&](const GraphDelta& d) {
            if (d.kind_ == kind) ++n;
            return true;
        });
        return n;
    }

    static std::once_flag logging_once_;
};

std::once_flag RetirementTest::logging_once_;

// Retirement of prefix references. A read fact or edge is stamped with the
// epochs of its endpoints; a prefix reference is stamped with the transitive
// retirement bound of the cited prefix, which may hold facts of live
// transactions that began after the referencing one.

TEST_F(RetirementTest, CitationRetirementDoesNotHideALiveFact) {
    auto run = [this]() -> std::tuple<uint64_t, uint64_t, bool> {
        reinitialize(true);

        // One slot lags at epoch 1 while the global epoch is 2 (the one-epoch
        // spread the epoch protocol permits).
        GlobalEpoch.obj_ = 2;
        for (int i = 0; i < NumVirtualThreads; ++i)
            ThManagementTable[i]->atomicStoreEpoch(2);
        ThManagementTable[9]->atomicStoreEpoch(1);

        // C is the CITER: a long transaction that begins while a slot lags, so
        // its txid_.epoch is 1 even though GlobalEpoch is already 2.
        begin("1");
        const TxID c = txmap[1]->txid_;
        EXPECT_EQ(c.epoch, 1U);

        // The lagging slot catches up and the epoch advances to 3: a new
        // transaction stamps its slot 3 but takes epoch 2.
        ThManagementTable[9]->atomicStoreEpoch(2);
        atomicAddGE();
        for (int i = 0; i < NumVirtualThreads; ++i) {
            if (i == 9) continue;
            ThManagementTable[i]->atomicStoreEpoch(3);
        }
        begin("2");
        const TxID a = txmap[2]->txid_;
        EXPECT_EQ(a.epoch, 2U);

        Tuple* cited = table["s"];
        Tuple* carrier = table["t"];

        // A, live throughout, publishes a fact on the cited record.
        cited->lock_.w_lock();
        const uint64_t a_seq = txmap[2]->append_edge(
            cited, a, TxID(), GraphEdgeKind::WrittenBy);
        cited->lock_.w_unlock();
        EXPECT_EQ(a_seq, 1U);

        // C, still live, references the cited prefix (including A's fact)
        // from the carrier record.
        cited->lock_.w_lock();
        txmap[1]->published_seq_[cited] = a_seq;
        txmap[1]->published_prefix_epoch_[cited] =
            cited->graph_log().prefix_epoch();
        cited->lock_.w_unlock();
        carrier->lock_.w_lock();
        txmap[1]->append_include(carrier, cited, a_seq);
        carrier->lock_.w_unlock();
        EXPECT_EQ(carrier->graph_log().committed(), 1U);

        // C finishes and the floor rises to 2; A (epoch 2) is still live, so
        // neither its fact nor the only reference to it may be retired.
        for (int i = 0; i < NumVirtualThreads; ++i)
            ThManagementTable[i]->atomicStoreEpoch(3);
        begin("3");
        EXPECT_EQ(txmap[3]->reclamation_epoch_, 2U);

        // X retires both logs.
        for (Tuple* t : {carrier, cited}) {
            t->lock_.w_lock();
            EXPECT_TRUE(txmap[3]->retire_record_graph_log_facts(t));
            t->lock_.w_unlock();
        }
        const uint64_t carrier_retired =
            carrier->graph_log().retired_seq_.load(std::memory_order_acquire);
        const uint64_t cited_retired =
            cited->graph_log().retired_seq_.load(std::memory_order_acquire);

        // Is A's fact still reachable from the carrier?
        Graph g;
        txmap[3]->graph_.clear();
        txmap[3]->last_imported_seq_.clear();
        EXPECT_TRUE(txmap[3]->import_record(
            carrier, carrier->graph_log().committed(), g));
        const bool a_reachable = g.find(a) != g.end();
        return std::make_tuple(carrier_retired, cited_retired, a_reachable);
    };

    const auto result = run();
    EXPECT_EQ(std::get<1>(result), 0U) << "A is live; its fact must survive";
    EXPECT_EQ(std::get<0>(result), 0U)
        << "the citation carries the cited prefix's epoch bound";
    EXPECT_TRUE(std::get<2>(result))
        << "the live transaction's fact stays reachable";
}

// A reference to a prefix that lies wholly below the floor is still retired.
TEST_F(RetirementTest, DeadCitationsAreStillRetired) {
    reinitialize(true);
    GlobalEpoch.obj_ = 10;
    for (int i = 0; i < NumVirtualThreads; ++i)
        ThManagementTable[i]->atomicStoreEpoch(10);

    begin("1");
    const TxID old_tx = txmap[1]->txid_;
    ASSERT_EQ(old_tx.epoch, 10U);

    Tuple* cited = table["s"];
    Tuple* carrier = table["t"];
    cited->lock_.w_lock();
    const uint64_t s = txmap[1]->append_edge(cited, old_tx, TxID(),
                                             GraphEdgeKind::WrittenBy);
    txmap[1]->published_seq_[cited] = s;
    txmap[1]->published_prefix_epoch_[cited] = cited->graph_log().prefix_epoch();
    cited->lock_.w_unlock();
    carrier->lock_.w_lock();
    txmap[1]->append_include(carrier, cited, s);
    carrier->lock_.w_unlock();

    // Everyone moves well past epoch 10.
    for (int k = 0; k < 4; ++k) atomicAddGE();
    for (int i = 0; i < NumVirtualThreads; ++i)
        ThManagementTable[i]->atomicStoreEpoch(14);
    begin("3");
    ASSERT_GT(txmap[3]->reclamation_epoch_, old_tx.epoch);

    for (Tuple* t : {carrier, cited}) {
        t->lock_.w_lock();
        ASSERT_TRUE(txmap[3]->retire_record_graph_log_facts(t));
        t->lock_.w_unlock();
    }
    EXPECT_EQ(carrier->graph_log().retired_seq_.load(std::memory_order_acquire), 1U)
        << "a citation of an entirely dead prefix must still be retirable";
    EXPECT_EQ(cited->graph_log().retired_seq_.load(std::memory_order_acquire), 1U);
}

// A worker that leaves and rejoins the epoch keeps the epoch floor and the
// epoch-advance check intact.
TEST_F(RetirementTest, QuiescenceTransitionKeepsEpochFloor) {
    reinitialize(true);
    GlobalEpoch.obj_ = 7;
    for (int i = 0; i < NumVirtualThreads; ++i) {
        ThManagementTable[i]->atomicStoreEpoch(7);
        ThManagementTable[i]->atomicStoreParticipation(
            ParticipationState::Active);
    }
    ASSERT_TRUE(txmap[4]->become_quiescent());
    txmap[4]->resume_active();
    EXPECT_EQ(ThManagementTable[4]->atomicLoadEpoch(), 7U);
    EXPECT_EQ(txmap[0]->get_min_epoch(), 7U);
    EXPECT_TRUE(chkEpochLoaded());
}

TEST_F(RetirementTest,
       EpochMembershipScansRetryAcrossConcurrentTransitions) {
    reinitialize(true);
    GlobalEpoch.obj_ = 9;
    for (int i = 0; i < NumVirtualThreads; ++i) {
        ThManagementTable[i]->atomicStoreEpoch(9);
        ThManagementTable[i]->atomicStoreParticipation(
            ParticipationState::Active);
    }
    std::atomic<bool> start{false};
    std::thread transitions([&] {
        while (!start.load(std::memory_order_acquire)) {}
        for (int i = 0; i < 2000; ++i) {
            ASSERT_TRUE(txmap[4]->become_quiescent());
            txmap[4]->resume_active();
        }
    });
    start.store(true, std::memory_order_release);
    for (int i = 0; i < 2000; ++i) {
        EXPECT_EQ(txmap[0]->get_min_epoch(), 9U);
        EXPECT_TRUE(chkEpochLoaded());
    }
    transitions.join();
}

TEST_F(RetirementTest, TxIdCounterOverflowFailsLoudly) {
    reinitialize(true);
    GlobalEpoch.obj_ = 9;
    for (int i = 0; i < NumVirtualThreads; ++i) {
        ThManagementTable[i]->atomicStoreEpoch(9);
        ThManagementTable[i]->atomicStoreParticipation(
            ParticipationState::Active);
    }
    txmap[4]->txid_.epoch = 9;
    txmap[4]->txid_.tid = TxID::kMaxTid;
    EXPECT_DEATH(txmap[4]->begin(), "TxID per-worker counter exhausted");
}

// The registry's slot-chunk index never moves, so lock-free lookups are safe
// while it grows; these tests grow it across several chunks with live pins.

TEST_F(RetirementTest, RegistryChunkIndexIsStableAcrossGrowth) {
    reinitialize(true);
    // Enough rows to cross several 4,096-slot chunk boundaries.  The fixture
    // has already created 26.
    constexpr size_t kRows = 4096 * 3 + 17;
    std::vector<std::unique_ptr<Tuple>> rows;
    std::vector<RecordId> ids;
    rows.reserve(kRows);
    ids.reserve(kRows);
    for (size_t i = 0; i < kRows; ++i) {
        rows.emplace_back(new Tuple());
        HeapObject obj;
        obj.allocate<Data>();
        std::string key = "growth-" + std::to_string(i);
        rows.back()->init(0, TupleBody(key, std::move(obj)), nullptr);
        ids.push_back(rows.back()->graph_id());
        ASSERT_TRUE(ids.back().valid());
    }
    // Every identity handed out before, during and after growth still resolves
    // to its own state.
    for (size_t i = 0; i < kRows; ++i) {
        RecordGraphState* st = RecordGraphRegistry::instance().try_pin(ids[i]);
        ASSERT_NE(st, nullptr) << "identity " << i << " failed to pin";
        EXPECT_EQ(st->id_.slot_, ids[i].slot_);
        EXPECT_EQ(st->id_.generation_, ids[i].generation_);
        RecordGraphRegistry::instance().unpin(ids[i]);
    }
    for (auto& r : rows) release_version_chain(r.get());
}

TEST_F(RetirementTest,
       RegistryLookupIsRaceFreeDuringChunkPublication) {
    reinitialize(true);
    std::vector<std::unique_ptr<Tuple>> stable_rows;
    std::vector<RecordId> stable_ids;
    for (size_t i = 0; i < 64; ++i) {
        stable_rows.emplace_back(new Tuple());
        HeapObject obj;
        obj.allocate<Data>();
        stable_rows.back()->init(
            0, TupleBody("stable-" + std::to_string(i), std::move(obj)), nullptr);
        stable_ids.push_back(stable_rows.back()->graph_id());
    }
    std::atomic<bool> stop{false};
    std::thread reader([&] {
        while (!stop.load(std::memory_order_acquire)) {
            for (RecordId id : stable_ids) {
                RecordGraphState* state =
                    RecordGraphRegistry::instance().try_pin(id);
                ASSERT_NE(state, nullptr);
                RecordGraphRegistry::instance().unpin(id);
            }
        }
    });
    std::vector<std::unique_ptr<Tuple>> growing_rows;
    growing_rows.reserve(4096 * 3);
    for (size_t i = 0; i < 4096 * 3; ++i) {
        growing_rows.emplace_back(new Tuple());
        HeapObject obj;
        obj.allocate<Data>();
        growing_rows.back()->init(
            0, TupleBody("concurrent-growth-" + std::to_string(i),
                         std::move(obj)), nullptr);
    }
    stop.store(true, std::memory_order_release);
    reader.join();
    for (auto& row : growing_rows) release_version_chain(row.get());
    for (auto& row : stable_rows) release_version_chain(row.get());
}

}  // namespace
