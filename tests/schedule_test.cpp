// Designated anomaly schedules and randomized schedules, each run on Oze's
// record graphs (repr=legacy) and on Saros (repr=saros); see ScheduleTest.

#include <algorithm>
#include <array>
#include <atomic>
#include <cstdlib>
#include <filesystem>
#include <functional>
#include <iostream>
#include <map>
#include <mutex>
#include <random>
#include <set>
#include <sstream>
#include <string>
#include <vector>

#define GLOBAL_VALUE_DEFINE

#include "../saros/include/test.hh"

namespace {

class ScheduleTest : public TestCase {
protected:
    void SetUp() override {
        std::call_once(logging_once_, [] {
            google::InitGoogleLogging("schedule_test");
            FLAGS_stderrthreshold = 0;
        });
        apply(true);
        TestCase::SetUp();
    }

    // Oze's rules: insert-only transactions skip read validation. The
    // environment can enable OZE_TEST_READ_SET_INDEX, OZE_TEST_IMPORT_SKIP_RETIRABLE.
    static void apply(bool incremental) {
        FLAGS_thread_num = NumVirtualThreads;
        FLAGS_saros = incremental;
        FLAGS_retirement_threshold = 0;
        FLAGS_validate_insert_only_reads = false;
        // TxIDs repeat across schedules.
        ReadFootprintRegistry::instance().clear_for_tests();
        FLAGS_read_set_hash_index = std::getenv("OZE_TEST_READ_SET_INDEX") != nullptr;
        FLAGS_import_skip_retirable = std::getenv("OZE_TEST_IMPORT_SKIP_RETIRABLE") != nullptr;
    }

    void reinit(bool incremental) {
        TestCase::TearDown();
        apply(incremental);
        TestCase::SetUp();
        prefill_retired_histories(incremental);
    }

    // OZE_TEST_FREE_CHUNKS_PREFILL=N: every key's history starts with N facts
    // of epoch-1 transactions, which a retirer at floor 3 retires (and, with
    // --free_retired_history_chunks, unlinks and frees), so that the schedule
    // reads histories whose prefix is gone. Freeing on and off must then decide
    // every schedule alike.
    void prefill_retired_histories(bool incremental) {
        const char* v = std::getenv("OZE_TEST_FREE_CHUNKS_PREFILL");
        if (v == nullptr || !incremental) return;
        const uint64_t n = std::strtoull(v, nullptr, 10);
        if (n == 0) return;
        GlobalEpoch.obj_ = 1;
        for (int i = 0; i < NumVirtualThreads; ++i)
            ThManagementTable[i]->atomicStoreEpoch(1);
        TxExecutor* retirer = txmap[NumVirtualThreads - 1];
        retirer->begin();
        TxID old_tx = retirer->txid_;
        for (auto& [key, t] : table) {
            (void)key;
            t->lock_.w_lock();
            for (uint64_t i = 0; i < n; ++i) {
                GraphDelta d;
                d.kind_ = GraphDeltaKind::PublishRead;
                d.txid_ = old_tx;
                d.txid_.tid = static_cast<uint32_t>(1000 + i);
                d.reads_from_txid_ = old_tx;
                d.retire_epoch_ = 1;
                t->graph_log().append(std::move(d));
            }
            t->lock_.w_unlock();
        }
        GlobalEpoch.obj_ = 4;
        for (int i = 0; i < NumVirtualThreads; ++i)
            ThManagementTable[i]->atomicStoreEpoch(4);
        retirer->begin();   // floor 3 > 1
        for (auto& [key, t] : table) {
            (void)key;
            t->lock_.w_lock();
            EXPECT_TRUE(retirer->retire_record_graph_log_facts(t));
            t->lock_.w_unlock();
        }
        ChunkRetirementRegistry::instance().drain_all();
    }

    static bool all_of(const std::vector<bool>& v) {
        return std::all_of(v.begin(), v.end(), [](bool b) { return b; });
    }

    static std::string vec(const std::vector<bool>& v) {
        std::string s;
        for (bool b : v) s += b ? '1' : '0';
        return s;
    }

    static void report(const std::string& schedule, bool incremental,
                       const std::vector<bool>& commits) {
        std::cout << "[ gate     ] schedule=" << schedule
                  << " repr=" << (incremental ? "saros" : "legacy")
                  << " commits=" << vec(commits)
                  << " accepted_anomaly=" << all_of(commits) << std::endl;
    }

    // Schedules: each returns the commit outcome of every participant.

    // (1) Blind-write discovery cycle: t2 -rw-> t0 -wr-> t3 -rw-> t1 -wr-> t2.
    std::vector<bool> blind_write_cycle() {
        GlobalEpoch.obj_ = 5;
        for (int i = 0; i < NumVirtualThreads; ++i)
            ThManagementTable[i]->atomicStoreEpoch(5);
        for (const char* t : {"0", "1", "2", "3"}) begin(t);
        EXPECT_TRUE(write("e", "2"));   // private writes keep t2 and t3 off
        EXPECT_TRUE(write("f", "3"));   // the read-only path
        EXPECT_TRUE(read("b", "2"));
        EXPECT_TRUE(read("d", "3"));
        EXPECT_TRUE(write("a", "0"));
        EXPECT_TRUE(write("b", "0"));
        const bool c0 = commit("0");
        EXPECT_TRUE(write("c", "1"));
        EXPECT_TRUE(write("d", "1"));
        const bool c1 = commit("1");
        const bool r22 = read("c", "2");
        const bool r30 = read("a", "3");
        const bool c2 = r22 && commit("2");
        const bool c3 = r30 && commit("3");
        return {c0, c1, c2, c3};
    }

    // (2) Read-crown cycle: the same schedule without the private writes, so
    // that t2 and t3 take the read-only validation path.
    std::vector<bool> read_crown_cycle() {
        GlobalEpoch.obj_ = 5;
        for (int i = 0; i < NumVirtualThreads; ++i)
            ThManagementTable[i]->atomicStoreEpoch(5);
        for (const char* t : {"0", "1", "2", "3"}) begin(t);
        EXPECT_TRUE(read("b", "2"));
        EXPECT_TRUE(read("d", "3"));
        EXPECT_TRUE(write("a", "0"));
        EXPECT_TRUE(write("b", "0"));
        const bool c0 = commit("0");
        EXPECT_TRUE(write("c", "1"));
        EXPECT_TRUE(write("d", "1"));
        const bool c1 = commit("1");
        const bool r22 = read("c", "2");
        const bool r30 = read("a", "3");
        const bool c2 = r22 && commit("2");
        const bool c3 = r30 && commit("3");
        return {c0, c1, c2, c3};
    }

    // (3) Insert-only cycle: I reads A(init); W blind-writes B then A and
    // commits; I reads B(W), inserts a fresh key and validates.
    std::vector<bool> insert_only_cycle() {
        begin("1");
        begin("2");
        EXPECT_TRUE(read("a", "1"));
        EXPECT_TRUE(write("b", "2"));
        EXPECT_TRUE(write("a", "2"));
        const bool w = commit("2");
        const bool rb = read("b", "1");
        EXPECT_EQ(insert("0", "1"), Status::OK);
        const bool i = rb && commit("1");
        return {w, i};
    }

    // (4) Read-only anomaly (Fekete, O'Neil and O'Neil):
    //   R -> W1 (rw on A), W1 -> W2 (wr on C), W2 -> R (wr on B).
    std::vector<bool> read_only_anomaly(bool* r_read_b_from_w2 = nullptr) {
        begin("1");
        begin("2");
        begin("3");
        const TxID w2 = txmap[3]->txid_;
        EXPECT_TRUE(read("a", "1"));
        EXPECT_TRUE(write("a", "2"));
        EXPECT_TRUE(write("c", "2"));
        const bool w1 = commit("2");
        EXPECT_TRUE(read("c", "3"));
        EXPECT_TRUE(write("b", "3"));
        const bool w2c = commit("3");
        const bool rb = read("b", "1");
        if (r_read_b_from_w2 != nullptr) {
            auto it = txmap[1]->readSet_.find(row_key(table["b"]));
            *r_read_b_from_w2 =
                it != txmap[1]->readSet_.end() && it->second == w2;
        }
        const bool r = rb && commit("1");
        return {w1, w2c, r};
    }

    // (5) T validates two writes (x, then y); the targets discovered at both
    // name record p, whose graph holds U -> V. Recorded for both
    // representations.
    std::vector<bool> propagation_mark_case() {
        GlobalEpoch.obj_ = 5;
        for (int i = 0; i < NumVirtualThreads; ++i)
            ThManagementTable[i]->atomicStoreEpoch(5);
        // U = 1, V = 2, T = 3, R = 4
        for (const char* t : {"1", "2", "3", "4"}) begin(t);
        EXPECT_TRUE(read("p", "1"));    // U reads p(init)
        EXPECT_TRUE(read("x", "1"));    // U reads x(init)
        EXPECT_TRUE(read("y", "2"));    // V reads y(init)
        EXPECT_TRUE(read("p", "2"));    // V reads p(init)
        EXPECT_TRUE(write("q", "1"));   // U writes q (keeps U off the crown)
        const bool cu = commit("1");
        EXPECT_TRUE(write("p", "2"));   // V overwrites p: U -rw-> V on p
        const bool cv = commit("2");
        EXPECT_TRUE(read("q", "4"));    // R reads q <- U
        EXPECT_TRUE(write("x", "3"));   // T blind-writes x: U -rw-> T
        EXPECT_TRUE(write("y", "3"));   // T blind-writes y: V -rw-> T
        const bool ct = commit("3");
        const bool rp = read("p", "4"); // R reads p <- V
        EXPECT_TRUE(write("z", "4"));
        const bool cr = rp && commit("4");
        return {cu, cv, ct, cr};
    }

    static inline std::once_flag logging_once_;
};

// Runs `CALL` on legacy and on Saros and expects equal acceptance. With
// OZE_ACCEPTS, Oze's rules must accept the anomaly the schedule constructs.
#define RUN_GATE(NAME, CALL, OZE_ACCEPTS)                                      \
    do {                                                                        \
        reinit(false);                                                          \
        const auto leg = CALL;                                                  \
        report(NAME, false, leg);                                               \
        reinit(true);                                                           \
        const auto sar = CALL;                                                  \
        report(NAME, true, sar);                                                \
        EXPECT_EQ(all_of(leg), all_of(sar))                                     \
            << NAME << ": legacy and Saros disagree on acceptance";             \
        if (OZE_ACCEPTS) {                                                      \
            EXPECT_TRUE(all_of(leg))                                            \
                << NAME << ": Oze's rules reject -- schedule does not "         \
                           "exercise the rule";                                 \
        }                                                                       \
    } while (0)

TEST_F(ScheduleTest, BlindWriteDiscoveryCycle) {
    RUN_GATE("blind_write_discovery_cycle", blind_write_cycle(), true);
}

TEST_F(ScheduleTest, ReadCrownCycle) {
    RUN_GATE("read_crown_cycle", read_crown_cycle(), true);
}

TEST_F(ScheduleTest, InsertOnlyCycle) {
    RUN_GATE("insert_only_cycle", insert_only_cycle(), true);
}

// Recorded, not asserted to be accepted; legacy and Saros must agree.
TEST_F(ScheduleTest, PropagationMarkCase) {
    RUN_GATE("propagation_mark_case", propagation_mark_case(), false);
}

// Every designated schedule on both representations with
// --validate_insert_only_reads=false (Oze) and =true (Saros as evaluated).
// Validating the reads of insert-only transactions rejects the insert-only
// cycle; every other schedule keeps its outcome under Oze's rules.
TEST_F(ScheduleTest, DesignatedSchedules) {
    const char* names[] = {"blind_write_discovery_cycle", "read_crown_cycle",
                           "insert_only_cycle", "propagation_mark_case",
                           "read_only_anomaly"};
    for (bool incremental : {false, true}) {
        for (bool f13 : {false, true}) {
            for (int k = 0; k < 5; ++k) {
                reinit(incremental);
                FLAGS_validate_insert_only_reads = f13;
                const std::vector<bool> v =
                    k == 0 ? blind_write_cycle()
                    : k == 1 ? read_crown_cycle()
                    : k == 2 ? insert_only_cycle()
                    : k == 3 ? propagation_mark_case()
                             : read_only_anomaly();
                std::cout << "[ gate     ] designated schedule=" << names[k]
                          << " repr=" << (incremental ? "saros" : "legacy")
                          << " validate_insert_only_reads=" << f13
                          << " commits=" << vec(v)
                          << " accepted_anomaly=" << all_of(v) << std::endl;
                if (k == 2) {
                    EXPECT_EQ(all_of(v), !f13)
                        << "insert-only cycle: fix=" << f13
                        << " repr=" << incremental;
                } else {
                    EXPECT_TRUE(all_of(v))
                        << names[k] << " must keep its outcome under Oze's rules with "
                        << "fix=" << f13 << " repr=" << incremental;
                }
            }
        }
    }
}

// The read-only anomaly (Fekete, O'Neil and O'Neil): Saros must decide it as
// legacy Oze does.
TEST_F(ScheduleTest, ReadOnlyAnomaly) {
    bool leg_w2 = false, sar_w2 = false;
    reinit(false);
    const auto leg = read_only_anomaly(&leg_w2);
    report("read_only_anomaly", false, leg);
    reinit(true);
    const auto sar = read_only_anomaly(&sar_w2);
    report("read_only_anomaly", true, sar);
    std::cout << "[ gate     ] read_only_anomaly R_read_B_from_W2 legacy=" << leg_w2
              << " saros=" << sar_w2 << std::endl;
    EXPECT_EQ(all_of(sar), all_of(leg));
}


// Read-set reachability. Oze copies a validator's read set onto every record
// it validates, so a later validator can reach the other records it read;
// Saros publishes a read fact only at the record read. Whenever legacy rejects
// one of these schedules, Saros must too.
//
// (A) The read-only anomaly with R also writing a fresh record, so that R
// takes the full validation path: R -> W1 (rw on a), W1 -> W2 (wr on c),
// W2 -> R (wr on b).
TEST_F(ScheduleTest, ReadSetWritingReaderAnomaly) {
    auto sched = [&](bool* r_b_from_w2) {
        begin("1"); begin("2"); begin("3");
        const TxID w2 = txmap[3]->txid_;
        EXPECT_TRUE(read("a", "1"));          // R reads a(init)
        EXPECT_TRUE(write("a", "2"));         // W1 blind-writes a, c
        EXPECT_TRUE(write("c", "2"));
        const bool c1 = commit("2");
        EXPECT_TRUE(read("c", "3"));          // W2 reads c <- W1
        EXPECT_TRUE(write("b", "3"));         // W2 writes b
        const bool c2 = commit("3");
        const bool rb = read("b", "1");       // R reads b <- W2
        auto it = txmap[1]->readSet_.find(row_key(table["b"]));
        *r_b_from_w2 = it != txmap[1]->readSet_.end() && it->second == w2;
        EXPECT_TRUE(write("d", "1"));         // R is not read-only
        const bool cr = rb && commit("1");
        return std::vector<bool>{c1, c2, cr};
    };
    bool okv = false;
    reinit(false);
    const auto leg = sched(&okv);
    report("readset_writing_reader", false, leg);
    std::cout << "[ gate     ] readset_writing_reader legacy R_read_B_from_W2=" << okv << std::endl;
    reinit(true);
    const auto sar = sched(&okv);
    report("readset_writing_reader", true, sar);
    std::cout << "[ gate     ] readset_writing_reader saros R_read_B_from_W2=" << okv << std::endl;
    if (!all_of(leg))
        EXPECT_FALSE(all_of(sar))
            << "reduction violated: legacy rejects but Saros accepts";
}

// (B) The follower's read lies outside the follower's publication chain.
//   R reads a(init); F reads x(init);
//   G overwrites x and writes g, commits            -> F -rw-> G (on x)
//   F overwrites a, commits                         -> R -rw-> F (on a)
//   R reads g <- G, writes d, commits               -> G -wr-> R (on g)
// Cycle R -> F -> G -> R.  F is R's follower; the edge F -> G lives at x, a
// record F only READ.  Oze copies F's read set onto a when F validates its
// write of a, so R, validating a, can discover x.
TEST_F(ScheduleTest, ReadSetFollowerReadOutsideChain) {
    auto sched = [&](bool* versions_ok) {
        begin("1"); begin("2"); begin("3");   // R = 1, F = 2, G = 3
        const TxID a_init = table["a"]->ldAcqLatest()->txid_;
        const TxID g_tx = txmap[3]->txid_;
        EXPECT_TRUE(read("a", "1"));          // R reads a(init)
        EXPECT_TRUE(read("x", "2"));          // F reads x(init)
        EXPECT_TRUE(write("x", "3"));         // G overwrites x
        EXPECT_TRUE(write("g", "3"));         // G writes g
        const bool cg = commit("3");
        EXPECT_TRUE(write("a", "2"));         // F overwrites a
        const bool cf = commit("2");
        const bool rg = read("g", "1");       // R reads g <- G
        auto ia = txmap[1]->readSet_.find(row_key(table["a"]));
        auto ig = txmap[1]->readSet_.find(row_key(table["g"]));
        *versions_ok = ia != txmap[1]->readSet_.end() && ia->second == a_init &&
                       ig != txmap[1]->readSet_.end() && ig->second == g_tx;
        EXPECT_TRUE(write("d", "1"));         // R is not read-only
        const bool cr = rg && commit("1");
        return std::vector<bool>{cg, cf, cr};
    };
    bool okv = false;
    reinit(false);
    const auto leg = sched(&okv);
    report("readset_follower_outside_chain", false, leg);
    std::cout << "[ gate     ] readset_follower_outside_chain legacy versions_ok=" << okv << std::endl;
    reinit(true);
    const auto sar = sched(&okv);
    report("readset_follower_outside_chain", true, sar);
    std::cout << "[ gate     ] readset_follower_outside_chain saros versions_ok=" << okv << std::endl;
    if (!all_of(leg))
        EXPECT_FALSE(all_of(sar))
            << "reduction violated: legacy rejects but Saros accepts";
}

// (D) Two-L1 cycle (the BoMB shape):
//   S2 reads p(init)                 ; L1 reads m(init), writes p, commits
//   S1 reads m(init), writes m       -> L1 -rw-> S1 (on m), S2 -rw-> L1 (on p)
//   L1' reads m <- S1, writes q      -> S1 -wr-> L1' (on m)
//   S2 reads q <- L1', inserts, commits  -> L1' -wr-> S2 (on q)
// The middle edges lie only at m, a record L1 read.
TEST_F(ScheduleTest, TwoL1Cycle) {
    auto sched = [&](bool* ok) {
        begin("1"); begin("2"); begin("3"); begin("4");  // S2, L1, S1, L1'
        const TxID l1p = txmap[4]->txid_;
        EXPECT_TRUE(read("p", "1"));          // S2 reads p(init)
        EXPECT_TRUE(read("m", "2"));          // L1 reads m(init)
        EXPECT_TRUE(write("p", "2"));         // L1 writes p
        const bool c_l1 = commit("2");
        EXPECT_TRUE(read("m", "3"));          // S1 reads m(init)
        EXPECT_TRUE(write("m", "3"));         // S1 writes m
        const bool c_s1 = commit("3");
        EXPECT_TRUE(read("m", "4"));          // L1' reads m <- S1
        EXPECT_TRUE(write("q", "4"));         // L1' writes q
        const bool c_l1p = commit("4");
        const bool rq = read("q", "1");       // S2 reads q <- L1'
        auto iq = txmap[1]->readSet_.find(row_key(table["q"]));
        *ok = iq != txmap[1]->readSet_.end() && iq->second == l1p;
        EXPECT_EQ(insert("0", "1"), Status::OK);  // S2 is insert-only
        const bool c_s2 = rq && commit("1");
        return std::vector<bool>{c_l1, c_s1, c_l1p, c_s2};
    };
    bool okv = false;
    reinit(false);
    const auto leg = sched(&okv);
    report("two_l1_cycle", false, leg);
    std::cout << "[ gate     ] two_l1_cycle legacy S2_read_q_from_L1p=" << okv << std::endl;
    reinit(true);
    const auto sar = sched(&okv);
    report("two_l1_cycle", true, sar);
    std::cout << "[ gate     ] two_l1_cycle saros S2_read_q_from_L1p=" << okv << std::endl;
}

// (E) Forwarding without an aborted reader:
//   T1: wa, rc, wc, commit;  T2: ra(init) ... wa, we, commit (validates last)
//   T3: we, ra(<-T1), commit.
// T2 forwards its version of a below T1's; nothing orders T3's and T2's
// writes of e.
TEST_F(ScheduleTest, ForwardingWithoutAbortedReader) {
    auto sched = [&]() {
        begin("1"); begin("2"); begin("3");
        EXPECT_TRUE(write("a", "1"));
        EXPECT_TRUE(read("c", "1"));
        EXPECT_TRUE(read("a", "2"));          // T2 reads a(init)
        EXPECT_TRUE(write("c", "1"));
        const bool c1 = commit("1");
        EXPECT_TRUE(write("a", "2"));
        EXPECT_TRUE(write("e", "2"));
        EXPECT_TRUE(write("e", "3"));
        const bool r3 = read("a", "3");       // T3 reads a <- T1
        const bool c3 = r3 && commit("3");
        const bool c2 = commit("2");
        return std::vector<bool>{c1, c2, c3};
    };
    GlobalEpoch.obj_ = 5;
    reinit(false);
    GlobalEpoch.obj_ = 5;
    for (int i = 0; i < NumVirtualThreads; ++i) ThManagementTable[i]->atomicStoreEpoch(5);
    const auto leg = sched();
    report("forwarding_without_aborted_reader", false, leg);
    reinit(true);
    GlobalEpoch.obj_ = 5;
    for (int i = 0; i < NumVirtualThreads; ++i) ThManagementTable[i]->atomicStoreEpoch(5);
    const auto sar = sched();
    report("forwarding_without_aborted_reader", true, sar);
}

// (C) Randomized differential with direction: for each schedule, does Saros
// commit a transaction that legacy aborts?  OZE_RULES_RANDOM_N schedules (default
// 2000), OZE_RULES_RANDOM_TX transactions (default 3), OZE_RULES_RANDOM_KEYS
// keys (default 4), OZE_RULES_RANDOM_SEED base seed (default 1000).
TEST_F(ScheduleTest, RandomScheduleDirection) {
    auto env = [](const char* k, int d) {
        const char* v = std::getenv(k); return v ? std::atoi(v) : d; };
    const int kSchedules = env("OZE_RULES_RANDOM_N", 2000);
    const int ntx = env("OZE_RULES_RANDOM_TX", 3);
    const int nkeys = env("OZE_RULES_RANDOM_KEYS", 4);
    const int seed0 = env("OZE_RULES_RANDOM_SEED", 1000);
    const char* keys[] = {"a", "b", "c", "d", "e", "f", "g", "h"};
    int same = 0, saros_more = 0, legacy_more = 0, incomparable = 0, printed = 0;
    for (int sidx = 0; sidx < kSchedules; ++sidx) {
        std::mt19937 rng(seed0 + sidx);
        struct Op { int tx; int kind; int key; };
        std::vector<std::vector<Op>> per(ntx);
        for (int t = 0; t < ntx; ++t) {
            int n = 1 + rng() % 3;
            for (int i = 0; i < n; ++i)
                per[t].push_back({t, (int)(rng() % 2), (int)(rng() % nkeys)});
            per[t].push_back({t, 2, 0});
        }
        std::vector<Op> ops;
        std::vector<size_t> pos(ntx, 0);
        while (true) {
            std::vector<int> live;
            for (int t = 0; t < ntx; ++t) if (pos[t] < per[t].size()) live.push_back(t);
            if (live.empty()) break;
            int t = live[rng() % live.size()];
            ops.push_back(per[t][pos[t]++]);
        }
        auto exec = [&](bool incremental) {
            // OZE_RULES_RANDOM_DUMP_ROOT: a history dump per (schedule,
            // representation), written at the next TearDown.
            if (const char* root = std::getenv("OZE_RULES_RANDOM_DUMP_ROOT")) {
                const std::string dir = std::string(root) + "/" +
                    std::to_string(seed0 + sidx) + (incremental ? "_saros" : "_legacy");
                std::filesystem::create_directories(dir);
                setenv("OZE_TEST_HISTORY_DUMP_PATH", dir.c_str(), 1);
            }
            reinit(incremental);
            GlobalEpoch.obj_ = 5;
            for (int i = 0; i < NumVirtualThreads; ++i)
                ThManagementTable[i]->atomicStoreEpoch(5);
            std::vector<bool> alive(ntx, true), committed(ntx, false);
            for (int t = 0; t < ntx; ++t) begin(std::to_string(t + 1));
            for (const auto& op : ops) {
                const std::string id = std::to_string(op.tx + 1);
                if (!alive[op.tx]) continue;
                bool ok = true;
                if (op.kind == 0) ok = read(keys[op.key], id);
                else if (op.kind == 1) ok = write(keys[op.key], id);
                else { committed[op.tx] = commit(id); alive[op.tx] = false; continue; }
                if (!ok) { txmap[op.tx + 1]->abort(); alive[op.tx] = false; }
            }
            return committed;
        };
        // OZE_RULES_RANDOM_ONLY=legacy|saros runs one representation, so that a
        // history dump (OZE_TEST_HISTORY_DUMP_PATH) holds that run only.
        const char* only = std::getenv("OZE_RULES_RANDOM_ONLY");
        if (only != nullptr) {
            const auto c = exec(std::string(only) == "saros");
            std::cout << "[ gate     ] random_single repr=" << only << " seed="
                      << (seed0 + sidx) << " commits=" << vec(c) << std::endl;
            continue;
        }
        const auto l = exec(false);
        const auto r = exec(true);
        bool s_extra = false, l_extra = false;
        for (int t = 0; t < ntx; ++t) {
            if (r[t] && !l[t]) s_extra = true;
            if (l[t] && !r[t]) l_extra = true;
        }
        if (!s_extra && !l_extra) ++same;
        else if (s_extra && !l_extra) ++saros_more;
        else if (l_extra && !s_extra) ++legacy_more;
        else ++incomparable;
        if (s_extra && printed < 20) {
            ++printed;
            std::cout << "[ gate     ] saros_commits_extra seed=" << (seed0 + sidx)
                      << " legacy=" << vec(l) << " saros=" << vec(r) << " ops=";
            for (const auto& op : ops)
                std::cout << "T" << (op.tx + 1)
                          << (op.kind == 0 ? "r" : op.kind == 1 ? "w" : "c")
                          << (op.kind == 2 ? "" : keys[op.key]) << " ";
            std::cout << std::endl;
        }
    }
    std::cout << "[ gate     ] random_direction schedules=" << kSchedules
              << " tx=" << ntx << " keys=" << nkeys
              << " identical=" << same
              << " saros_commits_more=" << saros_more
              << " legacy_commits_more=" << legacy_more
              << " incomparable=" << incomparable << std::endl;
    SUCCEED();
}

// An edge whose source is below the reclamation floor but whose destination is
// live must stay visible to import.
TEST_F(ScheduleTest, EdgeWithLiveDestinationSurvivesRetirement) {
    auto run = [this]() {
        reinit(true);
        GlobalEpoch.obj_ = 1;
        for (int i = 0; i < NumVirtualThreads; ++i)
            ThManagementTable[i]->atomicStoreEpoch(1);
        begin("1");                          // src, epoch 1
        const TxID src = txmap[1]->txid_;
        GlobalEpoch.obj_ = 3;
        for (int i = 0; i < NumVirtualThreads; ++i)
            ThManagementTable[i]->atomicStoreEpoch(3);
        begin("2");                          // dst, epoch 3, stays live
        const TxID dst = txmap[2]->txid_;
        EXPECT_EQ(src.epoch, 1U);
        EXPECT_EQ(dst.epoch, 3U);
        Tuple* s = table["s"];
        s->lock_.w_lock();
        txmap[2]->append_edge(s, src, dst, GraphEdgeKind::WrittenBy);
        s->lock_.w_unlock();
        // Floor rises: every slot but dst's moves to 4; dst holds 3.
        GlobalEpoch.obj_ = 4;
        for (int i = 0; i < NumVirtualThreads; ++i)
            ThManagementTable[i]->atomicStoreEpoch(i == 2 ? 3 : 4);
        begin("3");
        EXPECT_EQ(txmap[3]->reclamation_epoch_, 2U);
        s->lock_.w_lock();
        EXPECT_TRUE(txmap[3]->retire_record_graph_log_facts(s));
        s->lock_.w_unlock();
        const uint64_t retired =
            s->graph_log().retired_seq_.load(std::memory_order_acquire);
        Graph g;
        txmap[3]->last_imported_seq_.clear();
        EXPECT_TRUE(txmap[3]->import_record(s, s->graph_log().committed(), g));
        bool visible = false;
        if (auto it = g.find(src); it != g.end())
            visible = it->second.writtenBy_.count(dst) != 0;
        std::cout << "[ gate     ] retirement retired_seq=" << retired << " edge_visible=" << visible
                  << std::endl;
        return std::make_pair(retired, visible);
    };
    const auto both = run();
    EXPECT_EQ(both.first, 0U) << "both-endpoints stamp must keep the edge";
    EXPECT_TRUE(both.second) << "edge to a live destination must stay visible";
}

// An edge whose endpoints are both below the floor is retired.
TEST_F(ScheduleTest, EdgeWithBothEndpointsOldIsRetired) {
    reinit(true);
    GlobalEpoch.obj_ = 1;
    for (int i = 0; i < NumVirtualThreads; ++i)
        ThManagementTable[i]->atomicStoreEpoch(1);
    begin("1");
    begin("2");
    const TxID a = txmap[1]->txid_, b = txmap[2]->txid_;
    Tuple* s = table["s"];
    s->lock_.w_lock();
    txmap[2]->append_edge(s, a, b, GraphEdgeKind::WrittenBy);
    txmap[2]->append_publish_read(s, b, a);
    s->lock_.w_unlock();
    GlobalEpoch.obj_ = 5;
    for (int i = 0; i < NumVirtualThreads; ++i)
        ThManagementTable[i]->atomicStoreEpoch(5);
    begin("3");
    ASSERT_GT(txmap[3]->reclamation_epoch_, 1U);
    s->lock_.w_lock();
    EXPECT_TRUE(txmap[3]->retire_record_graph_log_facts(s));
    s->lock_.w_unlock();
    EXPECT_EQ(s->graph_log().retired_seq_.load(std::memory_order_acquire), 2U);
}

// Random small schedules: how often legacy and Saros commit the same
// transactions (printed, not asserted; read-time version choice can differ).
TEST_F(ScheduleTest, RandomScheduleDifferential) {
    const int kSchedules = std::getenv("OZE_RULES_RANDOM_N")
                               ? std::atoi(std::getenv("OZE_RULES_RANDOM_N"))
                               : 300;
    const char* keys[] = {"a", "b", "c", "d"};
    int agree = 0, disagree = 0, both_all = 0, anomalies_leg = 0, anomalies_sar = 0;
    for (int s = 0; s < kSchedules; ++s) {
        std::mt19937 rng(1000 + s);
        struct Op { int tx; int kind; int key; };  // kind 0=read 1=write 2=commit
        std::vector<Op> ops;
        const int ntx = 3;
        std::vector<std::vector<Op>> per(ntx);
        for (int t = 0; t < ntx; ++t) {
            int n = 1 + rng() % 3;
            for (int i = 0; i < n; ++i)
                per[t].push_back({t, (int)(rng() % 2), (int)(rng() % 4)});
            per[t].push_back({t, 2, 0});
        }
        std::vector<size_t> pos(ntx, 0);
        while (true) {
            std::vector<int> live;
            for (int t = 0; t < ntx; ++t) if (pos[t] < per[t].size()) live.push_back(t);
            if (live.empty()) break;
            int t = live[rng() % live.size()];
            ops.push_back(per[t][pos[t]++]);
        }
        auto exec = [&](bool incremental) {
            reinit(incremental);
            GlobalEpoch.obj_ = 5;
            for (int i = 0; i < NumVirtualThreads; ++i)
                ThManagementTable[i]->atomicStoreEpoch(5);
            std::vector<bool> alive(ntx, true), committed(ntx, false);
            for (int t = 0; t < ntx; ++t) begin(std::to_string(t + 1));
            for (const auto& op : ops) {
                const std::string id = std::to_string(op.tx + 1);
                if (!alive[op.tx]) continue;
                bool ok = true;
                if (op.kind == 0) ok = read(keys[op.key], id);
                else if (op.kind == 1) ok = write(keys[op.key], id);
                else { committed[op.tx] = commit(id); alive[op.tx] = false; continue; }
                if (!ok) { txmap[op.tx + 1]->abort(); alive[op.tx] = false; }
            }
            return committed;
        };
        const auto l = exec(false);
        const auto r = exec(true);
        if (l == r) ++agree; else ++disagree;
        if (all_of(l) && all_of(r)) ++both_all;
        (void)anomalies_leg; (void)anomalies_sar;
    }
    std::cout << "[ gate     ] random_differential schedules=" << kSchedules
              << " identical_commit_vectors=" << agree
              << " differing_commit_vectors=" << disagree
              << " both_all_committed=" << both_all << std::endl;
    SUCCEED();
}

}  // namespace
