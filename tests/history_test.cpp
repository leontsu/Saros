#include <algorithm>
#include <atomic>
#include <functional>
#include <limits>
#include <map>
#include <mutex>
#include <optional>
#include <random>
#include <set>
#include <string>
#include <thread>
#include <vector>

#define GLOBAL_VALUE_DEFINE

#include "fixture_saros.hh"

namespace {

class HistoryTest : public TestCase {
protected:
    struct GraphEdgeCounts {
        size_t read_facts = 0;
        size_t read_by_edges = 0;
        size_t written_by_edges = 0;
        size_t from_edges = 0;
    };

    struct LogEdgeCounts {
        size_t publish_reads = 0;
        size_t read_by_edges = 0;
        size_t written_by_edges = 0;
        size_t includes = 0;
        size_t aborts = 0;
    };

    void SetUp() override {
        std::call_once(logging_once_, [] {
            google::InitGoogleLogging("history_test");
            FLAGS_stderrthreshold = 0;
        });

        FLAGS_thread_num = NumVirtualThreads;
        FLAGS_saros = false;
        FLAGS_retirement_threshold = 0;
        TestCase::SetUp();
    }

    void TearDown() override {
        TestCase::TearDown();
    }

    void reinitialize(bool incremental) {
        TestCase::TearDown();
        FLAGS_thread_num = NumVirtualThreads;
        FLAGS_saros = incremental;
        FLAGS_retirement_threshold = 0;
        TestCase::SetUp();
    }

    // The graph that importing `tuple`'s whole history produces.
    Graph materialize_graph(TxExecutor* tx, Tuple* tuple) {
        Graph materialized;
        const auto saved = tx->last_imported_seq_;
        tx->last_imported_seq_.clear();
        EXPECT_TRUE(tx->import_record(
            tuple, tuple->graph_log().committed(), materialized));
        tx->last_imported_seq_ = saved;
        return materialized;
    }

    // Reader -> reads-from transaction of the unretired read facts of `tuple`.
    std::map<TxID, TxID> published_reads(TxExecutor*, Tuple* tuple) {
        std::map<TxID, TxID> reads;
        const RecordGraphState& log = tuple->graph_log();
        log.for_each_delta(log.retired_seq_.load(std::memory_order_acquire),
                           log.committed(), [&](const GraphDelta& delta) {
            if (delta.kind_ == GraphDeltaKind::PublishRead)
                reads[delta.txid_] = delta.reads_from_txid_;
            else if (delta.kind_ == GraphDeltaKind::MarkAborted)
                reads.erase(delta.txid_);
            return true;
        });
        return reads;
    }

    static GraphDelta delta_at(const RecordGraphState& log, uint64_t seq) {
        GraphDelta result;
        if (seq == 0) return result;
        log.for_each_delta(seq - 1, seq, [&](const GraphDelta& d) {
            result = d;
            return true;
        });
        return result;
    }

    std::set<GraphDeltaKind> delta_kinds(const Tuple* tuple) {
        std::set<GraphDeltaKind> kinds;
        tuple->graph_log().for_each([&](const GraphDelta& delta) {
            kinds.insert(delta.kind_);
            return true;
        });
        return kinds;
    }

    size_t count_delta_kind(const Tuple* tuple, GraphDeltaKind kind) {
        size_t count = 0;
        tuple->graph_log().for_each([&](const GraphDelta& delta) {
            if (delta.kind_ == kind) {
                ++count;
            }
            return true;
        });
        return count;
    }

    GraphEdgeCounts count_graph_edges(const Graph& graph) {
        GraphEdgeCounts counts;
        for (const auto& [_, node] : graph) {
            counts.read_facts += node.readSet_.size();
            counts.read_by_edges += node.readBy_.size();
            counts.written_by_edges += node.writtenBy_.size();
            counts.from_edges += node.from_.size();
        }
        return counts;
    }

    void expect_graph_equal(const Graph& expected, const Graph& actual) {
        ASSERT_EQ(expected.size(), actual.size());
        auto lhs = expected.begin();
        auto rhs = actual.begin();
        for (; lhs != expected.end(); ++lhs, ++rhs) {
            ASSERT_EQ(lhs->first, rhs->first);
            const TxNode& a = lhs->second;
            const TxNode& b = rhs->second;
            EXPECT_EQ(a.id_, b.id_);
            EXPECT_EQ(a.status_, b.status_);
            EXPECT_EQ(a.is_aborted, b.is_aborted);
            EXPECT_EQ(a.readSet_, b.readSet_);
            EXPECT_EQ(a.writeSet_, b.writeSet_);
            EXPECT_EQ(a.readBy_, b.readBy_);
            EXPECT_EQ(a.writtenBy_, b.writtenBy_);
            EXPECT_EQ(a.from_, b.from_);
        }
    }

    void clear_import_state(TxExecutor* tx) {
        tx->graph_.clear();
        tx->last_imported_seq_.clear();
        tx->import_visited_.clear();
        tx->import_aborted_.clear();
    }

    LogEdgeCounts count_log_edges(const Tuple* tuple) {
        LogEdgeCounts counts;
        tuple->graph_log().for_each([&](const GraphDelta& delta) {
            switch (delta.kind_) {
                case GraphDeltaKind::PublishRead:
                    ++counts.publish_reads;
                    break;
                case GraphDeltaKind::AddEdge:
                    if (delta.edge_kind_ == GraphEdgeKind::ReadBy) {
                        ++counts.read_by_edges;
                    } else {
                        ++counts.written_by_edges;
                    }
                    break;
                case GraphDeltaKind::IncludeRecord:
                    ++counts.includes;
                    break;
                case GraphDeltaKind::MarkAborted:
                    ++counts.aborts;
                    break;
                case GraphDeltaKind::EnsureNode:
                    break;
            }
            return true;
        });
        return counts;
    }

    void install_pending_write_version(const std::string& key,
                                       const std::string& txid) {
        TxExecutor* tx = txmap[std::stoi(txid)];
        Tuple* tuple = table[key];
        ASSERT_FALSE(tx->write_set_.empty());

        tuple->lock_.w_lock();
        TxSet followers;
        const Status stat = tx->insert_version(tx->write_set_.front(), followers);
        tuple->lock_.w_unlock();
        ASSERT_EQ(stat, Status::OK);
    }

    // Installs txid's pending write on key, as write_validation() does, then
    // aborts the transaction.
    void splice_then_abort(const std::string& key, const std::string& txid) {
        install_pending_write_version(key, txid);
        txmap[std::stoi(txid)]->abort();
    }

    uint64_t current_min_epoch() { return txmap[0]->get_min_epoch(); }

    std::vector<bool> run_simple_reader_then_writer_schedule(bool incremental) {
        reinitialize(incremental);
        begin("1");
        begin("2");
        (void)read("a", "1");
        (void)write("a", "2");
        return {commit("1"), commit("2")};
    }

    std::vector<bool> run_rw_cycle_schedule(bool incremental) {
        reinitialize(incremental);
        begin("1");
        begin("2");
        (void)read("a", "1");
        (void)read("b", "2");
        (void)write("b", "1");
        (void)write("a", "2");
        return {commit("1"), commit("2")};
    }

    std::vector<bool> run_same_tuple_writers_schedule(bool incremental) {
        reinitialize(incremental);
        begin("1");
        begin("2");
        (void)write("a", "1");
        (void)write("a", "2");
        return {commit("1"), commit("2")};
    }

    struct ReadOnlyAnomalyOutcome {
        bool w1_committed = false;
        bool w2_committed = false;
        bool r_committed = false;
        // The versions R read: all-commit is an anomaly only if R read W2's B.
        TxID r_read_a_from;
        TxID r_read_b_from;
        bool r_read_a_from_init = false;
        bool r_read_b_from_w2 = false;
        bool r_read_b_from_init = false;
        // Mechanism probes (incremental mode only).
        uint64_t w2_publish_seq_on_c = 0;   // W2's read-time PublishRead seq on C
        bool b_has_include_of_c = false;    // B's log gained IncludeRecord -> C at step 4
        uint64_t b_include_seq_of_c = 0;
    };

    // The read-only anomaly (Fekete, O'Neil and O'Neil): R -> W1 (rw on A), W1 -> W2 (wr on C),
    // W2 -> R (wr on B).
    ReadOnlyAnomalyOutcome run_read_only_anomaly_schedule(bool incremental) {
        reinitialize(incremental);
        ReadOnlyAnomalyOutcome out{};

        // R = tx 1 (read-only), W1 = tx 2, W2 = tx 3.
        begin("1");
        begin("2");
        begin("3");

        // Initial-version txids, captured before any write can supersede
        // them; W2's txid_ stays valid until its next begin().
        const TxID a_init_txid = table["a"]->ldAcqLatest()->txid_;
        const TxID b_init_txid = table["b"]->ldAcqLatest()->txid_;
        const TxID w2_txid = txmap[3]->txid_;

        // 1. R reads A -> initial version.
        EXPECT_TRUE(read("a", "1"));
        out.r_read_a_from = txmap[1]->readSet_.at(row_key(table["a"]));
        out.r_read_a_from_init = out.r_read_a_from == a_init_txid;

        // 2. W1 blind-writes A and C and commits, validating A, then C.
        EXPECT_TRUE(write("a", "2"));
        EXPECT_TRUE(write("c", "2"));
        out.w1_committed = commit("2");

        // 3. W2 reads C -> W1's version.
        EXPECT_TRUE(read("c", "3"));
        if (incremental) {
            // W2's read-time publication on C (writePhase() clears it).
            out.w2_publish_seq_on_c = txmap[3]->published_seq_.at(table["c"]);
        }

        // 4. W2 writes B and commits.
        EXPECT_TRUE(write("b", "3"));
        out.w2_committed = commit("3");

        // Mechanism probe, before R touches B: did W2's validation of B
        // publish an IncludeRecord reaching C?
        table["b"]->graph_log().for_each([&](const GraphDelta& delta) {
            if (delta.kind_ == GraphDeltaKind::IncludeRecord &&
                delta.include_record_id_ == table["c"]->graph_id()) {
                out.b_has_include_of_c = true;
                out.b_include_seq_of_c = delta.include_seq_;
                return false;
            }
            return true;
        });

        // 5. R reads B; capture the read-from transaction before commit().
        EXPECT_TRUE(read("b", "1"));
        out.r_read_b_from = txmap[1]->readSet_.at(row_key(table["b"]));
        out.r_read_b_from_w2 = out.r_read_b_from == w2_txid;
        out.r_read_b_from_init = out.r_read_b_from == b_init_txid;

        // 6. R validates read-only.
        out.r_committed = commit("1");
        return out;
    }

    struct ForwardingOutcome {
        bool w_committed = false;
        bool r_committed = false;
        bool me_committed = false;
        // Identities, for assertions in the tests.
        TxID me_txid;
        TxID follower_txid;   // W, the writer of the version r read on A
        // Probes on A's graph_log_ (meaningful in incremental mode only).
        bool a_has_reader_edge_r_me = false;    // AddEdge(r -> ME): must NOT be published when forwarded
        bool a_has_forward_edge_me_f = false;   // AddEdge(ME -> follower): must be published when forwarded
        // Version-chain observation on A (newest first).
        TxID a_latest_txid;
        bool a_has_second_version = false;
        TxID a_second_txid;
    };

    // ME's validation of A finds the cycle ME -> r -> ME through its reader r, so ME must forward
    // or abort; W, not the loader, wrote A, so forwarding below W closes no real cycle.
    ForwardingOutcome run_forwarding_schedule(bool incremental) {
        reinitialize(incremental);
        ForwardingOutcome out{};

        // r = tx 1, ME = tx 2, W = tx 3.
        begin("3");
        const TxID w_txid = txmap[3]->txid_;
        EXPECT_TRUE(write("a", "3"));  // 0. W blind-writes A...
        out.w_committed = commit("3"); //    ...and commits
        EXPECT_EQ(table["a"]->ldAcqLatest()->txid_, w_txid);

        begin("1");
        begin("2");
        const TxID r_txid = txmap[1]->txid_;
        const TxID me_txid = txmap[2]->txid_;
        out.me_txid = me_txid;
        out.follower_txid = w_txid;

        EXPECT_TRUE(read("c", "2"));   // 1. ME reads C(init)
        EXPECT_TRUE(read("a", "1"));   // 2. r reads A -> W's version
        EXPECT_TRUE(write("c", "1"));  // 3. r overwrites C...
        out.r_committed = commit("1"); //    ...and commits (publishes ME -> r)

        EXPECT_TRUE(write("a", "2"));  // 4. ME blind-writes A...
        out.me_committed = commit("2");

        Tuple* a = table["a"];
        a->graph_log().for_each([&](const GraphDelta& delta) {
            if (delta.kind_ != GraphDeltaKind::AddEdge ||
                delta.edge_kind_ != GraphEdgeKind::WrittenBy)
                return true;
            if (delta.txid_ == r_txid && delta.other_txid_ == me_txid)
                out.a_has_reader_edge_r_me = true;
            if (delta.txid_ == me_txid && delta.other_txid_ == w_txid)
                out.a_has_forward_edge_me_f = true;
            return true;
        });

        Version* latest = a->ldAcqLatest();
        out.a_latest_txid = latest->txid_;
        Version* second = latest->ldAcqNext();
        if (second != nullptr && second != VERSION_TERMINATION) {
            out.a_has_second_version = true;
            out.a_second_txid = second->txid_;
        }
        return out;
    }

    struct StretchedAnomalyOutcome {
        bool w1_committed = false;
        bool w2_committed = false;
        bool w3_committed = false;
        bool r_committed = false;
    };

    // A read-only anomaly whose middle edges lie on records (C, D) that R reaches only through
    // B's prefix references.
    StretchedAnomalyOutcome run_stretched_read_only_anomaly_schedule(bool incremental) {
        reinitialize(incremental);
        StretchedAnomalyOutcome out{};

        // R = tx 1 (read-only), W1 = tx 2, W2 = tx 3, W3 = tx 4.
        begin("1");
        begin("2");
        begin("3");
        begin("4");

        EXPECT_TRUE(read("a", "1"));   // R reads A (before W1 validates)

        EXPECT_TRUE(write("a", "2"));  // W1 blind-writes A and C...
        EXPECT_TRUE(write("c", "2"));
        out.w1_committed = commit("2");

        EXPECT_TRUE(read("c", "3"));   // W2 reads C -> W1's version...
        EXPECT_TRUE(write("d", "3"));  // ...writes D
        out.w2_committed = commit("3");

        EXPECT_TRUE(read("d", "4"));   // W3 reads D -> W2's version...
        EXPECT_TRUE(write("b", "4"));  // ...writes B
        out.w3_committed = commit("4");

        EXPECT_TRUE(read("b", "1"));   // R reads B (after W3 commits)

        out.r_committed = commit("1"); // R validates read-only.
        return out;
    }

    struct InsertOnlyCycleOutcome {
        bool w_committed = false;
        bool i_committed = false;
    };

    // An insert-only transaction I whose reads close a cycle with W: with
    // --validate_insert_only_reads I aborts.
    InsertOnlyCycleOutcome run_insert_only_cycle_schedule(bool incremental) {
        reinitialize(incremental);
        InsertOnlyCycleOutcome out{};

        // I = tx 1, W = tx 2.
        begin("1");
        begin("2");

        EXPECT_TRUE(read("a", "1"));   // I reads A (initial version)

        EXPECT_TRUE(write("b", "2"));  // W blind-writes B THEN A (order matters)...
        EXPECT_TRUE(write("a", "2"));
        out.w_committed = commit("2");

        EXPECT_TRUE(read("b", "1"));   // I reads B -> W's version
        // "0" is outside the fixture's 'a'..'z' key range, so this is a
        // genuine fresh insert, not a collision with an existing key.
        EXPECT_EQ(insert("0", "1"), Status::OK);

        out.i_committed = commit("1"); // I validates: insert-only WITH reads.
        return out;
    }

    // OZE_TEST_INSERT_ONLY_MODE=legacy|incremental runs one representation,
    // so that a history dump holds one run.
    static std::vector<bool> anomaly_arms() {
        const char* arm = std::getenv("OZE_TEST_ANOMALY_ARM");
        if (arm != nullptr) {
            if (std::string(arm) == "legacy") return {false};
            if (std::string(arm) == "incremental") return {true};
        }
        return {false, true};
    }

    // Printed so the log records the outcomes.
    static void report_outcome(
            const std::string& schedule,
            const std::vector<std::pair<std::string, bool>>& fields) {
        std::cout << "[ anomaly  ] " << schedule;
        for (const auto& field : fields) {
            std::cout << ' ' << field.first << '='
                      << (field.second ? "1" : "0");
        }
        std::cout << std::endl;
    }

    static inline std::once_flag logging_once_;
};

TEST_F(HistoryTest, ReaderThenWriterConflictOnOneTuple) {
    reinitialize(true);

    begin("1");
    ASSERT_TRUE(read("a", "1"));
    const TxID reader = txmap[1]->txid_;

    Tuple* tuple = table["a"];
    ASSERT_FALSE(tuple->graph_log().empty());
    EXPECT_EQ(delta_at(tuple->graph_log(), 1).kind_, GraphDeltaKind::PublishRead);
    EXPECT_EQ(delta_at(tuple->graph_log(), 1).txid_, reader);

    begin("2");
    ASSERT_TRUE(write("a", "2"));
    const TxID writer = txmap[2]->txid_;
    ASSERT_TRUE(commit("2"));

    bool saw_conflict_edge = false;
    tuple->graph_log().for_each([&](const GraphDelta& delta) {
        if (delta.kind_ == GraphDeltaKind::AddEdge &&
            delta.edge_kind_ == GraphEdgeKind::WrittenBy &&
            delta.txid_ == reader &&
            delta.other_txid_ == writer) {
            saw_conflict_edge = true;
            return false;
        }
        return true;
    });
    EXPECT_TRUE(saw_conflict_edge);

    const Graph materialized = materialize_graph(txmap[2], tuple);
    ASSERT_NE(materialized.find(reader), materialized.end());
    EXPECT_EQ(materialized.at(reader).writtenBy_.count(writer), 1U);
}

TEST_F(HistoryTest, PublishReadMaterializesReadFromEdge) {
    reinitialize(true);

    begin("1");
    begin("2");
    const TxID source = txmap[1]->txid_;
    const TxID reader = txmap[2]->txid_;
    Tuple* tuple = table["a"];

    tuple->lock_.w_lock();
    txmap[2]->append_publish_read(tuple, reader, source);
    tuple->lock_.w_unlock();

    const Graph materialized = materialize_graph(txmap[2], tuple);

    ASSERT_NE(materialized.find(source), materialized.end());
    ASSERT_NE(materialized.find(reader), materialized.end());
    EXPECT_EQ(materialized.at(reader).readSet_.at(row_key(tuple)), source);
    EXPECT_EQ(materialized.at(source).readBy_.count(reader), 1U);
    EXPECT_EQ(materialized.at(reader).from_.count(source), 1U);
}

TEST_F(HistoryTest, FindReadersUsesImportedGraph) {
    reinitialize(true);

    begin("1");
    begin("2");
    const TxID reader = txmap[1]->txid_;
    const TxID source = txmap[2]->txid_;
    Tuple* tuple = table["a"];

    Graph imported;
    imported.emplace(reader, TxNode(reader));
    imported.at(reader).readSet_[row_key(tuple)] = source;

    TxSet readers;
    txmap[2]->find_readers(imported, tuple, readers);

    EXPECT_EQ(readers.count(reader), 1U);
}

TEST_F(HistoryTest, SkippedVersionEdgePublication) {
    reinitialize(false);

    begin("1");
    begin("2");
    ASSERT_TRUE(write("a", "2"));
    const TxID legacy_reader = txmap[1]->txid_;
    const TxID legacy_skipped_writer = txmap[2]->txid_;
    install_pending_write_version("a", "2");

    ASSERT_TRUE(read("a", "1"));
    Tuple* legacy_tuple = table["a"];
    const auto legacy_counts = count_graph_edges(legacy_tuple->graph_);
    ASSERT_NE(legacy_tuple->graph_.find(legacy_reader), legacy_tuple->graph_.end());
    EXPECT_EQ(
        legacy_tuple->graph_.at(legacy_reader).writtenBy_.count(legacy_skipped_writer),
        1U);
    EXPECT_GT(legacy_counts.written_by_edges, 0U);

    reinitialize(true);

    begin("1");
    begin("2");
    ASSERT_TRUE(write("a", "2"));
    const TxID incremental_reader = txmap[1]->txid_;
    const TxID incremental_skipped_writer = txmap[2]->txid_;
    install_pending_write_version("a", "2");

    ASSERT_TRUE(read("a", "1"));
    Tuple* incremental_tuple = table["a"];
    const auto incremental_log_counts = count_log_edges(incremental_tuple);
    bool saw_skipped_version_edge_delta = false;
    incremental_tuple->graph_log().for_each([&](const GraphDelta& delta) {
        if (delta.kind_ == GraphDeltaKind::AddEdge &&
            delta.edge_kind_ == GraphEdgeKind::WrittenBy &&
            delta.txid_ == incremental_reader &&
            delta.other_txid_ == incremental_skipped_writer) {
            saw_skipped_version_edge_delta = true;
            return false;
        }
        return true;
    });

    const Graph materialized = materialize_graph(txmap[1], incremental_tuple);
    ASSERT_NE(materialized.find(incremental_reader), materialized.end());
    EXPECT_EQ(
        materialized.at(incremental_reader).writtenBy_.count(incremental_skipped_writer),
        1U);
    EXPECT_EQ(incremental_log_counts.publish_reads, 1U);
    EXPECT_EQ(incremental_log_counts.written_by_edges, 1U);
    EXPECT_TRUE(saw_skipped_version_edge_delta);
}

TEST_F(HistoryTest, InheritedDependencyAcrossMultipleTuples) {
    reinitialize(true);

    begin("1");
    begin("2");
    const TxID from = txmap[1]->txid_;
    const TxID to = txmap[2]->txid_;

    Tuple* first = table["a"];
    Tuple* second = table["b"];

    first->lock_.w_lock();
    const uint64_t first_seq =
        txmap[1]->append_edge(first, from, to, GraphEdgeKind::WrittenBy);
    first->lock_.w_unlock();

    second->lock_.w_lock();
    const uint64_t second_seq =
        txmap[1]->append_include(second, first, first_seq);
    second->lock_.w_unlock();

    txmap[1]->graph_.clear();
    txmap[1]->last_imported_seq_.clear();
    ASSERT_TRUE(txmap[1]->import_record(second, second_seq, txmap[1]->graph_));

    ASSERT_NE(txmap[1]->graph_.find(from), txmap[1]->graph_.end());
    EXPECT_EQ(txmap[1]->graph_.at(from).writtenBy_.count(to), 1U);
    EXPECT_EQ(txmap[1]->last_imported_seq_[first->graph_id()], first_seq);
    EXPECT_EQ(txmap[1]->last_imported_seq_[second->graph_id()], second_seq);
}

TEST_F(HistoryTest, CycleExposedOnlyThroughIncludedRecordState) {
    reinitialize(true);

    begin("1");
    begin("2");
    begin("3");
    const TxID t1 = txmap[1]->txid_;
    const TxID t2 = txmap[2]->txid_;

    Tuple* first = table["a"];
    Tuple* second = table["b"];

    first->lock_.w_lock();
    const uint64_t first_seq =
        txmap[3]->append_edge(first, t1, t2, GraphEdgeKind::WrittenBy);
    first->lock_.w_unlock();

    second->lock_.w_lock();
    const uint64_t include_seq =
        txmap[3]->append_include(second, first, first_seq);
    const uint64_t second_seq =
        txmap[3]->append_edge(second, t2, t1, GraphEdgeKind::WrittenBy);
    second->lock_.w_unlock();

    Graph local_only;
    txmap[3]->apply_delta(local_only, second, delta_at(second->graph_log(), second->graph_log().committed()));
    ASSERT_NE(local_only.find(t2), local_only.end());
    EXPECT_FALSE(txmap[3]->has_cycle(local_only, t2));

    txmap[3]->graph_.clear();
    txmap[3]->last_imported_seq_.clear();
    ASSERT_TRUE(txmap[3]->import_record(second, second_seq, txmap[3]->graph_));
    EXPECT_EQ(include_seq, 1U);
    EXPECT_TRUE(txmap[3]->has_cycle(txmap[3]->graph_, t1));
}

TEST_F(HistoryTest, AbortAfterPartialPublication) {
    reinitialize(true);

    begin("1");
    ASSERT_TRUE(read("a", "1"));
    ASSERT_TRUE(read("b", "1"));
    const TxID txid = txmap[1]->txid_;

    txmap[1]->abort();

    for (const char* key : {"a", "b"}) {
        Tuple* tuple = table[key];
        bool saw_publish = false;
        bool saw_abort = false;
        tuple->graph_log().for_each([&](const GraphDelta& delta) {
            if (delta.kind_ == GraphDeltaKind::PublishRead && delta.txid_ == txid) {
                saw_publish = true;
            }
            if (delta.kind_ == GraphDeltaKind::MarkAborted && delta.txid_ == txid) {
                saw_abort = true;
            }
            return true;
        });

        EXPECT_TRUE(saw_publish);
        EXPECT_TRUE(saw_abort);

        const std::map<TxID, TxID> reads = published_reads(txmap[1], tuple);
        EXPECT_EQ(reads.count(txid), 0U);

        const Graph materialized = materialize_graph(txmap[1], tuple);
        ASSERT_NE(materialized.find(txid), materialized.end());
        EXPECT_TRUE(materialized.at(txid).is_aborted);
        EXPECT_TRUE(materialized.at(txid).readBy_.empty());
        EXPECT_TRUE(materialized.at(txid).writtenBy_.empty());
        EXPECT_TRUE(materialized.at(txid).from_.empty());
    }
}

TEST_F(HistoryTest, CommitAbortParityWithLegacyOzeOnHandcraftedSchedules) {
    const auto legacy_simple = run_simple_reader_then_writer_schedule(false);
    const auto incremental_simple = run_simple_reader_then_writer_schedule(true);
    EXPECT_EQ(incremental_simple, legacy_simple);

    const auto legacy_cycle = run_rw_cycle_schedule(false);
    const auto incremental_cycle = run_rw_cycle_schedule(true);
    EXPECT_EQ(incremental_cycle, legacy_cycle);

    const auto legacy_same_tuple_writers = run_same_tuple_writers_schedule(false);
    const auto incremental_same_tuple_writers = run_same_tuple_writers_schedule(true);
    EXPECT_EQ(incremental_same_tuple_writers, legacy_same_tuple_writers);
}

// retire_record_graph_log_facts() only advances the watermark, so it is safe
// to call at any time.
TEST_F(HistoryTest, RetireRecordGraphLogFactsIsSafeToCall) {
    reinitialize(true);
    begin("1");
    begin("2");
    ASSERT_TRUE(read("a", "1"));
    ASSERT_TRUE(write("a", "2"));
    ASSERT_TRUE(commit("2"));

    Tuple* tuple = table["a"];
    const uint64_t committed_before = tuple->graph_log().committed();
    ASSERT_GT(committed_before, 0U);

    begin("3");
    tuple->lock_.w_lock();
    const bool ok = txmap[3]->retire_record_graph_log_facts(tuple);
    tuple->lock_.w_unlock();

    EXPECT_TRUE(ok);
    // Never renumbers or frees anything: committed_seq_ is unchanged.
    EXPECT_EQ(tuple->graph_log().committed(), committed_before);
    // Nothing is retirable yet: the floor trails every transaction by one.
    EXPECT_EQ(tuple->graph_log().retired_seq_.load(std::memory_order_acquire), 0U);
}

// maybe_retire_record_history() is a no-op at threshold 0.
TEST_F(HistoryTest, MaybeRetireRecordHistoryIsNoOpAtZeroThreshold) {
    reinitialize(true);
    ASSERT_EQ(FLAGS_retirement_threshold, 0U);

    begin("1");
    begin("2");
    ASSERT_TRUE(read("a", "1"));
    ASSERT_TRUE(write("a", "2"));
    ASSERT_TRUE(commit("2"));

    Tuple* tuple = table["a"];
    const uint64_t committed_before = tuple->graph_log().committed();
    ASSERT_GT(committed_before, 0U);

    tuple->lock_.w_lock();
    const bool ok = txmap[1]->maybe_retire_record_history(tuple);
    tuple->lock_.w_unlock();

    EXPECT_TRUE(ok);
    EXPECT_EQ(tuple->graph_log().committed(), committed_before);
}

// Retirement skips dead facts but never hides a cycle that exists only through
// a live prefix reference (whose retirement bound is that of the cited prefix).
TEST_F(HistoryTest, RetirementSkipsDeadFactsButKeepsLiveCycle) {
    reinitialize(true);
    GlobalEpoch.obj_ = 10;
    for (int i = 0; i < NumVirtualThreads; ++i) {
        ThManagementTable[i]->atomicStoreEpoch(10);
    }

    Tuple* first = table["a"];
    Tuple* second = table["b"];

    // Dead filler at epoch 10, retirable once the epoch has moved on.
    begin("1");
    begin("5");
    const TxID dead1 = txmap[1]->txid_;
    const TxID dead2 = txmap[5]->txid_;
    second->lock_.w_lock();
    const uint64_t filler_seq =
        txmap[1]->append_edge(second, dead1, dead2, GraphEdgeKind::WrittenBy);
    second->lock_.w_unlock();

    // The epoch moves on for everyone; threads 2 and 3 register the cycle
    // facts at the new epoch.
    atomicAddGE();
    atomicAddGE();
    atomicAddGE();
    for (int i = 0; i < NumVirtualThreads; ++i) {
        ThManagementTable[i]->atomicStoreEpoch(13);
    }

    begin("2");
    begin("3");
    const TxID t1 = txmap[2]->txid_;
    const TxID t2 = txmap[3]->txid_;
    ASSERT_EQ(t1.epoch, 13U);
    ASSERT_EQ(t2.epoch, 13U);

    // The cycle is visible only through second's prefix reference to first.
    first->lock_.w_lock();
    const uint64_t first_seq =
        txmap[3]->append_edge(first, t1, t2, GraphEdgeKind::WrittenBy);
    first->lock_.w_unlock();

    second->lock_.w_lock();
    const uint64_t include_seq =
        txmap[3]->append_include(second, first, first_seq);
    const uint64_t second_seq =
        txmap[3]->append_edge(second, t2, t1, GraphEdgeKind::WrittenBy);
    second->lock_.w_unlock();
    ASSERT_GT(include_seq, filler_seq);

    // Time moves on except for threads 2 and 3, parked at epoch 13.
    atomicAddGE();
    atomicAddGE();
    for (int i = 0; i < NumVirtualThreads; ++i) {
        if (i == 2 || i == 3) continue;
        ThManagementTable[i]->atomicStoreEpoch(15);
    }

    begin("6");
    ASSERT_EQ(txmap[6]->txid_.epoch, 13U);
    ASSERT_EQ(txmap[6]->reclamation_epoch_, 12U);

    second->lock_.w_lock();
    ASSERT_TRUE(txmap[6]->retire_record_graph_log_facts(second));
    second->lock_.w_unlock();

    const uint64_t retired =
        second->graph_log().retired_seq_.load(std::memory_order_acquire);
    EXPECT_EQ(retired, filler_seq);
    ASSERT_LT(retired, include_seq);

    txmap[6]->graph_.clear();
    txmap[6]->last_imported_seq_.clear();
    ASSERT_TRUE(txmap[6]->import_record(second, second_seq, txmap[6]->graph_));

    EXPECT_EQ(txmap[6]->graph_.find(dead1), txmap[6]->graph_.end());
    EXPECT_EQ(txmap[6]->graph_.find(dead2), txmap[6]->graph_.end());
    EXPECT_TRUE(txmap[6]->has_cycle(txmap[6]->graph_, t1));
}

// A live long transaction holds down get_min_epoch() through its epoch stamp;
// retirement never passes its epoch, and its facts still import.
TEST_F(HistoryTest, LiveLongTxnFactsNeverRetired) {
    reinitialize(true);
    GlobalEpoch.obj_ = 10;
    for (int i = 0; i < NumVirtualThreads; ++i) {
        ThManagementTable[i]->atomicStoreEpoch(10);
    }

    Tuple* tuple = table["a"];

    // Old facts, well before the long-running txn begins.
    begin("1");
    begin("4");
    ASSERT_TRUE(read("a", "1"));
    ASSERT_TRUE(write("a", "4"));
    ASSERT_TRUE(commit("4"));
    const uint64_t seq_after_old_facts = tuple->graph_log().committed();
    ASSERT_GT(seq_after_old_facts, 0U);

    // The long transaction begins at the new epoch, reads "a" and stays live.
    atomicAddGE();
    atomicAddGE();
    for (int i = 0; i < NumVirtualThreads; ++i) {
        if (i == 2) continue;
        ThManagementTable[i]->atomicStoreEpoch(12);
    }
    begin("2");
    ASSERT_EQ(txmap[2]->txid_.epoch, 12U);
    ASSERT_TRUE(read("a", "2"));

    // A short writer commits, publishing the edge long txn -> writer
    // (stamped with the long transaction's epoch).
    begin("8");
    ASSERT_TRUE(write("a", "8"));
    ASSERT_TRUE(commit("8"));
    const uint64_t seq_after_long_txn_facts = tuple->graph_log().committed();
    ASSERT_GT(seq_after_long_txn_facts, seq_after_old_facts);

    // Time moves on except for thread 2 (the long transaction).
    atomicAddGE();
    atomicAddGE();
    atomicAddGE();
    for (int i = 0; i < NumVirtualThreads; ++i) {
        if (i == 2) continue;
        ThManagementTable[i]->atomicStoreEpoch(15);
    }

    begin("3");
    ASSERT_EQ(txmap[3]->txid_.epoch, 12U);  // bounded by thread 2's stamp
    ASSERT_EQ(txmap[3]->reclamation_epoch_, 11U);

    tuple->lock_.w_lock();
    ASSERT_TRUE(txmap[3]->retire_record_graph_log_facts(tuple));
    tuple->lock_.w_unlock();

    uint64_t retired =
        tuple->graph_log().retired_seq_.load(std::memory_order_acquire);
    EXPECT_EQ(retired, seq_after_old_facts);
    ASSERT_LT(retired, seq_after_long_txn_facts);

    // Retrying retirement while thread 2 stays parked must never advance
    // retired_seq_ any further.
    tuple->lock_.w_lock();
    ASSERT_TRUE(txmap[3]->retire_record_graph_log_facts(tuple));
    tuple->lock_.w_unlock();
    retired = tuple->graph_log().retired_seq_.load(std::memory_order_acquire);
    EXPECT_EQ(retired, seq_after_old_facts);

    // The long txn's own facts (at its still-live epoch) must still replay.
    const Graph materialized = materialize_graph(txmap[3], tuple);
    EXPECT_NE(materialized.find(txmap[2]->txid_), materialized.end());
}

// Saros's validation prunes tuple->graph_ with gc() as Oze does; the same
// schedule runs in both modes.
TEST_F(HistoryTest, TupleGraphPrunedInPrototypeMode) {
    auto exercise_prune = [this](bool incremental) {
        reinitialize(incremental);
        GlobalEpoch.obj_ = 10;
        for (int i = 0; i < NumVirtualThreads; ++i) {
            ThManagementTable[i]->atomicStoreEpoch(10);
        }

        Tuple* tuple = table["a"];

        // Old reader: its node lands in tuple->graph_ no matter the mode.
        begin("1");
        ASSERT_TRUE(read("a", "1"));
        ASSERT_NE(tuple->graph_.find(txmap[1]->txid_), tuple->graph_.end());

        // Epoch moves on well past the old reader; a later transaction's
        // validation must prune it out of tuple->graph_.
        for (int i = 0; i < 5; ++i) {
            atomicAddGE();
        }
        for (int i = 0; i < NumVirtualThreads; ++i) {
            ThManagementTable[i]->atomicStoreEpoch(15);
        }

        begin("2");
        ASSERT_TRUE(write("a", "2"));
        ASSERT_TRUE(commit("2"));

        EXPECT_EQ(tuple->graph_.find(txmap[1]->txid_), tuple->graph_.end());
    };

    exercise_prune(/*incremental=*/true);
    exercise_prune(/*incremental=*/false);
}

// One thread appends to B under its latch while several threads import A
// (which references B) and B without a lock, across chunk boundaries.
TEST_F(HistoryTest, ConcurrentImportRecordUnderConcurrentAppendIsRaceFree) {
    reinitialize(true);

    // Many chunks, so that readers cross chunk boundaries while the writer
    // allocates.
    constexpr uint64_t kNumDeltas = 6000;
    constexpr uint64_t kDstOffset = 1'000'000;
    constexpr int kNumReaderThreads = 4;
    constexpr uint64_t kUnbounded = std::numeric_limits<uint64_t>::max();

    Tuple* a = table["a"];
    Tuple* b = table["b"];

    // A references B with an unbounded sequence, so imports clamp to B's
    // committed count at the time.
    a->lock_.w_lock();
    txmap[0]->append_include(a, b, kUnbounded);
    a->lock_.w_unlock();

    std::atomic<bool> writer_done{false};

    std::thread writer([&] {
        TxExecutor* tx = txmap[0];
        for (uint64_t i = 1; i <= kNumDeltas; ++i) {
            TxID src, dst;
            src.tid = i;
            dst.tid = i + kDstOffset;
            b->lock_.w_lock();
            tx->append_edge(b, src, dst, GraphEdgeKind::WrittenBy);
            b->lock_.w_unlock();
        }
        writer_done.store(true, std::memory_order_release);
    });

    // Checks that g holds exactly the dense prefix [1, N] of the appended
    // edges; returns an error description, or nullopt.
    auto verify_prefix = [&](const Graph& g, uint64_t upto_seq_requested,
                              uint64_t* observed_count_out = nullptr) -> std::optional<std::string> {
        uint64_t expected_next = 1;
        for (const auto& [txid, node] : g) {
            if (txid.tid == 0 || txid.tid >= kDstOffset) continue;
            if (txid.tid != expected_next) {
                return "gap or reorder in imported prefix: expected tid " +
                       std::to_string(expected_next) + " got " +
                       std::to_string(txid.tid);
            }
            TxID expected_dst;
            expected_dst.tid = txid.tid + kDstOffset;
            if (node.writtenBy_.size() != 1U || node.writtenBy_.count(expected_dst) != 1U) {
                return "corrupt payload for tid " + std::to_string(txid.tid);
            }
            ++expected_next;
        }
        const uint64_t observed_count = expected_next - 1;
        if (observed_count_out != nullptr) *observed_count_out = observed_count;
        if (observed_count > kNumDeltas) {
            return "observed more deltas than were ever appended";
        }
        if (observed_count > upto_seq_requested) {
            return "import read past the requested upto_seq bound";
        }
        return std::nullopt;
    };

    std::vector<std::thread> readers;
    std::vector<std::optional<std::string>> reader_errors(kNumReaderThreads);
    for (int r = 0; r < kNumReaderThreads; ++r) {
        readers.emplace_back([&, r] {
            TxExecutor* tx = txmap[1 + r];
            uint64_t iter = 0;
            while (!writer_done.load(std::memory_order_acquire) || iter < 2) {
                ++iter;
                // Every third iteration reads a bounded prefix.
                const uint64_t upto =
                    (iter % 3 == 0) ? (iter * 97) % (kNumDeltas * 2) : kUnbounded;

                // Direct bounded import of B, no include chain involved.
                tx->graph_.clear();
                tx->last_imported_seq_.clear();
                tx->import_visited_.clear();
                if (!tx->import_record(b, upto, tx->graph_)) {
                    reader_errors[r] = "import_record(b) returned false";
                    return;
                }
                if (auto err = verify_prefix(tx->graph_, upto)) {
                    reader_errors[r] = "direct import of B: " + *err;
                    return;
                }

                // Import through A's IncludeRecord chain into B.
                tx->graph_.clear();
                tx->last_imported_seq_.clear();
                tx->import_visited_.clear();
                if (!tx->import_record(a, kUnbounded, tx->graph_)) {
                    reader_errors[r] = "import_record(a) returned false";
                    return;
                }
                if (auto err = verify_prefix(tx->graph_, kUnbounded)) {
                    reader_errors[r] = "import through A's IncludeRecord: " + *err;
                    return;
                }
            }
        });
    }

    writer.join();
    for (auto& th : readers) th.join();

    for (int r = 0; r < kNumReaderThreads; ++r) {
        EXPECT_FALSE(reader_errors[r].has_value())
            << "reader " << r << ": " << reader_errors[r].value_or("");
    }

    // Final sanity: once the writer has finished, a fresh import observes
    // every appended delta exactly once.
    TxExecutor* tx = txmap[0];
    tx->graph_.clear();
    tx->last_imported_seq_.clear();
    tx->import_visited_.clear();
    ASSERT_TRUE(tx->import_record(b, kUnbounded, tx->graph_));
    uint64_t final_observed_count = 0;
    auto final_err = verify_prefix(tx->graph_, kUnbounded, &final_observed_count);
    EXPECT_FALSE(final_err.has_value()) << final_err.value_or("");
    EXPECT_EQ(final_observed_count, kNumDeltas);
}

// Write skew across two records: T1 reads A, writes B; T2 reads B, writes A.
// The prefix references cross-link the histories, so one of them aborts.
TEST_F(HistoryTest, WriteSkewDetectedViaIncludeChain) {
    reinitialize(true);

    begin("1");
    begin("2");
    ASSERT_TRUE(read("a", "1"));  // T1 reads A
    ASSERT_TRUE(read("b", "2"));  // T2 reads B

    // Both reads are already published (PublishRead) on their respective
    // tuples' logs before either transaction attempts to write or validate.
    Tuple* a = table["a"];
    Tuple* b = table["b"];
    EXPECT_GT(count_log_edges(a).publish_reads, 0U);
    EXPECT_GT(count_log_edges(b).publish_reads, 0U);

    ASSERT_TRUE(write("b", "1"));  // T1 writes B (anti-dependency on T2's read)
    ASSERT_TRUE(write("a", "2"));  // T2 writes A (anti-dependency on T1's read)

    const bool t1_committed = commit("1");
    const bool t2_committed = commit("2");

    EXPECT_NE(t1_committed, t2_committed)
        << "exactly one of the two write-skew transactions must be rejected "
           "via cycle detection; t1=" << t1_committed << " t2=" << t2_committed;
}

// insert_validation() publishes the scanner -> inserter edge to the inserted record's history,
// driven with a hand-made scan entry and a range covering every key prefix.
TEST_F(HistoryTest, PhantomEdgePublishedOnInsert) {
    reinitialize(true);

    begin("1");  // scanner S
    begin("2");  // inserter I
    const TxID scanner = txmap[1]->txid_;
    const TxID inserter = txmap[2]->txid_;

    // "0" is outside the fixture's keys, so insert() creates a new row.
    const int history_offset =
        SCAN_HISTORY_INDEX(Storage::TestTable, txmap[2]->txid_.epoch);
    ScanRange[history_offset].store(0xffffffffULL, std::memory_order_release);
    ScanEntry* scan_entry =
        new ScanEntry(scanner, "0", false, "0", false, nullptr);
    ScanHistory[history_offset].store(scan_entry, std::memory_order_release);

    ASSERT_EQ(insert("0", "2"), Status::OK);
    ASSERT_FALSE(txmap[2]->write_set_.empty());
    Tuple* tuple = txmap[2]->write_set_.front().rcdptr_;

    // validation() seeds graph_ with the transaction's node.
    txmap[2]->graph_.emplace(inserter, TxNode(inserter));

    ASSERT_TRUE(txmap[2]->insert_validation(txmap[2]->write_set_.front()));

    // (a) the phantom scanner->inserter edge is published to the inserted
    // tuple's own log.
    bool saw_edge = false;
    tuple->graph_log().for_each([&](const GraphDelta& delta) {
        if (delta.kind_ == GraphDeltaKind::AddEdge &&
            delta.edge_kind_ == GraphEdgeKind::WrittenBy &&
            delta.txid_ == scanner && delta.other_txid_ == inserter) {
            saw_edge = true;
            return false;
        }
        return true;
    });
    EXPECT_TRUE(saw_edge);

    // (b) a third executor importing only the inserted tuple's log observes
    // the edge in its reconstructed graph.
    Graph imported;
    ASSERT_TRUE(txmap[3]->import_record(
        tuple, tuple->graph_log().committed(), imported));
    ASSERT_NE(imported.find(scanner), imported.end());
    EXPECT_EQ(imported.at(scanner).writtenBy_.count(inserter), 1U);

    // (c) If the inserter aborts, the history gains its abort mark. The write
    // set entry is dropped first so that abort() does not free the row.
    txmap[2]->write_set_.clear();
    txmap[2]->abort();

    bool saw_abort = false;
    tuple->graph_log().for_each([&](const GraphDelta& delta) {
        if (delta.kind_ == GraphDeltaKind::MarkAborted &&
            delta.txid_ == inserter) {
            saw_abort = true;
            return false;
        }
        return true;
    });
    EXPECT_TRUE(saw_abort);

    // Fresh executor (never having imported this tuple before) so its replay
    // starts from sequence 0 and reflects the full post-abort history.
    const Graph materialized = materialize_graph(txmap[4], tuple);
    ASSERT_NE(materialized.find(inserter), materialized.end());
    EXPECT_TRUE(materialized.at(inserter).is_aborted);
    EXPECT_TRUE(materialized.at(inserter).writtenBy_.empty());
    EXPECT_TRUE(materialized.at(inserter).from_.empty());
}

// A read-only transaction's validation also appends the reference chain, so
// that a later importer can follow its reads.
TEST_F(HistoryTest, ReadOnlyTrailPublished) {
    reinitialize(true);

    begin("1");
    ASSERT_TRUE(read("a", "1"));
    ASSERT_TRUE(read("b", "1"));
    TxExecutor* ro = txmap[1];
    const TxID ro_id = ro->txid_;

    // The order in which validation() visits the two rows follows readSet_
    // (by identity), not the read order.
    ASSERT_EQ(ro->readSet_.size(), 2U);
    Tuple* first = row_key_ptr(ro->readSet_.begin()->first);
    Tuple* second = row_key_ptr(std::next(ro->readSet_.begin())->first);
    ASSERT_NE(first, nullptr);
    ASSERT_NE(second, nullptr);
    ASSERT_NE(first, second);

    const auto first_reads_before = published_reads(ro, first);
    ASSERT_EQ(first_reads_before.count(ro_id), 1U);
    const TxID expected_source = first_reads_before.at(ro_id);
    const uint64_t first_seq_at_publish = ro->published_seq_.at(first);

    ASSERT_TRUE(commit("1"));

    // (a) The second history references the first at or after T_ro's fact.
    bool saw_include = false;
    uint64_t include_seq_seen = 0;
    second->graph_log().for_each([&](const GraphDelta& delta) {
        if (delta.kind_ == GraphDeltaKind::IncludeRecord &&
            delta.include_record_id_ == first->graph_id()) {
            saw_include = true;
            include_seq_seen = delta.include_seq_;
            return false;
        }
        return true;
    });
    ASSERT_TRUE(saw_include);
    EXPECT_GE(include_seq_seen, first_seq_at_publish);

    // (b) Importing only the second history reaches T_ro's read fact.
    Graph imported;
    ASSERT_TRUE(txmap[2]->import_record(
        second, second->graph_log().committed(), imported));
    ASSERT_NE(imported.find(ro_id), imported.end());
    ASSERT_EQ(imported.at(ro_id).readSet_.count(row_key(first)), 1U);
    EXPECT_EQ(imported.at(ro_id).readSet_.at(row_key(first)), expected_source);
}

// Oze's merge path commits all three: the W1 -> W2 edge lies only in C's graph, which R's
// read-only validation never merges.
TEST_F(HistoryTest, ReadOnlyAnomalyLegacy) {
    const auto out = run_read_only_anomaly_schedule(false);
    report_outcome("legacy",
                   {{"W1_committed", out.w1_committed},
                    {"W2_committed", out.w2_committed},
                    {"R_committed", out.r_committed},
                    {"R_read_A_init", out.r_read_a_from_init},
                    {"R_read_B_from_W2", out.r_read_b_from_w2},
                    {"R_read_B_init", out.r_read_b_from_init}});
    EXPECT_TRUE(out.r_read_a_from_init)
        << "R read A from " << out.r_read_a_from << ", not the initial version";
    EXPECT_TRUE(out.r_read_b_from_w2)
        << "R read B from " << out.r_read_b_from << ", not W2's version";
    EXPECT_TRUE(out.w1_committed);
    EXPECT_TRUE(out.w2_committed);
    EXPECT_TRUE(out.r_committed);
}

// Saros commits what Oze commits on the forwarding schedule, publishes ME -> W, and places
// ME's version below the version the reader read.
TEST_F(HistoryTest, ForwardingParityWithLegacy) {
    // Both modes must commit all three transactions.
    const auto legacy = run_forwarding_schedule(false);
    EXPECT_TRUE(legacy.w_committed);
    EXPECT_TRUE(legacy.r_committed);
    EXPECT_TRUE(legacy.me_committed)
        << "legacy failed to forward - the schedule no longer triggers "
           "forwarding and this test needs rework";

    const auto inc = run_forwarding_schedule(true);
    EXPECT_EQ(inc.w_committed, legacy.w_committed);
    EXPECT_EQ(inc.r_committed, legacy.r_committed);
    EXPECT_EQ(inc.me_committed, legacy.me_committed)
        << "incremental mode aborted where legacy forwarded";

    // (i) the undone reader->ME edge was never published on A.
    EXPECT_FALSE(inc.a_has_reader_edge_r_me);
    // (ii) the forwarding ordering ME->follower (W, the writer of the
    // version r read) WAS published on A.
    EXPECT_TRUE(inc.a_has_forward_edge_me_f);
    // (iii) ME's version was inserted BELOW the version r read: A's chain,
    // newest first, is [W, ME, init...].
    EXPECT_EQ(inc.a_latest_txid, inc.follower_txid);
    ASSERT_TRUE(inc.a_has_second_version);
    EXPECT_EQ(inc.a_second_txid, inc.me_txid);
}

// Forwarding does not mask a real cycle: ME read W's version of B, so the
// forwarding edge ME -> W closes W -> ME -> W and ME aborts.
TEST_F(HistoryTest, ForwardingStillDetectsRealCycle) {
    for (const bool incremental : {false, true}) {
        reinitialize(incremental);

        // r = tx 1, ME = tx 2, W = tx 3.
        begin("3");
        ASSERT_TRUE(write("a", "3"));   // W blind-writes A and B...
        ASSERT_TRUE(write("b", "3"));
        ASSERT_TRUE(commit("3"));       // ...and commits

        begin("1");
        begin("2");
        ASSERT_TRUE(read("b", "2"));    // ME reads B -> W's version (W -> ME)
        ASSERT_TRUE(read("c", "2"));    // ME reads C(init) (cycle-trigger setup)
        ASSERT_TRUE(read("a", "1"));    // r reads A -> W's version (follower will be W)
        ASSERT_TRUE(read("b", "1"));    // r reads B too, linking B into the
                                        // include closure r publishes (incremental)
        ASSERT_TRUE(write("c", "1"));   // r overwrites C...
        ASSERT_TRUE(commit("1"))        // ...publishing/recording ME -> r
            << "r must commit (incremental=" << incremental << ")";

        ASSERT_TRUE(write("a", "2"));   // ME blind-writes A
        EXPECT_FALSE(commit("2"))
            << "forwarding masked a real cycle (incremental=" << incremental
            << "): repositioning below W cannot fix W -> ME -> W";
    }
}

// With --validate_insert_only_reads, both representations abort the
// insert-only cycle (run_insert_only_cycle_schedule).
TEST_F(HistoryTest, InsertOnlyTransactionValidatesReads) {
    const bool prev_flag = FLAGS_validate_insert_only_reads;
    FLAGS_validate_insert_only_reads = true;
    for (const bool incremental : anomaly_arms()) {
        const auto out = run_insert_only_cycle_schedule(incremental);
        report_outcome(std::string("validate_insert_only_reads=1 incremental=") +
                           (incremental ? "1" : "0"),
                       {{"W_committed", out.w_committed},
                        {"I_committed", out.i_committed}});
        EXPECT_TRUE(out.w_committed) << "incremental=" << incremental;
        EXPECT_FALSE(out.i_committed)
            << "insert-only transaction with a read-conflict cycle "
               "committed (incremental="
            << incremental << ")";
    }
    FLAGS_validate_insert_only_reads = prev_flag;
}

}  // namespace

// A transaction that begins while another slot lags gets an epoch below its own stamp; this
// checks that the resulting floor inversion is reachable and that min_live() still bounds it.
TEST_F(HistoryTest, RetirementFloorInversionReachabilityProbe) {
    reinitialize(true);

    // Slot 0 lags at epoch 1 while the global epoch moves on.
    for (int i = 0; i < NumVirtualThreads; ++i) {
        ThManagementTable[i]->atomicStoreEpoch(1);
    }
    GlobalEpoch.obj_ = 5;

    // V begins while slot 0 lags. Its slot goes to GlobalEpoch (5) but its
    // epoch is the MINIMUM (1), so its floor is 0.
    begin("1");
    TxExecutor& victim = *txmap[1];
    const uint64_t victim_epoch = victim.txid_.epoch;
    const uint64_t victim_floor = victim.reclamation_epoch_;
    const uint64_t victim_slot = ThManagementTable[1]->atomicLoadEpoch();

    // The lagging thread catches up. Nothing about V changes: it is still live,
    // still at epoch 1, still announcing floor 0.
    for (int i = 0; i < NumVirtualThreads; ++i) {
        if (i == 1) continue;   // V's own slot keeps its value while V is live
        ThManagementTable[i]->atomicStoreEpoch(5);
    }

    // A publisher begins now.
    begin("3");
    TxExecutor& publisher = *txmap[3];
    const uint64_t publisher_epoch = publisher.txid_.epoch;
    const uint64_t publisher_floor = publisher.reclamation_epoch_;

    std::cout << "[ g3-probe ] victim_slot=" << victim_slot
              << " victim_epoch=" << victim_epoch
              << " victim_floor=" << victim_floor
              << " publisher_epoch=" << publisher_epoch
              << " publisher_floor=" << publisher_floor
              << " min_live=" << ReclamationFloorAnnouncer::min_live()
              << " inversion_vs_victim_floor="
              << (publisher_floor > victim_floor ? 1 : 0)
              << " publisher_floor_above_live_victim_epoch="
              << (publisher_floor > victim_epoch ? 1 : 0) << std::endl;

    // What begin() computes, restated as assertions so the probe cannot rot.
    EXPECT_EQ(victim_slot, 5u)
        << "begin() sets the caller's slot to GlobalEpoch";
    EXPECT_EQ(victim_epoch, 1u)
        << "begin() takes txid_.epoch from get_min_epoch(), i.e. the LAGGING "
           "thread, not from GlobalEpoch";
    EXPECT_EQ(victim_floor, 0u);
    EXPECT_EQ(publisher_epoch, 5u);
    EXPECT_EQ(publisher_floor, 4u);

    // (1) A floor inversion between two live transactions IS reachable: the
    //     publisher's projection floor is strictly above the victim's.
    EXPECT_GT(publisher_floor, victim_floor)
        << "a floor inversion between two live transactions is not reachable "
           "by this route; the closure-level witness's premise would then be "
           "unreachable in production and should be reported as such";

    // (2) The publisher's floor is above the epoch of a live transaction.
    EXPECT_GT(publisher_floor, victim_epoch)
        << "the publisher's floor does not exceed a live transaction's epoch, "
           "so an image it builds cannot drop that transaction's facts";

    // min_live() is still pinned by the live transaction.
    EXPECT_EQ(ReclamationFloorAnnouncer::min_live(), victim_floor)
        << "min_live() must still be pinned by the live victim; physical "
           "reclamation is not what this probe is about";
}

// VersionRetirementRegistry: the version of an aborted update or delete.

// An aborted update enters the retirement queue once, as unlinked.
TEST_F(HistoryTest, AbortedUpdateVersionEntersRetirementQueueExactlyOnce) {
    reinitialize(false);
    VersionRetirementRegistry::instance().drain_all();
    const uint64_t retired_before =
        VersionRetirementRegistry::counters().versions_retired.load();
    const uint64_t unlinked_before =
        VersionRetirementRegistry::counters().versions_retired_unlinked.load();

    begin("2");
    ASSERT_TRUE(write("a", "2"));
    splice_then_abort("a", "2");

    EXPECT_EQ(VersionRetirementRegistry::counters().versions_retired.load(),
              retired_before + 1);
    EXPECT_EQ(VersionRetirementRegistry::counters().versions_retired_unlinked.load(),
              unlinked_before + 1)
        << "the version was spliced into the chain before the abort, so it "
           "must be retired via the unlink path, not treated as unspliced";
    EXPECT_EQ(VersionRetirementRegistry::instance().pending(), 1u);

    // The chain itself must no longer contain the aborted version: 'a' reads
    // back exactly its original committed content.
    Version* head = table["a"]->ldAcqLatest();
    EXPECT_EQ(head->status_.load(), VersionStatus::committed)
        << "the aborted version must be unlinked, leaving the original "
           "committed version reachable as the chain head";

    VersionRetirementRegistry::instance().drain_all();
}

// The follower-epoch guard: a read may skip a pending version whose writer is
// one epoch older (a writer that began while a slot lagged), not two.
TEST_F(HistoryTest, FollowerEpochGuardAllowsOneEpochOldPendingWriter) {
    for (const bool incremental : {false, true}) {
        for (const uint64_t gap : {1u, 2u}) {
            reinitialize(incremental);
            VersionRetirementRegistry::instance().drain_all();
            const uint64_t now = 5 + gap;
            GlobalEpoch.obj_ = now;
            for (int i = 0; i < NumVirtualThreads; ++i)
                ThManagementTable[i]->atomicStoreEpoch(now);
            ThManagementTable[9]->atomicStoreEpoch(5);
            begin("2");  // W registers at `now` but takes epoch 5 from slot 9
            ASSERT_EQ(txmap[2]->txid_.epoch, 5u);
            ASSERT_TRUE(write("a", "2"));
            ASSERT_TRUE(txmap[2]->validation());  // W's version is now pending
            ThManagementTable[9]->atomicStoreEpoch(now);
            begin("1");  // R
            ASSERT_EQ(txmap[1]->txid_.epoch, now);
            const bool read_ok = read("a", "1");
            EXPECT_EQ(read_ok, gap == 1)
                << "incremental=" << incremental << " gap=" << gap;
            if (read_ok) {
                auto it = txmap[1]->readSet_.find(row_key(table["a"]));
                ASSERT_NE(it, txmap[1]->readSet_.end());
                EXPECT_EQ(it->second, TxID())
                    << "R reads the committed version below W's pending one";
                const Graph& g = table["a"]->graph_;
                ASSERT_NE(g.find(txmap[1]->txid_), g.end());
                EXPECT_EQ(g.at(txmap[1]->txid_).writtenBy_.count(txmap[2]->txid_), 1u)
                    << "R -> W, the anti-dependency of the skipped version";
            }
            txmap[1]->abort();
            txmap[2]->abort();
            VersionRetirementRegistry::instance().drain_all();
        }
    }
}

// An update whose version was never installed is freed at once.
TEST_F(HistoryTest, NeverSplicedAbortedUpdateIsFreedImmediatelyNotQueued) {
    reinitialize(false);
    const uint64_t freed_before =
        VersionRetirementRegistry::counters().versions_freed.load();
    const uint64_t unspliced_before =
        VersionRetirementRegistry::counters().versions_retired_unspliced.load();
    const size_t pending_before = VersionRetirementRegistry::instance().pending();

    begin("2");
    ASSERT_TRUE(write("a", "2"));   // allocates new_ver_, never spliced in
    txmap[2]->abort();

    EXPECT_EQ(VersionRetirementRegistry::counters().versions_retired_unspliced.load(),
              unspliced_before + 1);
    EXPECT_EQ(VersionRetirementRegistry::counters().versions_freed.load(),
              freed_before + 1);
    EXPECT_EQ(VersionRetirementRegistry::instance().pending(), pending_before)
        << "an unspliced version must not sit in the deferred queue at all";
}

// A live older reader blocks the aborted version's freeing; once it ends and
// the floor passes the retirement epoch, reclaim() frees it.
TEST_F(HistoryTest,
      AbortedUpdateVersionSurvivesWhileOlderReaderLiveThenFreedAfterGrace) {
    reinitialize(false);
    for (int t = 0; t < NumVirtualThreads; ++t) ReclamationFloorAnnouncer::withdraw(t);
    for (int i = 0; i < NumVirtualThreads; ++i) ThManagementTable[i]->atomicStoreEpoch(1);
    // The global epoch starts ahead of every slot, so R's floor stays low.
    GlobalEpoch.obj_ = 5;

    // R begins and reads 'a' while every other slot lags; R stays live.
    begin("1");
    ASSERT_TRUE(read("a", "1"));
    const uint64_t reader_floor = txmap[1]->reclamation_epoch_;
    ASSERT_EQ(ReclamationFloorAnnouncer::min_live(), reader_floor);

    // Every other slot catches up; a new transaction's floor rises, R's does
    // not (R's own slot stays as its begin() stamped it).
    for (int i = 0; i < NumVirtualThreads; ++i) {
        if (i == 1) continue;  // R's own slot stays put; R is still live.
        ThManagementTable[i]->atomicStoreEpoch(5);
    }
    begin("2");
    ASSERT_GT(txmap[2]->reclamation_epoch_, reader_floor)
        << "the writer must retire at a strictly later epoch than R's floor "
           "for this test to prove anything";
    ASSERT_TRUE(write("a", "2"));
    splice_then_abort("a", "2");
    // The writer has no live transaction any more: withdraw its floor.
    ReclamationFloorAnnouncer::withdraw(2);
    ASSERT_EQ(VersionRetirementRegistry::instance().pending(), 1u);

    // Advance reclamation while R is still live: must NOT free.
    auto live_floor = [&]() {
        const uint64_t min_epoch = current_min_epoch();
        const uint64_t min_future_floor = min_epoch == 0 ? 0 : min_epoch - 1;
        return std::min(ReclamationFloorAnnouncer::min_live(), min_future_floor);
    };
    const uint64_t blocked_before =
        VersionRetirementRegistry::counters().blocked_by_floor.load();
    VersionRetirementRegistry::instance().reclaim(live_floor(), 256);
    EXPECT_EQ(VersionRetirementRegistry::instance().pending(), 1u)
        << "the aborted version must not be freed while R, which began "
           "before the retirement, is still live";
    EXPECT_GT(VersionRetirementRegistry::counters().blocked_by_floor.load(),
              blocked_before);

    // R commits and its slot's floor is withdrawn.
    ASSERT_TRUE(commit("1"));
    ReclamationFloorAnnouncer::withdraw(1);

    // Every slot passes the retirement epoch: now it is freed.
    for (int i = 0; i < NumVirtualThreads; ++i) ThManagementTable[i]->atomicStoreEpoch(20);
    GlobalEpoch.obj_ = 20;
    const uint64_t freed_before =
        VersionRetirementRegistry::counters().versions_freed.load();
    VersionRetirementRegistry::instance().reclaim(live_floor(), 256);
    EXPECT_EQ(VersionRetirementRegistry::instance().pending(), 0u)
        << "once every possible live reader has passed the retirement epoch, "
           "reclaim() must free the version";
    EXPECT_EQ(VersionRetirementRegistry::counters().versions_freed.load(),
              freed_before + 1);
}

// After the aborted version is freed, a new committed write to the key reads
// back its own content (no stale reference aliases a reused address).
TEST_F(HistoryTest, FreedVersionAddressCannotAliasItsReplacement) {
    reinitialize(false);
    VersionRetirementRegistry::instance().drain_all();
    begin("2");
    ASSERT_TRUE(write("a", "2"));
    splice_then_abort("a", "2");
    ASSERT_EQ(VersionRetirementRegistry::instance().pending(), 1u);
    // Frees at once (this test is about address reuse).
    VersionRetirementRegistry::instance().drain_all();
    ASSERT_EQ(VersionRetirementRegistry::instance().pending(), 0u);

    // A brand-new committed write to the same key, which may or may not land
    // on the just-freed address depending on the allocator.
    begin("3");
    ASSERT_TRUE(write("a", "3"));
    ASSERT_TRUE(commit("3"));

    Version* head = table["a"]->ldAcqLatest();
    EXPECT_EQ(head->status_.load(), VersionStatus::committed);
    EXPECT_EQ(head->txid_.thid, 3u)
        << "the replacement version must carry its OWN transaction's "
           "identity even if it reused the freed version's address -- no "
           "aliasing of the old (aborted) transaction's identity or content";
}

// Many aborted updates with periodic reclamation keep the queue bounded.
TEST_F(HistoryTest, ManyAbortedUpdatesKeepPendingRetentionBounded) {
    reinitialize(false);
    for (int t = 0; t < NumVirtualThreads; ++t) ReclamationFloorAnnouncer::withdraw(t);
    for (int i = 0; i < NumVirtualThreads; ++i) ThManagementTable[i]->atomicStoreEpoch(1);
    GlobalEpoch.obj_ = 1;
    VersionRetirementRegistry::instance().drain_all();

    constexpr int kIterations = 4000;
    size_t peak_seen = 0;
    for (int i = 0; i < kIterations; ++i) {
        begin("2");
        ASSERT_TRUE(write("a", "2"));
        splice_then_abort("a", "2");
        // Thread 2's transaction has ended; withdraw its stale floor.
        ReclamationFloorAnnouncer::withdraw(2);
        if (i % 50 == 0) {
            const uint64_t epoch = 2 + static_cast<uint64_t>(i);
            for (int t = 0; t < NumVirtualThreads; ++t)
                ThManagementTable[t]->atomicStoreEpoch(epoch);
            GlobalEpoch.obj_ = epoch;
            const uint64_t min_epoch = current_min_epoch();
            const uint64_t floor = std::min(ReclamationFloorAnnouncer::min_live(),
                                            min_epoch == 0 ? 0 : min_epoch - 1);
            VersionRetirementRegistry::instance().reclaim(floor, 256);
        }
        peak_seen = std::max(peak_seen, VersionRetirementRegistry::instance().pending());
    }
    // Final drain to reach a known steady state.
    ReclamationFloorAnnouncer::withdraw(2);
    for (int t = 0; t < NumVirtualThreads; ++t)
        ThManagementTable[t]->atomicStoreEpoch(2 + kIterations + 10);
    GlobalEpoch.obj_ = 2 + kIterations + 10;
    const uint64_t min_epoch = current_min_epoch();
    VersionRetirementRegistry::instance().reclaim(
        std::min(ReclamationFloorAnnouncer::min_live(), min_epoch - 1), 1u << 20);

    EXPECT_LT(peak_seen, static_cast<size_t>(kIterations))
        << "pending retention grew as if reclamation never ran at all";
    EXPECT_EQ(VersionRetirementRegistry::instance().pending(), 0u)
        << "after enough epoch advancement, pending retention must return to "
           "zero -- no permanent per-abort growth";
}

// An old live transaction blocks reclamation while it is live; once it ends,
// the queue drains.
TEST_F(HistoryTest, IndefinitelyOldLiveTransactionBlocksReclamationUntilItEnds) {
    reinitialize(false);
    for (int t = 0; t < NumVirtualThreads; ++t) ReclamationFloorAnnouncer::withdraw(t);
    for (int i = 0; i < NumVirtualThreads; ++i) ThManagementTable[i]->atomicStoreEpoch(1);
    GlobalEpoch.obj_ = 1;
    VersionRetirementRegistry::instance().drain_all();

    begin("1");
    ASSERT_TRUE(read("a", "1"));  // the long-running transaction; never ends
    const uint64_t old_floor = txmap[1]->reclamation_epoch_;

    auto live_floor = [&]() {
        const uint64_t min_epoch = current_min_epoch();
        const uint64_t min_future_floor = min_epoch == 0 ? 0 : min_epoch - 1;
        return std::min(ReclamationFloorAnnouncer::min_live(), min_future_floor);
    };

    for (int round = 0; round < 20; ++round) {
        const uint64_t epoch = 10 + static_cast<uint64_t>(round) * 5;
        for (int t = 0; t < NumVirtualThreads; ++t) {
            if (t == 1) continue;  // the long transaction's slot never moves
            ThManagementTable[t]->atomicStoreEpoch(epoch);
        }
        GlobalEpoch.obj_ = epoch;
        begin("2");
        ASSERT_TRUE(write("a", "2"));
        splice_then_abort("a", "2");
        VersionRetirementRegistry::instance().reclaim(live_floor(), 256);
        EXPECT_EQ(ReclamationFloorAnnouncer::min_live(), old_floor)
            << "the long transaction must still be the pinned minimum";
    }
    EXPECT_EQ(VersionRetirementRegistry::instance().pending(), 20u)
        << "every aborted update from this block must still be pending -- "
           "none of them can be reclaimed while the old transaction is live";

    // The long transaction finally ends.
    ASSERT_TRUE(commit("1"));
    ReclamationFloorAnnouncer::withdraw(1);
    // Thread 2 has no live transaction either; withdraw its stale floor.
    ReclamationFloorAnnouncer::withdraw(2);
    for (int t = 0; t < NumVirtualThreads; ++t) ThManagementTable[t]->atomicStoreEpoch(1000);
    GlobalEpoch.obj_ = 1000;
    VersionRetirementRegistry::instance().reclaim(live_floor(), 1u << 20);
    EXPECT_EQ(VersionRetirementRegistry::instance().pending(), 0u)
        << "once the long transaction ends and the grace period elapses, the "
           "whole backlog must drain";
}

// A committed update never enters this registry, and an aborted insert goes
// to RowRetirementRegistry.
TEST_F(HistoryTest, CommittedUpdateAndAbortedInsertAreUnaffectedByVersionRetirement) {
    reinitialize(false);
    const uint64_t ver_retired_before =
        VersionRetirementRegistry::counters().versions_retired.load();
    const uint64_t row_retired_before =
        RowRetirementRegistry::counters().retired_aborted_insert.load();

    // Normal committed UPDATE.
    begin("2");
    ASSERT_TRUE(write("a", "2"));
    ASSERT_TRUE(commit("2"));
    EXPECT_EQ(VersionRetirementRegistry::counters().versions_retired.load(),
              ver_retired_before)
        << "a committed update must never be retired through this registry";
    Version* head = table["a"]->ldAcqLatest();
    EXPECT_EQ(head->status_.load(), VersionStatus::committed);

    // Aborted INSERT of a brand-new key: still RowRetirementRegistry's job.
    begin("3");
    ASSERT_EQ(insert("z_new_insert_key", "3"), Status::OK);
    txmap[3]->abort();
    EXPECT_EQ(RowRetirementRegistry::counters().retired_aborted_insert.load(),
              row_retired_before + 1);
    EXPECT_EQ(VersionRetirementRegistry::counters().versions_retired.load(),
              ver_retired_before)
        << "an aborted INSERT must not be retired through the version-only "
           "registry -- RowRetirementRegistry owns the whole row+version pair";
}

// An aborted delete is retired like an aborted update.
TEST_F(HistoryTest, AbortedDeleteVersionIsAlsoRetiredThroughTheSameRegistry) {
    reinitialize(false);
    const uint64_t retired_before =
        VersionRetirementRegistry::counters().versions_retired.load();

    begin("2");
    TxExecutor* tx = txmap[2];
    Tuple* tuple = table["a"];
    ASSERT_EQ(tx->delete_record(Storage::TestTable, "a"), Status::OK);
    ASSERT_FALSE(tx->write_set_.empty());
    tuple->lock_.w_lock();
    TxSet followers;
    const Status stat = tx->insert_version(tx->write_set_.front(), followers);
    tuple->lock_.w_unlock();
    ASSERT_EQ(stat, Status::OK);
    tx->abort();

    EXPECT_EQ(VersionRetirementRegistry::counters().versions_retired.load(),
              retired_before + 1);
    Version* head = table["a"]->ldAcqLatest();
    EXPECT_EQ(head->status_.load(), VersionStatus::committed)
        << "the aborted delete-marker must be unlinked, leaving the original "
           "committed version reachable";
    VersionRetirementRegistry::instance().drain_all();
}

// Version retirement works in both representations.
TEST_F(HistoryTest, VersionRetirementWorksInBothLegacyAndIncrementalModes) {
    for (bool incremental : {false, true}) {
        reinitialize(incremental);
        for (int t = 0; t < NumVirtualThreads; ++t) ReclamationFloorAnnouncer::withdraw(t);
        VersionRetirementRegistry::instance().drain_all();
        for (int i = 0; i < NumVirtualThreads; ++i) ThManagementTable[i]->atomicStoreEpoch(1);
        GlobalEpoch.obj_ = 1;

        begin("2");
        ASSERT_TRUE(write("a", "2"));
        splice_then_abort("a", "2");
        EXPECT_EQ(VersionRetirementRegistry::instance().pending(), 1u)
            << "incremental=" << incremental;
        ReclamationFloorAnnouncer::withdraw(2);  // thid 2 has no live tx now

        for (int i = 0; i < NumVirtualThreads; ++i) ThManagementTable[i]->atomicStoreEpoch(50);
        GlobalEpoch.obj_ = 50;
        const uint64_t min_epoch = current_min_epoch();
        VersionRetirementRegistry::instance().reclaim(
            std::min(ReclamationFloorAnnouncer::min_live(), min_epoch - 1), 256);
        EXPECT_EQ(VersionRetirementRegistry::instance().pending(), 0u)
            << "incremental=" << incremental;
    }
}

// Outcome of ThreeTransactionReadOnlyAnomalyStyleG3Attempt.
namespace {
struct ThreeTxG3Outcome {
    bool w1_committed = false;
    bool w2_committed = false;
    bool r_committed = false;
    bool all_committed = false;
    uint64_t r_floor = 0;
    uint64_t w1_epoch = 0;
    uint64_t w2_epoch = 0;
};
}  // namespace

// A read-only anomaly in which R, the only possible cycle detector, has a floor above the
// epochs of W1 and W2: write validation's epoch guard aborts W1 first.
TEST_F(HistoryTest, ThreeTransactionReadOnlyAnomalyStyleG3Attempt) {
    {
        reinitialize(true);
        for (int t = 0; t < NumVirtualThreads; ++t) ReclamationFloorAnnouncer::withdraw(t);

        constexpr int kIdleSlot = 10;  // unused by thids 1/2/3 (R/W1/W2)
        for (int i = 0; i < NumVirtualThreads; ++i) ThManagementTable[i]->atomicStoreEpoch(1);
        GlobalEpoch.obj_ = 5;
        // Every slot, including the idle one, is high when R begins.
        for (int i = 0; i < NumVirtualThreads; ++i) ThManagementTable[i]->atomicStoreEpoch(5);

        ThreeTxG3Outcome out{};
        begin("1");  // R: slot[1] already 5; get_min_epoch()=5 -> epoch=5, floor=4.
        out.r_floor = txmap[1]->reclamation_epoch_;
        ASSERT_EQ(out.r_floor, 4u)
            << "R must actually get the inflated floor, or this schedule "
               "proves nothing";

        // R reads A's initial version -- BEFORE W1 ever writes it.
        ASSERT_TRUE(read("a", "1"));

        // Lower the idle slot: W1 and W2 get a low epoch; R's floor is fixed.
        ThManagementTable[kIdleSlot]->atomicStoreEpoch(1);

        begin("2");  // W1
        out.w1_epoch = txmap[2]->txid_.epoch;
        ASSERT_TRUE(write("a", "2"));
        ASSERT_TRUE(write("c", "2"));
        out.w1_committed = commit("2");

        begin("3");  // W2
        out.w2_epoch = txmap[3]->txid_.epoch;
        ASSERT_TRUE(read("c", "3"));
        ASSERT_TRUE(write("b", "3"));
        out.w2_committed = commit("3");

        // R reads B (W2's version) and validates.
        ASSERT_TRUE(read("b", "1"));
        out.r_committed = commit("1");
        out.all_committed = out.w1_committed && out.w2_committed && out.r_committed;

        report_outcome(
            "g3_3tx",
            {{"w1_committed", out.w1_committed},
             {"w2_committed", out.w2_committed},
             {"r_committed", out.r_committed},
             {"all_committed", out.all_committed}});
        std::cout << "[ g3-3tx   ] r_floor=" << out.r_floor
                  << " w1_epoch=" << out.w1_epoch
                  << " w2_epoch=" << out.w2_epoch
                  << " w1_committed=" << out.w1_committed
                  << " w2_committed=" << out.w2_committed
                  << " r_committed=" << out.r_committed
                  << " ALL_COMMITTED=" << out.all_committed << std::endl;

        // W1 aborts: write validation aborts a writer more than one epoch
        // behind a reader of the record (the epoch guard).
        EXPECT_FALSE(out.w1_committed)
            << "if this ever starts passing (W1 committing), the guard above "
               "no longer blocks this shape and the all_committed outcome "
               "below needs re-examining for a genuine committed-cycle "
               "witness -- do not just delete this assertion";
        EXPECT_FALSE(out.all_committed)
            << "this attempt did not produce a committed non-serializable "
               "history; see the comment above for exactly why";
    }
}

// Epoch quiescence tests; epochs are advanced by the real gate, never by
// sleeping.
extern bool chkEpochLoaded();

namespace {

// One evaluation of the leader's epoch gate (without the timer).
bool epoch_tick() {
    if (!chkEpochLoaded()) return false;
    atomicAddGE();
    return true;
}

// Runs `ticks` gate evaluations and returns how many actually advanced.
int epoch_ticks(int ticks) {
    int advanced = 0;
    for (int i = 0; i < ticks; ++i) if (epoch_tick()) ++advanced;
    return advanced;
}

}  // namespace

// An S4-style delete of a row a long transaction read: the history is imported by identity,
// and the delete's write fact is the reader's rw edge.
class DeletedRowValidationTest : public HistoryTest {
protected:
    void SetUp() override {
        HistoryTest::SetUp();
        reinitialize(true);
    }
};

// T2 reads a; T3 deletes a and commits; T2 then writes b.  Serial order
// T2 -> T3 exists, so T2 must commit -- and must not crash.
TEST_F(DeletedRowValidationTest, ReaderOfDeletedRowCommitsWhenSerializable) {
    Tuple* row_a = table["a"];
    begin("2");
    ASSERT_TRUE(read("a", "2"));
    begin("3");
    ASSERT_EQ(txmap[3]->delete_record(Storage::TestTable, "a"), Status::OK);
    ASSERT_TRUE(commit("3"));
    ASSERT_TRUE(row_a->logically_deleted());
    ASSERT_EQ(row_a->graph_state_ptr(), nullptr) << "precondition: history detached";
    ASSERT_TRUE(row_a->detached_graph_id().valid());
    ASSERT_TRUE(write("b", "2"));
    EXPECT_TRUE(commit("2"));
}

// Write skew through a delete: T2 -rw-> T3 (a), T3 -rw-> T2 (b). T2 can see the first edge only
// through the deleted row's history.
TEST_F(DeletedRowValidationTest, WriteSkewThroughDeletedRowIsDetected) {
    Tuple* row_a = table["a"];
    begin("2");
    ASSERT_TRUE(read("a", "2"));
    begin("3");
    ASSERT_TRUE(read("b", "3"));
    ASSERT_EQ(txmap[3]->delete_record(Storage::TestTable, "a"), Status::OK);
    ASSERT_TRUE(commit("3"));
    ASSERT_EQ(row_a->graph_state_ptr(), nullptr) << "precondition: history detached";
    ASSERT_TRUE(write("b", "2"));
    EXPECT_FALSE(commit("2")) << "non-serializable schedule committed";
}

// Read-only reader of a deleted row (the read-crown path).
TEST_F(DeletedRowValidationTest, ReadOnlyReaderOfDeletedRowDoesNotCrash) {
    begin("2");
    ASSERT_TRUE(read("a", "2"));
    ASSERT_TRUE(read("b", "2"));
    begin("3");
    ASSERT_EQ(txmap[3]->delete_record(Storage::TestTable, "a"), Status::OK);
    ASSERT_TRUE(commit("3"));
    EXPECT_TRUE(commit("2"));
}

class EpochQuiescenceTest : public HistoryTest {
protected:
    void TearDown() override {
        // Never leave a slot quiescent for the next test.
        for (int i = 0; i < NumVirtualThreads; ++i) {
            ThManagementTable[i]->atomicStoreParticipation(
                ParticipationState::Active);
            ReclamationFloorAnnouncer::withdraw(i);
        }
        HistoryTest::TearDown();
    }

    // Fresh, fully-active world: every slot stamped 1, GlobalEpoch 1, no
    // announced floor. Mirrors process startup.
    void fresh_world(bool incremental) {
        reinitialize(incremental);
        for (int i = 0; i < NumVirtualThreads; ++i) {
            ThManagementTable[i]->atomicStoreEpoch(1);
            ThManagementTable[i]->atomicStoreParticipation(
                ParticipationState::Active);
            ReclamationFloorAnnouncer::withdraw(i);
        }
        GlobalEpoch.obj_ = 1;
    }

    // A worker that runs one trivial read-only transaction, which is what
    // refreshes its stamp through the real begin() path.
    void run_one(int slot, const char* key) {
        const std::string id = std::to_string(slot);
        begin(id);
        ASSERT_TRUE(read(key, id));
        ASSERT_TRUE(commit(id));
    }
};

// The idle worker leaves the epoch set and constrains nothing.
TEST_F(EpochQuiescenceTest, IdleWorkerFromStartupDoesNotFreezeEpochWhenRepaired) {
    fresh_world(true);
    constexpr int kIdle = 7;
    constexpr int kBusyA = 1, kBusyB = 2;

    // A worker that has never begun anything is trivially empty.
    EXPECT_FALSE(txmap[kIdle]->holds_protected_state());
    EXPECT_TRUE(txmap[kIdle]->become_quiescent());
    // Every other slot in this fixture is a stand-in for "some worker" and must
    // not hold the epoch down either; quiesce all of them but the two busy ones.
    for (int i = 0; i < NumVirtualThreads; ++i) {
        if (i == kBusyA || i == kBusyB || i == kIdle) continue;
        EXPECT_TRUE(txmap[i]->become_quiescent());
    }

    for (int round = 0; round < 20; ++round) {
        run_one(kBusyA, "a");
        run_one(kBusyB, "b");
        epoch_ticks(4);
    }

    EXPECT_GT(GlobalEpoch.obj_, 2u) << "the epoch must now advance";
    EXPECT_GT(txmap[0]->get_min_epoch(), 1u);
    begin("1");
    EXPECT_GT(txmap[1]->reclamation_epoch_, 0u)
        << "a non-zero floor is the whole point of the repair";
    EXPECT_TRUE(commit("1"));
    // The quiescent slot's stamp is deliberately stale and that is harmless.
    EXPECT_EQ(ThManagementTable[kIdle]->atomicLoadEpoch(), 1u);
}

// Quiescence is allowed after a commit.
TEST_F(EpochQuiescenceTest, QuiescenceAllowedAfterCommit) {
    fresh_world(true);
    begin("3");
    ASSERT_TRUE(read("a", "3"));
    ASSERT_TRUE(write("b", "3"));
    ASSERT_TRUE(commit("3"));
    EXPECT_FALSE(txmap[3]->holds_protected_state())
        << "commit() must leave nothing behind that reclamation could free";
    EXPECT_TRUE(txmap[3]->become_quiescent());
    EXPECT_EQ(ThManagementTable[3]->atomicLoadParticipation(),
              ParticipationState::Quiescent);
}

// Quiescence is allowed after an abort.
TEST_F(EpochQuiescenceTest, QuiescenceAllowedAfterAbort) {
    fresh_world(true);
    begin("3");
    ASSERT_TRUE(read("a", "3"));
    ASSERT_TRUE(write("b", "3"));
    txmap[3]->abort();
    EXPECT_FALSE(txmap[3]->holds_protected_state())
        << "abort() must leave nothing behind either";
    EXPECT_TRUE(txmap[3]->become_quiescent());
}

// Quiescence is refused while the executor holds protected state.
TEST_F(EpochQuiescenceTest, QuiescenceRefusedWhileTransactionOpen) {
    fresh_world(true);
    begin("3");
    ASSERT_TRUE(read("a", "3"));
    EXPECT_TRUE(txmap[3]->holds_protected_state());
    EXPECT_FALSE(txmap[3]->become_quiescent());
    EXPECT_EQ(ThManagementTable[3]->atomicLoadParticipation(),
              ParticipationState::Active)
        << "a refused transition must leave the slot fully participating";
    ASSERT_TRUE(commit("3"));
}

// Rejoining refreshes the stamp before the slot counts again.
TEST_F(EpochQuiescenceTest, ResumeRefreshesStampBeforeRejoining) {
    fresh_world(true);
    constexpr int kIdle = 7;
    for (int i = 0; i < NumVirtualThreads; ++i) {
        if (i == 1) continue;
        ASSERT_TRUE(txmap[i]->become_quiescent());
    }
    // Let the epoch run well ahead of the quiescent slot's stale stamp.
    for (int r = 0; r < 12; ++r) { run_one(1, "a"); epoch_ticks(2); }
    const uint64_t ge = GlobalEpoch.obj_;
    ASSERT_GT(ge, 3u);
    EXPECT_EQ(ThManagementTable[kIdle]->atomicLoadEpoch(), 1u);

    const uint64_t before = txmap[0]->get_min_epoch();
    txmap[kIdle]->resume_active();
    const uint64_t after = txmap[0]->get_min_epoch();

    EXPECT_EQ(ThManagementTable[kIdle]->atomicLoadParticipation(),
              ParticipationState::Active);
    EXPECT_EQ(ThManagementTable[kIdle]->atomicLoadEpoch(), ge)
        << "the slot must rejoin stamped with the CURRENT epoch";
    EXPECT_GE(after, before)
        << "P-MONO: re-entry must never lower get_min_epoch()";
}

// get_min_epoch() is monotone across quiesce, resume, epoch advance and
// transactions.
TEST_F(EpochQuiescenceTest, MinEpochIsMonotoneAcrossQuiescenceTransitions) {
    fresh_world(true);
    uint64_t last = txmap[0]->get_min_epoch();
    // Fixed seed, so the schedule is identical on every run.
    std::mt19937 rng(20260905);
    const char* keys = "abcdefgh";
    for (int step = 0; step < 400; ++step) {
        const int slot = 1 + static_cast<int>(rng() % (NumVirtualThreads - 1));
        switch (rng() % 4) {
            case 0: txmap[slot]->become_quiescent(); break;
            case 1: txmap[slot]->resume_active(); break;
            case 2: epoch_ticks(1); break;
            default: {
                txmap[slot]->resume_active();
                const std::string id = std::to_string(slot);
                begin(id);
                (void)read(std::string(1, keys[rng() % 8]), id);
                if (!commit(id)) { /* aborts are part of the schedule */ }
                break;
            }
        }
        const uint64_t now = txmap[0]->get_min_epoch();
        ASSERT_GE(now, last)
            << "P-MONO violated at step " << step << ": " << last << " -> " << now;
        last = now;
    }
}

// All workers idle, then new work arrives.
TEST_F(EpochQuiescenceTest, AllWorkersIdleThenNewWork) {
    fresh_world(true);
    for (int r = 0; r < 5; ++r) { run_one(1, "a"); epoch_ticks(2); }
    for (int i = 0; i < NumVirtualThreads; ++i) {
        ASSERT_TRUE(txmap[i]->become_quiescent()) << "slot " << i;
    }
    const uint64_t ge = GlobalEpoch.obj_;
    // With no ACTIVE slot the answer must be the current GlobalEpoch, NOT
    // UINT64_MAX: the next transaction takes its txid_.epoch from this.
    EXPECT_EQ(txmap[0]->get_min_epoch(), ge);
    EXPECT_NE(txmap[0]->get_min_epoch(), std::numeric_limits<uint64_t>::max());
    // The gate is trivially satisfied, but nothing calls it while every worker
    // waits, so this is a benign stall rather than an unbounded advance.
    EXPECT_TRUE(chkEpochLoaded());

    // New work arrives on one slot.
    txmap[2]->resume_active();
    begin("2");
    EXPECT_GE(txmap[2]->txid_.epoch, ge)
        << "a transaction that starts after an all-idle interval must not get "
           "an epoch below the floor that interval permitted";
    EXPECT_LT(txmap[2]->reclamation_epoch_, txmap[2]->txid_.epoch);
    EXPECT_TRUE(commit("2"));
}

// An idle worker does not let reclamation pass an active long transaction.
TEST_F(EpochQuiescenceTest, IdleWorkerDoesNotLetTheFloorPassAnActiveLongTransaction) {
    fresh_world(true);
    constexpr int kLong = 1, kShort = 2;
    for (int i = 0; i < NumVirtualThreads; ++i) {
        if (i == kLong || i == kShort) continue;
        ASSERT_TRUE(txmap[i]->become_quiescent());
    }
    for (int r = 0; r < 4; ++r) { run_one(kShort, "a"); epoch_ticks(2); }

    // The long transaction begins and STAYS OPEN.
    begin("1");
    ASSERT_TRUE(read("a", "1"));
    const uint64_t long_stamp = ThManagementTable[kLong]->atomicLoadEpoch();
    const uint64_t long_epoch = txmap[kLong]->txid_.epoch;

    // Short transactions keep running and the epoch keeps ticking around it.
    for (int r = 0; r < 20; ++r) { run_one(kShort, "b"); epoch_ticks(3); }

    EXPECT_EQ(ThManagementTable[kLong]->atomicLoadParticipation(),
              ParticipationState::Active)
        << "a worker inside a transaction is never quiescent";
    EXPECT_EQ(txmap[0]->get_min_epoch(), long_stamp)
        << "the live long transaction must still pin get_min_epoch()";
    // Every later transaction's floor must stay at or below the long
    // transaction's own epoch, which is what stops its facts being retired.
    begin("3");
    EXPECT_LE(txmap[3]->reclamation_epoch_, long_epoch)
        << "a later transaction could retire the live long transaction's facts";
    EXPECT_TRUE(commit("3"));
    EXPECT_TRUE(commit("1"));
}

// Facts reachable only through a prefix reference survive while the
// referencing transaction is live.
TEST_F(EpochQuiescenceTest, IncludeRecordOnlyFactsSurviveTheRepairedFloor) {
    fresh_world(true);
    constexpr int kLong = 1, kShort = 2;
    for (int i = 0; i < NumVirtualThreads; ++i) {
        if (i == kLong || i == kShort) continue;
        ASSERT_TRUE(txmap[i]->become_quiescent());
    }
    // Facts on "c" that only "b"'s log will cite.
    begin("4"); ASSERT_TRUE(read("c", "4")); ASSERT_TRUE(write("c", "4"));
    ASSERT_TRUE(commit("4"));
    const uint64_t c_committed = table["c"]->graph_log().committed();
    ASSERT_GT(c_committed, 0u);

    // b cites c.
    txmap[kShort]->begin();
    txmap[kShort]->append_include(table["b"], table["c"], c_committed);
    ASSERT_TRUE(commit(std::to_string(kShort)));

    // A long transaction opens now and must keep everything reachable.
    begin("1");
    ASSERT_TRUE(read("b", "1"));
    for (int r = 0; r < 25; ++r) { run_one(kShort, "a"); epoch_ticks(3); }

    // The citation and its target are still there: retirement never passed the
    // live long transaction's epoch.
    EXPECT_LE(table["c"]->graph_log().retired_seq_.load(std::memory_order_acquire),
              c_committed)
        << "retirement ran past the cited prefix";
    RecordGraphState* cited =
        RecordGraphRegistry::instance().try_pin(table["c"]->graph_id());
    EXPECT_NE(cited, nullptr) << "the cited record state became unresolvable";
    if (cited != nullptr) {
        EXPECT_EQ(cited->committed(), c_committed);
        RecordGraphRegistry::instance().unpin(table["c"]->graph_id());
    }
    EXPECT_TRUE(commit("1"));
}

// TxIDs stay unique and ordered per worker across quiescence.
TEST_F(EpochQuiescenceTest, TxIdRemainsUniqueAndOrderedAcrossReentry) {
    fresh_world(true);
    std::set<uint64_t> seen;
    std::map<int, std::pair<uint64_t, uint64_t>> last;  // slot -> (epoch, tid)
    for (int i = 3; i < NumVirtualThreads; ++i) {
        ASSERT_TRUE(txmap[i]->become_quiescent());
    }
    for (int round = 0; round < 30; ++round) {
        for (int slot : {1, 2}) {
            txmap[slot]->resume_active();
            const std::string id = std::to_string(slot);
            begin(id);
            const TxID t = txmap[slot]->txid_;
            EXPECT_TRUE(seen.insert(t.obj_).second)
                << "duplicate TxID at round " << round << " slot " << slot;
            auto it = last.find(slot);
            if (it != last.end()) {
                const bool ordered = (t.epoch > it->second.first) ||
                    (t.epoch == it->second.first && t.tid > it->second.second);
                EXPECT_TRUE(ordered)
                    << "per-worker TxID order broke at round " << round;
            }
            last[slot] = {t.epoch, t.tid};
            ASSERT_TRUE(commit(id));
            ASSERT_TRUE(txmap[slot]->become_quiescent());
        }
        epoch_ticks(2);
    }
}

// Reclamation progresses, and a reclaimed RecordId does not resolve to a
// recycled slot.
TEST_F(EpochQuiescenceTest, DetachedStateIsReclaimedOnlyWhenTheRepairedFloorAllows) {
    fresh_world(true);
    // Slot 1 is the only worker; every other slot must be quiescent.
    for (int i = 0; i < NumVirtualThreads; ++i) {
        if (i == 1) continue;
        ASSERT_TRUE(txmap[i]->become_quiescent()) << "slot " << i;
    }
    RecordGraphRegistry::instance().drain_for_test(
        std::numeric_limits<uint64_t>::max() - 2);

    // A detached state that nothing cites.
    RecordId doomed;
    {
        Tuple* t = new Tuple();
        doomed = t->graph_id();
        delete t;                       // detaches, does not destroy the state
    }
    ASSERT_NE(RecordGraphRegistry::instance().try_pin(doomed), nullptr);
    RecordGraphRegistry::instance().unpin(doomed);

    for (int r = 0; r < 10; ++r) { run_one(1, "a"); epoch_ticks(2); }
    const uint64_t min_epoch = txmap[0]->get_min_epoch();
    ASSERT_GT(min_epoch, 1u) << "the repaired floor must have advanced";
    RecordGraphRegistry::instance().reclaim(GlobalEpoch.obj_, min_epoch - 1, 1024);
    RecordGraphRegistry::instance().reclaim(GlobalEpoch.obj_ + 1, min_epoch - 1, 1024);

    EXPECT_EQ(RecordGraphRegistry::instance().try_pin(doomed), nullptr)
        << "a reclaimed RecordId must not resolve, even if its slot is reused";
}

// A retry keeps the worker active, and shutdown leaves no slot quiescent.
TEST_F(EpochQuiescenceTest, RetryStaysActiveAndShutdownLeavesNoQuiescentSlot) {
    fresh_world(true);
    for (int i = 3; i < NumVirtualThreads; ++i) {
        ASSERT_TRUE(txmap[i]->become_quiescent());
    }
    // Conflict: 2 writes "a" and commits; 1 read the pre-image and must abort,
    // then retries. Throughout the retry the slot stays ACTIVE.
    begin("1");
    ASSERT_TRUE(read("a", "1"));
    begin("2");
    ASSERT_TRUE(write("a", "2"));
    ASSERT_TRUE(commit("2"));
    ASSERT_TRUE(write("a", "1"));
    const bool committed = commit("1");
    EXPECT_EQ(ThManagementTable[1]->atomicLoadParticipation(),
              ParticipationState::Active);
    if (!committed) {
        // The retry: same worker, still active, new transaction.
        begin("1");
        EXPECT_EQ(ThManagementTable[1]->atomicLoadParticipation(),
                  ParticipationState::Active);
        ASSERT_TRUE(read("a", "1"));
        (void)commit("1");
    }
    // Shutdown discipline: every slot the workload owns is resumed before the
    // thread returns, so teardown never reads a half-transitioned world.
    for (int i = 0; i < NumVirtualThreads; ++i) txmap[i]->resume_active();
    for (int i = 0; i < NumVirtualThreads; ++i) {
        EXPECT_EQ(ThManagementTable[i]->atomicLoadParticipation(),
                  ParticipationState::Active) << "slot " << i;
    }
}

