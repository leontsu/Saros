#pragma once

#include <cstdio>
#include <cstdlib>
#include <memory>
#include <vector>

#include "include/common.hh"
#include "common/engine/result.hh"
#include "include/transaction.hh"
#include "include/tuple.hh"
#include "common/engine/util.hh"

#include "common/tuple_body.hh"
#include "gtest/gtest.h"

#define NumVirtualThreads 16
#define ClocksPerUsec 2100

enum class Storage : std::uint32_t {
  TestTable = 0,
};

struct Data {
  alignas(CACHE_LINE_SIZE)
  std::uint64_t id_;
  char val_[1];

  static void CreateKey(uint64_t id, char *out) {
    assign_as_bigendian(id, &out[0]);
  }
};

// Frees the version chain of a test-owned row, after every executor is destroyed.
inline void release_version_chain(Tuple* tuple) {
    if (tuple == nullptr) return;
    Version* v = tuple->latest_.load(std::memory_order_acquire);
    tuple->latest_.store(nullptr, std::memory_order_release);
    while (v != nullptr && v != VERSION_TERMINATION) {
        Version* next = v->next_.load(std::memory_order_acquire);
        delete v;
        v = next;
    }
}

// At exit, one stderr line with the chunk-freeing counters.
struct HistoryChunkRegistryExitReport {
    ~HistoryChunkRegistryExitReport() {
        const auto& c = ChunkRetirementRegistry::counters();
        auto ld = [](const std::atomic<uint64_t>& v) {
            return static_cast<unsigned long long>(v.load());
        };
        if (ld(c.chunks_unlinked) == 0 && ld(c.dirs_retired) == 0) return;
        std::fprintf(stderr,
                     "#history_chunk_registry unlinked=%llu freed=%llu "
                     "dirs_retired=%llu dirs_freed=%llu drained=%llu "
                     "reclaim_passes=%llu blocked_by_floor=%llu clamps=%llu "
                     "fallbacks=%llu relocations=%llu pending=%llu\n",
                     ld(c.chunks_unlinked), ld(c.chunks_freed),
                     ld(c.dirs_retired), ld(c.dirs_freed),
                     ld(c.drained_at_shutdown), ld(c.reclaim_passes),
                     ld(c.blocked_by_floor), ld(c.reader_clamps),
                     ld(c.fallback_walks), ld(c.cursor_relocations),
                     ld(c.pending_entries));
    }
};
inline HistoryChunkRegistryExitReport history_chunk_registry_exit_report;

// The test fixture of the Saros engine: 16 executors and the rows 'a'..'z' of one table.
class TestCase : public ::testing::Test {
protected:
    virtual void SetUp() {
        // OZE_TEST_HISTORY_DUMP_PATH: the history dump of this test process.
        saved_history_dump_path_ = FLAGS_history_dump_path;
        if (const char* path = std::getenv("OZE_TEST_HISTORY_DUMP_PATH")) {
            FLAGS_history_dump_path = path;
        }
        TotalThreadNum = NumVirtualThreads;
        owned_th_table_.reset(new ThreadManagementEntry*[TotalThreadNum]);
        ThManagementTable = owned_th_table_.get();
        owned_scan_history_.reset(new std::atomic<ScanEntry*>[2]());
        ScanHistory = owned_scan_history_.get();
        owned_scan_range_.reset(new std::atomic<uint64_t>[2]());
        ScanRange = owned_scan_range_.get();
        owned_tx_.reserve(TotalThreadNum);
        owned_th_.reserve(TotalThreadNum);
        for (int i = 0; i < TotalThreadNum; ++i) {
            owned_tx_.emplace_back(
                new TxExecutor(i, backoff_, &res_[i], std::ref(quit_)));
            txmap.emplace(i, owned_tx_.back().get());
            owned_th_.emplace_back(
                new ThreadManagementEntry(1));
            ThManagementTable[i] = owned_th_.back().get();
        }
        owned_tuples_.reserve(26);
        for (uint64_t i = (uint64_t)'a'; i <= (uint64_t)'z'; ++i) {
            std::string s{(char)i};
            owned_tuples_.emplace_back(new Tuple());
            table.emplace(s, owned_tuples_.back().get());
        }
        TxID txid = TxID();
        txid.thid = 0;
        for (auto& [key, tuple] : table) {
            HeapObject obj;
            obj.allocate<Data>();
            tuple->init(0, TupleBody(key, std::move(obj)), nullptr);
            for (auto& [k, t] : table) {
                tuple->graph_.at(txid).writeSet_.emplace(row_key(t));
            }
            Masstrees[get_storage(Storage::TestTable)].insert_value(key, tuple);
        }
        GlobalEpoch.obj_ = 1;
        apply_test_env();
    }

    // Environment overrides, because gtest_main does not parse flags.
    static void apply_test_env() {
        if (const char* io =
                std::getenv("OZE_TEST_VALIDATE_INSERT_ONLY_READS")) {
            FLAGS_validate_insert_only_reads = std::string(io) == "1";
        }
        if (const char* f =
                std::getenv("OZE_TEST_FREE_RETIRED_HISTORY_CHUNKS")) {
            FLAGS_free_retired_history_chunks = std::string(f) == "1";
        }
        if (const char* t = std::getenv("OZE_TEST_RETIREMENT_THRESHOLD")) {
            FLAGS_retirement_threshold =
                std::strtoull(t, nullptr, 10);
        }
    }

    // Destroys the fixture in a fixed order; the executors write the history dump here.
    virtual void TearDown() {
        Masstrees[get_storage(Storage::TestTable)].table_init();
        txmap.clear();
        table.clear();
        owned_tx_.clear();
        FLAGS_history_dump_path = saved_history_dump_path_;
        for (auto& tuple : owned_tuples_) {
            release_version_chain(tuple.get());
        }
        owned_tuples_.clear();
        ChunkRetirementRegistry::instance().drain_all();
        if (ScanHistory != nullptr) {
            for (int i = 0; i < 2; ++i) {
                ScanEntry* e =
                    ScanHistory[i].exchange(nullptr, std::memory_order_acq_rel);
                while (e != nullptr) {
                    ScanEntry* next = e->ldAcqNext();
                    delete e;
                    e = next;
                }
            }
        }
        owned_th_.clear();
        ThManagementTable = nullptr;
        owned_th_table_.reset();
        ScanHistory = nullptr;
        owned_scan_history_.reset();
        ScanRange = nullptr;
        owned_scan_range_.reset();
    }

    void clear() {
        TearDown();
        SetUp();
    }

    // Operations of transaction `txid` (an executor number).
    void begin(std::string txid) {
        txmap[std::stoi(txid)]->begin();
    }

    bool read(std::string key, std::string txid) {
        auto tx = txmap[std::stoi(txid)];
        tx->read(Storage::TestTable, key, &dummy_);
        if (tx->status_ == TransactionStatus::aborted)
            return false;
        else
            return true;
    }

    bool write(std::string key, std::string txid) {
        auto tx = txmap[std::stoi(txid)];
        HeapObject obj;
        obj.allocate<Data>();
        tx->write(Storage::TestTable, key, TupleBody(key, std::move(obj)));
        if (tx->status_ == TransactionStatus::aborted)
            return false;
        else
            return true;
    }

    Status insert(std::string key, std::string txid) {
        auto tx = txmap[std::stoi(txid)];
        HeapObject obj;
        obj.allocate<Data>();
        return tx->insert(Storage::TestTable, key, TupleBody(key, std::move(obj)));
    }

    bool commit(std::string txid) {
        TxExecutor* tx = txmap[std::stoi(txid)];
        if (!tx->commit()) {
            tx->abort();
            return false;
        }
        return true;
    }

    // Non-owning views.
    std::map<int,TxExecutor*> txmap;
    std::map<std::string,Tuple*> table;
    TupleBody* dummy_;
    Backoff backoff_ = Backoff(ClocksPerUsec);
    Result res_[NumVirtualThreads];
    bool quit_ = false;
    std::string saved_history_dump_path_;

    // The owners, declared last so that they are destroyed after the views.
    std::vector<std::unique_ptr<TxExecutor>> owned_tx_;
    std::vector<std::unique_ptr<Tuple>> owned_tuples_;
    std::vector<std::unique_ptr<ThreadManagementEntry>> owned_th_;
    std::unique_ptr<ThreadManagementEntry*[]> owned_th_table_;
    std::unique_ptr<std::atomic<ScanEntry*>[]> owned_scan_history_;
    std::unique_ptr<std::atomic<uint64_t>[]> owned_scan_range_;
};
