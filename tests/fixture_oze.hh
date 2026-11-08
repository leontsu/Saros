#pragma once

#include <map>
#include <memory>
#include <string>
#include <vector>

#include "include/common.hh"
#include "include/result.hh"
#include "include/transaction.hh"
#include "include/tuple.hh"
#include "include/util.hh"

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
};

// The test fixture of the Oze engine: 16 executors and the rows 'a'..'z' of one table.
class TestCase : public ::testing::Test {
protected:
    virtual void SetUp() {
        TotalThreadNum = NumVirtualThreads;
        owned_th_table_.reset(new ThreadManagementEntry*[TotalThreadNum]);
        ThManagementTable = owned_th_table_.get();
        owned_scan_history_.reset(new std::atomic<ScanEntry*>[2]());
        ScanHistory = owned_scan_history_.get();
        owned_scan_range_.reset(new std::atomic<uint64_t>[2]());
        ScanRange = owned_scan_range_.get();
        for (int i = 0; i < NumVirtualThreads; ++i) {
            owned_tx_.emplace_back(new TxExecutor(i, backoff_, &res_[i], std::ref(quit_)));
            txmap.emplace(i, owned_tx_.back().get());
            owned_th_.emplace_back(new ThreadManagementEntry(1));
            ThManagementTable[i] = owned_th_.back().get();
        }
        for (uint64_t i = (uint64_t)'a'; i <= (uint64_t)'z'; ++i) {
            std::string s{(char)i};
            owned_tuples_.emplace_back(new Tuple());
            table.emplace(s, owned_tuples_.back().get());
        }
        for (auto& [key, tuple] : table) {
            HeapObject obj;
            obj.allocate<Data>();
            tuple->init(0, TupleBody(key, std::move(obj)), nullptr);
            Masstrees[get_storage(Storage::TestTable)].insert_value(key, tuple);
        }
        GlobalEpoch.obj_ = 1;
    }

    virtual void TearDown() {
        Masstrees[get_storage(Storage::TestTable)].table_init();
        txmap.clear();
        table.clear();
        owned_tx_.clear();
        owned_tuples_.clear();
        owned_th_.clear();
        ThManagementTable = nullptr;
        ScanHistory = nullptr;
        ScanRange = nullptr;
    }

    void begin(std::string txid) {
        txmap[std::stoi(txid)]->begin();
    }

    bool read(std::string key, std::string txid) {
        auto tx = txmap[std::stoi(txid)];
        tx->read(Storage::TestTable, key, &dummy_);
        return tx->status_ != TransactionStatus::aborted;
    }

    bool write(std::string key, std::string txid) {
        auto tx = txmap[std::stoi(txid)];
        HeapObject obj;
        obj.allocate<Data>();
        tx->write(Storage::TestTable, key, TupleBody(key, std::move(obj)));
        return tx->status_ != TransactionStatus::aborted;
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

    std::map<int,TxExecutor*> txmap;
    std::map<std::string,Tuple*> table;
    TupleBody* dummy_;
    Backoff backoff_ = Backoff(ClocksPerUsec);
    Result res_[NumVirtualThreads];
    bool quit_ = false;

    std::vector<std::unique_ptr<TxExecutor>> owned_tx_;
    std::vector<std::unique_ptr<Tuple>> owned_tuples_;
    std::vector<std::unique_ptr<ThreadManagementEntry>> owned_th_;
    std::unique_ptr<ThreadManagementEntry*[]> owned_th_table_;
    std::unique_ptr<std::atomic<ScanEntry*>[]> owned_scan_history_;
    std::unique_ptr<std::atomic<uint64_t>[]> owned_scan_range_;
};
