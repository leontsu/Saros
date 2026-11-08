#pragma once

#include <atomic>
#include <cstdint>
#include <map>

#include "common/cache_line_size.hh"

#include "oze.hh"
#include "common/engine/version.hh"

// Ends a version chain ("no older version"); nullptr means no readable version.
#define VERSION_TERMINATION (Version*)0x1

using namespace std;

struct TupleId {
  union {
    uint64_t obj_;
    struct {
      uint64_t tid: 32;
      uint64_t epoch: 32;
    };
  };

  TupleId() { obj_ = 0; }

  bool operator==(const TupleId &right) const { return obj_ == right.obj_; }

  bool operator!=(const TupleId &right) const { return !operator==(right); }

  bool operator<(const TupleId &right) const { return this->obj_ < right.obj_; }
};

class Tuple {
public:
  alignas(CACHE_LINE_SIZE) TupleId tuple_id_;
  RWLock lock_;
  atomic<Version *> latest_;
  TupleBody body_; // only used for index tuple as single version

  Graph graph_;
  // Unused; keeps sizeof(Tuple) at the measured size.
  alignas(8) unsigned char reserved_[48] = {};

  Tuple() : latest_(nullptr) {}

  Version *ldAcqLatest() { return latest_.load(std::memory_order_acquire); }

  void init(TxID txid, TupleBody&& body) {
    latest_.store(new Version(), std::memory_order_release);
    (latest_.load(std::memory_order_acquire))
            ->set(txid, VERSION_TERMINATION, VersionStatus::pending);
    (latest_.load(std::memory_order_acquire))->body_ = std::move(body);
    body_ = std::ref((latest_.load(std::memory_order_acquire))->body_);

    Graph* g = ::new(&graph_) Graph;
    g->emplace(txid, TxNode(txid, TransactionStatus::validating));
  }

  // only for initial data preparation
  void init([[maybe_unused]] size_t thid, TupleBody&& body, void* param) {
    TxID txid = TxID();
    txid.thid = thid;
    latest_.store(new Version(), std::memory_order_release);
    (latest_.load(std::memory_order_acquire))
            ->set(txid, nullptr, VersionStatus::committed);
    (latest_.load(std::memory_order_acquire))->body_ = std::move(body);
    body_ = std::ref((latest_.load(std::memory_order_acquire))->body_);

    Graph* g = ::new(&graph_) Graph;
    g->emplace(txid, TxNode(txid, TransactionStatus::validating));
  }
};
