#pragma once

#include <atomic>
#include <cstdint>
#include <map>

#include "../../include/cache_line_size.hh"

#include <gflags/gflags.h>

#include "oze.hh"
#include "version.hh"

DECLARE_bool(reuse_fresh_graph_state);

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

  // Oze's record graph. Saros also uses it during reads, for the version
  // choice; validation uses the history instead.
  Graph graph_;

  // Saros's history of this record, owned by RecordGraphRegistry. Deleting or
  // reinitializing the row detaches it (see logically_delete()).
  RecordGraphState* graph_state_ = nullptr;

  // Unused; keeps sizeof(Tuple) at the measured 256 bytes.
  alignas(8) unsigned char reserved_[48] = {};

  // The floor of the last gc() of graph_ by Saros's validation, which skips a
  // gc() at an unchanged floor. Guarded by the latch.
  uint64_t graph_gc_epoch_ = 0;
  bool graph_gc_ran_ = false;

  Tuple() : latest_(nullptr) {
    graph_state_ = RecordGraphRegistry::instance().create(this);
  }

  // Detaches the history (references may still reach it). Reads nothing else:
  // the row's versions may already be freed.
  ~Tuple() {
    if (graph_state_ != nullptr) {
      RecordGraphRegistry::instance().detach(graph_state_, 0);
      graph_state_ = nullptr;
    }
  }

  Tuple(const Tuple&) = delete;
  Tuple& operator=(const Tuple&) = delete;

  // The identity of the record's history; stored facts name records by it.
  RecordId graph_id() const {
    const RecordGraphState* state = graph_state_ptr();
    if (state != nullptr) return state->id_;
    return logically_deleted() ? detached_id_ : RecordId();
  }

  // nullptr once the row is logically deleted: its history must then be reached
  // by identity (detached_graph_id()) under a pin. Load it once per use.
  RecordGraphState* graph_state_ptr() const {
    return __atomic_load_n(&graph_state_, __ATOMIC_ACQUIRE);
  }
  // Valid once graph_state_ptr() has returned nullptr for a deleted row.
  RecordId detached_graph_id() const { return detached_id_; }

  RecordGraphState& graph_log() { return *graph_state_; }
  const RecordGraphState& graph_log() const { return *graph_state_; }

  Version *ldAcqLatest() { return latest_.load(std::memory_order_acquire); }

  void init(TxID txid, TupleBody&& body) {
    const bool fresh = latest_.load(std::memory_order_acquire) == nullptr;

    latest_.store(new Version(), std::memory_order_release);
    (latest_.load(std::memory_order_acquire))
            ->set(txid, VERSION_TERMINATION, VersionStatus::pending);
    (latest_.load(std::memory_order_acquire))->body_ = std::move(body);
    body_ = std::ref((latest_.load(std::memory_order_acquire))->body_);

    graph_.~Graph();
    Graph* g = ::new(&graph_) Graph;
    g->emplace(txid, TxNode(txid, TransactionStatus::validating));
    // A reinitialized row gets a new history and identity. With
    // --reuse_fresh_graph_state a new row keeps the one Tuple() created.
    if (!(fresh && FLAGS_reuse_fresh_graph_state && graph_state_ != nullptr))
      reset_graph_state(txid.epoch);
    graph_gc_epoch_ = 0;
    graph_gc_ran_ = false;
  }

  // Initial data load.
  void init([[maybe_unused]] size_t thid, TupleBody&& body, void* param) {
    const bool fresh = latest_.load(std::memory_order_acquire) == nullptr;

    TxID txid = TxID();
    txid.thid = thid;
    latest_.store(new Version(), std::memory_order_release);
    (latest_.load(std::memory_order_acquire))
            ->set(txid, nullptr, VersionStatus::committed);
    (latest_.load(std::memory_order_acquire))->body_ = std::move(body);
    body_ = std::ref((latest_.load(std::memory_order_acquire))->body_);

    graph_.~Graph();
    Graph* g = ::new(&graph_) Graph;
    g->emplace(txid, TxNode(txid, TransactionStatus::validating));
    if (!(fresh && FLAGS_reuse_fresh_graph_state && graph_state_ != nullptr))
      reset_graph_state(txid.epoch);
    graph_gc_epoch_ = 0;
    graph_gc_ran_ = false;
  }

  // Marks a deleted row and detaches its history, so that identity no longer
  // resolves to it; RowRetirementRegistry frees the object later.
  void logically_delete() {
    if (logically_deleted_.exchange(true, std::memory_order_acq_rel)) return;
    if (graph_state_ != nullptr) {
      // The identity is set before the pointer is cleared (release).
      detached_id_ = graph_state_->id_;
      RecordGraphState* state = graph_state_;
      __atomic_store_n(&graph_state_, static_cast<RecordGraphState*>(nullptr),
                       __ATOMIC_RELEASE);
      RecordGraphRegistry::instance().detach(state, 0);
    }
  }

  bool logically_deleted() const {
    return logically_deleted_.load(std::memory_order_acquire);
  }

private:
  std::atomic<bool> logically_deleted_{false};
  RecordId detached_id_;

  void reset_graph_state(uint64_t epoch) {
    RecordGraphRegistry& registry = RecordGraphRegistry::instance();
    if (graph_state_ != nullptr) registry.detach(graph_state_, epoch);
    graph_state_ = registry.create(this);
  }
};

static_assert(sizeof(Tuple) == 256, "keeps the measured row size");

// The key under which read and write sets name a row.
inline RowKey row_key(const Tuple* tuple) {
    if (tuple == nullptr) return RowKey();
    return RowKey(tuple->graph_id().bits());
}

// Key -> live row, or nullptr once the row is deleted or its history reclaimed.
inline Tuple* row_key_ptr(RowKey key) {
    if (!key.valid()) return nullptr;
    const RecordId id(static_cast<uint32_t>(key.bits_ & 0xffffffffU),
                      static_cast<uint32_t>(key.bits_ >> 32));
    return RecordGraphRegistry::instance().live_owner(id);
}

inline RecordId row_key_record(RowKey key) {
    if (!key.valid()) return RecordId();
    return RecordId(static_cast<uint32_t>(key.bits_ & 0xffffffffU),
                    static_cast<uint32_t>(key.bits_ >> 32));
}
