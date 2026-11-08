#pragma once

#include <atomic>
#include <cstdint>

#include "common/cache_line_size.hh"
#include "common/engine/transaction_status.hh"

class ThreadManagementEntry {
  public:
  explicit ThreadManagementEntry(uint64_t epoch) {
    this->epoch_.store(epoch, std::memory_order_relaxed);
  }

  void atomicStoreEpoch(uint64_t epoch) {
    this->epoch_.store(epoch, std::memory_order_release);
  }

  uint64_t atomicLoadEpoch() {
    return this->epoch_.load(std::memory_order_acquire);
  }

  private:
  alignas(CACHE_LINE_SIZE) std::atomic<uint64_t> epoch_;
};
