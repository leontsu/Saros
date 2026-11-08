#pragma once

#include <atomic>

using namespace std;

// Spin lock; counter == -1 when locked, 0 when free.
class RWLock {
public:
  std::atomic<int> counter;

  RWLock() { counter.store(0, std::memory_order_release); }

  void w_lock() {
    int expected;
    for (;;) {
      expected = counter.load(memory_order_acquire);
RETRY_W_LOCK:
      if (expected != 0) continue;
      if (counter.compare_exchange_strong(expected, -1, memory_order_acq_rel,
                                          memory_order_acquire))
        break;
      else
        goto RETRY_W_LOCK;
    }
  }

  void w_unlock() { counter++; }
};
