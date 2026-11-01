// Based on atomic_wrapper.hpp of https://github.com/starpos/oltp-cc-bench
// (thanks to Mr. Hoshino).

#pragma once

template<typename T>
static T loadAcquire(T &ptr) {
  return __atomic_load_n(&ptr, __ATOMIC_ACQUIRE);
}

template<typename T, typename T2>
static void storeRelease(T &ptr, T2 val) {
  __atomic_store_n(&ptr, (T) val, __ATOMIC_RELEASE);  // NOLINT
}
