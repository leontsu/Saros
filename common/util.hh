#pragma once

#include <stdio.h>
#include <string.h>
#include <sys/resource.h>
#include <unistd.h>

#include <atomic>
#include <cassert>
#include <cstdint>
#include <cstring>
#include <string>
#include <string_view>
#include <thread>
#include <vector>

#include "cpu_pause.hh"
#include "debug.hh"
#include "random.hh"
#include "result.hh"
#include "tsc.hh"

// Largest thread count up to the core count that divides tuple_num evenly.
extern size_t decideParallelBuildNumber(size_t tuple_num);

extern void displayRusageRUMaxrss();

extern bool isReady(const std::vector<char> &readys);

extern void sleepMs(size_t ms);

extern void sleepMicroSec(size_t us);

extern void waitForReady(const std::vector<char> &readys);

[[maybe_unused]] inline static bool chkClkSpan(const uint64_t start,
                                               const uint64_t stop,
                                               const uint64_t threshold) {
  uint64_t diff = 0;
  diff = stop - start;
  if (diff > threshold)
    return true;
  else
    return false;
}

template<typename Int>
Int byteswap(Int in) {
  switch (sizeof(Int)) {
    case 1:
      return in;
    case 2:
      return __builtin_bswap16(in);
    case 4:
      return __builtin_bswap32(in);
    case 8:
      return __builtin_bswap64(in);
    default:
      assert(false);
  }
}

template<typename Int>
void assign_as_bigendian(Int value, char *out) {
  Int tmp = byteswap(value);
  ::memcpy(out, &tmp, sizeof(tmp));
}
