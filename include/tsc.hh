#pragma once

#include <stdint.h>

// Waits for all earlier instructions; later ones may still overtake the read.
[[maybe_unused]] static uint64_t
rdtscp() {
  uint64_t rax;
  uint64_t rdx;
  uint32_t aux;
  asm volatile("rdtscp" : "=a"(rax), "=d"(rdx), "=c"(aux)::);
  return (rdx << 32) | rax;
}
