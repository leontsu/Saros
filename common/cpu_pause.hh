#pragma once

#include <xmmintrin.h>

inline void cpu_pause() {
  _mm_pause();
}
