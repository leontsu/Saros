#pragma once

#include <stdint.h>
#include <random>

// xoroshiro128+ by David Blackman and Sebastiano Vigna
// (http://xoroshiro.di.unimi.it/xoroshiro128plus.c), adapted by Takayuki Tanabe.
class Xoroshiro128Plus {
public:
  Xoroshiro128Plus() {
    init();
  }

  uint64_t s[2];

  inline void init() {
    std::random_device rnd;
    s[0] = rnd();
    s[1] = splitMix64(s[0]);
  }

  uint64_t splitMix64(uint64_t seed) {
    uint64_t z = (seed += 0x9e3779b97f4a7c15);
    z = (z ^ (z >> 30)) * 0xbf58476d1ce4e5b9;
    z = (z ^ (z >> 27)) * 0x94d049bb133111eb;
    return z ^ (z >> 31);
  }

  inline uint64_t rotl(const uint64_t x, int k) {
    return (x << k) | (x >> (64 - k));
  }

  uint64_t next(void) {
    const uint64_t s0 = s[0];
    uint64_t s1 = s[1];
    const uint64_t result = s0 + s1;

    s1 ^= s0;
    s[0] = rotl(s0, 24) ^ s1 ^ (s1 << 16);  // a, b
    s[1] = rotl(s1, 37);                    // c

    return result;
  }

  inline uint64_t random_int(uint64_t min, uint64_t max) {
    return next() % (max - min + 1) + min;
  }

  uint64_t operator()() { return next(); }
};
