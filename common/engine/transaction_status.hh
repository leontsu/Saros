#pragma once

#include <cstdint>

enum class TransactionStatus : uint8_t {
  invalid, // used when quit_ flag is true. quit_ becomes true when e.g. benchmark time expires; main thread sets quit_ = true
  inflight,
  committed,
  aborted,
  validating,
};
