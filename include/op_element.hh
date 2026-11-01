#pragma once

#include "debug.hh"
#include "workload.hh"

enum class OpType : std::uint8_t {
  NONE,
  READ,
  SCAN,
  INSERT,
  DELETE,
  UPDATE,
  RMW,
};

template<typename T>
class OpElement {
public:
  Storage storage_;
  std::string key_;
  T *rcdptr_;
  OpType op_;

  OpElement(Storage s, std::string_view key, T *rcdptr)
    : storage_(s), key_(key), rcdptr_(rcdptr) {}

  OpElement(Storage s, std::string_view key, T *rcdptr, OpType op)
    : storage_(s), key_(key), rcdptr_(rcdptr), op_(op) {}
};
