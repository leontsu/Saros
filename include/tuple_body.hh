#pragma once

#include <atomic>
#include <memory>
#include <string>
#include <string_view>
#include <utility>

#include "heap_object.hh"
#include "util.hh"

// A record's key and value.
class TupleBody {  // NOLINT
public:
  TupleBody() = default;

  TupleBody(const TupleBody &right) : TupleBody() {
    key_ = right.key_;
    val_.deep_copy_from(right.val_);
  }

  TupleBody(TupleBody &&right) noexcept : TupleBody() {
    key_ = std::move(right.key_);
    val_ = std::move(right.val_);
  }

  TupleBody(std::string_view key, HeapObject&& obj) : TupleBody() {
    set(key, std::move(obj));
  }

  TupleBody &operator=(const TupleBody &right) {
    key_ = right.key_;
    val_.deep_copy_from(right.val_);
    return *this;
  }

  TupleBody &operator=(TupleBody &&right) noexcept {
    key_ = std::move(right.key_);
    val_ = std::move(right.val_);
    return *this;
  }

  [[nodiscard]] std::string_view get_key() const { return key_; }

  [[nodiscard]] std::size_t get_val_size() const { return val_.size(); }

  void set_key(std::string_view key) {
    key_ = key; // copy
  }
  void set_value(HeapObject&& val) {
    val_ = std::move(val);
  }
  void set(std::string_view key, HeapObject&& val) {
    set_key(key);
    set_value(std::move(val));
  }
  HeapObject& get_value() { return val_; }
  const HeapObject& get_value() const { return val_; }

private:
  std::string key_{};
  HeapObject val_;
};
