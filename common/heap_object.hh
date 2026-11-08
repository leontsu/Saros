#pragma once

#include <cstring>
#include <cassert>
#include <new>
#include <type_traits>

// An aligned heap buffer that holds one record value.
struct HeapObject
{
private:
  void* data_;
  size_t size_;
  std::align_val_t align_;
  bool owner_;

public:
  HeapObject() : data_(nullptr), size_(0), align_(std::align_val_t(0)), owner_(false) {
  }
  HeapObject(const HeapObject& rhs) = delete;
  HeapObject(HeapObject&& rhs) noexcept : HeapObject() { swap(rhs); }

  ~HeapObject() { reset(); }

  HeapObject& operator=(const HeapObject&) = delete;
  HeapObject& operator=(HeapObject&& rhs) noexcept { swap(rhs); return *this; }

  class TmpRefObj {
    HeapObject* ptr;
  public:
    explicit TmpRefObj(HeapObject* ptr0) : ptr(ptr0) {}
    template <typename T> operator T&() {
      assert(ptr->is_compatible<T>());
      return *reinterpret_cast<T*>(ptr->data());
    }
    template <typename T> operator const T&() {
      assert(ptr->is_compatible<T>());
      return *reinterpret_cast<const T*>(ptr->data());
    }
  };
  class ConstTmpRefObj {
    const HeapObject* ptr;
  public:
    explicit ConstTmpRefObj(const HeapObject* ptr0) : ptr(ptr0) {}
    template <typename T> operator const T&() {
      assert(ptr->is_compatible<T>());
      return *reinterpret_cast<const T*>(ptr->data());
    }
  };
  TmpRefObj ref() { TmpRefObj obj(this); return obj; }
  ConstTmpRefObj ref() const { ConstTmpRefObj obj(this); return obj; }

  template <typename T> T& cast_to() {
    assert(is_compatible<T>());
    return *reinterpret_cast<T*>(data());
  }
  template <typename T> const T& cast_to() const {
    assert(is_compatible<T>());
    return *reinterpret_cast<const T*>(data());
  }

  const void* data() const { return data_; }
  void* data() { return data_; }
  size_t size() const { return size_; }

  void allocate(size_t size, std::align_val_t align = std::align_val_t(sizeof(uint8_t))) {
    reset();
    data_ = ::operator new(size, align);
    size_ = size;
    align_ = align;
    owner_ = true;
  }
  template <typename T>
  void allocate() {
    static_assert(std::is_trivially_copyable_v<T>);
    allocate(sizeof(T), std::align_val_t(alignof(T)));
  }

  void swap(HeapObject& rhs) noexcept {
    std::swap(data_, rhs.data_);
    std::swap(size_, rhs.size_);
    std::swap(align_, rhs.align_);
    std::swap(owner_, rhs.owner_);
  }
  void deep_copy_from(const HeapObject& rhs) {
    allocate(rhs.size_, rhs.align_);
    ::memcpy(data_, rhs.data_, size_);
  }

  template <typename T>
  bool is_compatible() const {
    static_assert(std::is_trivially_copyable_v<T>);
    return data_ != nullptr && size_ == sizeof(T) && align_ == std::align_val_t(alignof(T));
  }

  void reset() noexcept {
    if (owner_) {
      assert(data_!= nullptr); assert(size_ > 0); assert(static_cast<std::size_t>(align_) > 0);
      ::operator delete(data_, size_, align_);
    }
    data_ = nullptr;
    size_ = 0;
    align_ = std::align_val_t(0);
    owner_ = false;
  }
};
