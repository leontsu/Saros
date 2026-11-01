#pragma once

#include <iomanip>
#include <iostream>
#include <random>
#include <thread>
#include <vector>

#include <pthread.h>
#include <stdlib.h>

// config.h must precede compiler.hh; the blank line keeps formatters from
// reordering them. "masstree/" is the configured Masstree build (CMake).
#include "masstree/config.h"

#include "masstree/compiler.hh"

#include "masstree/kvthread.hh"
#include "masstree/masstree.hh"
#include "masstree/masstree_insert.hh"
#include "masstree/masstree_print.hh"
#include "masstree/masstree_remove.hh"
#include "masstree/masstree_scan.hh"
#include "masstree/masstree_stats.hh"
#include "masstree/masstree_tcursor.hh"
#include "masstree/string.hh"

#include "debug.hh"
#include "status.hh"
#include "util.hh"

class key_unparse_unsigned {
public:
  static int unparse_key(Masstree::key<std::uint64_t> key, char *buf, int buflen) {
    return snprintf(buf, buflen, "%" PRIu64, key.ikey());
  }
};

// A Masstree index whose values are pointers to T.
template<typename T>
class MasstreeWrapper {
public:
  struct table_params : public Masstree::nodeparams<15, 15> {
    typedef T *value_type;
    typedef Masstree::value_print<value_type> value_print_type;
    typedef threadinfo threadinfo_type;
    typedef key_unparse_unsigned key_unparse_type;
    static constexpr ssize_t print_max_indent_depth = 12;
  };

  typedef Masstree::Str Str;
  typedef Masstree::basic_table<table_params> table_type;
  typedef Masstree::unlocked_tcursor<table_params> unlocked_cursor_type;
  typedef Masstree::tcursor<table_params> cursor_type;
  typedef Masstree::leaf<table_params> leaf_type;
  typedef Masstree::internode<table_params> internode_type;

  typedef typename table_type::leaf_type node_type;
  typedef typename unlocked_cursor_type::nodeversion_value_type
          nodeversion_value_type;

  static __thread typename table_params::threadinfo_type *ti;

  MasstreeWrapper() { this->table_init(); }

  // A scan calls on_resp_node() for every leaf whose range overlaps the scan
  // range. The scans here use DefaultScanCallback, which does nothing.
  class ScanCallback {
    public:
    virtual ~ScanCallback() {}
    virtual void on_resp_node(const node_type *n, uint64_t version) = 0;
  };

  void table_init() {
    if (ti == nullptr) ti = threadinfo::make(threadinfo::TI_MAIN, -1);
    table_.initialize(*ti);
  }

  static void thread_init(int thread_id) {
    if (ti == nullptr) ti = threadinfo::make(threadinfo::TI_PROCESS, thread_id);
  }

  Status insert_value(std::string_view key, T *value) {
    cursor_type lp(table_, key.data(), key.size());
    bool found = lp.find_insert(*ti);
    if (found) {
      lp.finish(0, *ti);
      return Status::WARN_ALREADY_EXISTS;
    }
    lp.value() = value;
    fence();
    lp.finish(1, *ti);
    return Status::OK;
  }

  Status remove_value(std::string_view key) {
    cursor_type lp(table_, key.data(), key.size());
    bool found = lp.find_locked(*ti);
    if (found) {
      lp.finish(-1, *ti);
      return Status::OK;
    }
    lp.finish(-1, *ti);
    return Status::WARN_NOT_FOUND;
  }

  T *get_value(std::string_view key) {
    unlocked_cursor_type lp(table_, key.data(), key.size());
    bool found = lp.find_unlocked(*ti);
    if (found) {
      return lp.value();
    }
    return nullptr;
  }

  void scan(const char *const lkey, const std::size_t len_lkey,
            const bool l_exclusive, const char *const rkey,
            const std::size_t len_rkey, const bool r_exclusive,
            std::vector<T*> *res, int64_t max_scan_num,
            ScanCallback& callback) {
    Str mtkey;
    if (lkey == nullptr) {
      mtkey = Str();
    } else {
      mtkey = Str(lkey, len_lkey);
    }

    SearchRangeScanner scanner(rkey, len_rkey, r_exclusive, res, max_scan_num, callback);
    table_.scan(mtkey, !l_exclusive, scanner, *ti);
  }

  void scan(const char *const lkey, const std::size_t len_lkey,
            const bool l_exclusive, const char *const rkey,
            const std::size_t len_rkey, const bool r_exclusive,
            std::vector<T*> *res, int64_t max_scan_num) {
    scan(lkey, len_lkey, l_exclusive, rkey, len_rkey, r_exclusive, res, max_scan_num, default_callback);
  }

  class SearchRangeScanner;
  class DefaultScanCallback;
  static inline DefaultScanCallback default_callback{};

private:
  table_type table_;
};

template<typename T>
class MasstreeWrapper<T>::DefaultScanCallback : public ScanCallback {
  void on_resp_node(const node_type *n, uint64_t version) {}
};

template<typename T>
class MasstreeWrapper<T>::SearchRangeScanner {
  public:
  using Str = Masstree::Str;

  SearchRangeScanner(const char *const rkey, const std::size_t len_rkey,
                     const bool r_exclusive, std::vector<T *> *scan_buffer,
                     int64_t max_scan_num, ScanCallback& callback)
      : rkey_(rkey),
        len_rkey_(len_rkey),
        r_exclusive_(r_exclusive),
        scan_buffer_(scan_buffer),
        max_scan_num_(max_scan_num),
        callback_(callback) {
    if (max_scan_num_ > 0) {
      scan_buffer->reserve(max_scan_num_);
    }
  }

  void visit_leaf(const Masstree::scanstackelt<table_params>& iter,
                  const Masstree::key<uint64_t>& key,
                  threadinfo& ti) {
    const Masstree::leaf<table_params>* node = iter.node();
    uint64_t version = iter.full_version_value();
    callback_.on_resp_node(node, version);
  }

  bool visit_value(const Str key, T *val, threadinfo &) {
    if (max_scan_num_ >= 0 && scan_buffer_->size() >= max_scan_num_) {
      return false;
    }

    if (rkey_ == nullptr) {
      scan_buffer_->emplace_back(val);
      return true;
    }

    const int res_memcmp = memcmp(
        rkey_, key.s, std::min(len_rkey_, static_cast<std::size_t>(key.len)));
    if (res_memcmp > 0 ||
        (res_memcmp == 0 &&
         ((!r_exclusive_ && len_rkey_ == static_cast<std::size_t>(key.len)) ||
          len_rkey_ > static_cast<std::size_t>(key.len)))) {
      scan_buffer_->emplace_back(val);
      return true;
    }
    return false;
  }

  private:
  const char *const rkey_{};
  const std::size_t len_rkey_{};
  const bool r_exclusive_{};
  std::vector<T *> *scan_buffer_{};
  int64_t max_scan_num_ = -1;
  ScanCallback& callback_;
};

template<typename T>
__thread typename MasstreeWrapper<T>::table_params::threadinfo_type *
        MasstreeWrapper<T>::ti = nullptr;
#ifdef GLOBAL_VALUE_DEFINE
volatile mrcu_epoch_type active_epoch = 1;
volatile std::uint64_t globalepoch = 1;
volatile bool recovering = false;
#else
extern volatile mrcu_epoch_type active_epoch;
extern volatile std::uint64_t globalepoch;
extern volatile bool recovering;
#endif
