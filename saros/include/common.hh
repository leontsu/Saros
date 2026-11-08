#pragma once

#include <pthread.h>
#include <atomic>
#include <iostream>
#include <mutex>
#include <queue>
#include <unordered_map>

#include "common/cache_line_size.hh"
#include "common/int64byte.hh"
#include "common/masstree_wrapper.hh"
#include "common/lock.hh"

#include "scan.hh"
#include "thread_management.hh"
#include "tuple.hh"
#include "version.hh"

#include "gflags/gflags.h"
#include "glog/logging.h"

#ifdef GLOBAL_VALUE_DEFINE
#define GLOBAL
alignas(CACHE_LINE_SIZE) GLOBAL uint64_t_64byte GlobalEpoch(1);
alignas(CACHE_LINE_SIZE) GLOBAL MasstreeWrapper<Tuple> MT;
// Global order of insert_version() calls (taken under the record latch), for
// --history_dump_path: it recovers each key's final version order.
GLOBAL std::atomic<uint64_t> HistoryInstallSeq(0);
#else
#define GLOBAL extern
alignas(CACHE_LINE_SIZE) GLOBAL uint64_t_64byte GlobalEpoch;
alignas(CACHE_LINE_SIZE) GLOBAL MasstreeWrapper<Tuple> MT;
GLOBAL std::atomic<uint64_t> HistoryInstallSeq;
#endif

#ifdef GLOBAL_VALUE_DEFINE
DEFINE_uint64(clocks_per_us, 2100, "TSC ticks per microsecond (the CPU's TSC MHz).");
DEFINE_uint64(extime, 3, "Measured run time in seconds.");
DEFINE_uint64(epoch_time, 40, "Epoch interval in milliseconds.");
DEFINE_uint64(thread_num, 10, "Number of worker threads.");
DEFINE_bool(saros, true,
            "Saros: keep dependencies in per-record histories with prefix references. "
            "false = Oze's merge-based record graphs.");
DEFINE_uint64(retirement_threshold, 128,
              "Saros: retire history entries at a record once its history holds more "
              "than this many entries (0 disables retirement).");
DEFINE_bool(validate_insert_only_reads, true,
            "Validate the reads of insert-only transactions. false = Oze's shortcut, "
            "which skips their read validation.");
DEFINE_uint64(insert_only_discovery_limit, 1000,
              "Saros: abort (and retry) an insert-only transaction whose propagation "
              "targets exceed this many records (0 = no limit).");
DEFINE_uint64(short_discovery_limit, 0,
              "Saros: the same limit for every other transaction that read fewer "
              "records than this (0 = off).");
DEFINE_bool(free_retired_history_chunks, true,
            "Saros: free history chunks that lie wholly at or below the retirement "
            "watermark, one epoch grace after unlinking them.");
DEFINE_uint64(free_retired_history_chunks_budget, 65536,
              "Maximum chunks and chunk directories freed per reclamation pass.");
DEFINE_uint64(reclaim_interval, 64,
              "mainte() calls per thread between two reclamation passes of each kind.");
DEFINE_bool(reuse_fresh_graph_state, true,
            "A new row keeps the history its Tuple() created instead of registering "
            "a second one in Tuple::init().");
DEFINE_bool(read_set_hash_index, true,
            "Answer read-set lookups from a hash index instead of a linear scan.");
DEFINE_bool(import_skip_retirable, true,
            "Saros: an import skips the leading history entries that retirement "
            "would drop at the importer's floor.");
DEFINE_bool(l1_cost_diag, false,
            "Print one #l1cost line per commit of thread 0 (the BoMB L1 thread).");
DEFINE_string(history_dump_path, "",
              "Directory for per-thread commit histories (tools/check_history.py); "
              "empty = off.");
DEFINE_string(retention_sample_path, "",
              "BoMB: TSV file for periodic samples of history and record-state "
              "memory; empty = off.");
DEFINE_uint64(retention_sample_interval_s, 5,
              "Seconds between retention samples.");
#else
DECLARE_uint64(clocks_per_us);
DECLARE_uint64(extime);
DECLARE_uint64(epoch_time);
DECLARE_uint64(thread_num);
DECLARE_bool(saros);
DECLARE_uint64(retirement_threshold);
DECLARE_bool(validate_insert_only_reads);
DECLARE_uint64(insert_only_discovery_limit);
DECLARE_uint64(short_discovery_limit);
DECLARE_bool(free_retired_history_chunks);
DECLARE_uint64(free_retired_history_chunks_budget);
DECLARE_uint64(reclaim_interval);
DECLARE_bool(reuse_fresh_graph_state);
DECLARE_bool(read_set_hash_index);
DECLARE_bool(import_skip_retirable);
DECLARE_bool(l1_cost_diag);
DECLARE_string(history_dump_path);
DECLARE_string(retention_sample_path);
DECLARE_uint64(retention_sample_interval_s);
#endif

GLOBAL uint64_t TotalThreadNum;

alignas(CACHE_LINE_SIZE) GLOBAL Tuple *Table;
alignas(CACHE_LINE_SIZE) GLOBAL ThreadManagementEntry **ThManagementTable;

#define SCAN_HISTORY_INDEX(storage, epoch) 2 * (uint32_t)storage + epoch % 2
alignas(CACHE_LINE_SIZE) GLOBAL atomic<ScanEntry*> *ScanHistory;
alignas(CACHE_LINE_SIZE) GLOBAL atomic<uint64_t> *ScanRange;
