#pragma once

#include <pthread.h>
#include <atomic>
#include <iostream>
#include <queue>

#include "common/cache_line_size.hh"
#include "common/int64byte.hh"
#include "common/masstree_wrapper.hh"
#include "common/lock.hh"

#include "common/engine/scan.hh"
#include "thread_management.hh"
#include "tuple.hh"
#include "common/engine/version.hh"

#include "gflags/gflags.h"
#include "glog/logging.h"

#ifdef GLOBAL_VALUE_DEFINE
#define GLOBAL
alignas(CACHE_LINE_SIZE) GLOBAL uint64_t_64byte GlobalEpoch(1);
alignas(CACHE_LINE_SIZE) GLOBAL MasstreeWrapper<Tuple> MT;
#else
#define GLOBAL extern
alignas(CACHE_LINE_SIZE) GLOBAL uint64_t_64byte GlobalEpoch;
alignas(CACHE_LINE_SIZE) GLOBAL MasstreeWrapper<Tuple> MT;
#endif

#ifdef GLOBAL_VALUE_DEFINE
DEFINE_uint64(clocks_per_us, 2100, "TSC ticks per microsecond (the CPU's TSC MHz).");
DEFINE_uint64(extime, 3, "Measured run time in seconds.");
DEFINE_uint64(epoch_time, 40, "Epoch interval in milliseconds.");
DEFINE_uint64(thread_num, 10, "Number of worker threads.");
DEFINE_uint64(validation_th_num, 1, "Validation threads per transaction (1 = serial).");
DEFINE_uint64(validation_threshold, 100,
              "Parallel validation runs only with more propagation targets than this.");
DEFINE_bool(insert_only_single_writer, true,
            "Single-writer insert-only rule: an insert-only transaction skips read "
            "validation if all versions it read have one writer and no scan covers its "
            "inserts; mixed writers abort. false = it validates its reads.");
DEFINE_uint64(insert_only_discovery_limit, 1000,
              "Abort (and retry) an insert-only transaction whose propagation targets "
              "exceed this many records (0 = no limit).");
DEFINE_bool(read_set_hash_index, true,
            "Answer read-set lookups from a hash index instead of a linear scan.");
DEFINE_bool(l1_cost_diag, false,
            "Print one #l1cost line per commit of thread 0 (the BoMB L1 thread).");
#else
DECLARE_uint64(clocks_per_us);
DECLARE_uint64(extime);
DECLARE_uint64(epoch_time);
DECLARE_uint64(thread_num);
DECLARE_uint64(validation_th_num);
DECLARE_uint64(validation_threshold);
DECLARE_bool(insert_only_single_writer);
DECLARE_uint64(insert_only_discovery_limit);
DECLARE_bool(read_set_hash_index);
DECLARE_bool(l1_cost_diag);
#endif

GLOBAL uint64_t TotalThreadNum;
GLOBAL std::atomic<uint64_t> InsertOnlySkips;        // read validation skipped
GLOBAL std::atomic<uint64_t> InsertOnlyMixedAborts;  // aborted, mixed writers
GLOBAL std::atomic<uint64_t> InsertOnlyScanFalls;    // a scan covers the inserts: full validation

alignas(CACHE_LINE_SIZE) GLOBAL Tuple *Table;
alignas(CACHE_LINE_SIZE) GLOBAL ThreadManagementEntry **ThManagementTable;

#define SCAN_HISTORY_INDEX(storage, epoch) 2 * (uint32_t)storage + epoch % 2
alignas(CACHE_LINE_SIZE) GLOBAL atomic<ScanEntry*> *ScanHistory;
alignas(CACHE_LINE_SIZE) GLOBAL atomic<uint64_t> *ScanRange;
