#include <stdint.h>
#include <stdlib.h>
#include <atomic>
#include <iostream>
#include <mutex>
#include <string>

#include "common/backoff.hh"

#include "common/engine/atomic_tool.hh"
#include "include/common.hh"
#include "common/engine/util.hh"

using std::cout, std::endl;

void init(int32_t table_num) {
  displayParameter();

  TotalThreadNum = FLAGS_thread_num;

  if (FLAGS_clocks_per_us < 100) {
    printf("CPU_MHZ is less than 100. are you really?\n");
    exit(0);
  }

  // Two (even/odd) scan histories per table: offset = 2 * storage_index + epoch % 2.
  if (posix_memalign((void **) &ScanHistory, CACHE_LINE_SIZE,
                     table_num * 2 * sizeof(atomic<ScanEntry*>)) != 0)
    ERR;
  if (posix_memalign((void **) &ScanRange, CACHE_LINE_SIZE,
                     table_num * 2 * sizeof(atomic<uint64_t>)) != 0)
    ERR;

  // init
  try {
    ThManagementTable = new ThreadManagementEntry *[TotalThreadNum];
  } catch (bad_alloc) {
    ERR;
  }
  for (unsigned int i = 0; i < TotalThreadNum; ++i) {
    ThManagementTable[i] = new ThreadManagementEntry(1);
  }
  for (unsigned int i = 0; i < table_num * 2; ++i) {
    ScanHistory[i].store(nullptr);
  }
}

void displayParameter() {
  cout << "#FLAGS_clocks_per_us:\t" << FLAGS_clocks_per_us << endl;
  cout << "#FLAGS_extime:\t\t" << FLAGS_extime << endl;
  cout << "#FLAGS_epoch_time:\t" << FLAGS_epoch_time << endl;
  cout << "#FLAGS_thread_num:\t" << FLAGS_thread_num << endl;
  cout << "#FLAGS_saros:\t"
       << FLAGS_saros << endl;
  cout << "#FLAGS_retirement_threshold:\t"
       << FLAGS_retirement_threshold << endl;
  cout << "#FLAGS_validate_insert_only_reads:\t"
       << FLAGS_validate_insert_only_reads << endl;
  cout << "#FLAGS_insert_only_discovery_limit:\t"
       << FLAGS_insert_only_discovery_limit << endl;
  cout << "#FLAGS_short_discovery_limit:\t" << FLAGS_short_discovery_limit << endl;
  cout << "#FLAGS_free_retired_history_chunks:\t"
       << FLAGS_free_retired_history_chunks << endl;
  cout << "#FLAGS_free_retired_history_chunks_budget:\t"
       << FLAGS_free_retired_history_chunks_budget << endl;
  cout << "#FLAGS_reclaim_interval:\t"
       << FLAGS_reclaim_interval << endl;
  cout << "#FLAGS_reuse_fresh_graph_state:\t" << FLAGS_reuse_fresh_graph_state << endl;
  cout << "#FLAGS_read_set_hash_index:\t" << FLAGS_read_set_hash_index << endl;
  cout << "#FLAGS_import_skip_retirable:\t" << FLAGS_import_skip_retirable << endl;
}

// True when every slot that counts has observed the epoch. Quiescent slots are skipped; Entering
// slots count, so the epoch cannot advance under a worker that is rejoining.
bool chkEpochLoaded() {
  std::lock_guard<std::mutex> membership_lock(EpochMembershipWriterMutex);
  const uint64_t nowepo = atomicLoadGE();
  for (unsigned int i = 0; i < TotalThreadNum; ++i) {
    if (!ThManagementTable[i]->countsTowardEpochAdvance())
      continue;
    if (ThManagementTable[i]->atomicLoadEpoch() != nowepo) {
      return false;
    }
  }
  return true;
}

void ozeLeaderWork(uint64_t &epoch_timer_start, uint64_t &epoch_timer_stop) {
  epoch_timer_stop = rdtscp();
  if (chkClkSpan(epoch_timer_start, epoch_timer_stop,
                 FLAGS_epoch_time * FLAGS_clocks_per_us * 1000) &&
      chkEpochLoaded()) {
    atomicAddGE();
    epoch_timer_start = epoch_timer_stop;
  }
}
