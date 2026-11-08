#include <stdint.h>
#include <algorithm>
#include <chrono>
#include <map>
#include <string>
#include <thread>
#include <vector>

#define GLOBAL_VALUE_DEFINE

#include "include/common.hh"
#include "include/result.hh"
#include "include/transaction.hh"
#include "include/util.hh"

#include "common/atomic_wrapper.hh"
#include "common/backoff.hh"
#include "workloads/bomb.hh"
#include "common/debug.hh"
#include "common/result.hh"
#include "common/util.hh"

#ifdef ENGINE_SAROS
#include "include/retention_sampler.hh"
#endif

using namespace std;

void worker(size_t thid, char &ready, const bool &start, const bool &quit) {
  Backoff backoff(FLAGS_clocks_per_us);
  TxExecutor trans(thid, backoff, (Result *) &OzeResult[thid], quit);
  BombWorkload<Tuple,void> workload;
  workload.prepare(trans, nullptr);

  MasstreeWrapper<Tuple>::thread_init(thid);

  storeRelease(ready, 1);
  while (!loadAcquire(start)) _mm_pause();
  if (thid == 0) trans.epoch_timer_start_ = rdtscp();
  while (!loadAcquire(quit)) {
    workload.run<TxExecutor,TransactionStatus>(trans);
  }
}

int main(int argc, char *argv[]) try {
  google::InitGoogleLogging(argv[0]);
  google::InstallFailureSignalHandler();

  gflags::SetUsageMessage("BoMB benchmark.");
  gflags::ParseCommandLineFlags(&argc, &argv, true);
  init(BombWorkload<Tuple,void>::getTableNum());
  // Opened before makeDB() so that the sampler records load_begin.
  eval::Sampler eval_sampler;
  {
    std::map<uint32_t, std::string> eval_types;
    eval_types[get_tx_type(TxType::UpdateProductCostMaster)] = "L1";
    eval_types[get_tx_type(TxType::UpdateMaterialCostMaster)] = "S1";
    eval_types[get_tx_type(TxType::IssueJournalVoucher)] = "S2";
    eval_types[get_tx_type(TxType::AddNewProduct)] = "S3";
    eval_types[get_tx_type(TxType::ChangeRawMaterial)] = "S4";
    eval_types[get_tx_type(TxType::ChangeProductQuantity)] = "S5";
    eval_sampler.open(FLAGS_eval_sample_path, TotalThreadNum, eval_types);
  }
  auto eval_global_epoch = [] {
    return (uint64_t)__atomic_load_n(&GlobalEpoch.obj_, __ATOMIC_ACQUIRE);
  };
  auto eval_min_epoch = [] {
    uint64_t m = UINT64_MAX;
    for (size_t i = 0; i < TotalThreadNum; ++i)
      m = std::min<uint64_t>(m, ThManagementTable[i]->atomicLoadEpoch());
    return m;
  };
  BombWorkload<Tuple,void>::makeDB(nullptr);

  alignas(CACHE_LINE_SIZE) bool start = false;
  alignas(CACHE_LINE_SIZE) bool quit = false;
  initResult();
  std::vector<char> readys(TotalThreadNum + 1);
  std::vector<std::thread> thv;
  for (size_t i = 0; i < TotalThreadNum; ++i)
    thv.emplace_back(worker, i, std::ref(readys[i]),
                     std::ref(start), std::ref(quit));
  thv.emplace_back(BombWorkload<Tuple,void>::request_dispatcher,
                   TotalThreadNum, std::ref(readys[TotalThreadNum]),
                   std::ref(start), std::ref(quit));
  waitForReady(readys);
#ifdef ENGINE_SAROS
  RetentionSampler retention(FLAGS_retention_sample_path);
  retention.sample(-1, "load");
  const uint64_t retention_interval =
      std::max<uint64_t>(1, FLAGS_retention_sample_interval_s);
#endif
  eval_sampler.event("processing_start");
  eval_sampler.sample(0, eval_global_epoch(), eval_min_epoch());
  storeRelease(start, true);
#ifdef ENGINE_SAROS
  retention.start_thread(std::chrono::steady_clock::now(), retention_interval);
#endif
  for (size_t i = 0; i < FLAGS_extime; ++i) {
    sleepMs(1000);
    eval_sampler.sample(i + 1, eval_global_epoch(), eval_min_epoch());
  }
  storeRelease(quit, true);
  eval_sampler.event("quit_released");
#ifdef ENGINE_SAROS
  retention.stop_thread();
#endif
  for (auto &th : thv) th.join();
  eval_sampler.event("processing_end");
  eval_sampler.sample(FLAGS_extime + 1, eval_global_epoch(), eval_min_epoch());
  eval_sampler.finish();
#ifdef ENGINE_SAROS
  retention.sample(static_cast<int64_t>(FLAGS_extime), "joined");
#endif

  for (unsigned int i = 0; i < TotalThreadNum; ++i) {
    OzeResult[0].addLocalAllResult(OzeResult[i]);
    OzeResult[0].addLocalPerTxResult(OzeResult[i], TxTypes);
  }
#ifdef ENGINE_SAROS
  // Taken before the drain below, so the output shows the run's pending state.
  snapshot_history_chunk_counters(&OzeResult[0]);
#endif
  OzeResult[0].displayAllResult(FLAGS_clocks_per_us, FLAGS_extime, TotalThreadNum);
  std::cout << "Details per transaction type:" << std::endl;
  OzeResult[0].displayPerTxResult(TxTypes);
  if (FLAGS_insert_only_single_writer) {
    std::cout << "insert_only_skips:\t" << InsertOnlySkips.load() << std::endl;
    std::cout << "insert_only_mixed_aborts:\t" << InsertOnlyMixedAborts.load() << std::endl;
    std::cout << "insert_only_scan_falls:\t" << InsertOnlyScanFalls.load() << std::endl;
  }

#ifdef ENGINE_SAROS
  // Every worker has joined, so nothing can still reference retired state.
  RowRetirementRegistry::instance().drain_all();
  VersionRetirementRegistry::instance().drain_all();
  ChunkRetirementRegistry::instance().drain_all();
#endif

  return 0;
} catch (bad_alloc) {
  ERR;
}
