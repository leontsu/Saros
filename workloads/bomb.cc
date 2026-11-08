#include <stdint.h>
#include <algorithm>
#include <chrono>
#include <cmath>
#include <condition_variable>
#include <cstdio>
#include <fstream>
#include <limits>
#include <map>
#include <mutex>
#include <sstream>
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

// --retention_sample_path: one TSV line per sample of process memory, record
// states, history chunks and the reclamation floor. The load-phase sample is
// taken before the workers start, run-phase samples every
// --retention_sample_interval_s seconds from a thread of their own, and a last
// one after the workers have joined. Each line is flushed when written.
class RetentionSampler {
public:
  explicit RetentionSampler(const std::string& path) : path_(path) {
    if (path_.empty()) return;
    for (const auto& [id, name] : TxTypes) {
      type_ids_.push_back(id);
      type_names_.push_back(name);
    }
    fh_ = std::fopen(path_.c_str(), "w");
    if (fh_ == nullptr) {
      std::cerr << "retention sampler: cannot open " << path_ << std::endl;
      std::abort();
    }
    std::ostringstream head;
    head << "#sizeof_RecordGraphState=" << sizeof(RecordGraphState)
         << " sizeof_GraphDelta=" << sizeof(GraphDelta)
         << " sizeof_GraphDeltaChunk=" << sizeof(GraphDeltaChunk)
         << " delta_slots_per_chunk=" << kGraphDeltaChunkSlots
         << " sizeof_Tuple=" << sizeof(Tuple)
         << " sizeof_Version=" << sizeof(Version);
    emit(head.str());
    std::ostringstream cols;
    cols << "second\tphase\twall_ms\tsample_ms\tvmrss_kb\tvmhwm_kb\ttotal_commits";
    for (const std::string& name : type_names_) cols << "\tcommits_" << name;
    cols << "\tglobal_epoch\tactual_floor"
         << "\tstates_scanned\tstates_with_log\tstruct_bytes"
         << "\tdeltas_committed\tdeltas_retired"
         << "\tchunk_bytes\tchunk_bytes_retired_prefix\tchunks_unlinked_live_states"
         << "\tdir_bytes\trow_pending\tversion_pending\tversion_retained_bytes"
         << "\thc_chunks_unlinked\thc_chunks_freed\thc_pending_bytes"
         << "\thc_peak_pending_bytes\thc_blocked_by_floor";
    emit(cols.str());
  }

  ~RetentionSampler() {
    stop_thread();
    if (fh_ != nullptr) std::fclose(fh_);
  }

  // Starts the run-phase sampling thread; `t0` is the start of processing.
  void start_thread(std::chrono::steady_clock::time_point t0,
                    uint64_t interval_s) {
    if (path_.empty()) return;
    t0_ = t0;
    thread_ = std::thread([this, interval_s] {
      for (uint64_t k = 0;; ++k) {
        const auto due = t0_ + std::chrono::seconds(k * interval_s);
        {
          std::unique_lock<std::mutex> lock(stop_mutex_);
          if (stop_cv_.wait_until(lock, due, [this] { return stop_; })) return;
        }
        sample(static_cast<int64_t>(k * interval_s), "run");
      }
    });
  }

  void stop_thread() {
    if (!thread_.joinable()) return;
    {
      std::lock_guard<std::mutex> lock(stop_mutex_);
      stop_ = true;
    }
    stop_cv_.notify_all();
    thread_.join();
  }

  void sample(int64_t second, const char* phase) {
    if (path_.empty()) return;
    const auto t0 = std::chrono::steady_clock::now();
    RecordGraphRegistry::RetentionSample r;
    RecordGraphRegistry::instance().sample_retention(r);
    auto ld = [](const std::atomic<uint64_t>& v) {
      return v.load(std::memory_order_relaxed);
    };
    uint64_t total = 0;
    std::vector<uint64_t> per_tx(type_ids_.size(), 0);
    for (size_t i = 0; i < TotalThreadNum; ++i) {
      total += OzeResult[i].local_commit_counts_;
      for (size_t t = 0; t < type_ids_.size(); ++t) {
        const uint32_t id = type_ids_[t];
        if (id >= MAX_TX_TYPE) continue;
        per_tx[t] += OzeResult[i].local_commit_counts_per_tx_[id];
      }
    }
    // The floor the reclamation passes compute: the oldest live transaction
    // or one below the oldest participating worker epoch.
    uint64_t min_epoch = std::numeric_limits<uint64_t>::max();
    for (unsigned int i = 0; i < TotalThreadNum; ++i) {
      if (!ThManagementTable[i]->countsTowardMinEpoch()) continue;
      const uint64_t e = ThManagementTable[i]->atomicLoadEpoch();
      if (e < min_epoch) min_epoch = e;
    }
    if (min_epoch == std::numeric_limits<uint64_t>::max()) {
      min_epoch = atomicLoadGE();
    }
    const uint64_t min_live = ReclamationFloorAnnouncer::min_live();
    const uint64_t min_future_floor = min_epoch == 0 ? 0 : min_epoch - 1;
    const uint64_t actual_floor = std::min(min_live, min_future_floor);
    const auto& h = ChunkRetirementRegistry::counters();
    const auto t1 = std::chrono::steady_clock::now();
    const int64_t wall_ms =
        t0_ == std::chrono::steady_clock::time_point{}
            ? -1
            : std::chrono::duration_cast<std::chrono::milliseconds>(t0 - t0_)
                  .count();

    std::ostringstream line;
    line << second << '\t' << phase << '\t' << wall_ms << '\t'
         << std::chrono::duration_cast<std::chrono::milliseconds>(t1 - t0).count()
         << '\t' << status_field_u64("VmRSS") << '\t' << status_field_u64("VmHWM")
         << '\t' << total;
    for (uint64_t v : per_tx) line << '\t' << v;
    line << '\t' << atomicLoadGE() << '\t' << actual_floor
         << '\t' << r.states_scanned << '\t' << r.states_with_log
         << '\t' << r.struct_bytes
         << '\t' << r.deltas_committed << '\t' << r.deltas_retired
         << '\t' << r.chunk_bytes << '\t' << r.chunk_bytes_retired_prefix
         << '\t' << r.chunks_unlinked << '\t' << r.dir_bytes
         << '\t' << RowRetirementRegistry::instance().pending()
         << '\t' << VersionRetirementRegistry::instance().pending()
         << '\t' << VersionRetirementRegistry::instance().retained_bytes()
         << '\t' << ld(h.chunks_unlinked) << '\t' << ld(h.chunks_freed)
         << '\t' << ld(h.pending_bytes) << '\t' << ld(h.peak_pending_bytes)
         << '\t' << ld(h.blocked_by_floor);
    emit(line.str());
  }

private:
  static uint64_t status_field_u64(const std::string& key) {
    std::ifstream in("/proc/self/status");
    std::string line;
    while (std::getline(in, line)) {
      if (line.rfind(key + ":", 0) == 0) {
        std::istringstream iss(line.substr(key.size() + 1));
        uint64_t value = 0;
        iss >> value;
        return value;
      }
    }
    return 0;
  }

  void emit(const std::string& line) {
    std::lock_guard<std::mutex> lock(file_mutex_);
    std::fputs(line.c_str(), fh_);
    std::fputc('\n', fh_);
    std::fflush(fh_);
  }

  std::string path_;
  std::vector<uint32_t> type_ids_;
  std::vector<std::string> type_names_;
  std::FILE* fh_ = nullptr;
  std::mutex file_mutex_;
  std::thread thread_;
  std::mutex stop_mutex_;
  std::condition_variable stop_cv_;
  bool stop_ = false;
  std::chrono::steady_clock::time_point t0_{};
};

int main(int argc, char *argv[]) try {
  google::InitGoogleLogging(argv[0]);
  google::InstallFailureSignalHandler();

  gflags::SetUsageMessage("BoMB benchmark for Saros and Oze.");
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
  RetentionSampler retention(FLAGS_retention_sample_path);
  retention.sample(-1, "load");
  const uint64_t retention_interval =
      std::max<uint64_t>(1, FLAGS_retention_sample_interval_s);
  eval_sampler.event("processing_start");
  eval_sampler.sample(0, eval_global_epoch(), eval_min_epoch());
  storeRelease(start, true);
  retention.start_thread(std::chrono::steady_clock::now(), retention_interval);
  for (size_t i = 0; i < FLAGS_extime; ++i) {
    sleepMs(1000);
    eval_sampler.sample(i + 1, eval_global_epoch(), eval_min_epoch());
  }
  storeRelease(quit, true);
  eval_sampler.event("quit_released");
  retention.stop_thread();
  for (auto &th : thv) th.join();
  eval_sampler.event("processing_end");
  eval_sampler.sample(FLAGS_extime + 1, eval_global_epoch(), eval_min_epoch());
  eval_sampler.finish();
  retention.sample(static_cast<int64_t>(FLAGS_extime), "joined");

  for (unsigned int i = 0; i < TotalThreadNum; ++i) {
    OzeResult[0].addLocalAllResult(OzeResult[i]);
    OzeResult[0].addLocalPerTxResult(OzeResult[i], TxTypes);
  }
  // Taken before the drain below, so the output shows the run's pending state.
  snapshot_history_chunk_counters(&OzeResult[0]);
  OzeResult[0].displayAllResult(FLAGS_clocks_per_us, FLAGS_extime, TotalThreadNum);
  std::cout << "Details per transaction type:" << std::endl;
  OzeResult[0].displayPerTxResult(TxTypes);
  if (FLAGS_insert_only_single_writer) {
    std::cout << "insert_only_skips:\t" << InsertOnlySkips.load() << std::endl;
    std::cout << "insert_only_mixed_aborts:\t" << InsertOnlyMixedAborts.load() << std::endl;
    std::cout << "insert_only_scan_falls:\t" << InsertOnlyScanFalls.load() << std::endl;
  }

  // Every worker has joined, so nothing can still reference retired state.
  RowRetirementRegistry::instance().drain_all();
  VersionRetirementRegistry::instance().drain_all();
  ChunkRetirementRegistry::instance().drain_all();

  return 0;
} catch (bad_alloc) {
  ERR;
}
