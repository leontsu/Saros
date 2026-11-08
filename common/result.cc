
#include <cmath>
#include <iomanip>
#include <iostream>

#include "debug.hh"
#include "result.hh"

using std::cout;
using std::endl;
using std::fixed;
using std::setprecision;
using namespace std;

extern void displayRusageRUMaxrss();

void Result::displaySuccessForwarding() {
  cout << "success_forwarding_:\t" << total_success_fw_ << endl;
}

void Result::displayAbortCounts() {
  cout << "abort_counts_:\t" << total_abort_counts_ << endl;
  cout << "batch_abort_counts_:\t" << total_batch_abort_counts_ << endl;
}

void Result::displayAbortRate() {
  long double ave_rate = (double) total_abort_counts_ /
                         (double) (total_commit_counts_ + total_abort_counts_);
  cout << fixed << setprecision(4) << "abort_rate:\t" << ave_rate << endl;
  ave_rate = (double) total_batch_abort_counts_ /
             (double) (total_batch_commit_counts_ + total_batch_abort_counts_);
  cout << fixed << setprecision(4) << "batch_abort_rate:\t" << ave_rate << endl;
}

void Result::displayCommitCounts() {
  cout << "commit_counts_:\t" << total_commit_counts_ << endl;
  cout << "batch_commit_counts_:\t" << total_batch_commit_counts_ << endl;
}

void Result::displayTps(size_t extime, size_t thread_num) {
  uint64_t result = (total_commit_counts_ + total_batch_commit_counts_) / extime;
  cout << "latency[ns]:\t" << powl(10.0, 9.0) / result * thread_num << endl;
  cout << "throughput[tps]:\t" << result << endl;
}


void Result::addLocalAbortCounts(const uint64_t count) {
  total_abort_counts_ += count;
}

void Result::addLocalBatchAbortCounts(const uint64_t count) {
  total_batch_abort_counts_ += count;
}

void Result::addLocalCommitCounts(const uint64_t count) {
  total_commit_counts_ += count;
}

void Result::addLocalBatchCommitCounts(const uint64_t count) {
  total_batch_commit_counts_ += count;
}

void Result::addLocalSuccessForwarding(const uint64_t count) {
  total_success_fw_ += count;
}


void Result::displayAllResult([[maybe_unused]] size_t clocks_per_us,
                              size_t extime,
                              [[maybe_unused]] size_t thread_num) {
  displaySuccessForwarding();
  displayHistoryChunkResult();
  displayAbortCounts();
  displayCommitCounts();
  displayRusageRUMaxrss();
  displayAbortRate();
  displayTps(extime, thread_num);
}

void Result::displayHistoryChunkResult() {
  if (history_chunk_lines_.empty()) return;
  cout << "history_chunks:" << endl;
  for (const auto& [name, value] : history_chunk_lines_) {
    cout << "    " << name << ": " << value << endl;
  }
}

void Result::addLocalAllResult(const Result &other) {
  addLocalAbortCounts(other.local_abort_counts_);
  addLocalBatchAbortCounts(other.local_batch_abort_counts_);
  addLocalCommitCounts(other.local_commit_counts_);
  addLocalBatchCommitCounts(other.local_batch_commit_counts_);
  addLocalSuccessForwarding(other.local_success_fw_);
}

void Result::displayPerTxResult(std::map<uint32_t,std::string> tx_types) {
  for (auto& [type, name] : tx_types) {
    long double rate = (double) total_abort_counts_per_tx_[type] /
                       (double) (total_commit_counts_per_tx_[type] + total_abort_counts_per_tx_[type]);
    long double latency;
    if (total_commit_counts_per_tx_[type]) {
      latency = (double) total_latency_per_tx_[type] / (double) total_commit_counts_per_tx_[type];
    } else {
      latency = 0;
    }
    std::cout << "  Transaction type: " << name << std::endl;
    std::cout << "    commits: " << total_commit_counts_per_tx_[type] << std::endl;
    std::cout << "    aborts: " << total_abort_counts_per_tx_[type] << std::endl;
    std::cout << "    abort rate: "  << std::fixed << setprecision(4) << rate << std::endl;
    std::cout << "    latency[us]: " << std::fixed << setprecision(2) << latency << std::endl;
  }
  std::cout << "  Summary: ";
  for (auto& [type, name] : tx_types) {
    std::cout << total_commit_counts_per_tx_[type]
              << ","
              << total_abort_counts_per_tx_[type]
              << ",";
  }
  std::cout << std::endl;
  std::cout << "  All transaction total latencies: ";
  for (auto& [type, name] : tx_types) {
    std::cout << total_latency_per_tx_[type]
              << ",";
  }
  std::cout << std::endl;
}

void Result::addLocalPerTxResult(const Result &other, std::map<uint32_t,std::string> tx_types) {
  for (auto& [type, name] : tx_types) {
    total_commit_counts_per_tx_[type] += other.local_commit_counts_per_tx_[type];
    total_abort_counts_per_tx_[type] += other.local_abort_counts_per_tx_[type];
    total_latency_per_tx_[type] += other.local_latency_per_tx_[type];
  }
}
