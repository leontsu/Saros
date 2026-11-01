#pragma once

#include <cstdint>
#include <iomanip>
#include <iostream>
#include <map>
#include <string>
#include <utility>
#include <vector>

#include "./cache_line_size.hh"

#define MAX_TX_TYPE 10

class Result {
public:
  alignas(CACHE_LINE_SIZE) uint64_t local_abort_counts_ = 0;
  alignas(CACHE_LINE_SIZE) uint64_t local_batch_abort_counts_ = 0;
  uint64_t local_commit_counts_ = 0;
  uint64_t local_batch_commit_counts_ = 0;
  uint64_t local_commit_counts_per_tx_[MAX_TX_TYPE] = {0};
  uint64_t local_abort_counts_per_tx_[MAX_TX_TYPE] = {0};
  int64_t local_latency_per_tx_[MAX_TX_TYPE] = {0};
  uint64_t local_success_fw_ = 0;
  uint64_t total_abort_counts_ = 0;
  uint64_t total_batch_abort_counts_ = 0;
  uint64_t total_commit_counts_ = 0;
  uint64_t total_batch_commit_counts_ = 0;
  uint64_t total_commit_counts_per_tx_[MAX_TX_TYPE] = {0};
  uint64_t total_abort_counts_per_tx_[MAX_TX_TYPE] = {0};
  uint64_t total_latency_per_tx_[MAX_TX_TYPE] = {0};
  uint64_t total_success_fw_ = 0;

  // Counters of freeing retired history chunks, copied in by the driver
  // after the workers have joined (snapshot_history_chunk_counters()).
  std::vector<std::pair<std::string, uint64_t>> history_chunk_lines_;

  void displayAbortCounts();
  void displayAbortRate();
  void displayCommitCounts();
  void displaySuccessForwarding();
  void displayTps(size_t extime, size_t thread_num);
  void displayHistoryChunkResult();
  void displayAllResult(size_t clocks_per_us, size_t extime, size_t thread_num);
  void displayPerTxResult(std::map<uint32_t,std::string> tx_types);
  void addLocalAllResult(const Result &other);
  void addLocalPerTxResult(const Result &other, std::map<uint32_t,std::string> tx_types);
  void addLocalAbortCounts(const uint64_t count);
  void addLocalBatchAbortCounts(const uint64_t count);
  void addLocalCommitCounts(const uint64_t count);
  void addLocalBatchCommitCounts(const uint64_t count);
  void addLocalSuccessForwarding(const uint64_t count);
};
