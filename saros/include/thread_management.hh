#pragma once

#include <atomic>
#include <cstdint>
#include <mutex>

#include "common/cache_line_size.hh"
#include "common/engine/transaction_status.hh"

// A worker's participation in the epoch protocol.
enum class ParticipationState : uint8_t {
  Active = 0,
  Entering = 1,
  Quiescent = 2,
};

// Serializes membership changes with the scanners (get_min_epoch(), chkEpochLoaded()).
inline std::mutex EpochMembershipWriterMutex;

class EpochMembershipChange {
 public:
  EpochMembershipChange() : lock_(EpochMembershipWriterMutex) {}
  EpochMembershipChange(const EpochMembershipChange&) = delete;
  EpochMembershipChange& operator=(const EpochMembershipChange&) = delete;

 private:
  std::unique_lock<std::mutex> lock_;
};

/**
 * @brief One worker's epoch stamp and participation state, in one cache line.
 * 
 * Active: may hold protected state; counted by get_min_epoch() and chkEpochLoaded().
 * Entering: rejoining, stamp not yet refreshed; counted only by chkEpochLoaded().
 * Quiescent: waiting for a request, holds nothing; counted by neither, so that it does not
 * hold back the global epoch and every reclamation floor.
 */
class ThreadManagementEntry {
  public:
  explicit ThreadManagementEntry(uint64_t epoch) {
    this->epoch_.store(epoch, std::memory_order_relaxed);
    this->participation_.store(static_cast<uint8_t>(ParticipationState::Active),
                               std::memory_order_relaxed);
  }

  // Release/acquire: a reader that sees Active also sees the refreshed stamp.
  void atomicStoreParticipation(ParticipationState state) {
    this->participation_.store(static_cast<uint8_t>(state),
                               std::memory_order_release);
  }

  ParticipationState atomicLoadParticipation() const {
    return static_cast<ParticipationState>(
        this->participation_.load(std::memory_order_acquire));
  }

  bool countsTowardMinEpoch() const {
    return atomicLoadParticipation() == ParticipationState::Active;
  }

  bool countsTowardEpochAdvance() const {
    return atomicLoadParticipation() != ParticipationState::Quiescent;
  }

  void atomicStoreEpoch(uint64_t epoch) {
    this->epoch_.store(epoch, std::memory_order_release);
  }

  uint64_t atomicLoadEpoch() {
    return this->epoch_.load(std::memory_order_acquire);
  }

  private:
  alignas(CACHE_LINE_SIZE) std::atomic<uint64_t> epoch_;
  std::atomic<uint8_t> participation_;
};
