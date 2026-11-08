#include <algorithm>
#include <atomic>
#include <cstdio>
#include <iostream>
#include <string>
#include <vector>

#include "common/engine/atomic_tool.hh"
#include "include/common.hh"
#include "include/transaction.hh"
#include "common/engine/txid.hh"
#include "common/engine/version.hh"

extern void ozeLeaderWork(uint64_t &epoch_timer_start, uint64_t &epoch_timer_stop);
extern std::vector<Result> OzeResult;

using namespace std;

/**
 * @brief Starts a transaction, or restarts one after an abort.
 * 
 * The thread registers at the global epoch, but the transaction is labeled with the minimum
 * epoch over active workers, and its reclamation floor is one below that minimum.
 */
void TxExecutor::begin() {
    if (l1d_on()) { l1d_t0_ = rdtscp(); l1d_val_ = 0; l1d_facts_ = 0; l1d_trav_ = 0; l1d_reads_ = 0; l1d_imp_ = 0; l1d_fp_ = 0; l1d_hist_ = 0; }
    reset_read_set_index();
    this->diag_ = DiagVal{};
    this->status_ = TransactionStatus::inflight;
    this->transaction_open_ = true;
    this->last_published_tuple_ = nullptr;

    const uint64_t ge = atomicLoadGE();
    atomicStoreThLocalEpoch(thid_, ge);
    auto e = get_min_epoch(); // align with slowest worker's epoch
    const uint64_t label = e;
    if (this->txid_.epoch != label) {
        this->txid_.tid = 0;
    } else {
        if (this->txid_.tid == TxID::kMaxTid) {
            std::cerr << "FATAL: TxID per-worker counter exhausted in epoch "
                      << e << " on worker " << static_cast<unsigned>(thid_)
                      << std::endl;
            std::abort();
        }
        this->txid_.tid++;
    }
    this->txid_.epoch = label;
    this->reclamation_epoch_ = e - 1;
    // The registries take the minimum announced floor over live transactions.
    ReclamationFloorAnnouncer::announce(this->thid_, this->reclamation_epoch_);

    // reset the flags for insert-only transactions
    this->has_write_ = false;
    this->has_insert_ = false;
    this->scan_conflict_found_ = false;

    // just for statistics
    this->is_forwarded_ = false;
}

Status TxExecutor::read(Storage s, std::string_view key, TupleBody** body) {
    Status ret = Status::OK;
    Tuple *tuple;
    Version *ver = nullptr;
    ReadElement<Tuple>* re;
    WriteElement<Tuple>* we;

    // Read own writes, or re-read from the local read set.
    re = searchReadSet(s, key);
    if (re) {
        *body = &(re->ver_->body_);
        goto FINISH_READ;
    }
    we = searchWriteSet(s, key);
    if (we) {
        *body = &(we->new_ver_->body_);
        goto FINISH_READ;
    }

    // Search the versions.
    tuple = Masstrees[get_storage(s)].get_value(key);
    if (tuple == nullptr) {
        ret = Status::WARN_NOT_FOUND;
        goto FINISH_READ;
    }

    ret = read_internal(s, key, tuple, &ver);
    if (ret == Status::WARN_NOT_FOUND) {
        goto FINISH_READ;
    }
    if (ret != Status::OK) {
        goto ABORT_READ;
    }

    // Read the payload.
    *body = &(ver->body_);

FINISH_READ:
    return ret;

ABORT_READ:
    this->status_ = TransactionStatus::aborted;
    return Status::WARN_NOT_FOUND;
}

/**
 * @brief Chooses a version under the record latch and records the read; Saros also publishes it.
 * 
 * Publication at read time keeps a read visible to later writers even if validation skips it.
 * @param s The table.
 * @param key The key.
 * @param tuple The record.
 * @param return_ver Receives the version read.
 * @return OK, WARN_NOT_FOUND for a deleted row, or an error that aborts the transaction.
 */
Status TxExecutor::read_internal(Storage s, std::string_view key, Tuple* tuple, Version** return_ver) {
    TxID target;
    Version *ver = nullptr;
    Status stat = Status::OK;

    tuple->lock_.w_lock();

    if (!reconnoitering_) {
      tuple->graph_.emplace(this->txid_, TxNode(this->txid_));
    }

    stat = get_visible_version(tuple, &ver);

    if (stat != Status::OK) {
        goto OUT;
    }
    if (ver == nullptr) {
        ERR;
    }
    if (ver->status_ == VersionStatus::deleted) {
        stat = Status::WARN_NOT_FOUND;
        goto OUT;
    }
    *return_ver = ver;
    target = ver->txid_;

    if (!reconnoitering_) {
      tuple->graph_.at(this->txid_).readSet_.emplace(row_key(tuple), target);
      tuple->graph_.at(this->txid_).from_.emplace(target);
      if (FLAGS_saros) {
        // The row was deleted after the index lookup: its history is closed, so the read cannot
        // be published.
        if (row_detached(tuple)) {
            if (this->status_ != TransactionStatus::invalid) {
                this->status_ = TransactionStatus::aborted;
            }
            stat = Status::ERROR_PREEMPTIVE_ABORT;
            goto OUT;
        }
        append_publish_read(tuple, this->txid_, target);
        if (!maybe_retire_record_history(tuple)) {
            if (this->status_ != TransactionStatus::invalid) {
                this->status_ = TransactionStatus::aborted;
            }
            stat = Status::ERROR_PREEMPTIVE_ABORT;
            goto OUT;
        }
      }
    }

    this->read_set_.emplace_back(s, key, tuple, ver, target, tuple->tuple_id_);
    this->readSet_.emplace(row_key(tuple), target);

OUT:
    tuple->lock_.w_unlock();
    return stat;
}

Status TxExecutor::write(Storage s, std::string_view key, TupleBody&& body) {
    Status ret = Status::OK;
    Tuple *tuple;

    if (searchWriteSet(s, key)) goto FINISH_WRITE;

    ReadElement<Tuple> *re;
    re = searchReadSet(s, key);
    if (re) {
        tuple = re->rcdptr_;
    } else {
        tuple = Masstrees[get_storage(s)].get_value(key);
        if (tuple == nullptr) {
            ret = Status::WARN_NOT_FOUND;
            goto FINISH_WRITE;
        }
    }

    this->writeSet_.emplace(row_key(tuple));

    Version *new_ver;
    new_ver = newVersionGeneration(tuple, std::move(body));
    write_set_.emplace_back(s, key, tuple, new_ver, OpType::UPDATE);
    has_write_ = true;

FINISH_WRITE:
  return ret;
}

Status TxExecutor::insert(Storage s, std::string_view key, TupleBody&& body) {
    if (searchWriteSet(s, key)) return Status::WARN_ALREADY_EXISTS;

    Tuple* tuple = Masstrees[get_storage(s)].get_value(key);
    if (tuple != nullptr) {
        return Status::WARN_ALREADY_EXISTS;
    }

    tuple = new Tuple();
    tuple->init(this->txid_, std::move(body));

    Status stat = Masstrees[get_storage(s)].insert_value(key, tuple);
    if (stat == Status::WARN_ALREADY_EXISTS) {
        // Never published in the index, so it can be freed at once.
        RowRetirementRegistry::counters().unpublished_insert_frees.fetch_add(
            1, std::memory_order_relaxed);
        delete tuple;
        return stat;
    }
    this->writeSet_.emplace(row_key(tuple));

    write_set_.emplace_back(s, key, tuple, tuple->ldAcqLatest(), OpType::INSERT);
    has_insert_ = true;

  return Status::OK;
}

Status TxExecutor::delete_record(Storage s, std::string_view key) {
  // cancel previous write
  for (auto itr = write_set_.begin(); itr != write_set_.end(); ++itr) {
    if ((*itr).storage_ != s) continue;
    if ((*itr).key_ == key) {
      write_set_.erase(itr);
    }
  }

  Tuple* tuple;
  ReadElement<Tuple> *re;
  re = searchReadSet(s, key);
  if (re) {
    tuple = re->rcdptr_;
  } else {
    tuple = Masstrees[get_storage(s)].get_value(key);
    if (tuple == nullptr) return Status::WARN_NOT_FOUND;
  }

  this->writeSet_.emplace(row_key(tuple));

  Version *new_ver;
  new_ver = newVersionGeneration(tuple);
  write_set_.emplace_back(s, key, tuple, new_ver, OpType::DELETE);
  has_write_ = true; // since delete is a sort of update

  return Status::OK;
}

Status TxExecutor::scan(const Storage s,
                        std::string_view left_key, bool l_exclusive,
                        std::string_view right_key, bool r_exclusive,
                        std::vector<TupleBody*>& result) {
  return scan(s, left_key, l_exclusive, right_key, r_exclusive, result, -1);
}

// Range read: the scan is recorded in the scan history (inserts into the range
// get a scanner -> inserter edge), then every row is read as by read().
Status TxExecutor::scan(const Storage s,
                        std::string_view left_key, bool l_exclusive,
                        std::string_view right_key, bool r_exclusive,
                        std::vector<TupleBody*>& result, int64_t limit) {
  result.clear();
  auto rset_init_size = read_set_.size();

  if (!reconnoitering_) {
    addScanEntry(s, left_key, l_exclusive, right_key, r_exclusive);
  }

  std::vector<Tuple*> scan_res;
  Masstrees[get_storage(s)].scan(
      left_key.empty() ? nullptr : left_key.data(), left_key.size(),
      l_exclusive, right_key.empty() ? nullptr : right_key.data(),
      right_key.size(), r_exclusive, &scan_res, limit);

  for (auto &&itr : scan_res) {
    // A copy: the latest version may change while we read.
    std::string key(itr->latest_.load(memory_order_acquire)->body_.get_key());
    ReadElement<Tuple>* re = searchReadSet(s, key);
    if (re) {
      result.emplace_back(&(re->ver_->body_));
      continue;
    }

    WriteElement<Tuple>* we = searchWriteSet(s, std::string_view(key));
    if (we) {
      result.emplace_back(&(we->new_ver_->body_));
      continue;
    }

    Version* ver = nullptr;
    Status stat = read_internal(s, key, itr, &ver);
    if (stat != Status::OK && stat != Status::WARN_NOT_FOUND) {
        status_ = TransactionStatus::aborted;
        return stat;
    }
  }

  if (rset_init_size != read_set_.size()) {
    for (auto itr = read_set_.begin() + rset_init_size;
         itr != read_set_.end(); ++itr) {
      result.emplace_back(&((*itr).ver_->body_));
    }
  }

  return Status::OK;
}

/**
 * @brief Validates the transaction record by record; true means it may commit.
 * 
 * An insert-only transaction that read from one writer L only skips its read validation (the
 * single-writer rule): it has only wr edges from L and rw edges to writers newer than L, so it
 * can be serialized right after L, and its reads were already published. Mixed writers abort;
 * a scan covering its inserts sends it to full validation. Everything else validates its writes,
 * its reads and the targets discovered on the way.
 * @return False if the transaction must abort. status_ becomes invalid when the run ends.
 */
bool TxExecutor::validation() {
    // Saros: register the read set once, in the footprint registry.
    if (FLAGS_saros && !this->readSet_.empty()) {
        auto fp = std::make_shared<ReadFootprintRegistry::Keys>();
        fp->reserve(this->readSet_.size());
        for (const auto& [k, _] : this->readSet_) fp->push_back(k);
        if (l1d_on()) l1d_facts_ += fp->size();
        ReadFootprintRegistry::instance().publish(this->txid_, std::move(fp),
                                                  this->reclamation_epoch_);
    }

    KeySet finished_set;
    KeySet propagate_set;

    bool insert_only = FLAGS_validate_insert_only_reads && FLAGS_insert_only_single_writer &&
                       !has_write_ && has_insert_ && !this->readSet_.empty();

    this->graph_.emplace(this->txid_, TxNode(this->txid_));
    this->graph_.at(this->txid_).status_ = TransactionStatus::validating;

    // Read-only: each record read is validated, without discovery, including rows deleted since.
    if (this->writeSet_.size() == 0) {
        KeySet crown_rows;
        for (const auto& re : this->read_set_) crown_rows.insert(re.rcdptr_);
        for (Tuple* const tpl : crown_rows) {
            tpl->lock_.w_lock();
            if (FLAGS_saros) {
                const uint64_t upto_seq = row_committed_seq(tpl);
                if (!import_record(tpl, upto_seq, this->graph_)) {
                    if (this->status_ != TransactionStatus::invalid)
                        this->status_ = TransactionStatus::aborted;
                    tpl->lock_.w_unlock();
                    goto ABORT_VALIDATION;
                }
                widen_citation_to_imported(tpl, upto_seq);
                if (has_cycle(this->graph_, this->txid_)) {
                    if (this->status_ != TransactionStatus::invalid)
                        this->status_ = TransactionStatus::aborted;
                    tpl->lock_.w_unlock();
                    goto ABORT_VALIDATION;
                }
                // Reference chain, as in read_validation().
                if (last_published_tuple_ != nullptr && last_published_tuple_ != tpl) {
                    if (auto it = published_seq_.find(last_published_tuple_); it != published_seq_.end())
                        append_include(tpl, last_published_tuple_, it->second);
                }
                if (auto it = published_seq_.find(tpl); it != published_seq_.end())
                    last_published_tuple_ = tpl;
            } else {
                merge(tpl->graph_, this->graph_);
                if (has_cycle(tpl->graph_, this->txid_)) {
                    if (this->status_ != TransactionStatus::invalid)
                        this->status_ = TransactionStatus::aborted;
                    tpl->lock_.w_unlock();
                    goto ABORT_VALIDATION;
                } else {
                    this->graph_.clear();
                    this->graph_.insert(tpl->graph_.begin(), tpl->graph_.end());
                }
            }
            tpl->lock_.w_unlock();
        }
        goto FINISH_VALIDATION;
    }

    // Insert-only without reads: only scanner -> inserter conflicts; without
    // --validate_insert_only_reads, also with reads.
    if (!has_write_ && has_insert_ &&
        (this->readSet_.empty() || !FLAGS_validate_insert_only_reads)) {
      for (auto& we : this->write_set_) {
        if (!insert_validation(we))
          goto ABORT_VALIDATION;
        finished_set.insert(we.rcdptr_);
      }
      goto FINISH_VALIDATION;
    }

    // Single-writer rule: with reads from one writer L only, the transaction can
    // be serialized right after L. Mixed writers abort.
    if (insert_only && !read_set_single_writer()) {
        InsertOnlyMixedAborts.fetch_add(1, std::memory_order_relaxed);
        if (this->status_ != TransactionStatus::invalid)
            this->status_ = TransactionStatus::aborted;
        goto ABORT_VALIDATION;
    }

    for (auto& we : this->write_set_) {
        if (we.op_ == OpType::INSERT) {
            if (!insert_validation(we))
                goto ABORT_VALIDATION;
            finished_set.insert(we.rcdptr_);
        } else { // OpType::UPDATE or OpType::DELETE
            if (!write_validation(we, finished_set, propagate_set))
                goto ABORT_VALIDATION;
        }
        if (loadAcquire(this->quit_)) {
            this->status_ = TransactionStatus::invalid;
            goto FINISH_VALIDATION;
        }
    }

    if (insert_only) {
        if (!scan_conflict_found_) {
            InsertOnlySkips.fetch_add(1, std::memory_order_relaxed);
            goto FINISH_VALIDATION;
        }
        // A scanner may depend on the inserts; its edges need the full check.
        InsertOnlyScanFalls.fetch_add(1, std::memory_order_relaxed);
    }

    // This transaction's own reads, from the rows it holds.
    for (const auto& re : this->read_set_) {
        if (this->writeSet_.find(row_key(re.rcdptr_)) == this->writeSet_.end())
            propagate_set.insert(re.rcdptr_);
    }

    // read_validation() returns early with targets left only when the run ends, with status_ invalid.
    if (!read_validation(propagate_set, finished_set)) {
        goto ABORT_VALIDATION;
    }

FINISH_VALIDATION:
    this->diag_fan_record();
    return true;

ABORT_VALIDATION:
    this->diag_fan_record();
    return false;
}

/**
 * @brief Validates one written record.
 * 
 * Adds an edge from every reader of the record to this transaction; on a cycle, order forwarding
 * places the new version before the writers of the versions those readers read. Saros imports the
 * history, publishes the edges and a prefix reference to the previous record, and runs discovery;
 * Oze merges and copies its read and write sets into the record graph.
 * @param element The write.
 * @param finished_set Records already validated.
 * @param propagate_set Receives the targets discovered.
 * @return False if the transaction must abort.
 */
bool TxExecutor::write_validation(WriteElement<Tuple> element, KeySet& finished_set, KeySet& propagate_set) {
    Status stat;
    TxSet decided, reachable_to;
    TxSet readers;
    TxSet followers;
    // `followers` before insert_version() consumes it, for publication.
    TxSet forwarded_followers;
    bool forwarded = false;
    KeySet followers_read_keys;
    Tuple* tuple = element.rcdptr_;
    OpType op = element.op_;
    bool legacy_cycle = false;

    tuple->lock_.w_lock();

    if (FLAGS_saros) {
        // Writing a row deleted after this transaction reached it: abort.
        if (row_detached(tuple)) {
            if (this->status_ != TransactionStatus::invalid)
                this->status_ = TransactionStatus::aborted;
            goto ABORT_VALIDATION;
        }
        const uint64_t upto_seq = tuple->graph_log().committed();

        if (!import_record(tuple, upto_seq, this->graph_)) {
            goto ABORT_VALIDATION;
        }
        widen_citation_to_imported(tuple, upto_seq);

        find_readers(this->graph_, tuple, readers);
        for (const auto& r : readers) {
            if (this->txid_.epoch < r.epoch - 1) {
                goto ABORT_VALIDATION;
            }
            this->graph_.at(r).writtenBy_.insert(this->txid_);
            this->graph_.at(this->txid_).from_.insert(r);
        }

        const bool found_cycle = has_cycle(this->graph_, this->txid_);
        if (found_cycle) {
            if (op == OpType::DELETE) {
                goto ABORT_VALIDATION;
            }
            find_followers(this->graph_, tuple, readers, followers);
            // The reader -> me edges are not published yet; undo them locally.
            for (const auto& r : readers) {
                this->graph_.at(r).writtenBy_.erase(this->txid_);
                this->graph_.at(this->txid_).from_.erase(r);
            }
            for (const auto& f : followers) {
                if (f.epoch < this->txid_.epoch - 1) {
                    // Do not forward to the position before the tx in the previous epoch
                    goto ABORT_VALIDATION;
                }
                if (auto it = this->graph_.find(f); it == this->graph_.end()) {
                    this->graph_.emplace(f, TxNode(f));
                }
                this->graph_.at(this->txid_).writtenBy_.insert(f);
                this->graph_.at(f).from_.insert(this->txid_);
            }
            if (has_cycle(this->graph_, this->txid_)) {
                goto ABORT_VALIDATION;
            }
            forwarded_followers = followers;
            forwarded = true;
            this->is_forwarded_ = true;
        }

        // inserts write version to the appropriate position in the linked list
        stat = insert_version(element, followers);
        if (stat != Status::OK) {
            goto ABORT_VALIDATION;
        }

        if (last_published_tuple_ != nullptr && last_published_tuple_ != tuple) {
            if (auto it = published_seq_.find(last_published_tuple_); it != published_seq_.end()) {
                append_include(tuple, last_published_tuple_, it->second);
            }
        }
        if (forwarded) {
            for (const auto& f : forwarded_followers) {
                append_edge(tuple, this->txid_, f, GraphEdgeKind::WrittenBy);
            }
        } else {
            for (const auto& r : readers) {
                append_edge(tuple, r, this->txid_, GraphEdgeKind::WrittenBy);
            }
        }
        if (auto it = published_seq_.find(tuple); it != published_seq_.end()) {
            last_published_tuple_ = tuple;
        }

        discover_propagation_targets(this->graph_, finished_set, propagate_set);

        if (!maybe_retire_record_history(tuple)) {
            goto ABORT_VALIDATION;
        }

        // Reads still add to the record graph; prune it as Oze does, skipping gc() at an unchanged floor.
        if (!tuple->graph_gc_ran_ || this->reclamation_epoch_ > tuple->graph_gc_epoch_) {
            gc(tuple->graph_, this->reclamation_epoch_);
            tuple->graph_gc_epoch_ = this->reclamation_epoch_;
            tuple->graph_gc_ran_ = true;
        }

        tuple->lock_.w_unlock();
        finished_set.insert(tuple);
        if (loadAcquire(this->quit_)) {
            this->status_ = TransactionStatus::invalid;
            return true;
        }
        goto FINISH_VALIDATION;
    }

    tuple->graph_.emplace(this->txid_, TxNode(this->txid_));
    tuple->graph_.at(this->txid_).status_ = TransactionStatus::validating;

    // merge graph
    merge(tuple->graph_, this->graph_);
    gc(tuple->graph_, this->reclamation_epoch_);

    find_readers(tuple->graph_, tuple, readers);
    for (const auto& r : readers) {
        if (this->txid_.epoch < r.epoch - 1) {
            goto ABORT_VALIDATION;
        }
        tuple->graph_.at(r).writtenBy_.insert(this->txid_);
        tuple->graph_.at(this->txid_).from_.insert(r);
    }

    legacy_cycle = has_cycle(tuple->graph_, this->txid_);
    if (legacy_cycle) {
        if (op == OpType::DELETE) {
            goto ABORT_VALIDATION;
        }
        find_followers(tuple->graph_, tuple, readers, followers);
        for (const auto& r : readers) {
            if (auto it = decided.find(r); it == decided.end()) {
                tuple->graph_.at(r).writtenBy_.erase(this->txid_);
                tuple->graph_.at(this->txid_).from_.erase(r);
            }
        }
        for (const auto& f : followers) {
            if (f.epoch < this->txid_.epoch - 1) {
                // Do not forward to the position before the tx in the previous epoch
                goto ABORT_VALIDATION;
            }
            tuple->graph_.at(this->txid_).writtenBy_.insert(f);
            tuple->graph_.at(f).from_.insert(this->txid_);
        }
        if (has_cycle(tuple->graph_, this->txid_)) {
            goto ABORT_VALIDATION;
        }
    } else {
        for (const auto& r : readers) {
            decided.insert(r);
        }
    }

    tuple->graph_.at(this->txid_).readSet_.insert(
        this->readSet_.begin(), this->readSet_.end()
    );
    tuple->graph_.at(this->txid_).writeSet_.insert(this->writeSet_.begin(), this->writeSet_.end());

    // insert write version to the appropriate position in the linked list
    stat = insert_version(element, followers);
    if (stat != Status::OK) {
        goto ABORT_VALIDATION;
    }

    // Propagation targets: the records read by transactions reachable from this one.
    find_reachable_to(this->txid_, tuple->graph_, reachable_to);
    for (auto& txid : reachable_to) {
        add_related_read_keys(&tuple->graph_.at(txid), followers_read_keys);
    }
    for (const auto& k : followers_read_keys) {
        (void)k;
        push_candidate_keys(propagate_set, finished_set, followers_read_keys);
    }

    this->graph_.clear();
    this->graph_.insert(tuple->graph_.begin(), tuple->graph_.end());

    tuple->lock_.w_unlock();
    finished_set.insert(tuple);
    if (loadAcquire(this->quit_)) {
        this->status_ = TransactionStatus::invalid;
        return true;
    }

FINISH_VALIDATION:
    return true;

ABORT_VALIDATION:
    if (!FLAGS_saros) {
        clean_up_node_edges(tuple->graph_, this->txid_);
    }
    tuple->lock_.w_unlock();
    return false;
}

/**
 * @brief Validates one inserted record against the scans of this epoch.
 * 
 * A scan whose range contains the new key gets a scanner -> inserter edge, published by Saros,
 * and sends an insert-only transaction to full validation.
 * @param element The insert.
 * @return False if the edge closes a cycle.
 */
bool TxExecutor::insert_validation(WriteElement<Tuple> element) {
    Tuple* tuple = element.rcdptr_;
    Storage s = element.storage_;
    string_view key = element.key_;
    ScanEntry *prev, *entry;
    int history_offset = SCAN_HISTORY_INDEX(s, this->txid_.epoch);

    tuple->lock_.w_lock();

    if (FLAGS_saros) {
        // The reference chain continues whether or not a scanner conflict is found.
        if (last_published_tuple_ != nullptr && last_published_tuple_ != tuple) {
            if (auto it = published_seq_.find(last_published_tuple_); it != published_seq_.end()) {
                append_include(tuple, last_published_tuple_, it->second);
            }
        }
    }

    // check scan index
    uint64_t range = ScanRange[history_offset].load(std::memory_order_acquire);
    uint32_t min = range >> 32;
    uint32_t max = range & 0xffffffff;
    uint32_t prefix = *reinterpret_cast<const uint32_t*>(key.data());
    if (prefix < min || max < prefix) {
      goto OUT;
    }

    // check scan history
    prev = nullptr;
    entry = ScanHistory[history_offset].load(std::memory_order_acquire);
    while (entry && entry->txid_.epoch == this->txid_.epoch) {
        if (entry->txid_ != txid_ && entry->contains(element.key_)) {
            scan_conflict_found_ = true;
            // add edge; scanner to inserter
            if (auto it = this->graph_.find(entry->txid_); it == this->graph_.end())
                this->graph_.emplace(entry->txid_, TxNode(entry->txid_));
            this->graph_.at(entry->txid_).writtenBy_.insert(this->txid_);
            this->graph_.at(this->txid_).from_.insert(entry->txid_);
            if (FLAGS_saros) {
                append_edge(tuple, entry->txid_, this->txid_, GraphEdgeKind::WrittenBy);
            }
            // check cycle
            if (has_cycle(this->graph_, this->txid_)) {
                tuple->lock_.w_unlock();
                return false;
            }
        }
        prev = entry;
        entry = entry->ldAcqNext();
    }

    if (entry && entry->txid_.epoch <= reclamation_epoch_) {
        if (prev == nullptr) {
            if (ScanHistory[history_offset].compare_exchange_strong(entry, nullptr,
                memory_order_acq_rel, memory_order_acquire)) {
                delete entry;
            }
        } else {
            if (prev->next_.compare_exchange_strong(entry, nullptr,
                memory_order_acq_rel, memory_order_acquire)) {
                while (entry) {
                    prev = entry;
                    entry = entry->ldAcqNext();
                    delete prev;
                }
            }
        }
    }

OUT:
    // Oze writes the working graph back into the record graph.
    if (FLAGS_saros) {
        if (auto it = published_seq_.find(tuple); it != published_seq_.end()) {
            last_published_tuple_ = tuple;
        }
    } else {
        tuple->graph_.insert(this->graph_.begin(), this->graph_.end());
    }

    tuple->lock_.w_unlock();

    return true;
}

/**
 * @brief Validates the records in target_set until none is left; discovery may add targets.
 * 
 * Saros imports each history, checks for a cycle, cites the previous record and runs discovery.
 * An insert-only transaction (with --short_discovery_limit, any that read fewer records) is aborted
 * and retried once its targets exceed the limit. Oze merges record graphs instead.
 * @param target_set The records left to validate.
 * @param finished_set Records already validated.
 * @return False if the transaction must abort.
 */
bool TxExecutor::read_validation(KeySet& target_set, KeySet& finished_set) {
    while (!target_set.empty()) {
        auto iterator = target_set.cbegin();
        Tuple* tuple = *iterator;

        tuple->lock_.w_lock();

        if (FLAGS_saros) {
            // A row deleted since is imported by identity.
            const uint64_t upto_seq = row_committed_seq(tuple);
            if (!import_record(tuple, upto_seq, this->graph_)) {
                tuple->lock_.w_unlock();
                goto ABORT_VALIDATION;
            }
            widen_citation_to_imported(tuple, upto_seq);

            if (has_cycle(this->graph_, this->txid_)) {
                tuple->lock_.w_unlock();
                goto ABORT_VALIDATION;
            }

            if (last_published_tuple_ != nullptr && last_published_tuple_ != tuple) {
                if (auto it = published_seq_.find(last_published_tuple_); it != published_seq_.end()) {
                    append_include(tuple, last_published_tuple_, it->second);
                }
            }
            if (auto it = published_seq_.find(tuple); it != published_seq_.end()) {
                last_published_tuple_ = tuple;
            }

            const bool insert_only_limit =
                FLAGS_insert_only_discovery_limit != 0 && !has_write_ && has_insert_;
            const bool short_limit = !insert_only_limit && FLAGS_short_discovery_limit != 0 &&
                this->read_set_.size() < FLAGS_short_discovery_limit;
            const bool limit_applies = insert_only_limit || short_limit;
            const uint64_t limit = insert_only_limit ? FLAGS_insert_only_discovery_limit
                                                     : FLAGS_short_discovery_limit;
            discover_propagation_targets(this->graph_, finished_set, target_set);
            const bool over_limit = limit_applies &&
                target_set.size() + finished_set.size() > limit;
            if (over_limit) {
                tuple->lock_.w_unlock();
                goto ABORT_VALIDATION;
            }

            if (!maybe_retire_record_history(tuple)) {
                tuple->lock_.w_unlock();
                goto ABORT_VALIDATION;
            }

            // As in write_validation().
            if (!tuple->graph_gc_ran_ || this->reclamation_epoch_ > tuple->graph_gc_epoch_) {
                gc(tuple->graph_, this->reclamation_epoch_);
                tuple->graph_gc_epoch_ = this->reclamation_epoch_;
                tuple->graph_gc_ran_ = true;
            }
        } else {
            tuple->graph_.emplace(this->txid_, TxNode(this->txid_));
            tuple->graph_.at(this->txid_).status_ = TransactionStatus::validating;

            // merge graph
            merge(tuple->graph_, this->graph_);
            gc(tuple->graph_, this->reclamation_epoch_);

            if (has_cycle(tuple->graph_, this->txid_)) {
                clean_up_node_edges(tuple->graph_, this->txid_);
                tuple->lock_.w_unlock();
                goto ABORT_VALIDATION;
            }

            tuple->graph_.at(this->txid_).readSet_.insert(
                this->readSet_.begin(), this->readSet_.end()
            );
            tuple->graph_.at(this->txid_).writeSet_.insert(this->writeSet_.begin(), this->writeSet_.end());

            // for propagation
            KeySet keys;
            TxSet reachable_to;
            find_reachable_to(this->txid_, tuple->graph_, reachable_to);
            for (auto& txid : reachable_to) {
                add_related_read_keys(&tuple->graph_.at(txid), keys);
            }
            for (const auto& k : keys) {
                (void)k;
                push_candidate_keys(target_set, finished_set, keys);
            }

            this->graph_.clear();
            this->graph_.insert(tuple->graph_.begin(), tuple->graph_.end());
        }

        tuple->lock_.w_unlock();
        finished_set.insert(tuple);
        target_set.erase(iterator);

        if (loadAcquire(this->quit_)) {
            this->status_ = TransactionStatus::invalid;
            return true;
        }
    }

    return true;

ABORT_VALIDATION:
    return false;
}

/**
 * @brief Aborts the transaction: its published facts stop counting, and its local state is cleared.
 */
void TxExecutor::abort() {

    if (FLAGS_saros) {
        // Append an abort mark to every history this transaction appended to.
        std::vector<Tuple*> published_tuples;
        published_tuples.reserve(published_seq_.size());
        for (const auto& [tuple, _] : published_seq_) {
            if (tuple != nullptr) {
                published_tuples.push_back(tuple);
            }
        }

        for (auto* tuple : published_tuples) {
            tuple->lock_.w_lock();
            append_abort(tuple, this->txid_);
            (void)maybe_retire_record_history(tuple);
            tuple->lock_.w_unlock();
        }
    }

    writeSetClean();
    read_set_.clear();

    graph_.clear();
    readSet_.clear();
    writeSet_.clear();
    last_imported_seq_.clear();
    published_seq_.clear();
    published_prefix_epoch_.clear();
    import_visited_.clear();
    import_aborted_.clear();
    reset_identity_caches();
    last_published_tuple_ = nullptr;
    history_write_pred_.clear();
    transaction_open_ = false;

    mainte();

    backoff();
}

// Per-pass budget of the state, row and version reclamation passes.
constexpr uint64_t kReclaimBudget = 256;

void TxExecutor::mainte() {
  reclaim_record_graph_states();
  reclaim_retired_rows();
  reclaim_retired_versions();
  reclaim_retired_chunks();
}

/**
 * @brief Periodic pass that frees detached histories, bounded by the floor of future transactions.
 */
void TxExecutor::reclaim_record_graph_states() {
  const uint64_t interval = std::max<uint64_t>(1, FLAGS_reclaim_interval);
  if (++state_reclaim_tick_ < interval) return;
  state_reclaim_tick_ = 0;
  // get_min_epoch() - 1 bounds the floor of every future transaction.
  const uint64_t min_epoch = get_min_epoch();
  const uint64_t min_future_floor = min_epoch == 0 ? 0 : min_epoch - 1;
  RecordGraphRegistry::instance().reclaim(
      atomicLoadGE(), min_future_floor, kReclaimBudget);
}

// Frees retired rows at the floor min(announced live minimum, get_min_epoch() - 1).
void TxExecutor::reclaim_retired_rows() {
  const uint64_t interval = std::max<uint64_t>(1, FLAGS_reclaim_interval);
  if (++row_reclaim_tick_ < interval) return;
  row_reclaim_tick_ = 0;
  const uint64_t min_epoch = get_min_epoch();
  const uint64_t min_future_floor = min_epoch == 0 ? 0 : min_epoch - 1;
  const uint64_t live_floor =
      std::min(ReclamationFloorAnnouncer::min_live(), min_future_floor);
  RowRetirementRegistry::instance().reclaim(live_floor, kReclaimBudget);
}

// Frees retired versions of aborted updates and deletes, at the same floor.
void TxExecutor::reclaim_retired_versions() {
  const uint64_t interval = std::max<uint64_t>(1, FLAGS_reclaim_interval);
  if (++version_reclaim_tick_ < interval) return;
  version_reclaim_tick_ = 0;
  const uint64_t min_epoch = get_min_epoch();
  const uint64_t min_future_floor = min_epoch == 0 ? 0 : min_epoch - 1;
  const uint64_t live_floor =
      std::min(ReclamationFloorAnnouncer::min_live(), min_future_floor);
  VersionRetirementRegistry::instance().reclaim(live_floor, kReclaimBudget);
}

/**
 * @brief Periodic pass that frees unlinked history chunks and superseded directories.
 * 
 * The floor is min(announced live minimum, get_min_epoch() - 1); an entry is freed only once
 * the floor is strictly above its tag.
 */
void TxExecutor::reclaim_retired_chunks() {
  if (!FLAGS_free_retired_history_chunks) return;
  const uint64_t interval = std::max<uint64_t>(1, FLAGS_reclaim_interval);
  if (++chunk_reclaim_tick_ < interval) return;
  chunk_reclaim_tick_ = 0;
  const uint64_t min_epoch = get_min_epoch();
  const uint64_t min_future_floor = min_epoch == 0 ? 0 : min_epoch - 1;
  const uint64_t live_floor =
      std::min(ReclamationFloorAnnouncer::min_live(), min_future_floor);
  ChunkRetirementRegistry::instance().reclaim(
      live_floor, FLAGS_free_retired_history_chunks_budget);
}

/**
 * @brief The epoch tag of an unlinked chunk or superseded directory, read after the unlinking stores.
 * @return The global epoch.
 */
uint64_t history_chunk_retire_tag() {
  std::atomic_thread_fence(std::memory_order_seq_cst);
  return atomicLoadGE();
}

/**
 * @brief Commits the transaction's versions and clears its local state; validation has passed.
 */
void TxExecutor::writePhase() {
    for (auto we : write_set_) {
        commit_version(we);
    }

    if (!FLAGS_history_dump_path.empty()) {
        append_history_record();
    }

    read_set_.clear();
    write_set_.clear();
    readSet_.clear();
    writeSet_.clear();
    graph_.clear();
    last_imported_seq_.clear();
    published_seq_.clear();
    published_prefix_epoch_.clear();
    import_visited_.clear();
    import_aborted_.clear();
    reset_identity_caches();
    last_published_tuple_ = nullptr;
    history_write_pred_.clear();
    transaction_open_ = false;
}

/**
 * @brief Validates and, on success, installs the transaction.
 * @return False if it must abort or was abandoned at the end of the run; the caller then calls abort().
 */
bool TxExecutor::commit() {
  const uint64_t l1d_v0 = l1d_on() ? rdtscp() : 0;
  if (l1d_on()) l1d_reads_ = this->readSet_.size();
  if (!validation()) {
    return false;
  }
  if (l1d_on()) l1d_val_ += rdtscp() - l1d_v0;

  // Abandon a long transaction when the run ends.
  if (status_ == TransactionStatus::invalid)
    return false;

  writePhase();
  mainte();
  if (l1d_on()) {
    const double us = static_cast<double>(FLAGS_clocks_per_us);
    std::fprintf(stderr, "#l1cost total_us=%.1f val_us=%.1f facts=%lu reads=%lu trav=%lu imported=%lu fp_scanned=%lu hist=%lu\n",
                 (rdtscp() - l1d_t0_) / us, l1d_val_ / us,
                 (unsigned long)l1d_facts_, (unsigned long)l1d_reads_, (unsigned long)l1d_trav_,
                 (unsigned long)l1d_imp_, (unsigned long)l1d_fp_, (unsigned long)l1d_hist_);
  }
  return true;
}

// Reconnoitering (BoMB's target selection, as if from a cache): reads choose versions
// without recording anything; reconnoiter_end() restarts the transaction.
void TxExecutor::reconnoiter_begin() {
    if (status_ != TransactionStatus::inflight) {
      begin();
    }
    reconnoitering_ = true;
}

void TxExecutor::reconnoiter_end() {
    read_set_.clear();
    readSet_.clear();
    graph_.clear();
    last_imported_seq_.clear();
    published_seq_.clear();
    published_prefix_epoch_.clear();
    import_visited_.clear();
    import_aborted_.clear();
    reset_identity_caches();
    last_published_tuple_ = nullptr;
    history_write_pred_.clear();
    if (write_set_.size() != 0 || writeSet_.size() != 0) {
        dump(thid_, "WARN: Write happened in reconnoitering mode.");
    }
    reconnoitering_ = false;
    begin();
}

// Thread 1 advances the global epoch (the BoMB L1 runs on thread 0).
bool TxExecutor::isLeader() {
    return this->thid_ == 1;
}

void TxExecutor::leaderWork() {
    ozeLeaderWork(epoch_timer_start_, epoch_timer_stop_);
    leaderBackoffWork(backoff_, OzeResult);
}
