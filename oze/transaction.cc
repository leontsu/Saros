#include <algorithm>
#include <atomic>
#include <cstdio>
#include <iostream>
#include <string>
#include <vector>

#include "include/atomic_tool.hh"
#include "include/common.hh"
#include "include/transaction.hh"
#include "include/txid.hh"
#include "include/version.hh"

extern void ozeLeaderWork(uint64_t &epoch_timer_start, uint64_t &epoch_timer_stop);
extern std::vector<Result> OzeResult;

using namespace std;

void TxExecutor::begin() {
    if (l1d_on()) { l1d_t0_ = rdtscp(); l1d_val_ = 0; l1d_facts_ = 0; l1d_trav_ = 0; l1d_reads_ = 0; }
    reset_read_set_index();
    this->status_ = TransactionStatus::inflight;

    atomicStoreThLocalEpoch(thid_, atomicLoadGE());
    auto e = get_min_epoch(); // align with slowest worker's epoch
    if (this->txid_.epoch != e) {
        this->txid_.tid = 0;
    } else
        this->txid_.tid++;
    this->txid_.epoch = e;
    this->reclamation_epoch_ = e - 1;

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

    *body = &(ver->body_);

FINISH_READ:
    return ret;

ABORT_READ:
    this->status_ = TransactionStatus::aborted;
    return Status::WARN_NOT_FOUND;
}

Status TxExecutor::read_internal(Storage s, std::string_view key, Tuple* tuple, Version** return_ver) {
    TxID target;
    Version *ver = nullptr;
    Status stat = Status::OK;

    tuple->lock_.w_lock();

    // sync pages that I read
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
      tuple->graph_.at(this->txid_).readSet_.emplace(tuple, target);
      tuple->graph_.at(this->txid_).from_.emplace(target);
    }

    this->read_set_.emplace_back(s, key, tuple, ver, target, tuple->tuple_id_);
    this->readSet_.emplace(tuple, target);

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

    this->writeSet_.emplace(tuple);

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
        delete tuple;
        return stat;
    }
    this->writeSet_.emplace(tuple);

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

  this->writeSet_.emplace(tuple);

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

void TxExecutor::validation_worker(size_t worker_id,
    KeySet &target_set, size_t offset, size_t assignment,
    KeySet &propagate_set, Graph &graph, TransactionStatus &result) {
    // copy initial state
    graph.clear();
    graph.insert(this->graph_.begin(), this->graph_.end());

    // set assignment
    auto iterator = target_set.begin();
    auto end = target_set.begin();
    advance(iterator, offset);
    advance(end, offset + assignment);

    for (; iterator != end; iterator++) {
        if (iterator == target_set.end())
            break;
        Tuple* tuple = *iterator;

        tuple->lock_.w_lock();
        tuple->graph_.emplace(this->txid_, TxNode(this->txid_));
        tuple->graph_.at(this->txid_).status_ = TransactionStatus::validating;


        // merge graph
        merge(tuple->graph_, graph);

        if (has_cycle(tuple->graph_, this->txid_)) {
            clean_up_node_edges(tuple->graph_, this->txid_);
            tuple->lock_.w_unlock();
            goto ABORT_VALIDATION;
        }

        if (l1d_on()) l1d_facts_ += this->readSet_.size() + this->writeSet_.size();

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
            auto itr = std::find(target_set.begin(), target_set.end(), k);
            if (itr == target_set.end())
                propagate_set.insert(k);
        }

        graph.clear();
        graph.insert(tuple->graph_.begin(), tuple->graph_.end());


        tuple->lock_.w_unlock();

        if (loadAcquire(this->quit_)) {
            result = TransactionStatus::invalid;
            return;
        }

    }
    result = TransactionStatus::validating;
    return;

ABORT_VALIDATION:
    result = TransactionStatus::aborted;
    return;
}

bool TxExecutor::validation() {

    KeySet finished_set;
    KeySet propagate_set;

    bool insert_only = FLAGS_insert_only_single_writer &&
                       !has_write_ && has_insert_ && !this->readSet_.empty();

    this->graph_.emplace(this->txid_, TxNode(this->txid_));
    this->graph_.at(this->txid_).status_ = TransactionStatus::validating;

    // for read-only transacton (cf. read crown)
    if (this->writeSet_.size() == 0) {
        for (const auto& [tpl, _] : this->readSet_) {
            tpl->lock_.w_lock();
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
            tpl->lock_.w_unlock();
        }
        goto FINISH_VALIDATION;
    }

    // Insert-only without reads: only scanner -> inserter conflicts.
    if (!has_write_ && has_insert_ && this->readSet_.empty()) {
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
            // all write-set entries were inserts and nobody scanned them
            InsertOnlySkips.fetch_add(1, std::memory_order_relaxed);
            goto FINISH_VALIDATION;
        }
        // a scanner may depend on our inserts; its edges need the full check
        InsertOnlyScanFalls.fetch_add(1, std::memory_order_relaxed);
    }

    for (const auto& [tpl, _] : this->readSet_) {
        if (auto it = this->writeSet_.find(tpl); it == this->writeSet_.end())
            propagate_set.insert(tpl);
    }

    if (FLAGS_validation_th_num == 1 ||
        propagate_set.size() <= FLAGS_validation_threshold) {
        bool ret = read_validation(propagate_set, finished_set);
        if (ret && propagate_set.empty()) {
            goto FINISH_VALIDATION;
        } else if (!ret) {
            goto ABORT_VALIDATION;
        }
    }

    while (!propagate_set.empty()) {
        uint64_t workers = 1;
        uint64_t total_task = propagate_set.size();
        uint64_t assignment = total_task;
        if (FLAGS_validation_th_num > 1 &&
            total_task > FLAGS_validation_threshold) {
            workers = FLAGS_validation_th_num;
            assignment = (size_t)ceil((double)total_task/(double)workers);
        }
        std::vector<std::thread> thv;
        std::vector<KeySet> propagate_subset(workers);
        std::vector<Graph> graph(workers);
        TransactionStatus result[workers];

        for (size_t i = 0; i < workers; ++i) {
            propagate_subset[i] = KeySet();
            graph[i] = Graph();
            thv.emplace_back(&TxExecutor::validation_worker, this, i,
                std::ref(propagate_set), assignment*i, assignment,
                std::ref(propagate_subset[i]), std::ref(graph[i]),
                std::ref(result[i]));
        }
        for (auto &th : thv) th.join();

        for (size_t i = 0; i < workers; ++i) {
            if (result[i] == TransactionStatus::validating) {
                merge(this->graph_, graph[i]);
            } else if (result[i] == TransactionStatus::aborted) {
                goto ABORT_VALIDATION;
            } else if (result[i] == TransactionStatus::invalid) {
                // cancel due to timeout
                this->status_ = TransactionStatus::invalid;
                goto FINISH_VALIDATION;
            } else {
                ERR;
            }
        }

        if (has_cycle(this->graph_, this->txid_)) {

            goto ABORT_VALIDATION;
        }

        finished_set.insert(propagate_set.begin(), propagate_set.end());
        propagate_set.clear();

        for (size_t i = 0; i < workers; ++i) {
            for (auto& k : propagate_subset[i]) {
                if (auto it = finished_set.find(k); it == finished_set.end()) {
                    propagate_set.insert(k);
                }
            }
        }

        if (loadAcquire(this->quit_)) {
            this->status_ = TransactionStatus::invalid;
            goto FINISH_VALIDATION;
        }
    }

FINISH_VALIDATION:
    return true;

ABORT_VALIDATION:
    return false;
}

bool TxExecutor::write_validation(WriteElement<Tuple> element, KeySet& finished_set, KeySet& propagate_set) {
    Status stat;
    TxSet decided, reachable_to;
    KeySet followers_read_keys;
    Tuple* tuple = element.rcdptr_;
    OpType op = element.op_;

    tuple->lock_.w_lock();

    tuple->graph_.emplace(this->txid_, TxNode(this->txid_));
    tuple->graph_.at(this->txid_).status_ = TransactionStatus::validating;

    // merge graph
    merge(tuple->graph_, this->graph_);
    gc(tuple->graph_, this->reclamation_epoch_);

    // Add an edge from every reader of the record to this transaction, then check for a cycle.
    TxSet readers;
    TxSet followers;
    find_readers(tuple->graph_, tuple, readers);
    for (const auto& r : readers) {
        if (this->txid_.epoch < r.epoch - 1) {
            goto ABORT_VALIDATION;
        }
        tuple->graph_.at(r).writtenBy_.insert(this->txid_);
        tuple->graph_.at(this->txid_).from_.insert(r);
    }

    if (has_cycle(tuple->graph_, this->txid_)) {
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

    if (l1d_on()) l1d_facts_ += this->readSet_.size() + this->writeSet_.size();

    tuple->graph_.at(this->txid_).readSet_.insert(
        this->readSet_.begin(), this->readSet_.end()
    );
    tuple->graph_.at(this->txid_).writeSet_.insert(this->writeSet_.begin(), this->writeSet_.end());

    // insert write version to the appropriate position in the linked list
    stat = insert_version(element, followers);
    if (stat != Status::OK) {
        goto ABORT_VALIDATION;
    }

    // for propagation
    find_reachable_to(this->txid_, tuple->graph_, reachable_to);
    for (auto& txid : reachable_to) {
        add_related_read_keys(&tuple->graph_.at(txid), followers_read_keys);
    }
    for (const auto& k : followers_read_keys) {
        push_candidate_keys(propagate_set, finished_set, followers_read_keys);
    }

    this->graph_.clear();

    l1d_graph_facts(tuple->graph_);

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
    clean_up_node_edges(tuple->graph_, this->txid_);
    tuple->lock_.w_unlock();
    return false;
}

bool TxExecutor::insert_validation(WriteElement<Tuple> element) {
    Tuple* tuple = element.rcdptr_;
    Storage s = element.storage_;
    string_view key = element.key_;
    ScanEntry *prev, *entry;
    int history_offset = SCAN_HISTORY_INDEX(s, this->txid_.epoch);

    tuple->lock_.w_lock();

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
    // sync graph to log my reads-from at least
    tuple->graph_.insert(this->graph_.begin(), this->graph_.end());

    tuple->lock_.w_unlock();

    return true;
}

bool TxExecutor::read_validation(KeySet& target_set, KeySet& finished_set) {

    while (!target_set.empty()) {
        auto iterator = target_set.cbegin();
        Tuple* tuple = *iterator;

        tuple->lock_.w_lock();

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

        if (l1d_on()) l1d_facts_ += this->readSet_.size() + this->writeSet_.size();

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
        if (FLAGS_insert_only_discovery_limit != 0 && !has_write_ && has_insert_ &&
            target_set.size() + finished_set.size() + keys.size() >
                FLAGS_insert_only_discovery_limit) {
            clean_up_node_edges(tuple->graph_, this->txid_);
            tuple->lock_.w_unlock();
            goto ABORT_VALIDATION;
        }
        for (const auto& k : keys) {
            push_candidate_keys(target_set, finished_set, keys);
        }

        this->graph_.clear();

        l1d_graph_facts(tuple->graph_);

        this->graph_.insert(tuple->graph_.begin(), tuple->graph_.end());

        tuple->lock_.w_unlock();
        finished_set.insert(tuple);
        target_set.erase(iterator);

        if (FLAGS_validation_th_num != 1 &&
            target_set.size() > FLAGS_validation_threshold) {
            // Many targets found: continue with parallel validation.
            return true;
        }

        if (loadAcquire(this->quit_)) {
            this->status_ = TransactionStatus::invalid;
            return true;
        }
    }

FINISH_VALIDATION:
    return true;

ABORT_VALIDATION:
    return false;
}

void TxExecutor::abort() {

    writeSetClean();
    read_set_.clear();

    graph_.clear();
    readSet_.clear();
    writeSet_.clear();

    mainte();

    backoff();
}

void TxExecutor::gc_records() {
  // for records
  while (!gc_records_.empty()) {
    Tuple* rec = gc_records_.front();
    Version* latest = rec->ldAcqLatest();
    if (latest->txid_.epoch > reclamation_epoch_) break;
    if (latest->status_ != VersionStatus::deleted) ERR;
    delete rec;
    gc_records_.pop_front();
  }
}

void TxExecutor::mainte() {

  gc_records();

}

void TxExecutor::writePhase() {
    for (auto we : write_set_) {
        commit_version(we);
    }

    read_set_.clear();
    write_set_.clear();
    readSet_.clear();
    writeSet_.clear();
    graph_.clear();
}

bool TxExecutor::commit() {
  const uint64_t l1d_v0 = l1d_on() ? rdtscp() : 0;
  if (l1d_on()) l1d_reads_ = this->readSet_.size();
  if (!validation()) {
    return false;
  }
  if (l1d_on()) l1d_val_ += rdtscp() - l1d_v0;

  // Abandon ongoing long transaction when time up
  if (status_ == TransactionStatus::invalid)
    return false;

  writePhase();
  mainte();
  if (l1d_on()) {
    const double us = static_cast<double>(FLAGS_clocks_per_us);
    std::fprintf(stderr, "#l1cost total_us=%.1f val_us=%.1f facts=%lu reads=%lu trav=%lu imported=0 fp_scanned=0 hist=0\n",
                 (rdtscp() - l1d_t0_) / us, l1d_val_ / us,
                 (unsigned long)l1d_facts_, (unsigned long)l1d_reads_, (unsigned long)l1d_trav_);
  }
  return true;
}

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
    if (write_set_.size() != 0 || writeSet_.size() != 0) {
        dump(thid_, "WARN: Write happened in reconnoitering mode.");
    }
    reconnoitering_ = false;
    begin();
}

bool TxExecutor::isLeader() {
    return this->thid_ == 1;
}

void TxExecutor::leaderWork() {
    ozeLeaderWork(epoch_timer_start_, epoch_timer_stop_);
    leaderBackoffWork(backoff_, OzeResult);
}

