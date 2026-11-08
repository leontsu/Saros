#pragma once

#include <stdio.h>
#include <atomic>
#include <cmath>
#include <deque>
#include <iostream>
#include <map>
#include <string>
#include <thread>
#include <unordered_map>
#include <vector>

#include "common/backoff.hh"
#include "common/debug.hh"
#include "common/inline.hh"
#include "common/result.hh"
#include "common/status.hh"
#include "common/util.hh"
#include "oze.hh"
#include "oze_op_element.hh"
#include "common.hh"
#include "atomic_tool.hh"
#include "scan.hh"
#include "tuple.hh"
#include "txid.hh"
#include "version.hh"

class TxExecutor {
public:
    TransactionStatus status_ = TransactionStatus::invalid;
    std::vector<ReadElement<Tuple>> read_set_;
    std::vector<WriteElement<Tuple>> write_set_;
    std::deque<Tuple*> gc_records_;
    Result *result_ = nullptr;
    Backoff& backoff_;
    uint64_t epoch_timer_start_, epoch_timer_stop_;

    ReadSet readSet_;
    WriteSet writeSet_;
    Graph graph_;
    uint64_t reclamation_epoch_;

    TxID txid_;
    bool reconnoitering_ = false;
    bool is_forwarded_;
    bool has_write_ = false;
    bool has_insert_ = false;
    bool scan_conflict_found_ = false; // insert_validation found a scanner covering an inserted key
    uint8_t thid_ = 0;

    RWLock lock_;

    const bool& quit_; // for thread termination control

    TxExecutor(uint8_t thid, Backoff& backoff, Result *res, const bool &quit)
        : result_(res), thid_(thid), backoff_(backoff), quit_(quit) {
        txid_.thid = thid_;
        txid_.tid = 0;

        is_forwarded_ = false;
    }

    ~TxExecutor() {
        read_set_.clear();
        write_set_.clear();
    }

    void abort();

    void mainte();  // maintenance

    void begin();

    Status read(Storage s, std::string_view key, TupleBody** body);
    Status read_internal(Storage s, std::string_view key, Tuple* tuple, Version** return_ver);

    Status write(Storage s, std::string_view key, TupleBody&& body);

    Status insert(Storage s, std::string_view key, TupleBody&& body);

    Status delete_record(Storage s, std::string_view key);

    Status scan(Storage s,
                std::string_view left_key, bool l_exclusive,
                std::string_view right_key, bool r_exclusive,
                std::vector<TupleBody *>&result);

    Status scan(Storage s,
                std::string_view left_key, bool l_exclusive,
                std::string_view right_key, bool r_exclusive,
                std::vector<TupleBody *>&result, int64_t limit);

    bool read_validation(KeySet &target_set, KeySet &finished_set);
    bool write_validation(WriteElement<Tuple> element, KeySet &finished_set, KeySet &propagate_set);
    bool insert_validation(WriteElement<Tuple> element);
    bool validation();
    void validation_worker(size_t worker_id, KeySet &propagate_set,
                           size_t offset, size_t assignment,
                           KeySet &propagate_subset, Graph &graph, TransactionStatus &result);

    void writePhase();

    bool commit();

    void reconnoiter_begin();
    void reconnoiter_end();

    bool isLeader();

    void leaderWork();

    void gc_records();

    void backoff() {

        Backoff::backoff(FLAGS_clocks_per_us);

    }

    // true if every version in the read set was written by the same transaction
    // (trivially true for an empty read set)
    bool read_set_single_writer() {
        auto itr = read_set_.begin();
        if (itr == read_set_.end()) return true;
        const TxID writer = itr->txid_;
        for (++itr; itr != read_set_.end(); ++itr) {
            if (itr->txid_ != writer) return false;
        }
        return true;
    }

    Version *newVersionGeneration([[maybe_unused]] Tuple *tuple) {
        return new Version(this->txid_);
    }

    Version *newVersionGeneration([[maybe_unused]] Tuple *tuple, TupleBody&& body) {
        return new Version(this->txid_, std::move(body));
    }

    /**
     * @brief Search xxx set
     * @detail Search element of local set corresponding to given key.
     * In this prototype system, the value to be updated for each worker thread 
     * is fixed for high performance, so it is only necessary to check the key match.
     * @param Key [in] the key of key-value
     * @return Corresponding element of local set
     */
    // READ-SET INDEX (--read_set_hash_index; identical to the Saros tree).
    // Built lazily: each lookup first indexes the elements appended since the
    // previous lookup (read_set_ is only appended to and cleared), and a
    // shrunken read_set_ resets the index.  Positions, not pointers; emplace
    // keeps the FIRST occurrence, which is what the linear scan returns.
    std::unordered_map<std::string, uint32_t> rs_index_;
    size_t rs_indexed_ = 0;
    static std::string rs_index_key(Storage s, std::string_view key) {
        std::string k;
        k.reserve(key.size() + 4);
        const uint32_t sv = static_cast<uint32_t>(s);
        k.append(reinterpret_cast<const char*>(&sv), sizeof(sv));
        k.append(key.data(), key.size());
        return k;
    }
    void reset_read_set_index() {
        rs_index_.clear();
        rs_indexed_ = 0;
    }

    inline ReadElement<Tuple> *searchReadSet(Storage s, std::string_view key) {
        if (FLAGS_read_set_hash_index) {
            if (read_set_.size() < rs_indexed_) reset_read_set_index();
            for (; rs_indexed_ < read_set_.size(); ++rs_indexed_) {
                const auto& re = read_set_[rs_indexed_];
                rs_index_.emplace(rs_index_key(re.storage_, re.key_),
                                  static_cast<uint32_t>(rs_indexed_));
            }
            auto it = rs_index_.find(rs_index_key(s, key));
            return it == rs_index_.end() ? nullptr : &read_set_[it->second];
        }
        for (auto &re : read_set_) {
            if (re.storage_ != s) continue;
            if (re.key_ == key) return &re;
        }
        return nullptr;
    }

    /**
     * @brief Search xxx set
     * @detail Search element of local set corresponding to given key.
     * In this prototype system, the value to be updated for each worker thread 
     * is fixed for high performance, so it is only necessary to check the key match.
     * @param Key [in] the key of key-value
     * @return Corresponding element of local set
     */
    inline WriteElement<Tuple> *searchWriteSet(Storage s, std::string_view key) {
        for (auto &we : write_set_) {
            if (we.storage_ != s) continue;
            if (we.key_ == key) return &we;
        }

        return nullptr;
    }

    inline WriteElement<Tuple> *searchWriteSet(Tuple* tuple) {
        for (auto &we : write_set_) {
            if (we.rcdptr_ == tuple) return &we;
        }

        return nullptr;
    }

    void writeSetClean() {
        for (auto itr = write_set_.begin(); itr != write_set_.end(); ++itr) {
            (*itr).new_ver_->status_.store(VersionStatus::aborted, std::memory_order_release);
            if ((*itr).op_ == OpType::INSERT) {
                Masstrees[get_storage((*itr).storage_)].remove_value((*itr).key_);
                delete (*itr).new_ver_;
                delete (*itr).rcdptr_;
            }
        }
        write_set_.clear();
    }

    void addScanEntry(const Storage s,
                      std::string_view left_key, bool l_exclusive,
                      std::string_view right_key, bool r_exclusive) {
        auto index = SCAN_HISTORY_INDEX(s, this->txid_.epoch);
        while(true) {
            uint64_t expected = ScanRange[index].load(std::memory_order_acquire);
            uint32_t min = expected >> 32;
            uint32_t max = expected & 0xffffffff;
            uint32_t left = *reinterpret_cast<const uint32_t*>(left_key.data());
            uint32_t right = *reinterpret_cast<const uint32_t*>(right_key.data());
            uint64_t updated;
            if (max < right) {
              updated |= static_cast<uint64_t>(right) & 0xffffffff;
            } else {
              updated |= static_cast<uint64_t>(max) & 0xffffffff;
            }
            if (left < min) {
              updated |= static_cast<uint64_t>(left) << 32;
            } else {
              updated |= static_cast<uint64_t>(min) << 32;
            }
            if (expected != updated) {
              if (ScanRange[index].compare_exchange_strong(
                  expected, updated, memory_order_acq_rel, memory_order_acquire))
                break;
            } else {
              break;
            }
        }
        while(true) {
          ScanEntry* expected = ScanHistory[index].load(std::memory_order_acquire);
          ScanEntry* new_entry = new ScanEntry(txid_, left_key, l_exclusive,
                                               right_key, r_exclusive, expected);
          if (ScanHistory[index].compare_exchange_strong(expected, new_entry,
                                                         memory_order_acq_rel, memory_order_acquire))
            break;
        }
    }

    void find_readers(const Graph &g, const Key key, TxSet &readers) {
        for (const auto& [txid, node] : g) {
            if (txid == this->txid_) continue; // to avoid including me in the RMW case
            if (g.at(txid).is_aborted) continue;
            if (auto it = g.at(txid).readSet_.find(key);
                it !=g.at(txid).readSet_.end()) {
                readers.insert(txid);
            }
        }
    }
    
    void find_followers(const Graph &g, const Key key, TxSet &readers, TxSet &followers) {
        for (const auto& reader : readers) {
            followers.insert(g.at(reader).readSet_.at(key));
        }
    }

    void find_reachable_to_dfs(const Graph &g, std::map<TxID,bool> &seen, const TxID txID) {
        if (l1d_on()) l1d_trav_ += 1 + g.at(txID).readBy_.size() + g.at(txID).writtenBy_.size();
        seen.emplace(txID, true);
        TxSet edges(g.at(txID).readBy_);
        edges.insert(g.at(txID).writtenBy_.begin(), g.at(txID).writtenBy_.end());

        for (const auto& next : edges) {
            if (auto it = seen.find(next); it != seen.end() && seen[next]) continue;
            if (auto it = g.find(next); it != g.end())
                find_reachable_to_dfs(g, seen, next);
        }
    }

    void find_reachable_to(const TxID start, const Graph &g, TxSet &txns) {
        // find transactions that can be visited from start transaction
        std::map<TxID,bool> seen;
        for (const auto& next : g.at(start).readBy_) {
            // TODO: This kind of checking might be skipped if merge/GC works well-mannered
            if (auto it = g.find(next); it != g.end())
                find_reachable_to_dfs(g, seen, next);
        }
        for (const auto& next : g.at(start).writtenBy_) {
            if (auto it = g.find(next); it != g.end())
                find_reachable_to_dfs(g, seen, next);
        }
        for (const auto& [id, _] : seen)
            txns.insert(id);
    }

    Version* get_a_version(Version* ver, std::vector<Version*>& followers) {
        while (ver->status_.load(memory_order_acquire) != VersionStatus::committed
            && ver->status_.load(memory_order_acquire) != VersionStatus::deleted) {
            if (ver->status_.load(memory_order_acquire) == VersionStatus::aborted) {
                ver = ver->ldAcqNext();
            } else if (ver->status_.load(memory_order_acquire) == VersionStatus::pending
                || ver->status_.load(memory_order_acquire) == VersionStatus::deleting) {
                followers.push_back(ver);
                ver = ver->ldAcqNext();
            }
            if (ver == VERSION_TERMINATION) {
                return ver;
            }
            if (ver == nullptr) {
                return ver;
            }
        }
        return ver;
    }

    void update_read_from(Tuple* tuple, Version* ver) {
        // TODO: do not remove TxNode if the version still alive?
        if (auto it = tuple->graph_.find(ver->txid_); it == tuple->graph_.end()) {
            tuple->graph_.emplace(ver->txid_, TxNode(ver->txid_));
        }
        // add candidate edge
        tuple->graph_.at(ver->txid_).readBy_.emplace(this->txid_);
    }

    Status get_visible_version(Tuple *tuple, Version **return_ver) {
        Status stat = Status::OK;
        Version* ver = nullptr;
        std::vector<Version*> followers;
        ver = tuple->ldAcqLatest();
RETRY:
        followers.clear();

        ver = get_a_version(ver, followers);
        if (ver == VERSION_TERMINATION) {
            // versions exist but no committed ones
            stat = Status::WARN_NOT_FOUND;
            goto OUT;
        }
        if (ver == nullptr) {
            // committed versions exist but no visible ones
            stat = Status::ERROR_NO_VISIBLE_VERSION;
            goto OUT;
        }

        if (reconnoitering_) {
          goto OUT;
        }

        // add anti-dependency edges found when skipping versions
        for (auto& follower : followers) {
            if (static_cast<uint64_t>(follower->txid_.epoch) + 1 < this->txid_.epoch) {
                stat = Status::ERROR_NO_VISIBLE_VERSION;
                goto OUT;
            }
            tuple->graph_.at(this->txid_).writtenBy_.emplace(follower->txid_);
        }

        update_read_from(tuple, ver);

        if (has_cycle(tuple->graph_, this->txid_)) {
            // remove candidate edge and try next candidate
            tuple->graph_.at(ver->txid_).readBy_.erase(this->txid_);

            if (ver->txid_.epoch < this->txid_.epoch) {
                stat = Status::ERROR_NO_VISIBLE_VERSION;
                goto OUT;
            }

            // add anti-dependency edge before retry
            tuple->graph_.at(this->txid_).writtenBy_.emplace(ver->txid_);
            ver = ver->ldAcqNext();
            if (ver == VERSION_TERMINATION) {
                stat = Status::WARN_NOT_FOUND;
                goto OUT;
            }
            if (ver == nullptr) {
                stat = Status::ERROR_NO_VISIBLE_VERSION;
                goto OUT;
            } else {
                goto RETRY;
            }
        }
OUT:
        *return_ver = ver;
        return stat;
    }

    void add_related_read_keys(const TxNode *tx, KeySet &keys) {
        for (const auto& [key, id] : tx->readSet_)
            keys.insert(key);
    }

    void push_candidate_keys(KeySet &targets, KeySet &done, KeySet &keys) {
        for (const auto& key : keys) {
            auto result1 = targets.find(key);
            auto result2 = done.find(key);
            if (result1 == targets.end() && result2 == done.end())
                targets.insert(key);
        }
    }

    bool check_cycle_dfs(const Graph &g, std::map<TxID,bool> &seen, std::map<TxID,bool> &finished, const TxID txID) {
        if (l1d_on()) l1d_trav_ += 1 + g.at(txID).readBy_.size() + g.at(txID).writtenBy_.size();
        if (quit_) {
            dump(thid_, "WARN: Quit cycle check because benchmark has finished.");
            status_ = TransactionStatus::invalid;
            return true; // TODO: should return with INVALID
        }
        seen.emplace(txID, true);
        TxSet edges(g.at(txID).readBy_);
        edges.insert(g.at(txID).writtenBy_.begin(), g.at(txID).writtenBy_.end());

        for (const auto& next : edges) {
            auto f = finished.find(next);
            if (f != finished.end() && finished[next]) continue;
            auto s = seen.find(next);
            if (s != seen.end() && seen[next] && f == finished.end())
                return true;
            if (auto it = g.find(next); it != g.end()) {
                auto ret = check_cycle_dfs(g, seen, finished, next);
                if (ret)
                    return ret;
            }
        }
        finished[txID] = true;
        return false;
    }

    bool has_cycle(const Graph &g, const TxID start) {
        std::map<TxID,bool> seen, finished;
        TxSet edges(g.at(start).readBy_);
        edges.insert(g.at(start).writtenBy_.begin(), g.at(start).writtenBy_.end());

        for (const auto& next : edges) {
            if (auto it = g.find(next); it != g.end()) {
                auto ret = check_cycle_dfs(g, seen, finished, next);
                if (ret) return true;
            }
        }
        return false;
    }

    // L1 COST DIAGNOSTIC (--l1_cost_diag, thread 0 only): dependency facts
    // (read/write-set entries and edges) copied by validation, and timings.
    uint64_t l1d_t0_ = 0, l1d_val_ = 0, l1d_facts_ = 0;
    uint64_t l1d_trav_ = 0, l1d_reads_ = 0;   // DFS nodes+edges visited; read-set size
    bool l1d_on() const { return FLAGS_l1_cost_diag && thid_ == 0; }
    static uint64_t l1d_node_facts(const TxNode& n) {
        return n.readSet_.size() + n.writeSet_.size() + n.readBy_.size() +
               n.writtenBy_.size() + n.from_.size();
    }
    void l1d_graph_facts(const Graph& g) {
        if (!l1d_on()) return;
        for (const auto& [tid, n] : g) l1d_facts_ += l1d_node_facts(n);
    }

    void merge(Graph &target, Graph &source) {
        if (l1d_on()) l1d_graph_facts(source);
        for (const auto& [tid, sourceTxNode] : source) {
            TxNode *targetTxNode;
            if (auto it = target.find(tid); it != target.end()) {
                targetTxNode = &target.at(tid);
            } else {
                target.emplace(tid, TxNode(tid));
                targetTxNode = &target.at(tid);
            }
            if (targetTxNode->is_aborted) {
                clean_up_node_edges(target, tid);
                continue;
            }
            targetTxNode->readSet_.insert(sourceTxNode.readSet_.begin(), sourceTxNode.readSet_.end());
            targetTxNode->writeSet_.insert(sourceTxNode.writeSet_.begin(), sourceTxNode.writeSet_.end());
            for (const auto& to : sourceTxNode.readBy_)
                targetTxNode->readBy_.insert(to);
            for (const auto& to : sourceTxNode.writtenBy_)
                targetTxNode->writtenBy_.insert(to);
            for (const auto& from : sourceTxNode.from_)
                targetTxNode->from_.insert(from);
            // Update status if the transaction in source graph is validating
            if (targetTxNode->status_ ==  TransactionStatus::inflight
                && sourceTxNode.status_ ==  TransactionStatus::validating)
                targetTxNode->status_ = TransactionStatus::validating;
        }
    }

    uint64_t get_min_epoch() {
        uint64_t min = UINT64_MAX;
        for (unsigned int i = 0; i < TotalThreadNum; ++i) {
            auto e = ThManagementTable[i]->atomicLoadEpoch();
            if (e < min)
                min = e;
        }
        return min;
    }

    void erase_following_edges(Graph &g, const TxID txid) {
        for (auto& reader : g.at(txid).readBy_) {
            if (auto it = g.find(reader); it != g.end()) // TODO: why is this check necessary?
                g.at(reader).from_.erase(txid);
        }
        for (auto& writer : g.at(txid).writtenBy_) {
            if (auto it = g.find(writer); it != g.end()) // TODO: why is this check necessary?
                g.at(writer).from_.erase(txid);
        }
    }

    void gc(Graph &g, uint64_t epoch) {
        for (auto it = g.begin(); it != g.end();) {
            auto txid = (*it).first;
            auto txNode = &(*it).second;
            if (txid.epoch <= epoch) {
                bool keep = false;
                for (auto i = txNode->readBy_.begin(); i != txNode->readBy_.end();) {
                    auto reader = (*i);
                    if (reader.epoch <= epoch) {
                        if (auto it = g.find(reader); it != g.end()) // TODO: why is this check necessary?
                            g.at(reader).from_.erase(txid);
                        i = txNode->readBy_.erase(i);
                    } else {
                        keep = true;
                        ++i;
                    }
                }
                for (auto i = txNode->writtenBy_.begin(); i != txNode->writtenBy_.end();) {
                    auto writer = (*i);
                    if (writer.epoch <= epoch) {
                        if (auto it = g.find(writer); it != g.end()) // TODO: why is this check necessary?
                            g.at(writer).from_.erase(txid);
                        i = txNode->writtenBy_.erase(i);
                    } else {
                        keep = true;
                        ++i;
                    }
                }
                for (auto i = txNode->from_.begin(); i != txNode->from_.end();) {
                    auto from = (*i);
                    if (from.epoch > epoch) {
                        keep = true;
                        break;
                    }
                    ++i;
                }
                if (keep) {
                    ++it;
                    continue;
                }
                if (txNode->from_.size() > 0) {
                    ++it;
                    continue;
                }
                //           << ", Reclaimation epoch = " << epoch
                //           << ", Local epoch = " << ThLocalEpoch[thid_].obj_
                //           << ")" << std::endl;
                erase_following_edges(g, txid);
                it = g.erase(it);
            }
            else {
                ++it;
            }
        }
    }

    Status insert_version(WriteElement<Tuple> we, TxSet& followers) {
        Tuple* tuple = we.rcdptr_;
        if (we.op_ == OpType::DELETE) {
            we.new_ver_->status_ = VersionStatus::deleting;
        } else {
            we.new_ver_->status_ = VersionStatus::pending;
        }
        Version* v = tuple->latest_;
        if (v->status_ == VersionStatus::deleting || v->status_ == VersionStatus::deleted) {
            return Status::ERROR_CONCURRENT_WRITE_OR_DELETE;
        }
        if (followers.size() == 0) {
            we.new_ver_->next_ = v;
            tuple->latest_ = we.new_ver_;
        } else {
            Version* prev = v;
            while (followers.size() > 0) {
                followers.erase(v->txid_);
                prev = v;
                v = v->next_;
            }
            prev->next_ = we.new_ver_;
            we.new_ver_->next_ = v;
        }
        return Status::OK;
    }

    void clean_up_node_edges(Graph &g, const TxID txid) {
        // TODO: can be sophisticated?
        for (const auto& to : g.at(txid).writtenBy_) {
            if (auto it = g.find(to); it != g.end()) // TODO: can be checkless?
                g.at(to).from_.erase(txid);
        }
        for (const auto& from : g.at(txid).from_) {
            if (auto it = g.find(from); it != g.end()) { // TODO: can be checkless?
                g.at(from).readBy_.erase(txid);
                g.at(from).writtenBy_.erase(txid);
            }
        }
        // We want to just remove this aborted node,
        // but some other worker may have seen this node
        // (that is still inflight at that time), and try to merge
        // without knowing it has already aborted.
        // So, just mark it aborted and prevent it from adding edges
        g.at(txid).readBy_.clear();
        g.at(txid).writtenBy_.clear();
        g.at(txid).from_.clear();
        g.at(txid).is_aborted = true;
    }

    void commit_version(WriteElement<Tuple> we) {
      if (we.op_ == OpType::DELETE) {
        Masstrees[get_storage(we.storage_)].remove_value(we.key_);
        we.new_ver_->status_.store(VersionStatus::deleted, std::memory_order_release);
        gc_records_.push_back(we.rcdptr_);
      } else {
        we.new_ver_->status_.store(VersionStatus::committed, std::memory_order_release);
      }
    }

};
