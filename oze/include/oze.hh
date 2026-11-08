#pragma once

#include <atomic>
#include <cstdint>
#include <map>
#include <set>
#include <unordered_map>
#include <unordered_set>

#include "common/cache_line_size.hh"
#include "tuple.hh"
#include "transaction_status.hh"
#include "txid.hh"

class Tuple;
typedef Tuple* Key;
typedef std::map<Key,TxID> ReadSet;
typedef std::set<Key> WriteSet;
typedef std::set<Key> KeySet;
typedef std::set<TxID> TxSet;

class TxNode {
    public:
        TxID id_;
        TransactionStatus status_;
        bool is_aborted;
        ReadSet readSet_;
        WriteSet writeSet_;
        TxSet readBy_;     // outgoing edges
        TxSet writtenBy_;  // outgoing edges
        TxSet from_;       // incoming edges
        TxNode(TxID id) :
            id_(id),
            is_aborted(false),
            status_(TransactionStatus::inflight)
            {};
        TxNode(TxID id, TransactionStatus status) :
            id_(id),
            is_aborted(false),
            status_(status)
            {};
};

typedef std::map<TxID,TxNode> Graph;
