#!/usr/bin/env python3
"""Usage: check_history.py <dump_dir> [--verbose]
Checks a --history_dump_path dump for serializability; exit 0: acyclic, 1: cycle, 2: error."""

import json
import os
import sys
import glob
from collections import defaultdict


class Txn:
    __slots__ = ("txid", "reads", "writes")

    def __init__(self, txid, reads, writes):
        self.txid = txid
        self.reads = reads    # list of (storage, key, vtxid)
        self.writes = writes  # list of (storage, key, pred_or_None, install_seq_or_None)


# Loads every history_<thid>.log under dump_dir: one JSON object per committed
# transaction,
#   {"txid": "epoch:thid:tid", "order": n,
#    "reads":  [{"s": storage, "key": "<hex>", "vtxid": "epoch:thid:tid"}, ...],
#    "writes": [{"s": storage, "key": "<hex>", "pred": "epoch:thid:tid" | null,
#                "install_seq": n | null}, ...]}
# where pred is the version below the written one (null for an insert) and
# install_seq the global order of version installs.
def load_history(dump_dir):
    files = sorted(glob.glob(os.path.join(dump_dir, "history_*.log")))
    if not files:
        raise ValueError(f"no history_*.log files found under {dump_dir!r}")

    txns = []
    for path in files:
        with open(path, "r") as f:
            for lineno, line in enumerate(f, start=1):
                line = line.strip()
                if not line:
                    continue
                try:
                    rec = json.loads(line)
                except json.JSONDecodeError as e:
                    raise ValueError(f"{path}:{lineno}: malformed JSON: {e}") from e
                reads = [(r["s"], r["key"], r["vtxid"]) for r in rec["reads"]]
                writes = [(w["s"], w["key"], w.get("pred"), w.get("install_seq"))
                          for w in rec["writes"]]
                txns.append(Txn(rec["txid"], reads, writes))
    return txns


# Returns (storage, key) -> the final version chain, oldest first. With order
# forwarding, two writers can record the same predecessor (a later install is
# spliced below an earlier one), so the splices are replayed per key in
# install_seq order, as insert_version() performs them: writer goes above pred,
# and whoever was above pred moves above writer. The oldest version of a key
# (loaded before the dump started) is inferred from the earliest splice.
def recover_version_order(txns):
    per_key_writes = defaultdict(list)
    for txn in txns:
        for (s, key, pred, install_seq) in txn.writes:
            k = (s, key)
            sort_key = -1 if install_seq is None else install_seq
            per_key_writes[k].append((sort_key, txn.txid, pred))

    order = {}
    for key, writes in per_key_writes.items():
        writes.sort(key=lambda t: t[0])
        succ = {}       # pred_txid -> txid currently spliced directly above it
        root = None
        insert_writer = None
        for (_, writer, pred) in writes:
            if pred is None:
                if insert_writer is not None and insert_writer != writer:
                    raise ValueError(
                        f"key {key} has more than one root writer "
                        f"({insert_writer} and {writer}) -- unexpected "
                        "duplicate INSERT")
                insert_writer = writer
                if root is None:
                    root = writer
                continue
            if root is None:
                # Earliest write observed for this key in the dump window;
                # its pred is the inferred external root.
                root = pred
            old_succ = succ.get(pred)
            succ[pred] = writer
            if old_succ is not None and old_succ != writer:
                succ[writer] = old_succ

        if root is None:
            continue  # unreachable: per_key_writes only holds actual writes

        chain = [root]
        seen = {root}
        cur = root
        while cur in succ:
            nxt = succ[cur]
            if nxt in seen:
                raise ValueError(
                    f"key {key}: cycle detected while reconstructing version "
                    f"chain at {nxt} -- dump is inconsistent")
            chain.append(nxt)
            seen.add(nxt)
            cur = nxt
        order[key] = chain  # oldest .. newest

    return order


# The direct serialization graph, as (src, dst, kind, key) with src before dst:
# wr writer -> reader, ww version -> next version, rw reader -> writer of the
# next version.
def build_edges(txns, version_order):
    committed = {t.txid for t in txns}
    edges = []

    # (key, txid) -> position in the key's version chain.
    pos_of = {}
    for key, chain in version_order.items():
        for i, txid in enumerate(chain):
            pos_of[(key, txid)] = i

    def successor(key, txid):
        chain = version_order.get(key)
        if chain is None:
            return None
        i = pos_of.get((key, txid))
        if i is None or i + 1 >= len(chain):
            return None
        return chain[i + 1]

    for txn in txns:
        for (s, key, vtxid) in txn.reads:
            k = (s, key)
            # wr: only if the writer committed within the dump.
            if vtxid in committed and vtxid != txn.txid:
                edges.append((vtxid, txn.txid, "wr", k))
            # rw: the successor, if any, is a write in the dump.
            succ = successor(k, vtxid)
            if succ is not None and succ != txn.txid:
                edges.append((txn.txid, succ, "rw", k))

    for key, chain in version_order.items():
        for i in range(len(chain) - 1):
            a, b = chain[i], chain[i + 1]
            if a in committed and b in committed and a != b:
                edges.append((a, b, "ww", key))

    return edges


# Iterative Tarjan: the strongly connected components, each a list of nodes.
def tarjan_scc(nodes, adj):
    index_counter = [0]
    index = {}
    lowlink = {}
    on_stack = {}
    stack = []
    result = []

    for start in nodes:
        if start in index:
            continue
        # Work stack of (node, iterator over its successors).
        work = [(start, iter(adj.get(start, ())))]
        index[start] = index_counter[0]
        lowlink[start] = index_counter[0]
        index_counter[0] += 1
        stack.append(start)
        on_stack[start] = True

        while work:
            node, it = work[-1]
            advanced = False
            for nxt in it:
                if nxt not in index:
                    index[nxt] = index_counter[0]
                    lowlink[nxt] = index_counter[0]
                    index_counter[0] += 1
                    stack.append(nxt)
                    on_stack[nxt] = True
                    work.append((nxt, iter(adj.get(nxt, ()))))
                    advanced = True
                    break
                elif on_stack.get(nxt):
                    lowlink[node] = min(lowlink[node], index[nxt])
            if advanced:
                continue
            work.pop()
            if work:
                parent = work[-1][0]
                lowlink[parent] = min(lowlink[parent], lowlink[node])
            if lowlink[node] == index[node]:
                scc = []
                while True:
                    w = stack.pop()
                    on_stack[w] = False
                    scc.append(w)
                    if w == node:
                        break
                result.append(scc)
    return result


def check(dump_dir, verbose=False):
    txns = load_history(dump_dir)
    if verbose:
        print(f"loaded {len(txns)} committed transactions from {dump_dir}")

    version_order = recover_version_order(txns)
    if verbose:
        print(f"recovered version order for {len(version_order)} keys")

    edges = build_edges(txns, version_order)
    if verbose:
        print(f"built {len(edges)} DSG edges (wr/ww/rw)")

    adj = defaultdict(set)  # src -> set of dst (dedup parallel edges for traversal/printing)
    edge_index = defaultdict(list)  # (src,dst) -> list of (kind, key)
    nodes = set()
    for (src, dst, kind, key) in edges:
        nodes.add(src)
        nodes.add(dst)
        adj[src].add(dst)
        edge_index[(src, dst)].append((kind, key))

    sccs = tarjan_scc(nodes, adj)
    cycles = [scc for scc in sccs if len(scc) > 1]
    # Self-loops (excluded above, checked anyway).
    for scc in sccs:
        if len(scc) == 1 and (scc[0], scc[0]) in edge_index:
            cycles.append(scc)

    if not cycles:
        if verbose:
            print("0 cycles found: history is serializable (DSG is acyclic).")
        return 0

    print(f"{len(cycles)} cycle(s) found -- serializability VIOLATION:\n")
    for i, scc in enumerate(cycles):
        print(f"--- cycle {i + 1} ({len(scc)} txns) ---")
        scc_set = set(scc)
        print("  txns: " + ", ".join(scc))
        print("  edges within cycle:")
        for src in scc:
            for dst in adj[src]:
                if dst in scc_set:
                    for (kind, key) in edge_index[(src, dst)]:
                        s, hexkey = key
                        print(f"    {src} --{kind}--> {dst}  (storage={s}, key={hexkey})")
        print()
    return 1


def main():
    args = sys.argv[1:]
    verbose = "--verbose" in args
    args = [a for a in args if a != "--verbose"]
    if len(args) != 1:
        print(__doc__)
        return 2
    dump_dir = args[0]
    try:
        return check(dump_dir, verbose=verbose)
    except ValueError as e:
        print(f"error: {e}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    sys.exit(main())
