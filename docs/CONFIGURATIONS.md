# Configurations

This document gives every configuration the paper evaluates with this code as
a command line of the binaries built here, maps the flags of the archived runs
to these binaries, and lists the differences between the engines' workload
code. The run scripts in `scripts/` use exactly these command lines;
`scripts/common.sh` defines the flag groups below.

## Binaries

| Archived binary | Here | Used for |
|---|---|---|
| `bomb_oze_SW.exe` (Saros tree) | `build/bin/bomb_saros` | every BoMB number with Saros |
| `bomb_oze_sw.exe` (Oze tree) | `build/bin/bomb_oze` | every BoMB number with Oze |
| `ldbc_oze_GS.exe` (Saros tree) | `build/bin/ldbc_saros` | LDBC Q2, Saros and Oze (`--saros=false`) |
| none | `build/bin/ldbc_oze` | LDBC Q2 on the standalone Oze engine; never measured (see below) |

`bomb_saros` and `ldbc_saros` run Saros by default; `--saros=false` selects
Oze's merge-based record graphs in the same binary. The paper uses that path
for its Oze LDBC numbers and for the designated schedules, not for BoMB.

## Flags

### Protocol: both engines (`saros/include/common.hh`, `oze/include/common.hh`)

| Flag | Default | Meaning |
|---|---|---|
| `--insert_only_single_writer` | true | the single-writer insert-only rule; false: insert-only transactions validate their reads |
| `--insert_only_discovery_limit` | 1000 | abort an insert-only transaction with more propagation targets (0: no limit) |
| `--read_set_hash_index` | true | read-set lookups through a hash index |
| `--epoch_time` | 40 | epoch length in milliseconds |
| `--clocks_per_us` | 2100 | TSC frequency in MHz (the paper's machine: 1800) |
| `--thread_num`, `--extime` | 10, 3 | worker threads; measured seconds |
| `--l1_cost_diag` | false | one `#l1cost` line per L1 commit on stderr (Fig. 1b, Table I) |

### Protocol: Saros engine only

| Flag | Default | Meaning |
|---|---|---|
| `--saros` | true | Saros's histories and prefix references; false: Oze's record graphs |
| `--retirement_threshold` | 128 | retire history entries of a record once its history holds more entries (0: never) |
| `--validate_insert_only_reads` | true | validate the reads of insert-only transactions (false: they skip validation unconditionally; only the designated schedules use it) |
| `--short_discovery_limit` | 0 | the discovery limit for other transactions that read fewer records (0: off); read only by the Saros path |
| `--free_retired_history_chunks` | true | free history chunks that retirement has made obsolete |
| `--free_retired_history_chunks_budget` | 65536 | chunks and directories freed per reclamation pass |
| `--reuse_fresh_graph_state` | true | a new row keeps the history its constructor created |
| `--import_skip_retirable` | true | imports skip the entries retirement would drop at the importer's floor |
| `--reclaim_interval` | 64 | `mainte()` calls between two reclamation passes |
| `--retention_sample_path`, `--retention_sample_interval_s` | (off), 5 | periodic memory samples of histories and record states (BoMB) |
| `--history_dump_path` | (off) | per-thread dump of every committed transaction (`tools/check_history.py`) |

### Protocol: Oze engine only

| Flag | Default | Meaning |
|---|---|---|
| `--validation_th_num` | 1 | validation threads per transaction (the paper: 1, 4, 8, 32) |
| `--validation_threshold` | 100 | parallel validation runs only with more propagation targets than this |

### BoMB (`workloads/bomb.hh`)

`--bomb_use_cache` (static BoM: L1 walks a BoM cached at start-up),
`--bomb_mark_obsolete` (the obsolete-row version of BoMB),
`--bomb_perc_s1` ... `--bomb_perc_s4` (request mix; S5 gets the rest),
`--bomb_base_product_size_per_factory` (products per factory, i.e. per L1),
`--bomb_mixed_short_rate` with `--bomb_mixed_short_rate_tps` and
`--bomb_req_batch_size` (the short-request load: batches per second, or per
minute without `_tps`, over all queues), `--eval_sample_path` (per-second
commits, aborts, latency histograms), and the database sizes
`--bomb_factory_size`, `--bomb_product_size`, `--bomb_work_size`,
`--bomb_material_size`, `--bomb_tree_num_per_product`, `--bomb_base_tree_size`,
`--bomb_material_per_wip`, `--bomb_mc_update_size` (the paper uses their
defaults). Thread 0 runs L1; every other worker serves one request queue.
Thread 1 advances the global epoch on both engines, between its own requests.

### LDBC (`workloads/ldbc.cc`)

`--ldbc_path`, `--q2_clients`, `--q2_stmt_delay_us`, `--q2_single`,
`--q2_all_countries`, `--q2_hub`, `--tf_rate`, `--tf_lr_delay_s`,
`--ldbc_read_ratio` (default 80). Q2's Countries, path lengths (2-4), hub
factor (10) and the short transactions' `--ops` (10) are TuskFlow's and fixed
in the code.

## Flag groups

    RUN       = --epoch_time=40 --clocks_per_us=1800 --insert_only_discovery_limit=1000
    RULE      = --insert_only_single_writer=true
    NORULE    = --insert_only_single_writer=false
    SAROS     = --saros=true --retirement_threshold=128 --validate_insert_only_reads=true RUN
    SAROS_OPT = --read_set_hash_index=true --import_skip_retirable=true
                --reuse_fresh_graph_state=true
    CF        = --free_retired_history_chunks=true --free_retired_history_chunks_budget=65536
    NOCF      = --free_retired_history_chunks=false
    OZE       = --read_set_hash_index=true RUN
    OZE_IN_SAROS = --saros=false --retirement_threshold=128 --validate_insert_only_reads=true RUN
                --read_set_hash_index=true --import_skip_retirable=false
                --reuse_fresh_graph_state=false NOCF
    STATIC    = --bomb_use_cache=true --bomb_perc_s1=50 --bomb_perc_s2=50 --bomb_perc_s3=0
                --bomb_perc_s4=0 --bomb_base_product_size_per_factory=100
    DYNAMIC   = --bomb_use_cache=false --bomb_perc_s1=45 --bomb_perc_s2=45 --bomb_perc_s3=1
                --bomb_perc_s4=1 --bomb_base_product_size_per_factory=100
    OBSOLETE  = --bomb_mark_obsolete=true
    RATE(r)   = --bomb_req_batch_size=1 --bomb_mixed_short_rate_tps=true --bomb_mixed_short_rate=r
    RATE(rxb) = --bomb_req_batch_size=b --bomb_mixed_short_rate_tps=true --bomb_mixed_short_rate=r
    RATE(p15) = --bomb_req_batch_size=1 --bomb_mixed_short_rate_tps=false --bomb_mixed_short_rate=15

The rate ladder is 1, 2, 4, ..., 8192, 8192x2, 8192x4, 8192x8. Every run is
started as `systemd-run --user --scope -p MemoryMax=64G -p MemorySwapMax=0 --
numactl --membind=0 taskset -c <CPUs> /usr/bin/time -v <binary> <flags>`.

## Evaluated configurations

CPUs: `C78` = 0, 2, ..., 78; `C94` = 0, 2, ..., 94; `C30` = 0, 2, ..., 30.

### BoMB

| Paper item | Script | Command line | CPUs |
|---|---|---|---|
| Fig. 1b, Sec. V-C, Saros | `run_l1cost.sh` | `bomb_saros SAROS SAROS_OPT RULE CF STATIC --bomb_base_product_size_per_factory=p RATE(64) --thread_num=9 --extime=90 --l1_cost_diag=true --eval_sample_path=...`, p = 50, 100, 150, 200 | C94 |
| Table I, Fig. 1b, Sec. V-C, Oze | `run_l1cost.sh` | `bomb_oze OZE RULE --validation_th_num=v STATIC --bomb_base_product_size_per_factory=p RATE(64) --thread_num=9 --extime=90 --l1_cost_diag=true --eval_sample_path=...`, v = 1, 4, 8, 32, same p | C94 |
| Fig. 1a, Fig. 5, Sec. V-B, Saros | `run_memory.sh` | `bomb_saros SAROS SAROS_OPT RULE CF STATIC RATE(x) --thread_num=8 --extime=600 --retention_sample_path=... --retention_sample_interval_s=10 --eval_sample_path=...` (freeing on) and the same with `NOCF` (off, dashed), x = 1024, p15, 64, 3 repetitions; RSS once per second (`scripts/rss_sampler.py`) | C30 |
| Fig. 1a, Fig. 5, Sec. V-B, Oze | `run_memory.sh` | `bomb_oze OZE RULE STATIC RATE(x) --thread_num=8 --extime=600 --eval_sample_path=...`, same x, 3 repetitions; RSS as above | C30 |
| Fig. 6a/b, Saros | `run_throughput.sh static saros` | `bomb_saros SAROS SAROS_OPT RULE CF STATIC RATE(r) --thread_num=N+1 --extime=60 --eval_sample_path=...`, N = 1, 2, 4, 8, 16, 24, 32, r on the ladder | C78 |
| Fig. 6a/b, Oze | `run_throughput.sh static oze` | `bomb_oze OZE RULE STATIC RATE(r) --thread_num=N+1 --extime=60 --eval_sample_path=...` | C78 |
| Fig. 6a/b, Oze with 32 validation threads | `run_throughput.sh static oze32` | the same with `--validation_th_num=32` | C94 |
| Sec. V-D, rule off, Saros | `run_throughput.sh static saros_rule_off` | `bomb_saros SAROS SAROS_OPT NORULE CF STATIC RATE(r) --thread_num=N+1 --extime=60 ...` | C78 |
| Sec. V-D, rule off, Oze | `run_throughput.sh static oze_rule_off` | `bomb_oze OZE NORULE STATIC RATE(r) --thread_num=N+1 --extime=60 ...`; the paper's numbers come from an earlier build of this baseline without the rule and without the guard's epoch of slack | C78 |
| Fig. 6c/d, Sec. V-E, Saros | `run_throughput.sh dynamic saros` | `bomb_saros SAROS SAROS_OPT RULE CF DYNAMIC OBSOLETE RATE(r) --thread_num=N+1 --extime=60 --eval_sample_path=...` | C78 |
| Fig. 6c/d, Sec. V-E, Oze | `run_throughput.sh dynamic oze`, `oze32` | `bomb_oze OZE RULE DYNAMIC OBSOLETE RATE(r) --thread_num=N+1 --extime=60 ...` (`oze32`: with `--validation_th_num=32`) | C78; `oze32`: C94 |
| Sec. V-E, Oze at 200 GiB | `MEM=200G NODE=0,1 RATES="1 2 ... 8192" run_throughput.sh dynamic oze32 8 32` | as above, memory on both sockets | C94 |
| Sec. V-A, dynamic BoM without marking | `run_throughput.sh dynamic saros_no_obsolete` | `bomb_saros SAROS SAROS_OPT RULE CF DYNAMIC RATE(r) --thread_num=N+1 --extime=60 ...` | C78 |
| Fig. 7, Sec. V-F, Oze bars | `run_ablation.sh` | `bomb_oze OZE RULE STATIC RATE(r) --thread_num=ns+1 --extime=60 ...`, without (`--read_set_hash_index=false`) and with the index, r = 1024, 8192, ns = 8, 32, 3 repetitions | C78 |
| Fig. 7, Sec. V-F, Saros bars | `run_ablation.sh` | `bomb_saros SAROS RULE <stage> STATIC RATE(r) --thread_num=ns+1 --extime=60 ...`, same r, ns, repetitions; S1: index, skip and reuse false, `NOCF`; S2: + index; S3: + skip; S4: + reuse; S5: + `CF` | C78 |

The paper's Fig. 6 campaigns walked the rate ladder per thread count and
stopped once the short-transaction throughput changed by less than 5 % or after
three invalid points; Saros repeated the deciding rates to three runs. The
scripts run the whole ladder (`RATES` and `REPS` select a subset).

### LDBC

| Paper item | Script | Command line | CPUs |
|---|---|---|---|
| Fig. 8, Sec. V-G, Saros | `run_ldbc.sh many saros` | `ldbc_saros SAROS SAROS_OPT CF NORULE --short_discovery_limit=1000 --ldbc_path=<SF3> --thread_num=32 --ldbc_read_ratio=80 --q2_clients=k --q2_stmt_delay_us=d --tf_rate=r --extime=60`, k = 1, 2, 4, 8, 16, r = 1000, 10000, d = 500, 0, 3 repetitions, stack limit 1 GiB | C94 |
| Fig. 8, Sec. V-G, Oze (what produced the paper's numbers) | `run_ldbc.sh many oze` | `ldbc_saros OZE_IN_SAROS NORULE --short_discovery_limit=1000 <the same workload flags>` | C94 |
| Sec. V-H, single Q2 | `run_ldbc.sh single saros`, `oze` | as above with `--q2_clients=1 --q2_stmt_delay_us=0 --tf_rate=10000 --extime=130 --tf_lr_delay_s=60 --q2_single --q2_all_countries --q2_hub=h`, h = 0, 1, 2 | C94 |
| none | `run_ldbc.sh many oze_standalone` | `ldbc_oze OZE NORULE <the same workload flags>` | C94 |

LDBC transactions insert nothing, so the single-writer rule and the insert-only
limit have no effect there; `NORULE` matches the measured binaries, which
predate the rule. `--short_discovery_limit` is read only on the Saros path.

## The LDBC driver on the standalone Oze engine

`ldbc_oze` compiles the same `workloads/ldbc.cc` against `oze/`. **It was never
measured: every Oze LDBC number in the paper comes from
`ldbc_saros --saros=false`.** It needed no addition to the Oze engine; the
driver only includes `atomic_tool.hh` itself, and two outputs are compiled for
Saros only: the reclamation floor (`floor=-1` on Oze) and the chunk counters.

One 60-second run of each, on the paper's machine:

| Point | Run | Q2/min | short commits | short p99 | peak memory |
|---|---|---|---|---|---|
| k = 1, 500 us, 1K/s | `ldbc_saros --saros=false` | 534 | 59,991 | 387 ms | 12.9 GiB |
| | `ldbc_oze` | 553 | 60,006 | 0.08 ms | 8.7 GiB |
| k = 16, 500 us, 1K/s | `ldbc_saros --saros=false` | 8,250 | 60,006 | 230 ms | 14.6 GiB |
| | `ldbc_oze` | 8,164 | 60,007 | 0.73 ms | 10.4 GiB |
| k = 16, 500 us, 10K/s | `ldbc_saros --saros=false` | 7,241 | 212,224 | 38.7 s | 23.5 GiB |
| | `ldbc_oze` | 6,657 | 576,734 | 6.5 s | 59.4 GiB |
| k = 16, one-shot, 1K/s | `ldbc_saros --saros=false` | 29,703 | 60,007 | 244 ms | 16.0 GiB |
| | `ldbc_oze` | 26,046 | 59,977 | 194 ms | 11.8 GiB |

Q2 throughput agrees within about 4% with round trips at 1K/s and is 8-12% lower
on `ldbc_oze` at the two heavier points. Short-transaction service does not
agree, and that cause lies in the measured path, not in Oze: with
`--reuse_fresh_graph_state=false`, every row leaves a detached history state in
the registry's list (10.4M rows on SF3), and each reclamation pass walks that
list under the registry mutex, which stalls the short transactions that run the
pass. With `ldbc_saros --saros=false --reuse_fresh_graph_state=true` the short p99
falls to 0.08 ms (k = 1) and 0.65 ms (k = 16) at 1K/s. `ldbc_oze` at 10K/s serves
nearly the whole load, and its memory then grows to near the 64 GiB cap.

## Differences between the engines' workload code

The two engines share `workloads/bomb.hh`, `workloads/bomb.cc`,
`workloads/ldbc.cc`, `workloads/eval_sampler.hh` and `common/`. The archived
Saros and Oze copies of `bomb.hh`, of the BoMB driver and of the support code
differ, at the paper's flag values, only as follows; `ENGINE_SAROS` marks each
place. Request dispatch, the request mix, database generation, the
transactions, retries and the obsolete-row version of BoMB are the same code.

| Where | Saros | Oze |
|---|---|---|
| `bomb.hh`, `Query::getRequest()` | a worker that waits for a request leaves the epoch set (`become_quiescent()`) and rejoins before it runs the request (`resume_active()`) | a waiting worker stays in the epoch set and keeps its stamp, so at low load it holds back the global epoch until its next request |
| `bomb.hh`, `prepare()` | closes the empty transaction `reconnoiter_end()` leaves open (`abort()`), which allows the above | leaves it open |
| `bomb.cc` | retention sampler (off unless `--retention_sample_path`); after the workers join, it prints the chunk counters and drains the retirement registries | neither |
| `ldbc.cc` | prints the reclamation floor and the chunk counters | prints `floor=-1`; no chunk counters |

The engines themselves differ beyond the representation in what Saros adds as
safeguards (`docs/CORRECTNESS.md`): deleted rows and aborted versions are
retired and freed later (Oze frees a deleted row when its epoch is old enough
and never frees an aborted update's version), `get_min_epoch()` scans under a
mutex, and read and write sets name rows by stable identity rather than by
pointer. Validation threads exist only in Oze (`--validation_th_num`).

## Archived flags

The archived command lines (in the paper's run scripts) map to these binaries
as follows; the differential test of `docs/CORRECTNESS.md` applies exactly
this mapping.

Renamed:

| Archived | Here |
|---|---|
| `--incremental_graph_inheritance` | `--saros` |
| `--graph_log_compaction_threshold` | `--retirement_threshold` |
| `--graph_closure_state_reclaim_interval` | `--reclaim_interval` |
| `--r2_clients` | `--q2_clients` |
| `--ycsb_rratio` | `--ldbc_read_ratio` |

New defaults (an archived command line that omits one of these flags had the
archived default):

| Flag | Archived default | Engines |
|---|---|---|
| `--insert_only_single_writer` | false | both |
| `--read_set_hash_index` | false | both |
| `--insert_only_discovery_limit` | 0 | both |
| `--saros` | false | Saros |
| `--free_retired_history_chunks`, `--free_retired_history_chunks_budget` | false, 4096 | Saros |
| `--reuse_fresh_graph_state` | false | Saros |
| `--import_skip_retirable` | false | Saros |
| `--retirement_threshold` | 0 | Saros |
| `--validate_insert_only_reads` | false | Saros |
| `--ldbc_read_ratio` | 50 | LDBC |

Removed, with the value every evaluated configuration used, which the code now
always applies:

| Archived flag | Value | Engines |
|---|---|---|
| `--cc_mode` | 2 (graph mode; the switch to OCC never triggers) | both |
| `--epoch_guard_slack` | true (a read may skip a pending version of a writer one epoch older; see `docs/CORRECTNESS.md`) | both |
| `--insert_only_mixed_abort` | true (mixed writers abort) | both |
| `--forwarding` | true | both |
| `--bomb_mixed_mode` | true | both |
| `--bomb_l1_thread_num` | 1 | both |
| `--bomb_s1_thread_num` ... `--bomb_s4_thread_num` | 0 (used only without mixed mode) | both |
| `--validate_insert_only_reads` | true | Oze |
| `--validation_th_num`, `--validation_threshold` | 1, 100 (serial validation) | Saros |
| `--publish_record_write_facts`, `--complete_read_only_validation` | false, false | Saros |
| `--include_fanout` | false | Saros |
| `--historical_transaction_wide_propagation_marks` | true | Saros |
| `--graph_log_reachability_retirement`, `--graph_log_edge_retire_both_endpoints` | true, true | Saros |
| `--graph_log_include_depth_compaction_threshold` | 0 | Saros |
| `--historical_tuple_pointer_node_sets` | false | Saros |
| `--deferred_row_reclamation`, `--epoch_quiescence_repair` | true, true | Saros |
| `--legacy_validation_worker_local_propagation_scope` | true | Saros |
| `--graph_closure_conservative_retirement_floor`, `--graph_closure_cache_projection_floor` | true, true | Saros |
| `--graph_closure_cache`, `--graph_closure_packed_builder`, `--graph_closure_parallel_stream_builder` | false, false, false | Saros |
| `--enforce_oze_rules_configuration` | true (a start-up check of the values above) | Saros |
| `--read_footprint`, `--widen_imported_citations` | equal to `--saros` | Saros |
| `--ldbc_mode` | q2 | LDBC |

Every other archived flag (diagnostics, experiments, other workloads and
protocols) took its default, which disabled the code it controlled; that code
and the flag were removed. The output lines `insert_only_mixed_fulls`,
`#ShowOptParameters()` and `#insert_only_discovery_aborts=` are gone with them.

## Not reproducible with this repository

| Paper item | Source |
|---|---|
| D2PL, SSN, Silo, TicToc, MOCC and Cicada in Fig. 6 and Sec. V-D, V-E | other CCBench protocols |
| TuskFlow in Sec. V-G, V-H | TuskFlow's code |
| Sec. II-C duplication census | a diagnostic build |
| Sec. V-D, Oze with the rule off | an earlier build of this baseline (see above) |
