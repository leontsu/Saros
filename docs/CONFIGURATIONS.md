# Configurations

This document gives every configuration the paper evaluates with this code as
a command line of the binaries built here, and maps the flags of the archived
runs to these binaries. The run scripts in `scripts/` use exactly these
command lines; `scripts/common.sh` defines the flag groups below.

## Binaries

| Archived binary | Here | Used for |
|---|---|---|
| `bomb_oze_GS.exe` | `build/saros/bomb_saros` | every BoMB number with Saros |
| `ldbc_oze_GS.exe` | `build/saros/ldbc_saros` | LDBC Q2, Saros and Oze arms |

The archived binaries take `--epoch_guard_slack=true` in every evaluated
configuration; here that rule is fixed (see below).

Both binaries run Saros by default; `--saros=false` selects Oze's merge-based
record graphs in the same binary (the LDBC Oze arm and the designated
schedules).

## Flags

### Protocol (`saros/include/common.hh`)

| Flag | Default | Meaning |
|---|---|---|
| `--saros` | true | Saros's histories and prefix references; false: Oze's record graphs |
| `--retirement_threshold` | 128 | retire history entries of a record once its history holds more entries (0: never) |
| `--validate_insert_only_reads` | true | validate the reads of insert-only transactions (false: Oze's shortcut) |
| `--insert_only_discovery_limit` | 1000 | abort an insert-only transaction with more propagation targets (0: no limit) |
| `--short_discovery_limit` | 0 | the same limit for other transactions that read fewer records (0: off) |
| `--free_retired_history_chunks` | true | free history chunks that retirement has made obsolete |
| `--free_retired_history_chunks_budget` | 65536 | chunks and directories freed per reclamation pass |
| `--reuse_fresh_graph_state` | true | a new row keeps the history its constructor created |
| `--read_set_hash_index` | true | read-set lookups through a hash index |
| `--import_skip_retirable` | true | imports skip the entries retirement would drop at the importer's floor |
| `--reclaim_interval` | 64 | `mainte()` calls between two reclamation passes |
| `--epoch_time` | 40 | epoch length in milliseconds |
| `--clocks_per_us` | 2100 | TSC frequency in MHz (the paper's machine: 1800) |
| `--thread_num`, `--extime` | 10, 3 | worker threads; measured seconds |
| `--l1_cost_diag` | false | one `#l1cost` line per L1 commit on stderr (Fig. 1b) |
| `--eval_sample_path` | (off) | per-second commits, aborts, latency histograms (`include/eval_sampler.hh`) |
| `--retention_sample_path`, `--retention_sample_interval_s` | (off), 5 | periodic memory samples of histories and record states (BoMB) |
| `--history_dump_path` | (off) | per-thread dump of every committed transaction (`tools/check_history.py`) |

### BoMB (`include/bomb.hh`)

`--bomb_use_cache` (static BoM: L1 walks a BoM cached at start-up),
`--bomb_perc_s1` ... `--bomb_perc_s4` (request mix; S5 gets the rest),
`--bomb_base_product_size_per_factory` (products per factory, i.e. per L1),
`--bomb_mixed_short_rate` with `--bomb_mixed_short_rate_tps` and
`--bomb_req_batch_size` (the short-request load: batches per second, or per
minute without `_tps`, over all queues), `--bomb_interactive_us` (round trip
per statement), and the database sizes `--bomb_factory_size`,
`--bomb_product_size`, `--bomb_work_size`, `--bomb_material_size`,
`--bomb_tree_num_per_product`, `--bomb_base_tree_size`,
`--bomb_material_per_wip`, `--bomb_mc_update_size` (the paper uses their
defaults). Thread 0 runs L1; every other worker serves one request queue.

### LDBC (`saros/ldbc.cc`)

`--ldbc_path`, `--q2_clients`, `--q2_stmt_delay_us`, `--q2_single`,
`--q2_all_countries`, `--q2_hub`, `--tf_rate`, `--tf_lr_delay_s`,
`--ldbc_read_ratio` (default 80). Q2's Countries, path lengths (2-4), hub
factor (10) and the short transactions' `--ops` (10) are TuskFlow's and fixed
in the code.

## Flag groups

    SAROS     = --saros=true --retirement_threshold=128 --validate_insert_only_reads=true
                --insert_only_discovery_limit=1000 --epoch_time=40 --clocks_per_us=1800
    SAROS_OPT = --read_set_hash_index=true --import_skip_retirable=true
                --reuse_fresh_graph_state=true
    CF        = --free_retired_history_chunks=true --free_retired_history_chunks_budget=65536
    NOCF      = --free_retired_history_chunks=false
    OZE       = --saros=false --retirement_threshold=128 --validate_insert_only_reads=true
                --insert_only_discovery_limit=1000 --epoch_time=40 --clocks_per_us=1800
                --read_set_hash_index=true --import_skip_retirable=false
                --reuse_fresh_graph_state=false --free_retired_history_chunks=false
    STATIC    = --bomb_use_cache=true --bomb_perc_s1=50 --bomb_perc_s2=50 --bomb_perc_s3=0
                --bomb_perc_s4=0 --bomb_base_product_size_per_factory=100
    DYNAMIC   = --bomb_use_cache=false --bomb_perc_s1=45 --bomb_perc_s2=45 --bomb_perc_s3=1
                --bomb_perc_s4=1 --bomb_base_product_size_per_factory=100
    RATE(r)   = --bomb_req_batch_size=1 --bomb_mixed_short_rate_tps=true --bomb_mixed_short_rate=r
    RATE(rxb) = --bomb_req_batch_size=b --bomb_mixed_short_rate_tps=true --bomb_mixed_short_rate=r
    RATE(p15) = --bomb_req_batch_size=1 --bomb_mixed_short_rate_tps=false --bomb_mixed_short_rate=15

The rate ladder is 1, 2, 4, ..., 8192, 8192x2, 8192x4, 8192x8. Every run is
started as `systemd-run --user --scope -p MemoryMax=64G -p MemorySwapMax=0 --
numactl --membind=0 taskset -c <CPUs> /usr/bin/time -v <binary> <flags>`.

## Evaluated configurations

| Paper item | Script | Command line | CPUs |
|---|---|---|---|
| Fig. 1b, Sec. V-C | `run_fig1b.sh` | `bomb_saros SAROS SAROS_OPT CF STATIC --bomb_base_product_size_per_factory=p RATE(64) --thread_num=9 --extime=90 --l1_cost_diag=true --eval_sample_path=...`, p = 50, 100, 150, 200 | 0, 2, ..., 94 |
| Fig. 1a, Fig. 5, Sec. V-B | `run_fig5.sh` | `bomb_saros SAROS SAROS_OPT CF STATIC RATE(x) --thread_num=8 --extime=600 --retention_sample_path=... --retention_sample_interval_s=10 --eval_sample_path=...` (freeing on) and the same with `NOCF` (off), x = 1024, p15, 64, 3 repetitions; RSS once per second (`scripts/rss_sampler.py`) | 0, 2, ..., 30 |
| Fig. 6a/b | `run_fig6.sh static` | `bomb_saros SAROS SAROS_OPT CF STATIC RATE(r) --thread_num=N+1 --extime=60 --eval_sample_path=...`, N = 1, 2, 4, 8, 16, 24, 32, r on the ladder | 0, 2, ..., 78 |
| Fig. 6c/d | `run_fig6.sh dynamic` | as Fig. 6a/b with `DYNAMIC` | 0, 2, ..., 78 |
| Fig. 7 | `run_fig7.sh` | `bomb_saros SAROS SAROS_OPT CF STATIC RATE(r) --thread_num=33 --extime=60 --bomb_interactive_us=d --eval_sample_path=...`, d = 0, 100, 200, 300, 500 | 0, 2, ..., 78 |
| Fig. 9 | `run_fig9.sh` | `bomb_saros SAROS <stage> STATIC RATE(r) --thread_num=ns+1 --extime=60 --eval_sample_path=...`, r = 1024, 8192, ns = 8, 32, 3 repetitions; S1: index, skip and reuse false, `NOCF`; S2: + index; S3: + skip; S4: + reuse; S5: + `CF` | 0, 2, ..., 78 |
| Sec. V-G, many Q2 | `run_ldbc.sh many` | `ldbc_saros SAROS SAROS_OPT CF` (or `OZE`) `--short_discovery_limit=1000 --ldbc_path=<SF3> --thread_num=32 --ldbc_read_ratio=80 --q2_clients=k --q2_stmt_delay_us=d --tf_rate=r --extime=60`, k = 1, 2, 4, 8, 16, r = 1000, 10000, d = 500, 0, 3 repetitions, stack limit 1 GiB | 0, 2, ..., 94 |
| Sec. V-G, single Q2 | `run_ldbc.sh single` | as above with `--q2_clients=1 --q2_stmt_delay_us=0 --tf_rate=10000 --extime=130 --tf_lr_delay_s=60 --q2_single --q2_all_countries --q2_hub=h`, h = 0, 1, 2 | 0, 2, ..., 94 |
| Sec. V-D, history check | `run_history_check.sh` | `bomb_saros SAROS SAROS_OPT CF STATIC RATE(r) --thread_num=8 --extime=60 --history_dump_path=<dir>` (plus both samplers), r = 64, 1024; then `tools/check_history.py <dir>` | 0, 2, ..., 30 |
| Sec. V-D, schedules and census | `ctest`, `run_census.sh` | see `docs/CORRECTNESS.md` | |

The archived Fig. 6 and Fig. 7 campaigns walked the rate ladder per thread
count (or delay) and stopped once the short-transaction throughput changed by
less than 5 % or after three invalid points, then repeated the best and the
next rate twice; the scripts run the whole ladder (`RATES` and `REPS` select a
subset).

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
| `--bomb_interactive_ms` (it counted microseconds) | `--bomb_interactive_us` |
| `--r2_clients` | `--q2_clients` |
| `--ycsb_rratio` | `--ldbc_read_ratio` |

New defaults (an archived command line that omits one of these flags had the
archived default): `--saros` (archived default false), `--free_retired_history_chunks`
(false), `--free_retired_history_chunks_budget` (4096), `--reuse_fresh_graph_state`
(false), `--read_set_hash_index` (false), `--import_skip_retirable` (false),
`--retirement_threshold` (0), `--validate_insert_only_reads` (false),
`--insert_only_discovery_limit` (0), `--ldbc_read_ratio` (50).

Removed, with the value every evaluated configuration used, which the code now
always applies:

| Archived flag | Value |
|---|---|
| `--cc_mode` | 2 (graph mode; the switch to OCC never triggers) |
| `--epoch_guard_slack` | true (a read may skip a pending version of a writer one epoch older; see `docs/CORRECTNESS.md`) |
| `--forwarding` | true |
| `--validation_th_num`, `--validation_threshold` | 1, 100 (serial validation) |
| `--publish_record_write_facts`, `--complete_read_only_validation` | false, false |
| `--include_fanout` | false |
| `--historical_transaction_wide_propagation_marks` | true |
| `--graph_log_reachability_retirement`, `--graph_log_edge_retire_both_endpoints` | true, true |
| `--graph_log_include_depth_compaction_threshold` | 0 |
| `--historical_tuple_pointer_node_sets` | false |
| `--deferred_row_reclamation`, `--epoch_quiescence_repair` | true, true |
| `--legacy_validation_worker_local_propagation_scope` | true |
| `--graph_closure_conservative_retirement_floor`, `--graph_closure_cache_projection_floor` | true, true |
| `--graph_closure_cache`, `--graph_closure_packed_builder`, `--graph_closure_parallel_stream_builder` | false, false, false |
| `--enforce_oze_rules_configuration` | true (a start-up check of the values above) |
| `--read_footprint`, `--widen_imported_citations` | equal to `--saros` |
| `--bomb_mixed_mode` | true |
| `--bomb_l1_thread_num` | 1 |
| `--bomb_s1_thread_num` ... `--bomb_s4_thread_num` | 0 (used only without mixed mode) |
| `--ldbc_mode` | q2 |

Every other archived flag (diagnostics, experiments, other workloads and
protocols) took its default, which disabled the code it controlled; that code
and the flag were removed.

## Not reproducible with this repository

| Paper item | Source |
|---|---|
| Table I; Oze points of Fig. 1b; Oze with 32 validation threads | an Oze build with an L1 cost diagnostic |
| Oze and published-Oze curves of Figs. 1a, 5, 6, 7, 9 | Oze builds |
| D2PL, 2PL, Silo and the other protocols of Figs. 6 and 7 | other CCBench protocols |
| Sec. II-C duplication census | a diagnostic build |
| TuskFlow arms of Sec. V-G | TuskFlow's code |
