# Saros

Saros is a dependency representation for multi-version serialization graph
(MVSG) validation. Instead of copying dependencies into the record graph of
every record a transaction validates, as Oze does, Saros keeps each record's
dependencies once, in an append-only per-record history, and lets a history
refer to a prefix of another record's history. This repository contains Saros
exactly as the ICDE 2027 paper evaluates it: implemented inside Oze, on the
CCBench code base, with retired history chunks freed; and the paper's Oze
baseline as a standalone engine.

Authors: see the paper.

## Contents

| Path | Contents |
|---|---|
| `saros/` | The Saros engine (Oze with Saros): transactions, validation, record histories, prefix references, retirement, chunk freeing (`include/oze.hh`, `include/transaction.hh`, `transaction.cc`). `--saros=false` selects Oze's merge-based record graphs in the same engine. |
| `oze/` | The Oze baseline as a standalone engine: Oze's validation with parallel validation, the read-set index, the discovery limit and the single-writer insert-only rule |
| `workloads/` | The BoMB workload (`bomb.hh`), the BoMB and LDBC SNB Q2 drivers (`bomb.cc`, `ldbc.cc`, compiled once per engine) and the per-second sampler (`eval_sampler.hh`) |
| `common/` | CCBench support code; `common/engine/` holds the engine files that are the same for Saros and Oze |
| `tests/` | Unit tests, designated anomaly schedules and randomized schedules |
| `tools/check_history.py` | Offline serializability checker for `--history_dump_path` dumps |
| `scripts/` | Build script and run scripts |
| `docs/CONFIGURATIONS.md` | Every evaluated configuration as a command line, how the archived flags map to this tree, and the differences between the engines' workload code |
| `docs/CORRECTNESS.md` | Tests, schedules, the history checker and known inherited issues |
| `third_party/` | Masstree, mimalloc and GoogleTest (git submodules) |

The essential Saros functions and structs carry Doxygen comments, starting at
`TxExecutor::validation()` in `saros/transaction.cc` and `RecordGraphState` in
`saros/include/oze.hh`.

Binaries, in `build/bin/`:

| Binary | Engine | Used for |
|---|---|---|
| `bomb_saros` | `saros/` | every BoMB number of Saros |
| `ldbc_saros` | `saros/` | LDBC Q2, Saros; with `--saros=false`, the paper's LDBC numbers of Oze |
| `bomb_oze` | `oze/` | every BoMB number of Oze |
| `ldbc_oze` | `oze/` | LDBC Q2 on the standalone Oze engine; never measured for the paper (`docs/CONFIGURATIONS.md`) |

## Paper mapping

Figure and section numbers follow the current manuscript and may still change.

| Paper | Script | Notes |
|---|---|---|
| Sec. II-C, Table I; Fig. 1b, Sec. V-C (L1 validation cost) | `scripts/run_l1cost.sh` | `#l1cost` lines on stderr |
| Fig. 1a, Fig. 5, Sec. V-B (memory over 600 s) | `scripts/run_memory.sh` | Saros with freeing on and off, Oze |
| Fig. 6a/b, Sec. V-D (static BoM) | `scripts/run_throughput.sh static saros`, `oze`, `oze32` | rule off: `saros_rule_off`, `oze_rule_off` |
| Fig. 6c/d, Sec. V-E (dynamic BoM) | `scripts/run_throughput.sh dynamic saros`, `oze`, `oze32` | all with the obsolete-row version of BoMB; without it: `saros_no_obsolete` |
| Fig. 7, Sec. V-F (ablation) | `scripts/run_ablation.sh` | |
| Fig. 8, Sec. V-G (LDBC Q2) | `scripts/run_ldbc.sh many` | needs the LDBC SNB SF3 data set |
| Sec. V-H (single Q2 without Country) | `scripts/run_ldbc.sh single` | |

Not in this repository: the other protocols of Fig. 6 (D2PL, SSN, Silo, TicToc,
MOCC, Cicada), TuskFlow in Sec. V-G, and the diagnostic build of the duplication
census in Sec. II-C. `docs/CORRECTNESS.md` describes checks that are not part of the
paper's evaluation.

## Requirements

- Linux on x86-64 (the code reads the TSC with `rdtscp`), GCC 11 (the paper's
  binaries were built with GCC 11.4), CMake 3.10 or later, and
  autoconf/automake for Masstree.
- gflags and glog (`libgflags-dev`, `libgoogle-glog-dev` on Ubuntu).
- For the run scripts: `numactl`, GNU `time` (`/usr/bin/time`), systemd user
  scopes (`systemd-run --user --scope`) and Python 3.
- The paper ran every process in a 64 GiB memory scope without swap, with its
  memory on NUMA node 0 and its threads on even-numbered CPUs of that node
  (up to 48 hardware threads). Set `CLOCKS_PER_US` to your TSC frequency in MHz
  (the paper's machine: 1800).

## Build

    git clone --recursive <this repository> saros && cd saros
    scripts/build.sh          # Masstree and mimalloc into build/deps, then the binaries
    (cd build && ctest)       # the tests

This yields the four binaries in `build/bin/` and the test binaries in
`build/tests/`.

## Quick start

A 10-second BoMB run on a reduced database (static BoM, one L1 thread and two
short-transaction threads, 64 short requests per second):

    build/bin/bomb_saros --clocks_per_us=1800 --thread_num=3 --extime=10 \
      --bomb_use_cache=true --bomb_product_size=7200 --bomb_work_size=19800 \
      --bomb_material_size=7500 --bomb_mixed_short_rate_tps=true --bomb_mixed_short_rate=64

`build/bin/bomb_oze` takes the same flags. The output ends with per-type
commits, aborts and latencies; its `Summary:` line lists, per transaction type
(L1, S1, ..., S5), commits then aborts.

## Reproducing the figures

Each `scripts/run_*.sh` runs the paper's configurations for one family of
results and writes the raw outputs (`<label>.cmd`, `.out`, `.err`, `.time`, and
the sampler files) to `runs/`; a run that already has a `.time` file is
skipped. `BUILD`, `OUT`, `CPUS`, `MEM`, `NODE` and `CLOCKS_PER_US` can be set in
the environment (`scripts/common.sh`). Plotting scripts are not included yet.

## License

Apache License 2.0 (`LICENSE`); see `NOTICE` for the origin of the code and
the licenses of the submodules.
