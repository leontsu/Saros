# Saros

Saros is a dependency representation for multi-version serialization graph
(MVSG) validation. Instead of copying dependencies into the record graph of
every record a transaction validates, as Oze does, Saros keeps each record's
dependencies once, in an append-only per-record history, and lets a history
refer to a prefix of another record's history. This repository contains Saros
exactly as the ICDE 2027 paper evaluates it: implemented inside Oze, on the
CCBench code base, with retired history chunks freed.

Authors: see the paper.

## Contents

| Path | Contents |
|---|---|
| `saros/` | Oze with Saros: transactions, validation, record histories, prefix references, retirement, chunk freeing (`include/oze.hh`, `include/transaction.hh`, `transaction.cc`), and the drivers `bomb.cc` (BoMB) and `ldbc.cc` (LDBC SNB Q2) |
| `include/`, `common/` | CCBench support code; the BoMB workload (`include/bomb.hh`) and the per-second sampler (`include/eval_sampler.hh`) |
| `tests/` | Unit tests, designated anomaly schedules and randomized schedules |
| `tools/check_history.py` | Offline serializability checker for `--history_dump_path` dumps |
| `scripts/` | Build script and one run script per figure |
| `docs/CONFIGURATIONS.md` | Every evaluated configuration as a command line, and how the archived flags map to this tree |
| `docs/CORRECTNESS.md` | Tests, schedules, the history checker and known inherited issues |
| `third_party/` | Masstree, mimalloc and GoogleTest (git submodules) |

`bomb_saros` and `ldbc_saros` run Saros by default (`--saros=true`) and
Oze's merge-based record graphs with `--saros=false`.

## Paper mapping

| Paper | Script | Notes |
|---|---|---|
| Fig. 1a, Fig. 5, Sec. V-B (memory over 600 s) | `scripts/run_fig5.sh` | `run_fig5.sh ab`: freeing off vs. on |
| Fig. 1b, Sec. V-C (L1 validation cost) | `scripts/run_fig1b.sh` | `#l1cost` lines on stderr |
| Fig. 6 (static and dynamic BoM) | `scripts/run_fig6.sh static`, `dynamic` | |
| Fig. 7 (interactive transactions) | `scripts/run_fig7.sh` | |
| Fig. 9 (ablation) | `scripts/run_fig9.sh` | |
| Sec. V-G (LDBC Q2) | `scripts/run_ldbc.sh many`, `single` | needs the LDBC SNB SF3 data set |
| Sec. V-D (correctness) | `ctest`, `scripts/run_census.sh`, `scripts/run_history_check.sh` | see `docs/CORRECTNESS.md` |

Numbers of Oze, the other protocols and TuskFlow come from other code bases
and are not reproduced here (`docs/CONFIGURATIONS.md`).

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
    scripts/build.sh          # Masstree and mimalloc into build/deps, then Saros
    (cd build && ctest)       # the tests

This yields `build/saros/bomb_saros`, `build/saros/ldbc_saros` and the test
binaries in `build/tests/`.

## Quick start

A 10-second BoMB run on a reduced database (static BoM, one L1 thread and two
short-transaction threads, 64 short requests per second):

    build/saros/bomb_saros --clocks_per_us=1800 --thread_num=3 --extime=10 \
      --bomb_use_cache=true --bomb_product_size=7200 --bomb_work_size=19800 \
      --bomb_material_size=7500 --bomb_mixed_short_rate_tps=true --bomb_mixed_short_rate=64

The output ends with per-type commits, aborts and latencies; its `Summary:`
line lists, per transaction type (L1, S1, ..., S5), commits then aborts.

## Reproducing the figures

Each `scripts/run_*.sh` runs the paper's configurations for one figure and
writes the raw outputs (`<label>.cmd`, `.out`, `.err`, `.time`, and the
sampler files) to `runs/<figure>/`; a run that already has a `.time` file is
skipped. `BUILD`, `OUT`, `CPUS`, `MEM`, `NODE` and `CLOCKS_PER_US` can be set
in the environment (`scripts/common.sh`). Plotting scripts are not included
yet.

## License

Apache License 2.0 (`LICENSE`); see `NOTICE` for the origin of the code and
the licenses of the submodules.
