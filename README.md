# Saros

Code for the ICDE 2027 submission *Saros: Reducing Dependency Duplication in
Decentralized Serialization-Graph Validation*. Saros stores each dependency fact
once, in an append-only history at the record where it is created, and lets
other records refer to fixed prefixes of these histories. It extends Oze's
implementation on CCBench. `saros/` is the Saros engine, `oze/` the Oze
baseline as a standalone engine, and `workloads/` BoMB and LDBC Q2.

## Build

    git clone --recursive https://github.com/leontsu/Saros.git && cd Saros
    scripts/build.sh          # binaries in build/bin
    (cd build && ctest)       # tests

Requires Linux on x86-64, GCC 11, CMake 3.10+, autoconf/automake, gflags and
glog. The run scripts also need numactl, GNU time, systemd user scopes and
Python 3. Set `CLOCKS_PER_US` to the machine's TSC frequency in MHz.

## Paper mapping

| Paper | Script |
|---|---|
| Table I, Fig. 1b, Sec. V-C | `scripts/run_l1cost.sh` |
| Fig. 1a, Fig. 4, Sec. V-B | `scripts/run_memory.sh` |
| Fig. 5, Sec. V-B | `scripts/run_multil1.sh` |
| Fig. 6a-b, Sec. V-D | `scripts/run_throughput.sh static saros\|oze\|oze32` |
| Fig. 6c-d, Sec. V-E | `scripts/run_throughput.sh dynamic saros\|oze\|oze32` |
| Fig. 7, Sec. V-F | `scripts/run_ldbc.sh many` (LDBC SNB SF3) |
| Sec. III-G, randomized schedules | `scripts/run_census.sh` |

`docs/CONFIGURATIONS.md` lists every command line. `docs/CORRECTNESS.md`
describes the tests and the history checks (`scripts/run_history_check.sh`).

Not included: the other protocols of Fig. 6 (Oze's public code,
github.com/jnmt/ccbench, branch fix-vldb-paper), TuskFlow, the duplication
census of Sec. II-C, and plotting scripts. Binaries built here validate faster
than the measured ones, Oze by about 25% (`docs/CORRECTNESS.md`).

## License

Apache License 2.0. See `NOTICE` for the origin of the code and the submodules'
licenses.
