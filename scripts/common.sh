# Shared settings of the run scripts (sourced); every setting can be overridden from the environment.
# docs/CONFIGURATIONS.md gives the paper's values.
ROOT=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
BUILD=${BUILD:-$ROOT/build}
BOMB_SAROS=${BOMB_SAROS:-$BUILD/bin/bomb_saros}
LDBC_SAROS=${LDBC_SAROS:-$BUILD/bin/ldbc_saros}
BOMB_OZE=${BOMB_OZE:-$BUILD/bin/bomb_oze}
LDBC_OZE=${LDBC_OZE:-$BUILD/bin/ldbc_oze}
OUT=${OUT:-$ROOT/runs}
CLOCKS_PER_US=${CLOCKS_PER_US:-1800}  # TSC MHz of the machine
MEM=${MEM:-64G}                       # memory cap of every run (no swap)
NODE=${NODE:-0}                       # NUMA node(s) for memory

RUN="--epoch_time=40 --clocks_per_us=$CLOCKS_PER_US --insert_only_discovery_limit=1000"
RULE="--insert_only_single_writer=true"
NORULE="--insert_only_single_writer=false"

# Saros, with and without its three optimizations and the freeing of retired history chunks.
SAROS="--saros=true --retirement_threshold=128 --validate_insert_only_reads=true $RUN"
SAROS_OPT="--read_set_hash_index=true --import_skip_retirable=true --reuse_fresh_graph_state=true"
CF="--free_retired_history_chunks=true --free_retired_history_chunks_budget=65536"
NOCF="--free_retired_history_chunks=false"

# Oze: the standalone engine, and the path of the paper's Oze LDBC numbers (ldbc_saros --saros=false).
OZE="--read_set_hash_index=true $RUN"
OZE_IN_SAROS="--saros=false --retirement_threshold=128 --validate_insert_only_reads=true $RUN \
--read_set_hash_index=true --import_skip_retirable=false --reuse_fresh_graph_state=false $NOCF"

# BoMB: static BoM (S1/S2 = 50/50; L1 walks a cached BoM); dynamic BoM (S1..S5 = 45/45/1/1/8).
STATIC="--bomb_use_cache=true --bomb_perc_s1=50 --bomb_perc_s2=50 --bomb_perc_s3=0 \
--bomb_perc_s4=0 --bomb_base_product_size_per_factory=100"
DYNAMIC="--bomb_use_cache=false --bomb_perc_s1=45 --bomb_perc_s2=45 --bomb_perc_s3=1 \
--bomb_perc_s4=1 --bomb_base_product_size_per_factory=100"
OBSOLETE="--bomb_mark_obsolete=true"

# The short-request load: R per second, RxB (R batches of B per second), or pR (R per minute).
rate_flags() {
  case $1 in
    p*) echo "--bomb_req_batch_size=1 --bomb_mixed_short_rate_tps=false --bomb_mixed_short_rate=${1#p}" ;;
    *x*) echo "--bomb_req_batch_size=${1#*x} --bomb_mixed_short_rate_tps=true --bomb_mixed_short_rate=${1%x*}" ;;
    *) echo "--bomb_req_batch_size=1 --bomb_mixed_short_rate_tps=true --bomb_mixed_short_rate=$1" ;;
  esac
}

# run <dir> <label> <cpus> <command...>: one benchmark process in its own memory-capped scope, under
# /usr/bin/time -v; writes <label>.cmd, .out, .err and .time, and skips a run that has a .time file.
run() {
  local dir=$1 label=$2 cpus=$3; shift 3
  mkdir -p "$dir"
  [ -s "$dir/$label.time" ] && return 0
  echo "$*" > "$dir/$label.cmd"
  systemd-run --user --scope --quiet -p MemoryMax=$MEM -p MemorySwapMax=0 -- \
    numactl --membind=$NODE taskset -c "$cpus" /usr/bin/time -v -o "$dir/$label.time" \
    "$@" > "$dir/$label.out" 2> "$dir/$label.err"
}
