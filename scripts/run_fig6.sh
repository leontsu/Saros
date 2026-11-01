#!/bin/bash
# Fig. 6: BoMB on a static (a, b) or dynamic (c, d) BoM, N short-transaction threads plus L1, every
# rate of the ladder. Usage: run_fig6.sh static|dynamic [N...]  (default N: 1 2 4 8 16 24 32)
source "$(dirname "$0")/common.sh"
CPUS=${CPUS:-$(seq -s, 0 2 78)}
mode=${1:?static or dynamic}; shift
[ $mode = static ] && W=$STATIC || W=$DYNAMIC
RATES=${RATES:-"1 2 4 8 16 32 64 128 256 512 1024 2048 4096 8192 8192x2 8192x4 8192x8"}
REPS=${REPS:-1}
D=$OUT/fig6_$mode
for n in ${@:-1 2 4 8 16 24 32}; do
  for rep in $(seq $REPS); do
    for r in $RATES; do
      l=t${n}_r${r}_$rep
      run $D $l $CPUS $BOMB $SAROS $SAROS_OPT $CF $W $(rate_flags $r) \
        --thread_num=$((n + 1)) --extime=60 --eval_sample_path=$D/$l.eval.tsv
    done
  done
done
