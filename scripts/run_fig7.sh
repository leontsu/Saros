#!/bin/bash
# Fig. 7: interactive transactions on a static BoM with 32 short-transaction
# threads; a round trip of d microseconds at each statement.
source "$(dirname "$0")/common.sh"
CPUS=${CPUS:-$(seq -s, 0 2 78)}
RATES=${RATES:-"1 2 4 8 16 32 64 128 256 512 1024 2048 4096 8192 8192x2 8192x4 8192x8"}
REPS=${REPS:-1}
D=$OUT/fig7
for d in ${@:-0 100 200 300 500}; do
  for rep in $(seq $REPS); do
    for r in $RATES; do
      l=d${d}_r${r}_$rep
      run $D $l $CPUS $BOMB $SAROS $SAROS_OPT $CF $STATIC $(rate_flags $r) \
        --thread_num=33 --extime=60 --bomb_interactive_us=$d --eval_sample_path=$D/$l.eval.tsv
    done
  done
done
