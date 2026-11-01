#!/bin/bash
# Fig. 5 and 1a: memory over 600 s at 0.25, 64 and 1,024 short requests per second (3 runs each).
# "run_fig5.sh ab": the freeing comparison of Sec. V-B (off, and on with the budget 4096) instead.
source "$(dirname "$0")/common.sh"
CPUS=${CPUS:-$(seq -s, 0 2 30)}
D=$OUT/fig5
declare -A ARM=([saros]="$CF")
if [ "${1:-}" = ab ]; then
  D=$OUT/fig5_ab
  ARM=([off]="$NOCF" [on]="--free_retired_history_chunks=true --free_retired_history_chunks_budget=4096")
fi
for rep in 1 2 3; do
  for point in p15 64 1024; do
    for arm in "${!ARM[@]}"; do
      l=r${point}_${arm}_$rep
      [ -s $D/$l.time ] && continue
      mkdir -p $D
      "$(dirname "$0")/rss_sampler.py" $D/$l.rss.tsv bomb_saros 780 &
      run $D $l $CPUS $BOMB $SAROS $SAROS_OPT ${ARM[$arm]} $STATIC $(rate_flags $point) \
        --thread_num=8 --extime=600 --retention_sample_path=$D/$l.retention.tsv \
        --retention_sample_interval_s=10 --eval_sample_path=$D/$l.eval.tsv
      wait
    done
  done
done
