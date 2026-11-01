#!/bin/bash
# Fig. 5, Fig. 1a and Sec. V-B: memory over 600 s at 0.25, 64 and 1,024 short requests per
# second, freeing off and on, 3 repetitions (the second in the order on, off).
source "$(dirname "$0")/common.sh"
CPUS=${CPUS:-$(seq -s, 0 2 30)}
D=$OUT/fig5
declare -A ARM=([off]="$NOCF" [on]="$CF")
for rep in 1 2 3; do
  [ $rep = 2 ] && order="on off" || order="off on"
  for point in 1024 p15 64; do
    for arm in $order; do
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
