#!/bin/bash
# Fig. 5, Sec. V-B: k concurrent L1s, each on its own factories (static BoM, 64 factories, 1,024 short
# requests per second, 60 s, 3 repetitions); RSS once per second. Usage: run_multil1.sh [saros|oze ...]
source "$(dirname "$0")/common.sh"
CPUS=${CPUS:-$(seq -s, 0 2 78)}
D=$OUT/multil1
declare -A ARM=([saros]="$BOMB_SAROS $SAROS $SAROS_OPT $RULE $CF" [oze]="$BOMB_OZE $OZE $RULE")
for rep in $(seq 1 ${REPS:-3}); do
  for k in ${K:-1 2 4 8}; do
    for arm in ${@:-saros oze}; do
      l=k${k}_${arm}_$rep
      [ -s $D/$l.time ] && continue
      mkdir -p $D
      "$(dirname "$0")/rss_sampler.py" $D/$l.rss.tsv bomb_$arm 90 &
      run $D $l $CPUS ${ARM[$arm]} $STATIC $(rate_flags 1024) --bomb_factory_size=64 \
        --bomb_l1_own_factories=true --bomb_l1_thread_num=$k --leader_thid=$k --thread_num=33 \
        --extime=${EXTIME:-60} --eval_sample_path=$D/$l.eval.tsv
      wait
    done
  done
done
