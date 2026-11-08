#!/bin/bash
# Fig. 1a, Fig. 5, Sec. V-B: memory over 600 s at 0.25, 64 and 1,024 short requests per second, 3 repetitions.
# Arms: Saros with freeing on (solid) and off (dashed), Oze. Usage: run_memory.sh [arm...]
source "$(dirname "$0")/common.sh"
CPUS=${CPUS:-$(seq -s, 0 2 30)}
D=$OUT/memory
declare -A ARM=(
  [saros_on]="$BOMB_SAROS $SAROS $SAROS_OPT $RULE $CF"
  [saros_off]="$BOMB_SAROS $SAROS $SAROS_OPT $RULE $NOCF"
  [oze]="$BOMB_OZE $OZE $RULE")
for rep in 1 2 3; do
  for point in 1024 p15 64; do
    for arm in ${@:-saros_off saros_on oze}; do
      l=r${point}_${arm}_$rep
      [ -s $D/$l.time ] && continue
      mkdir -p $D
      case $arm in saros*) name=bomb_saros; retention="--retention_sample_path=$D/$l.retention.tsv --retention_sample_interval_s=10" ;;
                   *) name=bomb_oze; retention= ;; esac
      "$(dirname "$0")/rss_sampler.py" $D/$l.rss.tsv $name 780 &
      run $D $l $CPUS ${ARM[$arm]} $STATIC $(rate_flags $point) --thread_num=8 --extime=600 \
        $retention --eval_sample_path=$D/$l.eval.tsv
      wait
    done
  done
done
