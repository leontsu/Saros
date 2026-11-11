#!/bin/bash
# Dumps every committed transaction of 60-second Saros BoMB runs (static BoM at 64 and 1,024 requests per
# second, and the dynamic obsolete-row BoM at 1,024, the single-writer rule on) and checks each history
# offline. Oze has no history dump. Usage: run_history_check.sh [r64|r1024|dyn1024 ...]
source "$(dirname "$0")/common.sh"
CPUS=${CPUS:-$(seq -s, 0 2 30)}
D=$OUT/history_check
for l in ${@:-r64 r1024 dyn1024}; do
  case $l in dyn*) W="$DYNAMIC $OBSOLETE"; r=${l#dyn} ;; *) W=$STATIC; r=${l#r} ;; esac
  rm -rf "${D:?}/${l:?}.hist"; mkdir -p $D/$l.hist
  run $D $l $CPUS $BOMB_SAROS $SAROS $SAROS_OPT $RULE $CF $W $(rate_flags $r) \
    --thread_num=8 --extime=60 --history_dump_path=$D/$l.hist \
    --retention_sample_path=$D/$l.retention.tsv --retention_sample_interval_s=10 \
    --eval_sample_path=$D/$l.eval.tsv
  python3 "$ROOT/tools/check_history.py" $D/$l.hist --verbose > $D/$l.check.txt
  echo "$l: check_history exit $? ($(cat $D/$l.hist/history_*.log | wc -l) transactions)"
done
