#!/bin/bash
# Dumps every committed transaction of a 60-second Saros BoMB run (64 and 1,024 requests per second, the
# single-writer rule on) and checks the history offline. Oze has no history dump.
source "$(dirname "$0")/common.sh"
CPUS=${CPUS:-$(seq -s, 0 2 30)}
D=$OUT/history_check
for r in 64 1024; do
  l=r$r; rm -rf "${D:?}/${l:?}.hist"; mkdir -p $D/$l.hist
  run $D $l $CPUS $BOMB_SAROS $SAROS $SAROS_OPT $RULE $CF $STATIC $(rate_flags $r) \
    --thread_num=8 --extime=60 --history_dump_path=$D/$l.hist \
    --retention_sample_path=$D/$l.retention.tsv --retention_sample_interval_s=10 \
    --eval_sample_path=$D/$l.eval.tsv
  python3 "$ROOT/tools/check_history.py" $D/$l.hist --verbose > $D/$l.check.txt
  echo "$l: check_history exit $? ($(cat $D/$l.hist/history_*.log | wc -l) transactions)"
done
