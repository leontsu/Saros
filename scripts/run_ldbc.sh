#!/bin/bash
# Fig. 7, Sec. V-F: TuskFlow's LDBC Q2 on SF3, k concurrent Q2 clients, each with its own Country.
# Usage: run_ldbc.sh [saros|oze|tf ...]; LDBC_PATH: raw CSV directory. Oze is ldbc_saros --saros=false.
# tf is TuskFlow's C++ port (submodule TuskFlow/), which queues concurrent mammoths and runs them one at a time.
source "$(dirname "$0")/common.sh"
CPUS=${CPUS:-$(seq -s, 0 2 94)}
: "${LDBC_PATH:?set LDBC_PATH to the SF3 raw CSV directory}"
ulimit -s 1048576
WORK="--ldbc_path=$LDBC_PATH --thread_num=32 --ldbc_read_ratio=80"
declare -A ARM=(
  [saros]="$LDBC_SAROS $SAROS $SAROS_OPT $CF $NORULE --short_discovery_limit=1000 $WORK"
  [oze]="$LDBC_SAROS $OZE_IN_SAROS $NORULE --short_discovery_limit=1000 $WORK")
# The port's flags (docs/CONFIGURATIONS.md). The load is r/100 paced clients of 100 transactions per second each.
TF_WORK="--dataset=ldbc --ldbc_path=$LDBC_PATH --hops=1 --use_zipf=false --read_ratio=80 --client_tx_per_sec=100 \
--client_paced --workers=32 --lr_budget=2000000 --epoch_size=2000 --lr_kind=q2 --lr_repeat --lr_delay_ms=0 \
--duration_ms=60000 --fix_getnext_race --fix_mode_switch_race"
D=$OUT/ldbc_q2
for d in 500 0; do for rep in 1 2 3; do for r in 1000 10000; do for k in 1 2 4 8 16; do
  for arm in ${@:-saros oze tf}; do
    if [ "$arm" = tf ]; then
      # The paper's one-shot runs (d=0) used the port's QSBR fix. Its interactive runs predate the flag,
      # which only changes when reclaimed memory is freed.
      fix=; [ "$d" = 0 ] && fix=--fix_qsbr_retire_pin
      run $D tf_d${d}_k${k}_r${r}_$rep $CPUS $TF $TF_WORK --clients=$((r / 100)) \
        --lr_clients=$k --q2_stmt_delay_us=$d $fix
    else
      run $D ${arm}_d${d}_k${k}_r${r}_$rep $CPUS ${ARM[$arm]} --q2_clients=$k \
        --q2_stmt_delay_us=$d --tf_rate=$r --extime=60
    fi
  done
done; done; done; done
