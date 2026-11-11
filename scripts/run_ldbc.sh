#!/bin/bash
# Fig. 7, Sec. V-F: TuskFlow's LDBC Q2 on SF3, k concurrent Q2 clients, each with its own Country.
# Usage: run_ldbc.sh [saros|oze ...]; LDBC_PATH: raw CSV directory. Oze is ldbc_saros --saros=false.
source "$(dirname "$0")/common.sh"
CPUS=${CPUS:-$(seq -s, 0 2 94)}
: "${LDBC_PATH:?set LDBC_PATH to the SF3 raw CSV directory}"
ulimit -s 1048576
WORK="--ldbc_path=$LDBC_PATH --thread_num=32 --ldbc_read_ratio=80"
declare -A ARM=(
  [saros]="$LDBC_SAROS $SAROS $SAROS_OPT $CF $NORULE --short_discovery_limit=1000 $WORK"
  [oze]="$LDBC_SAROS $OZE_IN_SAROS $NORULE --short_discovery_limit=1000 $WORK")
D=$OUT/ldbc_q2
for d in 500 0; do for rep in 1 2 3; do for r in 1000 10000; do for k in 1 2 4 8 16; do
  for arm in ${@:-saros oze}; do
    run $D ${arm}_d${d}_k${k}_r${r}_$rep $CPUS ${ARM[$arm]} --q2_clients=$k \
      --q2_stmt_delay_us=$d --tf_rate=$r --extime=60
  done
done; done; done; done
