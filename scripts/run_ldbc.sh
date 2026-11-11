#!/bin/bash
# Fig. 7, Sec. V-F: TuskFlow's LDBC Q2 on SF3. Usage: run_ldbc.sh many|single [arm...] (single is not in the paper); LDBC_PATH: raw CSV directory.
# Arms: saros, oze (the paper's numbers: ldbc_saros --saros=false), oze_standalone (ldbc_oze; never measured for the paper).
source "$(dirname "$0")/common.sh"
CPUS=${CPUS:-$(seq -s, 0 2 94)}
: "${LDBC_PATH:?set LDBC_PATH to the SF3 raw CSV directory}"
ulimit -s 1048576
mode=${1:?many or single}; shift
ARMS=${@:-saros oze}
WORK="--ldbc_path=$LDBC_PATH --thread_num=32 --ldbc_read_ratio=80"
arm_command() {
  case $1 in
    saros) echo "$LDBC_SAROS $SAROS $SAROS_OPT $CF $NORULE --short_discovery_limit=1000 $WORK" ;;
    oze) echo "$LDBC_SAROS $OZE_IN_SAROS $NORULE --short_discovery_limit=1000 $WORK" ;;
    oze_standalone) echo "$LDBC_OZE $OZE $NORULE $WORK" ;;
    *) echo "unknown arm $1" >&2; exit 1 ;;
  esac
}
case $mode in
  many)  # k concurrent Q2 clients, each with its own Country
    D=$OUT/ldbc_q2
    for d in 500 0; do for rep in 1 2 3; do for r in 1000 10000; do for k in 1 2 4 8 16; do
      for arm in $ARMS; do
        run $D ${arm}_d${d}_k${k}_r${r}_$rep $CPUS $(arm_command $arm) --q2_clients=$k \
          --q2_stmt_delay_us=$d --tf_rate=$r --extime=60
      done
    done; done; done; done ;;
  single)  # one Q2 without Country from hub h, started 60 s in
    D=$OUT/ldbc_q2_single
    for h in 0 1 2; do for arm in $ARMS; do
      run $D ${arm}_h$h $CPUS $(arm_command $arm) --q2_clients=1 --q2_stmt_delay_us=0 \
        --tf_rate=10000 --extime=130 --tf_lr_delay_s=60 --q2_single --q2_all_countries --q2_hub=$h
    done; done ;;
esac
