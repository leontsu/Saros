#!/bin/bash
# Fig. 6, Sec. V-D, V-E: BoMB on a static or dynamic BoM, N short-transaction threads plus L1, the whole rate ladder.
# Usage: run_throughput.sh static|dynamic <arm> [N...]; arms and their paper items: docs/CONFIGURATIONS.md.
source "$(dirname "$0")/common.sh"
mode=${1:?static or dynamic}; arm=${2:?arm}; shift 2
[ $mode = static ] && W=$STATIC || W=$DYNAMIC
[ $mode = dynamic ] && OBS=$OBSOLETE || OBS=
CPUS78=$(seq -s, 0 2 78); CPUS94=$(seq -s, 0 2 94)
case $arm in
  saros)             F="$BOMB_SAROS $SAROS $SAROS_OPT $RULE $CF $OBS"; C=${CPUS:-$CPUS78} ;;
  oze)               F="$BOMB_OZE $OZE $RULE $OBS"; C=${CPUS:-$CPUS78} ;;
  oze32)             F="$BOMB_OZE $OZE $RULE $OBS --validation_th_num=32"; C=${CPUS:-$CPUS94} ;;
  *) echo "unknown arm $arm" >&2; exit 1 ;;
esac
RATES=${RATES:-"1 2 4 8 16 32 64 128 256 512 1024 2048 4096 8192 8192x2 8192x4 8192x8"}
REPS=${REPS:-1}
D=$OUT/throughput_${mode}_$arm
for n in ${@:-1 2 4 8 16 24 32}; do
  for rep in $(seq $REPS); do
    for r in $RATES; do
      l=t${n}_r${r}_$rep
      run $D $l $C $F $W $(rate_flags $r) --thread_num=$((n + 1)) --extime=60 --eval_sample_path=$D/$l.eval.tsv
    done
  done
done
