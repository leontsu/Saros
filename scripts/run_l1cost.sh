#!/bin/bash
# Fig. 1b, Table I, Sec. V-C: L1 validation cost (#l1cost lines on stderr) at 64 short requests per second,
# for 50, 100, 150 and 200 products per factory: Saros, and Oze with 1, 4, 8 and 32 validation threads.
source "$(dirname "$0")/common.sh"
CPUS=${CPUS:-$(seq -s, 0 2 94)}
D=$OUT/l1cost
RATE="$(rate_flags 64) --thread_num=9 --extime=90 --l1_cost_diag=true"
for p in 50 100 150 200; do
  l=p${p}_saros
  run $D $l $CPUS $BOMB_SAROS $SAROS $SAROS_OPT $RULE $CF $STATIC --bomb_base_product_size_per_factory=$p \
    $RATE --eval_sample_path=$D/$l.eval.tsv
  for v in 1 4 8 32; do
    l=p${p}_oze_v$v
    run $D $l $CPUS $BOMB_OZE $OZE $RULE --validation_th_num=$v $STATIC --bomb_base_product_size_per_factory=$p \
      $RATE --eval_sample_path=$D/$l.eval.tsv
  done
done
