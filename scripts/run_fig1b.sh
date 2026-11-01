#!/bin/bash
# Fig. 1b and Sec. V-C: L1 validation cost (#l1cost lines on stderr) at 64
# short requests per second, for 50, 100, 150 and 200 products per factory.
source "$(dirname "$0")/common.sh"
CPUS=${CPUS:-$(seq -s, 0 2 94)}
D=$OUT/fig1b
for p in 50 100 150 200; do
  l=p${p}_saros
  run $D $l $CPUS $BOMB $SAROS $SAROS_OPT $CF $STATIC --bomb_base_product_size_per_factory=$p \
    $(rate_flags 64) --thread_num=9 --extime=90 --l1_cost_diag=true --eval_sample_path=$D/$l.eval.tsv
done
