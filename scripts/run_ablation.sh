#!/bin/bash
# Fig. 7, Sec. V-F: ablation. Oze without and with the read-set index; S1 Saros's core; S2 + read-set index;
# S3 + skipping retirable entries; S4 + reuse of a new row's history; S5 + chunk freeing.
source "$(dirname "$0")/common.sh"
CPUS=${CPUS:-$(seq -s, 0 2 78)}
declare -A STAGE=(
  [OZE]="$BOMB_OZE $OZE $RULE --read_set_hash_index=false"
  [OZE_IDX]="$BOMB_OZE $OZE $RULE"
  [S1]="$BOMB_SAROS $SAROS $RULE --read_set_hash_index=false --import_skip_retirable=false --reuse_fresh_graph_state=false $NOCF"
  [S2]="$BOMB_SAROS $SAROS $RULE --read_set_hash_index=true --import_skip_retirable=false --reuse_fresh_graph_state=false $NOCF"
  [S3]="$BOMB_SAROS $SAROS $RULE --read_set_hash_index=true --import_skip_retirable=true --reuse_fresh_graph_state=false $NOCF"
  [S4]="$BOMB_SAROS $SAROS $RULE --read_set_hash_index=true --import_skip_retirable=true --reuse_fresh_graph_state=true $NOCF"
  [S5]="$BOMB_SAROS $SAROS $RULE --read_set_hash_index=true --import_skip_retirable=true --reuse_fresh_graph_state=true $CF")
D=$OUT/ablation
for rep in 1 2 3; do
  for ns in 8 32; do
    for r in 1024 8192; do
      for st in OZE OZE_IDX S1 S2 S3 S4 S5; do
        l=${st}_s${ns}_r${r}_$rep
        run $D $l $CPUS ${STAGE[$st]} $STATIC $(rate_flags $r) \
          --thread_num=$((ns + 1)) --extime=60 --eval_sample_path=$D/$l.eval.tsv
      done
    done
  done
done
