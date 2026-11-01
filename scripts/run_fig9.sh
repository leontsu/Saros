#!/bin/bash
# Fig. 9: ablation. S1 Saros alone; S2 + read-set index; S3 + skipping
# retirable entries; S4 + reuse of a new row's history; S5 + chunk freeing.
source "$(dirname "$0")/common.sh"
CPUS=${CPUS:-$(seq -s, 0 2 78)}
declare -A STAGE=(
  [S1]="--read_set_hash_index=false --import_skip_retirable=false --reuse_fresh_graph_state=false $NOCF"
  [S2]="--read_set_hash_index=true --import_skip_retirable=false --reuse_fresh_graph_state=false $NOCF"
  [S3]="--read_set_hash_index=true --import_skip_retirable=true --reuse_fresh_graph_state=false $NOCF"
  [S4]="--read_set_hash_index=true --import_skip_retirable=true --reuse_fresh_graph_state=true $NOCF"
  [S5]="--read_set_hash_index=true --import_skip_retirable=true --reuse_fresh_graph_state=true $CF")
D=$OUT/fig9
for rep in 1 2 3; do
  for ns in 8 32; do
    for r in 1024 8192; do
      for st in S1 S2 S3 S4 S5; do
        l=${st}_s${ns}_r${r}_$rep
        run $D $l $CPUS $BOMB $SAROS ${STAGE[$st]} $STATIC $(rate_flags $r) \
          --thread_num=$((ns + 1)) --extime=60 --eval_sample_path=$D/$l.eval.tsv
      done
    done
  done
done
