#!/bin/bash
# Sec. V-D: the randomized census, 5,000 random schedules of 3 transactions on
# 4 keys, one per process, each on Oze's record graphs and on Saros.
source "$(dirname "$0")/common.sh"
D=$OUT/census; mkdir -p $D
for seed in $(seq ${FIRST:-1000} ${LAST:-5999}); do
  OZE_RULES_RANDOM_N=1 OZE_RULES_RANDOM_SEED=$seed $BUILD/tests/schedule_test \
    --gtest_filter=ScheduleTest.RandomScheduleDirection | grep 'random_direction\|saros_commits_extra'
done > $D/census.txt
awk '/random_direction/ { for (i = 1; i <= NF; i++) { split($i, kv, "="); n[kv[1]] += kv[2] } }
     END { print "identical=" n["identical"], "oze_commits_more=" n["legacy_commits_more"],
                 "saros_commits_more=" n["saros_commits_more"] }' $D/census.txt
