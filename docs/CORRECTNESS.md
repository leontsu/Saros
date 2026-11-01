# Correctness

## Tests

`scripts/build.sh` builds four test binaries into `build/tests/`; `ctest` (in
`build/`) runs them. Each binary drives the production transaction code on
16 executors and the rows `a`..`z` of one table (`saros/include/test.hh`).

| Binary | Covers |
|---|---|
| `history_test` | record histories, prefix references, import, publication, order forwarding, retirement and its floor, deferred freeing of rows and versions, deleted rows, epoch quiescence; both representations |
| `retirement_test` | retirement bounds of prefix references, epoch membership transitions, the transaction-id limit, the record-state registry under growth |
| `schedule_test` | the designated anomaly schedules and randomized schedules, each on Oze's record graphs and on Saros |
| `chunk_freeing_test` | freeing of retired history chunks: references into freed prefixes, readers inside unlinked chunks, stale watermarks, fallback walks, byte counts, a concurrent stress test, deleted records |

Environment variables (the tests do not parse flags):
`OZE_TEST_VALIDATE_INSERT_ONLY_READS`, `OZE_TEST_FREE_RETIRED_HISTORY_CHUNKS`
(0 or 1), `OZE_TEST_RETIREMENT_THRESHOLD`, `OZE_TEST_READ_SET_INDEX`,
`OZE_TEST_IMPORT_SKIP_RETIRABLE` and `OZE_TEST_HISTORY_DUMP_PATH` (a history
dump of the test process); the variables of `schedule_test` are listed below.

## Epoch rules

A transaction's epoch is the minimum epoch over the active workers when it
begins, while its thread registers at the global epoch; the two can differ by
one, and the epoch rules allow for that difference:

- Write validation aborts a writer that is more than one epoch older than a
  reader of the record it writes.
- Order forwarding aborts instead of placing a version below the version of a
  writer more than one epoch older.
- The follower-epoch guard of the read (`get_visible_version()`): a read may
  skip a pending version whose writer is at most one epoch older than the
  reader; skipping an older writer's pending version aborts the read. Without
  the slack, a writer that began while one worker lagged would make every later
  reader of its pending versions abort, for as long as it runs.

`HistoryTest.FollowerEpochGuardAllowsOneEpochOldPendingWriter` checks the
guard with writers one and two epochs older, on both representations.

## Designated schedules (Sec. V-D)

`ScheduleTest.DesignatedSchedules` runs five schedules (a blind-write
discovery cycle, a read-crown cycle, an insert-only cycle, a propagation-mark
case and the read-only anomaly) on both representations, with
`--validate_insert_only_reads` false (Oze's shortcut) and true (Saros as
evaluated), and prints every commit vector. Saros decides every schedule as
Oze's record graphs do. Validating the reads of insert-only transactions
rejects the insert-only cycle; under Oze's rules both representations commit
every transaction of the other schedules, including those of the
non-serializable blind-write, read-crown and read-only-anomaly schedules. The
read-set reachability schedules of `ScheduleTest` check that Saros rejects
each of them whenever Oze does.

## Randomized census (Sec. V-D)

`scripts/run_census.sh` runs `ScheduleTest.RandomScheduleDirection` once per
seed (1000-5999): a random schedule of 3 transactions over 4 keys, run on
Oze's record graphs and on Saros. It reports how many schedules commit the
same transactions, how many commit more under Oze, and how many commit more
under Saros (the paper: 4977, 23 and 0). `OZE_RULES_RANDOM_N`,
`OZE_RULES_RANDOM_SEED`, `OZE_RULES_RANDOM_TX` and `OZE_RULES_RANDOM_KEYS`
set the schedules; `OZE_RULES_RANDOM_ONLY=legacy|saros` runs one
representation and `OZE_RULES_RANDOM_DUMP_ROOT=<dir>` dumps each run's
history for `tools/check_history.py`.

## Freeing does not change decisions (Sec. V-B)

With `OZE_TEST_FREE_CHUNKS_PREFILL=N`, `schedule_test` starts every history
with N retired entries, so that with freeing on their chunks are unlinked and
freed before the schedule runs. Freeing off and on must print the same commit
vectors (N = 192 and 200, retirement thresholds 1 and 128); the two
retirement unit tests assume empty histories and are excluded:

    for f in 0 1; do
      OZE_TEST_FREE_CHUNKS_PREFILL=200 OZE_TEST_RETIREMENT_THRESHOLD=1 \
      OZE_TEST_FREE_RETIRED_HISTORY_CHUNKS=$f build/tests/schedule_test \
        --gtest_filter='-ScheduleTest.Edge*' | grep -o 'schedule=.*commits=[01]*' > free$f.txt
    done
    cmp free0.txt free1.txt

## History checker (Sec. V-D)

`--history_dump_path=<dir>` makes every worker write one JSON line per
committed transaction (its reads with the version read, its writes with the
version below and a global install order) to `<dir>/history_<thread>.log`.
`tools/check_history.py <dir>` rebuilds each key's version order, builds the
direct serialization graph (wr, ww and rw edges) and reports every cycle;
it exits with 0 if there is none. `scripts/run_history_check.sh` does this
for 60-second BoMB runs at 64 and 1,024 requests per second.

## Equivalence with the measured build

This tree was produced from the source of the measured binaries by removing
code that no evaluated configuration executes, fixing the flags those
configurations held constant, and renaming. To check that no decision
changed, a differential test linked the original and the cleaned transaction
code into the same harness and ran randomized schedules (reads, writes,
inserts, deletes, scans, split validation and write phases, epoch advances,
retirement and chunk freeing) through both, for every evaluated
configuration; every per-operation result, commit decision and final history
was identical.

## Known inherited issues

These are in the measured code and kept unchanged:

- A scan's entry in the scan history (`ScanEntry`) keeps `std::string_view`s
  of the scan bounds, which point into buffers of the caller (the BoMB
  transactions' keys on the stack). When a later insert is checked against
  the scan (`insert_validation()`), the bounds may already hold other data,
  so the scanner -> inserter edge can be missed or added wrongly. This
  affects inserts into scanned ranges, i.e. S3 and S4 on a dynamic BoM.
- `TxExecutor::addScanEntry()` combines the scan range into a local variable
  that is not initialized (`uint64_t updated;`), so the range recorded for
  the insert fast path depends on stack contents. It also reads the first
  four bytes of each scan bound as an integer.
- `TxExecutor::delete_record()` erases matching entries from `write_set_`
  while iterating over it and does not free the erased version. The
  evaluated workloads never delete a key the transaction already wrote.
- BoMB's S4 (`Node::collect_leafs()`) also collects the inner nodes below a
  work-in-progress item, so S4 may delete an inner BoM edge instead of a
  material.
