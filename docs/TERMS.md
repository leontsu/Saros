# Paper terms in the code

The code keeps some older names. This table maps the paper's terms (Sec. III and IV)
to them. Unless noted, everything is in `saros/transaction.cc`, `saros/include/transaction.hh`
and `saros/include/oze.hh`.

| Paper | Code |
|---|---|
| History of a record | `RecordGraphState` |
| History entries: read fact, edge, prefix reference, abort marker | `GraphDelta` of kind `PublishRead`, `AddEdge`, `IncludeRecord`, `MarkAborted` |
| Validation (Algorithm 1) | `TxExecutor::validation()`, `write_validation()`, `read_validation()` |
| Order forwarding | the forwarding step of `write_validation()` |
| Publication, prefix reference (Algorithm 2) | `append_include()`; a read fact is appended at read time by `append_publish_read()` |
| Cutoff | `published_seq_` (a "citation") |
| The record where the validator last published | `last_published_tuple_` |
| Widening | `widen_citation_to_imported()` |
| Import (Algorithm 3) | `import_record()` |
| Discovery, targets (Algorithm 4) | `discover_propagation_targets()`, "propagation targets" |
| Footprint registry | `ReadFootprintRegistry` |
| Retirement, MaintainRecord (Algorithm 5) | `maybe_retire_record_history()` |
| Watermark | `retired_seq_` |
| Epoch floor | `reclamation_epoch_` |
| Retirement bound | `delta_retirement_epoch()` |
| Transitive bound | `citation_reach_epoch()` |
| Record registry (Sec. IV) | `RecordGraphRegistry` |
| Freeing retired chunks (Sec. IV) | `unlink_retired_prefix()`, `ChunkRetirementRegistry`, `reclaim_retired_chunks()` |
