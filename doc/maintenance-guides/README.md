# TiKV Maintenance Guides

This directory is a maintainership-oriented index for the TiKV repository. It is
not a user manual. It is meant to help reviewers and developers answer four
questions quickly:

1. Which subsystem owns this behavior?
2. Which files are the real entry points?
3. Which invariants are easy to break?
4. Which tests and metrics should move with the change?

## Maintainer Contract

These files are part of the repository's maintenance surface.

- Developers and reviewers should read the relevant guide here first before
  making or reviewing non-trivial changes.
- These guides are assistant documents for implementation and review, not
  optional afterthoughts.
- If a change modifies ownership boundaries, startup order, data contracts,
  invariants, operational signals, or the recommended reading map for a covered
  subsystem, the matching guide in this directory should be updated in the same
  change.
- A code change that invalidates these guides but does not update them should be
  treated as an incomplete maintenance change.

## Standard Section Contract

Each subsystem guide in this directory is expected to cover these domains:

1. Purpose and scope
2. Architectural views
3. Process lifecycle and startup sequencing
4. Data model and metadata contracts
5. Observability and operational signals
6. Change management guidance
7. Reading map and companion docs
8. Glossary
9. Must-read file order
10. Change-impact matrix

The depth varies by subsystem size. Small glue crates can keep some sections
short, but they should still be present.

## How To Use This Set

- Start with `repo-overview.md` to understand layer boundaries.
- Open the guide for the subsystem you are touching.
- Use the "Must-Read File Order", "Change-Impact Matrix", and "Review
  checklist" sections first.
- Treat the guide as required context for development or review when the change
  is non-trivial.
- Treat every guide as a map, not a complete specification. The source of truth
  is still the code.

## System Map

- Startup and process wiring: `components/server`
- Runtime gRPC, Raft transport, status server, GC, debug services: `src/server`
- `RaftKv2` bridge and tablet-based replica stack:
  `src/server/raftkv2` + `components/raftstore-v2`
- Transactional KV, MVCC, raw KV, scheduling, lock waiting: `src/storage`
- Classic TiDB coprocessor DAG/analyze/checksum path: `src/coprocessor`
- Plugin-based coprocessor framework: `src/coprocessor_v2`
- Classic raft-kv replica state machines, routing, snapshots, and
  split/heartbeat workers: `components/raftstore`
- Tablet-based replica state machines for `EngineType::RaftKv2`:
  `components/raftstore-v2`
- Generic FSM batching framework used heavily by raftstore:
  `components/batch-system`
- Shared region progress tracking for CDC, backup-stream, and stale-read-style
  consumers: `components/resolved_ts`
- MVCC GC, safe-point enforcement, and compaction-filter helpers:
  `src/server/gc_worker`
- Resource groups, fairness, admission control, quota adjustment:
  `components/resource_control`
- Hybrid disk + region-cache engine glue: `components/hybrid_engine`
- Region cache engine and background lifecycle: `components/in_memory_engine`
- Small service-control shim used by server/status server:
  `components/service`

## Guide Index

### Repository

- [Repository Overview](./repo-overview.md)

### `components/`

- [backup](./components/backup.md)
- [backup-stream](./components/backup-stream.md)
- [cdc](./components/cdc.md)
- [resolved_ts](./components/resolved_ts.md)
- [raftstore](./components/raftstore.md)
- [raftstore-v2](./components/raftstore-v2.md)
- [resource_control](./components/resource_control.md)
- [hybrid_engine](./components/hybrid_engine.md)
- [in_memory_engine](./components/in_memory_engine.md)
- [batch-system](./components/batch-system.md)
- [server](./components/server.md)
- [service](./components/service.md)
- [sst_importer](./components/sst_importer.md)

### `src/`

- [coprocessor](./src/coprocessor.md)
- [coprocessor_v2](./src/coprocessor_v2.md)
- [gc_worker](./src/gc_worker.md)
- [import](./src/import.md)
- [server](./src/server.md)
- [storage](./src/storage.md)

## Cross-Cutting Review Checklist

- Check whether the change touches a `#[PerformanceCriticalPath]` file or a
  request hot path.
- Verify whether the change is in a boundary layer:
  `src/server/service/kv.rs`, `src/storage/mod.rs`,
  `src/storage/txn/scheduler.rs`, `components/raftstore/src/store/peer.rs`,
  `components/batch-system/src/batch.rs`.
- Confirm thread or worker ownership:
  read pools, background workers, Yatp tasks, raft/apply pollers, status server.
- Check region-scoped assumptions:
  region epoch, bounds, leader state, snapshot lifetime, safe point, read ts.
- Check resource-control hooks:
  request metadata, quota limiter, fair scheduling, admission control labels.
- Check observability:
  metrics, tracing, slow-log, memory accounting, health/status endpoints.
- Check dynamic config behavior:
  config structs, config managers, runtime side effects, compatibility.
- Check failure semantics:
  timeout, cancellation, retries, undetermined results, backpressure,
  degraded-service mode.
- Move tests with the behavior:
  inline unit tests, failpoints, `components/test_raftstore`,
  `components/test_storage`, `components/test_coprocessor`.

## Current Scope

- The guide set currently includes:
  - `repo-overview.md`
  - `components/backup.md`
  - `components/backup-stream.md`
  - `components/cdc.md`
  - `components/resolved_ts.md`
  - `components/raftstore.md`
  - `components/raftstore-v2.md`
  - `components/resource_control.md`
  - `components/hybrid_engine.md`
  - `components/in_memory_engine.md`
  - `components/batch-system.md`
  - `components/server.md`
  - `components/service.md`
  - `components/sst_importer.md`
  - `src/coprocessor.md`
  - `src/coprocessor_v2.md`
  - `src/gc_worker.md`
  - `src/import.md`
  - `src/server.md`
  - `src/storage.md`
- The guide set now covers both the classic `raftstore` path and the
  `RaftKv2` / `raftstore-v2` path at maintainer-map granularity.
- The guides focus on maintenance and review, not on external deployment or SQL
  semantics.
- Additional cross-component subsystems such as `components/engine_rocks` and
  `components/pd_client` are still described primarily through
  `repo-overview.md` and the guides that depend on them.

## External Reference Links

These upstream docs are useful orientation material and were used together with
 the local code while drafting this guide set:

- TiKV overview: `https://tikv.org/docs/latest/concepts/overview/`
- Deep Dive TiKV: `https://tikv.org/deep-dive/introduction/`
- TiDB / TiKV overview: `https://docs.pingcap.com/tidb/stable/tikv-overview`
- TiDB resource control and resource groups:
  `https://docs.pingcap.com/tidb/stable/tidb-resource-control-overview`
- TiDB BR backup and restore overview:
  `https://docs.pingcap.com/tidb/stable/backup-and-restore-overview/`
- TiCDC overview:
  `https://docs.pingcap.com/tidb/stable/ticdc-overview/`
