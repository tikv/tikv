# `components/hybrid_engine` Maintenance Guide

## Purpose And Scope

`hybrid_engine` is a thin composition layer between the disk KV engine and the
region-cache engine. It does not own storage semantics by itself. It owns how a
single snapshot or iterator chooses between disk data and cache data.

## Architectural Views

### Composition view

- disk KV engine remains authoritative
- region cache engine is opportunistic and region-scoped
- hybrid snapshot chooses the correct backend per CF and snapshot availability

## Process Lifecycle And Startup Sequencing

- This crate is assembled in
  `components/server/src/common.rs::build_hybrid_engine` after RocksDB and the
  region-cache engine both exist.
- The runtime path then depends on observer registration in
  `components/server/src/server.rs`, where
  `HybridSnapshotObserver`, `LoadEvictionObserver`, and
  `RegionCacheWriteBatchObserver` are attached before raftstore starts serving
  cache-aware traffic.
- The main steady-state bridge is not direct engine bootstrap but observed
  snapshot handoff through `src/server/raftkv/mod.rs`, where
  `HybridEngineSnapshot::from_observed_snapshot` reconstructs the combined
  snapshot from the disk snapshot and the optional cache pin.
- If startup wiring changes, review both creation and observer-installation
  order. A cache-capable `HybridEngine` without the matching observers is
  usually a correctness bug, not only a performance regression.

## Data Model And Metadata Contracts

- Snapshot context includes region and read-ts information.
- Sequence number must align between disk snapshot and cache snapshot.
- Cache-backed access is valid only for region-scoped data CF reads.
- The high-risk contract is that cache visibility is derived from the disk
  snapshot sequence number. `components/hybrid_engine/src/snapshot.rs` treats
  the disk snapshot as the authoritative sequence-number source and only
  switches data-CF reads to cache when the cache snapshot was created for that
  same sequence number.
- `from_observed_snapshot` depends on the `ObservedSnapshot` downcast produced
  by `observer/snapshot.rs`. If that type or ownership model changes, the
  downcast path and pin-drop semantics must be revalidated together.
- `engine_iterator.rs` and `db_vector.rs` preserve the “same API, different
  backing store” contract. Iterator and point-get behavior must remain
  indistinguishable except for metrics and performance.

## Start Here

- `components/hybrid_engine/src/lib.rs`
- `components/hybrid_engine/src/engine.rs`
- `components/hybrid_engine/src/snapshot.rs`
- `components/hybrid_engine/src/engine_iterator.rs`
- `components/hybrid_engine/src/observer/mod.rs`
- `components/hybrid_engine/src/observer/write_batch.rs`
- `components/hybrid_engine/src/observer/snapshot.rs`
- `components/hybrid_engine/src/observer/load_eviction.rs`

## Must-Read File Order

1. `components/hybrid_engine/src/engine.rs`
2. `components/hybrid_engine/src/snapshot.rs`
3. `components/hybrid_engine/src/engine_iterator.rs`
4. `components/hybrid_engine/src/observer/snapshot.rs`
5. `components/hybrid_engine/src/observer/write_batch.rs`
6. `components/hybrid_engine/src/observer/load_eviction.rs`
7. `src/server/raftkv/mod.rs`

## Main Responsibilities

- hold both a disk engine and a region-cache engine
- create a `HybridEngineSnapshot`
- choose cache-backed reads only when a valid region-cache snapshot exists
- expose write-batch and snapshot observers that keep the cache coherent with
  raftstore activity

## Core Contracts

- Disk engine is still the authoritative full database.
- Region cache only accelerates eligible region-scoped reads.
- Cache snapshots are only valid if:
  - the cache engine is enabled
  - the caller provides region context
  - the cache snapshot can be created for the requested read ts and sequence
    number
- Data-CF reads may come from cache; non-data CFs still use disk snapshots.

## Observability And Operational Signals

- `components/hybrid_engine/src/metrics.rs` and
  `components/hybrid_engine/src/engine_iterator.rs` are the first places to
  inspect for hybrid read-path instrumentation.
- Hit or fallback behavior is still interpreted mostly through
  `components/in_memory_engine/src/metrics.rs` and coprocessor/storage metrics,
  not through a large metric surface in this crate itself.
- If hybrid reads start diverging from expected behavior, triage in this order:
  `src/server/raftkv/mod.rs`, `observer/snapshot.rs`, `snapshot.rs`,
  `engine_iterator.rs`, then the corresponding in-memory-engine metrics and
  logs.

## Change Management Guidance

- Keep this crate thin. If the change adds policy or lifecycle logic, verify
  whether that logic belongs in `in_memory_engine`, raftstore observers, or the
  storage bridge instead.
- If snapshot-selection rules change, update both this guide and the related
  cache-engine guide.
- Any change that touches observed snapshot conversion, iterator fallback, or
  observer registration must be reviewed as a cross-component change spanning
  `components/server`, `src/server/raftkv`, `components/in_memory_engine`, and
  raftstore observers.
- The TODO in `components/hybrid_engine/src/lib.rs` matters for maintenance:
  this crate may eventually be merged or reduced further. Do not build new
  subsystem ownership here unless there is a strong reason.

## Change-Impact Matrix

- Snapshot reconstruction or cache fallback changes:
  inspect `snapshot.rs`, `engine_iterator.rs`, and `src/server/raftkv/mod.rs`
- Observer or pin-lifetime changes:
  inspect `observer/snapshot.rs`, `observer/write_batch.rs`,
  `components/server`, and raftstore observer registration
- Cache-eligibility or read-path changes:
  inspect `snapshot.rs`, `components/in_memory_engine`, and coprocessor/storage
  read callers

## Review Checklist

- Does the change preserve fallback to disk when cache snapshots are missing or
  invalid?
- Does it preserve sequence-number alignment between disk and cache snapshots?
- Does it affect observer registration or the semantics of write mirroring?
- Does it incorrectly widen cache usage beyond region-scoped or safe-point-safe
  reads?
- Does it accidentally move more logic into this crate than the crate should
  own? This crate should stay thin.

## Observability And Tests

- Most validation is by unit tests in this crate and by integration with
  `in_memory_engine` and raftstore observers.
- `observer/test_write_batch.rs` and tests in `snapshot.rs` are the key local
  references.
- `components/test_raftstore/src/server.rs` and
  `components/test_raftstore/src/util.rs` are good integration anchors because
  they exercise the observer and hybrid snapshot wiring in a more realistic
  environment.

## Common Failure Modes

- cache hit on an invalid snapshot
- stale observer state after write application
- serving unsupported CFs from cache
- treating cache as authoritative instead of opportunistic

## Reading Map And Companion Docs

Suggested reading order:

1. `engine.rs`
2. `snapshot.rs`
3. `engine_iterator.rs`
4. `observer/write_batch.rs`
5. `observer/snapshot.rs`
6. `observer/load_eviction.rs`

Companion docs:

- `repo-overview.md`
- `components/in_memory_engine.md`
- `src/storage.md`

## Glossary

- Hybrid snapshot:
  snapshot abstraction over disk and cache backends
- Region cache:
  optional fast path for region-scoped reads
- Observed snapshot pin:
  the raftstore observer-owned object that keeps a cache snapshot valid until
  the hybrid snapshot reconstructs and drops it

## Related Components

- `components/in_memory_engine` is the actual cache engine implementation.
- `components/raftstore` provides the observer hooks and region metadata.
- `src/server/raftkv/mod.rs` and `src/storage` consume hybrid snapshots.
