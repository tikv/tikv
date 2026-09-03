# `src/server/gc_worker` Maintenance Guide

## Purpose And Scope

`src/server/gc_worker` owns TiKV's MVCC garbage-collection worker subsystem at
the actual repo ownership boundary. It covers:

- MVCC GC for leader-owned regions
- raw KV GC for raw keys
- unsafe destroy-range execution
- orphan-version cleanup delegated from compaction filters
- auto-GC scheduling based on PD safe point
- GC-related compaction-filter and auto-compaction integration
- dynamic config and runtime scaling for GC worker threads

This is the subsystem both BR- and CDC-adjacent maintainers need to read when a
change touches safe point, GC pace, or MVCC retention behavior.

## Architectural Views

### Task-execution view

1. `GcWorker` owns the worker scheduler, runner pool, and current safe point.
2. `GcManager` polls PD for the latest GC safe point and scans leader regions
   on this store.
3. Region work becomes `GcTask` items such as `Gc`, `GcKeys`, `RawGcKeys`,
   `UnsafeDestroyRange`, or `OrphanVersions`.
4. `GcRunner` performs MVCC reads, deletes obsolete versions, and writes the
   cleanup results back through the engine.
5. Compaction-filter and auto-compaction helpers either delete obsolete
   versions inline or bounce fallback work back into the GC worker.

### Control-plane view

- `GcSafePointProvider` abstracts PD safe-point reads.
- `GcWorkerConfigManager` applies dynamic config changes.
- `CompactionRunner` evaluates region candidates for automatic compaction.
- `WriteCompactionFilterFactory` and raw-KV compaction filters reuse the same
  safe point and config state.

## Process Lifecycle And Startup Sequencing

- Classic startup wiring lives in `components/server/src/server.rs`.
- `RaftKv2` startup wiring lives in `components/server/src/server2.rs`.
- Shared startup order:
  1. create `GcWorker`
  2. register `GcWorkerConfigManager`
  3. wire the worker into storage/server services that submit GC tasks
  4. after the node/store id is known, call `gc_worker.start(...)`
  5. call `start_auto_gc(...)`, which also initializes compaction-filter
     context for the disk engine before starting `GcManager`
- Path-specific note:
  - classic `RaftKv` currently starts auto-compaction after `start_auto_gc(...)`
  - `RaftKv2` currently does not start the auto-compaction runner in bootstrap
- `GcWorker::new` owns:
  - the safe-point cache
  - the runner pool and write limiter
  - scheduler capacity and task backpressure behavior
  - dynamic config tracker

Shutdown rule:

- Stop auto-GC and, if it was started on this path, auto-compaction before
  dropping the worker scheduler or compaction-filter context. Otherwise
  background threads can keep enqueueing work into a partially dismantled
  worker.

## Data Model And Metadata Contracts

- `GcTask` in `gc_worker.rs` is the main execution contract:
  `Gc`, `GcKeys`, `RawGcKeys`, `UnsafeDestroyRange`, and `OrphanVersions`.
- PD GC safe point is a correctness contract, not a hint. GC must never delete
  MVCC history newer than the effective safe point.
- `check_need_gc` in `mod.rs` is an optimization boundary based on MVCC table
  properties. It may produce false positives, but false negatives or a
  semantic change affect liveness and cleanup cost.
- `GcConfig` and `AutoCompactionConfig` are live operational contracts for:
  - batch size
  - write throttling
  - compaction-filter enablement
  - thread count
  - automatic compaction thresholds
- `RegionInfoProvider` is part of the subsystem contract because auto-GC and
  range tasks depend on current leader-owned region layout.

## Start Here

- `src/server/gc_worker/mod.rs`
- `src/server/gc_worker/gc_worker.rs`
- `src/server/gc_worker/gc_manager.rs`
- `src/server/gc_worker/config.rs`
- `src/server/gc_worker/compaction_filter.rs`
- `src/server/gc_worker/rawkv_compaction_filter.rs`
- `src/server/gc_worker/compaction_runner.rs`

## Must-Read File Order

1. `src/server/gc_worker/mod.rs`
2. `src/server/gc_worker/gc_worker.rs`
3. `src/server/gc_worker/gc_manager.rs`
4. `src/server/gc_worker/config.rs`
5. `src/server/gc_worker/compaction_filter.rs`
6. `src/server/gc_worker/rawkv_compaction_filter.rs`
7. `src/server/gc_worker/compaction_runner.rs`

## Core Concepts

- Safe point:
  the global lower bound below which obsolete MVCC history may be removed
- Auto-GC:
  the loop that polls PD and schedules per-region GC on leader-owned regions
- Orphan versions:
  default-CF versions left for GC worker cleanup when compaction filter cannot
  finish the delete path inline
- Compaction filter:
  RocksDB-time GC of old MVCC versions, guarded by cluster-version and runtime
  safety checks
- Auto compaction:
  background region candidate selection based on tombstones, redundant rows, and
  optionally MVCC-read pressure

## Why It Matters

This subsystem enforces retention and cleanup safety for TiKV MVCC state. Bugs
here can corrupt snapshot visibility, leak large amounts of obsolete data, or
let safe-point-sensitive subsystems such as CDC and BR observe history that was
deleted too early.

## Critical Invariants

- Safe point must be treated as a hard upper bound on what GC may delete.
- Auto-GC should only schedule region GC for regions whose leader is on this
  store.
- `GcTask` ordering and backpressure must not silently drop cleanup work.
- `UnsafeDestroyRange` remains a destructive administrative path and must keep
  strict region/context validation.
- Compaction filter must not run before safe point is initialized and must
  honor version-gate restrictions unless explicitly overridden.
- Fallback work from compaction filter must preserve correctness when the DB is
  stalled or inline cleanup cannot proceed.
- Dynamic config changes, especially thread count and write throttling, must
  take effect without leaving the worker in a half-updated state.

## Observability And Operational Signals

Start with:

- `src/server/metrics.rs`
- metrics registered in `compaction_filter.rs`
- metrics registered in `compaction_runner.rs`

The most useful signals are:

- auto-GC status gauges
- GC task latency and backlog metrics
- write-throttle and batch behavior metrics
- `tikv_gc_compaction_filtered`
- `tikv_gc_compaction_failure`
- `tikv_gc_compaction_filter_skip`
- `tikv_gc_compaction_filter_perform`
- `tikv_gc_compaction_filter_orphan_versions`
- `tikv_gc_compaction_filter_mvcc_deletion_met`
- `tikv_gc_compaction_filter_mvcc_deletion_handled`
- `tikv_gc_compaction_filter_mvcc_deletion_wasted`
- `tikv_auto_compaction_duration_seconds`
- `tikv_auto_compaction_regions_meet_threshold`
- `tikv_auto_compaction_pending_candidates`
- `tikv_auto_compaction_score`

Operationally useful logs appear around:

- safe-point polling failures
- repeated region scan/retry loops in auto-GC
- compaction-filter version-gate or stalled-DB skips
- worker saturation or schedule failures
- long-running GC tasks and destroy-range paths

## Change Management Guidance

- If a patch changes safe-point semantics, update this guide and review CDC,
  backup-stream, and storage snapshot readers in the same change.
- If a patch changes compaction-filter behavior, audit fallback orphan-version
  cleanup and downgrade/version-gate safety.
- If a patch changes region selection or auto-GC cadence, review split/merge
  and leader-transfer behavior explicitly.
- If a patch changes auto-compaction scoring, thresholds, or MVCC-read-aware
  prioritization, review operational impact as well as correctness.

## Change-Impact Matrix

- Safe-point or task semantics changes:
  inspect `gc_worker.rs`, `gc_manager.rs`, and storage MVCC GC helpers
- Dynamic config changes:
  inspect `config.rs`, pool scaling, and limiter behavior
- Compaction-filter changes:
  inspect `compaction_filter.rs`, `rawkv_compaction_filter.rs`, and fallback
  `OrphanVersions` task flow
- Auto-compaction changes:
  inspect `compaction_runner.rs`, region candidate scoring, and thresholds
- Destroy-range or raw-GC changes:
  inspect `GcTask` variants and region/provider validation

## Review Checklist

- Does the patch preserve safe-point correctness under retries and failures?
- Does it change which regions auto-GC schedules on a store?
- Does it change batch sizing, limiter behavior, or task backlog semantics?
- Does it change compaction-filter gating, fallback, or downgrade safety?
- Does it change orphan-version cleanup or raw-KV GC behavior?
- Does it change auto-compaction scoring or candidate eligibility?

## Observability And Tests

- Inline tests exist across `gc_worker.rs`, `mod.rs`, compaction-filter code,
  and auto-compaction helpers.
- Failpoint and cluster-style validation are important for compaction-filter,
  leadership, and safe-point race conditions.
- This guide complements `src/server.md`; use the dedicated guide when the
  change is truly about GC semantics rather than general server wiring.

## Common Failure Modes

- safe-point drift causing GC to run too conservatively or too aggressively
- GC worker backlog causing long retention lag
- auto-GC scanning stale region ownership and skipping needed cleanup
- compaction filter disabled unexpectedly due to version gate or DB stall
- orphan versions accumulating after fallback cleanup fails
- auto-compaction picking poor candidates and amplifying write pressure

## Reading Map And Companion Docs

Suggested reading order:

1. `mod.rs`
2. `gc_worker.rs`
3. `gc_manager.rs`
4. `config.rs`
5. `compaction_filter.rs`
6. `compaction_runner.rs`

Companion docs:

- `repo-overview.md`
- `src/server.md`
- `src/storage.md`
- `components/cdc.md`
- `components/backup-stream.md`
- `components/resolved_ts.md`

## Glossary

- GC safe point:
  the PD-published timestamp below which obsolete MVCC history may be deleted
- Auto-GC round:
  one pass that scans current leader-owned regions and schedules GC tasks
- Orphan versions:
  default-CF remnants left for worker-side cleanup after compaction-filter work
- Destroy range:
  the administrative path that forcefully removes a key range

## Related Components

- `src/storage`
- `src/server`
- `components/cdc`
- `components/backup-stream`
- `components/resolved_ts`
