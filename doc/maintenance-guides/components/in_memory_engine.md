# `components/in_memory_engine` Maintenance Guide

## Purpose And Scope

`in_memory_engine` implements the region cache engine used to accelerate
region-scoped reads. It owns cached-region lifecycle, skiplist-backed storage,
safe-point-aware snapshots, background loading and GC, and cache-driven
eviction.

## Architectural Views

### Storage view

- skiplist-backed cache for data CFs
- region metadata and snapshot metadata tracked separately from the raw data

### Lifecycle view

- pending/load/active/evict states
- background workers for load, GC, and memory-driven eviction

## Process Lifecycle And Startup Sequencing

- Created during engine/bootstrap setup.
- Background workers, range-hint services, and callbacks are attached before
  cache-assisted runtime behavior is expected.
- Correct shutdown requires background workers to stop after in-flight snapshot
  and region cleanup logic has safely drained.
- The maintainer entry points for lifecycle are
  `components/in_memory_engine/src/engine.rs::RegionCacheMemoryEngine::new`,
  `background.rs::BgWorkManager::new`, and
  `background.rs::PdRangeHintService::start`.
- Online config updates are applied through
  `config.rs::InMemoryEngineConfigManager`. Review startup defaults and online
  reconfiguration together because thresholds like `capacity`,
  `evict-threshold`, and `stop-load-threshold` are coupled.
- Shutdown behavior is partly implicit through `Drop` implementations in
  `background.rs` and `read.rs`. If a patch adds a new background worker or
  snapshot-holding path without a symmetric stop/drain path, it is probably
  incomplete.

## Data Model And Metadata Contracts

- Region cache metadata:
  region bounds, epoch version, safe point, state, active snapshots
- Snapshot metadata:
  read ts, region id/epoch, sequence number
- Memory model:
  internal encoded keys and skiplist-backed values across relevant CFs
- The most important contract lives between `region_manager.rs` and `read.rs`:
  `region_snapshot()` validates region identity, epoch, safe point, and active
  state before `RegionCacheSnapshot` is created, and snapshot drop feeds back
  into region-removal eligibility.
- `read.rs` requires bounded iteration. Callers that do not provide lower and
  upper bounds are rejected, and callers that exceed snapshot region bounds are
  rejected. That rule protects both correctness and implementation simplicity.
- `config.rs::validate` encodes operational contracts that are easy to miss in
  review: `gc_run_interval` bounds, `load_evict_interval` minimums, capacity
  derivation from block cache, and threshold ordering.
- `memory_controller.rs` treats node overhead as an estimated component of total
  memory use. If write-path accounting or skiplist structure changes, revisit
  memory-pressure decisions and the eviction thresholds they drive.

## Start Here

- `components/in_memory_engine/src/lib.rs`
- `components/in_memory_engine/src/engine.rs`
- `components/in_memory_engine/src/read.rs`
- `components/in_memory_engine/src/region_manager.rs`
- `components/in_memory_engine/src/background.rs`
- `components/in_memory_engine/src/memory_controller.rs`
- `components/in_memory_engine/src/write_batch.rs`
- `components/in_memory_engine/src/config.rs`

## Must-Read File Order

1. `components/in_memory_engine/src/engine.rs`
2. `components/in_memory_engine/src/region_manager.rs`
3. `components/in_memory_engine/src/read.rs`
4. `components/in_memory_engine/src/background.rs`
5. `components/in_memory_engine/src/memory_controller.rs`
6. `components/in_memory_engine/src/write_batch.rs`
7. `components/in_memory_engine/src/config.rs`
8. `components/in_memory_engine/src/metrics.rs`

## Internal Model

### Storage

- The cache is backed by a global skiplist engine across data CFs.
- Keys are encoded with MVCC-aware internal keys so that snapshots can observe a
  sequence-number-consistent view.

### Region lifecycle

- `region_manager.rs` tracks per-region metadata and lifecycle.
- Region states include:
  `Pending`, `Loading`, `Active`, `LoadingCanceled`, `PendingEvict`, `Evicting`.
- State transitions are constrained. Many correctness bugs here look like simple
  lifecycle bugs but become read-consistency bugs quickly.

### Snapshot semantics

- `read.rs` creates `RegionCacheSnapshot`.
- Snapshot creation validates region id, epoch, safe point, bounds, and
  sequence number assumptions.
- Snapshot drop feeds back into region lifecycle and may trigger region cleanup.

### Background work

- `background.rs` schedules region loading, GC, memory checks, delete-range
  cleanup, label-driven loading, and optional cross-checking.
- Background tasks also interact with PD for range hints and TSO.

## Critical Invariants

- A cache snapshot must never exceed region bounds.
- Safe point only moves forward.
- Cached data is valid only up to the region metadata and sequence number used
  for the snapshot.
- Region state transitions must stay legal; ad hoc transitions are a red flag.
- Eviction and deletion must respect active snapshots and GC tasks.
- Memory accounting must stay aligned with actual cached data structures.

## Observability And Operational Signals

- Start with `components/in_memory_engine/src/metrics.rs`. The highest-signal
  metrics include:
  `tikv_in_memory_engine_memory_usage_bytes`,
  `tikv_in_memory_engine_load_duration_secs`,
  `tikv_in_memory_engine_gc_duration_secs`,
  `tikv_in_memory_engine_eviction_duration_secs`,
  `tikv_in_memory_engine_cache_count`,
  `tikv_in_memory_engine_oldest_safe_point`,
  `tikv_in_memory_engine_newest_safe_point`, and
  `tikv_safe_point_gap_with_in_memory_engine`.
- `background.rs` emits the most important operational logs for load, GC,
  eviction, PD-hint handling, and task scheduling failures.
- `read.rs` seek/iterator behavior and `statistics.rs` ticker flushing matter
  when the symptom is query latency rather than obvious cache lifecycle failure.
- Triage entry points:
  `engine.rs`, `region_manager.rs`, `background.rs`, `read.rs`,
  `memory_controller.rs`, `metrics.rs`.

## Change Management Guidance

- Any region-state transition change should be documented here in the same
  patch.
- If snapshot or sequence-number rules change, update this guide and the
  `hybrid_engine` guide together.
- Treat changes to `RegionState`, safe-point advancement, snapshot drop
  behavior, or memory-threshold derivation as high-risk correctness changes.
- If a change introduces a new eviction reason, update
  `metrics.rs::EvictReasonType`, the associated logs, and this guide together.
- If config semantics change, review both bootstrap validation and online
  config dispatch. Silent threshold drift after a live config change is a real
  operational risk.

## Change-Impact Matrix

- Region-state or snapshot-lifetime changes:
  inspect `region_manager.rs`, `read.rs`, `background.rs`, and
  `hybrid_engine`
- Background load/GC/eviction changes:
  inspect `background.rs`, `memory_controller.rs`, metrics, and PD hint usage
- Memory-threshold or accounting changes:
  inspect `config.rs`, `memory_controller.rs`, write-batch paths, and operator
  metrics
- Safe-point or sequence-number changes:
  inspect `read.rs`, `engine.rs`, `region_manager.rs`, and hybrid snapshot
  consumers

## Review Checklist

- Does the change affect `RegionState` transitions or snapshot pin/unpin logic?
- Does it touch background task scheduling, retry, or shutdown behavior?
- Does it alter safe-point or sequence-number validation?
- Does it change region-boundary encoding or range deletion semantics?
- Does it update memory accounting, compaction, or lock tombstone cleanup?
- Does it require corresponding observer changes in `hybrid_engine` or
  `raftstore`?

## Observability And Tests

- Key tests are local:
  - `tests/failpoints/*`
  - `src/prop_test.rs`
  - `src/memory_usage_test.rs`
  - `benches/load_region.rs`
- Metrics are spread across `metrics.rs`, background tasks, and read paths.
- `background.rs` contains many lifecycle-heavy tests for GC, split, eviction,
  load limits, and config changes. It is one of the best change-impact oracles
  in this subsystem.

## Common Failure Modes

- use-after-drop style logic around snapshot removal and region cleanup
- stale or over-wide region bounds
- memory leakage due to skipped delete-range cleanup
- illegal region-state transitions during load/evict races
- serving reads under a too-old safe point

## Reading Map And Companion Docs

Suggested reading order:

1. `lib.rs`
2. `engine.rs`
3. `region_manager.rs`
4. `read.rs`
5. `write_batch.rs`
6. `background.rs`
7. `memory_controller.rs`

Companion docs:

- `repo-overview.md`
- `components/hybrid_engine.md`
- `src/coprocessor.md`

## Glossary

- Safe point:
  GC boundary before which old versions may be removed
- Cache region:
  region-scoped range eligible for cache operations
- Snapshot pin:
  lifetime hold that prevents premature cleanup
- Stop-load threshold:
  memory boundary after which new region loads should stop even before hard
  capacity is hit
- MVCC amplification:
  heuristic used by auto load/evict logic to decide whether caching a region is
  still beneficial

## Related Components

- `components/hybrid_engine` consumes snapshots from this engine.
- `components/raftstore` provides region metadata and observer integration.
- `src/coprocessor` is a major beneficiary of cache-backed reads.
