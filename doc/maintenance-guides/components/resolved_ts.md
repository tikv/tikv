# `components/resolved_ts` Maintenance Guide

## Purpose And Scope

`components/resolved_ts` owns TiKV's shared resolved-ts engine. It provides the
leader-side machinery used to compute a conservative progress timestamp for
observed regions so that CDC, backup-stream, stale read, and related features
can reason about which commits are definitely visible.

Its maintenance surface covers:

- lock-only apply-event observation from raftstore
- initial lock scan when a region starts being observed
- per-region resolver state and lock heap tracking
- periodic min-ts advancement driven by PD TSO and concurrency-manager locks
- leadership confirmation before publishing progress
- memory quota, re-registration, and diagnosis surfaces for observed regions

This crate does not own TiCDC or log backup task semantics. It owns the shared
timestamp computation and observation pipeline those subsystems depend on.

## Architectural Views

### Progress-computation view

1. `Observer` subscribes to lock-relevant applied command batches plus
   role/region changes from raftstore.
2. `Endpoint` owns the observed-region map and per-region `Resolver`.
3. `ScannerPool` acquires an incremental snapshot and scans existing locks for
   a newly observed region.
4. `Resolver` tracks pending and steady-state locks, tracked apply index, and
   the region's current resolved-ts.
5. `AdvanceTsWorker` obtains a fresh lower bound from PD and the concurrency
   manager, then `LeadershipResolver` confirms leadership before the endpoint
   publishes the advanced timestamp.

### State-ownership view

- `ObserveRegion`: one observed region plus observe handle, metadata, and
  resolver state
- `ResolverStatus::Pending`:
  initialization state while lock scan and buffered change logs are still being
  merged
- `Resolver`:
  steady-state lock heap, tracked index, min-ts, and diagnosis state
- `TsSource`:
  the explanation for why progress advanced or stalled

## Process Lifecycle And Startup Sequencing

- Classic `RaftKv` startup wiring lives in `components/server/src/server.rs`.
- `RaftKv2` startup wiring lives in `components/server/src/server2.rs`.
- `resolved_ts` starts only when `resolved_ts.enable` is true.
- Required startup order:
  1. create the worker and scheduler
  2. register `Observer` on the coprocessor host
  3. register `ResolvedTsConfigManager`
  4. construct `Endpoint`
  5. start the worker that owns the endpoint
  6. keep the scheduler available for subsystems that explicitly schedule
     resolved-ts tasks, such as debug-service diagnosis
- Relationship note:
  - CDC and backup-stream are closely related consumers of resolved-ts
    semantics, config, and invariants
  - they do not consume the resolved-ts worker scheduler directly during
    bootstrap; each owns its own min-ts / leadership-resolution path
- `Endpoint::new` also creates:
  - the lock-scan runtime and concurrency semaphore
  - the advance-ts worker
  - memory quota tracking for pending locks and steady-state lock heaps
  - the leadership resolver used to validate leader quorum before publishing
    progress

Shutdown rule:

- Stop dependent publishers and diagnostics before tearing down the endpoint and
  scan runtimes. Otherwise in-flight re-register, advance-ts, or diagnosis
  tasks can observe partially dismantled state and report misleading errors.

## Data Model And Metadata Contracts

- `Task` in `endpoint.rs` is the main internal protocol:
  `RegisterRegion`, `ScanLocks`, `ResolvedTsAdvanced`, `RegionUpdated`,
  `RegionDestroyed`, `ReRegisterRegion`, and diagnosis tasks all route through
  it.
- `ObserveHandle` / `ObserveId` are ABA guards. They must stay attached to
  scan, re-register, and applied-change processing.
- `ObserveRegion` binds region metadata, one observe handle, one resolver, and
  one initialization state machine.
- `Resolver` tracks:
  - `locks_by_key`
  - `lock_ts_heap`
  - large-transaction representatives
  - `tracked_index`
  - `resolved_ts`
  - `min_ts`
  - `last_attempt`
- `TsSource` is a diagnosis and contract surface. It distinguishes whether
  progress was limited by a lock, a memory lock in concurrency manager, PD TSO,
  CDC, or backup-stream.
- `RegionReadProgress` integration is part of the contract because stale-read
  users consume the published safe progress.

## Start Here

- `components/resolved_ts/src/lib.rs`
- `components/resolved_ts/src/observer.rs`
- `components/resolved_ts/src/endpoint.rs`
- `components/resolved_ts/src/resolver.rs`
- `components/resolved_ts/src/scanner.rs`
- `components/resolved_ts/src/advance.rs`
- `components/resolved_ts/src/cmd.rs`
- `components/resolved_ts/src/metrics.rs`

## Must-Read File Order

1. `components/resolved_ts/src/lib.rs`
2. `components/resolved_ts/src/observer.rs`
3. `components/resolved_ts/src/endpoint.rs`
4. `components/resolved_ts/src/resolver.rs`
5. `components/resolved_ts/src/scanner.rs`
6. `components/resolved_ts/src/advance.rs`
7. `components/resolved_ts/src/cmd.rs`
8. `components/resolved_ts/src/metrics.rs`

## Core Concepts

- Resolved-ts:
  a conservative timestamp such that no future commit below it should still
  appear for the observed region
- Initial scan:
  the snapshot-based lock scan that seeds the resolver before steady-state
  apply-event tracking begins
- Tracked index:
  the apply index boundary proving how far the resolver has consumed region
  history
- Leadership resolution:
  quorum confirmation via `CheckLeader` RPC before a leader publishes advanced
  progress
- `TsSource`:
  the reason progress advanced or stalled

## Why It Matters

This crate is a cross-component correctness hinge. Bugs here can make CDC or
backup-stream look healthy while silently advancing progress too far, or they
can stall whole downstream systems behind a conservative but incorrect zero or
stale resolved-ts.

## Critical Invariants

- Resolved-ts may advance only after the leader has applied on its own term.
- Only lock-relevant change logs should be fed into the resolver. Expanding or
  shrinking that filter changes both correctness and memory pressure.
- Initial scan results must merge with buffered change logs without losing the
  tracked-index ordering boundary.
- `ObserveId` checks must guard all long-lived async work to prevent stale scan
  results or re-registers from mutating a new owner.
- Resolver tracked index must be monotonic.
- Published resolved-ts must be no greater than the minimum of:
  - the current lower-bound timestamp
  - the oldest tracked lock
  - the concurrency manager's global in-memory min lock
- Leadership must be confirmed before publishing advanced progress; local
  leader state alone is not sufficient.
- Memory quota accounting for pending locks and lock heaps must stay balanced on
  error, cancellation, and drop paths.

## Observability And Operational Signals

Open `components/resolved_ts/src/metrics.rs` first. The most useful signals
are:

- `tikv_resolved_ts_min_resolved_ts`
- `tikv_resolved_ts_min_resolved_ts_gap_millis`
- `tikv_resolved_ts_min_leader_resolved_ts`
- `tikv_resolved_ts_min_leader_resolved_ts_gap_millis`
- `tikv_resolved_ts_min_follower_resolved_ts`
- `tikv_resolved_ts_min_follower_resolved_ts_gap_millis`
- `tikv_resolved_ts_zero_resolved_ts`
- `tikv_resolved_ts_scan_tasks`
- `tikv_resolved_ts_scan_duration_seconds`
- `tikv_resolved_ts_initial_scan_backoff_duration_seconds`
- `tikv_resolved_ts_fail_advance_count`
- `tikv_resolved_ts_lock_heap_bytes`
- `tikv_resolved_ts_memory_quota_in_use_bytes`
- `tikv_check_leader_request_pending_count`
- `tikv_resolved_ts_region_resolve_status`
- `tikv_concurrency_manager_min_lock_ts`

Operationally useful logs cluster around:

- repeated re-register after scan or epoch errors
- check-leader timeouts or failures
- memory quota exhaustion for pending locks
- regions stuck at zero resolved-ts
- long gaps between now and the minimum leader/follower resolved-ts

## Change Management Guidance

- If a patch changes lock observation, tracked apply-index behavior, or
  scan/re-register flow, review CDC and backup-stream in the same change.
- If a patch changes `TsSource`, diagnosis output, or read-progress integration,
  review stale-read and debug-service consumers too.
- If a patch changes memory quota, lock tracking strategy, or large-txn
  handling, audit both steady-state latency and failure cleanup paths.
- If a patch changes leadership validation or PD/concurrency-manager min-ts
  handling, review it as a correctness change, not a performance tweak.

## Change-Impact Matrix

- Observer or applied-change filtering changes:
  inspect `observer.rs`, `cmd.rs`, and resolver lock tracking
- Initial scan changes:
  inspect `scanner.rs`, `endpoint.rs`, and re-register behavior
- Resolver logic changes:
  inspect `resolver.rs`, lock heap accounting, and read-progress updates
- Min-ts advancement changes:
  inspect `advance.rs`, concurrency manager interaction, and leader checks
- Diagnosis or debug-service changes:
  inspect endpoint diagnosis tasks and `src/server/service/debug.rs`

## Review Checklist

- Does the patch preserve the apply-on-current-term requirement?
- Does it change the set of applied commands that resolved-ts observes?
- Does it change scan/barrier ordering between initial locks and delta events?
- Does it change `ObserveId` or tracked-index guards on async work?
- Does it change how concurrency-manager min locks cap progress?
- Does it change leader-check quorum logic, retry cadence, or timeout behavior?
- Does it change memory quota accounting for pending or steady-state locks?

## Observability And Tests

- Unit-heavy coverage lives in `observer.rs`, `resolver.rs`, `scanner.rs`, and
  `endpoint.rs`.
- Production regressions often require leadership change, split/merge, or
  long-running lock scenarios; pure unit tests are often insufficient.
- The debug service in `src/server/service/debug.rs` is a useful maintainer
  entry point when triaging stuck progress on live nodes.

## Common Failure Modes

- region stuck at zero resolved-ts after failed initial scan or missed
  re-register
- progress advancing too far because a lock source was missed
- progress lag caused by stale in-memory min-lock state or failed max-ts update
- stale async scan result mutating a newly re-observed region
- memory pressure from large lock heaps or pending-lock buildup
- check-leader RPC failures causing long progress stalls

## Reading Map And Companion Docs

Suggested reading order:

1. `lib.rs`
2. `observer.rs`
3. `endpoint.rs`
4. `resolver.rs`
5. `scanner.rs`
6. `advance.rs`

Companion docs:

- `repo-overview.md`
- `src/server.md`
- `components/cdc.md`
- `components/backup-stream.md`
- `src/storage.md`
- `src/gc_worker.md`

## Glossary

- Resolved-ts:
  the conservative timestamp below which no new commit should still appear for
  the observed region
- Change log:
  the filtered apply-event payload used to track locks and commits
- Initial scan:
  the bootstrap lock scan over a region snapshot
- Tracked index:
  the highest apply index the resolver has incorporated
- Check-leader:
  the quorum confirmation RPC used before publishing progress

## Related Components

- `components/cdc`
- `components/backup-stream`
- `components/raftstore`
- `components/raftstore-v2`
- `src/storage`
