# `components/backup-stream` Maintenance Guide

## Purpose And Scope

`components/backup-stream` owns TiKV's log-backup / PITR capture path. It
covers:

- task watch and task lifecycle for log-backup jobs
- leader-side observation of Raft apply events
- initial scans for newly observed regions
- temporary-file buffering, flush, and upload
- per-region, per-store, and global checkpoint tracking
- gRPC endpoints for force flush and checkpoint inspection

It is closely related to `resolved_ts`, `raftstore` observers, and the BR/TiDB
control plane, but the data-plane logic lives here.

## Architectural Views

### Data-plane view

1. A task is created in metadata under `/tidb/br-stream/...`.
2. `Endpoint` watches metadata and installs the task into the in-memory router.
3. `BackupStreamObserver` subscribes leader regions whose ranges overlap a task.
4. `router.rs` converts `CmdBatch` apply events into per-task event streams and
   tracks lock/resolved-ts state.
5. Temp files accumulate encoded events until flush.
6. Flush uploads data files and backup metadata, then updates task
   checkpoints.

### Control-plane view

- `metadata/client.rs` abstracts the metadata store and task status keys.
- `checkpoint_manager.rs` tracks resolved, frozen, and durable checkpoints.
- `service.rs` exposes force-flush and checkpoint-query RPCs.
- Fatal task errors pause the task and publish a service safe point in PD so GC
  does not outrun the last safe checkpoint.

## Process Lifecycle And Startup Sequencing

- The subsystem starts only when `log_backup.enable` is true.
- Startup wiring is in:
  - `components/server/src/server.rs` for classic `RaftKv`
  - `components/server/src/server2.rs` for `RaftKv2`
- Startup order matters:
  1. register `BackupStreamObserver` on the coprocessor host
  2. register `BackupStreamConfigManager`
  3. construct `Endpoint`
  4. start the worker that owns `Endpoint`
  5. register the gRPC `LogBackup` service
- `Endpoint::new` immediately spawns background loops on its Tokio runtime:
  - flush ticker
  - metadata task watcher
  - region subscription manager
  - checkpoint subscription manager
  - min-ts worker

Shutdown rule:

- The endpoint runtime owns task watching, region operations, and checkpoint
  callbacks. Do not drop it before the worker is stopped or checkpoint/task
  state can be left half-updated.

## Data Model And Metadata Contracts

`metadata/keys.rs` is the best place to start. The core metadata contracts are:

- task info:
  `/tidb/br-stream/info/<task>`
- task ranges:
  `/tidb/br-stream/ranges/<task>/<start_key>`
- region/store checkpoints:
  `/tidb/br-stream/checkpoint/<task>/...`
- storage checkpoint:
  `/tidb/br-stream/storage-checkpoint/<task>/<store_id>`
- pause and last-error state:
  `/tidb/br-stream/pause/<task>` and `/last-error/...`

Important in-memory state holders:

- `StreamTask`: task info plus paused state
- `Task`: endpoint message enum
- `ObserveOp`: region-observation state transitions
- `TaskSelector`: task matching by name, key, range, or all tasks
- `CheckpointManager`: resolved, frozen, and durable checkpoint maps
- `ResolvedRegions`: batch result of region checkpoint resolution

Checkpoint contract:

- `resolved_ts` is still advancing with incoming mutations.
- `freeze()` snapshots that progress for the next flush wave.
- `flush_and_notify()` moves frozen progress into durable checkpoint state and
  informs subscribers.
- global checkpoint updates must be monotonic and conservative; they are
  consumed by the outer control plane and by safe-point management.

## Start Here

- `components/backup-stream/src/lib.rs`
- `components/backup-stream/src/endpoint.rs`
- `components/backup-stream/src/router.rs`
- `components/backup-stream/src/observer.rs`
- `components/backup-stream/src/subscription_manager.rs`
- `components/backup-stream/src/checkpoint_manager.rs`
- `components/backup-stream/src/metadata/keys.rs`
- `components/backup-stream/src/metadata/client.rs`
- `components/backup-stream/src/service.rs`
- `components/backup-stream/src/metrics.rs`

## Must-Read File Order

1. `components/backup-stream/src/endpoint.rs`
2. `components/backup-stream/src/router.rs`
3. `components/backup-stream/src/observer.rs`
4. `components/backup-stream/src/subscription_manager.rs`
5. `components/backup-stream/src/checkpoint_manager.rs`
6. `components/backup-stream/src/metadata/keys.rs`
7. `components/backup-stream/src/metadata/client.rs`
8. `components/backup-stream/src/service.rs`
9. `components/backup-stream/src/metrics.rs`

## Core Concepts

- `StreamTask`: one log-backup task definition from metadata
- `Router`: per-task event routing, temp-file ownership, and flush handling
- `ObserveOp`: explicit state transitions for region observation
- `SubscriptionTracer`: region subscription registry and resolver ownership
- `CheckpointManager`: region/store/global checkpoint bookkeeping
- `InitialDataLoader`: initial scan engine for new regions

## Why It Matters

This crate is one of the easiest places to create silent RPO regressions. A
bug can leave a task apparently healthy while dropping mutations, advancing a
checkpoint too far, or pausing a task too late to keep GC safe.

## Critical Invariants

- Only leader-owned regions should emit apply events for a task.
- Initial scan output must be ordered with subsequent delta events; the barrier
  and observe-handle logic in `subscription_manager.rs` and `router.rs` are
  part of the correctness boundary.
- Locks tracked in the router must stay consistent with resolved-ts advancement
  or checkpoints can move ahead of unflushed data.
- Checkpoint advancement must be monotonic and must not publish data that has
  not been durably flushed.
- Fatal task errors must pause the task and publish a conservative service safe
  point.
- Range matching must stay stable across split/merge/leader changes.
- Metadata key layout is a compatibility surface. Renaming keys or changing
  interpretation without migration is unsafe.

## Observability And Operational Signals

Open `components/backup-stream/src/metrics.rs` first. The most useful signals
are:

- `tikv_log_backup_internal_actor_acting_duration_sec`
- `tikv_log_backup_event_handle_duration_sec`
- `tikv_log_backup_flush_duration_sec`
- `tikv_log_backup_store_checkpoint_ts`
- `tikv_log_backup_store_last_checkpoint_ts`
- `tikv_log_backup_store_last_checkpoint_region_id`
- `tikv_log_backup_errors`
- `tikv_log_backup_fatal_errors`
- `tikv_log_backup_pending_initial_scan`
- `tikv_log_backup_active_subscription_number`
- `tikv_log_backup_temp_file_memory_usage`
- `tikv_log_backup_temp_file_count`
- `tikv_log_backup_task_status`

Operationally useful logs tend to appear around:

- skipped or retried observation
- fatal upload/checkpoint errors
- global checkpoint updates
- failover marking
- high-memory warnings during observation

## Change Management Guidance

- Any change to metadata paths, pause semantics, or checkpoint semantics must
  update this guide in the same change.
- If a patch changes how apply events are decoded or which locks are tracked,
  audit `resolved_ts`, `cdc`, and the affected transaction metadata format.
- If a patch changes flush or temp-file behavior, audit global checkpoint and
  safepoint side effects, not only file upload code.
- If a patch changes observation retry logic, review split/merge and leader
  transfer cases explicitly.

## Change-Impact Matrix

- Task lifecycle or metadata watch changes:
  inspect `endpoint.rs` and `metadata/client.rs`
- Apply-event decoding or filtering changes:
  inspect `router.rs`, observer wiring, and transaction metadata readers
- Initial scan changes:
  inspect `subscription_manager.rs`, `event_loader.rs`, and resolver ordering
- Checkpoint semantics changes:
  inspect `checkpoint_manager.rs`, global checkpoint updates, and PD safe-point
  calls
- Temp-file or upload changes:
  inspect `router.rs`, `tempfiles.rs`, and metadata file emission

## Review Checklist

- Does the patch change when a region starts or stops being observed?
- Does it change barrier ordering between initial scan and delta events?
- Does it change checkpoint freeze/flush/global-update sequencing?
- Does it change metadata key naming or persistence format?
- Does it change pause/error behavior or service-safe-point updates?
- Does it change memory/backpressure handling in temp-file or routing code?

## Observability And Tests

- Integration tests live in `components/backup-stream/tests/integration/mod.rs`.
- Failpoint coverage lives in `components/backup-stream/tests/failpoints/mod.rs`.
- Unit coverage is spread across `router.rs`, `checkpoint_manager.rs`,
  `metadata/test.rs`, `observer.rs`, and `subscription_manager.rs`.
- Many production regressions require cluster tests because leadership and
  region topology changes are part of the steady-state design.

## Common Failure Modes

- repeated observe retries on stale leadership or epoch drift
- checkpoint advancing ahead of flushed data
- task pause state not matching actual fatal error state
- failover stores publishing checkpoints too aggressively
- temp-file memory growth or swap behavior causing backpressure
- metadata drift between task ranges and in-memory router state

## Reading Map And Companion Docs

Suggested reading order:

1. `endpoint.rs`
2. `router.rs`
3. `subscription_manager.rs`
4. `checkpoint_manager.rs`
5. `metadata/client.rs`

Companion docs:

- `repo-overview.md`
- `components/cdc.md`
- `components/resolved_ts.md`
- `src/gc_worker.md`
- `components/raftstore.md`
- `components/raftstore-v2.md`
- BR overview:
  `https://docs.pingcap.com/tidb/stable/backup-and-restore-overview/`

## Glossary

- Initial scan:
  the catch-up scan from last checkpoint when a region first becomes observed
- Resolved-ts:
  the in-flight per-region progress bound used to determine which mutations are
  safe to flush into the next checkpoint wave
- Frozen checkpoint:
  the snapshot of resolved progress associated with the current flush wave
- Storage checkpoint:
  the durable checkpoint uploaded by one store for one task
- Global checkpoint:
  the conservative lower bound used by the outer control plane

## Related Components

- `components/raftstore`
- `components/raftstore-v2`
- `components/resolved_ts`
- `src/server/gc_worker`
- `components/external_storage`
- `components/encryption`
- `components/cdc`
