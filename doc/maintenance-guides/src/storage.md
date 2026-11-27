# `src/storage` Maintenance Guide

## Purpose And Scope

`src/storage` is TiKV's transaction and storage layer. It is one of the most
important maintenance surfaces in the repository. It owns:

- the `Storage` API
- transactional command scheduling
- MVCC
- raw KV operations
- latches and lock waiting
- flow control
- storage-related dynamic config

## Architectural Views

### Layer view

- top-level `Storage` API
- command scheduler
- MVCC reader/writer logic
- raw KV logic
- lock waiting and flow control

### Request execution view

- RPC edge calls `Storage`
- scheduler admits the command
- MVCC/raw execution happens on snapshots and engines
- callbacks complete back to RPC or higher layers

## Process Lifecycle And Startup Sequencing

- Storage is constructed during server startup after engines, read pools,
  concurrency manager, quota limiter, and relevant runtime helpers exist.
- Runtime config managers update storage-side behavior after startup.
- Shutdown must preserve callback and worker ownership safety until all strong
  references are gone.

Concrete runtime anchors:

- storage construction in server bootstrap
- scheduler execution in `txn/scheduler.rs`
- dynamic config side effects in `config_manager.rs`

## Data Model And Metadata Contracts

- MVCC data model across default, lock, and write CFs
- scheduler task metadata and latch ownership
- lock-wait metadata and tokens
- raw KV encoding and optional API-version behavior
- resource-control metadata consumed during scheduling

High-risk contracts:

- MVCC relationships across default/lock/write CFs
- `ProcessResult` and callback completion semantics
- `TxnStatusCache` and max-ts related assumptions
- raw KV API version and TTL rules from `config.rs`

## Start Here

- `src/storage/mod.rs`
- `src/storage/txn/scheduler.rs`
- `src/storage/txn/mod.rs`
- `src/storage/txn/commands/mod.rs`
- `src/storage/mvcc/mod.rs`
- `src/storage/txn/store.rs`
- `src/storage/raw/store.rs`
- `src/storage/config.rs`
- `src/storage/config_manager.rs`

## Must-Read File Order

1. `src/storage/mod.rs`
2. `src/storage/config.rs`
3. `src/storage/txn/mod.rs`
4. `src/storage/txn/scheduler.rs`
5. `src/storage/txn/commands/mod.rs`
6. `src/storage/mvcc/mod.rs`
7. `src/storage/mvcc/reader/reader.rs`
8. `src/storage/txn/store.rs`
9. `src/storage/lock_manager/mod.rs`
10. `src/storage/config_manager.rs`

## Internal Structure

### Top-level API

- `mod.rs` defines `Storage` and many public operations.
- This is the coordination layer that ties together scheduler, read pool,
  concurrency manager, quota limiter, and resource control.

### Scheduler and command execution

- `txn/scheduler.rs` owns command admission, latches, task tracking, and worker
  handoff.
- `txn/commands/*` defines concrete transactional commands.
- `txn/task.rs`, `txn/sched_pool.rs`, and `txn/tracker.rs` support execution.
- `txn/sched_pool.rs` selects the priority queue only for customized resource
  groups. Background-only control stays on the vanilla queue and throttles
  matching long-running tasks inside the pool.
- `txn/sched_pool.rs` accumulates MVCC read flow from non-readonly scheduler
  commands, including reads performed by foreground writes, and reports
  region-level `ReadStats` to the raftstore reporter during ticker and worker
  shutdown flushes. Key ranges and bucket deltas are unavailable on this path.

### MVCC

- `mvcc/mod.rs` defines MVCC errors and public surface.
- `mvcc/reader/*` owns reads, scanners, point getters, and conflict detection.
- `mvcc/txn.rs` owns write-side MVCC mutation handling.

### Raw KV

- `raw/*` implements raw-key APIs and their optional MVCC wrappers.

### Waiting and flow control

- `lock_manager/*` defines storage-side lock-wait contracts and local wait-queue
  helpers.
- The live waiter-manager workers, deadlock detector, and lock-manager RPC
  service live under `src/server/lock_manager/*`.
- `txn/flow_controller/*` controls write pressure behavior.

### Max-ts dynamic configuration

- `config.rs` validates `max_ts.max_drift` against `cache_sync_interval` using
  their effective whole-millisecond values.
- `config_manager.rs` dispatches the exact duration to `ConcurrencyManager`.
- `ConcurrencyManager` deliberately stores the allowance in whole milliseconds
  because TSO physical timestamps are millisecond-based. For example, a valid
  configured value of `15s1us` has an effective enforcement allowance of
  `15000ms`; this is a domain boundary, not online-config truncation.

## Critical Invariants

- Command callbacks must complete exactly once with the correct error/result
  semantics.
- Lock-wait requeueing must check cancellation while holding the queue shard
  lock, but release that lock before waiting for a cancellation error or
  invoking a callback. The cancellation sender needs the same shard lock.
- Latch ownership must serialize conflicting commands without deadlocking the
  scheduler.
- MVCC readers and writers must preserve lock, write, and default-CF
  relationships.
- A shared primary belongs to one transaction identified by its start
  timestamp. Heartbeat, status checks, and TTL rollback update only that
  member. Resolving an expired pessimistic member for a pessimistic secondary
  removes the member without deciding the transaction. Resolving a prewritten
  secondary checks its named primary and protects the rollback on expiry.
  A terminal write takes precedence over a stale member.
- Shared pessimistic lock acquisition honors lock-only-if-exists before inserting
  a holder. Replaying a locking read on its own member returns the requested
  value or existence at the request's for-update timestamp. A newer for-update
  timestamp may change the member's primary and preserves its acknowledged
  TTL and minimum commit timestamp.
- Flashback rollback reads the full shared wrapper in the prepared region.
  A partial batch persists remaining holders and resumes at the same key.
  Its write budget includes the surviving wrapper and rollback records. An
  indivisible member and its remaining wrapper can exceed the batch target.
- A shared read on its own exclusive pessimistic lock persists that lock in
  CF_LOCK and waits for apply to remove any in-memory copy. Shared-lock
  prewrite accepts either the transaction's shared member or its retained
  exclusive lock. An explicit upgrade records its statement timestamp in the
  exclusive lock. Prewrite preserves that timestamp for replay.
- While an explicit upgrader is queued, its slot closes admission to new
  shared holders. Removing the upgrader wakes registered shared readers for a client retry. Partial resolutions
  publish the final holder set under the key latch after apply.
- Shared pessimistic locks, upgrades, and transitions of a shared wrapper
  to shrink-only take effect on Raft apply. Requests that change this state
  bypass in-memory pessimistic locking and pipelined responses.
- New shared members are admitted up to 1,024 per physical key and a 1 MiB
  encoded Lock CF value. Existing members can renew, prepare and drain even
  at capacity. Waiter admission and blocker refresh enforce the same
  16,384-edge budget per key. A refresh keeps the oldest registrations
  and completes excess waiters with the queue-capacity error.
- Region bounds, snapshot context, and flashback/max-ts safety must remain
  enforced.
- Max-ts relational validation must use the same whole-millisecond precision as
  runtime TSO enforcement.
- Memory quota and pending-write thresholds must remain operationally effective.

## Observability And Operational Signals

- scheduler latency, latch wait, and pending-write signals
- MVCC conflict, read, and GC-related metrics
- flow-control and memory-quota behavior
- lock-wait and deadlock diagnostics
- PD read-flow reports include reads performed by foreground write commands;
  inspect `txn/sched_pool.rs` when PD read bytes do not match the read-pool
  workload.
- Resource metering / TopSQL records logical IO when
  `resource-metering.enable-network-io-collection` is enabled. With
  `resource-metering.enable-detailed-io-collection` also enabled, logical reads
  and writes are selected independently and the foreground SQL request's
  RocksDB PerfContext delta is recorded as `rocksdb_block_read_count`. This
  field feeds the downstream `read_iops` dimension for relative attribution; it
  is not a device-level IOPS measurement. Storage command boundaries in
  `metrics.rs` own this attribution for read-only commands and read phases of
  write commands. In particular, transactional writes and
  `raw_compare_and_swap` must retain the `Storage` PerfContext because CAS reads
  the prior value before deciding whether to write. Resumed pessimistic-lock
  batches can contain work from multiple requests. TopSQL intentionally uses
  the synthetic command's first context as the representative for the whole
  batch, including logical writes and the single detailed-I/O PerfContext
  observation. Mixed-tag batches therefore have approximate attribution,
  avoiding per-item work on the lock-wakeup path. Do not attribute Raftstore
  apply/store write-worker activity to the request: those paths use write-only
  PerfContext metrics and may batch work from multiple requests.

Start triage with:

- `src/storage/metrics.rs`
- `txn/scheduler.rs`
- `lock_manager/*`
- `config_manager.rs`

## Change Management Guidance

- Any change to `Storage` API, callback semantics, MVCC contracts, or dynamic
  config side effects should update this guide in the same patch.
- Server and raftstore bridges should be audited whenever storage semantics or
  error mapping changes.
- If a change updates config values without updating `config_manager.rs` side
  effects, the change is usually incomplete.

## Change-Impact Matrix

- Public `Storage` API or callback changes:
  inspect `mod.rs`, `txn/scheduler.rs`, and RPC-facing callers in `src/server`
- Scheduler, latch, or task-flow changes:
  inspect `txn/scheduler.rs`, `txn/task.rs`, `txn/sched_pool.rs`, and metrics
- MVCC read/write or conflict changes:
  inspect `mvcc/*`, `txn/store.rs`, and transactional commands
- Raw KV changes:
  inspect `raw/*`, API-version handling, and coprocessor_v2 storage adapter
- Lock-wait or flow-control changes:
  inspect `lock_manager/*`, `txn/flow_controller/*`, and runtime workers in
  `src/server`
- Dynamic config changes:
  inspect `config.rs`, `config_manager.rs`, and runtime side-effect consumers

## Review Checklist

- Does the change affect `Storage` API behavior or callback contract?
- Does it touch `txn/scheduler.rs`, latches, or lock-wait queues?
- Does it modify MVCC conflict detection, lock resolution, or commit-ts rules?
- Does it alter raw KV behavior shared with plugin coprocessor or external APIs?
- Does it change snapshot or write assumptions that must still hold across both
  `raftkv` and `raftkv2` bridges?
- Does it require a config-manager side effect for dynamic config?
- Does it change resource-control integration, flow control, or quota-limiter
  behavior?

## Observability And Tests

- There is heavy inline unit-test coverage across MVCC, txn commands, latches,
  lock waiting, raw MVCC, and scheduler helpers.
- Integration tests often belong in `components/test_storage`.
- Hot-path metrics exist throughout scheduler, MVCC, read pools, and flow
  control.

## Common Failure Modes

- callback never invoked or invoked twice
- latch release bugs causing stuck or reordered commands
- MVCC write/read drift across CFs
- incorrect region error extraction from lower layers
- dynamic config updated in memory but not applied to runtime objects
- lock wait wake-up logic causing starvation or missed wake-ups

Shared lock upgrades keep the requesting transaction's member while waiting.
There is one pending upgrader per key. Removing another member can make that
upgrader the sole holder without unlocking the key. After the write succeeds,
the scheduler notifies the matching wait token while it still owns the key
latch. The client retries acquire and rechecks the holder state. When
admitting a shared read, upgrade, or wait on a shared wrapper, each key has a budget of
1,024 waiting requests and 16,384 waiter/holder pairs. Exceeding either
returns ServerIsBusy before a new wait or upgrade slot is registered.

## Reading Map And Companion Docs

Suggested reading order:

1. `mod.rs`
2. `config.rs`
3. `txn/mod.rs`
4. `txn/scheduler.rs`
5. `txn/commands/mod.rs`
6. `mvcc/mod.rs`
7. `mvcc/reader/reader.rs`
8. `txn/store.rs`
9. `lock_manager/mod.rs`
10. `config_manager.rs`

Companion docs:

- `repo-overview.md`
- `src/server.md`
- `components/raftstore.md`
- `PERFORMANCE_CRITICAL_PATH.md`

## Glossary

- MVCC:
  multi-version concurrency control over default/lock/write CFs
- Latch:
  scheduler-side serialization primitive for conflicting commands
- ProcessResult:
  scheduler-execution result returned back through callbacks
- Flow control:
  pressure-management logic that slows or gates write-heavy activity

## Related Components

- `src/server/service/kv.rs` is the main caller.
- `src/server/raftkv/mod.rs` bridges writes into raftstore.
- `src/server/lock_manager/*` owns the live lock-manager workers and deadlock
  service around the storage-side wait-queue contracts.
- `components/resource_control` and `resource_metering` integrate directly with
  scheduling and accounting.
