# `components/cdc` Maintenance Guide

## Purpose And Scope

`components/cdc` owns TiKV's change data capture service for TiCDC. It covers:

- gRPC event-feed connection handling
- region capture registration and deregistration
- apply-event observation from raftstore
- incremental scan initialization for new downstreams
- old-value lookup and cache management
- resolved-ts advancement coordination for captured regions
- connection watchdog and sink-memory backpressure behavior

`components/resolved_ts` is a companion subsystem, but CDC owns the
changefeed-facing state machine and delivery semantics.

## Architectural Views

### Event-feed view

1. `service.rs` accepts `ChangeData` event-feed streams and opens a `Conn`.
2. `Endpoint` owns region capture state, downstream registration, and
   background scan/tso runtimes.
3. `CdcObserver` registers on raftstore and forwards applied command batches
   plus role/region changes into the endpoint task queue.
4. `Initializer` acquires an incremental snapshot and installs the downstream
   into steady-state event delivery.
5. `Delegate` per region fans events and resolved-ts updates to downstreams.

### State-ownership view

- `Conn`: one gRPC connection and its request-to-downstream map
- `Delegate`: one captured region and its observed downstreams
- `Downstream`: one request's consumer state machine
- `ObserveId` / `DownstreamId`: ABA guards for region observation and sink
  ownership

## Process Lifecycle And Startup Sequencing

- Bootstrap wiring is in:
  - `components/server/src/server.rs` for classic `RaftKv`
  - `components/server/src/server2.rs` for `RaftKv2`
- Required startup order:
  1. create CDC worker and scheduler
  2. install `CdcTxnExtraScheduler` into the storage engine
  3. register `CdcObserver` on the coprocessor host
  4. register `CdcConfigManager`
  5. create and start `Endpoint`
  6. register the gRPC `ChangeData` service
- `Endpoint::new` creates:
  - a worker runtime for incremental scans
  - a TSO runtime for min-ts work
  - scan concurrency semaphore and speed limiters
  - old-value cache
  - leader resolver bootstrap for min-ts events

Shutdown rule:

- The event-feed service should stop accepting new connections before the
  endpoint and scan runtimes are torn down. Otherwise incremental-scan or sink
  tasks can race shutdown and produce misleading downstream errors.

## Data Model And Metadata Contracts

- `Task` in `endpoint.rs` is the central endpoint message contract.
- `Conn`, `RequestId`, and `ConnId` in `service.rs` define connection-side
  routing.
- `Downstream`, `DownstreamState`, and `DownstreamId` in `delegate.rs` define
  sink ownership and event-delivery readiness.
- `ObserveId` from raftstore is the region-observation identity. CDC depends on
  it to avoid ABA bugs when leadership or region ownership changes quickly.
- `OldValueCache` is a best-effort cache, but its size, miss behavior, and
  value encoding expectations are part of the maintenance surface.
- `TxnSource` bits in `txn_source.rs` are part of a cross-component contract.
  CDC deliberately treats Lightning physical-import writes specially; import
  changes that touch txn-source bits must be reviewed together with CDC.

## Start Here

- `components/cdc/src/lib.rs`
- `components/cdc/src/service.rs`
- `components/cdc/src/endpoint.rs`
- `components/cdc/src/delegate.rs`
- `components/cdc/src/initializer.rs`
- `components/cdc/src/observer.rs`
- `components/cdc/src/old_value.rs`
- `components/cdc/src/watchdog.rs`
- `components/cdc/src/txn_source.rs`
- `components/cdc/src/metrics.rs`

## Must-Read File Order

1. `components/cdc/src/service.rs`
2. `components/cdc/src/endpoint.rs`
3. `components/cdc/src/delegate.rs`
4. `components/cdc/src/initializer.rs`
5. `components/cdc/src/observer.rs`
6. `components/cdc/src/old_value.rs`
7. `components/cdc/src/watchdog.rs`
8. `components/cdc/src/txn_source.rs`
9. `components/cdc/src/metrics.rs`

## Core Concepts

- `Conn`: one event-feed stream from a client
- `Delegate`: one captured region's fanout and resolver state
- `Downstream`: one request on one connection
- Incremental scan:
  the snapshot catch-up phase before steady-state delta event delivery
- Old value:
  previous committed value lookup for TiCDC row-change semantics
- Watchdog:
  per-connection abort logic for idle or quota-stressed streams

## Why It Matters

CDC is easy to break with changes that appear local to storage, apply order, or
import. Failures here often show up as lag, duplicate events, missing old
values, or downstream termination rather than obvious local crashes.

## Critical Invariants

- Observer scheduling must stay FIFO enough to preserve event ordering
  expectations documented in `CdcObserver`.
- Incremental scan results must not overtake earlier delta events. The barrier
  flow in `Initializer::initialize` is a correctness boundary.
- `DownstreamState` transitions must stay monotonic:
  `Uninitialized -> Initializing -> Normal -> Stopped`.
- No more change events or resolved-ts updates may be sent after an error event
  has been emitted for a downstream.
- `ObserveId` and `DownstreamId` checks must stay in place to prevent stale
  deregistration from removing new owners.
- Old-value reads must observe a snapshot that is valid for the corresponding
  command batch.
- Sink memory quota and congestion handling are correctness-relevant because
  they drive explicit downstream errors and retry behavior.

## Observability And Operational Signals

Open `components/cdc/src/metrics.rs` first. The most useful signals are:

- `tikv_cdc_endpoint_pending_tasks`
- `tikv_cdc_grpc_connection_count`
- `tikv_cdc_scan_tasks`
- `tikv_cdc_scan_duration_seconds`
- `tikv_cdc_scan_bytes_total`
- `tikv_cdc_pending_bytes`
- `tikv_cdc_captured_region_total`
- `tikv_cdc_min_resolved_ts`
- `tikv_cdc_min_resolved_ts_lag`
- `tikv_cdc_old_value_cache_bytes`
- `tikv_cdc_old_value_duration`
- `tikv_cdc_sink_memory_bytes`
- `tikv_cdc_aborted_connections`

Useful logs are usually around:

- duplicate or invalid registration
- observer deregistration after leader/split/merge changes
- slow or canceled incremental scans
- sink congestion and memory-quota aborts
- watchdog-triggered connection aborts

## Change Management Guidance

- If a patch changes write-path ordering, old-value encoding, or txn-source
  bits, review CDC even if the patch is nominally in storage or import.
- If a patch changes scan concurrency or sink-memory behavior, audit both
  liveness and downstream-visible error semantics.
- If a patch changes region lifecycle or observe-handle behavior, audit split,
  merge, leader transfer, and not-leader cleanup paths.
- If a patch changes feature negotiation or request validation, review both old
  and new TiCDC client behavior.

## Change-Impact Matrix

- Connection or request-protocol changes:
  inspect `service.rs`, `Conn`, and feature-gate logic
- Region capture lifecycle changes:
  inspect `endpoint.rs`, `delegate.rs`, and `observer.rs`
- Incremental scan changes:
  inspect `initializer.rs`, scan concurrency, and barrier logic
- Old-value changes:
  inspect `old_value.rs`, MVCC readers, and apply-batch callbacks
- Import / txn-source changes:
  inspect `txn_source.rs` and `src/import/sst_service.rs`

## Review Checklist

- Does the patch change event ordering between incremental scan and delta flow?
- Does it change observe-handle registration or deregistration conditions?
- Does it change old-value lookup or cache semantics?
- Does it change how sink congestion or memory pressure is surfaced?
- Does it change txn-source handling for imported or replicated writes?
- Does it change resolved-ts advancement inputs or delegate lock tracking?

## Observability And Tests

- Integration tests live in `components/cdc/tests/integrations/`.
- Benchmarks live in `components/cdc/benches/cdc_event.rs`.
- Unit coverage is spread through `endpoint.rs`, `observer.rs`, `service.rs`,
  `delegate.rs`, `old_value.rs`, and `watchdog.rs`.
- CDC regressions often need multi-region or leadership-changing tests. Pure
  unit coverage is not enough for most ordering fixes.

## Common Failure Modes

- stale deregistration after fast leader changes or region replacement
- missing or duplicated events due to barrier/order regressions
- old-value lookup regressions under cache miss or GC-fence edge cases
- too many pending incremental scans causing immediate downstream failure
- sink congestion causing repeated aborted or stuck connections
- imported Lightning writes unexpectedly showing up in CDC streams

## Reading Map And Companion Docs

Suggested reading order:

1. `service.rs`
2. `endpoint.rs`
3. `delegate.rs`
4. `initializer.rs`
5. `observer.rs`

Companion docs:

- `repo-overview.md`
- `src/storage.md`
- `src/gc_worker.md`
- `components/raftstore.md`
- `components/raftstore-v2.md`
- `components/resolved_ts.md`
- `src/import.md`
- TiCDC overview:
  `https://docs.pingcap.com/tidb/stable/ticdc-overview/`

## Glossary

- Event feed:
  the long-lived gRPC stream used by TiCDC
- Delegate:
  the per-region CDC owner in TiKV
- Downstream:
  the per-request consumer attached to a delegate
- Changefeed checkpoint:
  the durable lower bound a TiCDC changefeed has fully replicated to its sink;
  it must lag or equal every region-level progress signal that still protects
  correctness
- Changefeed resolved-ts:
  the in-flight upper bound that says all commits below this timestamp are
  known and ordered for the captured span, but may not yet be durably flushed
  by the downstream sink
- Old value:
  the previously committed value used to construct row changes

## Related Components

- `components/resolved_ts`
- `src/gc_worker`
- `components/raftstore`
- `components/raftstore-v2`
- `src/storage`
- `src/import`
