# Repository Overview

This file is the top-level maintenance map for the TiKV repository. It is the
first file maintainers and reviewers should read before working on a
cross-component change.

## Purpose And Scope

- Describe the repository at the maintenance boundary level, not at the user
  feature level.
- Explain how major subsystems fit together and where ownership boundaries are.
- Highlight repository-wide invariants, sequencing rules, and failure modes.
- Serve as an index into the deeper subsystem guides in this directory.

## System Context And External Dependencies

TiKV is a distributed transactional key-value database that normally runs with:

- PD / Placement Driver:
  cluster metadata, timestamps, scheduling, resource-group metadata, labels
- TiDB:
  SQL layer and a major caller of transactional and coprocessor APIs
- RocksDB and raft-log engine:
  the main local persistence backends
- gRPC:
  request/response transport, raft transport, and administrative RPCs
- CDC, backup, backup-stream, import/restore tooling:
  data movement and external integration surfaces

Major local or in-repo infrastructure dependencies include:

- `components/engine_rocks`
- `components/raft_log_engine`
- `components/batch-system`
- `components/pd_client`
- `components/security`
- `components/sst_importer`
- `components/resource_control`

Operationally significant external dependencies that often affect maintenance
work:

- filesystem and disk layout under the configured data directory
- TLS / encryption material managed through `components/security` and
  `components/encryption`
- dynamic config propagation through PD-backed config registration and watches
- container or host-level CPU and memory quota reporting used by startup and
  throttling logic

## Core Components And Boundaries

### Process bootstrap

- `components/server` owns process startup, configuration validation, engine
  initialization, worker wiring, service registration, graceful pause/resume,
  and shutdown.

### Network and service edge

- `src/server` owns the runtime-facing boundary:
  gRPC services, Raft transport, snapshot transport, status HTTP server, debug
  endpoints, lock manager service, GC worker, and TTL helpers.

### Transaction and storage layer

- `src/storage` owns command scheduling, MVCC, raw KV, latches, lock waiting,
  flow control, and the `Storage` API used by RPC handlers.

### Raft replication and region state

- `components/raftstore` owns region replicas, raft messages, snapshot/apply
  lifecycle, split/merge handling, local read decisions, PD heartbeats, and the
  peer/store FSM structure.
- `components/batch-system` provides the generic polling and mailbox machinery
  that classic raftstore is built on.

### Alternate tablet-based replication path

- TiKV also contains a second replication path selected by
  `storage.engine = partitioned-raft-kv` / `EngineType::RaftKv2`.
- The bridge is `src/server/raftkv2`, the runtime bootstrap is
  `components/server/src/server2.rs`, and the replica implementation is
  `components/raftstore-v2`.
- This guide set includes a maintainer-map guide for `components/raftstore-v2`
  under `doc/maintenance-guides/components/raftstore-v2.md`, but maintainers
  still need to ask whether a server/storage fix is path-agnostic or requires a
  matching `RaftKv2` audit.

### Compute pushdown

- `src/coprocessor` is the classic TiDB coprocessor path for DAG, analyze, and
  checksum requests.
- `src/coprocessor_v2` is a plugin framework for raw coprocessor extensions.

### Resource governance and cache engines

- `components/resource_control` owns resource groups, fairness, quota limiters,
  PD metadata watches, and admission control helpers.
- `components/in_memory_engine` owns region-cache storage and background tasks.
- `components/hybrid_engine` bridges disk snapshots and the in-memory region
  cache for read acceleration.

### Data movement and ecosystem integrations

- `components/backup` and `components/backup-stream` own backup and log-backup
  surfaces.
- `components/cdc` owns changefeed capture and delivery.
- `components/resolved_ts` owns the shared progress engine used by CDC,
  backup-stream, and stale-read-style consumers.
- `src/import` and `components/sst_importer` own SST import and ingest flows.
- `src/server/gc_worker` owns MVCC GC, GC auto-scheduling, compaction-filter
  integration, and related safe-point enforcement.

Repository boundary rule:

- The top-level ownership lines above are the default review boundaries. If a
  change crosses one of them, the PR or review should call that out explicitly.

## Architectural Views

### Layered view

1. Process bootstrap and configuration
2. Network/service edge
3. Storage API and transactional scheduling
4. Replication and region state machines
5. Persistence engines and filesystem integration
6. Background services and external integrations

### Runtime ownership view

- Main startup and stop orchestration: `components/server`
- Long-lived RPC services: `src/server`
- Request scheduling and transactional correctness: `src/storage`
- Region/peer/store correctness: `components/raftstore`
- Disk and cache engines: engine crates plus `hybrid_engine` /
  `in_memory_engine`

### Deployment and dependency view

- TiKV node depends on PD for metadata and timestamps.
- TiKV participates in Raft groups across nodes.
- TiKV exposes gRPC and status HTTP surfaces.
- Optional subsystems such as CDC, backup, and import plug into the same node
  lifecycle.

## Process Lifecycle And Startup Sequencing

The high-level startup order is:

1. Validate config and process environment.
2. Initialize logging, filesystem state, lock files, reserved disk space, and
   panic markers.
3. Initialize engines and engine-related helpers.
4. Build workers, read pools, quota limiters, resource control, and
   coprocessor/runtime services.
5. Start raftstore and storage bridges.
6. Start gRPC server, status server, and other auxiliary services.
7. Enter the service-event loop for pause/resume/shutdown handling.

The high-level shutdown order is the reverse:

1. Stop admitting or pause/resume appropriately.
2. Drain or stop RPC-facing services.
3. Stop background workers and runtime helpers.
4. Stop storage and replication stacks after dependents are done.
5. Release process-owned resources.

Key maintainership rule:

- Startup and shutdown ordering are correctness concerns, not only operational
  details. Cross-thread and cross-worker ownership bugs often surface here.

Concrete startup anchors in code:

- process setup and config persistence:
  `components/server/src/common.rs`, `components/server/src/setup.rs`
- full classic startup orchestration:
  `components/server/src/server.rs`
- runtime gRPC server construction:
  `src/server/server.rs`
- classic raftstore bootstrap and store registration:
  `src/server/raft_server.rs`

## Data Model And Metadata Contracts

Repository-wide metadata and state contracts include:

- Region metadata:
  region id, epoch, peers, key range, leader/read state
- Raft metadata:
  term, log index, apply state, truncated state, snapshot state
- Transactional metadata:
  locks, writes, default-CF values, max-ts, causal-ts, lock wait state
- Snapshot metadata:
  sequence number, region bound, read ts, cache pin, safe point
- Resource governance metadata:
  resource-group definitions, request metadata, RU accounting
- Store metadata:
  store id, addresses, labels, advertised endpoints, deployment info

Cross-component rule:

- Any change to a persisted, network-exposed, or dynamically watched metadata
  contract should trigger review of the matching maintenance guide and usually a
  doc update in this directory.

Hot metadata classes that deserve explicit reviewer attention:

- `metapb::Store`, `metapb::Region`, and region epoch transitions
- `RaftCmdRequest` / `RaftCmdResponse` headers and region error semantics
- `kvrpcpb::Context` and derived request metadata such as resource-group tags
- storage API version and TTL expectations in `src/storage/config.rs`

## Write Path And Raft Pipeline

Classic write path:

1. `src/server/service/kv.rs` parses RPCs and builds storage calls.
2. `src/storage/mod.rs` and `src/storage/txn/scheduler.rs` turn requests into
   scheduled commands.
3. `src/server/raftkv/mod.rs` bridges storage writes to raftstore commands.
4. `components/raftstore/src/router.rs` and
   `components/raftstore/src/store/fsm/*` route work to the peer/store FSMs.
5. `components/raftstore/src/store/peer.rs` and related apply/write paths drive
   raft replication and persistence.

Alternate `RaftKv2` write path:

1. `src/server/service/kv.rs` still parses RPCs and calls storage.
2. `src/storage` still owns scheduling and transactional semantics.
3. `src/server/raftkv2/mod.rs` bridges writes into the tablet-based path.
4. `components/raftstore-v2` owns router, batch/FSM execution, and tablet-side
   persistence.

Review rule:

- If a change touches storage-to-replication bridging or callback semantics,
  explicitly audit both paths.

Hot files for this pipeline:

- `src/server/service/kv.rs`
- `src/storage/txn/scheduler.rs`
- `src/server/raftkv/mod.rs`
- `components/raftstore/src/store/peer.rs`
- `components/raftstore/src/store/fsm/apply.rs`

## Read Path And Coprocessor Execution

Classic KV read path:

1. `src/server/service/kv.rs` builds storage or coprocessor requests.
2. `src/storage` serves raw or MVCC reads, sometimes using `raftkv` snapshots.
3. `components/raftstore` may serve local reads if lease/read-index rules hold.
4. `components/hybrid_engine` and `components/in_memory_engine` can satisfy a
   region-scoped snapshot from the cache engine when available.

Coprocessor path:

1. `src/server/service/kv.rs` forwards coprocessor RPCs.
2. `src/coprocessor/endpoint.rs` or `src/coprocessor_v2/endpoint.rs` parses
   and dispatches them.
3. `src/storage` provides snapshots and raw access.
4. Classic coprocessor runs built-in executors; v2 calls dynamically loaded
   plugins.

## Foreground And Background IO, And Determinism

TiKV deliberately separates foreground request work from background work where
possible.

- Foreground examples:
  RPC handling, scheduler execution, raft pollers, local reads, coprocessor
  execution
- Background examples:
  PD heartbeats, GC tasks, snapshot generation, compaction-related work, cache
  loading/eviction, backup, CDC, metrics flushing

Important rule:

- Avoid introducing unnecessary synchronous IO on request hot paths or FSM
  pollers.
- Preserve determinism around callback ordering, message ordering, and snapshot
  semantics even when background offload changes.

Code anchors for IO classification and separation:

- foreground read pools:
  `src/coprocessor/readpool_impl.rs`, `src/read_pool.rs`
- filesystem IO typing and rate limiting:
  `file_system`, `src/storage/config_manager.rs`
- raftstore worker separation:
  `components/raftstore/src/store/worker/*`

## Snapshot, Split/Merge, And Restore Semantics

- Snapshot correctness depends on region bounds, sequence number, apply state,
  and sometimes cache pinning or safe point.
- Split and merge correctness depends on epoch transitions, metadata updates,
  and message ordering across peer/store/apply workers.
- Restore/import flows interact with snapshots and raftstore because ingest must
  align with region ownership and persistent state.

These are cross-component flows spanning:

- `components/raftstore`
- `src/server/raftkv*`
- `src/storage`
- `src/import`
- `components/sst_importer`

Maintainer warning:

- Bugs in these flows often present far away from the original code change:
  stale epoch errors, missing callbacks, duplicate or lost data, or restore
  failures.

## Engines For Raft And RocksDB Deep Dive

Two key persistence families matter repository-wide:

- RocksDB-backed KV engine via `components/engine_rocks`
- Raft log engine via `components/raft_log_engine`

Maintainers should remember:

- engine configuration, filesystem integration, background errors, compaction,
  and snapshot behavior are not isolated implementation details; they influence
  correctness and operability across server, storage, and raftstore.
- `src/storage/config.rs` chooses between `RaftKv` and `RaftKv2` stack variants.
- `components/server/src/common.rs` and startup code wire engine lifecycle and
  resources.

Useful engine-specific files:

- `components/engine_rocks/src/lib.rs`
- `components/engine_rocks/src/event_listener.rs`
- `components/engine_rocks/src/flow_listener.rs`
- `components/engine_rocks/src/misc.rs`
- `components/raft_log_engine/src/lib.rs`

## Backup And Restore

- `components/backup` owns backup request processing and writing backup output.
- `components/backup-stream` owns log backup / continuous backup surfaces.
- `src/import` and `components/sst_importer` own ingest/import/restore-adjacent
  SST flows.

Maintenance rule:

- Changes in snapshot, ingest, region ownership, or engine write semantics
  should be audited for backup/import/restore impact.

Relevant code anchors:

- `components/backup/src/lib.rs`
- `components/backup-stream/src/lib.rs`
- `src/import/mod.rs`
- `components/sst_importer/src/lib.rs`

## CDC Architecture

- `components/cdc` owns change data capture endpoint, observer integration, and
  service logic.
- CDC depends on raft/apply timing, old-value handling, and transactional
  metadata correctness.

Maintenance rule:

- Write-path, old-value, apply-ordering, or resolved-ts changes can affect CDC
  even if the immediate feature change looks unrelated.

Relevant code anchors:

- `components/cdc/src/lib.rs`
- `components/cdc/src/observer.rs`
- `components/cdc/src/endpoint.rs`

## Cross-Component Invariants And Ordering Rules

- Region-scoped operations must respect epoch, key range, and leader/read-index
  constraints.
- Snapshot-backed reads must not outlive the underlying safety assumptions:
  sequence number, safe point, snapshot pinning, or region cache lifetime.
- Write-path changes must preserve callback behavior:
  success, region error, timeout, undetermined result, and cancellation.
- Dynamic config changes must update both config state and runtime side effects.
- Metrics and logging on hot paths must remain cheap.
- Background work must not violate ordering guarantees expected by foreground
  logic.

## Failure Modes And Recovery Playbook

Common repository-wide failure modes:

- cross-thread ownership and shutdown ordering bugs
- callback completion rules broken on error or cancellation paths
- range validation bugs around region snapshots and raw scans
- local read served under invalid lease or stale progress
- runtime config updated in memory but not applied operationally
- resource-control or fairness logic causing unintended latency shifts
- engine background errors or disk-space issues propagating badly

Recovery / triage guidance:

1. Identify the boundary layer where the wrong behavior first became visible.
2. Check metrics, logs, and status-server endpoints before editing code.
3. Verify whether the failure is path-specific:
   classic raftstore, `RaftKv2`, coprocessor, raw KV, CDC, backup, import.
4. Re-check callback ordering, snapshot assumptions, and dynamic config side
   effects.
5. Re-check whether the current maintenance guide is stale relative to the code
   you are debugging.

## Observability And Operational Signals

Important observability surfaces include:

- Prometheus metrics across server, storage, raftstore, engines, resource
  control, CDC, backup
- slow logs and regular logs
- status server endpoints
- health controller / gRPC health
- memory and thread-load tracking
- engine and filesystem statistics

Operationally important subsystems include:

- `src/server/status_server`
- `src/server/metrics.rs`
- raftstore metrics and local metrics
- coprocessor trackers and wait-time metrics
- storage scheduler and MVCC metrics

Concrete metric modules worth opening first:

- `components/raftstore/src/store/metrics.rs`
- `components/raftstore/src/store/local_metrics.rs`
- `src/storage/metrics.rs`
- `src/coprocessor/metrics.rs`
- `src/server/metrics.rs`
- `components/resource_control/src/metrics.rs`
- `components/in_memory_engine/src/metrics.rs`

## Change Management Guidance

- Treat this file and the matching subsystem guide as required context for any
  non-trivial change.
- If a change modifies:
  startup order, ownership boundaries, metadata contracts, path-specific
  behavior, invariants, observability, or reading maps, update the relevant
  guide in the same change.
- If a fix touches storage/server boundaries, explicitly evaluate whether
  `RaftKv2`, CDC, backup/import, or coprocessor paths also need attention.
- If the guide is missing facts needed for a safe review, the guide itself
  should be improved as part of the maintenance work.

## Change-Impact Matrix

- Startup, shutdown, or process ownership changes:
  read `components/server`, `src/server`, worker lifecycle, and service control
  guides together
- RPC or request-boundary changes:
  read `src/server`, `src/storage`, `src/coprocessor`, and resource-control
  guides together
- Transaction, MVCC, callback, or lock-wait changes:
  read `src/storage` first, then `src/server` and replication bridge guides
- Replica, snapshot, split/merge, local-read, or apply-path changes:
  read `components/raftstore`, `components/batch-system`,
  `src/server/raftkv`-related code, and audit whether the same reasoning also
  applies to `src/server/raftkv2` / `components/raftstore-v2`
- Cache-engine or hybrid snapshot changes:
  read `components/in_memory_engine`, `components/hybrid_engine`,
  `src/coprocessor`, and replication observer wiring together
- Fairness, admission, or RU-accounting changes:
  read `components/resource_control`, `src/server/service/kv.rs`,
  `src/storage`, and `components/batch-system`
- Import, backup, restore, or CDC changes:
  audit `src/import`, `components/sst_importer`, `components/backup`,
  `components/backup-stream`, and `components/cdc` plus the affected storage or
  raftstore contracts

## Must-Read File Order

Use this faster file order when the goal is review or change-impact analysis
rather than general onboarding:

1. `src/server/service/kv.rs`
2. `src/storage/mod.rs`
3. `src/storage/txn/scheduler.rs`
4. `src/server/raftkv/mod.rs`
5. `components/raftstore/src/router.rs`
6. `components/raftstore/src/store/peer.rs`
7. `components/raftstore/src/store/fsm/apply.rs`
8. `src/coprocessor/endpoint.rs`
9. `components/resource_control/src/lib.rs`
10. `components/server/src/server.rs`

If the change primarily targets `EngineType::RaftKv2`, swap in these parallel
anchors early:

- `src/server/raftkv2/mod.rs`
- `components/raftstore-v2/src/router/mod.rs`
- `components/raftstore-v2/src/fsm/mod.rs`
- `components/raftstore-v2/src/raft/mod.rs`
- `components/raftstore-v2/src/operation/mod.rs`

## Reading Map And Companion Docs

Suggested reading order for new maintainers:

1. `components/server/src/common.rs`
2. `components/server/src/server.rs`
3. `src/server/mod.rs`
4. `src/server/service/kv.rs`
5. `src/storage/mod.rs`
6. `src/storage/txn/scheduler.rs`
7. `src/server/raftkv/mod.rs`
8. `components/raftstore/src/router.rs`
9. `components/raftstore/src/store/mod.rs`
10. `components/raftstore/src/store/fsm/mod.rs`
11. `src/coprocessor/mod.rs`
12. `components/resource_control/src/lib.rs`
13. the matching deeper guide in `doc/maintenance-guides/`

If the work is on the `RaftKv2` / tablet-based stack, also read:

- `src/server/raftkv2/mod.rs`
- `components/raftstore-v2/src/lib.rs`
- `components/raftstore-v2/src/router/mod.rs`
- `components/raftstore-v2/src/fsm/mod.rs`

Useful companion docs in this repo:

- `README.md`
- `PERFORMANCE_CRITICAL_PATH.md`
- `CODE_COMMENT_STYLE.md`
- `doc/http.md`
- `doc/deploy.md`

Useful external references:

- TiKV overview: `https://tikv.org/docs/latest/concepts/overview/`
- Deep Dive TiKV: `https://tikv.org/deep-dive/introduction/`
- TiDB / TiKV overview: `https://docs.pingcap.com/tidb/stable/tikv-overview`
- TiDB resource control overview:
  `https://docs.pingcap.com/tidb/stable/tidb-resource-control-overview`

## Glossary

- Region:
  the key-range shard and replication unit in TiKV
- Peer:
  one replica of a region on one store
- Store:
  a TiKV node-local storage service identity
- FSM:
  finite state machine used in poll-loop execution
- Local read:
  serving a read without full raft round-trip when invariants allow it
- Read index:
  raft-based read freshness mechanism
- MVCC:
  multi-version concurrency control
- Snapshot:
  a bounded, point-in-time read view
- Safe point:
  GC boundary before which old versions may be removed
- RU:
  request unit used by resource control
- CDC:
  change data capture
