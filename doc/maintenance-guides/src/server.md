# `src/server` Maintenance Guide

## Purpose And Scope

`src/server` is the runtime-facing server stack. It owns the network edge and
service plumbing after process startup:

- gRPC request handling
- raft message transport
- snapshot transport
- status server and diagnostics
- GC worker
- lock-manager service
- debug and diagnostics services
- TTL helper services

## Architectural Views

### Runtime edge view

- gRPC service edge
- storage and coprocessor dispatch
- raft transport and snapshot transport
- status and diagnostics HTTP surface

### Service decomposition view

- KV service
- debug/diagnostics
- GC worker
- lock manager
- status server
- raft server/bootstrap bridge

## Process Lifecycle And Startup Sequencing

- This module is started by `components/server` after engines and major workers
  exist.
- Service construction order matters:
  transport, storage bridge, coprocessor endpoints, health service, status
  server, and runtime pools all depend on prior setup.
- Shutdown order must preserve callback and scheduler safety for in-flight
  requests.

Concrete runtime anchors:

- gRPC server construction:
  `server.rs`
- store bootstrap and raftstore start:
  `raft_server.rs`
- status HTTP surface:
  `status_server/mod.rs`
- primary RPC implementation:
  `service/kv.rs`

## Data Model And Metadata Contracts

- gRPC metadata such as store id and resource-group context
- server config contract for concurrency, quotas, transport, and snapshot
  settings
- region/store/cluster identity checks at service boundaries

High-risk service contracts:

- metadata-derived store-id checks
- request batching and stream callback completion
- region-error and timeout mapping from storage/raftstore to RPCs

## Start Here

- `src/server/mod.rs`
- `src/server/service/mod.rs`
- `src/server/service/kv.rs`
- `src/server/server.rs`
- `src/server/debug2.rs`
- `src/server/raft_server.rs`
- `src/server/raftkv/mod.rs`
- `src/server/raftkv2/mod.rs`
- `src/server/status_server/mod.rs`
- `src/server/gc_worker/*`
- `src/server/config.rs`

## Must-Read File Order

1. `src/server/config.rs`
2. `src/server/service/mod.rs`
3. `src/server/service/kv.rs`
4. `src/server/server.rs`
5. `src/server/raftkv/mod.rs`
6. `src/server/raftkv2/mod.rs`
7. `src/server/raft_server.rs`
8. `src/server/status_server/mod.rs`
9. `src/server/gc_worker/mod.rs`

## Main Subsystems

### RPC edge

- `service/kv.rs` is the most important file in this module.
- It maps gRPC methods to storage, coprocessor, GC, and raft paths.
- It is hot-path code and should be reviewed with latency and allocation in
  mind.

### Server runtime

- `server.rs` builds and owns the gRPC server runtime and related pools.
- It wires the request service, snapshot worker, health service, and transport.

### Raft transport and storage bridge

- `raft_server.rs` owns `MultiRaftServer`, store bootstrap, and raftstore
  runtime startup.
- `raftkv/mod.rs` is the bridge between storage and raftstore.
- `raftkv2/mod.rs` is the bridge between storage and `raftstore-v2`.
- `debug2.rs` is the matching debugger surface for the `RaftKv2` path.

### Status and diagnostics

- `status_server/mod.rs` exposes HTTP endpoints for config, metrics, profiles,
  resource-manager state, pause/resume, and debugging.

### GC and lock manager

- `gc_worker/*` implements MVCC garbage collection and compaction filter logic.
- `lock_manager/*` owns the live waiter-manager workers, deadlock detector, and
  RPC-facing lock-manager runtime.
- This module sits on top of the storage-side wait-queue contracts in
  `src/storage/lock_manager/*`.

## Critical Invariants

- Request methods must preserve exact callback and error mapping semantics.
- Store id / cluster id / region error checks must stay strict at service edges.
- Raft message rejection logic must stay compatible with memory-pressure policy.
- Engine-specific behavior must stay aligned when a feature spans both
  `raftkv` and `raftkv2`.
- Status-server actions must match the real runtime control plane.
- GC worker changes must preserve MVCC and safe-point correctness.

## Observability And Operational Signals

- gRPC service metrics and request-duration tracking
- raft transport rejection and memory-pressure signals
- status-server endpoints for config, metrics, health, and profiles
- GC and diagnostics metrics and logs

Start triage with:

- `src/server/metrics.rs`
- `src/server/status_server/mod.rs`
- request logs and slow-path logs in `service/kv.rs`
- health-controller state

## Change Management Guidance

- If request dispatch, transport semantics, status-server controls, or bootstrap
  behavior changes, update this guide in the same patch.
- Storage and raftstore boundary changes should be reviewed together with this
  module.
- If a new RPC method or control endpoint is added, document who owns the
  contract and which lower layer is authoritative for errors and callbacks.

## Change-Impact Matrix

- RPC method or callback changes:
  inspect `service/kv.rs`, storage callers, and response/error mapping
- Raft bridge changes:
  inspect `raftkv/mod.rs`, `raftkv2/mod.rs`, and `raft_server.rs`
- Status or control-plane changes:
  inspect `status_server/mod.rs`, service manager/control events, and security
  posture
- GC or lock-manager changes:
  inspect `gc_worker/*`, `lock_manager/*`, and storage-side contracts

## Review Checklist

- Does the change touch `service/kv.rs` or `raftkv/mod.rs`?
- Does it also need a matching `raftkv2/mod.rs` or `debug2.rs` change for the
  `RaftKv2` path?
- Does it change request batching, stream behavior, or backpressure?
- Does it alter memory-pressure rejection or raft append filtering?
- Does it change bootstrap or store-registration ordering in `raft_server.rs`?
- Does it add new status-server behavior without security or readiness review?
- Does it alter dynamic config behavior in `config.rs` or config managers?

## Observability And Tests

- Inline tests exist in `service/kv.rs`, `gc_worker/*`, `status_server/*`, and
  diagnostics modules.
- Metrics are extensive and often the fastest signal for regressions.
- Many runtime issues require end-to-end validation with a running node.

## Common Failure Modes

- incorrect gRPC to storage error mapping
- partial stream or callback completion on errors
- startup-time bootstrap edge cases
- status-server control operations not reaching real runtime owners
- GC compaction behavior drifting from MVCC rules

## Reading Map And Companion Docs

Suggested reading order:

1. `mod.rs`
2. `config.rs`
3. `service/mod.rs`
4. `service/kv.rs`
5. `server.rs`
6. `raftkv/mod.rs`
7. `raftkv2/mod.rs`
8. `raft_server.rs`
9. `status_server/mod.rs`
10. `gc_worker/mod.rs`

Companion docs:

- `repo-overview.md`
- `components/server.md`
- `src/storage.md`

## Glossary

- KvService:
  primary gRPC service implementation for TiKV RPCs
- Status server:
  HTTP surface for metrics, config, debug, and control actions
- RaftKv:
  storage bridge from transactional API into classic raftstore
- Health controller:
  shared readiness/health state exposed through gRPC and runtime services

## Related Components

- `components/server` wires this module into the process lifecycle.
- `src/storage` is the main backend for most RPC methods.
- `components/raftstore` is the backend for raft and region-level operations.
