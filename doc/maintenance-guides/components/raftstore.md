# `components/raftstore` Maintenance Guide

## Purpose And Scope

`raftstore` owns the classic raft-kv region replica state machines and most
region-level distributed systems behavior on that engine path:

- routing raft and admin requests
- peer/store FSM execution
- local read decisions
- snapshot generation and application
- split/merge and metadata transitions
- PD heartbeat and region/store statistics
- raft log storage and peer storage state

This crate is one of the highest-risk maintenance surfaces in TiKV.

## Architectural Views

### Responsibility view

- region replica state machines
- peer/store/apply coordination
- local-read and read-index policy
- snapshot generation/application
- split/merge and region metadata transitions
- PD-facing region/store reporting

### Runtime view

- outer layers route requests here through router and transport traits
- batch/FSM execution drives peer and store polling
- worker subsystems offload PD, snapshot, split-check, read, and cleanup tasks

## Process Lifecycle And Startup Sequencing

- Startup ownership comes from `components/server` and `src/server/raft_server`.
- During startup, routers, batch systems, workers, transport, coprocessor host,
  and metadata delegates are wired before the node begins serving traffic.
- During shutdown, pollers and workers must drain in an order that preserves
  callback completion and avoids orphaning peer/store state.

Concrete startup anchors:

- batch-system creation:
  `store/fsm/mod.rs`
- store bootstrap and runtime start:
  `src/server/raft_server.rs`
- server-side routing bridge:
  `router.rs`, `src/server/raftkv/mod.rs`

## Data Model And Metadata Contracts

- Region metadata:
  id, epoch, peers, bounds, merge/split state
- Raft metadata:
  term, index, hard/soft state, truncated state, apply state
- Snapshot metadata:
  key, apply state, state transitions, snapshot file set
- Read metadata:
  lease/read progress, read-index contexts, local reader delegates

High-risk contracts:

- `RaftCmdRequest` header validation against region epoch and peer identity
- persisted apply/raft state alignment in `peer_storage.rs`
- callback/result semantics in `store/msg.rs` and `store/fsm/apply.rs`

## Start Here

- `components/raftstore/src/lib.rs`
- `components/raftstore/src/router.rs`
- `components/raftstore/src/store/mod.rs`
- `components/raftstore/src/store/fsm/mod.rs`
- `components/raftstore/src/store/fsm/apply.rs`
- `components/raftstore/src/store/peer.rs`
- `components/raftstore/src/store/peer_storage.rs`
- `components/raftstore/src/store/msg.rs`
- `components/raftstore/src/store/worker/mod.rs`

## Must-Read File Order

1. `components/raftstore/src/router.rs`
2. `components/raftstore/src/store/msg.rs`
3. `components/raftstore/src/store/fsm/store.rs`
4. `components/raftstore/src/store/fsm/apply.rs`
5. `components/raftstore/src/store/peer.rs`
6. `components/raftstore/src/store/peer_storage.rs`
7. `components/raftstore/src/store/worker/read.rs`
8. `components/raftstore/src/store/worker/pd.rs`

## Internal Structure

### Routing and outer interfaces

- `router.rs` defines the router traits and the server-facing router wrapper.
- `store/msg.rs` is the central message taxonomy:
  `PeerMsg`, `StoreMsg`, `SignificantMsg`, callbacks, ticks, and request
  wrappers.
- `store/transport.rs` defines the internal transport traits.

### Core FSM execution

- `store/fsm/peer.rs` and `store/fsm/store.rs` own the poll-loop behavior for
  peer FSMs and the store FSM.
- `store/fsm/apply.rs` owns committed-log apply, callback completion, and
  apply-state persistence.
- `store/peer.rs` is the core replica state machine and policy engine.
- `store/hibernate_state.rs` manages hibernation and wake-up behavior.

### Persistent region/raft state

- `store/peer_storage.rs` owns apply/raft state persistence and snapshot state.
- `store/entry_storage.rs` owns raft log entry storage and fetch behavior.
- `store/region_meta.rs` models persisted region and raft metadata.

### Reads, snapshots, and async IO

- `store/read_queue.rs` owns read-index queueing and batching.
- `store/region_snapshot.rs` is the region-scoped snapshot view.
- `store/snap/*` owns snapshot files and application helpers.
- `store/async_io/*` separates read/write background IO from FSM execution.

### Worker sub-systems

- `store/worker/pd.rs`: store and region stats, PD heartbeats
- `store/worker/split_check.rs`: split detection and buckets
- `store/worker/snap_gen.rs`: snapshot generation
- `store/worker/read.rs`: local reader and read delegates
- `store/worker/refresh_config.rs`: runtime config propagation

### Coprocessor hooks

- `coprocessor/dispatcher.rs` hosts raftstore observers.
- `coprocessor/region_info_accessor.rs` exposes region metadata for other
  subsystems.

## Critical Invariants

- Region epoch checks must remain strict. Most stale command and split/merge
  safety depends on this.
- A `Peer` must preserve role, applied index, raft log, and lease/read-progress
  consistency across ticks and messages.
- Snapshot lifecycle must not leak:
  generation, transport, application, and cleanup are all coupled.
- Local reads must only bypass raft when lease and read-progress guarantees are
  valid.
- FSM messages must preserve ordering assumptions between peer/store/apply
  workers.
- Any write-path change must preserve callback completion and region-error
  semantics.

## Observability And Operational Signals

- metrics under `store/metrics.rs`, `store/local_metrics.rs`, and worker metrics
- PD heartbeat and region/store statistics
- logs around snapshot, split/merge, peer lifecycle, disk-full, and unsafe
  recovery paths
- memory accounting for raft entries/messages/apply state

Open these first when triaging:

- `store/metrics.rs`
- `store/local_metrics.rs`
- `store/worker/pd.rs`
- `store/worker/read.rs`

## Change Management Guidance

- Any change to message types, peer/store/apply ordering, snapshot lifecycle,
  split/merge metadata, or local-read policy should update this guide.
- Changes in server/storage bridges should also be audited against this crate,
  and vice versa.
- Changes to config in `store/config.rs` or runtime reconfiguration in
  `worker/refresh_config.rs` should update both contract and operational notes
  here.

## Change-Impact Matrix

- Router/message taxonomy changes:
  inspect `router.rs`, `store/msg.rs`, `store/transport.rs`, and
  `src/server/raftkv/mod.rs`
- Peer state-machine or local-read changes:
  inspect `store/peer.rs`, `store/read_queue.rs`, `store/worker/read.rs`, and
  `src/server/service/kv.rs`
- Apply-path or callback changes:
  inspect `store/fsm/apply.rs`, `peer_storage.rs`, `store/msg.rs`, and
  `src/storage`
- Snapshot or peer-storage changes:
  inspect `peer_storage.rs`, `store/snap/*`, `store/worker/snap_gen.rs`, and
  restore/import call sites
- Split/merge or region-epoch changes:
  inspect `peer.rs`, split-check workers, PD heartbeat workers, and region
  metadata call sites
- Config or scheduling changes:
  inspect `store/config.rs`, `store/worker/refresh_config.rs`, and
  `components/batch-system`

## Review Checklist

- Does the change alter `PeerMsg`, `StoreMsg`, tick ordering, or rescheduling?
- Does it touch `peer.rs`, `peer_storage.rs`, `entry_storage.rs`, or
  `fsm/*`? Treat as correctness-sensitive by default.
- Does it affect read-index, lease renewals, or local reader delegation?
- Does it change split/merge, unsafe recovery, or replication-mode behavior?
- Does the same fix also need to exist in `components/raftstore-v2` for
  `EngineType::RaftKv2`?
- Does it move synchronous work onto a poller thread or add logging in the hot
  path?
- Does it create a new snapshot, raft log, or disk IO path without updating
  cleanup and metrics?

## Observability And Test Surfaces

- Metrics are concentrated under `store/metrics.rs`, `store/local_metrics.rs`,
  and worker metrics modules.
- Integration coverage usually lives outside this crate:
  `components/test_raftstore` is the first place to extend.
- There are also many inline tests in:
  `peer.rs`, `peer_storage.rs`, `entry_storage.rs`, `read_queue.rs`,
  `fsm/apply.rs`, `compaction_guard.rs`, `region_info_accessor.rs`, and others.

## Common Failure Modes

- stale epoch acceptance
- read served under invalid lease or progress
- missing callback completion on canceled or rerouted work
- snapshot state machine stuck in a transient state
- peer metadata drift after split, merge, or destroy
- extra CPU or blocking IO on the FSM hot path

## Reading Map And Companion Docs

Suggested reading order:

1. `router.rs`
2. `store/msg.rs`
3. `store/fsm/mod.rs`
4. `store/fsm/apply.rs`
5. `store/peer.rs`
6. `store/peer_storage.rs`
7. `store/read_queue.rs`
8. `store/worker/read.rs`
9. `store/worker/pd.rs`
10. `coprocessor/dispatcher.rs`

Companion docs:

- `repo-overview.md`
- `src/server.md`
- `src/storage.md`
- `PERFORMANCE_CRITICAL_PATH.md`
- `components/batch-system.md`

## Glossary

- Peer:
  one region replica state machine on one store
- Store FSM:
  the node-level FSM coordinating cross-region tasks
- Apply:
  the phase that applies committed raft entries to state
- Local read:
  read served without full raft round-trip when lease/progress allow
- Read index:
  raft freshness mechanism for reads
- Hibernate:
  reduced-activity peer/group mode to cut unnecessary polling

## Related Components

- `components/batch-system` supplies the FSM execution model.
- `src/server/raftkv` is the storage bridge into raftstore.
- `components/hybrid_engine` and `components/in_memory_engine` integrate via
  observers and region metadata.
