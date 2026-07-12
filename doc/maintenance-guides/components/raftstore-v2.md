# `components/raftstore-v2` Maintenance Guide

## Purpose And Scope

`raftstore-v2` is the tablet-based replica stack used by
`EngineType::RaftKv2`. It owns router message delivery, batch/FSM execution,
tablet-backed raft state, operation modules, and PD/tablet background workers.

It is not just a small variant of classic `components/raftstore`. Reviewers
should treat it as a separate maintenance surface with its own read/write/apply
paths and its own failure modes.

## Architectural Views

### Stack view

- router and response channels
- batch/FSM execution
- raft peer/storage layer
- operation modules for query/write/split/merge
- PD and tablet background workers

### Boundary view

- `src/server/raftkv2` is the storage bridge into this crate
- `components/server/src/server2.rs` wires this stack into process startup
- `src/storage/config.rs` selects this path via `EngineType::RaftKv2`

## Process Lifecycle And Startup Sequencing

- This crate is started from the `server2.rs` path after engines, PD client,
  and major runtime helpers exist.
- Router, store system, and worker construction must stay consistent with
  `src/server/raftkv2/mod.rs` expectations.
- Shutdown order matters because peer/store/apply execution, tablet tasks, and
  response channels all depend on ownership being drained in the right order.

Concrete runtime anchors:

- startup and wiring:
  `components/server/src/server2.rs`
- bridge entry:
  `src/server/raftkv2/mod.rs`
- crate entry:
  `components/raftstore-v2/src/lib.rs`

## Data Model And Metadata Contracts

- router message and response-channel contracts
- peer/store/apply FSM ownership boundaries
- tablet-backed raft storage and apply state
- operation-layer request/response semantics

High-risk contracts:

- callback completion and response-stream behavior back to `raftkv2`
- peer/apply ordering across FSM boundaries
- tablet state staying aligned with region/raft metadata
- split/merge/bootstrap/destroy flows keeping router and tablet state coherent

## Start Here

- `components/raftstore-v2/src/lib.rs`
- `components/raftstore-v2/src/router/mod.rs`
- `components/raftstore-v2/src/batch/mod.rs`
- `components/raftstore-v2/src/fsm/mod.rs`
- `components/raftstore-v2/src/raft/mod.rs`
- `components/raftstore-v2/src/operation/mod.rs`
- `components/raftstore-v2/src/worker/pd/mod.rs`
- `components/raftstore-v2/src/worker/tablet.rs`

## Must-Read File Order

1. `components/raftstore-v2/src/lib.rs`
2. `components/raftstore-v2/src/router/mod.rs`
3. `components/raftstore-v2/src/batch/mod.rs`
4. `components/raftstore-v2/src/fsm/mod.rs`
5. `components/raftstore-v2/src/raft/mod.rs`
6. `components/raftstore-v2/src/operation/mod.rs`
7. `components/raftstore-v2/src/worker/pd/mod.rs`
8. `components/raftstore-v2/src/worker/tablet.rs`
9. `src/server/raftkv2/mod.rs`
10. `components/server/src/server2.rs`

## Internal Structure

### Router and response layer

- `router/*` defines message types, response channels, and router
  implementation.
- This is the outer interface consumed by `src/server/raftkv2`.

### Batch and FSM execution

- `batch/*` provides the specialized store batch system.
- `fsm/*` defines store, peer, and apply FSMs.
- Message ordering and ownership transfer are core correctness constraints here.

### Raft and operations

- `raft/*` contains peer/storage logic.
- `operation/*` implements query, write, split/merge, bootstrap, and related
  behavior on top of the FSM/raft layers.

### Background workers

- `worker/pd/*` owns PD-facing reporting and control paths.
- `worker/tablet.rs` owns tablet-specific background tasks.
- `worker/refresh_config.rs` carries runtime config changes.

## Critical Invariants

- Router message types and response channels must preserve the callback and
  streaming semantics expected by `src/server/raftkv2`.
- Peer/store/apply FSM ownership must remain single-owner and ordering-safe.
- Read paths must not silently weaken local-read, read-index, snapshot, or
  apply guarantees.
- Tablet-backed state transitions must remain aligned with region/raft
  metadata.
- Split/merge/bootstrap/destroy flows must keep router, FSM, and tablet state
  mutually consistent.

## Observability And Operational Signals

- PD-worker logs and metrics
- response-channel and router failures
- tablet-task and refresh-config behavior
- any divergence surfaced at the `src/server/raftkv2` bridge

Start triage with:

- `components/raftstore-v2/src/router/*`
- `components/raftstore-v2/src/fsm/*`
- `components/raftstore-v2/src/worker/pd/*`
- `src/server/raftkv2/mod.rs`

## Change Management Guidance

- If a change alters callback timing, router semantics, startup/shutdown
  ordering, tablet-state assumptions, or PD/tablet worker behavior, update this
  guide in the same patch.
- When reviewing fixes in classic `raftstore`, explicitly check whether the
  same issue also exists here.
- Do not assume parity with classic `raftstore`; verify it in code.

## Change-Impact Matrix

- Router or callback changes:
  inspect `router/*`, `src/server/raftkv2/mod.rs`, and response channels
- FSM ordering or apply-path changes:
  inspect `fsm/*`, `raft/*`, and relevant operation modules
- Read/write/query-path changes:
  inspect `operation/*`, `raft/*`, and server bridge behavior
- Tablet lifecycle or config changes:
  inspect `worker/tablet.rs`, `worker/refresh_config.rs`, and startup wiring
- PD/reporting changes:
  inspect `worker/pd/*` and external-facing metrics/logs

## Review Checklist

- Does the change touch router messages, response channels, or bridge
  assumptions from `src/server/raftkv2`?
- Does it modify `fsm/*`, `raft/*`, or `operation/*` in a way that changes
  callback timing or ordering?
- Does the same fix also need to exist in classic `components/raftstore`, or
  vice versa?
- Does it change tablet lifecycle, PD reporting, or refresh-config behavior
  without updating the corresponding worker path?
- Does it add synchronous work, logging, or allocation pressure on a poller or
  hot read/write path?

## Observability And Tests

- Primary local integration coverage lives in
  `components/raftstore-v2/tests/integrations/*`
- Failpoint coverage lives in
  `components/raftstore-v2/tests/failpoints/*`
- Cluster harness support lives in
  `components/test_raftstore-v2`

## Common Failure Modes

- fix applied only to classic `raftstore`, leaving `raftstore-v2` divergent
- router/response behavior drifting from `raftkv2` expectations
- peer/apply lifecycle bugs surfacing as missing callback completion
- tablet/bootstrap state out of sync with region metadata
- PD worker logic lagging behind split/slowness/runtime-config changes

## Reading Map And Companion Docs

Suggested reading order:

1. `lib.rs`
2. `router/mod.rs`
3. `batch/mod.rs`
4. `fsm/mod.rs`
5. `raft/mod.rs`
6. `operation/mod.rs`
7. `worker/pd/mod.rs`
8. `worker/tablet.rs`

Companion docs:

- `repo-overview.md`
- `src/server.md`
- `src/storage.md`
- `components/server.md`

## Glossary

- Tablet:
  per-region local storage unit used by the tablet-based engine path
- Router:
  message delivery layer into store/peer/apply execution
- Apply FSM:
  apply-side state machine for committed work
- Response channel:
  callback/stream bridge back to the caller
