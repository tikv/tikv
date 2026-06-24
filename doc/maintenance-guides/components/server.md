# `components/server` Maintenance Guide

## Purpose And Scope

`components/server` owns TiKV process construction and orchestration. It is the
startup and shutdown shell around the runtime subsystems. This crate should be
read as lifecycle glue, not business logic.

## Architectural Views

### Bootstrap orchestration view

- config validation and process setup
- engine assembly
- service and worker registration
- runtime startup and stop control

### Variant view

- `server.rs` drives the classic path
- `server2.rs` drives the alternate tablet-based path
- `common.rs` holds shared process capabilities

## Process Lifecycle And Startup Sequencing

- This crate is the main owner of startup sequencing for a TiKV process.
- Correct ordering is:
  config and filesystem setup, engine setup, worker/runtime setup, service
  registration, runtime start, event-loop control.
- Correct shutdown is the reverse, with extra care for in-flight service events
  and worker ownership.

Concrete startup checkpoints:

1. `TikvServerCore::init_config`
2. `check_conflict_addr`
3. `init_fs`
4. engine and utility initialization
5. service registration and runtime start
6. status server start
7. service-event loop

## Data Model And Metadata Contracts

- process-level config and persisted-config contract
- store path and lock-file contract
- engine selection contract
- service event routing contract with `components/service`

High-risk config contracts:

- data-directory and engine-layout compatibility
- reserved-space behavior for recovery
- process-wide memory limits and high-water handling

## Start Here

- `components/server/src/lib.rs`
- `components/server/src/common.rs`
- `components/server/src/raft_engine_switch.rs`
- `components/server/src/server.rs`
- `components/server/src/server2.rs`
- `components/server/src/setup.rs`
- `components/server/src/signal_handler.rs`

## Must-Read File Order

1. `components/server/src/common.rs`
2. `components/server/src/setup.rs`
3. `components/server/src/server.rs`
4. `components/server/src/server2.rs`
5. `components/server/src/raft_engine_switch.rs`
6. `components/server/src/signal_handler.rs`

## Major Responsibilities

- validate and persist config
- initialize filesystem, lock files, reserved disk space, and panic markers
- create engines and engine-related helpers
- wire classic raftstore or `RaftKv2` / `raftstore-v2`, plus storage,
  coprocessor, status server, GC worker, CDC, backup, and other services
- register dynamic-config managers
- handle service pause/resume and graceful shutdown
- support both v1 and v2 engine/server startup paths

## Important Structure

- `common.rs` contains reusable server-core facilities used by different startup
  modes.
- `server.rs` is the main startup path for the existing raft-kv oriented stack.
- `server2.rs` is the analogous startup path for the `RaftKv2` /
  `raftstore-v2` stack.
- `raft_engine_switch.rs` owns the online raft-engine migration helper path and
  is easy to miss when changing engine bootstrap behavior.
- `setup.rs` owns logging and early process environment setup.

## Critical Invariants

- Startup ordering matters. Many subsystems assume other pieces already exist.
- Shutdown ordering matters. Workers and services must stop after their
  dependents drain.
- Config validation must remain backward-compatible with persisted data layout
  and engine selection.
- Service pause/resume must stay consistent with the small control plane in
  `components/service`.

## Observability And Operational Signals

- startup and shutdown logs
- config persistence and validation output
- disk-space reservation and lock-file failures
- memory/high-water signals registered during startup

Start triage with:

- startup logs from `setup.rs` and `common.rs`
- config-controller state
- process lifecycle logs in `server.rs`

## Change Management Guidance

- Treat startup ordering and stop ordering as correctness-sensitive.
- If a new subsystem is started or stopped here, update this guide and the
  matching runtime guide in the same patch.
- If a change only updates the startup path but not the operational stop path,
  it is usually incomplete.

## Change-Impact Matrix

- Startup or dependency-order changes:
  inspect `common.rs`, `setup.rs`, `server.rs`, and `src/server/raft_server.rs`
- Service wiring changes:
  inspect `server.rs` or `server2.rs`, runtime owners in `src/server`, and
  service-control consumers
- Engine bootstrap or path-selection changes:
  inspect `common.rs`, `raft_engine_switch.rs`, `server.rs`, and `server2.rs`
- Shutdown or signal changes:
  inspect `signal_handler.rs`, service-event loop handling, and the matching
  runtime stop paths

## Review Checklist

- Does the change alter startup order or create new cyclic dependencies?
- Does it register a new worker/service without adding a stop path?
- Does it change engine selection, data-path validation, or filesystem layout?
- Does it update only `server.rs` but also need parity in `server2.rs`, or the
  reverse?
- Does it alter graceful shutdown semantics?
- Does it add expensive initialization to the critical startup path?

## Observability And Tests

- There is not much isolated test surface here; validation usually requires
  wider integration testing or startup smoke checks.
- Logging, metrics registration, and signal handling are all useful places to
  verify when process-lifecycle bugs happen.

## Common Failure Modes

- subsystem started before config or dependency is ready
- service added without a coordinated shutdown path
- persistent-data-layout mismatch after config changes
- pause/resume events not reaching the real runtime owner

## Reading Map And Companion Docs

Suggested reading order:

1. `common.rs`
2. `raft_engine_switch.rs`
3. `setup.rs`
4. `server.rs`
5. `server2.rs`
6. `signal_handler.rs`

Companion docs:

- `repo-overview.md`
- `src/server.md`
- `src/storage.md`

## Glossary

- Graceful shutdown:
  stop sequence intended to drain or stop subsystems cleanly
- Persisted config:
  config material validated against local data layout and prior runs
- Conflict address lock:
  temp-file based guard against multiple TiKV instances serving the same addr

## Related Components

- `src/server` is the runtime service stack that this crate wires together.
- `components/service` supplies the control events used for pause/resume.
