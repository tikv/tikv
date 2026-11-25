# `components/service` Maintenance Guide

## Purpose And Scope

`components/service` is a small control-plane helper crate. It does not own the
real gRPC service implementations. It owns service-control events and a tiny
manager used by the runtime and status server.

## Architectural Views

### Control-plane view

- event enum defines the control vocabulary
- manager tracks local state and emits service events outward

## Process Lifecycle And Startup Sequencing

- The manager is created during server bootstrap and then shared with runtime
  owners such as the status server and the main control loop.
- Event handling correctness depends on the outer runtime, but this crate owns
  the local state machine and idempotent emission behavior.
- The concrete outer owners today are `components/server/src/server.rs`,
  `components/server/src/server2.rs`, `components/server/src/signal_handler.rs`,
  and `src/server/status_server/mod.rs`.
- `GrpcServiceManager::new` starts in `Serving`, while `dummy()` is test-only.
  Any change to that default must be reviewed against startup probes, status
  APIs, and signal handling behavior.

## Data Model And Metadata Contracts

- Service state contract:
  init, serving, not-serving
- Event contract:
  pause, resume, graceful shutdown, exit
- This crate deliberately keeps a tiny local state model:
  `GrpcServiceStatus::{Init, Serving, NotServing}` plus
  `ServiceEvent::{PauseGrpc, ResumeGrpc, GracefulShutdown, Exit}`.
- The most important contract is idempotent state-to-event mapping in
  `service_manager.rs`: `pause()` should not emit repeated pause requests when
  already paused, and `resume()` should not emit repeated resume requests when
  already serving.
- `GracefulShutdown` and `Exit` are owned by the outer runtime rather than the
  manager itself. Do not move process-orchestration policy into this crate.

## Start Here

- `components/service/src/lib.rs`
- `components/service/src/service_event.rs`
- `components/service/src/service_manager.rs`

## Must-Read File Order

1. `components/service/src/service_event.rs`
2. `components/service/src/service_manager.rs`
3. `components/server/src/server.rs`
4. `src/server/status_server/mod.rs`

## What It Owns

- `ServiceEvent`: pause, resume, graceful shutdown, exit
- `GrpcServiceManager`: lightweight state plus event sender

## Why It Matters

This crate is simple, but changes here can break pause/resume or coordinated
shutdown semantics across the process.

## Critical Invariants

- Event emission must remain idempotent for pause/resume.
- The local status state should not drift from the events sent to the outer
  handler.
- This crate should stay minimal. Do not move real server logic into it.

## Observability And Operational Signals

- operational visibility mainly comes from outer runtime behavior rather than
  local metrics in this crate
- There is little local metric surface. Real triage usually starts with:
  `components/server/src/server.rs` or `server2.rs`,
  `components/server/src/signal_handler.rs`, and
  `src/server/status_server/mod.rs`.
- Failures here usually present as “pause/resume API says success but serving
  state did not change”, or as shutdown ordering issues rather than as direct
  metric anomalies.

## Change Management Guidance

- Keep this crate small and contract-focused.
- If control vocabulary or state behavior changes, update both this guide and
  the owners in `components/server` and `src/server/status_server`.
- If a new service-control event is added, document:
  who emits it, who consumes it, whether it is idempotent, and whether it is a
  local serving-state transition or a process-level shutdown signal.
- A patch that changes manager state transitions without updating the HTTP
  status server and signal-handling consumers is almost certainly incomplete.

## Change-Impact Matrix

- Event vocabulary changes:
  inspect `service_event.rs`, `components/server`, and status-server consumers
- Pause/resume state changes:
  inspect `service_manager.rs`, status-server control endpoints, and runtime
  handlers
- Shutdown-signal changes:
  inspect signal handling and outer process-control loops rather than only this
  crate

## Review Checklist

- Does the change preserve pause/resume idempotency?
- Does it affect event ordering seen by `components/server`?
- Is new state being added here that really belongs in runtime code instead?

## Observability And Tests

- There is little dedicated test surface now; integration behavior is visible
  through `components/server` and `src/server/status_server`.
- `GrpcServiceManager::dummy()` is a hint that many tests cover this behavior
  indirectly through status-server and server integration rather than local unit
  tests.

## Reading Map And Companion Docs

- `service_event.rs`
- `service_manager.rs`
- `components/server.md`
- `src/server.md`

## Glossary

- Serving:
  the local state in which the manager considers gRPC available
- Pause:
  the control event used to stop serving without full process exit
- Exit:
  hard process termination signal in the service event vocabulary, distinct from
  graceful gRPC pause or drain

## Related Components

- `components/server` consumes these events in its main control loop.
- `src/server/status_server/mod.rs` exposes pause/resume operations.
