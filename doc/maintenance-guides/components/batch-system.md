# `components/batch-system` Maintenance Guide

## Purpose And Scope

`batch-system` is the generic FSM execution framework underneath raftstore. It
owns mailbox routing, polling, batching, rescheduling, and pool-level metrics.

## Architectural Views

### Execution view

- mailbox delivers messages
- router finds the target FSM
- batch groups normal and control FSMs into one polling round
- pollers and handlers execute the work

## Process Lifecycle And Startup Sequencing

- This subsystem is instantiated by higher-level runtime owners, mainly
  raftstore.
- FSM, mailbox, and router ownership must be fully established before pollers
  start running.
- Shutdown must use the crate's sentinel and release logic correctly so no FSM
  is stranded.
- The main runtime construction path is `batch.rs::create_system`, then the
  subsystem-specific builders in raftstore such as
  `components/raftstore/src/store/fsm/store.rs::create_raft_batch_system` and
  `components/raftstore/src/store/fsm/apply.rs::create_apply_batch_system`.
- Poller threads execute with explicit IO classification in `batch.rs`. If a
  patch changes what pollers do synchronously, verify the foreground/background
  IO assumptions still hold.
- Shutdown uses the `FsmTypes::Empty` sentinel plus mailbox closing and FSM
  state clearing. Review `batch.rs`, `mailbox.rs`, and `fsm.rs` together when
  touching stop-path logic.

## Data Model And Metadata Contracts

- Mailbox ownership and queue length are part of the scheduling contract.
- Normal FSMs and control FSMs have intentionally different semantics.
- Reschedule policy is part of the runtime contract between the poller and the
  caller.
- `fsm.rs::FsmState` is the core ownership contract. The `NOTIFIED`, `IDLE`,
  and `DROP` state machine is small but easy to break with seemingly harmless
  refactors.
- `mailbox.rs` couples message enqueue and “schedule if idle” behavior. Any
  change that splits or reorders those steps risks lost wakeups or duplicate
  scheduling.
- `batch.rs::Batch::release` depends on queue length checks and mailbox
  re-taking to preserve message visibility. Reviewers should treat that logic as
  correctness-sensitive, not only performance-sensitive.
- Resource control is part of the contract here because `Fsm::Message:
  ResourceMetered` and `FsmScheduler::consume_msg_resource` affect scheduling
  fairness and accounting.

## Start Here

- `components/batch-system/src/lib.rs`
- `components/batch-system/src/batch.rs`
- `components/batch-system/src/fsm.rs`
- `components/batch-system/src/mailbox.rs`
- `components/batch-system/src/router.rs`
- `components/batch-system/src/scheduler.rs`
- `components/batch-system/src/config.rs`

## Must-Read File Order

1. `components/batch-system/src/fsm.rs`
2. `components/batch-system/src/mailbox.rs`
3. `components/batch-system/src/router.rs`
4. `components/batch-system/src/batch.rs`
5. `components/batch-system/src/scheduler.rs`
6. `components/batch-system/src/metrics.rs`

## Core Concepts

- `Fsm`: the executable state machine abstraction.
- `Mailbox`: the message queue plus ownership handoff mechanism.
- `Batch`: one poll-round of normal and control FSMs.
- `Router`: lookup and message delivery.
- `Poller` / `PollHandler`: execution hooks used by raftstore.

## Why It Matters

This crate is small, but it sits on a critical path. Tiny changes here can
shift fairness, latency, shutdown behavior, and queue backpressure for
raftstore.

## Critical Invariants

- At most one poller owns a given FSM at a time.
- Release/remove/reschedule paths must preserve pending-message visibility.
- Control FSM and normal FSM scheduling semantics must remain distinct.
- Metrics should not distort hot-path behavior.
- Queue ownership handoff must remain lock-safe and race-safe.

## Observability And Operational Signals

- FSM count, schedule wait duration, poll duration, and reschedule counters
- queueing and batch-size behavior during load tests
- Start with `components/batch-system/src/metrics.rs`. The most useful signals
  are:
  `tikv_batch_system_fsm_reschedule_total`,
  `tikv_batch_system_fsm_schedule_wait_seconds`,
  `tikv_batch_system_fsm_poll_seconds`,
  `tikv_batch_system_fsm_poll_rounds`,
  `tikv_batch_system_fsm_count_per_poll`, and
  `tikv_channel_full_total`.
- Router broadcast latency is tracked through
  `tikv_broadcast_normal_duration_seconds`.
- Many user-visible symptoms surface first in raftstore metrics rather than
  here, so batch-system signals should usually be read together with raftstore
  queue, proposal, and apply latency signals.

## Change Management Guidance

- Changes here should be treated as cross-cutting changes because raftstore
  behavior depends on them heavily.
- If mailbox, ownership, or scheduling semantics change, update this guide and
  the `raftstore` guide together.
- Do not add instrumentation, allocation, or synchronization on the hot path
  casually. This crate sits beneath both store and apply FSM execution.
- If a change affects `FsmState`, mailbox closure, or release/remove semantics,
  require explicit reasoning about why wakeups cannot be lost and why duplicate
  ownership cannot occur.
- If a change affects priority or resource accounting, review it with
  `components/resource_control` in mind rather than only with raftstore in
  mind.

## Change-Impact Matrix

- FSM ownership or wakeup changes:
  inspect `fsm.rs`, `mailbox.rs`, and `batch.rs`
- Router or delivery changes:
  inspect `router.rs`, mailbox semantics, and raftstore router callers
- Polling, reschedule, or batch-shape changes:
  inspect `batch.rs`, `scheduler.rs`, and raftstore poll handlers
- Priority or resource-accounting changes:
  inspect `fsm.rs`, `scheduler.rs`, `components/resource_control`, and
  raftstore apply/store pollers

## Review Checklist

- Does the change alter mailbox ownership or release behavior?
- Does it alter batch reschedule policy or the polling round shape?
- Does it add work to the poller hot path?
- Does it change shutdown signaling or empty-sentinel handling?
- Does it change priority integration with resource control?

## Observability And Tests

- Tests exist directly in `tests/cases/*`.
- Benches exist in `benches/router.rs` and `benches/batch-system.rs`.
- Many regressions show up first as raftstore latency or queueing anomalies, so
  validation often needs raftstore-level testing too.
- `components/raftstore/src/store/fsm/store.rs` and `apply.rs` are practical
  companion reading because they show the intended use pattern of this
  framework.

## Common Failure Modes

- duplicate ownership of an FSM
- lost reschedule after release
- mailbox state drift causing stuck FSMs
- queue fairness regressions under load

## Reading Map And Companion Docs

Suggested reading order:

1. `fsm.rs`
2. `mailbox.rs`
3. `scheduler.rs`
4. `batch.rs`
5. `router.rs`

Companion docs:

- `repo-overview.md`
- `components/raftstore.md`

## Glossary

- FSM:
  finite state machine polled by the runtime
- Mailbox:
  queue plus ownership handoff container for an FSM
- Poller:
  worker that executes batched FSM rounds
- Control FSM:
  global or coordinating FSM with different routing and lifecycle semantics
  from normal peer/apply FSMs

## Related Components

- `components/raftstore/src/store/fsm/*` is the main consumer.
- `components/resource_control` affects execution priority and queueing.
