# `components/resource_control` Maintenance Guide

## Purpose And Scope

`resource_control` owns request-level fairness and resource-isolation logic for
resource groups. It watches PD metadata, maintains group state, wraps futures
and channels with accounting, and adjusts limiters over time.

## Architectural Views

### Policy view

- resource-group definition and runtime state
- limiter implementation
- fairness and admission control policy
- PD watch/report control plane

### Runtime view

- hot-path wrappers on futures/channels
- periodic background adjustment workers
- asynchronous PD metadata synchronization

## Process Lifecycle And Startup Sequencing

- The owning process starts this subsystem from server bootstrap.
- Periodic tasks are started by `start_periodic_tasks`:
  min-virtual-time advancement, PD watch loop, quota adjustment, RU reporting.
- Shutdown safety depends on the owning worker/runtime lifecycle, so long-lived
  tasks here should remain cancellation-safe and retry-safe.

Concrete runtime anchors:

- periodic-task setup:
  `lib.rs::start_periodic_tasks`
- PD watch and reload loop:
  `service.rs`
- quota adjustment:
  `worker.rs`

## Data Model And Metadata Contracts

- Resource-group definitions come from PD meta storage.
- Runtime metadata includes:
  group mode, quotas, virtual-time state, baseline usage, limiter statistics,
  admission decisions.
- Background usage reports back to PD as request-unit accounting.

Hot contracts to review carefully:

- group-name identity and default/background-group semantics
- baseline-window and fairness-phase encoding
- RU accounting inputs and report-unit interpretation

## Start Here

- `components/resource_control/src/lib.rs`
- `components/resource_control/src/resource_group.rs`
- `components/resource_control/src/resource_limiter.rs`
- `components/resource_control/src/service.rs`
- `components/resource_control/src/config.rs`
- `components/resource_control/src/worker.rs`
- `components/resource_control/src/future.rs`
- `components/resource_control/src/channel.rs`

## Must-Read File Order

1. `components/resource_control/src/lib.rs`
2. `components/resource_control/src/config.rs`
3. `components/resource_control/src/resource_group.rs`
4. `components/resource_control/src/resource_limiter.rs`
5. `components/resource_control/src/service.rs`
6. `components/resource_control/src/worker.rs`
7. `components/resource_control/src/future.rs`
8. `components/resource_control/src/channel.rs`

## Main Responsibilities

- maintain `ResourceGroupManager`
- track per-group quotas and consumption
- wrap execution with `ControlledFuture` / `with_resource_limiter`
- watch resource-group definitions from PD meta storage
- report background RU usage back to PD
- adjust quotas and throttling behavior periodically
- expose fair scheduling and admission-control decisions to callers

## Important Design Points

- `service.rs` is the PD-facing control plane. It reloads resource groups,
  watches config paths, and retries on compaction or transient failures.
- `resource_group.rs` is the policy core. It contains:
  - resource group state
  - virtual-time and baseline logic
  - admission decisions
  - fairness/two-phase scheduling helpers
- `resource_limiter.rs` is the actual limiter and statistics store.
- `worker.rs` adjusts background quota and related runtime state periodically.
- `config.rs` defines dynamic policy knobs such as fair scheduling and
  admission-control thresholds.
- Background quota limiting is independent of priority-queue selection. A
  background limiter can throttle a task in place even when the transaction
  scheduler uses its vanilla queue.
- `ResourceController::is_customized` reflects non-default resource groups;
  background configuration is tracked separately by `ResourceGroupManager`.

## Critical Invariants

- Group configuration must converge safely when PD watch streams restart or the
  watch revision is compacted.
- Accounting paths must remain cheap because they can sit on hot request paths.
- Admission control and fair scheduling must degrade specific traffic classes,
  not accidentally all traffic.
- Dynamic config updates must alter runtime behavior, not only the stored config
  value.
- Background-group reporting must not silently double-count or regress versioned
  limiter statistics.

## Observability And Operational Signals

- limiter and scheduling metrics in `metrics.rs`
- logs on PD watch/reload failures, compaction restarts, and config loads
- RU reporting cadence and background-group behavior

Start triage with:

- `metrics.rs`
- `service.rs` watch/reload logs
- `worker.rs` quota-adjustment logic

## Change Management Guidance

- If policy knobs, admission behavior, or PD metadata contracts change, update
  this guide in the same patch.
- If callers in storage/server/batch-system start depending on new semantics,
  update both ends of the contract.
- Fairness or admission changes should come with before/after reasoning about
  who gets delayed, who gets rejected, and under which pressure signal.

## Change-Impact Matrix

- PD watch or config reload changes:
  inspect `service.rs`, PD client interactions, and caller assumptions about
  convergence
- Fairness or baseline changes:
  inspect `resource_group.rs`, `resource_limiter.rs`, and hot-path consumers in
  storage/server
- Request admission or delay/reject changes:
  inspect `future.rs`, `channel.rs`, `src/server/service/kv.rs`, and
  `src/storage`
- Background RU reporting changes:
  inspect `worker.rs`, limiter statistics, and PD-facing reporting contracts
- Config knob changes:
  inspect `config.rs`, runtime update sites, metrics, and reviewer-facing docs

## Review Checklist

- Does the change affect `watch_resource_groups`, reload, or retry behavior?
- Does it alter fairness phase encoding, priority ordering, or baseline logic?
- Does it change request rejection versus delay behavior?
- Does it change accounting units or resource-cost interpretation?
- Does it create lock contention in the hot path?
- Does it update metrics and tests for new scheduling outcomes?

## Observability And Tests

- Metrics live under `metrics.rs` and within the group/limiter code.
- Many unit tests are inline in:
  `future.rs`, `service.rs`, `worker.rs`, `channel.rs`, and `resource_limiter.rs`.
- Changes should usually be validated together with the call sites in
  `src/server`, `src/storage`, and `components/batch-system`.

## Common Failure Modes

- watcher stalls after compaction or transient PD errors
- starvation due to incorrect priority or baseline math
- too-aggressive shedding that impacts non-target traffic
- silently stale background-limiter reporting
- dynamic-config changes that fail to take effect operationally

## Reading Map And Companion Docs

Suggested reading order:

1. `lib.rs`
2. `config.rs`
3. `resource_group.rs`
4. `resource_limiter.rs`
5. `service.rs`
6. `worker.rs`
7. `future.rs`

Companion docs:

- `repo-overview.md`
- `src/server.md`
- `src/storage.md`

## Glossary

- RU:
  request unit used for resource accounting
- Baseline:
  historical usage reference for fairness and admission control
- Admission control:
  delay or reject logic under pressure
- Virtual time:
  scheduling progress notion used to compare group fairness state

## Related Components

- `src/server/service/kv.rs` consumes resource-group context at the RPC edge.
- `src/storage` uses resource-control metadata and limiters during scheduling.
- `components/batch-system` integrates with priority-aware execution.
