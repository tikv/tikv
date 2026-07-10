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
- `components/resource_control/src/score.rs`
- `components/resource_control/src/future.rs`
- `components/resource_control/src/channel.rs`
- `src/read_pool.rs` (external caller coupled to the read-pool contract below)

## Must-Read File Order

1. `components/resource_control/src/lib.rs`
2. `components/resource_control/src/config.rs`
3. `components/resource_control/src/resource_group.rs`
4. `components/resource_control/src/resource_limiter.rs`
5. `components/resource_control/src/service.rs`
6. `components/resource_control/src/worker.rs`
7. `components/resource_control/src/score.rs`
8. `components/resource_control/src/future.rs`
9. `components/resource_control/src/channel.rs`

## Main Responsibilities

- maintain `ResourceGroupManager`
- track per-group quotas and consumption
- wrap execution with `ControlledFuture` / `with_resource_limiter`
- watch resource-group definitions from PD meta storage
- report background RU usage back to PD
- adjust quotas and throttling behavior periodically
- expose fair scheduling and admission-control decisions to callers
- compute common CPU/IO/compaction pressure scores (`score.rs`) shared by
  background quota adjustment and foreground/read-pool throttling
- expose a read-pool CPU-pressure/target-CPU contract that `src/read_pool.rs`
  consumes to drive unified-read-pool scale in/out

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
  Each tick it measures CPU/IO/compaction inputs, calls
  `score::compute_resource_scores` to get `cpu_score`/`io_score`/
  `compaction_score`, uses `cpu_score`/`io_score` for its own background-quota
  adjustment, and passes `cpu_score` to
  `ResourceGroupManager::online_adjust_resource_quota` so the same signal
  drives foreground throttling and read-pool scheduling in `resource_group.rs`.
- `score.rs` is the shared pressure-scoring module. `compute_resource_scores`
  turns raw CPU/IO/compaction measurements into three independent 0-100
  scores; `pressure_fraction` maps a score onto a `[0, 1]` pressure fraction
  against caller-supplied thresholds; `ThreadGroupCpuTracker` measures CPU
  cores consumed by threads matching a name prefix (e.g.
  `UNIFIED_READ_POOL_THREAD`, `GRPC_SERVER_THREAD`) via `/proc` thread stats.
  This is the one shared code path behind both background quota adjustment
  (`worker.rs`) and foreground/read-pool throttling (`resource_group.rs`) — a
  bug here silently skews both.
- `config.rs` defines dynamic policy knobs such as fair scheduling and
  admission-control thresholds.

## Cross-Component Contract: Unified Read Pool Coupling

`ResourceGroupManager` exposes a small API that `src/read_pool.rs` depends on
to scale the unified read pool's thread count under foreground CPU pressure.
This is a real cross-crate contract, not an internal implementation detail —
changes to either side must keep the other consistent:

- `online_adjust_resource_quota(cpu_score)` — called once per tick from
  `worker.rs` with the shared `cpu_score` from `score::compute_resource_scores`.
  Internally refreshes `read_pool_cpu_pressure` (a `[0, 1]` fraction, `0` when
  foreground is not under pressure) and `read_pool_scale_up_allowed` (whether
  CPU is comfortably idle enough to let the read pool grow back toward its
  max).
- `compute_read_pool_target_cpu(read_pool_cpu, interval_secs)` — the read
  pool's actual scale-down input. Records `read_pool_cpu` into a historical
  tracker and returns either `f64::INFINITY` (no ceiling — caller's own
  ceiling wins) or a target below `read_pool_cpu` that slides toward the
  historical-CPU floor as pressure increases. This only ever scales down; the
  read pool itself converts the target into a thread count and owns scale-up.
- `read_pool_scale_up_allowed()` — read pool consults this before growing its
  thread count back up.
- `read_pool_cpu_floor(read_pool_cpu, interval_secs)` — lower-level primitive
  behind `compute_read_pool_target_cpu`; records usage into the historical
  tracker and returns the floor CPU on its own.

Invariant: `compute_read_pool_target_cpu` must never return a target above
`read_pool_cpu` when pressure is engaged; a caller `min()`-ing this into its
own ceiling relies on `INFINITY` (not the current usage) as the "no pressure"
value. If you change the pressure/threshold math in `resource_group.rs`,
re-check the read-pool scaling tests in `src/read_pool.rs` (search for
`online_adjust_resource_quota`, `read_pool_cpu_pressure`,
`read_pool_scale_up_allowed`).

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
- Pressure-scoring changes (`score.rs`):
  inspect both consumers — background quota adjustment in `worker.rs` and
  foreground/read-pool throttling in `resource_group.rs` — since they share
  the same `cpu_score`/`io_score`/`compaction_score` computation
- Read-pool coupling changes (`online_adjust_resource_quota`,
  `compute_read_pool_target_cpu`, `read_pool_scale_up_allowed`,
  `read_pool_cpu_floor`):
  inspect both `resource_group.rs` and `src/read_pool.rs`; update this guide's
  "Cross-Component Contract" section in the same change

## Review Checklist

- Does the change affect `watch_resource_groups`, reload, or retry behavior?
- Does it alter fairness phase encoding, priority ordering, or baseline logic?
- Does it change request rejection versus delay behavior?
- Does it change accounting units or resource-cost interpretation?
- Does it create lock contention in the hot path?
- Does it update metrics and tests for new scheduling outcomes?
- Does it touch `score.rs`? If so, does it affect both the background
  (`worker.rs`) and foreground/read-pool (`resource_group.rs`) consumers as
  intended?
- Does it touch the read-pool coupling API (`online_adjust_resource_quota`,
  `compute_read_pool_target_cpu`, `read_pool_scale_up_allowed`,
  `read_pool_cpu_floor`)? If so, is `src/read_pool.rs` updated and are its
  scaling tests still valid?

## Observability And Tests

- Metrics live under `metrics.rs` and within the group/limiter code.
- Many unit tests are inline in:
  `future.rs`, `service.rs`, `worker.rs`, `channel.rs`, `resource_limiter.rs`,
  and `score.rs`.
- Changes should usually be validated together with the call sites in
  `src/server`, `src/storage`, `components/batch-system`, and (for
  pressure-scoring / read-pool coupling changes) `src/read_pool.rs`.

## Common Failure Modes

- watcher stalls after compaction or transient PD errors
- starvation due to incorrect priority or baseline math
- too-aggressive shedding that impacts non-target traffic
- silently stale background-limiter reporting
- dynamic-config changes that fail to take effect operationally
- transient `/proc` read failures in `ThreadGroupCpuTracker` corrupting the
  next tick's CPU delta baseline (see `score.rs`)
- read-pool scale-down target computed from stale or unrecorded
  `read_pool_cpu` history, causing the read pool to over- or under-shrink

## Reading Map And Companion Docs

Suggested reading order:

1. `lib.rs`
2. `config.rs`
3. `resource_group.rs`
4. `resource_limiter.rs`
5. `service.rs`
6. `worker.rs`
7. `score.rs`
8. `future.rs`

Companion docs:

- `repo-overview.md`
- `src/server.md`
- `src/storage.md`
- `src/read_pool.rs` — no dedicated guide yet; see "Cross-Component Contract:
  Unified Read Pool Coupling" above for the API surface it depends on here

## Glossary

- RU:
  request unit used for resource accounting
- Baseline:
  historical usage reference for fairness and admission control
- Admission control:
  delay or reject logic under pressure
- Virtual time:
  scheduling progress notion used to compare group fairness state
- Resource score (`cpu_score`/`io_score`/`compaction_score`):
  0-100 utilization percentages from `score::compute_resource_scores`, shared
  by background quota adjustment and foreground/read-pool throttling
- Pressure fraction:
  a score mapped to `[0, 1]` via `score::pressure_fraction` against a
  caller-supplied `(start, end)` threshold range; drives both background
  throttling and read-pool target-CPU scaling
- Read-pool target CPU:
  the scale-down ceiling `resource_group.rs::compute_read_pool_target_cpu`
  hands to `src/read_pool.rs`; `INFINITY` means no pressure-driven ceiling

## Related Components

- `src/server/service/kv.rs` consumes resource-group context at the RPC edge.
- `src/storage` uses resource-control metadata and limiters during scheduling.
- `src/read_pool.rs` consumes `ResourceGroupManager`'s pressure/target-CPU API
  (see "Cross-Component Contract: Unified Read Pool Coupling" above) to scale
  the unified read pool's thread count under foreground CPU pressure.
- `components/batch-system` integrates with priority-aware execution.
