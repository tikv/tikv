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
- name the groups responsible for a foreground overload, and drive both
  actuators from that one verdict

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
- Background quota limiting is independent of priority-queue selection. A
  background limiter can throttle a task in place even when the transaction
  scheduler uses its vanilla queue.
- `ResourceController::is_customized` reflects non-default resource groups;
  background configuration is tracked separately by `ResourceGroupManager`.

## Cross-Component Contract: Unified Read Pool Coupling

`ResourceGroupManager` exposes a small API that `src/read_pool.rs` depends on
to scale the unified read pool's thread count under foreground CPU pressure.
This is a real cross-crate contract, not an internal implementation detail —
changes to either side must keep the other consistent:

> **`enable-fair-scheduling` requires `readpool.unified.auto-adjust-pool-size`.**
> A group is deprioritised while the pool is scaled in and released once the
> pool recovers to `core_thread_count`, so pool movement *is* the release
> signal. `adjust_pool_size` returns early when auto-adjustment is off, which
> means the pool never moves and no group is ever deprioritised. Since
> `auto-adjust-pool-size` defaults to false, enabling fair scheduling on its
> own silently does nothing. This is deliberately not rejected at config load,
> to stay backward compatible with configs that already set only one of them.


- `online_adjust_resource_quota(cpu_score)` — called once per tick from
  `worker.rs` with the shared `cpu_score` from `score::compute_resource_scores`.
  Runs detection (see "Noisy-Group Selection") and then refreshes
  `read_pool_cpu_pressure` (`1.0` or `0.0` — a flag, not a varying fraction,
  because the read pool only tests it against zero) and
  `read_pool_scale_up_allowed` (whether CPU is comfortably idle enough to let
  the read pool grow back toward its max).
- `compute_read_pool_target_cpu(read_pool_cpu, interval_secs)` — the read
  pool's actual scale-down input. Records `read_pool_cpu` into the pool's
  tracker and returns either `f64::INFINITY` (no ceiling — caller's own
  ceiling wins) or a target below `read_pool_cpu`, stepped down per engaged
  tick and floored at the pool's quiet-window CPU.
- The floor is the *quiet-window* freeze, not the live historical average. The
  average keeps recording the overload, so it rises as load is shed and floats
  the floor up under the ratchet. Until there has been a quiet tick the floor
  is `MIN_READ_POOL_TARGET_CORES` (1 core), which is also what stops the
  ratchet compounding toward zero. Same mechanism as a group's baseline.
- `read_pool_scale_up_allowed()` — read pool consults this before growing its
  thread count back up.

Invariant: `compute_read_pool_target_cpu` must never return a target above
`read_pool_cpu` when pressure is engaged; a caller `min()`-ing this into its
own ceiling relies on `INFINITY` (not the current usage) as the "no pressure"
value. If you change the pressure/threshold math in `resource_group.rs`,
re-check the read-pool scaling tests in `src/read_pool.rs` (search for
`online_adjust_resource_quota`, `read_pool_cpu_pressure`,
`read_pool_scale_up_allowed`).

## Noisy-Group Selection

Detection runs once per tick in `online_adjust_resource_quota_at`, before
either actuator, so both act on the same verdict against one set of
measurements. The verdict is a set of group names, published by
`noisy_groups()` and consumed by `adjust_group_throttling` (per-group CPU rate
limit, -15% per engaged tick, +10% per tick on recovery) and
`deprioritize_over_quota_groups` (read-scheduler phase).

`select_noisy_groups(cpu_score)`:

- `survey_groups` makes one pass over the per-group trackers. A group is a
  candidate when it is above its own baseline by more than `baseline_burst_pct`
  (default 20%) and has been for `MIN_ENGAGE_TICKS` ticks.
- Candidates rank by *excess* (rate above baseline) — what identifies the group
  that changed, not the group that is merely large. Ties break on name, since
  candidates arrive in `DashMap` order and would otherwise vary per restart.
- `take_biggest_movers` takes from the top until credited relief covers
  `total_usage * (cpu_score - fg_cpu_throttle_threshold) / 100` — a tick barely
  over the threshold names only the worst offender, a saturated one reaches
  further down. Candidates below `TAIL_EXCESS_RATIO` (10%) of the top excess
  are spared, and the top candidate is always taken unless something is
  already held.
- Groups an actuator is already holding stay named — they sit inside their gate
  only because they are held there — and their whole share is credited against
  the target rather than inflating one the innocent tail would be taken to
  meet.

Baselines are *quiet-window* frozen, not rolling: a group's baseline updates
only while the node is quiet, so the reference does not drift upward during the
overload it is meant to explain.

What a *missing* baseline means is the operator's call, and it is the only
thing separating the first two policies below. `candidate_baseline` is the sole
interpreter of `noisy_detection`: it answers both "is this group a candidate"
and "what baseline is its excess charged against". `refresh_trackers` gathers
statistics only and never sees the policy — it advances two policy-free
counters, `over_baseline_ticks` and `active_ticks`, and each policy reads
whichever one it rests on. A new policy is therefore one arm in one `match`.

`noisy_detection` (`NoisyDetection`, default `baseline-fallback-current-usage`)
picks the ranking key:

- `baseline` — furthest above its own quiet baseline. Names the group that
  changed. A group whose quiet window has not elapsed has no baseline and is
  never a candidate, so an overload driven entirely by a group with no history
  goes unattributed and nothing is throttled.
- `baseline-fallback-current-usage` — as `baseline`, except a missing baseline
  reads as zero, so every bit of such a group's usage counts as excess and it
  ranks on current usage. Keeps a node with no history protected, which is why
  it is the default: excluding a fresh group hid the culprit during its ramp.
- `current-usage` — largest consumer right now, no history. Nothing to go
  stale, but the legitimately largest tenant is blamed every time.

Baselines are recorded under every policy, including `current-usage`, so a
switch takes effect on the next tick rather than waiting a quiet window for
history to reappear.

`tikv_resource_control_effective_noisy_detection{policy=...}` reads 1 for the
policy in force, which is how you confirm an online change landed. It is the
configured policy: the fallback is per group, not node-wide, and a group
currently falling back is the one whose
`tikv_resource_control_group_ru_baseline` reads 0.

Accounting knob: `request_base_cost_micros` (default 40µs) is a fixed arrival
charge, added to a group's foreground tracker once per request at the gRPC
handler entry — before admission control, so a rejected request still pays, and
skipped for background-routed requests, which their background limiter meters.
It is cached outside the config lock and re-read by `refresh_cached_config` on
each control tick, so a config change lands within one tick.

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
  `compute_read_pool_target_cpu`, `read_pool_scale_up_allowed`):
  inspect both `resource_group.rs` and `src/read_pool.rs`; update this guide's
  "Cross-Component Contract" section in the same change
- Detection changes (`select_noisy_groups`, `survey_groups`, quiet baselines,
  `noisy_detection`): inspect both actuators — `adjust_group_throttling` and
  `deprioritize_over_quota_groups` — since they consume one shared verdict

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
  `compute_read_pool_target_cpu`, `read_pool_scale_up_allowed`)? If so, is
  `src/read_pool.rs` updated and are its scaling tests still valid?
- Does it change who gets blamed for an overload (candidacy, ranking, the
  target, or the quiet baseline)? If so, does a test pin *which* group is
  picked, not just how many?

## Observability And Tests

- Metrics live under `metrics.rs` and within the group/limiter code.
- Many unit tests are inline in:
  `future.rs`, `service.rs`, `worker.rs`, `channel.rs`, `resource_limiter.rs`,
  `score.rs`, and — for detection, throttling and the read-pool contract —
  `resource_group.rs`.
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
- a floor taken from live average CPU instead of the quiet window, which rises
  as load is shed and so floats up under the ratchet
- blame that lands on a group whose baseline is stale, or on the largest tenant
  rather than the one that changed

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
  a group's usage reference, frozen from quiet windows rather than rolling, so
  it does not drift up during the overload it is meant to explain. The read
  pool's own floor is the same mechanism on the pool's tracker.
- Excess:
  a group's rate above its own baseline — the ranking key for blame, since it
  identifies the group that *changed* rather than the group that is large
- Noisy group:
  a group this tick blamed for the foreground overload; see
  "Noisy-Group Selection"
- Held group:
  a group an actuator is already holding (finite CPU rate limit or scheduler
  backpressure). Stays named, and its share counts toward the target
- Admission control:
  delay or reject logic under pressure
- Virtual time:
  scheduling progress notion used to compare group fairness state
- Resource score (`cpu_score`/`io_score`/`compaction_score`):
  0-100 utilization percentages from `score::compute_resource_scores`, shared
  by background quota adjustment and foreground/read-pool throttling
- Pressure fraction:
  a score mapped to `[0, 1]` via `score::pressure_fraction` against a
  caller-supplied `(start, end)` threshold range; drives background quota
  adjustment in `worker.rs`. Foreground/read-pool throttling does not use it —
  it engages on a flag and steps by a fixed -15% per tick
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
