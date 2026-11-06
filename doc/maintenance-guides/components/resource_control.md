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
- read-policy generation, positive weights, and priority updates
- shared baseline-window accounting for foreground write admission
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
  background quota adjustment and foreground write throttling
- expose stable `GroupPolicy` handles through `GroupProvider` for the unified
  read pool; its scheduling ledger and CPU budget live in `tikv_util`

## Important Design Points

- `service.rs` is the PD-facing control plane. It reloads resource groups,
  watches config paths, and retries on compaction or transient failures.
- `resource_group.rs` is the policy core. It contains:
  - resource group state
  - legacy virtual-time controllers for non-unified-read pools
  - stable policy identities for the unified read scheduler
  - shared usage history and foreground write admission decisions
- `resource_limiter.rs` is the actual limiter and statistics store.
- `worker.rs` adjusts background quota and related runtime state periodically.
  Each tick it measures CPU/IO/compaction inputs, calls
  `score::compute_resource_scores` to get `cpu_score`/`io_score`/
  `compaction_score`, uses `cpu_score`/`io_score` for its own background-quota
  adjustment, and passes `cpu_score` to
  `ResourceGroupManager::online_adjust_resource_quota` so the same signal
  drives foreground write throttling in `resource_group.rs`. The unified read
  scheduler does not consume this signal or the historical baseline.
- `score.rs` is the shared pressure-scoring module. `compute_resource_scores`
  turns raw CPU/IO/compaction measurements into three independent 0-100
  scores; `pressure_fraction` maps a score onto a `[0, 1]` pressure fraction
  against caller-supplied thresholds; `ThreadGroupCpuTracker` measures CPU
  cores consumed by threads matching a name prefix (e.g.
  `UNIFIED_READ_POOL_THREAD`, `GRPC_SERVER_THREAD`) via `/proc` thread stats.
  This is the one shared code path behind both background quota adjustment
  (`worker.rs`) and foreground write throttling (`resource_group.rs`) — a
  bug here silently skews both.
- `config.rs` defines background/write admission policy knobs. The obsolete
  `enable-fair-scheduling` and `enable-read-admission-control` fields remain
  parseable but are ignored; validation warns when they are enabled.
- Background quota limiting is independent of priority-queue selection. A
  background limiter can throttle a task in place even when the transaction
  scheduler uses its vanilla queue.
- `ResourceController::is_customized` reflects non-default resource groups;
  background configuration is tracked separately by `ResourceGroupManager`.

## Cross-Component Contract: Unified Read Pool

`ResourceGroupManager` implements `tikv_util::yatp_pool::fair_queue::GroupProvider`.
The provider returns an `Arc<GroupPolicy>` containing canonical name, identity,
configured priority, positive weight, and retirement state. It does not read or
mutate scheduling VT. The fair queue owns each priority lane's committed VT,
reservations, idle placement, task history, and CPU gate.

Identity and update rules:

- Names are ASCII-case-insensitive. Unknown names resolve to the existing
  default handle; lookup never creates another scheduling entitlement.
- Ordinary PD updates and watch reloads update the existing handle. They must
  not create a generation or reset queue history.
- Deletion and `retain` retire the removed handle. A later create receives a
  new identity. In-flight work retains its original handle for settlement;
  the queue redirects retired queued work to default.
- Default remains available. Deleting its customized definition restores its
  default policy without retiring its identity. The queue caches this handle
  and registers its scheduling group before workers start.
- Normal finite weights equal read quota (RU fill rate in RU mode, CPU fill
  rate in raw mode). Zero quota and default's unlimited sentinel use
  `min(10 * max(10000, largest normal quota), MAX_RU_QUOTA)` instead.
  Changing this reference weight updates only special handles; ordinary
  groups keep their units and the queue retains committed progress.
- The registry serializes metadata publication. It never calls the scheduler
  or destroys tasks while holding its lock. Consumers must not call the
  provider while holding a scheduler lock that can be reentered.

Read admission and accounting:

- Foreground reads bypass the shared historical limiter's admission delay.
  Its debt/rate is not cleared, because foreground writes may still use it.
- `LimitedFuture` continues recording shared read/write usage. Keeping this
  history preserves existing write policy; splitting read and write history
  would be a separate behavior change.
- Background requests still consult their background limiter. Removing
  historical read admission must not disable background CPU/IO control.
- New read scheduling does not consume the legacy controller's VT or external
  RPC CPU/write-byte penalties. Other pools keep their legacy controllers.
- The global CPU gate and live-task limits belong to the unified read pool.
  There is no Phase bit, historical read-pool CPU floor, pressure-driven
  target, or recovery-to-core release signal. RC unified pools keep their
  configured `max-thread-count`: `auto-adjust-pool-size` and `cpu-threshold`
  do not control that pool, and `read-cpu-ratio` owns its CPU budget. Non-RC
  pools retain their existing CPU-threshold and autoscaling behavior.
- YATP prestarts surplus physical workers for online growth, and its Custom
  queue can be popped before those workers park. The fair queue therefore
  bounds active dispatches under its existing scheduler lock. The bound is
  installed before workers start and updated by `scale_pool_size`. Only
  `Dispatch::drop` releases a slot; inline repolls and cancellation of a running
  task do not release it early. Lowering the bound lets current dispatches
  finish but blocks new ones until below the new limit. A full bound returns
  `Pending` with a 1ms retry, while `has_ready_task` still signals queued work
  to the current runner for preemption. Online growth retains YATP's wake
  behavior: new submissions or worker turnover activate the additional threads.
- A ready `BTreeSet` indexes current committed-plus-reserved VT per nonempty
  priority lane. Selection and the initial precharge share one scheduler
  critical section. A separate committed index advances each priority's
  monotonic reference; the new queue does not use periodic legacy VT calibration.
- RG generations share live capacity and task-id history across their priority
  lanes. The internal scheduler task ID identifies one live task for lookup and
  cancellation; it is distinct from the reusable MLFQ history task ID.
- CPU ratio, per-RG base capacity, precharge, and time slice are startup
  settings. Global capacity follows configured workers times tasks per worker;
  online changes update the queue shared by existing handles.

Cancellation ownership lives in `tikv_util::yatp_pool::fair_queue`. The queue
registry owns Records, permits weakly reference Records, and a Record weakly
references the task's `FutureSlot`. A slot owns a boxed pinned future behind a
per-task mutex. Cancel uses `try_lock`, never waits for a running poll, and
drops extracted futures outside locks to break IO-waker retention cycles.
Do not replace this with a strong Record-to-future ownership cycle or run
future destructors under the scheduler lock.

YATP may call `FairQueue::pop` inside `park.validate` while holding a
`parking_lot` bucket lock. Releasing the scheduler lock does not release that
outer lock. `pop` must not invoke the provider, task-history helpers, cancellation,
or task/waker destructors. Retired queued entries move directly to the cached
default group's lane, retaining their Record, original live-capacity ownership,
MLFQ level/history, and schedule time. They rejoin priority/VT selection before
dispatch. A canceled entry is cleaned up by its cancellation owner or runner
outside the parking callback.

Implementation and validation details are in
[`resource-control-read-pool.md`](../../design/resource-control-read-pool.md).

## Noisy-Group Selection

Detection runs once per quota-worker tick in `online_adjust_resource_quota_at`,
before `adjust_group_throttling`. The verdict is a set of group names used by
foreground write admission's per-group rate limit: -15% per engaged tick and
+10% per tick on recovery. A finite write limiter is the only held state;
there is no read-scheduler Phase actuator or pool-size recovery handshake.
The quota worker uses `normal-control-interval` (10s by default) while quiet
and `overloaded-control-interval` (5s) while loaded.

`reports_noisy_groups` follows only `enable-write-admission-control`.
`noisy_group_names` returns an empty list when this gate is disabled, so the
client can clear an earlier verdict. The deprecated read flags do not enable
reporting. These names retain master's attribution over shared read/write
usage; they are not proof of write-only consumption, nor a verdict from the
new read fair queue. Splitting read and write attribution requires a separate
accounting change. Read-pool busy-threshold decisions must use the actual
queue contract rather than these history-based names.

The fair read queue skips the client's whole-pool FIFO `busy_threshold`
estimate even when no group is classified as noisy: another group's backlog
does not predict this request's position under weighted RG scheduling and
MLFQ. Hard live-task caps, background admission, and the global CPU gate still
apply. Non-RC pools retain the estimate-based rejection. The aggregate wait
gauge remains available as a backlog indicator, not a per-request latency
prediction for the fair queue.

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
- Groups a finite write limiter is already holding stay named — they sit inside their gate
  only because they are held there — and their whole share is credited against
  the target rather than inflating one the innocent tail would be taken to
  meet.

Baselines are *quiet-window* frozen, not rolling: a group's baseline updates
after a full window of quiet samples (CPU below 85% of the foreground
threshold), so the reference does not drift upward during an overload.
Recovery starts below 90% of that threshold, and the cached verdict clears
once pressure has cleared and no finite foreground write limiter remains.

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
each control tick, so a config change lands within one tick. `RuTrackerSlot`
shares the open-bucket atomic counter to avoid its per-group mutex on ordinary
recording. This ledger contains synchronous poll wall time and the fixed
arrival charge, not measured thread CPU. The arrival charge does not add
limiter debt and does not charge the new read scheduler's wall-VT or CPU gate.

## Background Egress Limit (`bg-egress-limit`)

`resource-control.bg-egress-limit` caps the response bytes that background
reads send out per node. It is an `egress_limiter` token bucket on the shared
background `ResourceLimiter`, next to its CPU, IO and write-IO limiters.
`worker.rs::adjust_egress_limit` sets its rate from the config each tick; the
rate is absolute and does not scale with utilization, and 0 means unlimited.

How it is enforced:

- Charged where the response is built: callers pass the same bytes they report
  through `record_network_out_bytes` to
  `resource_limiter.rs::update_background_egress` (unary coprocessor in
  `src/coprocessor/endpoint.rs`, and the transactional KV reads in
  `src/storage/mod.rs`). Charging only builds debt; it never sleeps.
- A coprocessor request with batch tasks is charged once, for the data its
  response returns, when that response is committed: in
  `batch.rs::account_returned_response` when results are merged, and in
  `Endpoint::parse_and_handle_unary_request` when they are not. Its top task and
  children are not charged as they finish, so a later task of the same request
  never waits at admission for the request's own buffered output, and a request
  that times out is not charged for data it never returns.
- Repaid only at background read admission: `ResourceLimiter::admission_delay`
  adds the egress debt when `is_read` and the limiter is background, and
  `src/read_pool.rs::admission_and_enqueue` waits for it before the yatp
  enqueue.
- The admission wait, for egress as for CPU/IO debt, is bounded by the request
  deadline for coprocessor requests (`src/coprocessor/endpoint.rs`) and
  transactional KV reads (the `admission_deadline` of
  `ReadPoolHandle::spawn_handle`). A read whose deadline passes is dropped
  before it enters the pool, releasing its admission delay slot, and fails
  with `DeadlineExceeded`. Raw KV reads are not bounded.

Invariant: egress debt stays out of `ResourceLimiter::consume`. Backup and
ImportSST pay CPU/IO debt through `LimitedFuture` (which calls `consume`), and
background writes are admitted with `is_read = false`, so none of them wait for
egress debt they do not create. Adding egress to `consume` or to write
admission would make a background scan throttle them.

Scope and known gaps:

- A request is background only if its task type is listed in its group's
  `BACKGROUND=(TASK_TYPES=...)`, or in the `default` group's when its group has
  no background settings. Otherwise nothing is charged or paced.
- Streaming coprocessor and raw KV responses are not charged, though those
  reads still wait at admission. Streaming bytes are counted separately.
- Point gets merged from `BatchCommands` (`batch_get_command`) use the limiter
  of the first get for the whole batch (#20112).
- Reads on a legacy (non-unified) read pool are charged but never wait, since
  that pool has no admission gate; `TikvConfig::validate` warns about it.
- It is a soft limit. Admission only sees debt that is already charged and a
  delayed read does not check again, so the short-term excess is about one
  second's worth of the rate plus the responses of the background reads in
  flight.

Signals:

- `tikv_resource_control_background_resource_consumption{type="egress"}`:
  charged bytes, counted even when the limit is 0.
- `tikv_resource_control_background_egress_wait_duration_seconds`: egress debt
  background reads waited for at admission, i.e. whether the limit binds.
- `tikv_resource_control_background_egress_uncharged_bytes_total`: background
  streaming bytes the limit does not cover.
- `tikv_resource_control_background_quota_limiter{type="egress"}` is only the
  configured rate, not evidence that it binds.
- `worker.rs::check_egress_limit_active` warns once when the limit is set and
  no group has any background task type. It stays quiet when some group has
  one, even if the group serving a job lacks its task type; zero egress
  consumption during a background job is the sign of that.

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
- Background egress debt is repaid only at background read admission, never in
  `ResourceLimiter::consume` or write admission (see "Background Egress Limit").

## Observability And Operational Signals

- limiter and scheduling metrics in `metrics.rs`
- logs on PD watch/reload failures, compaction restarts, and config loads
- RU reporting cadence and background-group behavior
- background egress consumption, wait, and uncharged-bytes metrics (see
  "Background Egress Limit")
- noisy-detection policy, live usage, quiet baseline, and finite write-limit
  gauges; these describe the historical write policy independently of read VT
- The process-level `tikv_yatp_fair_pool_*` metrics in
  `tikv_util/src/yatp_pool/metrics.rs` report actual poll CPU/wall time, poll
  duration, CPU-gate Pending attempts, global/group capacity rejections, and
  eviction. They have no RG labels and do not report precharge as consumed CPU.
- `READ_POOL_CPU_VEC` continues publishing current CPU and the configured
  target for RC pools even though their worker count is fixed. Updating these
  measurements does not reintroduce a CPU feedback controller.

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
- Read fairness or metadata-lifecycle changes:
  inspect `resource_group.rs`, `tikv_util::yatp_pool::fair_queue`, and
  `src/read_pool.rs`; test ordinary updates, reload, delete/recreate, fallback,
  and in-flight settlement separately
- Write baseline changes:
  inspect `resource_group.rs`, `resource_limiter.rs`, and storage consumers
- Request admission or delay/reject changes:
  inspect `future.rs`, `channel.rs`, `src/server/service/kv.rs`, and
  `src/storage`
- Background RU reporting changes:
  inspect `worker.rs`, limiter statistics, and PD-facing reporting contracts
- Background egress changes:
  inspect `resource_limiter.rs`, `worker.rs`, read admission, and response
  accounting in coprocessor/storage; never move this debt into write admission
- Noisy feedback or arrival-accounting changes:
  inspect `resource_group.rs`, `config.rs`, RPC handler calls to
  `consume_penalty`, and client health reporting; deprecated read flags must
  remain inert
- Config knob changes:
  inspect `config.rs`, runtime update sites, metrics, and reviewer-facing docs
- Pressure-scoring changes (`score.rs`):
  inspect both consumers — background quota adjustment in `worker.rs` and
  foreground write throttling in `resource_group.rs` — since they share
  the same `cpu_score`/`io_score`/`compaction_score` computation
- Read provider or admission changes:
  inspect `resource_group.rs` and `src/read_pool.rs`; update this guide's
  "Cross-Component Contract" section and the design document together

## Review Checklist

- Does the change affect `watch_resource_groups`, reload, or retry behavior?
- Does it preserve read policy identity on update/reload and retire it on
  deletion? Does it change priority ordering or write baseline logic?
- Does it change request rejection versus delay behavior?
- Does it change accounting units or resource-cost interpretation?
- Does it create lock contention in the hot path?
- Does it update metrics and tests for new scheduling outcomes?
- Does it touch `score.rs`? If so, does it affect both the background
  (`worker.rs`) and foreground write (`resource_group.rs`) consumers as
  intended?
- Does read admission still bypass foreground historical debt without changing
  write/background delay behavior?
- Does it preserve the distinction between the new read policy weight and the
  legacy inverse-quota factor used by other pool controllers?

## Observability And Tests

- Metrics live under `metrics.rs` and within the group/limiter code.
- Many unit tests are inline in:
  `future.rs`, `service.rs`, `worker.rs`, `channel.rs`, `resource_limiter.rs`,
  `score.rs`, and — for detection, throttling and the read-pool contract —
  `resource_group.rs`.
- Changes should usually be validated together with the call sites in
  `src/server`, `src/storage`, `components/batch-system`, and (for
  read-provider / admission changes) `src/read_pool.rs`.

## Common Failure Modes

- watcher stalls after compaction or transient PD errors
- starvation due to incorrect priority or baseline math
- too-aggressive shedding that impacts non-target traffic
- silently stale background-limiter reporting
- dynamic-config changes that fail to take effect operationally
- transient `/proc` read failures in `ThreadGroupCpuTracker` corrupting the
  next tick's CPU delta baseline (see `score.rs`)
- replacing a read policy handle during a PD reload and accidentally granting
  fresh queue progress
- clearing shared foreground limiter debt when disabling read admission,
  thereby changing write admission

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
  Unified Read Pool" above for the API surface it depends on here

## Glossary

- RU:
  request unit used for resource accounting
- Baseline:
  shared historical usage reference for foreground write admission
- Admission control:
  delay or reject logic under pressure
- Virtual time:
  scheduling progress notion used to compare group fairness state
- Resource score (`cpu_score`/`io_score`/`compaction_score`):
  0-100 utilization percentages from `score::compute_resource_scores`, shared
  by background quota adjustment and foreground write throttling
- Pressure fraction:
  a score mapped to `[0, 1]` via `score::pressure_fraction` against a
  caller-supplied `(start, end)` threshold range for background throttling
- Read group generation:
  a stable scheduler identity spanning ordinary policy updates; deletion
  retires it and a later create obtains a new identity

## Related Components

- `src/server/service/kv.rs` consumes resource-group context at the RPC edge.
- `src/storage` uses resource-control metadata and limiters during scheduling.
- `src/read_pool.rs` consumes stable read policies and background admission
  helpers from `ResourceGroupManager`; its queue owns read fairness and CPU
  admission.
- `components/batch-system` integrates with priority-aware execution.
