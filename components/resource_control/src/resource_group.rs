// Copyright 2022 TiKV Project Authors. Licensed under Apache-2.0.

use std::{
    cell::Cell,
    cmp::{max, min},
    collections::HashSet,
    sync::{
        Arc, LockResult, Mutex, MutexGuard,
        atomic::{AtomicBool, AtomicI64, AtomicU64, Ordering},
    },
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use collections::HashMap;
use dashmap::{DashMap, mapref::one::Ref};
use fail::fail_point;
use kvproto::{
    kvrpcpb::{CommandPri, ResourceControlContext},
    resource_manager::{GroupMode, ResourceGroup as PbResourceGroup},
};
use parking_lot::{MappedRwLockReadGuard, RwLock, RwLockReadGuard};
use tikv_util::{
    config::VersionTrack,
    info,
    resource_control::{DEFAULT_RESOURCE_GROUP_NAME, TaskMetadata, TaskPriority},
    time::Instant,
};
use yatp::queue::priority::TaskPriorityProvider;

use crate::{
    config::{Config, NoisyDetection},
    metrics,
    metrics::{TWO_PHASE_THROTTLED_REQUESTS, deregister_metrics},
    resource_limiter::{ResourceLimiter, ResourceType},
    score::PEAK_CPU_PCT,
};

// a read task cost at least 50us.
const DEFAULT_PRIORITY_PER_READ_TASK: u64 = 50;
// extra task schedule factor
const TASK_EXTRA_FACTOR_BY_LEVEL: [u64; 3] = [0, 20, 100];
/// duration to update the minimal priority value of each resource group.
pub const MIN_PRIORITY_UPDATE_INTERVAL: Duration = Duration::from_secs(1);
/// default value of max RU quota.
const DEFAULT_MAX_RU_QUOTA: u64 = 10_000;
/// The maximum RU quota that can be configured.
const MAX_RU_QUOTA: u64 = i32::MAX as u64;

#[cfg(test)]
const LOW_PRIORITY: u32 = 1;
const MEDIUM_PRIORITY: u32 = 8;
#[cfg(test)]
const HIGH_PRIORITY: u32 = 16;

// the global maximum of virtual time is u64::MAX / 16, so when the virtual
// time of all groups are bigger than half of this value, we rest them to avoid
// virtual time overflow.
const RESET_VT_THRESHOLD: u64 = (u64::MAX >> 4) / 2;

/// Period of both control loops: the quota worker and the unified read pool's
/// thread ladder. Independent timers, so the same period but unsynchronised
/// phase — the read pool acts on a verdict up to one tick stale. Every `*_PCT`
/// below is per this tick.
pub const CONTROL_TICK: Duration = Duration::from_secs(10);

/// Margin below a setpoint before a controller trusts there is room to expand:
/// `measured < LEEWAY_FACTOR * setpoint`. Shared by the foreground throttle's
/// release gate, the read pool's scale-up permission and scale-out band, and
/// the background idle gate.
const LEEWAY_PCT: f64 = 10.0;
/// The margin itself. Only the scale-*in* band wants it in this form, as
/// `1 + LEEWAY_FRACTION`; everything comparing downward wants
/// [`LEEWAY_FACTOR`].
pub const LEEWAY_FRACTION: f64 = LEEWAY_PCT / 100.0;
pub const LEEWAY_FACTOR: f64 = 1.0 - LEEWAY_FRACTION;

/// How far below `fg_cpu_throttle_threshold` the node must sit for a group's
/// sliding average to be trusted as its baseline. Above that mark the baseline
/// is left alone, so it holds the last quiet reading for the whole episode.
/// Stricter than [`LEEWAY_FACTOR`]: that decides when to hand capacity back,
/// this decides when a sample is representative.
const BASELINE_QUIET_PCT: f64 = 15.0;
const BASELINE_QUIET_FACTOR: f64 = 1.0 - BASELINE_QUIET_PCT / 100.0;
// Leeway decides when it is safe to hand capacity back; the quiet gate decides
// when a sample is representative of normal behaviour. A reference taken at the
// release point would already include the run-up to the episode, so the quiet
// gate has to stay the stricter of the two -- checked here so retuning either
// percentage above cannot silently invert them.
//
// `assertions_on_constants` reads this as `assert!(true)` to be optimized out;
// in a `const` item there is nothing to optimize out, the evaluation *is* the
// check.
#[allow(clippy::assertions_on_constants)]
const _: () = assert!(BASELINE_QUIET_FACTOR < LEEWAY_FACTOR);

/// Per-tick step that tightens an enforced rate while pressure is engaged: the
/// per-group CPU limit and the read pool's CPU ceiling. Larger than the
/// increase step on purpose — react fast, restore slowly — so one engaged tick
/// undoes about 1.7 recovery ticks. The background budget has no equivalent;
/// it interpolates on pressure instead of stepping.
const THROTTLE_DECREASE_PCT: f64 = 15.0;

/// Per-tick step that gives an enforced rate back once pressure clears. Held
/// at 10% so the ramp can finish: ~5 ticks to reach the `2x` baseline that
/// lifts the limit, and any engaged tick in between resets that progress.
const THROTTLE_INCREASE_PCT: f64 = 10.0;

const THROTTLE_DECREASE_FACTOR: f64 = 1.0 - THROTTLE_DECREASE_PCT / 100.0;
pub(crate) const THROTTLE_INCREASE_FACTOR: f64 = 1.0 + THROTTLE_INCREASE_PCT / 100.0;

/// Consecutive loaded ticks a group must be over its own burst target before
/// `select_noisy_groups` will blame it. The read pool needs no equivalent: its
/// ceiling moves `THROTTLE_DECREASE_PCT` per tick, which is already gradual.
const MIN_ENGAGE_TICKS: u32 = 2;

/// A candidate whose excess is under this share of the worst offender's is
/// tail, not cause. A ratio, so it holds at any scale.
const TAIL_EXCESS_RATIO: f64 = 0.1;

/// Duration of each bucket in the RuTracker ring buffer.
const RU_BUCKET_SECS: u64 = 30;

/// Floor the read pool's CPU ceiling never ratchets below, in cores. The
/// counterpart of the 1 RU/s floor a throttled group gets: without a sampled
/// quiet floor the target is only `THROTTLE_DECREASE_PCT` below current, which
/// compounds toward zero and would leave the pool unable to serve anything.
const MIN_READ_POOL_TARGET_CORES: f64 = 1.0;

/// Sliding-window RU consumption tracker for both Tier-1 admission control
/// and two-phase scheduling phase decisions.
///
/// Tracks actual CPU µs consumed (via `consume_penalty`) across reads and
/// writes in a configurable ring buffer of 30-second buckets. Window size is
/// set from `historical_usage_window_mins` (× 2 buckets per minute). Using real
/// RU rather than virtual
/// time avoids weight-skewing: a high-weight group accumulates VT faster
/// without necessarily consuming more CPU.
pub struct RuTracker {
    /// Ring buffer of completed 30-second bucket totals (oldest at `head`).
    buckets: Vec<u64>,
    /// RU accumulated in the currently-open (incomplete) bucket.
    /// Atomic, and shared with the enclosing [`RuTrackerSlot`], so that
    /// `record_ru_consumption` can add without taking the Mutex.
    current_bucket: Arc<AtomicU64>,
    /// RU/s over the trailing 30-60 seconds, refreshed once per tick. See
    /// [`Self::current_rate`].
    cached_current_rate: f64,
    /// Unix seconds at which the current bucket started.
    bucket_start_secs: u64,
    /// Index of the oldest completed bucket.
    head: usize,
    /// Number of completed buckets (≤ buckets.len()).
    completed: usize,
    /// Cached historical rate (RU/s), updated by `online_adjust_resource_quota`
    /// every ~10s.
    cached_historical_rate: f64,
    /// Consecutive ramp-up epochs where new_limit >= 2x hist. Must reach
    /// MIN_RAMP_UP_EPOCHS before the limit is fully lifted to INFINITY.
    ramp_up_epochs: u32,
    /// `cached_historical_rate` as of the last quiet window, in RU/s. The
    /// clamp-down paths use it; the paths that let go use the live value,
    /// which has climbed with the load and so keeps release conservative.
    /// `None` until the first quiet window.
    quiet_baseline: Option<f64>,
    /// True while this group is deprioritized in the read scheduler.
    scheduler_backpressure: bool,
    /// Consecutive ticks over this group's own burst target, saturating at
    /// `MIN_ENGAGE_TICKS`. See [`Self::sustained_over_baseline`].
    over_baseline_ticks: u32,
    /// When the current run of quiet ticks began, or `u64::MAX` if the last
    /// tick was not quiet. See [`Self::refresh_quiet_baseline`].
    quiet_since_secs: u64,
}

impl RuTracker {
    /// Create a new tracker with `num_buckets` 30-second slots.
    pub fn new(now_secs: u64, num_buckets: usize) -> Self {
        let num_buckets = num_buckets.max(2); // need at least 2 to warm up
        Self {
            buckets: vec![0u64; num_buckets],
            current_bucket: Arc::new(AtomicU64::new(0)),
            cached_current_rate: 0.0,
            bucket_start_secs: now_secs,
            head: 0,
            completed: 0,
            cached_historical_rate: 0.0,
            ramp_up_epochs: 0,
            quiet_baseline: None,
            scheduler_backpressure: false,
            over_baseline_ticks: 0,
            quiet_since_secs: u64::MAX,
        }
    }

    pub fn now_secs() -> u64 {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs()
    }

    #[inline]
    fn num_buckets(&self) -> usize {
        self.buckets.len()
    }

    /// The open-bucket counter, so a holder can add to it without reaching
    /// through the Mutex. Only [`RuTrackerSlot::new`] needs this.
    fn open_bucket_counter(&self) -> Arc<AtomicU64> {
        self.current_bucket.clone()
    }

    /// Record `ru` in the current bucket (lock-free atomic add).
    pub fn record(&self, ru: u64) {
        self.current_bucket.fetch_add(ru, Ordering::Relaxed);
    }

    /// Advance to `now_secs` and record `ru`. Used by tests and callers
    /// that hold exclusive access.
    #[cfg(test)]
    pub fn is_warmed_up(&self) -> bool {
        self.completed >= 2
    }

    #[cfg(test)]
    pub fn record_at(&mut self, ru: u64, now_secs: u64) {
        self.advance(now_secs);
        self.record(ru);
    }

    fn advance(&mut self, now_secs: u64) {
        let n = self.num_buckets();
        let elapsed = now_secs.saturating_sub(self.bucket_start_secs);
        let buckets_to_advance = (elapsed / RU_BUCKET_SECS) as usize;
        if buckets_to_advance == 0 {
            return;
        }
        if buckets_to_advance >= n {
            // Gap larger than the window: everything aged out.
            self.buckets.iter_mut().for_each(|b| *b = 0);
            self.head = 0;
            self.completed = 0;
            self.current_bucket.store(0, Ordering::Relaxed);
        } else {
            // Commit the current bucket to the ring, then zero the rest.
            let write_pos = (self.head + self.completed) % n;
            self.buckets[write_pos] = self.current_bucket.swap(0, Ordering::Relaxed);
            if self.completed < n {
                self.completed += 1;
            } else {
                self.head = (self.head + 1) % n;
            }
            for _ in 1..buckets_to_advance {
                let slot = (self.head + self.completed) % n;
                self.buckets[slot] = 0;
                if self.completed < n {
                    self.completed += 1;
                } else {
                    self.head = (self.head + 1) % n;
                }
            }
        }
        self.bucket_start_secs += buckets_to_advance as u64 * RU_BUCKET_SECS;
    }

    /// RU/s over the trailing 30-60 seconds, sampled by
    /// [`Self::refresh_cached_current_rate`]. A getter, so either clock that
    /// reaches `select_noisy_groups` may call it freely; the read pool's sees
    /// it one tick stale, as it already does the baseline.
    #[inline]
    pub fn current_rate(&self) -> f64 {
        self.cached_current_rate
    }

    /// The newest closed bucket plus the one still filling, over the time the
    /// two actually cover. The window slides between 30 and 60 seconds and
    /// always ends at `now`, so a single tick of traffic cannot swing the
    /// verdict, and traffic that has not yet closed a bucket is still visible.
    ///
    /// Idempotent, unlike the per-tick delta this replaced: a second caller in
    /// the same tick reads the same value rather than a consumed one.
    pub fn refresh_cached_current_rate(&mut self, now_secs: u64) {
        let open = self.current_bucket.load(Ordering::Relaxed);
        // Clamped because a caller that has not advanced the ring would
        // otherwise divide by a window the buckets do not cover.
        let open_secs = now_secs
            .saturating_sub(self.bucket_start_secs)
            .min(RU_BUCKET_SECS);
        self.cached_current_rate = if self.completed == 0 {
            // No closed bucket yet: measure the open one alone rather than
            // report zero, so a group's first traffic is not invisible.
            open as f64 / open_secs.max(1) as f64
        } else {
            let n = self.num_buckets();
            let closed = self.buckets[(self.head + self.completed - 1) % n];
            (closed + open) as f64 / (RU_BUCKET_SECS + open_secs) as f64
        };
    }

    /// Average RU/s baseline. Once the system has been up longer than the
    /// window, always divides by the full window rather than by the completed
    /// buckets, so a tracker that just started has historical = 0 and any
    /// traffic reads as over baseline instead of inflating its own.
    pub fn historical_rate(&self, system_start_secs: u64, now_secs: u64) -> f64 {
        let window_secs = self.num_buckets() as u64 * RU_BUCKET_SECS;
        let system_uptime = now_secs.saturating_sub(system_start_secs);
        if system_uptime >= window_secs {
            // System older than window — use full window as denominator.
            // Missing buckets count as zero, diluting any fresh traffic.
            if self.completed == 0 {
                return 0.0;
            }
            let total: u64 = self.buckets.iter().take(self.completed).sum();
            total as f64 / (self.num_buckets() as f64 * RU_BUCKET_SECS as f64)
        } else {
            if self.completed == 0 {
                return 0.0;
            }
            let total: u64 = self.buckets.iter().take(self.completed).sum();
            // Divide by elapsed system uptime (not just filled buckets) so the
            // historical rate ramps up smoothly rather than jumping immediately
            // to the full rate after the first bucket completes.
            let denom = (system_uptime as f64).max(RU_BUCKET_SECS as f64);
            total as f64 / denom
        }
    }

    /// Returns true if no RU has been recorded: all completed buckets and the
    /// current bucket are zero. Used to garbage-collect stale ru_trackers
    /// entries.
    pub fn is_idle(&self) -> bool {
        self.current_bucket.load(Ordering::Relaxed) == 0 && self.buckets.iter().all(|&b| b == 0)
    }

    /// Takes the reference the clamp-down paths use, but only after a full
    /// window of quiet ticks: above the gate the sliding average climbs with
    /// the load it is used to judge, and for a window after an episode the
    /// ring buffer still holds it, so a sample would come back inflated.
    ///
    /// A cold tracker is skipped: a zero here is indistinguishable from never
    /// having had a quiet window, and the next quiet tick freezes a real value.
    fn refresh_quiet_baseline(&mut self, quiet: bool, now: u64) {
        if !quiet {
            self.quiet_since_secs = u64::MAX;
            return;
        }
        if self.quiet_since_secs == u64::MAX {
            self.quiet_since_secs = now;
        }
        if now.saturating_sub(self.quiet_since_secs) >= self.window_secs()
            && self.cached_historical_rate > 0.0
        {
            self.quiet_baseline = Some(self.cached_historical_rate);
        }
    }

    /// Baseline the eligibility gate compares against: the last quiet reading,
    /// or zero until there has been one. Zero, not the live sliding average --
    /// that average climbs with the load it is being used to judge, so a group
    /// ramping into an overload measures itself against its own spike and
    /// stays inside the gate. Judged on raw usage instead, it ranks by what it
    /// is actually consuming, which is what the biggest-mover ranking needs.
    /// Also what `GROUP_RU_BASELINE` reports, so the panel cannot disagree
    /// with the gate.
    fn effective_baseline(&self) -> f64 {
        self.quiet_baseline.unwrap_or(0.0)
    }

    /// Whether this group is over its own burst target right now.
    fn is_over_burst_target(&self, burst_factor: f64) -> bool {
        let current = self.current_rate();
        current > 0.0 && current > self.effective_baseline() * burst_factor
    }

    /// Advances the candidacy counter, once per tick for every group. Load
    /// matters as well as excess: on excess alone the counter sat saturated,
    /// since a group above its trailing average stays there for minutes.
    ///
    /// `cleared`, not `!loaded`, is what wipes it — a score hovering on the
    /// threshold would otherwise erase the evidence every other tick. A group
    /// inside its own target still resets outright: that is evidence about the
    /// group, not the node. Background reaching its floor is deliberately not a
    /// condition; it gates whether an actuator may fire, not who is at fault.
    fn update_over_baseline_ticks(&mut self, burst_factor: f64, loaded: bool, cleared: bool) {
        if !self.is_over_burst_target(burst_factor) || cleared {
            self.over_baseline_ticks = 0;
        } else if loaded {
            self.over_baseline_ticks = self
                .over_baseline_ticks
                .saturating_add(1)
                .min(MIN_ENGAGE_TICKS);
        }
    }

    /// Whether this group has been over its burst target long enough to be
    /// blamed. One sample is not evidence.
    fn sustained_over_baseline(&self) -> bool {
        self.over_baseline_ticks >= MIN_ENGAGE_TICKS
    }

    /// Records whether a CPU rate limit is applied to this group.
    /// Records whether this group is deprioritized in the read scheduler.
    fn set_scheduler_backpressure(&mut self, on: bool) {
        self.scheduler_backpressure = on;
    }

    /// Span the ring buffer covers, in seconds: also how long an episode takes
    /// to age out of it.
    fn window_secs(&self) -> u64 {
        self.num_buckets() as u64 * RU_BUCKET_SECS
    }

    /// Refresh the cached historical rate. Called periodically from
    /// `online_adjust_resource_quota` (~every 10s) so the inline hot path
    /// avoids iterating all buckets.
    pub fn refresh_cached_historical_rate(&mut self, system_start_secs: u64, now_secs: u64) {
        self.cached_historical_rate = self.historical_rate(system_start_secs, now_secs);
    }
}

/// Encodes a two-phase scheduling priority.
///
/// Bit position of the phase bit within the 64-bit encoded priority.
/// Format: `[4-bit group_priority | 1-bit phase | 59-bit tag]`
const PRIORITY_TAG_BITS: u32 = 59;

/// Whether a task is within or over its historical RU baseline.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum PriorityPhase {
    /// Within baseline — given scheduling preference over phase-1 tasks.
    WithinBaseline = 0,
    /// Over baseline — deprioritised relative to within-baseline tasks.
    OverBaseline = 1,
}

/// Format: `[4-bit group_priority | 1-bit phase | 59-bit tag]`
///
/// Phase 0 (within baseline) always sorts before phase 1 (over baseline)
/// within the same group priority tier. Using only 1 bit for phase leaves
/// 59 bits for the VT tag, matching `RESET_VT_THRESHOLD` (`2^59`).
fn encode_two_phase_priority(group_priority: u32, phase: PriorityPhase, tag: u64) -> u64 {
    assert!((1..=16).contains(&group_priority));
    let tag = tag & ((1u64 << PRIORITY_TAG_BITS) - 1);
    let phase_bit = (phase as u64) << PRIORITY_TAG_BITS;
    (!((group_priority - 1) as u64) << 60) | phase_bit | tag
}

/// Returns true if the encoded priority value represents a phase-1 task.
#[inline]
fn is_phase1(priority: u64) -> bool {
    (priority >> PRIORITY_TAG_BITS) & 1 == 1
}
pub enum ResourceConsumeType {
    CpuTime(Duration),
    IoBytes(u64),
}

/// Result of the Tier-1 admission control check for a single request.
#[derive(Debug, PartialEq)]
pub enum AdmissionDecision {
    /// No throttling needed; let the request proceed immediately.
    Allow,
    /// Delay the request by the given duration. The caller **must** call
    /// [`ResourceGroupManager::release_delay_slot`] once the delay is over
    /// (or the delayed future is dropped/cancelled).
    Delay(Duration),
    /// Reject the request outright (SchedTooBusy). Returned when the number
    /// of concurrently delayed requests exceeds `admission_max_delayed_count`.
    Reject,
}

/// RAII guard that releases an admission-control delay slot on drop.
/// Ensures the `delayed_req_count` counter is decremented even if the
/// future is cancelled during the sleep.
pub struct DelaySlotGuard {
    mgr: Option<Arc<ResourceGroupManager>>,
}

impl DelaySlotGuard {
    fn new(mgr: Arc<ResourceGroupManager>) -> Self {
        Self { mgr: Some(mgr) }
    }

    /// Explicitly release the slot and disarm the guard so Drop is a no-op.
    pub fn release(&mut self) {
        if let Some(mgr) = self.mgr.take() {
            mgr.release_delay_slot();
        }
    }
}

impl Drop for DelaySlotGuard {
    fn drop(&mut self) {
        self.release();
    }
}

/// ResourceGroupManager manages the metadata of each resource group.
/// A group's sliding-window tracker together with its foreground limiter.
///
/// `open_bucket` is the tracker's own open-bucket counter, held out here so
/// `record_ru_consumption` can add to it with a single atomic and no lock --
/// which is what making that counter atomic was always for. The Mutex is
/// still what the tick takes to advance buckets and read the derived rates.
pub(crate) struct RuTrackerSlot {
    open_bucket: Arc<AtomicU64>,
    inner: Mutex<(RuTracker, Arc<ResourceLimiter>)>,
}

impl RuTrackerSlot {
    fn new(tracker: RuTracker, limiter: Arc<ResourceLimiter>) -> Self {
        Self {
            open_bucket: tracker.open_bucket_counter(),
            inner: Mutex::new((tracker, limiter)),
        }
    }

    /// Add to the open bucket without taking the lock.
    fn record(&self, ru: u64) {
        self.open_bucket.fetch_add(ru, Ordering::Relaxed);
    }

    /// Deliberately the same shape as locking the Mutex directly, so every
    /// caller that needs the tracker itself reads as it did before the
    /// counter was lifted out.
    fn lock(&self) -> LockResult<MutexGuard<'_, (RuTracker, Arc<ResourceLimiter>)>> {
        self.inner.lock()
    }

    fn get_mut(&mut self) -> LockResult<&mut (RuTracker, Arc<ResourceLimiter>)> {
        self.inner.get_mut()
    }
}

pub struct ResourceGroupManager {
    pub(crate) resource_groups: DashMap<String, ResourceGroup>,
    // the count of all groups, a fast path because call `DashMap::len` is a little slower.
    group_count: AtomicU64,
    registry: RwLock<Vec<Arc<ResourceController>>>,
    // the shared resource limiter of each priority
    priority_limiters: [Arc<ResourceLimiter>; TaskPriority::PRIORITY_COUNT],
    bg_limiter: Arc<ResourceLimiter>,
    // cached: true when at least one group has background settings configured.
    has_background: AtomicBool,
    // lastest config.
    config: Arc<VersionTrack<Config>>,
    // Per-group sliding-window tracker and token-bucket limiter. The Arc
    // allows handing out limiter references to LimitedFuture wrappers in the
    // read/write pools without copying the limiter state.
    ru_trackers: DashMap<String, RuTrackerSlot>,
    // Number of requests currently held in the admission-control delay phase.
    delayed_req_count: AtomicI64,
    // Unix seconds when this manager was created. Used to determine whether
    // the system has been up long enough that new trackers should be treated
    // as if they missed the entire historical window (baseline = 0).
    start_secs: u64,
    // True when background CPU budget is at the minimum floor (1 core) AND
    // background consumption is within that floor. Foreground throttling
    // only engages when this flag is set, ensuring background is fully
    // squeezed before foreground traffic is touched.
    bg_cpu_at_floor: AtomicBool,
    // Whether foreground CPU pressure is engaged, refreshed every tick by
    // `online_adjust_resource_quota`. A plain 1.0/0.0 rather than a varying
    // fraction, since the read pool only tests it against zero to drive its
    // thread-count scale-down. Encoded via `f64::to_bits`.
    read_pool_cpu_pressure: AtomicU64,
    // `Config::request_base_cost_micros`, mirrored out of the config lock.
    // Read once per request on the gRPC handler path, where taking a shared
    // RwLock read on every gRPC thread is itself the contention. Refreshed by
    // `refresh_cached_config`.
    request_base_cost_micros: AtomicU64,
    // Sliding-window tracker of the unified read pool's actual CPU usage (in
    // µs of CPU time per tick). Its `quiet_baseline` is the floor: the pool
    // should never be scaled below the thread count needed to sustain what it
    // sustained while the node was quiet.
    read_pool_cpu_tracker: Mutex<RuTracker>,
    // True when the last `adjust_group_scheduling` tick saw cpu_score below
    // the leeway threshold, i.e. the system is comfortably idle and the
    // unified read pool may scale its thread count up toward its max.
    read_pool_scale_up_allowed: AtomicBool,
    // The groups the last tick blamed. One writer, both actuators reading, so
    // detection cannot run on two unsynchronised clocks.
    noisy_groups: RwLock<HashSet<String>>,
}

impl Default for ResourceGroupManager {
    fn default() -> Self {
        Self::new(Config::default())
    }
}

/// A group eligible to be blamed this tick.
struct Candidate {
    name: String,
    /// Rate above its own baseline. What identifies the group that *changed*,
    /// so it is what the ranking uses.
    excess: f64,
    /// Whole share, credited against the target when selected.
    current: f64,
}

/// What one pass over the trackers found.
#[derive(Default)]
struct GroupSurvey {
    /// Ranked, biggest mover first.
    candidates: Vec<Candidate>,
    /// Every group's rate, whether eligible or not: the target is a share of
    /// what the node is actually doing.
    total_usage: f64,
    /// Load an actuator is already reclaiming, from every held group.
    relieved: f64,
    /// Groups the actuators are already holding. Still noisy — they are only
    /// inside their gate because they are being held there.
    held: HashSet<String>,
}

impl GroupSurvey {
    /// Takes candidates from the top until the relief they provide covers
    /// `target`, stopping early at the tail cut. Always takes the first unless
    /// something is already held.
    fn take_biggest_movers(&self, target: f64) -> Vec<&Candidate> {
        let tail_floor = self.candidates.first().map_or(0.0, |c| c.excess) * TAIL_EXCESS_RATIO;
        let mut relieved = self.relieved;
        let mut noisy_tenants: Vec<&Candidate> = Vec::new();
        for candidate in &self.candidates {
            let covered = relieved >= target || candidate.excess < tail_floor;
            if (!self.held.is_empty() || !noisy_tenants.is_empty()) && covered {
                break;
            }
            // Already in `relieved`, credited when the survey saw it held.
            if !self.held.contains(&candidate.name) {
                relieved += candidate.current;
            }
            noisy_tenants.push(candidate);
        }
        noisy_tenants
    }
}

impl ResourceGroupManager {
    pub fn new(config: Config) -> Self {
        let priority_limiters = TaskPriority::priorities().map(|p| {
            Arc::new(ResourceLimiter::new(
                p.as_str().to_owned(),
                f64::INFINITY,
                f64::INFINITY,
                0,
                false,
            ))
        });
        let bg_limiter = Arc::new(ResourceLimiter::new(
            DEFAULT_RESOURCE_GROUP_NAME.to_owned(),
            f64::INFINITY,
            f64::INFINITY,
            0,
            true,
        ));
        let start_secs = RuTracker::now_secs();
        // 2 buckets per minute (30s each) to match RU_BUCKET_SECS, mirroring
        // the per-group ru_trackers window sizing in `record_ru_consumption`.
        let read_pool_num_buckets = (config.historical_usage_window_mins.max(2) as usize) * 2;
        let request_base_cost_micros = config.request_base_cost_micros;
        let manager = Self {
            resource_groups: Default::default(),
            group_count: AtomicU64::new(0),
            registry: Default::default(),
            delayed_req_count: AtomicI64::new(0),
            priority_limiters,
            bg_limiter,
            has_background: AtomicBool::new(false),
            config: Arc::new(VersionTrack::new(config)),
            ru_trackers: Default::default(),
            start_secs,
            bg_cpu_at_floor: AtomicBool::new(false),
            read_pool_cpu_pressure: AtomicU64::new(0.0f64.to_bits()),
            request_base_cost_micros: AtomicU64::new(request_base_cost_micros),
            read_pool_cpu_tracker: Mutex::new(RuTracker::new(start_secs, read_pool_num_buckets)),
            read_pool_scale_up_allowed: AtomicBool::new(false),
            noisy_groups: RwLock::new(HashSet::new()),
        };

        // init the default resource group by default.
        let mut default_group = PbResourceGroup::new();
        default_group.name = DEFAULT_RESOURCE_GROUP_NAME.into();
        default_group.priority = MEDIUM_PRIORITY;
        default_group.mode = GroupMode::RuMode;
        default_group
            .mut_r_u_settings()
            .mut_r_u()
            .mut_settings()
            .fill_rate = MAX_RU_QUOTA;
        manager.add_resource_group(default_group);

        manager
    }

    #[inline]
    pub fn get_group_count(&self) -> u64 {
        self.group_count.load(Ordering::Relaxed)
    }

    fn get_ru_setting(rg: &PbResourceGroup, is_read: bool) -> u64 {
        match (rg.get_mode(), is_read) {
            // RU mode, read and write use the same setting.
            (GroupMode::RuMode, _) => rg
                .get_r_u_settings()
                .get_r_u()
                .get_settings()
                .get_fill_rate(),
            // TODO: currently we only consider the cpu usage in the read path, we may also take
            // io read bytes into account later.
            (GroupMode::RawMode, true) => rg
                .get_raw_resource_settings()
                .get_cpu()
                .get_settings()
                .get_fill_rate(),
            (GroupMode::RawMode, false) => rg
                .get_raw_resource_settings()
                .get_io_write()
                .get_settings()
                .get_fill_rate(),
            // return a default value for unsupported config.
            (GroupMode::Unknown, _) => 1,
        }
    }

    pub fn add_resource_group(&self, rg: PbResourceGroup) {
        let group_name = rg.get_name().to_ascii_lowercase();
        self.registry.read().iter().for_each(|controller| {
            let ru_quota = Self::get_ru_setting(&rg, controller.is_read);
            controller.add_resource_group(group_name.clone().into_bytes(), ru_quota, rg.priority);
        });
        info!("add resource group"; "name"=> &rg.name, "ru" => rg.get_r_u_settings().get_r_u().get_settings().get_fill_rate());
        let limiter = self.build_resource_limiter(&rg);

        if self
            .resource_groups
            .insert(group_name, ResourceGroup::new(rg, limiter))
            .is_none()
        {
            self.group_count.fetch_add(1, Ordering::Relaxed);
        }
        self.update_has_background();
    }

    fn update_has_background(&self) {
        let any_has_bg = self
            .resource_groups
            .iter()
            .any(|g| !g.background_source_types.is_empty());
        let prev = self.has_background.swap(any_has_bg, Ordering::Release);
        // When the last background group is removed, reset the shared limiter to
        // unlimited so that a later re-add does not inherit stale throttled rates.
        if prev && !any_has_bg {
            use crate::resource_limiter::ResourceType;
            self.bg_limiter
                .get_limiter(ResourceType::Cpu)
                .set_rate_limit(f64::INFINITY);
            self.bg_limiter
                .get_limiter(ResourceType::Io)
                .set_rate_limit(f64::INFINITY);
            self.bg_limiter
                .get_write_io_limiter()
                .set_rate_limit(f64::INFINITY);
        }
    }

    fn build_resource_limiter(&self, rg: &PbResourceGroup) -> Option<Arc<ResourceLimiter>> {
        if !rg.get_background_settings().get_job_types().is_empty() {
            Some(self.bg_limiter.clone())
        } else {
            None
        }
    }

    pub fn remove_resource_group(&self, name: &str) {
        let group_name = name.to_ascii_lowercase();
        self.registry.read().iter().for_each(|controller| {
            controller.remove_resource_group(group_name.as_bytes());
        });
        if self.resource_groups.remove(&group_name).is_some() {
            info!("remove resource group"; "name"=> name);
            self.group_count.fetch_sub(1, Ordering::Relaxed);
            deregister_metrics(&group_name);
        }
        self.update_has_background();
    }

    pub fn retain(&self, mut f: impl FnMut(&String, &PbResourceGroup) -> bool) {
        let mut removed_names = vec![];
        self.resource_groups.retain(|k, v| {
            // avoid remove default group.
            if k == DEFAULT_RESOURCE_GROUP_NAME {
                return true;
            }
            let ret = f(k, &v.group);
            if !ret {
                removed_names.push(k.clone());
            }
            ret
        });
        if !removed_names.is_empty() {
            self.registry.read().iter().for_each(|controller| {
                for name in &removed_names {
                    controller.remove_resource_group(name.as_bytes());
                }
            });
            self.group_count
                .fetch_sub(removed_names.len() as u64, Ordering::Relaxed);
            self.update_has_background();
        }
    }

    pub(crate) fn get_resource_group(&self, name: &str) -> Option<Ref<'_, String, ResourceGroup>> {
        self.resource_groups.get(&name.to_ascii_lowercase())
    }

    pub fn get_config(&self) -> &Arc<VersionTrack<Config>> {
        &self.config
    }

    pub fn get_all_resource_groups(&self) -> Vec<PbResourceGroup> {
        self.resource_groups
            .iter()
            .map(|g| g.group.clone())
            .collect()
    }

    pub fn derive_controller(&self, name: String, is_read: bool) -> Arc<ResourceController> {
        let controller = Arc::new(ResourceController::new(name, is_read, self.config.clone()));
        self.registry.write().push(controller.clone());
        for g in &self.resource_groups {
            let ru_quota = Self::get_ru_setting(&g.value().group, controller.is_read);
            controller.add_resource_group(g.key().clone().into_bytes(), ru_quota, g.group.priority);
        }
        controller
    }

    pub fn advance_min_virtual_time(&self) {
        for controller in self.registry.read().iter() {
            controller.update_min_virtual_time();
        }
    }

    pub fn consume_penalty(&self, ctx: &ResourceControlContext) {
        for controller in self.registry.read().iter() {
            // FIXME: Should consume CPU time for read controller and write bytes for write
            // controller, once CPU process time of scheduler worker is tracked. Currently,
            // we consume write bytes for read controller as the
            // order of magnitude of CPU time and write bytes is similar.
            controller.consume(
                ctx.resource_group_name.as_bytes(),
                ResourceConsumeType::CpuTime(Duration::from_nanos(
                    (ctx.get_penalty().total_cpu_time_ms * 1_000_000.0) as u64,
                )),
            );
            controller.consume(
                ctx.resource_group_name.as_bytes(),
                ResourceConsumeType::IoBytes(ctx.get_penalty().write_bytes as u64),
            );
        }
        // RU tracking for foreground admission control is handled by
        // LimitedFuture (measure-only mode) which calls record_ru_consumption
        // with actual CPU measured per poll.
        //
        // The fixed arrival cost is charged here because this is the one place
        // that runs exactly once per request, at gRPC handler entry -- before
        // admission control, so a request that gets rejected still pays it.
        self.charge_request_base_cost(&ctx.resource_group_name);
    }

    /// Map a client-supplied group name onto the bounded set of configured
    /// groups, so it is safe to use as a metric label or a map key.
    ///
    /// The name arrives off the wire and is never validated, so an unknown or
    /// empty one has to collapse to the default group. Used directly it would
    /// let a caller mint a new label value -- and so a new permanently
    /// retained metric series, or a new `ru_trackers` entry -- on every
    /// request.
    pub fn bounded_group_name<'a>(&self, group: &'a str) -> &'a str {
        if self.resource_groups.contains_key(group) {
            group
        } else {
            DEFAULT_RESOURCE_GROUP_NAME
        }
    }

    /// Charge `group` the fixed cost of a request arriving, whether or not it
    /// goes on to run. See `Config::request_base_cost_micros`.
    fn charge_request_base_cost(&self, group: &str) {
        let micros = self.request_base_cost_micros.load(Ordering::Relaxed);
        if micros == 0 {
            return;
        }
        self.record_ru_consumption(self.bounded_group_name(group), micros);
    }

    /// Re-read the config values the per-request path keeps cached outside the
    /// config lock. Called from the config dispatcher so a change applies at
    /// once, and again each tick so a config written straight through
    /// `VersionTrack` -- tests, and any future path that bypasses the
    /// dispatcher -- cannot leave the cache stale.
    /// The cached arrival cost, for asserting the cache tracks the config.
    #[cfg(test)]
    pub fn cached_request_base_cost_micros(&self) -> u64 {
        self.request_base_cost_micros.load(Ordering::Relaxed)
    }

    pub fn refresh_cached_config(&self) {
        self.request_base_cost_micros.store(
            self.config.value().request_base_cost_micros,
            Ordering::Relaxed,
        );
    }

    /// Record `ru` units consumed by `group` into the sliding-window tracker
    /// and consume tokens from the group's rate limiter.
    pub fn record_ru_consumption(&self, group: &str, ru: u64) {
        // Fast path: a shared shard read and a single atomic add. `entry()`
        // below takes the shard exclusively even when the key is already
        // there, and needs an owned key to do it, so on the hot default group
        // -- one key, every gRPC thread -- it would serialize this.
        if let Some(entry) = self.ru_trackers.get(group) {
            entry.record(ru);
            return;
        }
        let entry = self.ru_trackers.entry(group.to_owned()).or_insert_with(|| {
            // 2 buckets per minute (30s each) to match RU_BUCKET_SECS.
            let num_buckets =
                (self.config.value().historical_usage_window_mins.max(2) as usize) * 2;
            RuTrackerSlot::new(
                RuTracker::new(RuTracker::now_secs(), num_buckets),
                Arc::new(ResourceLimiter::new(
                    group.to_owned(),
                    f64::INFINITY,
                    f64::INFINITY,
                    0,
                    false,
                )),
            )
        });
        // advance() is called separately by online_adjust_resource_quota
        // under the lock.
        entry.record(ru);
    }

    /// Called by the background adjust worker after computing the new
    /// background CPU budget. `at_floor` should be true when the budget has
    /// been clamped to the minimum floor AND background consumption is within
    /// that floor.
    pub fn set_bg_cpu_at_floor(&self, at_floor: bool) {
        self.bg_cpu_at_floor.store(at_floor, Ordering::Relaxed);
    }

    /// Returns true when background CPU is fully throttled (at floor)
    /// and its consumption is within the floor budget.
    pub fn is_bg_cpu_at_floor(&self) -> bool {
        self.bg_cpu_at_floor.load(Ordering::Relaxed)
    }

    fn set_read_pool_cpu_pressure(&self, pressure: f64) {
        self.read_pool_cpu_pressure
            .store(pressure.to_bits(), Ordering::Relaxed);
    }

    /// Whether foreground CPU pressure is engaged, as 1.0 or 0.0, refreshed
    /// every tick by `online_adjust_resource_quota`.
    pub fn read_pool_cpu_pressure(&self) -> f64 {
        f64::from_bits(self.read_pool_cpu_pressure.load(Ordering::Relaxed))
    }

    /// Quiet-window floor in cores, or `None` before there has been one. The
    /// pool's floor is the same mechanism as a group's baseline, so it is the
    /// same field on the pool's own tracker.
    fn quiet_read_pool_floor(&self) -> Option<f64> {
        self.read_pool_cpu_tracker
            .lock()
            .unwrap()
            .quiet_baseline
            .map(|ru| ru / 1_000_000.0)
    }

    fn refresh_quiet_read_pool_floor(&self, quiet: bool, now: u64) {
        self.read_pool_cpu_tracker
            .lock()
            .unwrap()
            .refresh_quiet_baseline(quiet, now);
    }

    /// Records `read_pool_cpu` (in cores) into the historical tracker and
    /// returns the resulting live average, in cores. Not a floor: the floor is
    /// `quiet_read_pool_floor`, and the live average is deliberately not used
    /// as one -- see `compute_read_pool_target_cpu_at`.
    fn record_read_pool_cpu_at(&self, read_pool_cpu: f64, interval_secs: f64, now: u64) -> f64 {
        let mut tracker = self.read_pool_cpu_tracker.lock().unwrap();
        tracker.advance(now);
        if interval_secs > 0.0 {
            let cpu_us = (read_pool_cpu * interval_secs * 1_000_000.0).max(0.0) as u64;
            tracker.record(cpu_us);
        }
        tracker.refresh_cached_historical_rate(self.start_secs, now);
        tracker.cached_historical_rate / 1_000_000.0
    }

    /// `cpu_score` is the common CPU-utilization score (0-100) computed by
    /// [`crate::score::compute_resource_scores`]: the max of process and grpc
    /// normalized utilization.
    pub fn online_adjust_resource_quota(&self, cpu_score: f64) {
        self.online_adjust_resource_quota_at(cpu_score, RuTracker::now_secs());
    }

    /// One tick: measure, decide, then actuate. Detection lives here rather
    /// than inside either actuator, so it runs exactly once per tick against
    /// one set of measurements, and both actuators act on the same verdict.
    fn online_adjust_resource_quota_at(&self, cpu_score: f64, now: u64) {
        self.refresh_cached_config();
        // Three bands of the same score. Above the threshold is evidence,
        // below the leeway threshold clears it, and between them the candidacy
        // counter holds.
        let threshold = self.config.value().fg_cpu_throttle_threshold;
        let loaded = cpu_score > threshold;
        let cleared = cpu_score < threshold * LEEWAY_FACTOR;
        let quiet = cpu_score < threshold * BASELINE_QUIET_FACTOR;

        // A group over its own average is no problem on an idle node, so both
        // actuators need this, and neither debounces it further. Background
        // yielding first gates the actuators only — see
        // `update_over_baseline_ticks`.
        let under_pressure = loaded && self.is_bg_cpu_at_floor();

        self.refresh_trackers(now, loaded, cleared, quiet);
        self.evict_idle_trackers();
        // Written, never cleared here. The read pool reads this on its own
        // clock, driven by `busy_cpu_scale_in`, which does not track
        // `under_pressure` — wiping it on a quiet tick would leave that clock
        // without a verdict mid-ratchet. `reset_group_priorities` clears it.
        if under_pressure {
            *self.noisy_groups.write() = self.select_noisy_groups();
        }

        self.adjust_group_throttling(cpu_score, under_pressure);
        self.adjust_group_scheduling_at(cpu_score, under_pressure, now);
    }

    /// Brings every tracker up to `now` and samples it. Must precede
    /// detection, which reads what this leaves behind.
    fn refresh_trackers(&self, now: u64, loaded: bool, cleared: bool, quiet: bool) {
        let cfg = self.config.value();
        let burst_factor = 1.0 + cfg.baseline_burst_pct / 100.0;
        let usage_based = matches!(cfg.noisy_detection, NoisyDetection::CurrentUsage);
        for entry in &self.ru_trackers {
            let mut guard = entry.lock().unwrap();
            guard.0.advance(now);
            guard.0.refresh_cached_historical_rate(self.start_secs, now);
            // The one place these are sampled, so the read pool's clock can
            // neither consume the rate delta nor double-count the ticks.
            guard.0.refresh_cached_current_rate(now);
            if usage_based {
                // Judge every group on what it is consuming now. A zero
                // baseline is all that takes: the eligibility gate reduces to
                // "has traffic", the candidate ranking is by `excess`, which
                // becomes the raw rate, and the candidacy counter's target
                // becomes zero. Nothing else has to know about the mode.
                guard.0.quiet_baseline = Some(0.0);
            } else {
                // Only the branch above ever writes a zero here —
                // `refresh_quiet_baseline` requires a positive average — so a
                // zero is a leftover from `current-usage` and has to be
                // dropped. Left in place, a switch back to `baseline` would
                // read as a sampled baseline of zero until a full quiet window
                // has passed, which on a busy node may be never.
                if guard.0.quiet_baseline == Some(0.0) {
                    guard.0.quiet_baseline = None;
                }
                guard.0.refresh_quiet_baseline(quiet, now);
            }
            guard
                .0
                .update_over_baseline_ticks(burst_factor, loaded, cleared);
            let name = guard.1.name();

            metrics::GROUP_RU_HISTORICAL_RATE
                .with_label_values(&[name])
                .set((guard.0.cached_historical_rate / 1_000_000.0) * 100.0);
            metrics::GROUP_RU_CURRENT_RATE
                .with_label_values(&[name])
                .set((guard.0.current_rate() / 1_000_000.0) * 100.0);
            // Same `unwrap_or(0.0)` as `effective_baseline`, so the panel
            // reports the number the gate actually used.
            metrics::GROUP_RU_BASELINE
                .with_label_values(&[name])
                .set(guard.0.quiet_baseline.unwrap_or(0.0) / 1_000_000.0 * 100.0);
        }
    }

    /// The groups this tick blamed. Read by the actuators; written only by
    /// [`Self::online_adjust_resource_quota_at`].
    fn noisy_groups(&self) -> HashSet<String> {
        self.noisy_groups.read().clone()
    }

    /// Picks the groups responsible for the current overload: the biggest
    /// movers, taken until the relief they provide covers the overshoot.
    fn select_noisy_groups(&self) -> HashSet<String> {
        let cfg = self.config.value();
        let survey = self.survey_groups(1.0 + cfg.baseline_burst_pct / 100.0);
        // Groups already being held stay named. They sit inside their gate
        // only because an actuator is keeping them there, and the callers
        // overwrite this wholesale, so dropping them would release them.
        let mut noisy = survey.held.clone();

        let overshoot_pct = (PEAK_CPU_PCT - cfg.fg_cpu_throttle_threshold).max(0.0);
        let noisy_tenants = survey.take_biggest_movers(survey.total_usage * overshoot_pct / 100.0);

        for tenant in &noisy_tenants {
            noisy.insert(tenant.name.clone());
        }
        noisy
    }

    /// Drops trackers for groups that have gone quiet, so a removed or renamed
    /// group cannot grow the map without bound. Relies on `refresh_trackers`
    /// having just advanced them, which flushes partial buckets.
    fn evict_idle_trackers(&self) {
        self.ru_trackers.retain(|name, entry| {
            let inner = entry.get_mut().unwrap();
            if inner.0.is_idle() {
                metrics::deregister_tracker_gauges(name);
                return false;
            }
            true
        });
    }

    /// One pass over the trackers, reading each group's standing once.
    /// Candidates come back ranked, biggest mover first.
    fn survey_groups(&self, burst_factor: f64) -> GroupSurvey {
        let mut survey = GroupSurvey::default();
        for entry in &self.ru_trackers {
            let guard = entry.lock().unwrap();
            let baseline = guard.0.effective_baseline();
            let current = guard.0.current_rate();
            // A finite CPU rate limit *is* throttle backpressure, so read it
            // off the limiter rather than a mirror of it that is a tick stale.
            let throttled = guard
                .1
                .get_limiter(ResourceType::Cpu)
                .get_rate_limit()
                .is_finite();
            let held = throttled || guard.0.scheduler_backpressure;
            let sustained = guard.0.sustained_over_baseline();
            drop(guard);

            if held {
                survey.held.insert(entry.key().clone());
                // An actuator is already reclaiming this load, so it counts
                // toward the target rather than inflating one the innocent
                // tail would be taken to meet. Credited here rather than in
                // the arm below, because whether the group is still above its
                // gate says nothing about the relief already in flight, and
                // the ranking would otherwise only credit it at its own
                // position -- after anything with a larger excess had been
                // taken. `take_biggest_movers` skips held candidates so this
                // is not counted twice.
                survey.relieved += current;
            }
            survey.total_usage += current;
            // No history means any traffic is over baseline. Excluding those
            // groups hid the culprit during its ramp.
            let over_gate = current > 0.0 && current > baseline * burst_factor;
            if over_gate && sustained {
                survey.candidates.push(Candidate {
                    name: entry.key().clone(),
                    excess: current - baseline,
                    current,
                });
            }
        }
        survey
            .candidates
            .sort_unstable_by(|a, b| b.excess.total_cmp(&a.excess));
        survey
    }

    /// Per-group CPU rate-limit throttling. Only the noisy groups picked by
    /// [`Self::select_noisy_groups`] are limited; a group over its own
    /// baseline that is not among the biggest movers is left alone.
    ///
    /// Ramp-up: once CPU drops below the leeway threshold, recover one step
    /// per tick until the limit is infinite again.
    fn adjust_group_throttling(&self, cpu_score: f64, under_pressure: bool) {
        const MIN_RAMP_UP_EPOCHS: u32 = 2;

        let throttle_threshold = self.config.value().fg_cpu_throttle_threshold;
        let leeway_threshold = throttle_threshold * LEEWAY_FACTOR;
        let burst_factor = 1.0 + self.config.value().baseline_burst_pct / 100.0;

        // Live pressure, not the cache being non-empty: the cache outlives the
        // episode and says only *whom* to act on.
        if under_pressure {
            for name in self.noisy_groups() {
                // Gone if its tracker was evicted between the tick that named
                // it and now.
                let Some(entry) = self.ru_trackers.get(&name) else {
                    continue;
                };
                let mut guard = entry.lock().unwrap();
                // The quiet-tick baseline, so the throttle floor cannot drift
                // up with the load being shed.
                let hist = guard.0.effective_baseline();
                // No zero special case: a zero baseline is a target of zero,
                // not a reason to skip. A group with no history of its own,
                // and every group under `current-usage` detection, is
                // throttled on this same path -- the target simply stops
                // clamping the decrease, so a named group keeps being cut
                // while it stays named.
                let burst_target = hist * burst_factor;
                let current_limit = guard.1.get_limiter(ResourceType::Cpu).get_rate_limit();
                let base = if current_limit.is_infinite() {
                    guard.0.current_rate()
                } else {
                    current_limit
                };
                if base > burst_target {
                    let rate = (base * THROTTLE_DECREASE_FACTOR).max(burst_target);
                    guard.0.ramp_up_epochs = 0;
                    guard
                        .1
                        .get_limiter(ResourceType::Cpu)
                        .set_rate_limit(rate.max(1.0));
                }
            }
        }

        // One pass over every group, not just the named ones: a group that
        // has left the verdict still needs its capacity handed back. Skipping
        // it would strand a finite limit and freeze its baseline for good.
        let recovering = !under_pressure && cpu_score < leeway_threshold;
        for entry in &self.ru_trackers {
            let mut guard = entry.lock().unwrap();
            // Ramp up one step per tick, lifting to INFINITY only after
            // MIN_RAMP_UP_EPOCHS epochs past 2x hist so release is not
            // premature. Against the live average, not the quiet baseline:
            // recovery hands capacity back relative to what it consumes now,
            // and the live one has climbed, which keeps release conservative.
            let current_limit = guard.1.get_limiter(ResourceType::Cpu).get_rate_limit();
            if recovering && current_limit.is_finite() {
                let hist = guard.0.cached_historical_rate;
                let new_limit = current_limit * THROTTLE_INCREASE_FACTOR;
                // No zero special case: at `hist == 0` the comparison is
                // already true, so a zero takes the same path as any other
                // value.
                if new_limit >= 2.0 * hist {
                    guard.0.ramp_up_epochs += 1;
                    if guard.0.ramp_up_epochs >= MIN_RAMP_UP_EPOCHS {
                        guard.0.ramp_up_epochs = 0;
                        guard
                            .1
                            .get_limiter(ResourceType::Cpu)
                            .set_rate_limit(f64::INFINITY);
                    }
                } else {
                    guard.0.ramp_up_epochs = 0;
                    guard
                        .1
                        .get_limiter(ResourceType::Cpu)
                        .set_rate_limit(new_limit);
                }
            }

            let limit = guard.1.get_limiter(ResourceType::Cpu).get_rate_limit();
            let val = if limit.is_finite() {
                (limit / 1_000_000.0) * 100.0
            } else {
                0.0
            };
            metrics::GROUP_QUOTA_LIMIT_VEC
                .with_label_values(&[guard.1.name(), "cpu"])
                .set(val);
        }
    }

    fn adjust_group_scheduling_at(&self, cpu_score: f64, engaged: bool, now: u64) {
        let throttle_threshold = self.config.value().fg_cpu_throttle_threshold;
        let leeway_threshold = throttle_threshold * LEEWAY_FACTOR;

        // Reset to 0 whenever foreground is not under CPU pressure, so a
        // transient spike cannot pin the read pool down indefinitely.
        self.set_read_pool_cpu_pressure(if engaged { 1.0 } else { 0.0 });

        self.refresh_quiet_read_pool_floor(
            cpu_score < throttle_threshold * BASELINE_QUIET_FACTOR,
            now,
        );

        // Comfortably idle (below leeway) → allow the read pool to scale its
        // thread count back up toward its max on the next tick it checks in.
        self.read_pool_scale_up_allowed
            .store(cpu_score < leeway_threshold, Ordering::Relaxed);
    }

    /// Marks the noisy resource groups picked by
    /// [`Self::select_noisy_groups`] as over-quota (phase 1), deprioritizing
    /// them. A group within its baseline — or over it but not among the
    /// biggest movers — is left untouched.
    ///
    /// Only ever sets the flag, never clears it: the release signal is the
    /// unified read pool recovering to `core_thread_count`, which this path
    /// cannot see, so clearing is the caller's job via
    /// [`Self::reset_group_priorities`].
    pub fn deprioritize_over_quota_groups(&self) {
        if !self.config.value().enable_fair_scheduling {
            return;
        }
        // The quota-worker tick decided this; recomputing here would run
        // detection on a second, unsynchronised clock.
        for name in self.noisy_groups() {
            for controller in self.registry.read().iter() {
                controller.set_group_phase(name.as_bytes(), true);
            }
            if let Some(entry) = self.ru_trackers.get(&name) {
                entry.lock().unwrap().0.set_scheduler_backpressure(true);
            }
        }
    }

    /// Resets every tracked resource group's two-phase priority back to
    /// phase 0 (not over-quota). Called once the unified read pool has
    /// scaled back up to `core_thread_count`, releasing any groups that
    /// [`Self::deprioritize_over_quota_groups`] had deprioritized.
    pub fn reset_group_priorities(&self) {
        for controller in self.registry.read().iter() {
            controller.reset_all_group_phases();
        }
        self.noisy_groups.write().clear();
        for entry in &self.ru_trackers {
            entry.lock().unwrap().0.set_scheduler_backpressure(false);
        }
    }

    /// The unified read pool's target CPU in cores, while foreground pressure
    /// is engaged: one `THROTTLE_DECREASE_PCT` step below the measured
    /// `read_pool_cpu`, floored at the quiet-window baseline so it never drops
    /// below what the pool sustained while the node was quiet, and never below
    /// `MIN_READ_POOL_TARGET_CORES` whether or not there is one. Returns
    /// `f64::INFINITY` once pressure clears, so callers that `min()` it into
    /// their own ceiling get that ceiling back unaffected. Also records
    /// `read_pool_cpu`, keeping the floor current.
    pub fn compute_read_pool_target_cpu(&self, read_pool_cpu: f64, interval_secs: f64) -> f64 {
        self.compute_read_pool_target_cpu_at(read_pool_cpu, interval_secs, RuTracker::now_secs())
    }

    fn compute_read_pool_target_cpu_at(
        &self,
        read_pool_cpu: f64,
        interval_secs: f64,
        now: u64,
    ) -> f64 {
        let historical_cpu = self.record_read_pool_cpu_at(read_pool_cpu, interval_secs, now);
        // The quiet-tick floor, or a fixed one core until there has been a
        // quiet tick -- not the live average, which keeps recording the
        // overload and so rises in step with the load being shed, floating the
        // floor up under the ratchet. Same rule as a group's baseline, and
        // `MIN_READ_POOL_TARGET_CORES` is what stops the unfloored ratchet
        // compounding to zero.
        let quiet_cpu = self.quiet_read_pool_floor();
        let floor_cpu = quiet_cpu.unwrap_or(0.0).max(MIN_READ_POOL_TARGET_CORES);

        metrics::READ_POOL_CPU_VEC
            .with_label_values(&["historical"])
            .set(historical_cpu * 100.0);
        metrics::READ_POOL_CPU_VEC
            .with_label_values(&["baseline"])
            .set(quiet_cpu.unwrap_or(0.0) * 100.0);
        metrics::READ_POOL_CPU_VEC
            .with_label_values(&["current"])
            .set(read_pool_cpu * 100.0);

        // The floor and metrics above are kept current either way, so
        // re-enabling fair scheduling doesn't start from a stale floor; only
        // the ceiling itself is gated.
        if self.config.value().enable_fair_scheduling && self.read_pool_cpu_pressure() > 0.0 {
            (read_pool_cpu * THROTTLE_DECREASE_FACTOR).max(floor_cpu)
        } else {
            f64::INFINITY
        }
    }

    /// True when the system was comfortably idle (foreground CPU below the
    /// leeway threshold) on the last `adjust_group_scheduling` tick, i.e. the
    /// unified read pool may scale its thread count up toward its max.
    pub fn read_pool_scale_up_allowed(&self) -> bool {
        // Without fair scheduling there is nothing holding the pool back.
        !self.config.value().enable_fair_scheduling
            || self.read_pool_scale_up_allowed.load(Ordering::Relaxed)
    }

    /// Returns the token-bucket debt delay for `group`, or `None` if no
    /// throttling is active.
    ///
    /// Conditions for a non-zero delay (all must hold):
    ///   1. `enable_read_admission_control` / `enable_write_admission_control`
    ///      on
    ///   2. Rate-limit is active (finite)
    ///   3. Tracker has warmed up (≥2 completed 30s buckets)
    ///   4. Token-bucket has accumulated debt (group exceeded its allowed rate)
    pub fn compute_admission_delay(
        &self,
        resource_limiter: &ResourceLimiter,
        is_read: bool,
    ) -> Option<Duration> {
        // Always consume tokens so the token-bucket debt stays accurate
        // for scheduling decisions. Only return the delay when admission
        // control is enabled (or for background limiters, always).
        let delay = resource_limiter.admission_delay(is_read);
        if !resource_limiter.is_background() {
            let config = self.config.value();
            let ac_enabled = if is_read {
                config.enable_read_admission_control
            } else {
                config.enable_write_admission_control
            };
            if !ac_enabled {
                return None;
            }
        }
        if delay.is_zero() { None } else { Some(delay) }
    }

    /// Unified admission-control decision for a request from `group`.
    ///
    /// Combines token-bucket rate-limiter debt (`resource_limiter`) with
    /// RU-baseline overage delay into a single pre-pool sleep duration.
    /// Covers background, low-priority, and resource-group throttling for
    /// both reads (`is_read=true`) and writes (`is_read=false`).
    ///
    /// Returns:
    /// - [`AdmissionDecision::Allow`] — no throttling needed.
    /// - [`AdmissionDecision::Delay(d)`] — caller should sleep `d` then
    ///   proceed, and **must** call [`release_delay_slot`] afterwards.
    /// - [`AdmissionDecision::Reject`] — too many requests are already delayed;
    ///   reject immediately.
    pub fn admission_decision(
        &self,
        is_read: bool,
        resource_limiter: &ResourceLimiter,
    ) -> AdmissionDecision {
        let group = resource_limiter.name();
        let delay = self
            .compute_admission_delay(resource_limiter, is_read)
            .unwrap_or(std::time::Duration::ZERO);
        if delay.is_zero() {
            return AdmissionDecision::Allow;
        }
        let metric_label = if resource_limiter.is_background() {
            "background"
        } else {
            group
        };
        let max = self.config.value().admission_max_delayed_count;
        let prev = self.delayed_req_count.fetch_add(1, Ordering::Relaxed);
        metrics::ADMISSION_CURRENTLY_DELAYED.set(prev + 1);
        if max > 0 && prev >= max as i64 {
            self.delayed_req_count.fetch_sub(1, Ordering::Relaxed);
            metrics::ADMISSION_CURRENTLY_DELAYED.set(prev);
            crate::metrics::ADMISSION_REJECTED_REQUESTS
                .with_label_values(&[metric_label])
                .inc();
            return AdmissionDecision::Reject;
        }
        crate::metrics::ADMISSION_DELAYED_REQUESTS
            .with_label_values(&[metric_label])
            .inc();
        crate::metrics::ADMISSION_DELAY_DURATION
            .with_label_values(&[metric_label])
            .observe(delay.as_secs_f64());
        AdmissionDecision::Delay(delay)
    }

    /// Release a delay slot acquired by [`admission_decision`] returning
    /// [`AdmissionDecision::Delay`]. Must be called exactly once per `Delay`
    /// decision, whether the request completes normally or is cancelled.
    pub fn release_delay_slot(&self) {
        let prev = self.delayed_req_count.fetch_sub(1, Ordering::Relaxed);
        metrics::ADMISSION_CURRENTLY_DELAYED.set(prev - 1);
    }

    /// Returns an RAII guard that calls [`release_delay_slot`] on drop.
    /// Use this to ensure the slot is released even if the future is
    /// cancelled during the admission-control sleep.
    pub fn delay_slot_guard(self: &Arc<Self>) -> DelaySlotGuard {
        DelaySlotGuard::new(Arc::clone(self))
    }

    /// Returns the per-group admission-control limiter for `group`, creating
    /// the entry if it does not yet exist. Used by `with_resource_limiter`
    /// (measure-only mode) in the read/write pools so `LimitedFuture` can
    /// build token-bucket debt for pre-pool `admission_decision`.
    pub fn get_foreground_group_limiter(&self, group: &str) -> Arc<ResourceLimiter> {
        let now = RuTracker::now_secs();
        // 2 buckets per minute (30s each) to match RU_BUCKET_SECS.
        let num_buckets = (self.config.value().historical_usage_window_mins.max(2) as usize) * 2;
        self.ru_trackers
            .entry(group.to_owned())
            .or_insert_with(|| {
                RuTrackerSlot::new(
                    RuTracker::new(now, num_buckets),
                    Arc::new(ResourceLimiter::new(
                        group.to_owned(),
                        f64::INFINITY,
                        f64::INFINITY,
                        0,
                        false,
                    )),
                )
            })
            .lock()
            .unwrap()
            .1
            .clone()
    }

    /// Returns the appropriate `ResourceLimiter` for `with_resource_limiter`:
    /// - Background tasks → background limiter
    /// - Foreground tasks (any priority) → per-group limiter from `ru_trackers`
    pub fn get_resource_limiter(
        &self,
        rg: &str,
        request_source: &str,
        _override_priority: u64,
    ) -> Option<Arc<ResourceLimiter>> {
        let (limiter, _) = self.get_background_resource_limiter_with_priority(rg, request_source);
        if limiter.is_some() {
            return limiter;
        }
        // Only create a foreground limiter for known groups; unknown or removed
        // groups fall back to "default" to avoid leaking ru_trackers entries.
        Some(self.get_foreground_group_limiter(self.bounded_group_name(rg)))
    }

    // return a ResourceLimiter for background tasks only.
    // Returns None if request_source does not match a configured background job
    // type.
    pub fn get_background_resource_limiter(
        &self,
        rg: &str,
        request_source: &str,
    ) -> Option<Arc<ResourceLimiter>> {
        if request_source.is_empty() {
            return None;
        }
        self.get_background_resource_limiter_with_priority(rg, request_source)
            .0
    }

    fn get_background_resource_limiter_with_priority(
        &self,
        rg: &str,
        request_source: &str,
    ) -> (Option<Arc<ResourceLimiter>>, u32) {
        fail_point!("only_check_source_task_name", |name| {
            assert_eq!(&name.unwrap(), request_source);
            (None, 8)
        });
        let mut group_priority = None;
        if let Some(group) = self.resource_groups.get(rg) {
            group_priority = Some(group.group.priority);
            if !group.fallback_default {
                return (
                    group.get_background_resource_limiter(request_source),
                    group.group.priority,
                );
            }
        }

        let default_group = self
            .resource_groups
            .get(DEFAULT_RESOURCE_GROUP_NAME)
            .unwrap();
        (
            default_group.get_background_resource_limiter(request_source),
            group_priority.unwrap_or(default_group.group.priority),
        )
    }

    #[inline]
    pub fn get_priority_resource_limiters(
        &self,
    ) -> &[Arc<ResourceLimiter>; TaskPriority::PRIORITY_COUNT] {
        &self.priority_limiters
    }

    pub fn get_background_limiter(&self) -> Arc<ResourceLimiter> {
        self.bg_limiter.clone()
    }

    pub fn has_background_groups(&self) -> bool {
        self.has_background.load(Ordering::Acquire)
    }
}

pub(crate) struct ResourceGroup {
    pub group: PbResourceGroup,
    pub limiter: Option<Arc<ResourceLimiter>>,
    background_source_types: HashSet<String>,
    // whether to fallback background resource control to `default` group.
    fallback_default: bool,
}

impl ResourceGroup {
    fn new(group: PbResourceGroup, limiter: Option<Arc<ResourceLimiter>>) -> Self {
        let background_source_types =
            HashSet::from_iter(group.get_background_settings().get_job_types().to_owned());
        let fallback_default =
            !group.has_background_settings() && group.name != DEFAULT_RESOURCE_GROUP_NAME;
        Self {
            group,
            limiter,
            background_source_types,
            fallback_default,
        }
    }

    #[cfg(test)]
    pub fn get_ru_quota(&self) -> u64 {
        self.group
            .get_r_u_settings()
            .get_r_u()
            .get_settings()
            .get_fill_rate()
    }

    fn get_background_resource_limiter(
        &self,
        request_source: &str,
    ) -> Option<Arc<ResourceLimiter>> {
        self.limiter.as_ref().and_then(|limiter| {
            // the source task name is the last part of `request_source` separated by "_"
            // the request_source is
            // {extrenal|internal}_{tidb_req_source}_{source_task_name}
            let source_task_name = request_source.rsplit('_').next().unwrap_or("");
            if !source_task_name.is_empty()
                && self.background_source_types.contains(source_task_name)
            {
                Some(limiter.clone())
            } else {
                None
            }
        })
    }
}

pub struct ResourceController {
    // resource controller name is not used currently.
    #[allow(dead_code)]
    name: String,
    // We handle the priority differently between read and write request:
    // 1. the priority factor is calculated based on read/write RU settings.
    // 2. for read request, we increase a constant virtual time delta at each `get_priority` call
    //    because the cost can't be calculated at start, so we only increase a constant delta and
    //    increase the real cost after task is executed; but don't increase it at write because the
    //    cost is known so we just pre-consume it.
    is_read: bool,
    // Track the maximum ru quota used to calculate the factor of each resource group.
    // factor = max_ru_quota / group_ru_quota * 10.0
    // We use mutex here to ensure when we need to change this value and do adjust all resource
    // groups' factors, it can't be changed concurrently.
    // NOTE: because the ru config for "default" group is very large, and it can cause very big
    // group weight, we will not count this value by default.
    max_ru_quota: Mutex<u64>,
    // record consumption of each resource group, name --> resource_group
    resource_consumptions: RwLock<HashMap<Vec<u8>, GroupPriorityTracker>>,
    // the latest min vt, this value is used to init new added group vt
    last_min_vt: AtomicU64,
    // the last time min vt is overflow
    last_rest_vt_time: Cell<Instant>,
    // whether the settings are customized by user
    customized: AtomicBool,
    // Shared config. Read on the hot path to check enable_fair_scheduling.
    config: Arc<VersionTrack<Config>>,
    // Whether any group is in phase 1, so a reset can skip the sweep.
    any_group_deprioritized: AtomicBool,
}

// we are ensure to visit the `last_rest_vt_time` by only 1 thread so it's
// thread safe.
unsafe impl Send for ResourceController {}
unsafe impl Sync for ResourceController {}

impl ResourceController {
    fn new(name: String, is_read: bool, config: Arc<VersionTrack<Config>>) -> Self {
        Self {
            name,
            is_read,
            resource_consumptions: RwLock::new(HashMap::default()),
            last_min_vt: AtomicU64::new(0),
            max_ru_quota: Mutex::new(DEFAULT_MAX_RU_QUOTA),
            last_rest_vt_time: Cell::new(Instant::now_coarse()),
            customized: AtomicBool::new(false),
            config,
            any_group_deprioritized: AtomicBool::new(false),
        }
    }

    pub fn new_for_test(name: String, is_read: bool) -> Self {
        let controller = Self::new(
            name,
            is_read,
            Arc::new(VersionTrack::new(Config::default())),
        );
        // add the "default" resource group.
        controller.add_resource_group(
            DEFAULT_RESOURCE_GROUP_NAME.as_bytes().to_owned(),
            0,
            MEDIUM_PRIORITY,
        );
        controller
    }

    fn calculate_factor(max_quota: u64, quota: u64) -> u64 {
        // we don't adjust the max_quota if it's the "default" group's default
        // value(u32::MAX), so here it is possible that the quota is bigger than
        // the max quota
        if quota == 0 || quota > max_quota {
            1
        } else {
            // we use max_quota / quota as the resource group factor, but because we need to
            // cast the value to integer, so we times it by 10 to ensure the accuracy is
            // enough.
            let max_quota = min(max_quota * 10, MAX_RU_QUOTA);
            (max_quota as f64 / quota as f64).round() as u64
        }
    }

    fn add_resource_group(&self, name: Vec<u8>, mut ru_quota: u64, mut group_priority: u32) {
        if group_priority == 0 {
            // map 0 to medium priority(default priority)
            group_priority = MEDIUM_PRIORITY;
        }
        if ru_quota > MAX_RU_QUOTA {
            ru_quota = MAX_RU_QUOTA;
        }

        let mut max_ru_quota = self.max_ru_quota.lock().unwrap();
        // skip to adjust max ru if it is the "default" group and the ru config eq
        // MAX_RU_QUOTA
        if ru_quota > *max_ru_quota
            && (name != DEFAULT_RESOURCE_GROUP_NAME.as_bytes() || ru_quota < MAX_RU_QUOTA)
        {
            *max_ru_quota = ru_quota;
            // adjust all group weight because the current value is too small.
            self.adjust_all_resource_group_factors(ru_quota);
        }
        let weight = Self::calculate_factor(*max_ru_quota, ru_quota);

        let vt_delta_for_get = if self.is_read {
            DEFAULT_PRIORITY_PER_READ_TASK * weight
        } else {
            0
        };
        let group = GroupPriorityTracker {
            ru_quota,
            group_priority,
            weight,
            virtual_time: AtomicU64::new(self.last_min_vt.load(Ordering::Acquire)),
            vt_delta_for_get,
            // New groups start in phase 0; ResourceGroupManager updates this
            // each second once the RuTracker has enough history.
            is_over_baseline: AtomicBool::new(false),
        };

        // maybe update existed group
        self.resource_consumptions.write().insert(name, group);
        self.check_customized();
    }

    fn check_customized(&self) {
        let groups = self.resource_consumptions.read();
        if groups.len() == 1 && groups.get(DEFAULT_RESOURCE_GROUP_NAME.as_bytes()).is_some() {
            self.customized.store(false, Ordering::Release);
            return;
        }
        self.customized.store(true, Ordering::Release);
    }

    // we calculate the weight of each resource group based on the currently maximum
    // ru quota, if a incoming resource group has a bigger quota, we need to
    // adjust all the existing groups. As we expect this won't happen very
    // often, and iterate 10k entry cost less than 5ms, so the performance is
    // acceptable.
    fn adjust_all_resource_group_factors(&self, max_ru_quota: u64) {
        self.resource_consumptions
            .write()
            .iter_mut()
            .for_each(|(_, tracker)| {
                tracker.weight = Self::calculate_factor(max_ru_quota, tracker.ru_quota);
            });
    }

    fn remove_resource_group(&self, name: &[u8]) {
        // do not remove the default resource group, reset to default setting instead.
        if DEFAULT_RESOURCE_GROUP_NAME.as_bytes() == name {
            self.add_resource_group(
                DEFAULT_RESOURCE_GROUP_NAME.as_bytes().to_owned(),
                0,
                MEDIUM_PRIORITY,
            );
            self.check_customized();
            return;
        }
        self.resource_consumptions.write().remove(name);
        self.check_customized();
    }

    pub fn is_customized(&self) -> bool {
        self.customized.load(Ordering::Acquire)
    }

    #[inline]
    fn resource_group(&self, name: &[u8]) -> MappedRwLockReadGuard<'_, GroupPriorityTracker> {
        let guard = self.resource_consumptions.read();
        RwLockReadGuard::map(guard, |m| {
            if let Some(g) = m.get(name) {
                g
            } else {
                m.get(DEFAULT_RESOURCE_GROUP_NAME.as_bytes()).unwrap()
            }
        })
    }

    pub fn consume(&self, name: &[u8], resource: ResourceConsumeType) {
        self.resource_group(name).consume(resource)
    }

    /// Updates the two-phase `is_over_baseline` flag for a single group.
    /// Called by `ResourceGroupManager::adjust_group_scheduling`.
    pub fn set_group_phase(&self, group: &[u8], over_baseline: bool) {
        let consumptions = self.resource_consumptions.read();
        if let Some(tracker) = consumptions.get(group) {
            tracker
                .is_over_baseline
                .store(over_baseline, Ordering::Relaxed);
            if over_baseline {
                self.any_group_deprioritized.store(true, Ordering::Relaxed);
            }
        }
    }

    /// Clears every group's phase-1 flag, or does nothing if none is set.
    pub fn reset_all_group_phases(&self) {
        if !self.any_group_deprioritized.swap(false, Ordering::Relaxed) {
            return;
        }
        for tracker in self.resource_consumptions.read().values() {
            tracker.is_over_baseline.store(false, Ordering::Relaxed);
        }
    }

    pub fn update_min_virtual_time(&self) {
        let start = Instant::now_coarse();
        let mut min_vt = u64::MAX;
        let mut max_vt = 0;
        self.resource_consumptions
            .read()
            .iter()
            .for_each(|(_, tracker)| {
                let vt = tracker.current_vt();
                min_vt = min(min_vt, vt);
                max_vt = max(max_vt, vt);
            });

        // TODO: use different threshold for different resource type
        // needn't do update if the virtual different is less than 100ms/100KB.
        if min_vt >= max_vt.saturating_sub(100_000) && max_vt < RESET_VT_THRESHOLD {
            return;
        }

        fail_point!("increase_vt_duration_update_min_vt");

        let near_overflow = min_vt > RESET_VT_THRESHOLD;
        self.resource_consumptions
            .read()
            .iter()
            .for_each(|(_, tracker)| {
                let vt = tracker.current_vt();
                // NOTE: this decrease vt is not atomic across all resource groups,
                // but it should be ok as this operation should be extremely rare
                // and the impact is not big.
                if near_overflow {
                    tracker.decrease_vt(RESET_VT_THRESHOLD);
                } else if vt < max_vt {
                    // TODO: is increase by half is a good choice.
                    tracker.increase_vt((max_vt - vt) / 2);
                }
            });
        if near_overflow {
            let end = Instant::now_coarse();
            info!("all resource groups' virtual time are near overflow, do reset";
                "min" => min_vt, "max" => max_vt, "dur" => ?end.duration_since(start),
                "reset_dur" => ?end.duration_since(self.last_rest_vt_time.get()));
            max_vt -= RESET_VT_THRESHOLD;
            self.last_rest_vt_time.set(end);
        }
        // max_vt is actually a little bigger than the current min vt, but we don't
        // need totally accurate here.
        self.last_min_vt.store(max_vt, Ordering::Relaxed);
    }

    pub fn get_priority(&self, name: &[u8], pri: CommandPri) -> u64 {
        let level = Self::command_pri_to_level(pri);
        self.resource_group(name)
            .get_priority(level, None, true, self.is_two_phase_enabled())
    }

    /// Returns the priority for the given task metadata without incrementing
    /// virtual time. Used for pre-spawn eviction comparison.
    pub fn peek_priority_of(&self, metadata: &TaskMetadata<'_>, pri: CommandPri) -> u64 {
        let level = Self::command_pri_to_level(pri);
        let group = self.resource_group(metadata.group_name());
        let override_priority = if metadata.override_priority() == 0 {
            None
        } else {
            Some(metadata.override_priority())
        };
        group.get_priority(level, override_priority, false, self.is_two_phase_enabled())
    }

    /// Returns true when fair two-phase scheduling is active (read controller
    /// with enable_fair_scheduling enabled).
    #[inline]
    fn is_two_phase_enabled(&self) -> bool {
        self.is_read && self.config.value().enable_fair_scheduling
    }

    fn command_pri_to_level(pri: CommandPri) -> usize {
        match pri {
            CommandPri::High => 0,
            CommandPri::Normal => 1,
            CommandPri::Low => 2,
        }
    }
}

impl TaskPriorityProvider for ResourceController {
    fn priority_of(&self, extras: &yatp::queue::Extras) -> u64 {
        let metadata = TaskMetadata::from(extras.metadata());
        let p = self.resource_group(metadata.group_name()).get_priority(
            extras.current_level() as usize,
            if metadata.override_priority() == 0 {
                None
            } else {
                Some(metadata.override_priority())
            },
            true,
            self.is_two_phase_enabled(),
        );
        if is_phase1(p)
            && let Ok(name) = std::str::from_utf8(metadata.group_name())
        {
            TWO_PHASE_THROTTLED_REQUESTS
                .with_label_values(&[name])
                .inc();
        }
        p
    }
}

fn concat_priority_vt(group_priority: u32, vt: u64) -> u64 {
    assert!((1..=16).contains(&group_priority));

    // map group_priority from [1, 16] to [0, 15] to limit it 4 bits and get bitwise
    // negation to replace leading 4 bits of vt. So that the priority is ordered in
    // the descending order by group_priority first, then by vt in ascending order.
    vt | (!((group_priority - 1) as u64) << 60)
}

struct GroupPriorityTracker {
    // the ru setting of this group.
    ru_quota: u64,
    group_priority: u32,
    weight: u64,
    virtual_time: AtomicU64,
    // the constant delta value for each `get_priority` call,
    vt_delta_for_get: u64,
    // Two-phase scheduling: true when this group's current-minute RU rate
    // exceeds its historical baseline (set by ResourceGroupManager each second).
    // Phase 1 tasks are deprioritised relative to phase-0 (within-baseline) tasks.
    is_over_baseline: AtomicBool,
}

impl GroupPriorityTracker {
    /// Computes the scheduling priority for a task at the given level.
    ///
    /// When `advance_vt` is true, atomically increments the virtual time
    /// (used when actually scheduling a task). When false, reads virtual
    /// time without advancing it (used for priority comparison only).
    ///
    /// When `two_phase` is true and `is_over_baseline` is set, the task is
    /// placed in phase 1 (deprioritised); otherwise it runs in phase 0.
    /// `is_over_baseline` is updated each second by `ResourceGroupManager`
    /// from real CPU µs tracked across reads and writes via `consume_penalty`.
    fn get_priority(
        &self,
        level: usize,
        override_priority: Option<u32>,
        advance_vt: bool,
        two_phase: bool,
    ) -> u64 {
        let task_extra_priority = TASK_EXTRA_FACTOR_BY_LEVEL[level] * 1000 * self.weight;
        let priority = override_priority.unwrap_or(self.group_priority);
        let vt_delta = self.vt_delta_for_get;

        let vt = if advance_vt && vt_delta > 0 {
            self.virtual_time.fetch_add(vt_delta, Ordering::Relaxed) + vt_delta
        } else {
            self.virtual_time.load(Ordering::Relaxed) + vt_delta
        } + task_extra_priority;

        if two_phase && self.is_over_baseline.load(Ordering::Relaxed) {
            encode_two_phase_priority(priority, PriorityPhase::OverBaseline, vt)
        } else if two_phase {
            encode_two_phase_priority(priority, PriorityPhase::WithinBaseline, vt)
        } else {
            concat_priority_vt(priority, vt)
        }
    }

    #[inline]
    fn current_vt(&self) -> u64 {
        self.virtual_time.load(Ordering::Relaxed)
    }

    #[inline]
    fn increase_vt(&self, vt_delta: u64) {
        self.virtual_time.fetch_add(vt_delta, Ordering::Relaxed);
    }

    #[inline]
    fn decrease_vt(&self, vt_delta: u64) {
        self.virtual_time.fetch_sub(vt_delta, Ordering::Relaxed);
    }

    // TODO: make it delta type as generic to avoid mixed consume different types.
    #[inline]
    fn consume(&self, resource: ResourceConsumeType) {
        let vt_delta = match resource {
            ResourceConsumeType::CpuTime(dur) => dur.as_micros() as u64,
            ResourceConsumeType::IoBytes(bytes) => bytes,
        } * self.weight;
        self.increase_vt(vt_delta);
    }
}

#[cfg(test)]
pub(crate) mod tests {
    use file_system::IoBytes;
    use yatp::queue::Extras;

    use super::*;
    use crate::resource_limiter::ResourceType::{Cpu, Io};

    pub fn new_resource_group_ru(name: String, ru: u64, group_priority: u32) -> PbResourceGroup {
        new_resource_group(name, true, ru, ru, group_priority)
    }

    pub fn new_background_resource_group_ru(
        name: String,
        ru: u64,
        group_priority: u32,
        task_types: Vec<String>,
    ) -> PbResourceGroup {
        let mut rg = new_resource_group(name, true, ru, ru, group_priority);
        rg.mut_background_settings()
            .set_job_types(task_types.into());
        rg
    }

    pub fn new_resource_group(
        name: String,
        is_ru_mode: bool,
        read_tokens: u64,
        write_tokens: u64,
        group_priority: u32,
    ) -> PbResourceGroup {
        use kvproto::resource_manager::{GroupRawResourceSettings, GroupRequestUnitSettings};

        let mut group = PbResourceGroup::new();
        group.set_name(name);
        let mode = if is_ru_mode {
            GroupMode::RuMode
        } else {
            GroupMode::RawMode
        };
        group.set_mode(mode);
        group.set_priority(group_priority);
        if is_ru_mode {
            assert!(read_tokens == write_tokens);
            let mut ru_setting = GroupRequestUnitSettings::new();
            ru_setting
                .mut_r_u()
                .mut_settings()
                .set_fill_rate(read_tokens);
            group.set_r_u_settings(ru_setting);
        } else {
            let mut resource_setting = GroupRawResourceSettings::new();
            resource_setting
                .mut_cpu()
                .mut_settings()
                .set_fill_rate(read_tokens);
            resource_setting
                .mut_io_write()
                .mut_settings()
                .set_fill_rate(write_tokens);
            group.set_raw_resource_settings(resource_setting);
        }
        group
    }

    #[test]
    fn test_resource_group() {
        let resource_manager = ResourceGroupManager::default();
        assert_eq!(resource_manager.resource_groups.len(), 1);

        let group1 = new_resource_group_ru("TEST".into(), 100, 0);
        resource_manager.add_resource_group(group1);

        assert!(resource_manager.get_resource_group("test1").is_none());
        let group = resource_manager.get_resource_group("test").unwrap();
        assert_eq!(group.get_ru_quota(), 100);
        drop(group);
        assert_eq!(resource_manager.resource_groups.len(), 2);

        let group1 = new_resource_group_ru("Test".into(), 200, LOW_PRIORITY);
        resource_manager.add_resource_group(group1);
        let group = resource_manager.get_resource_group("test").unwrap();
        assert_eq!(group.get_ru_quota(), 200);
        assert_eq!(group.value().group.get_priority(), 1);
        drop(group);
        assert_eq!(resource_manager.resource_groups.len(), 2);

        let group2 = new_resource_group_ru("test2".into(), 400, 0);
        resource_manager.add_resource_group(group2);
        assert_eq!(resource_manager.resource_groups.len(), 3);

        let resource_ctl = resource_manager.derive_controller("test_read".into(), true);
        assert_eq!(resource_ctl.resource_consumptions.read().len(), 3);

        let group1 = resource_ctl.resource_group(b"test");
        let group2 = resource_ctl.resource_group(b"test2");
        assert_eq!(group1.weight, group2.weight * 2);
        assert_eq!(group1.current_vt(), 0);

        resource_ctl.consume(
            b"test",
            ResourceConsumeType::CpuTime(Duration::from_micros(10000)),
        );
        resource_ctl.consume(
            b"test2",
            ResourceConsumeType::CpuTime(Duration::from_micros(10000)),
        );

        assert_eq!(group1.current_vt(), group1.weight * 10000);
        assert_eq!(group1.current_vt(), group2.current_vt() * 2);

        // test update all group vts
        resource_manager.advance_min_virtual_time();
        let group1_vt = group1.current_vt();
        let group1_weight = group1.weight;
        assert_eq!(group1_vt, group1.weight * 10000);
        assert!(group2.current_vt() >= group1.current_vt() * 3 / 4);
        assert!(resource_ctl.resource_group(b"default").current_vt() >= group1.current_vt() / 2);

        drop(group1);
        drop(group2);

        // test add 1 new resource group
        let new_group = new_resource_group_ru("new_group".into(), 600, HIGH_PRIORITY);
        resource_manager.add_resource_group(new_group);

        assert_eq!(resource_ctl.resource_consumptions.read().len(), 4);
        let group3 = resource_ctl.resource_group("new_group".as_bytes());
        assert!(group1_weight - 10 <= group3.weight * 3 && group3.weight * 3 <= group1_weight + 10);
        assert!(group3.current_vt() >= group1_vt / 2);
        drop(group3);

        // test resource group resource limiter.
        let group1 = resource_manager.get_resource_group("test").unwrap();
        assert!(group1.limiter.is_none());
        assert!(
            resource_manager
                .get_resource_group("default")
                .unwrap()
                .limiter
                .is_none()
        );
        let new_default = new_background_resource_group_ru(
            "default".into(),
            10000,
            MEDIUM_PRIORITY,
            vec!["br".into()],
        );
        resource_manager.add_resource_group(new_default);
        let default_group = resource_manager.get_resource_group("default").unwrap();
        let limiter = default_group.limiter.as_ref().unwrap().clone();
        assert!(limiter.get_limiter(Cpu).get_rate_limit().is_infinite());
        assert!(limiter.get_limiter(Io).get_rate_limit().is_infinite());
        limiter.get_limiter(Cpu).set_rate_limit(100.0);
        limiter.get_limiter(Io).set_rate_limit(200.0);
        drop(group1);
        drop(default_group);

        let new_default = new_background_resource_group_ru(
            "default".into(),
            100,
            LOW_PRIORITY,
            vec!["lightning".into()],
        );
        resource_manager.add_resource_group(new_default);
        let default_group = resource_manager.get_resource_group("default").unwrap();
        assert_eq!(default_group.get_ru_quota(), 100);
        let new_limiter = default_group.limiter.as_ref().unwrap().clone();
        // check rate_limiter is not changed.
        assert_eq!(new_limiter.get_limiter(Cpu).get_rate_limit(), 100.0);
        assert_eq!(new_limiter.get_limiter(Io).get_rate_limit(), 200.0);
        assert_eq!(&*new_limiter as *const _, &*limiter as *const _);
        drop(default_group);

        // remove background setting, quota limiter should be none.
        let new_default = new_resource_group_ru("default".into(), 100, LOW_PRIORITY);
        resource_manager.add_resource_group(new_default);
        assert!(
            resource_manager
                .get_resource_group("default")
                .unwrap()
                .limiter
                .is_none()
        );
    }

    #[test]
    fn test_resource_group_crud() {
        let resource_manager = ResourceGroupManager::default();
        assert_eq!(resource_manager.get_group_count(), 1);

        let group1 = new_resource_group_ru("test1".into(), 100, HIGH_PRIORITY);
        resource_manager.add_resource_group(group1);
        assert_eq!(resource_manager.get_group_count(), 2);

        let group2 = new_resource_group_ru("test2".into(), 200, LOW_PRIORITY);
        resource_manager.add_resource_group(group2);
        assert_eq!(resource_manager.get_group_count(), 3);

        let group1 = new_resource_group_ru("test1".into(), 150, HIGH_PRIORITY);
        resource_manager.add_resource_group(group1.clone());
        assert_eq!(resource_manager.get_group_count(), 3);
        assert_eq!(
            resource_manager.get_resource_group("test1").unwrap().group,
            group1
        );

        resource_manager.remove_resource_group("test2");
        assert!(resource_manager.get_resource_group("test2").is_none());
        assert_eq!(resource_manager.get_group_count(), 2);

        resource_manager.remove_resource_group("test2");
        assert_eq!(resource_manager.get_group_count(), 2);
    }

    #[test]
    fn test_resource_group_priority() {
        let resource_manager = ResourceGroupManager::default();
        let group1 = new_resource_group_ru("test1".into(), 200, LOW_PRIORITY);
        resource_manager.add_resource_group(group1);
        let group2 = new_resource_group_ru("test2".into(), 400, 0);
        resource_manager.add_resource_group(group2);
        assert_eq!(resource_manager.resource_groups.len(), 3);

        let resource_ctl = resource_manager.derive_controller("test".into(), true);

        let group1 = resource_ctl.resource_group("test1".as_bytes());
        let group2 = resource_ctl.resource_group("test2".as_bytes());
        assert_eq!(group1.weight, group2.weight * 2);
        assert_eq!(group1.current_vt(), 0);

        let mut extras1 = Extras::single_level();
        extras1.set_metadata(
            TaskMetadata::from_ctx(&ResourceControlContext {
                resource_group_name: "test1".to_string(),
                override_priority: 0,
                ..Default::default()
            })
            .to_vec(),
        );
        assert_eq!(
            resource_ctl.priority_of(&extras1),
            concat_priority_vt(LOW_PRIORITY, group1.weight * 50)
        );
        assert_eq!(group1.current_vt(), group1.weight * 50);

        let mut extras2 = Extras::single_level();
        extras2.set_metadata(
            TaskMetadata::from_ctx(&ResourceControlContext {
                resource_group_name: "test2".to_string(),
                override_priority: 0,
                ..Default::default()
            })
            .to_vec(),
        );
        assert_eq!(
            resource_ctl.priority_of(&extras2),
            concat_priority_vt(MEDIUM_PRIORITY, group2.weight * 50)
        );
        assert_eq!(group2.current_vt(), group2.weight * 50);

        // test override priority
        let mut extras2_override = Extras::single_level();
        extras2_override.set_metadata(
            TaskMetadata::from_ctx(&ResourceControlContext {
                resource_group_name: "test2".to_string(),
                override_priority: LOW_PRIORITY as u64,
                ..Default::default()
            })
            .to_vec(),
        );
        assert_eq!(
            resource_ctl.priority_of(&extras2_override),
            concat_priority_vt(LOW_PRIORITY, group2.weight * 100)
        );
        assert_eq!(group2.current_vt(), group2.weight * 100);

        let mut extras3 = Extras::single_level();
        extras3.set_metadata(
            TaskMetadata::from_ctx(&ResourceControlContext {
                resource_group_name: "unknown_group".to_string(),
                override_priority: 0,
                ..Default::default()
            })
            .to_vec(),
        );
        assert_eq!(
            resource_ctl.priority_of(&extras3),
            concat_priority_vt(MEDIUM_PRIORITY, 50)
        );
        assert_eq!(
            resource_ctl
                .resource_group("default".as_bytes())
                .current_vt(),
            50
        );
    }

    #[test]
    fn test_reset_resource_group_vt() {
        let resource_manager = ResourceGroupManager::default();
        let resource_ctl = resource_manager.derive_controller("test_write".into(), false);

        let group1 = new_resource_group_ru("g1".into(), i32::MAX as u64, 1);
        resource_manager.add_resource_group(group1);
        let group2 = new_resource_group_ru("g2".into(), 1, 16);
        resource_manager.add_resource_group(group2);

        let g1 = resource_ctl.resource_group(b"g1");
        let g2 = resource_ctl.resource_group(b"g2");
        let threshold = 1 << 59;
        let mut last_g2_vt = 0;
        for i in 0..8 {
            resource_ctl.consume(b"g2", ResourceConsumeType::IoBytes(1 << 25));
            resource_manager.advance_min_virtual_time();
            if i < 7 {
                assert!(g2.current_vt() < threshold);
            }
            // after 8 round, g1's vt still under the threshold and is still increasing.
            assert!(g1.current_vt() < threshold && g1.current_vt() > last_g2_vt);
            last_g2_vt = g2.current_vt();
        }

        resource_ctl.consume(b"g2", ResourceConsumeType::IoBytes(1 << 25));
        resource_manager.advance_min_virtual_time();
        assert!(g1.current_vt() > threshold);

        // adjust again, the virtual time of each group should decrease
        resource_manager.advance_min_virtual_time();
        let g1_vt = g1.current_vt();
        let g2_vt = g2.current_vt();
        assert!(g2_vt < threshold / 2);
        assert!(g1_vt < threshold / 2 && g1_vt < g2_vt);
        assert_eq!(resource_ctl.last_min_vt.load(Ordering::Relaxed), g2_vt);
    }

    #[test]
    fn test_adjust_resource_group_weight() {
        let resource_manager = ResourceGroupManager::default();
        let resource_ctl = resource_manager.derive_controller("test_read".into(), true);
        let resource_ctl_write = resource_manager.derive_controller("test_write".into(), false);
        assert_eq!(resource_ctl.is_customized(), false);
        assert_eq!(resource_ctl_write.is_customized(), false);
        let group1 = new_resource_group_ru("test1".into(), 5000, 0);
        resource_manager.add_resource_group(group1);
        assert_eq!(resource_ctl.resource_group(b"test1").weight, 20);
        assert_eq!(resource_ctl_write.resource_group(b"test1").weight, 20);
        assert_eq!(resource_ctl.is_customized(), true);
        assert_eq!(resource_ctl_write.is_customized(), true);

        // add a resource group with big ru
        let group1 = new_resource_group_ru("test2".into(), 50000, 0);
        resource_manager.add_resource_group(group1);
        assert_eq!(*resource_ctl.max_ru_quota.lock().unwrap(), 50000);
        assert_eq!(resource_ctl.resource_group(b"test1").weight, 100);
        assert_eq!(resource_ctl.resource_group(b"test2").weight, 10);
        // resource_ctl_write should be unchanged.
        assert_eq!(*resource_ctl_write.max_ru_quota.lock().unwrap(), 50000);
        assert_eq!(resource_ctl_write.resource_group(b"test1").weight, 100);
        assert_eq!(resource_ctl_write.resource_group(b"test2").weight, 10);

        // add the default "default" group, the ru weight should not change.
        // add a resource group with big ru
        let group = new_resource_group_ru("default".into(), u32::MAX as u64, 0);
        resource_manager.add_resource_group(group);
        assert_eq!(resource_ctl_write.resource_group(b"test1").weight, 100);
        assert_eq!(resource_ctl_write.resource_group(b"default").weight, 1);

        // change the default group to another value, it can impact the ru then.
        let group = new_resource_group_ru("default".into(), 100000, 0);
        resource_manager.add_resource_group(group);
        assert_eq!(resource_ctl_write.resource_group(b"test1").weight, 200);
        assert_eq!(resource_ctl_write.resource_group(b"default").weight, 10);
    }

    #[test]
    fn test_background_settings_do_not_customize_resource_controller() {
        let resource_manager = ResourceGroupManager::default();
        let resource_ctl = resource_manager.derive_controller("test_write".into(), false);

        let default_group = new_background_resource_group_ru(
            DEFAULT_RESOURCE_GROUP_NAME.into(),
            MAX_RU_QUOTA,
            MEDIUM_PRIORITY,
            vec!["br".into()],
        );
        resource_manager.add_resource_group(default_group);

        assert!(!resource_ctl.is_customized());

        let group = new_resource_group_ru("test".into(), 5000, MEDIUM_PRIORITY);
        resource_manager.add_resource_group(group);
        assert!(resource_ctl.is_customized());

        resource_manager.remove_resource_group("test");
        assert!(!resource_ctl.is_customized());
    }

    #[cfg(feature = "failpoints")]
    #[test]
    fn test_reset_resource_group_vt_overflow() {
        use rand::{RngCore, thread_rng};
        let resource_manager = ResourceGroupManager::default();
        let resource_ctl = resource_manager.derive_controller("test_write".into(), false);
        let mut rng = thread_rng();

        let mut min_delta = u64::MAX;
        let mut max_delta = 0;
        for i in 0..10 {
            let name = format!("g{}", i);
            let g = new_resource_group_ru(name.clone(), 100, 1);
            resource_manager.add_resource_group(g);
            let delta = rng.next_u64() % 10000 + 1;
            min_delta = delta.min(min_delta);
            max_delta = delta.max(max_delta);
            resource_ctl
                .resource_group(name.as_bytes())
                .increase_vt(RESET_VT_THRESHOLD + delta);
        }
        resource_ctl
            .resource_group(b"default")
            .increase_vt(RESET_VT_THRESHOLD + 1);

        let old_max_vt = resource_ctl
            .resource_consumptions
            .read()
            .iter()
            .fold(0, |v, (_, g)| v.max(g.current_vt()));
        let resource_ctl_cloned = resource_ctl.clone();
        fail::cfg_callback("increase_vt_duration_update_min_vt", move || {
            resource_ctl_cloned
                .resource_consumptions
                .read()
                .iter()
                .enumerate()
                .for_each(|(i, (_, tracker))| {
                    if i % 2 == 0 {
                        tracker.increase_vt(max_delta - min_delta);
                    }
                });
        })
        .unwrap();
        resource_ctl.update_min_virtual_time();
        fail::remove("increase_vt_duration_update_min_vt");

        let new_max_vt = resource_ctl
            .resource_consumptions
            .read()
            .iter()
            .fold(0, |v, (_, g)| v.max(g.current_vt()));
        // check all vt has decreased by RESET_VT_THRESHOLD.
        assert!(new_max_vt < max_delta * 2);
        // check fail-point takes effect, the `new_max_vt` has increased.
        assert!(old_max_vt - RESET_VT_THRESHOLD < new_max_vt);
    }

    #[test]
    fn test_retain_resource_groups() {
        let resource_manager = ResourceGroupManager::default();
        let resource_ctl = resource_manager.derive_controller("test_read".into(), true);
        let resource_ctl_write = resource_manager.derive_controller("test_write".into(), false);

        for i in 0..5 {
            let group1 = new_resource_group_ru(format!("test{}", i), 100, 0);
            resource_manager.add_resource_group(group1);
            // add a resource group with big ru
            let group1 = new_resource_group_ru(format!("group{}", i), 100, 0);
            resource_manager.add_resource_group(group1);
        }
        // consume for default group
        resource_ctl.consume(
            b"default",
            ResourceConsumeType::CpuTime(Duration::from_micros(10000)),
        );
        resource_ctl_write.consume(b"default", ResourceConsumeType::IoBytes(10000));

        // 10 + 1(default)
        assert_eq!(resource_manager.get_all_resource_groups().len(), 11);
        assert_eq!(resource_ctl.resource_consumptions.read().len(), 11);
        assert_eq!(resource_ctl_write.resource_consumptions.read().len(), 11);

        resource_manager.retain(|k, _v| k.starts_with("test"));
        assert_eq!(resource_manager.get_all_resource_groups().len(), 6);
        assert_eq!(resource_ctl.resource_consumptions.read().len(), 6);
        assert_eq!(resource_ctl_write.resource_consumptions.read().len(), 6);
        assert!(resource_manager.get_resource_group("group1").is_none());
        // should use the virtual time of default group for non-exist group
        assert_ne!(resource_ctl.resource_group(b"group2").current_vt(), 0);
        assert_ne!(resource_ctl_write.resource_group(b"group2").current_vt(), 0);
    }

    #[test]
    fn test_concat_priority_vt() {
        let v1 = concat_priority_vt(MEDIUM_PRIORITY, 1000);
        let v2 = concat_priority_vt(MEDIUM_PRIORITY, 1111);
        assert!(v1 < v2);

        let v3 = concat_priority_vt(LOW_PRIORITY, 1000);
        assert!(v1 < v3);

        let v4 = concat_priority_vt(MEDIUM_PRIORITY, 1111);
        assert_eq!(v2, v4);

        let v5 = concat_priority_vt(HIGH_PRIORITY, 10);
        assert!(v5 < v1);
    }

    #[test]
    fn test_encode_two_phase_priority_ordering() {
        // Phase 0 (reservation) must sort before phase 1 (weight) for the
        // same group_priority and a larger tag value.
        let phase0 =
            encode_two_phase_priority(MEDIUM_PRIORITY, PriorityPhase::WithinBaseline, 9999);
        let phase1 = encode_two_phase_priority(MEDIUM_PRIORITY, PriorityPhase::OverBaseline, 1);
        assert!(
            phase0 < phase1,
            "phase 0 must be higher priority than phase 1"
        );

        // Within phase 0, lower tag = higher priority.
        let p0_low = encode_two_phase_priority(MEDIUM_PRIORITY, PriorityPhase::WithinBaseline, 100);
        let p0_high =
            encode_two_phase_priority(MEDIUM_PRIORITY, PriorityPhase::WithinBaseline, 200);
        assert!(p0_low < p0_high);

        // group_priority still dominates across phases.
        let high_phase1 =
            encode_two_phase_priority(HIGH_PRIORITY, PriorityPhase::OverBaseline, u64::MAX >> 8);
        let low_phase0 = encode_two_phase_priority(LOW_PRIORITY, PriorityPhase::WithinBaseline, 0);
        assert!(high_phase1 < low_phase0);
    }

    // Inserts a tracker whose current rate is `current` and whose cached
    // baseline is `hist`, both in RU/s.
    fn seed_tracker(mgr: &ResourceGroupManager, name: &str, hist: f64, current: f64, t0: u64) {
        let e = mgr.ru_trackers.entry(name.to_owned()).or_insert_with(|| {
            RuTrackerSlot::new(
                RuTracker::new(t0 - RU_BUCKET_SECS, 30),
                Arc::new(ResourceLimiter::new(
                    name.into(),
                    f64::INFINITY,
                    f64::INFINITY,
                    0,
                    false,
                )),
            )
        });
        let mut tr = e.lock().unwrap();
        // Recorded so the tracker is not idle, and so `retain` keeps it. The
        // two rates are then stated directly, as a tick would have sampled them.
        tr.0.record_at(
            (current * RU_BUCKET_SECS as f64) as u64,
            t0 - RU_BUCKET_SECS,
        );
        tr.0.advance(t0);
        tr.0.cached_historical_rate = hist;
        tr.0.cached_current_rate = current;
        // A group that has been running has had quiet ticks to sample.
        tr.0.quiet_baseline = Some(hist);
        // The fixture states the group is already running at `current`, so it
        // has been over its target for as long as it needs to be blamed.
        tr.0.over_baseline_ticks = MIN_ENGAGE_TICKS;
        assert!((tr.0.current_rate() - current).abs() < 1.0);
    }

    /// States the rate the quota-worker tick would have sampled for `name`.
    /// Needed by tests that reach selection without a tick — as the read pool
    /// does — since that path samples nothing.
    fn set_sampled_rate(mgr: &ResourceGroupManager, name: &str, rate: f64) {
        mgr.ru_trackers
            .get(name)
            .unwrap()
            .lock()
            .unwrap()
            .0
            .cached_current_rate = rate;
    }

    /// Records `ru` into the still-open bucket: traffic the ring has not
    /// absorbed yet, so it raises the current rate while leaving the closed
    /// buckets — and so the baseline — untouched.
    fn stage_open_bucket(mgr: &ResourceGroupManager, name: &str, ru: u64) {
        let entry = mgr.ru_trackers.get(name).unwrap();
        let guard = entry.lock().unwrap();
        guard.0.record(ru);
    }

    /// One tick with the sampled rates stated rather than measured, so a
    /// scenario reads as the story it tests instead of ring-buffer
    /// bookkeeping. Mirrors `online_adjust_resource_quota_at` without the part
    /// of `refresh_trackers` that recomputes the rates, and without the quiet
    /// baseline refresh: no time passes here for a quiet window to elapse.
    fn tick(mgr: &ResourceGroupManager, cpu_score: f64) {
        let cfg = mgr.get_config().value();
        let loaded = cpu_score > cfg.fg_cpu_throttle_threshold;
        let cleared = cpu_score < cfg.fg_cpu_throttle_threshold * LEEWAY_FACTOR;
        let under_pressure = loaded && mgr.is_bg_cpu_at_floor();
        let burst_factor = 1.0 + cfg.baseline_burst_pct / 100.0;
        for entry in &mgr.ru_trackers {
            let mut guard = entry.lock().unwrap();
            guard
                .0
                .update_over_baseline_ticks(burst_factor, loaded, cleared);
        }
        if under_pressure {
            *mgr.noisy_groups.write() = mgr.select_noisy_groups();
        }
        mgr.adjust_group_throttling(cpu_score, under_pressure);
    }

    /// States the pool's sampled quiet floor, in cores.
    fn set_quiet_read_pool_floor(mgr: &ResourceGroupManager, cores: f64) {
        mgr.read_pool_cpu_tracker.lock().unwrap().quiet_baseline = Some(cores * 1_000_000.0);
    }

    fn limit_of(mgr: &ResourceGroupManager, name: &str) -> f64 {
        mgr.ru_trackers
            .get(name)
            .unwrap()
            .lock()
            .unwrap()
            .1
            .get_limiter(ResourceType::Cpu)
            .get_rate_limit()
    }

    fn set_baseline(mgr: &ResourceGroupManager, name: &str, hist: f64) {
        mgr.ru_trackers
            .get(name)
            .unwrap()
            .lock()
            .unwrap()
            .0
            .cached_historical_rate = hist;
    }

    /// Runs detection and stores its verdict as a tick would, but without the
    /// tracker refresh, which would overwrite a staged rate.
    fn stage_noisy(mgr: &ResourceGroupManager) {
        *mgr.noisy_groups.write() = mgr.select_noisy_groups();
    }

    /// Marks `name` as having been over its target for long enough to be
    /// blamed. The quota-worker tick counts this; a test that calls
    /// `deprioritize_over_quota_groups` directly, as the read pool does, has
    /// to state it.
    fn mark_sustained(mgr: &ResourceGroupManager, name: &str) {
        mgr.ru_trackers
            .get(name)
            .unwrap()
            .lock()
            .unwrap()
            .0
            .over_baseline_ticks = MIN_ENGAGE_TICKS;
    }

    /// Ticks the manager until the per-group counter has been satisfied.
    fn tick_until_engaged(mgr: &ResourceGroupManager, cpu_score: f64) {
        for _ in 0..MIN_ENGAGE_TICKS {
            mgr.online_adjust_resource_quota(cpu_score);
        }
    }

    fn baseline_of(mgr: &ResourceGroupManager, name: &str) -> Option<f64> {
        mgr.ru_trackers
            .get(name)
            .unwrap()
            .lock()
            .unwrap()
            .0
            .quiet_baseline
    }

    fn set_backpressure(mgr: &ResourceGroupManager, name: &str, throttle: bool, scheduler: bool) {
        let e = mgr.ru_trackers.get(name).unwrap();
        let mut tr = e.lock().unwrap();
        // Throttle backpressure is the limiter's own state: a finite CPU
        // rate limit is what being throttled means.
        tr.1.get_limiter(ResourceType::Cpu)
            .set_rate_limit(if throttle { 1_000_000.0 } else { f64::INFINITY });
        tr.0.set_scheduler_backpressure(scheduler);
    }

    // seed_tracker builds a 30-bucket tracker, so one window is 15 minutes.
    const TEST_WINDOW_SECS: u64 = 30 * RU_BUCKET_SECS;

    #[test]
    fn test_held_load_counts_toward_the_target_instead_of_inflating_it() {
        // The tikv-50 case at 19:56:30. uds_006's baseline is 783 and it has been
        // throttled down to 464, so 464 > 783 * 1.2 is false and it dropped out
        // of the candidates -- while its 464 still set the target, and default
        // (spiking 25.6 -> 50.8) plus two negligible groups were taken to meet
        // a target none of them could reach.
        let mgr = ResourceGroupManager::new(Config::default());
        let t0 = RuTracker::now_secs();
        seed_tracker(&mgr, "uds_006", 783.0, 464.0, t0);
        seed_tracker(&mgr, "default", 25.6, 50.8, t0);
        seed_tracker(&mgr, "uds_000", 0.1, 0.9, t0);
        seed_tracker(&mgr, "uds_008", 0.0, 0.3, t0);
        // uds_006 is the group already under backpressure.
        set_backpressure(&mgr, "uds_006", true, true);

        let selected = mgr.select_noisy_groups();
        assert!(
            selected.contains("uds_006") && selected.len() == 1,
            "only the group already held is named: the load it is giving back \
             covers the target, so nothing new is taken: {selected:?}"
        );
    }

    #[test]
    fn test_held_group_still_over_its_gate_covers_the_target() {
        // The tikv-24-1b case at 16:12. uds_006 is throttled to its burst
        // target, which is the same 239.8 * 1.2 the gate uses, so it sits on
        // the gate and a one-minute burst to 294 put it over. That took its
        // 294 of already-reclaimed load out of `relieved` and made it a
        // candidate instead -- ranked second, behind default spiking 25.4 ->
        // 118.2 -- so the loop reached default before ever crediting it.
        let mgr = ResourceGroupManager::new(Config::default());
        let t0 = RuTracker::now_secs();
        seed_tracker(&mgr, "uds_006", 239.8, 294.3, t0);
        seed_tracker(&mgr, "default", 25.4, 118.2, t0);
        set_backpressure(&mgr, "uds_006", true, true);

        let selected = mgr.select_noisy_groups();
        assert!(
            selected.contains("uds_006") && selected.len() == 1,
            "the held group covers the target whether or not it is still over \
             its gate, so default is not taken with it: {selected:?}"
        );
    }

    #[test]
    fn test_tail_groups_below_the_head_ratio_are_spared() {
        // Nothing is held and the head cannot reach the target on its own, so
        // the loop used to walk the whole list. Groups an order of magnitude
        // below the worst offender are tail, not cause.
        let mgr = ResourceGroupManager::new(Config::default());
        let t0 = RuTracker::now_secs();
        seed_tracker(&mgr, "quiet", 900.0, 800.0, t0); // under its gate
        seed_tracker(&mgr, "head", 25.6, 50.8, t0); // excess 25.2
        seed_tracker(&mgr, "tail_a", 0.1, 0.9, t0); // excess 0.8
        seed_tracker(&mgr, "tail_b", 0.0, 0.3, t0); // excess 0.3

        let selected = mgr.select_noisy_groups();
        assert_eq!(
            selected.iter().map(String::as_str).collect::<Vec<_>>(),
            vec!["head"],
            "only the head is a plausible cause: {:?}",
            selected
        );
    }

    #[test]
    fn test_ramping_group_with_no_history_is_selected_alone() {
        // The tikv-48 case: the culprit ramped 0 -> 5428 with historical still
        // 0, so it was excluded and every small jittering group was taken.
        let mgr = ResourceGroupManager::new(Config::default());
        let t0 = RuTracker::now_secs();
        seed_tracker(&mgr, "ramping", 0.0, 5428.0, t0);
        seed_tracker(&mgr, "default", 7.40, 9.31, t0);
        seed_tracker(&mgr, "small_a", 0.235, 0.288, t0);
        seed_tracker(&mgr, "small_b", 0.187, 0.225, t0);

        let selected = mgr.select_noisy_groups();
        assert!(selected.contains("ramping"), "{:?}", selected);
        assert_eq!(
            selected.len(),
            1,
            "the ramping group covers the target alone, sparing the rest: {:?}",
            selected
        );
        assert_eq!(baseline_of(&mgr, "ramping"), Some(0.0));
    }

    #[test]
    fn test_zero_baseline_is_frozen_and_keeps_the_group_selected() {
        let mgr = ResourceGroupManager::new(Config::default());
        let t0 = RuTracker::now_secs();
        seed_tracker(&mgr, "fresh", 0.0, 500.0, t0);
        assert!(mgr.select_noisy_groups().contains("fresh"));
        assert_eq!(baseline_of(&mgr, "fresh"), Some(0.0));

        // First selection wins; a later historical does not displace it.
        mgr.ru_trackers
            .get("fresh")
            .unwrap()
            .lock()
            .unwrap()
            .0
            .cached_historical_rate = 100.0;
        assert!(mgr.select_noisy_groups().contains("fresh"));
        assert_eq!(baseline_of(&mgr, "fresh"), Some(0.0));
    }

    #[test]
    fn test_idle_eviction_clears_the_group_gauges() {
        // An evicted tracker's gauges keep reporting their last value -- a
        // stale baseline then reads as still noisy.
        //
        // The gauge registry is global to the test binary, so this group name
        // must not be shared with a test that could run alongside it.
        let mgr = ResourceGroupManager::new(Config::default());
        let t0 = RuTracker::now_secs();
        seed_tracker(&mgr, "evicted", 100.0, 1000.0, t0);
        mgr.select_noisy_groups();
        assert_eq!(baseline_of(&mgr, "evicted"), Some(100.0));
        metrics::GROUP_RU_BASELINE
            .with_label_values(&["evicted"])
            .set(100.0);

        // Traffic stops: advance past the window so every bucket is zero.
        mgr.online_adjust_resource_quota_at(0.0, t0 + 2 * TEST_WINDOW_SECS);

        assert!(
            mgr.ru_trackers.get("evicted").is_none(),
            "an idle tracker should be evicted"
        );
        assert_eq!(
            metrics::GROUP_RU_BASELINE
                .get_metric_with_label_values(&["evicted"])
                .unwrap()
                .get(),
            0.0,
            "the gauge must be dropped on eviction, not left holding its last value"
        );
    }

    #[test]
    fn test_select_noisy_groups_leaves_the_quiet_baseline_alone() {
        // The reference comes from the last quiet tick, so naming a group is
        // not what establishes it and must not disturb it.
        let mgr = ResourceGroupManager::new(Config::default());
        let t0 = RuTracker::now_secs();
        seed_tracker(&mgr, "noisy", 100.0, 1000.0, t0);
        seed_tracker(&mgr, "small", 100.0, 130.0, t0);

        let selected = mgr.select_noisy_groups();
        assert!(
            selected.contains("noisy") && !selected.contains("small"),
            "{selected:?}"
        );

        assert_eq!(baseline_of(&mgr, "noisy"), Some(100.0));
        assert_eq!(baseline_of(&mgr, "small"), Some(100.0));
    }

    #[test]
    fn test_named_group_survives_baseline_drift() {
        // The failure this prevents: the live baseline absorbs the spike, the
        // gate stops matching, and the culprit is released.
        let mgr = ResourceGroupManager::new(Config::default());
        let t0 = RuTracker::now_secs();
        seed_tracker(&mgr, "noisy", 100.0, 1000.0, t0);

        assert!(mgr.select_noisy_groups().contains("noisy"));
        assert_eq!(baseline_of(&mgr, "noisy"), Some(100.0));
        set_backpressure(&mgr, "noisy", true, true);

        // current == hist, so the gate can no longer match.
        mgr.ru_trackers
            .get("noisy")
            .unwrap()
            .lock()
            .unwrap()
            .0
            .cached_historical_rate = 1000.0;

        assert!(
            mgr.select_noisy_groups().contains("noisy"),
            "latched group must stay selected after its baseline drifts up"
        );
        assert_eq!(baseline_of(&mgr, "noisy"), Some(100.0));
    }

    #[test]
    fn test_a_group_back_at_its_frozen_baseline_takes_nobody_with_it() {
        // The actuators have driven the group back to the baseline it was
        // selected against. It stays named, since it is inside its gate only
        // because they are holding it there, but the load it has given back
        // must not pull anyone else in.
        let mgr = ResourceGroupManager::new(Config::default());
        let t0 = RuTracker::now_secs();
        seed_tracker(&mgr, "noisy", 100.0, 1000.0, t0);
        assert!(mgr.select_noisy_groups().contains("noisy"));
        assert_eq!(baseline_of(&mgr, "noisy"), Some(100.0));
        set_backpressure(&mgr, "noisy", true, true);

        // Rate back at its baseline, and the live average has drifted up to
        // meet it — neither can keep the group selected.
        let t1 = t0 + 10 * RU_BUCKET_SECS;
        seed_tracker(&mgr, "quiet", 100.0, 100.0, t1);
        {
            let e = mgr.ru_trackers.get("noisy").unwrap();
            let mut tr = e.lock().unwrap();
            tr.0.record_at(100 * RU_BUCKET_SECS, t1);
            tr.0.cached_historical_rate = 900.0;
            tr.0.cached_current_rate = 100.0;
        }
        let noisy = mgr.select_noisy_groups();
        assert!(
            noisy.contains("noisy"),
            "a held group stays named however far its rate has come back down"
        );
        assert!(
            !noisy.contains("quiet"),
            "and the load it returned must not drag in a group inside its gate"
        );
    }

    #[test]
    fn test_quiet_baseline_needs_a_full_window_of_quiet() {
        let mut tr = RuTracker::new(0, 30);
        let window = tr.window_secs();
        tr.cached_historical_rate = 200.0;

        tr.refresh_quiet_baseline(true, 0);
        assert_eq!(tr.quiet_baseline, None, "one quiet tick is not a window");
        tr.refresh_quiet_baseline(true, window - 1);
        assert_eq!(tr.quiet_baseline, None);
        tr.refresh_quiet_baseline(true, window);
        assert_eq!(tr.quiet_baseline, Some(200.0));

        // The sliding average climbs as the overload is recorded. A tick above
        // the gate must not take it, or the reference would drift up to absorb
        // the very load it is used to judge.
        tr.cached_historical_rate = 900.0;
        tr.refresh_quiet_baseline(false, window + 10);
        assert_eq!(tr.quiet_baseline, Some(200.0));
        assert_eq!(
            tr.effective_baseline(),
            200.0,
            "the clamp-down paths must see the pre-overload value"
        );

        // Quiet again, but the run restarts: for a window yet the ring buffer
        // still holds the episode, so a sample would come back inflated.
        tr.cached_historical_rate = 250.0;
        tr.refresh_quiet_baseline(true, window + 20);
        assert_eq!(tr.quiet_baseline, Some(200.0), "the run restarted");
        tr.refresh_quiet_baseline(true, 2 * window + 19);
        assert_eq!(tr.quiet_baseline, Some(200.0));
        tr.refresh_quiet_baseline(true, 2 * window + 20);
        assert_eq!(
            tr.quiet_baseline,
            Some(250.0),
            "a full quiet window re-takes it, with no latch to drop"
        );
    }

    #[test]
    fn test_baseline_is_zero_before_any_quiet_window() {
        // A tracker created mid-overload, or one on a node that never goes
        // quiet, has no sample yet, and is judged on raw usage rather than
        // against a live average that has already absorbed its own spike.
        let mut tr = RuTracker::new(0, 30);
        tr.cached_historical_rate = 400.0;
        tr.refresh_quiet_baseline(false, 0);
        assert_eq!(tr.quiet_baseline, None);
        assert_eq!(tr.effective_baseline(), 0.0);
        // Any traffic clears a zero gate, so such a group is always eligible.
        tr.current_bucket.store(10, Ordering::Relaxed);
        tr.refresh_cached_current_rate(1);
        assert!(tr.is_over_burst_target(1.2));
    }

    #[test]
    fn test_a_cold_tracker_never_takes_a_zero_baseline() {
        // A zero would outlast the warm-up and, since the throttle needs a
        // positive baseline, leave the group permanently unthrottleable.
        let mut tr = RuTracker::new(0, 30);
        let window = tr.window_secs();
        tr.refresh_quiet_baseline(true, 0);
        tr.refresh_quiet_baseline(true, window);
        assert_eq!(tr.quiet_baseline, None, "nothing to sample yet");

        tr.cached_historical_rate = 150.0;
        tr.refresh_quiet_baseline(true, 2 * window);
        assert_eq!(
            tr.quiet_baseline,
            Some(150.0),
            "taken once there is history"
        );
    }

    #[test]
    fn test_held_credit_covers_target_and_spares_new_candidates() {
        // A held group's credit keeps covering the target, sparing a small
        // group that drifts over its gate.
        let mgr = ResourceGroupManager::new(Config::default());
        let t0 = RuTracker::now_secs();
        seed_tracker(&mgr, "noisy", 100.0, 1000.0, t0);
        assert!(mgr.select_noisy_groups().contains("noisy"));
        set_backpressure(&mgr, "noisy", true, true);

        // A small group is now marginally over its own baseline.
        seed_tracker(&mgr, "small", 10.0, 20.0, t0);

        let selected = mgr.select_noisy_groups();
        assert!(selected.contains("noisy"), "{:?}", selected);
        assert!(
            !selected.contains("small"),
            "latched credit should already cover the target: {:?}",
            selected
        );
    }

    #[test]
    fn test_select_noisy_groups_credits_whole_share() {
        // Four groups each 1.25x their own baseline: excess 20 apiece, current
        // 100 apiece, total usage 400. Default threshold 70 → target is 20% of
        // 400 = 80. A selected group is credited with its whole share, so one
        // covers the target; crediting only the excess would have taken all four.
        let mgr = ResourceGroupManager::new(Config::default());
        let t0 = RuTracker::now_secs();
        for name in ["g1", "g2", "g3", "g4"] {
            seed_tracker(&mgr, name, 80.0, 100.0, t0);
        }

        assert_eq!(mgr.select_noisy_groups().len(), 1);
    }

    #[test]
    fn test_select_noisy_groups_prefers_biggest_movers() {
        // Default threshold 70 → target reduction is 20% of total usage.
        // "noisy" alone covers that, so "mild" is spared even though it is
        // over its own burst target.
        let mgr = ResourceGroupManager::new(Config::default());
        let t0 = RuTracker::now_secs();
        seed_tracker(&mgr, "noisy", 100.0, 3000.0, t0);
        seed_tracker(&mgr, "mild", 500.0, 1000.0, t0);
        seed_tracker(&mgr, "steady", 1000.0, 1000.0, t0);

        let selected = mgr.select_noisy_groups();
        assert!(selected.contains("noisy"), "biggest mover must be selected");
        assert!(
            !selected.contains("mild"),
            "2x mover must be spared once the 10x mover covers the target"
        );
        assert!(
            !selected.contains("steady"),
            "within burst target → never a candidate"
        );
    }

    #[test]
    fn test_select_noisy_groups_always_takes_top_candidate() {
        // No single group can cover the target, so selection walks down the
        // sorted list; the group within its burst target stays out.
        let mgr = ResourceGroupManager::new(Config::default());
        let t0 = RuTracker::now_secs();
        seed_tracker(&mgr, "mild", 500.0, 1000.0, t0);
        seed_tracker(&mgr, "steady", 1000.0, 1000.0, t0);

        let selected = mgr.select_noisy_groups();
        assert!(
            selected.contains("mild"),
            "the only candidate must still be penalized"
        );
        assert!(!selected.contains("steady"));
    }

    #[test]
    fn test_deprioritize_over_quota_groups_ru_based() {
        // Two-phase scheduling driven by real RU (CPU µs) from
        // ResourceGroupManager: only groups that have exceeded their own
        // historical quota are deprioritized, and only once the caller (the
        // unified read pool) actually calls deprioritize_over_quota_groups.
        let mut cfg = Config::default();
        cfg.enable_fair_scheduling = true;
        let mgr = ResourceGroupManager::new(cfg);

        let steady = new_resource_group_ru("steady".into(), 1000, MEDIUM_PRIORITY);
        let spike = new_resource_group_ru("spike".into(), 1000, MEDIUM_PRIORITY);
        mgr.add_resource_group(steady);
        mgr.add_resource_group(spike);

        let ctl = mgr.derive_controller("read".into(), true);

        // Warm up steady's RuTracker: 2 completed 30s buckets with consistent
        // rate (current == historical → within baseline).
        let t0 = RuTracker::now_secs();
        {
            let e = mgr
                .ru_trackers
                .entry("steady".to_owned())
                .or_insert_with(|| {
                    RuTrackerSlot::new(
                        RuTracker::new(t0, 30),
                        Arc::new(ResourceLimiter::new(
                            "".into(),
                            f64::INFINITY,
                            f64::INFINITY,
                            0,
                            false,
                        )),
                    )
                });
            let mut tr = e.lock().unwrap();
            tr.0.record_at(6000, t0 + 15);
            tr.0.record_at(0, t0 + 30); // close bucket 0: 6000 µs
            tr.0.record_at(6000, t0 + 45);
            tr.0.record_at(0, t0 + 60); // close bucket 1: 6000 µs → stable baseline
        }
        // Simulate a spike for "spike": current bucket >> historical.
        {
            let e = mgr
                .ru_trackers
                .entry("spike".to_owned())
                .or_insert_with(|| {
                    RuTrackerSlot::new(
                        RuTracker::new(t0, 30),
                        Arc::new(ResourceLimiter::new(
                            "".into(),
                            f64::INFINITY,
                            f64::INFINITY,
                            0,
                            false,
                        )),
                    )
                });
            let mut tr = e.lock().unwrap();
            tr.0.record_at(3000, t0 + 30);
            tr.0.record_at(0, t0 + 60); // close bucket [t0+30,t0+60): 3000µs baseline
            tr.0.record_at(12000, t0 + 90); // current open bucket: 12000µs spike (left open)
        }

        // deprioritize_over_quota_groups reads `cached_historical_rate`,
        // which is refreshed by `adjust_group_throttling` on
        // resource_control's own tick (independent of, but still running
        // alongside, the unified read pool's tick that calls
        // deprioritize_over_quota_groups in production).
        mgr.online_adjust_resource_quota(0.0);

        // Before the caller ever deprioritizes, neither group is
        // deprioritized, even though "spike" is already over its own
        // baseline.
        {
            let groups = ctl.resource_consumptions.read();
            assert!(
                !groups
                    .get(b"steady".as_ref())
                    .unwrap()
                    .is_over_baseline
                    .load(Ordering::Relaxed),
                "steady should be phase 0 before deprioritize_over_quota_groups runs"
            );
            assert!(
                !groups
                    .get(b"spike".as_ref())
                    .unwrap()
                    .is_over_baseline
                    .load(Ordering::Relaxed),
                "spike should be phase 0 before deprioritize_over_quota_groups runs"
            );
        }

        // Only "spike" (over its own baseline) is deprioritized; "steady"
        // (within baseline) is not.
        set_sampled_rate(&mgr, "spike", 400.0);
        mark_sustained(&mgr, "spike");
        stage_noisy(&mgr);
        mgr.deprioritize_over_quota_groups();
        {
            let groups = ctl.resource_consumptions.read();
            assert!(
                !groups
                    .get(b"steady".as_ref())
                    .unwrap()
                    .is_over_baseline
                    .load(Ordering::Relaxed),
                "steady is within its baseline → should stay phase 0"
            );
            assert!(
                groups
                    .get(b"spike".as_ref())
                    .unwrap()
                    .is_over_baseline
                    .load(Ordering::Relaxed),
                "spike exceeded its baseline → should be phase 1"
            );
        }

        // Phase ordering: steady (phase 0) must sort before spike (phase 1).
        let groups = ctl.resource_consumptions.read();
        let steady_pri = groups
            .get(b"steady".as_ref())
            .unwrap()
            .get_priority(1, None, false, true);
        let spike_pri = groups
            .get(b"spike".as_ref())
            .unwrap()
            .get_priority(1, None, false, true);
        assert!(!is_phase1(steady_pri), "steady should be phase 0");
        assert!(is_phase1(spike_pri), "spike should be phase 1");
        assert!(
            steady_pri < spike_pri,
            "phase 0 must schedule before phase 1"
        );
    }

    #[test]
    fn test_deprioritize_over_quota_groups_releases_when_inactive() {
        // A group over its own quota stays deprioritized until the caller
        // explicitly calls reset_group_priorities — e.g. once the unified
        // read pool has scaled back up to core_thread_count.
        // deprioritize/target-cpu are gated on fair scheduling.
        let mut cfg = Config::default();
        cfg.enable_fair_scheduling = true;
        let mgr = ResourceGroupManager::new(cfg);

        let spike = new_resource_group_ru("spike".into(), 1000, MEDIUM_PRIORITY);
        mgr.add_resource_group(spike);
        let ctl = mgr.derive_controller("read".into(), true);

        let t0 = RuTracker::now_secs();
        {
            let e = mgr
                .ru_trackers
                .entry("spike".to_owned())
                .or_insert_with(|| {
                    RuTrackerSlot::new(
                        RuTracker::new(t0, 30),
                        Arc::new(ResourceLimiter::new(
                            "".into(),
                            f64::INFINITY,
                            f64::INFINITY,
                            0,
                            false,
                        )),
                    )
                });
            let mut tr = e.lock().unwrap();
            tr.0.record_at(3000, t0 + 30);
            tr.0.record_at(0, t0 + 60); // close bucket: 3000µs baseline
            tr.0.record_at(12000, t0 + 90); // open bucket: 12000µs spike
        }

        // Refresh `cached_historical_rate`, normally done by
        // `adjust_group_throttling` on resource_control's own tick.
        mgr.online_adjust_resource_quota(0.0);

        set_sampled_rate(&mgr, "spike", 400.0);
        mark_sustained(&mgr, "spike");
        stage_noisy(&mgr);
        mgr.deprioritize_over_quota_groups();
        assert!(
            ctl.resource_consumptions
                .read()
                .get(b"spike".as_ref())
                .unwrap()
                .is_over_baseline
                .load(Ordering::Relaxed),
            "spike exceeded its quota → should be phase 1"
        );

        // Caller (e.g. the unified read pool, once it has scaled back up to
        // core_thread_count) calls reset_group_priorities: released
        // immediately, even though spike's RU history hasn't changed.
        mgr.reset_group_priorities();
        assert!(
            !ctl.resource_consumptions
                .read()
                .get(b"spike".as_ref())
                .unwrap()
                .is_over_baseline
                .load(Ordering::Relaxed),
            "spike should be released once the caller resets priorities"
        );
    }

    #[test]
    fn test_deprioritize_over_quota_groups_does_not_self_release() {
        // A sustained noisy group must stay deprioritized even once its own
        // `cached_historical_rate` has drifted up to absorb its elevated
        // usage (current no longer exceeds hist * burst_factor) — only
        // reset_group_priorities is allowed to release it.
        // deprioritize/target-cpu are gated on fair scheduling.
        let mut cfg = Config::default();
        cfg.enable_fair_scheduling = true;
        let mgr = ResourceGroupManager::new(cfg);

        let spike = new_resource_group_ru("spike".into(), 1000, MEDIUM_PRIORITY);
        mgr.add_resource_group(spike);
        let ctl = mgr.derive_controller("read".into(), true);

        let t0 = RuTracker::now_secs();
        {
            let e = mgr
                .ru_trackers
                .entry("spike".to_owned())
                .or_insert_with(|| {
                    RuTrackerSlot::new(
                        RuTracker::new(t0, 30),
                        Arc::new(ResourceLimiter::new(
                            "".into(),
                            f64::INFINITY,
                            f64::INFINITY,
                            0,
                            false,
                        )),
                    )
                });
            let mut tr = e.lock().unwrap();
            tr.0.record_at(3000, t0 + 30);
            tr.0.record_at(0, t0 + 60); // close bucket: 3000µs baseline
            tr.0.record_at(12000, t0 + 90); // open bucket: 12000µs spike
        }
        mgr.online_adjust_resource_quota(0.0);

        set_sampled_rate(&mgr, "spike", 400.0);
        mark_sustained(&mgr, "spike");
        stage_noisy(&mgr);
        mgr.deprioritize_over_quota_groups();
        assert!(
            ctl.resource_consumptions
                .read()
                .get(b"spike".as_ref())
                .unwrap()
                .is_over_baseline
                .load(Ordering::Relaxed),
            "spike exceeded its quota → should be phase 1"
        );

        // Simulate the historical baseline catching up to the sustained
        // elevated rate: overwrite cached_historical_rate directly to match
        // current, as if enough windows had rolled over.
        {
            let e = mgr.ru_trackers.get("spike").unwrap();
            let mut tr = e.lock().unwrap();
            tr.0.cached_historical_rate = tr.0.current_rate();
        }

        // Calling deprioritize_over_quota_groups again must not clear the
        // flag just because current no longer exceeds the (now-caught-up)
        // historical rate.
        mgr.deprioritize_over_quota_groups();
        assert!(
            ctl.resource_consumptions
                .read()
                .get(b"spike".as_ref())
                .unwrap()
                .is_over_baseline
                .load(Ordering::Relaxed),
            "spike must stay deprioritized even after its baseline catches up"
        );

        // Only an explicit reset releases it.
        mgr.reset_group_priorities();
        assert!(
            !ctl.resource_consumptions
                .read()
                .get(b"spike".as_ref())
                .unwrap()
                .is_over_baseline
                .load(Ordering::Relaxed),
            "spike should be released once the caller resets priorities"
        );
    }

    #[test]
    fn test_read_pool_scale_up_allowed_when_idle() {
        let mgr = ResourceGroupManager::default();
        // Drive cpu_score comfortably below the leeway threshold so
        // adjust_group_scheduling marks the system as idle.
        mgr.online_adjust_resource_quota(0.0);
        assert!(mgr.read_pool_scale_up_allowed());

        // With no pressure, compute_read_pool_target_cpu imposes no ceiling
        // at all — scaling up is the unified read pool's own responsibility.
        let target_cpu = mgr.compute_read_pool_target_cpu(2.0, 10.0);
        assert_eq!(target_cpu, f64::INFINITY, "no pressure → no ceiling");
    }

    #[test]
    fn test_compute_read_pool_target_cpu_holds_in_leeway_zone() {
        // deprioritize/target-cpu are gated on fair scheduling.
        let mut cfg = Config::default();
        cfg.enable_fair_scheduling = true;
        let mgr = ResourceGroupManager::new(cfg);
        // cpu_score in the leeway zone: not engaged (bg not at floor), and
        // above leeway_threshold, so neither scale-down nor scale-up fires.
        mgr.online_adjust_resource_quota(65.0);
        assert!(!mgr.read_pool_scale_up_allowed());
        assert_eq!(mgr.read_pool_cpu_pressure(), 0.0);

        let target_cpu = mgr.compute_read_pool_target_cpu(2.0, 10.0);
        assert_eq!(
            target_cpu,
            f64::INFINITY,
            "leeway zone should impose no ceiling"
        );
    }

    #[test]
    fn test_read_pool_entry_points_are_inert_without_fair_scheduling() {
        // The read pool consults the manager unconditionally and relies on it
        // to be a no-op while fair scheduling is off, so that gate lives here
        // rather than at the call site.
        let mgr = ResourceGroupManager::default();
        assert!(!mgr.get_config().value().enable_fair_scheduling);

        // Pressure is still tracked (adjust_group_scheduling is not gated)...
        mgr.set_bg_cpu_at_floor(true);
        tick_until_engaged(&mgr, PEAK_CPU_PCT);
        assert_eq!(mgr.read_pool_cpu_pressure(), 1.0);

        // ...but it must not turn into a ceiling, or hold back scale-out.
        assert_eq!(
            mgr.compute_read_pool_target_cpu(4.0, 10.0),
            f64::INFINITY,
            "no ceiling should be imposed while fair scheduling is off"
        );
        assert!(
            mgr.read_pool_scale_up_allowed(),
            "scale-out must not be blocked while fair scheduling is off"
        );

        // And a group that genuinely is over quota — so that dropping the gate
        // really would deprioritize it — must be left alone.
        let spike = new_resource_group_ru("spike".into(), 1000, MEDIUM_PRIORITY);
        mgr.add_resource_group(spike);
        let ctl = mgr.derive_controller("read".into(), true);
        let t0 = RuTracker::now_secs();
        {
            let e = mgr
                .ru_trackers
                .entry("spike".to_owned())
                .or_insert_with(|| {
                    RuTrackerSlot::new(
                        RuTracker::new(t0, 30),
                        Arc::new(ResourceLimiter::new(
                            "".into(),
                            f64::INFINITY,
                            f64::INFINITY,
                            0,
                            false,
                        )),
                    )
                });
            let mut tr = e.lock().unwrap();
            tr.0.record_at(3000, t0 + 30);
            tr.0.record_at(0, t0 + 60); // close bucket: 3000µs baseline
            tr.0.record_at(12000, t0 + 90); // open bucket: 12000µs spike
        }
        // Refresh cached_historical_rate, as resource_control's own tick would.
        mgr.online_adjust_resource_quota(0.0);
        mgr.deprioritize_over_quota_groups();
        assert!(
            !ctl.resource_consumptions
                .read()
                .get(b"spike".as_ref())
                .unwrap()
                .is_over_baseline
                .load(Ordering::Relaxed),
            "deprioritize must be a no-op while fair scheduling is off"
        );
    }

    #[test]
    fn test_compute_read_pool_target_cpu_scales_down_with_pressure() {
        // deprioritize/target-cpu are gated on fair scheduling.
        let mut cfg = Config::default();
        cfg.enable_fair_scheduling = true;
        let mgr = ResourceGroupManager::new(cfg);
        // Seed a historical floor of 0 cores (cold tracker), then engage
        // pressure (cpu_score == PEAK_CPU_PCT).
        mgr.set_bg_cpu_at_floor(true);
        tick_until_engaged(&mgr, PEAK_CPU_PCT);
        assert_eq!(mgr.read_pool_cpu_pressure(), 1.0);

        // Once engaged, the ceiling is one step below the currently measured
        // usage instead of collapsing straight to the (cold, i.e. 0)
        // historical floor.
        let target_cpu = mgr.compute_read_pool_target_cpu(4.0, 10.0);
        assert!(
            (target_cpu - 3.4).abs() < 1e-9,
            "should be 15% below measured usage, got {target_cpu}"
        );

        // Stateless: calling again with the same measured usage gives the
        // same result rather than ratcheting further down on its own.
        let target_cpu = mgr.compute_read_pool_target_cpu(4.0, 10.0);
        assert!(
            (target_cpu - 3.4).abs() < 1e-9,
            "repeated calls with unchanged usage should not ratchet further, got {target_cpu}"
        );

        // It does respond to a drop in measured usage (e.g. after the read
        // pool itself cut its thread count in response to the previous
        // tick's lower ceiling).
        let target_cpu = mgr.compute_read_pool_target_cpu(3.4, 10.0);
        assert!(
            (target_cpu - 2.89).abs() < 1e-9,
            "should track 15% below whatever usage is currently measured, got {target_cpu}"
        );
    }

    /// The pool gets at least one core, the counterpart of the 1 RU/s floor a
    /// throttled group gets. Without it the ceiling is only 15% below current
    /// on every tick, which compounds: the pool follows its own ceiling down
    /// and the two spiral toward zero.
    #[test]
    fn test_read_pool_target_never_goes_below_one_core() {
        let mut cfg = Config::default();
        cfg.enable_fair_scheduling = true;
        let mgr = ResourceGroupManager::new(cfg);
        mgr.set_bg_cpu_at_floor(true);
        tick_until_engaged(&mgr, PEAK_CPU_PCT);

        // No quiet window has elapsed, so there is no sampled floor.
        assert_eq!(mgr.quiet_read_pool_floor(), None);
        // 15% below 1.0 would be 0.85; the floor holds it at one core.
        let target = mgr.compute_read_pool_target_cpu(1.0, 10.0);
        assert!(
            (target - 1.0).abs() < 1e-9,
            "should hold at one core, got {target}"
        );
        // And it does not spiral: feeding the target back in as the measured
        // usage leaves it where it is.
        let target = mgr.compute_read_pool_target_cpu(target, 10.0);
        assert!(
            (target - 1.0).abs() < 1e-9,
            "should not ratchet, got {target}"
        );

        // Nor does the live average serve as the floor: build one well above a
        // core and the target still comes down to the minimum. The average is
        // computed over the overload being shed, so using it would float the
        // floor up under the ratchet and stall the shedding.
        let t0 = RuTracker::now_secs();
        for i in 0..12 {
            mgr.compute_read_pool_target_cpu_at(8.0, 10.0, t0 + 10 * i);
        }
        assert!(
            mgr.record_read_pool_cpu_at(0.0, 0.0, RuTracker::now_secs()) > 1.0,
            "the live average should exceed a core for this to prove anything"
        );
        let target = mgr.compute_read_pool_target_cpu_at(1.0, 10.0, t0 + 120);
        assert!(
            (target - 1.0).abs() < 1e-9,
            "the live average must not act as the floor, got {target}"
        );

        // A sampled floor under one core does not lower the floor either --
        // this node's was 0.075 cores.
        set_quiet_read_pool_floor(&mgr, 0.075);
        let target = mgr.compute_read_pool_target_cpu(1.0, 10.0);
        assert!(
            (target - 1.0).abs() < 1e-9,
            "a sub-core sampled floor is still one core, got {target}"
        );

        // A sampled floor above it still wins, which is the whole point of
        // sampling one.
        set_quiet_read_pool_floor(&mgr, 3.0);
        let target = mgr.compute_read_pool_target_cpu(2.0, 10.0);
        assert!(
            (target - 3.0).abs() < 1e-9,
            "the sampled floor should hold above the minimum, got {target}"
        );
    }

    /// The release path measures against the live average, not the baseline,
    /// and a group with no history at all reads zero there. Zero must still
    /// reach INFINITY: a throttled group that has since gone idle would
    /// otherwise ramp 10% a tick forever and never be handed back its limit.
    #[test]
    fn test_a_zero_history_group_is_still_released() {
        let mgr = ResourceGroupManager::default();
        mgr.add_resource_group(new_resource_group_ru("g1".into(), 1000, HIGH_PRIORITY));
        let limiter = mgr.get_foreground_group_limiter("g1");
        let now = RuTracker::now_secs();
        // Traffic in the open bucket only: enough to keep the tracker from
        // being evicted as idle, not enough to complete a bucket, so the
        // historical rate stays at zero.
        stage_open_bucket(&mgr, "g1", 1_000);
        limiter.get_limiter(ResourceType::Cpu).set_rate_limit(500.0);
        assert_eq!(
            mgr.ru_trackers
                .get("g1")
                .unwrap()
                .lock()
                .unwrap()
                .0
                .cached_historical_rate,
            0.0,
            "no traffic has ever been recorded"
        );

        // Quiet enough to be recovering: below the leeway threshold.
        // MIN_RAMP_UP_EPOCHS is local to the actuator, hence the literal.
        for _ in 0..2 {
            assert!(
                limit_of(&mgr, "g1").is_finite(),
                "not released before the epochs elapse"
            );
            mgr.online_adjust_resource_quota_at(10.0, now);
        }
        assert!(
            limit_of(&mgr, "g1").is_infinite(),
            "a zero live average releases on the same path as any other"
        );
    }

    /// A group that has never had a quiet window has a baseline of zero, so
    /// its burst target is zero and nothing clamps the decrease: it descends
    /// to the 1 RU/s floor for as long as it stays named. On a node that is
    /// never quiet, that is every group.
    #[test]
    fn test_a_zero_baseline_ratchets_to_the_floor() {
        let mgr = ResourceGroupManager::default();
        mgr.add_resource_group(new_resource_group_ru("g1".into(), 1000, HIGH_PRIORITY));
        let limiter = mgr.get_foreground_group_limiter("g1");
        let t0 = RuTracker::now_secs();
        {
            let entry = mgr.ru_trackers.get("g1").unwrap();
            let mut guard = entry.lock().unwrap();
            guard.0.record_at(6000, t0 + 30);
            guard.0.record_at(0, t0 + 60);
        }
        mgr.set_bg_cpu_at_floor(true);
        let now = t0 + 85;
        stage_open_bucket(&mgr, "g1", 20_000);

        assert_eq!(
            mgr.ru_trackers
                .get("g1")
                .unwrap()
                .lock()
                .unwrap()
                .0
                .quiet_baseline,
            None,
            "no quiet window has elapsed"
        );

        mark_sustained(&mgr, "g1");
        for _ in 0..120 {
            mgr.online_adjust_resource_quota_at(90.0, now);
        }
        assert_eq!(
            limiter.get_limiter(ResourceType::Cpu).get_rate_limit(),
            1.0,
            "a zero target leaves nothing to stop the ratchet"
        );
    }

    #[test]
    fn test_adjust_group_throttling_decreases_by_15_percent_per_tick() {
        // fg_cpu_throttle_threshold=70, baseline_burst_pct=20 (defaults) ->
        // burst_factor = 1.2.
        let mgr = ResourceGroupManager::default();
        mgr.add_resource_group(new_resource_group_ru("g1".into(), 1000, HIGH_PRIORITY));
        let limiter = mgr.get_foreground_group_limiter("g1");

        // Seed one completed bucket (historical) and a large spike in the
        // still-open bucket (current), so hist > 0 and current is well
        // above burst_target = hist * 1.2.
        let t0 = RuTracker::now_secs();
        {
            let entry = mgr.ru_trackers.get("g1").unwrap();
            let mut guard = entry.lock().unwrap();
            guard.0.record_at(6000, t0 + 30);
            guard.0.record_at(0, t0 + 60); // closes bucket, so hist is non-zero
        }
        mgr.set_bg_cpu_at_floor(true);
        let now = t0 + 85;
        // Spike confined to the last tick, so it does not also raise hist.
        stage_open_bucket(&mgr, "g1", 20_000);
        // A target now comes only from a sampled quiet window, so freeze one:
        // with no baseline the ratchet has nothing to stop at and runs to the
        // floor, which `test_a_zero_baseline_ratchets_to_the_floor` covers.
        let quiet_baseline = {
            let entry = mgr.ru_trackers.get("g1").unwrap();
            let mut guard = entry.lock().unwrap();
            guard.0.refresh_cached_historical_rate(t0, now);
            let hist = guard.0.cached_historical_rate;
            guard.0.quiet_baseline = Some(hist);
            hist
        };

        // First tick: no limit set yet (starts at INFINITY), so the base is
        // the measured current rate, tightened by one step — not an
        // interpolated jump straight to burst_target.
        mark_sustained(&mgr, "g1");
        mgr.online_adjust_resource_quota_at(90.0, now);
        let after_tick1 = limiter.get_limiter(ResourceType::Cpu).get_rate_limit();
        let current_rate = mgr
            .ru_trackers
            .get("g1")
            .unwrap()
            .lock()
            .unwrap()
            .0
            .current_rate();
        assert!(
            (after_tick1 - current_rate * 0.85).abs() < current_rate * 0.01,
            "first tick should tighten 15% below measured current rate, got {after_tick1}, \
             expected ~{}",
            current_rate * 0.85
        );

        // Second tick, same inputs: base is now the persisted current_limit
        // from tick 1 (not a freshly measured/interpolated value), so it
        // tightens another step relative to itself rather than staying put
        // or jumping to burst_target.
        mgr.online_adjust_resource_quota_at(90.0, now);
        let after_tick2 = limiter.get_limiter(ResourceType::Cpu).get_rate_limit();
        assert!(
            (after_tick2 - after_tick1 * 0.85).abs() < after_tick1 * 0.01,
            "second tick should tighten another 15% relative to the previous tick's limit, \
             got {after_tick2}, expected ~{}",
            after_tick1 * 0.85
        );

        // Repeated ticks converge to and stop at burst_target = hist * 1.2,
        // never going below it.
        for _ in 0..60 {
            mgr.online_adjust_resource_quota_at(90.0, now);
        }
        let floored = limiter.get_limiter(ResourceType::Cpu).get_rate_limit();
        let burst_target = quiet_baseline * 1.2;
        assert!(
            (floored - burst_target).abs() < burst_target * 0.01,
            "should converge to and stop at burst_target ({burst_target}), got {floored}"
        );
    }

    #[test]
    fn test_get_resource_limiter() {
        let mgr = ResourceGroupManager::default();

        let default_group = new_background_resource_group_ru(
            "default".into(),
            200,
            MEDIUM_PRIORITY,
            vec!["br".into(), "stats".into()],
        );
        mgr.add_resource_group(default_group);
        let default_limiter = mgr
            .get_resource_group("default")
            .unwrap()
            .limiter
            .clone()
            .unwrap();

        // Even with only 1 group (default), foreground returns the
        // per-group limiter regardless of priority level.
        let fg_default_limiter = mgr.get_resource_limiter("default", "query", 0).unwrap();
        assert!(Arc::ptr_eq(
            &mgr.get_resource_limiter("default", "query", HIGH_PRIORITY as u64)
                .unwrap(),
            &fg_default_limiter,
        ));
        assert!(Arc::ptr_eq(
            &mgr.get_resource_limiter("default", "query", LOW_PRIORITY as u64)
                .unwrap(),
            &fg_default_limiter,
        ));

        let group1 = new_resource_group("test1".into(), true, 100, 100, HIGH_PRIORITY);
        mgr.add_resource_group(group1);

        let bg_group = new_background_resource_group_ru(
            "bg".into(),
            50,
            LOW_PRIORITY,
            vec!["ddl".into(), "stats".into()],
        );
        mgr.add_resource_group(bg_group);
        let bg_limiter = mgr
            .get_resource_group("bg")
            .unwrap()
            .limiter
            .clone()
            .unwrap();

        assert!(
            mgr.get_background_resource_limiter("test1", "ddl")
                .is_none()
        );
        assert!(Arc::ptr_eq(
            &mgr.get_background_resource_limiter("test1", "stats")
                .unwrap(),
            &default_limiter
        ));

        assert!(Arc::ptr_eq(
            &mgr.get_background_resource_limiter("bg", "stats").unwrap(),
            &bg_limiter
        ));
        assert!(mgr.get_background_resource_limiter("bg", "br").is_none());
        assert!(
            mgr.get_background_resource_limiter("bg", "invalid")
                .is_none()
        );

        assert!(Arc::ptr_eq(
            &mgr.get_background_resource_limiter("unknown", "stats")
                .unwrap(),
            &default_limiter
        ));

        // Background path still takes priority for "stats" source.
        assert!(Arc::ptr_eq(
            &mgr.get_resource_limiter("test1", "stats", 0).unwrap(),
            &default_limiter
        ));

        // Multiple groups: all foreground priorities get the per-group limiter.
        // The same limiter is returned regardless of priority level.
        let fg_limiter = mgr
            .get_resource_limiter("test1", "query", LOW_PRIORITY as u64)
            .unwrap();
        assert!(Arc::ptr_eq(
            &mgr.get_resource_limiter("test1", "query", HIGH_PRIORITY as u64)
                .unwrap(),
            &fg_limiter,
        ));
        assert!(Arc::ptr_eq(
            &mgr.get_resource_limiter("test1", "query", 0).unwrap(),
            &fg_limiter,
        ));
    }

    #[test]
    fn test_ru_tracker() {
        let t0: u64 = 1_000_000;

        const BUCKETS: usize = 15;
        let mut tracker = RuTracker::new(t0, BUCKETS);
        assert!(!tracker.is_warmed_up());
        // No data at all, and no elapsed time to divide by.
        tracker.refresh_cached_current_rate(t0);
        assert_eq!(tracker.current_rate(), 0.0);
        assert_eq!(tracker.historical_rate(t0, t0), 0.0);

        // Record 6000 RU in the first 30s bucket.
        tracker.record_at(6000, t0 + 15);

        // Advance past the first bucket boundary (30s) — completes bucket 0.
        tracker.record_at(0, t0 + 30);
        assert_eq!(tracker.completed, 1);
        // 6000 RU over the 30s since the last sample = 200 RU/s.
        tracker.refresh_cached_current_rate(t0 + 30);
        assert!((tracker.current_rate() - 200.0).abs() < 0.01);
        assert!(!tracker.is_warmed_up()); // needs ≥2 buckets

        // Advance another 30s with 3000 RU — completes bucket 1.
        tracker.record_at(3000, t0 + 45);
        tracker.record_at(0, t0 + 60);
        assert_eq!(tracker.completed, 2);
        assert!(tracker.is_warmed_up());
        // A further 3000 RU over the next 30s = 100 RU/s.
        tracker.refresh_cached_current_rate(t0 + 60);
        assert!((tracker.current_rate() - 100.0).abs() < 0.01);
        // Nothing recorded since: the trailing window still holds the closed
        // bucket, so the rate decays rather than dropping to zero on the tick
        // traffic stops -- 3000 RU over the 45s the window now covers.
        tracker.refresh_cached_current_rate(t0 + 75);
        assert!((tracker.current_rate() - 66.667).abs() < 0.01);
        // historical_rate = (6000+3000) / (2*30) = 150 RU/s
        assert!((tracker.historical_rate(t0, t0 + 60) - 150.0).abs() < 0.01);

        // Ring buffer: advance 20 more minutes (fully evicts all data).
        let t_far = t0 + 60 * 20;
        tracker.record_at(0, t_far);
        // Gap exceeds window → ring reset, no completed buckets.
        assert_eq!(tracker.completed, 0);
        assert_eq!(tracker.current_bucket.load(Ordering::Relaxed), 0);

        // Re-populate after the big gap and fill the entire ring.
        for i in 1..=BUCKETS {
            tracker.record_at(100, t_far + (i as u64) * RU_BUCKET_SECS);
        }
        assert_eq!(tracker.completed, BUCKETS);
    }

    #[test]
    fn test_admission_decision() {
        let mut cfg = Config::default();
        cfg.enable_read_admission_control = true;
        cfg.admission_max_delayed_count = 10; // low limit to test rejection path
        let mgr = ResourceGroupManager::new(cfg);
        // Add a second group so the code path is active.
        mgr.add_resource_group(new_resource_group_ru("spike".into(), 1000, HIGH_PRIORITY));
        let spike_limiter = mgr.get_foreground_group_limiter("spike");

        // Seed initial RU so the tracker is not idle (prevents eviction by
        // online_adjust_resource_quota's retain call).
        let t0 = RuTracker::now_secs();
        mgr.record_ru_consumption("spike", 1);

        // CPU below threshold → Allow (limit stays infinite, no throttling).
        mgr.online_adjust_resource_quota(50.0); // below 80%
        assert_eq!(
            mgr.admission_decision(true, &spike_limiter),
            AdmissionDecision::Allow
        );

        // CPU above threshold but no warmed-up tracker data → limit stays
        // infinite → Allow.
        tick_until_engaged(&mgr, 90.0);
        assert_eq!(
            mgr.admission_decision(true, &spike_limiter),
            AdmissionDecision::Allow
        );

        // Warm up the tracker: 2 completed buckets. Set up BEFORE calling
        // online_adjust_resource_quota so historical_rate() is non-zero when the
        // limiter rate is configured.
        {
            let entry = mgr.ru_trackers.get("spike").unwrap();
            let mut guard = entry.lock().unwrap();
            guard.0.record_at(6000, t0 + 30);
            guard.0.record_at(0, t0 + 60); // close bucket 0: 6000 RU (baseline)
            guard.0.record_at(12000, t0 + 90); // open bucket: 12000 RU spike (left open)
            // historical ≈ 6000/30 = 200 RU/s (only completed buckets)
        }
        // ...and a spike in the last tick, giving current ≈ 400 RU/s.
        stage_open_bucket(&mgr, "spike", 4_000);
        // bg_cpu_at_floor must be true for the throttle branch to fire.
        mgr.set_bg_cpu_at_floor(true);
        tick_until_engaged(&mgr, 90.0);
        // Consume a burst well above the rate to build token-bucket debt.
        {
            let entry = mgr.ru_trackers.get("spike").unwrap();
            let guard = entry.lock().unwrap();
            // 10_000 µs consumed against ~133 RU/s rate → several seconds of debt.
            guard.1.consume(
                Duration::from_micros(10_000),
                IoBytes::default(),
                false,
                true,
            );
        }
        // Debt is non-zero → Delay.
        assert!(matches!(
            mgr.admission_decision(true, &spike_limiter),
            AdmissionDecision::Delay(_)
        ));
        // One slot acquired above; release it.
        mgr.release_delay_slot();

        // Exhaust the delay slots: acquire 10 (the configured max).
        for _ in 0..10 {
            assert!(matches!(
                mgr.admission_decision(true, &spike_limiter),
                AdmissionDecision::Delay(_)
            ));
        }
        // 11th request: over the limit → Reject.
        assert_eq!(
            mgr.admission_decision(true, &spike_limiter),
            AdmissionDecision::Reject
        );
        // Release all slots.
        for _ in 0..10 {
            mgr.release_delay_slot();
        }

        // Above the leeway threshold: no recovery, so the multiplier holds
        // and requests still delay.
        mgr.online_adjust_resource_quota(75.0);
        assert!(matches!(
            mgr.admission_decision(true, &spike_limiter),
            AdmissionDecision::Delay(_)
        ));

        // Drop CPU below the leeway threshold: the limit ramps up a step per
        // tick until it is lifted to infinity, clearing the token-bucket debt.
        for _ in 0..60 {
            mgr.online_adjust_resource_quota(50.0);
        }
        assert_eq!(
            mgr.admission_decision(true, &spike_limiter),
            AdmissionDecision::Allow
        );
    }

    #[test]
    fn test_current_rate_needs_no_warm_up() {
        let t0: u64 = 1_000_000;
        let mut tr = RuTracker::new(t0, 30);
        // A steady 100 RU/s from the moment the tracker exists. With no closed
        // bucket the open one is measured over its own elapsed time, so the
        // first tick already reports the real rate instead of half of it.
        for i in 0..10 {
            tr.record_at(100, t0 + i);
        }
        tr.refresh_cached_current_rate(t0 + 10);
        assert_eq!(tr.current_rate(), 100.0);
        assert_eq!(tr.completed, 0, "no bucket needs to have closed");

        // Reading or refreshing it again inside the same tick is free and
        // stable: the window is derived from the ring, not consumed from it.
        assert_eq!(tr.current_rate(), 100.0);
        tr.refresh_cached_current_rate(t0 + 10);
        assert_eq!(tr.current_rate(), 100.0, "a refresh must be idempotent");
    }

    #[test]
    fn test_current_rate_does_not_follow_a_single_tick_of_traffic() {
        let t0: u64 = 1_000_000;
        let mut tr = RuTracker::new(t0, 30);
        // One closed bucket at a steady 100 RU/s.
        tr.record_at(3000, t0 + 15);
        tr.record_at(0, t0 + 30);
        tr.refresh_cached_current_rate(t0 + 30);
        assert!((tr.current_rate() - 100.0).abs() < 0.01);

        // Then a burst running at 200 RU/s for one 10s tick. The window is the
        // closed bucket plus the open one over the 40s they cover, so the rate
        // moves to 125, not to the 200 the burst is running at.
        tr.record(2_000);
        tr.refresh_cached_current_rate(t0 + 40);
        assert!(
            (tr.current_rate() - 125.0).abs() < 0.01,
            "expected the 40s average, got {}",
            tr.current_rate()
        );
    }

    #[test]
    fn test_lifecycle_zero_history_tenant_spikes_alone_and_both_recover() {
        // tenant1 idle, tenant2 steady at 100. tenant1 jumps to 1000.
        let mgr = ResourceGroupManager::new(Config::default());
        let t0 = RuTracker::now_secs();
        mgr.set_bg_cpu_at_floor(true);
        seed_tracker(&mgr, "tenant1", 0.0, 1000.0, t0);
        seed_tracker(&mgr, "tenant2", 100.0, 100.0, t0);

        tick(&mgr, 90.0);
        let noisy = mgr.noisy_groups();
        assert!(
            noisy.contains("tenant1") && noisy.len() == 1,
            "the spike is tenant1's alone: {noisy:?}"
        );
        // Throttled off what it is consuming. With no history its target is
        // zero, so the decrease step is unclamped — a first-ever spike is cut
        // by the full step rather than being left alone.
        assert!(
            (limit_of(&mgr, "tenant1") - 1000.0 * THROTTLE_DECREASE_FACTOR).abs() < 1.0,
            "one decrease step off 1000: {}",
            limit_of(&mgr, "tenant1")
        );
        assert!(
            limit_of(&mgr, "tenant2").is_infinite(),
            "not named, untouched"
        );

        // It slows down, and its trailing average has now absorbed the spike.
        set_sampled_rate(&mgr, "tenant1", 10.0);
        set_baseline(&mgr, "tenant1", 500.0);
        for _ in 1..=3 {
            tick(&mgr, 50.0);
        }
        assert!(
            mgr.select_noisy_groups().is_empty(),
            "neither tenant is noisy once the load is back down"
        );
        assert!(limit_of(&mgr, "tenant1").is_infinite());
        assert!(limit_of(&mgr, "tenant2").is_infinite());
    }

    #[test]
    fn test_zero_baseline_tenant_is_both_throttled_and_deprioritized() {
        // A first-ever spike has no baseline, which is a target of zero rather
        // than an exemption: both actuators reach it.
        let mut cfg = Config::default();
        cfg.enable_fair_scheduling = true;
        let mgr = ResourceGroupManager::new(cfg);
        mgr.add_resource_group(new_resource_group_ru("fresh".into(), 1000, MEDIUM_PRIORITY));
        let ctl = mgr.derive_controller("read".into(), true);
        let t0 = RuTracker::now_secs();
        mgr.set_bg_cpu_at_floor(true);
        seed_tracker(&mgr, "fresh", 0.0, 1000.0, t0);

        tick(&mgr, 90.0);
        assert!(mgr.noisy_groups().contains("fresh"), "named");
        assert!(
            limit_of(&mgr, "fresh").is_finite(),
            "and throttled: a zero target does not clamp the decrease"
        );

        mgr.deprioritize_over_quota_groups();
        assert!(
            ctl.resource_consumptions
                .read()
                .get(b"fresh".as_ref())
                .unwrap()
                .is_over_baseline
                .load(Ordering::Relaxed),
            "the scheduler is the one actuator that does reach it"
        );
    }

    #[test]
    fn test_lifecycle_baseline_catching_up_does_not_release_the_culprit() {
        // Three tenants, one spikes. Its own trailing average then rises to
        // absorb the spike, which is exactly when the live baseline would let
        // it go while it is still the cause.
        let mgr = ResourceGroupManager::new(Config::default());
        let t0 = RuTracker::now_secs();
        mgr.set_bg_cpu_at_floor(true);
        seed_tracker(&mgr, "tenant1", 100.0, 1000.0, t0);
        seed_tracker(&mgr, "tenant2", 200.0, 200.0, t0);
        seed_tracker(&mgr, "tenant3", 300.0, 300.0, t0);

        tick(&mgr, 90.0);
        assert!(mgr.noisy_groups().contains("tenant1"));
        assert_eq!(
            baseline_of(&mgr, "tenant1"),
            Some(100.0),
            "frozen pre-spike"
        );
        let first = limit_of(&mgr, "tenant1");
        assert!(first.is_finite(), "throttled, since it has history");

        // The live average catches up to the elevated rate.
        set_baseline(&mgr, "tenant1", 1000.0);
        tick(&mgr, 90.0);
        assert!(
            mgr.noisy_groups().contains("tenant1"),
            "the frozen baseline keeps it named while its rate is still up"
        );
        assert!(limit_of(&mgr, "tenant1") < first, "and it keeps tightening");
        assert!(!mgr.noisy_groups().contains("tenant2"));
        assert!(!mgr.noisy_groups().contains("tenant3"));

        // Recovery: load back to its (now higher) baseline, CPU below leeway.
        set_sampled_rate(&mgr, "tenant1", 1000.0);
        for _ in 2..=40 {
            tick(&mgr, 50.0);
        }
        assert!(
            limit_of(&mgr, "tenant1").is_infinite(),
            "the limit is handed back"
        );
        assert!(
            mgr.select_noisy_groups().is_empty(),
            "and nobody is noisy any more"
        );
    }

    #[test]
    fn test_lifecycle_a_throttled_culprit_keeps_the_blame_off_a_small_spike() {
        // tenant1 spikes and is held. Its load comes back down, and tenant2
        // then rises slightly. The relief tenant1 is already giving covers the
        // target, so tenant2 must not be taken in its place.
        let mgr = ResourceGroupManager::new(Config::default());
        let t0 = RuTracker::now_secs();
        mgr.set_bg_cpu_at_floor(true);
        seed_tracker(&mgr, "tenant1", 300.0, 3000.0, t0);
        seed_tracker(&mgr, "tenant2", 100.0, 100.0, t0);

        tick(&mgr, 90.0);
        assert!(
            mgr.noisy_groups().contains("tenant1") && mgr.noisy_groups().len() == 1,
            "tenant1 alone"
        );

        // The throttle has worked: tenant1 back inside its gate. tenant2 edges
        // up just past its own.
        set_sampled_rate(&mgr, "tenant1", 300.0);
        set_sampled_rate(&mgr, "tenant2", 130.0);
        tick(&mgr, 90.0);

        let noisy = mgr.noisy_groups();
        assert!(
            noisy.contains("tenant1"),
            "still held, so still named: {noisy:?}"
        );
        // Spared by the counter on this tick: one tick over its gate is not
        // evidence, whatever else is going on.
        assert!(!noisy.contains("tenant2"), "{noisy:?}");

        // A second tick over its gate confirms it, so now it is a genuine
        // candidate -- and must still be spared, because the relief tenant1 is
        // already giving back covers the whole target.
        tick(&mgr, 90.0);
        let noisy = mgr.noisy_groups();
        assert!(noisy.contains("tenant1"), "{noisy:?}");
        assert!(
            !noisy.contains("tenant2"),
            "tenant1's relief covers the target, so the small riser is not \
             taken to meet it: {noisy:?}"
        );
        assert!(limit_of(&mgr, "tenant2").is_infinite(), "and not throttled");
    }

    #[test]
    fn test_the_cached_verdict_keeps_a_group_the_throttle_has_pushed_back_down() {
        // The callers overwrite the cache wholesale each pressured tick, so a
        // group dropping out of the verdict silently unnames it. Once the
        // throttle has driven it back inside its gate that is exactly when it
        // must stay named.
        let mgr = ResourceGroupManager::new(Config::default());
        let t0 = RuTracker::now_secs();
        seed_tracker(&mgr, "spike", 100.0, 1000.0, t0);

        *mgr.noisy_groups.write() = mgr.select_noisy_groups();
        assert!(
            mgr.noisy_groups().contains("spike"),
            "named on the first tick"
        );
        set_backpressure(&mgr, "spike", true, true);

        // Next tick: the throttle has worked and it is back inside its gate.
        set_sampled_rate(&mgr, "spike", 100.0);
        *mgr.noisy_groups.write() = mgr.select_noisy_groups();
        assert!(
            mgr.noisy_groups().contains("spike"),
            "still held, so the cache must still name it"
        );
    }

    #[test]
    fn test_a_held_group_still_over_its_gate_is_credited_not_ignored() {
        // A held group whose counter has been cleared -- by a tick inside its
        // gate, or by one without node pressure -- while it is still above
        // that gate. The actuators are on it, so its rate has to count toward
        // the target; otherwise a confirmed neighbour is taken to make up a
        // difference that is already being reclaimed.
        let mgr = ResourceGroupManager::new(Config::default());
        let t0 = RuTracker::now_secs();
        seed_tracker(&mgr, "culprit", 300.0, 3000.0, t0);
        seed_tracker(&mgr, "neighbour", 100.0, 130.0, t0);
        {
            let entry = mgr.ru_trackers.get("culprit").unwrap();
            let mut guard = entry.lock().unwrap();
            guard.0.over_baseline_ticks = 0;
        }
        set_backpressure(&mgr, "culprit", true, true);

        let noisy = mgr.select_noisy_groups();
        assert!(noisy.contains("culprit"), "held, so named: {noisy:?}");
        assert!(
            !noisy.contains("neighbour"),
            "the held group's rate covers the target, so the neighbour is not \
             taken to meet it: {noisy:?}"
        );
    }

    /// Same fixture for both detection modes: `small` has moved 10x above its
    /// own history but is tiny; `big` is the largest consumer but sits just
    /// above its own history.
    fn seed_mover_and_hog(mgr: &ResourceGroupManager, t0: u64) {
        seed_tracker(mgr, "big", 990.0, 1000.0, t0);
        seed_tracker(mgr, "small", 10.0, 100.0, t0);
        mgr.set_bg_cpu_at_floor(true);
    }

    #[test]
    fn test_baseline_detection_blames_the_group_that_moved() {
        let mgr = ResourceGroupManager::new(Config::default());
        let t0 = RuTracker::now_secs();
        seed_mover_and_hog(&mgr, t0);

        mgr.online_adjust_resource_quota_at(90.0, t0);
        assert!(
            mgr.noisy_groups().contains("small"),
            "10x its own baseline outranks a larger group sitting on its: {:?}",
            mgr.noisy_groups()
        );
    }

    #[test]
    fn test_current_usage_detection_blames_the_biggest_consumer() {
        // Zeroing the baseline is the whole mode: the eligibility gate reduces
        // to "has traffic" and `excess` becomes the raw rate, so the ranking is
        // by consumption and the small mover is spared.
        let cfg = Config {
            noisy_detection: NoisyDetection::CurrentUsage,
            ..Default::default()
        };
        let mgr = ResourceGroupManager::new(cfg);
        let t0 = RuTracker::now_secs();
        seed_mover_and_hog(&mgr, t0);

        mgr.online_adjust_resource_quota_at(90.0, t0);
        assert_eq!(baseline_of(&mgr, "big"), Some(0.0));
        assert_eq!(
            baseline_of(&mgr, "small"),
            Some(0.0),
            "the mode discards whatever history the fixture stated"
        );

        let noisy = mgr.noisy_groups();
        assert!(
            noisy.contains("big") && !noisy.contains("small"),
            "the biggest consumer is blamed, not the biggest mover: {noisy:?}"
        );

        // The zero baseline does not exempt it from the throttle: one
        // decrease step off what it is consuming, with nothing below to clamp
        // at but the 1 RU/s minimum.
        let limit = limit_of(&mgr, "big");
        assert!(
            (limit - 1000.0 * THROTTLE_DECREASE_FACTOR).abs() < 1.0,
            "expected one decrease step off the sampled rate, got {limit}"
        );
        assert!(limit_of(&mgr, "small").is_infinite());
    }

    #[test]
    fn test_switching_back_to_baseline_drops_the_stated_zero() {
        let cfg = Config {
            noisy_detection: NoisyDetection::CurrentUsage,
            ..Default::default()
        };
        let mgr = ResourceGroupManager::new(cfg);
        let t0 = RuTracker::now_secs();
        seed_mover_and_hog(&mgr, t0);

        mgr.online_adjust_resource_quota_at(90.0, t0);
        assert_eq!(baseline_of(&mgr, "big"), Some(0.0));

        // Switched back mid-episode, so the tick that follows is far too
        // loaded to sample a replacement baseline. The zero still has to go:
        // `None` records that this group has never had a quiet window, which
        // a zero stated by the other mode does not.
        mgr.get_config()
            .update(|c| -> Result<(), ()> {
                c.noisy_detection = NoisyDetection::Baseline;
                Ok(())
            })
            .unwrap();
        mgr.online_adjust_resource_quota_at(90.0, t0 + 10);

        assert_eq!(
            baseline_of(&mgr, "big"),
            None,
            "the zero was stated by the other mode, not sampled"
        );
    }

    #[test]
    fn test_a_group_over_its_target_for_one_tick_is_not_blamed() {
        let mgr = ResourceGroupManager::default();
        let t0 = RuTracker::now_secs();
        seed_tracker(&mgr, "spike", 100.0, 1000.0, t0);
        let burst_factor = 1.0 + mgr.get_config().value().baseline_burst_pct / 100.0;
        let tick = || {
            mgr.ru_trackers
                .get("spike")
                .unwrap()
                .lock()
                .unwrap()
                .0
                .update_over_baseline_ticks(burst_factor, true, false)
        };
        // Undo the fixture's claim, so the counter starts cold as it would on
        // the first tick a group runs hot.
        mgr.ru_trackers
            .get("spike")
            .unwrap()
            .lock()
            .unwrap()
            .0
            .over_baseline_ticks = 0;

        tick();
        assert!(
            mgr.select_noisy_groups().is_empty(),
            "one tick over target is not evidence"
        );

        tick();
        let noisy = mgr.select_noisy_groups();
        assert!(
            noisy.contains("spike") && noisy.len() == 1,
            "two consecutive ticks must blame the group, got {noisy:?}"
        );
    }

    #[test]
    fn test_a_score_hovering_on_the_threshold_keeps_the_evidence() {
        // The failure this prevents: cpu_score alternating either side of the
        // threshold wiped the count every other tick, so a node overloaded half
        // the time never reached two and blamed nobody.
        let mgr = ResourceGroupManager::default();
        let t0 = RuTracker::now_secs();
        seed_tracker(&mgr, "spike", 100.0, 1000.0, t0);
        let burst_factor = 1.0 + mgr.get_config().value().baseline_burst_pct / 100.0;
        let entry = mgr.ru_trackers.get("spike").unwrap();
        let mut guard = entry.lock().unwrap();
        guard.0.over_baseline_ticks = 0;

        // Above the threshold: evidence.
        guard
            .0
            .update_over_baseline_ticks(burst_factor, true, false);
        assert_eq!(guard.0.over_baseline_ticks, 1);

        // Below the threshold but above the leeway threshold: hold.
        guard
            .0
            .update_over_baseline_ticks(burst_factor, false, false);
        assert_eq!(
            guard.0.over_baseline_ticks, 1,
            "the band between the thresholds must hold, not wipe"
        );

        guard
            .0
            .update_over_baseline_ticks(burst_factor, true, false);
        assert!(
            guard.0.sustained_over_baseline(),
            "so the next loaded tick confirms instead of starting over"
        );
    }

    #[test]
    fn test_pressure_clearing_wipes_the_evidence() {
        let mgr = ResourceGroupManager::default();
        let t0 = RuTracker::now_secs();
        seed_tracker(&mgr, "spike", 100.0, 1000.0, t0);
        let burst_factor = 1.0 + mgr.get_config().value().baseline_burst_pct / 100.0;
        let entry = mgr.ru_trackers.get("spike").unwrap();
        let mut guard = entry.lock().unwrap();
        assert!(guard.0.sustained_over_baseline(), "fixture starts blamed");

        // Below the leeway threshold the node is genuinely fine, so whatever
        // the group is doing is not a problem worth blaming it for.
        guard
            .0
            .update_over_baseline_ticks(burst_factor, false, true);
        assert_eq!(guard.0.over_baseline_ticks, 0);
    }

    #[test]
    fn test_the_tick_wires_the_load_bands_into_the_candidacy_counter() {
        // Drives the real entry point, since the wiring is what is under test:
        // that the counter keys off the load bands and not off the background
        // gate. Threshold 70, so the leeway threshold is 63.
        let mgr = ResourceGroupManager::default();
        let t0 = RuTracker::now_secs();
        seed_tracker(&mgr, "spike", 100.0, 1000.0, t0);
        let count = |mgr: &ResourceGroupManager| {
            mgr.ru_trackers
                .get("spike")
                .unwrap()
                .lock()
                .unwrap()
                .0
                .over_baseline_ticks
        };
        mgr.ru_trackers
            .get("spike")
            .unwrap()
            .lock()
            .unwrap()
            .0
            .over_baseline_ticks = 0;

        // Background has not yielded yet, so nothing may be acted on — but the
        // evidence has to accrue anyway. Folding `is_bg_cpu_at_floor` into the
        // counter meant every tick of the background squeeze wiped it, making
        // the engage delay the whole squeeze plus MIN_ENGAGE_TICKS.
        mgr.set_bg_cpu_at_floor(false);
        mgr.online_adjust_resource_quota_at(90.0, t0);
        assert_eq!(count(&mgr), 1);
        mgr.online_adjust_resource_quota_at(90.0, t0);
        assert_eq!(
            count(&mgr),
            MIN_ENGAGE_TICKS,
            "the background squeeze must not wipe the evidence"
        );
        assert!(
            mgr.noisy_groups().is_empty(),
            "but no group may be blamed before background is at its floor"
        );

        // Between the two thresholds: held, so a score hovering on the
        // threshold cannot erase the evidence every other tick.
        mgr.online_adjust_resource_quota_at(66.0, t0);
        assert_eq!(count(&mgr), MIN_ENGAGE_TICKS, "the band must hold");

        // Below the leeway threshold the node is genuinely fine.
        mgr.online_adjust_resource_quota_at(50.0, t0);
        assert_eq!(count(&mgr), 0);
    }

    #[test]
    fn test_a_tick_inside_the_target_restarts_the_group_count() {
        let mgr = ResourceGroupManager::default();
        let t0 = RuTracker::now_secs();
        seed_tracker(&mgr, "spike", 100.0, 1000.0, t0);
        let burst_factor = 1.0 + mgr.get_config().value().baseline_burst_pct / 100.0;

        let entry = mgr.ru_trackers.get("spike").unwrap();
        let mut guard = entry.lock().unwrap();
        assert!(guard.0.sustained_over_baseline(), "fixture starts blamed");
        // A quiet tick has since raised its baseline, so this tick is inside
        // the target.
        guard.0.quiet_baseline = Some(10_000.0);
        guard
            .0
            .update_over_baseline_ticks(burst_factor, true, false);
        assert_eq!(
            guard.0.over_baseline_ticks, 0,
            "a tick inside the target must clear the count outright"
        );
    }

    #[test]
    fn test_read_pool_historical_cpu_cold_tracker() {
        let mgr = ResourceGroupManager::default();
        let t0 = RuTracker::now_secs();
        // Cold tracker → historical rate 0 → 0.0 cores.
        let historical = mgr.record_read_pool_cpu_at(8.0, 10.0, t0);
        assert_eq!(historical, 0.0);
    }

    /// A quiet period of a sustained 1 core, then the pool's own window
    /// (2 min = 4 x 30s) of quiet ticks so the floor is established.
    fn seed_read_pool_floor(mgr: &ResourceGroupManager, t0: u64) -> (f64, u64) {
        {
            let mut tracker = mgr.read_pool_cpu_tracker.lock().unwrap();
            for i in 1..=5 {
                tracker.record_at(30_000_000, t0 + 30 * i);
            }
            tracker.refresh_cached_historical_rate(t0, t0 + 150);
        }
        let quiet = mgr
            .read_pool_cpu_tracker
            .lock()
            .unwrap()
            .cached_historical_rate
            / 1e6;
        assert!(quiet > 0.0 && quiet < 6.8, "{}", quiet);
        let now = t0 + 150;
        mgr.adjust_group_scheduling_at(10.0, false, now);
        assert_eq!(
            mgr.quiet_read_pool_floor(),
            None,
            "one quiet tick is not a window"
        );
        mgr.adjust_group_scheduling_at(10.0, false, now + 120);
        let floor = mgr
            .quiet_read_pool_floor()
            .expect("a full quiet window establishes the floor");
        assert!(
            (floor - quiet).abs() < 1e-9,
            "floor {} should be the pre-overload {}",
            floor,
            quiet
        );
        (floor, now + 120)
    }

    fn read_pool_live_floor(mgr: &ResourceGroupManager) -> f64 {
        mgr.read_pool_cpu_tracker
            .lock()
            .unwrap()
            .cached_historical_rate
            / 1e6
    }

    #[test]
    fn test_read_pool_floor_holds_through_an_overload() {
        // Without the gate the floor climbs with the load and the ratchet
        // stalls, since the tracker keeps recording the overload.
        let cfg = Config {
            historical_usage_window_mins: 2,
            enable_fair_scheduling: true,
            ..Default::default()
        };
        let mgr = ResourceGroupManager::new(cfg);
        let (floor, engaged) = seed_read_pool_floor(&mgr, mgr.start_secs);

        mgr.set_bg_cpu_at_floor(true);
        mgr.adjust_group_scheduling_at(95.0, true, engaged);
        assert!(mgr.read_pool_cpu_pressure() > 0.0);

        // 8 cores through the pool: the live average climbs, the floor does
        // not, so the ratchet keeps stepping down.
        let mut target = f64::INFINITY;
        for i in 1..=6 {
            target = mgr.compute_read_pool_target_cpu_at(8.0, 10.0, engaged + 10 * i);
        }
        assert!(
            (target - 6.8).abs() < 0.01,
            "ratchet should hold at 0.85 * 8.0 above the floor, got {}",
            target
        );
        assert!(
            read_pool_live_floor(&mgr) > floor,
            "the live average should have absorbed the overload"
        );
        assert_eq!(
            mgr.quiet_read_pool_floor(),
            Some(floor),
            "the floor must not move while overload is engaged"
        );
    }

    #[test]
    fn test_read_pool_floor_is_not_re_taken_until_the_episode_ages_out() {
        // The window after recovery is the hazard: the tracker still holds the
        // episode, so re-taking the floor there would come back inflated by
        // the very load that was shed.
        let cfg = Config {
            historical_usage_window_mins: 2,
            enable_fair_scheduling: true,
            ..Default::default()
        };
        let mgr = ResourceGroupManager::new(cfg);
        let (floor, engaged) = seed_read_pool_floor(&mgr, mgr.start_secs);

        mgr.set_bg_cpu_at_floor(true);
        mgr.adjust_group_scheduling_at(95.0, true, engaged);
        for i in 1..=6 {
            mgr.compute_read_pool_target_cpu_at(8.0, 10.0, engaged + 10 * i);
        }
        let contaminated = read_pool_live_floor(&mgr);
        assert!(contaminated > floor, "tracker should be contaminated");

        let recovered = engaged + 60;
        mgr.adjust_group_scheduling_at(10.0, false, recovered);
        assert_eq!(
            mgr.quiet_read_pool_floor(),
            Some(floor),
            "the contaminated average must not be taken at the moment of recovery"
        );
        mgr.adjust_group_scheduling_at(10.0, false, recovered + 119);
        assert_eq!(mgr.quiet_read_pool_floor(), Some(floor));

        mgr.adjust_group_scheduling_at(10.0, false, recovered + 120);
        assert_ne!(
            mgr.quiet_read_pool_floor(),
            Some(floor),
            "a full quiet window re-takes it, with no latch to drop"
        );
    }

    #[test]
    fn test_read_pool_floor_is_not_taken_in_the_mid_range_band() {
        // Between the quiet gate (59.5) and the throttle threshold (70) the
        // node is not engaged but not quiet either, and the tracker is still
        // recording load. The run of quiet must not start there.
        let cfg = Config {
            historical_usage_window_mins: 2,
            enable_fair_scheduling: true,
            ..Default::default()
        };
        let mgr = ResourceGroupManager::new(cfg);
        let t0 = mgr.start_secs;
        {
            let mut tracker = mgr.read_pool_cpu_tracker.lock().unwrap();
            for i in 1..=5 {
                tracker.record_at(30_000_000, t0 + 30 * i);
            }
            tracker.refresh_cached_historical_rate(t0, t0 + 150);
        }
        let now = t0 + 150;

        mgr.adjust_group_scheduling_at(65.0, false, now);
        mgr.adjust_group_scheduling_at(65.0, false, now + 1000);
        assert_eq!(
            mgr.quiet_read_pool_floor(),
            None,
            "the mid-range band is not quiet, however long it lasts"
        );

        // The run starts here, not before.
        mgr.adjust_group_scheduling_at(10.0, false, now + 1000);
        assert_eq!(mgr.quiet_read_pool_floor(), None);
        mgr.adjust_group_scheduling_at(10.0, false, now + 1120);
        assert!(
            mgr.quiet_read_pool_floor().is_some(),
            "a window of quiet from that point establishes it"
        );
    }

    #[test]
    fn test_read_pool_historical_cpu_matches_usage() {
        // Small window (minimum 2 min = 4 buckets = 120s) so a handful of
        // directly-seeded buckets fully cover it, giving an exact,
        // non-ramping historical rate to assert against.
        let cfg = Config {
            historical_usage_window_mins: 2,
            ..Default::default()
        };
        let mgr = ResourceGroupManager::new(cfg);
        let t0 = mgr.start_secs;

        // Seed 4 fully-completed 30s buckets at a sustained 4 cores of usage
        // (4 cores * 30s * 1_000_000 us/s = 120_000_000 us per bucket). Each
        // `record_at` call commits the *previous* call's contribution to a
        // bucket, so a 5th call is needed to flush the 4th bucket closed.
        {
            let mut tracker = mgr.read_pool_cpu_tracker.lock().unwrap();
            tracker.record_at(120_000_000, t0 + 30);
            tracker.record_at(120_000_000, t0 + 60);
            tracker.record_at(120_000_000, t0 + 90);
            tracker.record_at(120_000_000, t0 + 120);
            tracker.record_at(120_000_000, t0 + 150);
        }
        // system_uptime (150s) >= window_secs (120s) → historical_rate uses
        // the full window as denominator: 480_000_000 / 120 = 4_000_000 us/s
        // = 4 cores.
        let historical = mgr.record_read_pool_cpu_at(4.0, 30.0, t0 + 150);
        assert_eq!(historical, 4.0);
    }

    /// RU sitting in the still-open bucket, which is where an arrival charge
    /// lands before any tick closes the bucket.
    fn open_bucket(mgr: &ResourceGroupManager, name: &str) -> u64 {
        mgr.ru_trackers
            .get(name)
            .unwrap()
            .lock()
            .unwrap()
            .0
            .current_bucket
            .load(Ordering::Relaxed)
    }

    /// The arrival cost is read from an `AtomicU64` cache rather than the
    /// config lock, so the cache has to actually track the config.
    #[test]
    fn test_request_base_cost_follows_config() {
        let mut cfg = Config::default();
        cfg.request_base_cost_micros = 70;
        let mgr = ResourceGroupManager::new(cfg);

        let mut ctx = ResourceControlContext::default();
        ctx.resource_group_name = DEFAULT_RESOURCE_GROUP_NAME.to_owned();
        mgr.consume_penalty(&ctx);
        assert_eq!(open_bucket(&mgr, DEFAULT_RESOURCE_GROUP_NAME), 70);

        mgr.get_config()
            .update::<_, _, ()>(|c| {
                c.request_base_cost_micros = 0;
                Ok(())
            })
            .unwrap();
        mgr.refresh_cached_config();
        mgr.consume_penalty(&ctx);
        assert_eq!(
            open_bucket(&mgr, DEFAULT_RESOURCE_GROUP_NAME),
            70,
            "0 disables the charge"
        );
    }

    /// A name that is not a configured group must not open a tracker of its
    /// own, or every request can mint one.
    #[test]
    fn test_request_base_cost_of_unknown_group_lands_on_default() {
        let mut cfg = Config::default();
        cfg.request_base_cost_micros = 40;
        let mgr = ResourceGroupManager::new(cfg);

        let mut ctx = ResourceControlContext::default();
        ctx.resource_group_name = "never-configured".to_owned();
        mgr.consume_penalty(&ctx);

        assert!(mgr.ru_trackers.get("never-configured").is_none());
        assert_eq!(open_bucket(&mgr, DEFAULT_RESOURCE_GROUP_NAME), 40);
    }

    /// The slot's lock-free counter and the tracker inside its Mutex have to
    /// be the same counter, or a tick would flush a bucket that never saw the
    /// arrival charges.
    #[test]
    fn test_slot_counter_is_the_trackers_own() {
        let mgr = ResourceGroupManager::default();
        mgr.record_ru_consumption("g", 9);

        let entry = mgr.ru_trackers.get("g").unwrap();
        // Written without the lock, read through it.
        assert_eq!(
            entry
                .lock()
                .unwrap()
                .0
                .current_bucket
                .load(Ordering::Relaxed),
            9
        );
        // And the other way round.
        entry.lock().unwrap().0.record(5);
        assert_eq!(entry.open_bucket.load(Ordering::Relaxed), 14);

        // A tick flushing the bucket must see both.
        entry
            .lock()
            .unwrap()
            .0
            .advance(RuTracker::now_secs() + RU_BUCKET_SECS);
        assert_eq!(entry.open_bucket.load(Ordering::Relaxed), 0);
    }

    /// `record_ru_consumption` takes a shared-read fast path when the group is
    /// already there and falls back to `entry()` only to create one. Both
    /// paths must accumulate into the same bucket.
    #[test]
    fn test_record_ru_consumption_creates_then_reuses_tracker() {
        let mgr = ResourceGroupManager::default();
        assert!(mgr.ru_trackers.get("g").is_none());

        mgr.record_ru_consumption("g", 5);
        assert_eq!(open_bucket(&mgr, "g"), 5);

        mgr.record_ru_consumption("g", 7);
        assert_eq!(open_bucket(&mgr, "g"), 12);
    }
}
