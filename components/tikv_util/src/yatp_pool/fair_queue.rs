// Copyright 2026 TiKV Project Authors. Licensed under Apache-2.0.

//! Strict business priority, weighted group scheduling and per-group MLFQ.

use std::{
    cell::RefCell,
    cmp::Reverse,
    collections::{BTreeMap, BTreeSet, HashMap, HashSet},
    future::Future,
    pin::Pin,
    rc::Rc,
    sync::{
        Arc, Mutex, Weak,
        atomic::{AtomicBool, AtomicU32, AtomicU64, Ordering},
    },
    task::{Context, Poll},
    time::{Duration, Instant},
};

use cpu_time::ThreadTime;
use futures::task::AtomicWaker;
use rand::Rng;
use yatp::{
    queue::{Pop, PopResult, TaskCell as _, TaskQueue, multilevel::TaskLevelManager},
    task::future::TaskCell,
};

use crate::{
    resource_control::TaskMetadata, task_reschedule::PollRescheduleContextGuard, time::LazyInstant,
};

// Nanoseconds * 2^32 / quota preserves sub-microsecond charges even at MAX_RU.
const VT_SCALE: u128 = 1 << 32;
const REFILL_INTERVAL: Duration = Duration::from_millis(10);
const BURST_WINDOW: Duration = Duration::from_millis(100);
const PARALLELISM_RETRY_INTERVAL: Duration = Duration::from_millis(1);
type LaneId = (u32, u64);
type ReadyKey = (Reverse<u32>, u128, u64);

/// Stable identity provided by resource control. Metadata reloads update in
/// place.
pub struct GroupPolicy {
    id: u64,
    name: String,
    priority: AtomicU32,
    weight: AtomicU64,
    retired: AtomicBool,
}

impl GroupPolicy {
    pub fn new(id: u64, name: String, priority: u32, weight: u64) -> Self {
        assert!(weight > 0);
        Self {
            id,
            name,
            priority: AtomicU32::new(priority.clamp(1, 16)),
            weight: AtomicU64::new(weight),
            retired: AtomicBool::new(false),
        }
    }
    pub fn update(&self, priority: u32, weight: u64) {
        assert!(weight > 0);
        self.weight.store(weight, Ordering::Relaxed);
        self.priority
            .store(priority.clamp(1, 16), Ordering::Relaxed);
    }
    pub fn retire(&self) {
        self.retired.store(true, Ordering::Release);
    }
    pub fn is_retired(&self) -> bool {
        self.retired.load(Ordering::Acquire)
    }
    pub fn id(&self) -> u64 {
        self.id
    }
    pub fn name(&self) -> &str {
        &self.name
    }
    pub fn priority(&self) -> u32 {
        self.priority.load(Ordering::Relaxed)
    }
    pub fn weight(&self) -> u64 {
        self.weight.load(Ordering::Relaxed)
    }
}

pub trait GroupProvider: Send + Sync + 'static {
    /// The default policy must keep its identity and never be retired. Policy
    /// updates, including changes to default, are published in place.
    fn group(&self, name: &str) -> Arc<GroupPolicy>;
}

#[derive(Clone, Copy, Debug)]
pub struct FairQueueConfig {
    pub max_tasks: usize,
    pub max_tasks_per_group: usize,
    pub cpu_cores: f64,
    pub cpu_ratio: f64,
    pub slice_precharge: Duration,
    pub max_time_slice: Duration,
}

#[derive(Debug, Default)]
pub struct FairQueueStats {
    pub queued: usize,
    pub live_tasks: usize,
    pub active_dispatches: usize,
    pub reserved_vt: u128,
    pub evicted_tasks: u64,
}

struct Group {
    policy: Arc<GroupPolicy>,
    history: Arc<TaskLevelManager>,
    live: usize,
}

struct Entry {
    task: TaskCell,
    record: Arc<Record>,
}

#[derive(Default)]
struct Lane {
    committed: u128,
    reserved: u128,
    in_flight: usize,
    queues: [BTreeMap<u64, Entry>; 3],
}

impl Lane {
    fn is_empty(&self) -> bool {
        self.queues.iter().all(BTreeMap::is_empty)
    }
    fn key(&self, id: LaneId) -> ReadyKey {
        (
            Reverse(id.0),
            self.committed.saturating_add(self.reserved),
            id.1,
        )
    }
    fn committed_key(&self, id: LaneId) -> ReadyKey {
        (Reverse(id.0), self.committed, id.1)
    }
}

struct State {
    groups: HashMap<u64, Group>,
    lanes: HashMap<LaneId, Lane>,
    ready: BTreeSet<ReadyKey>,
    committed: BTreeSet<ReadyKey>,
    reference: [u128; 17],
    records: HashMap<u64, Arc<Record>>,
    locations: HashMap<u64, (LaneId, usize, u64)>,
    next_id: u64,
    sequence: u64,
    live: usize,
    max_tasks: usize,
    parallelism: usize,
    active_dispatches: usize,
    evicted: u64,
    closed: bool,
}

impl State {
    // Remove before changing any ledger or queue field; reinsert after mutation.
    fn take_lane(&mut self, id: LaneId) -> Lane {
        let lane = self.lanes.remove(&id).unwrap_or_else(|| Lane {
            committed: self.reference[id.0 as usize],
            ..Lane::default()
        });
        if !lane.is_empty() {
            self.ready.remove(&lane.key(id));
            self.committed.remove(&lane.committed_key(id));
        }
        lane
    }
    fn put_lane(&mut self, id: LaneId, lane: Lane) {
        if !lane.is_empty() {
            self.ready.insert(lane.key(id));
            self.committed.insert(lane.committed_key(id));
        }
        self.lanes.insert(id, lane);
    }
    fn advance_reference(&mut self, priority: u32) {
        if let Some(&(Reverse(p), vt, _)) = self.committed.range((Reverse(priority), 0, 0)..).next()
            && p == priority
        {
            self.reference[priority as usize] = self.reference[priority as usize].max(vt);
        }
    }
    /// Inserts an owned entry without resolving policies, touching task
    /// history, or invoking task destructors. The destination group must
    /// already exist.
    fn enqueue(&mut self, id: LaneId, level: usize, entry: Entry, continuation: bool) {
        let mut lane = self.take_lane(id);
        if lane.is_empty() && lane.in_flight == 0 && !continuation {
            lane.committed = lane.committed.max(self.reference[id.0 as usize]);
        }
        let sequence = self.sequence;
        self.sequence = self
            .sequence
            .checked_add(1)
            .expect("fair queue sequence exhausted");
        assert!(
            self.locations
                .insert(entry.record.id, (id, level, sequence))
                .is_none(),
            "task enqueued twice"
        );
        lane.queues[level].insert(sequence, entry);
        self.put_lane(id, lane);
        self.advance_reference(id.0);
    }
    fn remove_queued(&mut self, task_id: u64) -> Option<Entry> {
        let (id, level, seq) = self.locations.remove(&task_id)?;
        let mut lane = self.take_lane(id);
        let entry = lane.queues[level].remove(&seq);
        self.put_lane(id, lane);
        self.advance_reference(id.0);
        entry
    }
    fn cleanup_retired(&mut self, id: u64) {
        if self
            .groups
            .get(&id)
            .is_some_and(|g| g.policy.is_retired() && g.live == 0)
            && self
                .lanes
                .iter()
                .all(|(lane_id, lane)| lane_id.1 != id || (lane.is_empty() && lane.in_flight == 0))
        {
            self.lanes.retain(|lane_id, _| lane_id.1 != id);
            self.groups.remove(&id);
        }
    }

    fn group(&mut self, policy: &Arc<GroupPolicy>) -> &mut Group {
        if !self.groups.contains_key(&policy.id()) {
            // Sweep retired idle identities on group creation, not per pop.
            // One pass avoids quadratic work after a bulk metadata deletion.
            let active: HashSet<_> = self
                .lanes
                .iter()
                .filter(|(_, lane)| !lane.is_empty() || lane.in_flight != 0)
                .map(|(id, _)| id.1)
                .collect();
            let retired: HashSet<_> = self
                .groups
                .iter()
                .filter(|(id, g)| g.policy.is_retired() && g.live == 0 && !active.contains(id))
                .map(|(id, _)| *id)
                .collect();
            self.groups.retain(|id, _| !retired.contains(id));
            self.lanes.retain(|id, _| !retired.contains(&id.1));
        }
        self.groups.entry(policy.id()).or_insert_with(|| Group {
            policy: policy.clone(),
            history: Arc::new(TaskLevelManager::new(
                [Duration::from_millis(5), Duration::from_millis(100)],
                Some(super::DEFAULT_CLEANUP_INTERVAL),
            )),
            live: 0,
        })
    }
}

struct CpuBucket {
    balance_ns: f64,
    rate: f64,
    last_refill: Instant,
}

impl CpuBucket {
    fn new(rate: f64) -> Self {
        Self {
            balance_ns: rate * BURST_WINDOW.as_nanos() as f64,
            rate,
            last_refill: Instant::now(),
        }
    }
    fn refill(&mut self, now: Instant) {
        let elapsed = now.saturating_duration_since(self.last_refill);
        if elapsed >= REFILL_INTERVAL {
            self.balance_ns = (self.balance_ns + elapsed.as_nanos() as f64 * self.rate)
                .min(self.rate * BURST_WINDOW.as_nanos() as f64);
            self.last_refill = now;
        }
    }
    fn retry_at(&mut self, now: Instant) -> Option<Instant> {
        self.refill(now);
        (self.balance_ns <= 0.0).then_some(self.last_refill + REFILL_INTERVAL)
    }
    fn consume(&mut self, elapsed: Duration) {
        self.refill(Instant::now());
        self.balance_ns -= elapsed.as_nanos() as f64;
    }
}

struct Shared {
    provider: Arc<dyn GroupProvider>,
    default_policy: Arc<GroupPolicy>,
    config: FairQueueConfig,
    state: Mutex<State>,
    cpu: Mutex<CpuBucket>,
    metrics: super::metrics::FairQueueMetrics,
}

pub struct FairQueue {
    shared: Arc<Shared>,
}

impl FairQueue {
    pub fn new(provider: Arc<dyn GroupProvider>, config: FairQueueConfig) -> Arc<Self> {
        assert!(config.cpu_cores.is_finite() && config.cpu_cores > 0.0);
        assert!(config.cpu_ratio.is_finite() && config.cpu_ratio > 0.0 && config.cpu_ratio <= 1.0);
        assert!(config.max_tasks > 0 && config.max_tasks_per_group > 0);
        assert!(!config.max_time_slice.is_zero());
        let default_policy = provider.group("default");
        let mut state = State {
            groups: HashMap::new(),
            lanes: HashMap::new(),
            ready: BTreeSet::new(),
            committed: BTreeSet::new(),
            reference: [0; 17],
            records: HashMap::new(),
            locations: HashMap::new(),
            next_id: 1,
            sequence: 0,
            live: 0,
            max_tasks: config.max_tasks,
            parallelism: usize::MAX,
            active_dispatches: 0,
            evicted: 0,
            closed: false,
        };
        // pop() can run under YATP's parking lock. Resolve and register default
        // before starting workers so retirement never invokes the provider or
        // creates/sweeps group histories from that context.
        state.group(&default_policy);
        Arc::new(Self {
            shared: Arc::new(Shared {
                provider,
                default_policy,
                config,
                state: Mutex::new(state),
                cpu: Mutex::new(CpuBucket::new(config.cpu_cores * config.cpu_ratio)),
                metrics: super::metrics::FairQueueMetrics::default(),
            }),
        })
    }

    pub fn cpu_budget_cores(&self) -> f64 {
        self.shared.config.cpu_cores * self.shared.config.cpu_ratio
    }

    pub fn set_max_tasks(&self, limit: usize) {
        self.shared.state.lock().unwrap().max_tasks = limit;
    }

    /// Limits simultaneous dispatches independently of YATP's physical worker
    /// count. Lowering the limit lets existing dispatches finish before more
    /// work starts; it never interrupts a poll or an inline repoll.
    pub fn set_parallelism(&self, limit: usize) {
        assert!(limit > 0);
        self.shared.state.lock().unwrap().parallelism = limit;
    }

    pub fn snapshot(&self) -> FairQueueStats {
        let s = self.shared.state.lock().unwrap();
        FairQueueStats {
            queued: s.locations.len(),
            live_tasks: s.live,
            active_dispatches: s.active_dispatches,
            reserved_vt: s.lanes.values().map(|l| l.reserved).sum(),
            evicted_tasks: s.evicted,
        }
    }

    /// The permit covers the entire future, including asynchronous waiting.
    /// Only a strictly higher business priority can evict a queued task.
    pub fn try_acquire(self: &Arc<Self>, metadata: &TaskMetadata<'_>) -> Option<FairTaskPermit> {
        let policy = self
            .shared
            .provider
            .group(std::str::from_utf8(metadata.group_name()).unwrap_or("default"));
        let override_priority = metadata.override_priority();
        let priority = effective_priority(&policy, override_priority);
        let cap = ((self.shared.config.max_tasks_per_group as u128 * policy.weight() as u128
            / 10_000)
            .max(64)
            .min(self.shared.config.max_tasks_per_group as u128)) as usize;
        // One eviction attempt: contention may consume the freed slot. Never
        // overcommit either cap, or repeatedly evict unrelated work to chase it.
        for attempt in 0..2 {
            let mut s = self.shared.state.lock().unwrap();
            if s.closed {
                return None;
            }
            let group_full = s.group(&policy).live >= cap;
            if !group_full && s.live < s.max_tasks {
                s.live += 1;
                let group = s.group(&policy);
                group.live += 1;
                let history = group.history.clone();
                let record = self.shared.new_record(
                    &mut s,
                    policy.clone(),
                    history,
                    override_priority,
                    true,
                );
                return Some(FairTaskPermit {
                    record: Arc::downgrade(&record),
                    id: record.id,
                });
            }
            if attempt != 0 {
                if group_full {
                    self.shared.metrics.rejected_group.inc();
                } else {
                    self.shared.metrics.rejected_global.inc();
                }
                return None;
            }
            // If only the group cap is full, evicting another group cannot help.
            let candidate = s
                .ready
                .iter()
                .rev()
                .find(|(Reverse(p), _, g)| *p < priority && (!group_full || *g == policy.id()))
                .copied();
            let Some((Reverse(p), _, group)) = candidate else {
                if group_full {
                    self.shared.metrics.rejected_group.inc();
                } else {
                    self.shared.metrics.rejected_global.inc();
                }
                return None;
            };
            let lane = &s.lanes[&(p, group)];
            let victim_id = lane
                .queues
                .iter()
                .rev()
                .find_map(|q| q.last_key_value().map(|(_, e)| e.record.id))
                .unwrap();
            let victim = s.remove_queued(victim_id);
            s.evicted += 1;
            self.shared.metrics.evicted_tasks.inc();
            drop(s);
            // Task destructors release permits and can reenter this queue.
            if let Some(victim) = victim {
                victim.record.cancel();
                drop(victim);
            }
        }
        None
    }
}

fn effective_priority(policy: &GroupPolicy, override_priority: u32) -> u32 {
    if override_priority == 0 {
        policy.priority()
    } else {
        override_priority.clamp(1, 16)
    }
}

fn weighted(cost: Duration, weight: u64) -> u128 {
    cost.as_nanos() * VT_SCALE / weight.max(1) as u128
}

struct Record {
    id: u64,
    group: Arc<GroupPolicy>,
    history: Arc<TaskLevelManager>,
    override_priority: u32,
    shared: Weak<Shared>,
    cancelled: AtomicBool,
    released: AtomicBool,
    counted: bool,
    waker: AtomicWaker,
    future: Mutex<Option<Weak<dyn CancelTask>>>,
}

impl Shared {
    fn new_record(
        self: &Arc<Self>,
        s: &mut State,
        group: Arc<GroupPolicy>,
        history: Arc<TaskLevelManager>,
        override_priority: u32,
        counted: bool,
    ) -> Arc<Record> {
        let id = s.next_id;
        s.next_id = s
            .next_id
            .checked_add(1)
            .expect("fair task identity exhausted");
        let record = Arc::new(Record {
            id,
            group,
            history,
            override_priority,
            shared: Arc::downgrade(self),
            cancelled: AtomicBool::new(false),
            released: AtomicBool::new(false),
            counted,
            waker: AtomicWaker::new(),
            future: Mutex::new(None),
        });
        s.records.insert(id, record.clone());
        record
    }

    fn settle(&self, lane_id: LaneId, reserved: u128, actual: u128) {
        let mut s = self.state.lock().unwrap();
        let mut lane = s.take_lane(lane_id);
        lane.reserved = lane
            .reserved
            .checked_sub(reserved)
            .expect("unbalanced fair reservation");
        lane.committed = lane.committed.saturating_add(actual);
        s.put_lane(lane_id, lane);
        s.advance_reference(lane_id.0);
    }
}

trait CancelTask: Send + Sync {
    // False means a poll currently owns the future; it will observe cancelled
    // before returning. Never wait for or recursively lock a running task.
    fn cancel(&self) -> bool;
}

struct FutureSlot<F> {
    future: Mutex<Option<Pin<Box<F>>>>,
}

impl<F: Future<Output = ()> + Send> CancelTask for FutureSlot<F> {
    fn cancel(&self) -> bool {
        let mut slot = match self.future.try_lock() {
            Ok(slot) => slot,
            Err(std::sync::TryLockError::WouldBlock) => return false,
            Err(std::sync::TryLockError::Poisoned(e)) => e.into_inner(),
        };
        let future = slot.take();
        drop(slot);
        // An IO future can own a waker back to its YATP TaskCell. Explicitly
        // dropping the inner future breaks that cycle on eviction/cancellation.
        drop(future);
        true
    }
}

impl Record {
    fn cancel(&self) {
        self.cancelled.store(true, Ordering::Release);
        let future = self.future.lock().unwrap().as_ref().and_then(Weak::upgrade);
        if future.is_none_or(|f| f.cancel()) {
            self.release();
        }
        self.waker.wake();
    }

    fn release(&self) {
        if self.released.swap(true, Ordering::AcqRel) {
            return;
        }
        if let Some(shared) = self.shared.upgrade() {
            let mut s = shared.state.lock().unwrap();
            s.records.remove(&self.id);
            if self.counted {
                s.live -= 1;
                s.groups.get_mut(&self.group.id()).unwrap().live -= 1;
            }
            s.cleanup_retired(self.group.id());
        }
        // The registry owns Record; the future holds only a Weak reference.
        // Clear the last saved task waker outside the scheduler lock.
        drop(self.waker.take());
    }
}

impl Drop for Record {
    fn drop(&mut self) {
        self.release();
    }
}

pub struct FairTaskPermit {
    record: Weak<Record>,
    id: u64,
}

impl FairTaskPermit {
    pub fn metadata(&self, original: Vec<u8>) -> Vec<u8> {
        TaskMetadata::from(original.as_slice()).with_scheduler_id(self.id)
    }
    pub fn cancellation(&self) -> FairCancellation {
        FairCancellation {
            record: self.record.clone(),
        }
    }
    pub fn wrap<F: Future<Output = ()> + Send + 'static>(self, future: F) -> FairTaskFuture<F> {
        let slot = Arc::new(FutureSlot {
            future: Mutex::new(Some(Box::pin(future))),
        });
        if let Some(record) = self.record.upgrade() {
            let erased: Arc<dyn CancelTask> = slot.clone();
            *record.future.lock().unwrap() = Some(Arc::downgrade(&erased));
            if record.cancelled.load(Ordering::Acquire) {
                record.cancel();
            }
        }
        FairTaskFuture { slot, permit: self }
    }
}

impl Drop for FairTaskPermit {
    fn drop(&mut self) {
        if let Some(record) = self.record.upgrade() {
            record.release();
        }
    }
}

#[derive(Clone)]
pub struct FairCancellation {
    record: Weak<Record>,
}

impl FairCancellation {
    pub fn cancel(&self) {
        let Some(record) = self.record.upgrade() else {
            return;
        };
        // Mark before removing so a racing push cannot publish normal work.
        record.cancelled.store(true, Ordering::Release);
        let entry = record
            .shared
            .upgrade()
            .and_then(|shared| shared.state.lock().unwrap().remove_queued(record.id));
        record.cancel();
        drop(entry);
    }
}

pub struct FairTaskFuture<F> {
    slot: Arc<FutureSlot<F>>,
    permit: FairTaskPermit,
}

impl<F: Future<Output = ()> + Send + 'static> Future for FairTaskFuture<F> {
    type Output = ();
    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<()> {
        let this = self.get_mut();
        let Some(record) = this.permit.record.upgrade() else {
            return Poll::Ready(());
        };
        record.waker.register(cx.waker());
        if record.cancelled.load(Ordering::Acquire) {
            record.cancel();
            return Poll::Ready(());
        }
        let _account = PollAccount::new(record.id);
        // Inline repolls bypass queue.pop(). Force a real scheduler yield when
        // debt appears, including for short self-waking futures.
        let exhausted = ACTIVE.with(|a| {
            a.borrow().as_ref().is_some_and(|d| {
                d.borrow()
                    .shared
                    .cpu
                    .lock()
                    .unwrap()
                    .retry_at(Instant::now())
                    .is_some()
            })
        });
        if exhausted {
            let mut yield_now = std::pin::pin!(yatp::task::future::yield_to_scheduler());
            let result = yield_now.as_mut().poll(cx);
            debug_assert!(result.is_pending());
            return Poll::Pending;
        }
        let mut slot = this.slot.future.lock().unwrap_or_else(|e| e.into_inner());
        let mut result = match slot.as_mut() {
            Some(future) => future.as_mut().poll(cx),
            None => Poll::Ready(()),
        };
        if record.cancelled.load(Ordering::Acquire) {
            result = Poll::Ready(());
        }
        let finished = if result.is_ready() { slot.take() } else { None };
        drop(slot);
        // Destructors may cancel another task, so never run them under the
        // task ownership lock or the scheduler lock.
        drop(finished);
        if result.is_ready() {
            record.release();
        }
        result
    }
}

impl TaskQueue<TaskCell> for FairQueue {
    fn push(&self, mut task: TaskCell) {
        let metadata = TaskMetadata::from(task.mut_extras().metadata());
        let id = metadata.scheduler_id();
        let record = id.and_then(|id| self.shared.state.lock().unwrap().records.get(&id).cloned());
        let record = match record {
            Some(record) => record,
            None if id.is_some() => {
                // A late wake after completion/cancellation is not a new task.
                drop(task);
                return;
            }
            None => {
                // Internal YATP tasks without an admission wrapper use default
                // policy. User read requests always carry a counted permit.
                let policy = self
                    .shared
                    .provider
                    .group(std::str::from_utf8(metadata.group_name()).unwrap_or("default"));
                let override_priority = metadata.override_priority();
                let mut s = self.shared.state.lock().unwrap();
                let history = s.group(&policy).history.clone();
                self.shared
                    .new_record(&mut s, policy, history, override_priority, false)
            }
        };
        // A queued task already has an owning TaskCell and needs no wakeup.
        // Dropping the saved waker here also prevents eviction from retaining it.
        drop(record.waker.take());
        // Never call the provider under the queue lock.
        let policy = if record.group.is_retired() {
            self.shared.default_policy.clone()
        } else {
            record.group.clone()
        };
        record.history.adjust_task_level(&mut task);
        let level = task.mut_extras().current_level() as usize;
        let lane_id = (
            effective_priority(&policy, record.override_priority),
            policy.id(),
        );
        let continuation = ACTIVE.with(|active| {
            active
                .borrow()
                .as_ref()
                .is_some_and(|d| d.borrow().record.id == record.id)
        });
        let mut s = self.shared.state.lock().unwrap();
        if s.closed
            || record.cancelled.load(Ordering::Acquire)
            || record.released.load(Ordering::Acquire)
        {
            drop(s);
            record.cancel();
            drop(task);
            return;
        }
        s.group(&policy);
        s.enqueue(lane_id, level, Entry { task, record }, continuation);
    }

    fn pop(&self) -> PopResult<TaskCell> {
        // YATP may call pop() from park.validate while holding a parking_lot
        // bucket lock. Do not call the provider, task-history helpers, cancel,
        // or task/waker destructors here, even after releasing our state lock.
        loop {
            // Gate and scheduler locks are never nested. A positive balance is
            // permission to start; actual CPU is charged by checkpoints/tail.
            let retry_at = self.shared.cpu.lock().unwrap().retry_at(Instant::now());
            let mut s = self.shared.state.lock().unwrap();
            let Some(&(Reverse(priority), _, group)) = s.ready.first() else {
                return PopResult::Empty;
            };
            if let Some(retry_at) = retry_at {
                self.shared.metrics.gate_pending.inc();
                return PopResult::Pending { retry_at };
            }
            if s.active_dispatches >= s.parallelism {
                // Custom YATP queues can be polled by physical workers above
                // core_thread_count. Keep queued work visible and arrange a
                // retry even when completing a dispatch produces no new push.
                return PopResult::Pending {
                    retry_at: Instant::now() + PARALLELISM_RETRY_INTERVAL,
                };
            }
            let lane_id = (priority, group);
            let mut lane = s.take_lane(lane_id);
            let roll = rand::thread_rng().gen_range(0..100);
            let first = if roll < 80 {
                0
            } else if roll < 96 {
                1
            } else {
                2
            };
            let level = (0..3)
                .map(|offset| (first + offset) % 3)
                .find(|&level| !lane.queues[level].is_empty())
                .unwrap();
            let (_, mut entry) = lane.queues[level].pop_first().unwrap();
            s.locations.remove(&entry.record.id);
            if entry.record.group.is_retired() && group == entry.record.group.id() {
                s.put_lane(lane_id, lane);
                s.advance_reference(priority);
                let policy = &self.shared.default_policy;
                let target = (
                    effective_priority(policy, entry.record.override_priority),
                    policy.id(),
                );
                // Transfer ownership without calling push(): cancellation there
                // can drop a future and wake this same pool. Keep its original
                // record, history and queued MLFQ level; only its scheduling
                // domain changes. A canceled task is cleaned up outside park
                // by its cancellation owner or by the runner.
                s.enqueue(target, level, entry, false);
                // Default may have a lower business priority or greater VT.
                // Re-select instead of dispatching the migrated task directly.
                continue;
            }
            let policy = s.groups[&group].policy.clone();
            let weight = policy.weight();
            let reserved = weighted(self.shared.config.slice_precharge, weight);
            lane.reserved = lane.reserved.saturating_add(reserved);
            lane.in_flight += 1;
            s.active_dispatches += 1;
            s.put_lane(lane_id, lane);
            s.advance_reference(priority);
            drop(s);
            let schedule_time = entry
                .task
                .mut_extras()
                .schedule_time()
                .unwrap_or_else(Instant::now);
            PREPARED.with(|p| {
                assert!(
                    p.borrow().is_none(),
                    "fair dispatch was not consumed by runner"
                );
                *p.borrow_mut() = Some(Dispatch {
                    shared: self.shared.clone(),
                    record: entry.record,
                    policy,
                    lane: lane_id,
                    weight,
                    reserved,
                    wall_start: Instant::now(),
                    cpu_start: ThreadTime::now(),
                    cpu_charged: Duration::ZERO,
                    poll_active: true,
                });
            });
            return PopResult::Ready(Pop {
                task_cell: entry.task,
                schedule_time,
                from_local: false,
            });
        }
    }

    fn drain(&self) {
        let (entries, records) = {
            let mut s = self.shared.state.lock().unwrap();
            s.closed = true;
            let mut entries = Vec::with_capacity(s.locations.len());
            for lane in s.lanes.values_mut() {
                for queue in &mut lane.queues {
                    entries.extend(std::mem::take(queue).into_values());
                }
            }
            s.ready.clear();
            s.committed.clear();
            s.locations.clear();
            (entries, s.records.values().cloned().collect::<Vec<_>>())
        };
        // Include async waiters in shutdown cleanup. Record owns only a weak
        // queue reference, and futures own only weak records, so no cycle can
        // keep a shut down pool alive.
        for record in records {
            record.cancel();
        }
        drop(entries);
    }

    fn has_ready_task(&self) -> bool {
        // This is also FutureRunner's preemption hint. The current dispatch
        // may occupy the last parallelism slot, but queued work must still be
        // allowed to preempt it. Only pop() enforces the dispatch limit.
        let has_cpu_budget = self
            .shared
            .cpu
            .lock()
            .unwrap()
            .retry_at(Instant::now())
            .is_none();
        has_cpu_budget && !self.shared.state.lock().unwrap().ready.is_empty()
    }
}

thread_local! {
    static PREPARED: RefCell<Option<Dispatch>> = const { RefCell::new(None) };
    static ACTIVE: RefCell<Option<Rc<RefCell<Dispatch>>>> = const { RefCell::new(None) };
}

struct Dispatch {
    policy: Arc<GroupPolicy>,
    shared: Arc<Shared>,
    record: Arc<Record>,
    lane: LaneId,
    weight: u64,
    reserved: u128,
    wall_start: Instant,
    cpu_start: ThreadTime,
    cpu_charged: Duration,
    poll_active: bool,
}

impl Dispatch {
    fn charge_cpu(&mut self) {
        let elapsed = self.cpu_start.elapsed();
        let delta = elapsed.saturating_sub(self.cpu_charged);
        self.cpu_charged = elapsed;
        self.shared.cpu.lock().unwrap().consume(delta);
    }
    fn settle(&mut self) {
        if !self.poll_active {
            return;
        }
        self.charge_cpu();
        let wall = self.wall_start.elapsed();
        self.shared.metrics.record_poll(wall, self.cpu_charged);
        self.shared
            .settle(self.lane, self.reserved, weighted(wall, self.weight));
        self.reserved = 0;
        self.poll_active = false;
    }
    fn begin_poll(&mut self) {
        if !self.poll_active {
            // FutureRunner can repoll inline. Capture a new slice weight while
            // retaining the dispatch lane, including after a metadata update.
            self.weight = self.policy.weight();
            self.reserved = weighted(self.shared.config.slice_precharge, self.weight);
            let mut s = self.shared.state.lock().unwrap();
            let mut lane = s.take_lane(self.lane);
            lane.reserved = lane.reserved.saturating_add(self.reserved);
            s.put_lane(self.lane, lane);
        }
        self.wall_start = Instant::now();
        self.cpu_start = ThreadTime::now();
        self.cpu_charged = Duration::ZERO;
        self.poll_active = true;
    }
}

impl Drop for Dispatch {
    fn drop(&mut self) {
        self.settle();
        let mut s = self.shared.state.lock().unwrap();
        s.active_dispatches = s
            .active_dispatches
            .checked_sub(1)
            .expect("unbalanced fair dispatch");
        let mut lane = s.take_lane(self.lane);
        lane.in_flight -= 1;
        // The final completion must advance an empty domain as well. Never
        // advance using a reservation, which is refundable rather than service.
        let no_ready = s
            .committed
            .range((Reverse(self.lane.0), 0, 0)..)
            .next()
            .is_none_or(|k| k.0.0 != self.lane.0);
        if lane.is_empty() && no_ready {
            s.reference[self.lane.0 as usize] =
                s.reference[self.lane.0 as usize].max(lane.committed);
        }
        s.put_lane(self.lane, lane);
        s.advance_reference(self.lane.0);
        s.cleanup_retired(self.lane.1);
        let release = !self.record.counted && !s.locations.contains_key(&self.record.id);
        drop(s);
        if release {
            self.record.release();
        }
    }
}

/// Runner lifetime includes FutureRunner's synchronous re-enqueue. This
/// prevents each cooperative yield from being mistaken for an idle-group
/// wakeup.
pub(super) struct DispatchGuard {
    previous: Option<Rc<RefCell<Dispatch>>>,
    installed: bool,
}

impl DispatchGuard {
    pub(super) fn install() -> Self {
        let dispatch = PREPARED.with(|p| p.borrow_mut().take());
        if let Some(dispatch) = dispatch {
            let previous = ACTIVE.with(|a| a.replace(Some(Rc::new(RefCell::new(dispatch)))));
            Self {
                previous,
                installed: true,
            }
        } else {
            Self {
                previous: None,
                installed: false,
            }
        }
    }
}

impl Drop for DispatchGuard {
    fn drop(&mut self) {
        if self.installed {
            let active = ACTIVE.with(|a| a.replace(self.previous.take()));
            drop(active);
        }
    }
}

struct PollAccount {
    dispatch: Option<Rc<RefCell<Dispatch>>>,
    _slice: Option<PollRescheduleContextGuard>,
}

impl PollAccount {
    fn new(id: u64) -> Self {
        let dispatch = ACTIVE
            .with(|a| a.borrow().clone())
            .filter(|d| d.borrow().record.id == id);
        let slice = dispatch.as_ref().map(|d| {
            let mut dispatch = d.borrow_mut();
            dispatch.begin_poll();
            PollRescheduleContextGuard::new(Instant::now(), dispatch.shared.config.max_time_slice)
        });
        Self {
            dispatch,
            _slice: slice,
        }
    }
}

impl Drop for PollAccount {
    fn drop(&mut self) {
        if let Some(dispatch) = &self.dispatch {
            dispatch.borrow_mut().settle();
            if std::thread::panicking() {
                dispatch.borrow().record.cancel();
            }
        }
    }
}

pub fn is_fair_cpu_dispatch_active() -> bool {
    ACTIVE.with(|a| a.borrow().is_some())
}

pub async fn yield_if_fair_cpu_exhausted(now: &mut LazyInstant) -> bool {
    let exhausted = ACTIVE.with(|a| {
        let active = a.borrow();
        let Some(dispatch) = active.as_ref() else {
            return false;
        };
        let mut dispatch = dispatch.borrow_mut();
        if now.now().saturating_duration_since(dispatch.wall_start) < Duration::from_millis(1) {
            return false;
        }
        dispatch.charge_cpu();
        dispatch
            .shared
            .cpu
            .lock()
            .unwrap()
            .retry_at(Instant::now())
            .is_some()
    });
    if exhausted {
        yatp::task::future::yield_to_scheduler().await;
    }
    exhausted
}

#[cfg(test)]
mod tests {
    use std::{
        sync::{Barrier, atomic::AtomicUsize},
        thread,
    };

    use kvproto::kvrpcpb::ResourceControlContext;
    use yatp::queue::Extras;

    use super::*;

    struct Provider(Vec<Arc<GroupPolicy>>);
    impl GroupProvider for Provider {
        fn group(&self, name: &str) -> Arc<GroupPolicy> {
            self.0
                .iter()
                .find(|p| p.name() == name && !p.is_retired())
                .unwrap_or(&self.0[0])
                .clone()
        }
    }
    fn queue(weights: &[u64]) -> Arc<FairQueue> {
        let provider = Provider(
            weights
                .iter()
                .enumerate()
                .map(|(id, &weight)| {
                    Arc::new(GroupPolicy::new(id as u64, format!("g{id}"), 8, weight))
                })
                .collect(),
        );
        FairQueue::new(
            Arc::new(provider),
            FairQueueConfig {
                max_tasks: 100_000,
                max_tasks_per_group: 100_000,
                cpu_cores: 128.0,
                cpu_ratio: 1.0,
                slice_precharge: Duration::from_micros(500),
                max_time_slice: Duration::from_millis(1),
            },
        )
    }
    fn task(
        queue: &Arc<FairQueue>,
        group: usize,
        priority: u32,
        level: Option<u8>,
    ) -> (TaskCell, FairCancellation) {
        let ctx = ResourceControlContext {
            resource_group_name: format!("g{group}"),
            override_priority: priority as u64,
            ..Default::default()
        };
        let metadata = TaskMetadata::from_ctx(&ctx);
        let permit = queue.try_acquire(&metadata).unwrap();
        let cancel = permit.cancellation();
        let mut extras = Extras::new_multilevel(0, level);
        extras.set_metadata(permit.metadata(metadata.to_vec()));
        (TaskCell::new(permit.wrap(async {}), extras), cancel)
    }
    fn dispatch(queue: &FairQueue, cost: Duration, requeue: bool) -> (u64, u8) {
        let PopResult::Ready(Pop { mut task_cell, .. }) = queue.pop() else {
            panic!("expected ready");
        };
        let guard = DispatchGuard::install();
        let level = task_cell.mut_extras().current_level();
        let group = ACTIVE.with(|a| {
            let active = a.borrow();
            let mut d = active.as_ref().unwrap().borrow_mut();
            d.shared
                .settle(d.lane, d.reserved, weighted(cost, d.weight));
            d.reserved = 0;
            d.poll_active = false;
            d.lane.1
        });
        if requeue {
            queue.push(task_cell);
        } else {
            drop(task_cell);
        }
        drop(guard);
        (group, level)
    }

    #[test]
    fn weighted_service_does_not_depend_on_batch_size() {
        for (weights, expected) in [
            ([10_000, 10_000], 0.5),
            ([10_000, 40_000], 0.8),
            ([1, 10_000], 0.9999),
        ] {
            let q = queue(&weights);
            for _ in 0..32 {
                q.push(task(&q, 0, 0, None).0);
            }
            q.push(task(&q, 1, 0, None).0);
            let mut served = [0; 2];
            for _ in 0..10_000 {
                served[dispatch(&q, Duration::from_millis(1), true).0 as usize] += 1;
            }
            let actual = served[1] as f64 / 10_000.0;
            assert!((actual - expected).abs() < 0.001, "{weights:?}: {served:?}");
            q.drain();
            assert_eq!(q.snapshot().live_tasks, 0);
            assert_eq!(q.snapshot().reserved_vt, 0);
        }
    }

    #[test]
    fn idle_placement_and_reference_exclude_reservations() {
        let q = queue(&[10_000, 10_000, 10_000]);
        q.push(task(&q, 0, 0, None).0);
        dispatch(&q, Duration::from_millis(10), false);
        let reference = q.shared.state.lock().unwrap().reference[8];
        assert_eq!(reference, weighted(Duration::from_millis(10), 10_000));
        q.push(task(&q, 1, 0, None).0);
        assert_eq!(
            q.shared.state.lock().unwrap().lanes[&(8, 1)].committed,
            reference
        );
        let PopResult::Ready(pop) = q.pop() else {
            panic!();
        };
        let guard = DispatchGuard::install();
        q.push(task(&q, 2, 0, None).0);
        assert_eq!(
            q.shared.state.lock().unwrap().lanes[&(8, 2)].committed,
            reference
        );
        drop(pop.task_cell);
        drop(guard);
        q.drain();
        assert_eq!(q.snapshot().reserved_vt, 0);
    }

    #[test]
    fn continuous_requeue_keeps_service_debt_and_captured_weight() {
        let q = queue(&[10_000, 10_000]);
        q.push(task(&q, 0, 0, None).0);
        q.push(task(&q, 1, 0, None).0);
        let PopResult::Ready(pop) = q.pop() else {
            panic!();
        };
        let guard = DispatchGuard::install();
        let original = ACTIVE.with(|a| a.borrow().as_ref().unwrap().borrow().weight);
        q.shared.provider.group("g0").update(8, 40_000);
        ACTIVE.with(|a| {
            let active = a.borrow();
            let mut d = active.as_ref().unwrap().borrow_mut();
            assert_eq!(d.weight, original);
            d.shared.settle(
                d.lane,
                d.reserved,
                weighted(Duration::from_millis(1), d.weight),
            );
            d.reserved = 0;
            d.poll_active = false;
        });
        let before = q.shared.state.lock().unwrap().lanes[&(8, 0)].committed;
        q.push(pop.task_cell);
        drop(guard);
        assert_eq!(
            q.shared.state.lock().unwrap().lanes[&(8, 0)].committed,
            before
        );
        q.drain();
    }

    #[test]
    fn strict_priority_eviction_and_cancel_while_gate_closed() {
        let q = queue(&[10_000, 10_000]);
        q.set_max_tasks(1);
        let (low, _) = task(&q, 0, 1, None);
        q.push(low);
        let (high, cancel) = task(&q, 1, 16, None);
        assert_eq!(q.snapshot().evicted_tasks, 1);
        assert_eq!(q.snapshot().live_tasks, 1);
        q.push(high);
        q.shared.cpu.lock().unwrap().balance_ns = -1e15;
        assert!(matches!(q.pop(), PopResult::Pending { .. }));
        assert!(!q.has_ready_task());
        cancel.cancel();
        assert_eq!(q.snapshot().queued, 0);
        assert_eq!(q.snapshot().live_tasks, 0);
        assert!(matches!(q.pop(), PopResult::Empty));
        // Draining an already canceled queue is idempotent.
        q.drain();
        q.drain();
    }

    #[test]
    fn business_priority_and_mlfq_are_separate() {
        let q = queue(&[10_000, 10_000]);
        q.shared.provider.group("g0").update(1, 10_000);
        q.shared.provider.group("g1").update(16, 10_000);
        q.push(task(&q, 0, 0, Some(0)).0);
        for level in 0..3 {
            q.push(task(&q, 1, 0, Some(level)).0);
        }
        let mut levels = [0; 3];
        for _ in 0..20_000 {
            let (group, level) = dispatch(&q, Duration::from_micros(1), true);
            assert_eq!(group, 1);
            levels[level as usize] += 1;
        }
        // Wide bounds test that all three probabilities are wired, without
        // depending on a particular thread_rng stream.
        assert!((15_000..17_000).contains(&levels[0]), "{levels:?}");
        assert!((2_500..4_000).contains(&levels[1]), "{levels:?}");
        assert!((400..1_200).contains(&levels[2]), "{levels:?}");
        q.drain();
    }

    #[test]
    fn cpu_bucket_refill_debt_and_burst_bound() {
        let mut bucket = CpuBucket::new(2.0);
        let start = bucket.last_refill;
        assert_eq!(bucket.balance_ns, 200_000_000.0);
        bucket.balance_ns = -30_000_000.0;
        assert!(bucket.retry_at(start + Duration::from_millis(10)).is_some());
        assert!(bucket.retry_at(start + Duration::from_millis(20)).is_none());
        bucket.refill(start + Duration::from_secs(100));
        assert_eq!(bucket.balance_ns, 200_000_000.0);
    }

    #[test]
    fn deleted_group_migrates_and_old_generation_releases() {
        let q = queue(&[10_000, 10_000]);
        q.push(task(&q, 1, 0, None).0);
        q.shared.provider.group("g1").retire();
        assert_eq!(dispatch(&q, Duration::from_millis(1), false).0, 0);
        let state = q.shared.state.lock().unwrap();
        assert!(!state.groups.contains_key(&1));
        assert_eq!(state.live, 0);
    }

    #[test]
    fn retired_cancelled_task_does_not_call_out_from_park_validation() {
        struct CheckedProvider {
            policies: [Arc<GroupPolicy>; 2],
            validating: Arc<AtomicBool>,
            calls_in_validation: Arc<AtomicUsize>,
        }
        impl GroupProvider for CheckedProvider {
            fn group(&self, name: &str) -> Arc<GroupPolicy> {
                if self.validating.load(Ordering::Relaxed) {
                    self.calls_in_validation.fetch_add(1, Ordering::Relaxed);
                }
                if name == "g1" {
                    self.policies[1].clone()
                } else {
                    self.policies[0].clone()
                }
            }
        }
        struct WakeOnDrop {
            address: usize,
            validating: Arc<AtomicBool>,
            drops: Arc<AtomicUsize>,
            drops_in_validation: Arc<AtomicUsize>,
        }
        impl Drop for WakeOnDrop {
            fn drop(&mut self) {
                self.drops.fetch_add(1, Ordering::Relaxed);
                if self.validating.load(Ordering::Relaxed) {
                    // Record the violation instead of really reentering park:
                    // a regression must fail this test, not deadlock its process.
                    self.drops_in_validation.fetch_add(1, Ordering::Relaxed);
                } else {
                    // A future destructor may wake another task in the same
                    // pool, which unparks this very parking address.
                    unsafe {
                        parking_lot_core::unpark_all(
                            self.address,
                            parking_lot_core::UnparkToken(0),
                        );
                    }
                }
            }
        }

        let validating = Arc::new(AtomicBool::new(false));
        let calls_in_validation = Arc::new(AtomicUsize::new(0));
        let drops = Arc::new(AtomicUsize::new(0));
        let drops_in_validation = Arc::new(AtomicUsize::new(0));
        let policies = [
            Arc::new(GroupPolicy::new(0, "default".into(), 8, 20_000)),
            Arc::new(GroupPolicy::new(1, "g1".into(), 16, 1_000)),
        ];
        let q = FairQueue::new(
            Arc::new(CheckedProvider {
                policies: policies.clone(),
                validating: validating.clone(),
                calls_in_validation: calls_in_validation.clone(),
            }),
            FairQueueConfig {
                max_tasks: 100,
                max_tasks_per_group: 100,
                cpu_cores: 128.0,
                cpu_ratio: 1.0,
                slice_precharge: Duration::from_micros(500),
                max_time_slice: Duration::from_millis(1),
            },
        );
        let ctx = ResourceControlContext {
            resource_group_name: "g1".into(),
            ..Default::default()
        };
        let metadata = TaskMetadata::from_ctx(&ctx);
        let permit = q.try_acquire(&metadata).unwrap();
        let record = permit.record.upgrade().unwrap();
        let mut extras = Extras::multilevel_default();
        extras.set_metadata(permit.metadata(metadata.to_vec()));
        // The address stays alive for the entire park/unpark operation.
        let address = Arc::as_ptr(&validating) as usize;
        let on_drop = WakeOnDrop {
            address,
            validating: validating.clone(),
            drops: drops.clone(),
            drops_in_validation: drops_in_validation.clone(),
        };
        q.push(TaskCell::new(
            permit.wrap(async move {
                let _on_drop = on_drop;
                futures::future::pending::<()>().await;
            }),
            extras,
        ));
        policies[1].retire();
        // This is the legal interleaving after FairCancellation has set its
        // flag but before it acquires the scheduler lock to remove the entry.
        record.cancelled.store(true, Ordering::Release);

        let mut popped = None;
        unsafe {
            parking_lot_core::park(
                address,
                || {
                    validating.store(true, Ordering::Relaxed);
                    popped = Some(q.pop());
                    validating.store(false, Ordering::Relaxed);
                    // Like YATP finding work in validate, abort the park and
                    // pass ownership back to the runner outside the bucket lock.
                    false
                },
                || {},
                |_, _| {},
                parking_lot_core::ParkToken(1),
                None,
            );
        }
        let was_ready = matches!(popped.as_ref(), Some(PopResult::Ready(_)));
        // Consume PREPARED and drop the task outside validation. Even when
        // cancelled, its dispatch reservation must be settled exactly once.
        let guard = DispatchGuard::install();
        drop(popped);
        drop(guard);
        q.drain();

        assert_eq!(calls_in_validation.load(Ordering::Relaxed), 0);
        assert_eq!(drops_in_validation.load(Ordering::Relaxed), 0);
        assert!(was_ready);
        assert_eq!(drops.load(Ordering::Relaxed), 1);
        let stats = q.snapshot();
        assert_eq!(stats.live_tasks, 0);
        assert_eq!(stats.queued, 0);
        assert_eq!(stats.active_dispatches, 0);
        assert_eq!(stats.reserved_vt, 0);
    }

    #[test]
    fn retired_group_reselects_priority_and_uses_current_default_weight() {
        let q = queue(&[20_000, 1_000, 10_000]);
        let default = q.shared.provider.group("g0");
        let retired = q.shared.provider.group("g1");
        // These updates happen after queue construction: caching the default
        // identity must not freeze its policy values.
        default.update(4, 20_000);
        retired.update(16, 1_000);
        q.push(task(&q, 1, 0, Some(2)).0);
        q.push(task(&q, 2, 0, Some(0)).0);
        retired.retire();

        // The retired priority-16 entry is initially first. Moving it into
        // default's priority-4 lane must allow priority 8 to run before it.
        assert_eq!(dispatch(&q, Duration::from_millis(1), false), (2, 0));
        {
            let state = q.shared.state.lock().unwrap();
            assert_eq!(state.live, 1);
            assert_eq!(state.groups[&1].live, 1);
            assert_eq!(state.groups[&0].live, 0);
        }
        default.update(4, 40_000);
        let PopResult::Ready(mut pop) = q.pop() else {
            panic!("the migrated task should remain ready");
        };
        let guard = DispatchGuard::install();
        assert_eq!(pop.task_cell.mut_extras().current_level(), 2);
        let lane_id = (4, 0);
        let expected_reservation = weighted(q.shared.config.slice_precharge, 40_000);
        let expected_charge = weighted(Duration::from_millis(3), 40_000);
        let before = q.shared.state.lock().unwrap().lanes[&lane_id].committed;
        ACTIVE.with(|active| {
            let active = active.borrow();
            let mut d = active.as_ref().unwrap().borrow_mut();
            assert_eq!(d.lane, lane_id);
            assert_eq!(d.weight, 40_000);
            assert_eq!(d.reserved, expected_reservation);
            assert_eq!(d.record.group.id(), 1);
            assert!(Arc::ptr_eq(&d.policy, &default));
            d.shared.settle(d.lane, d.reserved, expected_charge);
            d.reserved = 0;
            d.poll_active = false;
        });
        assert_eq!(
            q.shared.state.lock().unwrap().lanes[&lane_id].committed,
            before + expected_charge
        );
        drop(pop.task_cell);
        drop(guard);
        {
            let state = q.shared.state.lock().unwrap();
            assert!(!state.groups.contains_key(&1));
            assert_eq!(state.groups[&0].live, 0);
        }
        let stats = q.snapshot();
        assert_eq!(stats.live_tasks, 0);
        assert_eq!(stats.active_dispatches, 0);
        assert_eq!(stats.reserved_vt, 0);

        // A per-request override is independent of either group's priority.
        let q = queue(&[20_000, 1_000, 10_000]);
        q.shared.provider.group("g0").update(4, 40_000);
        q.push(task(&q, 1, 12, Some(1)).0);
        q.push(task(&q, 2, 0, Some(0)).0);
        q.shared.provider.group("g1").retire();
        assert_eq!(dispatch(&q, Duration::from_millis(1), false), (0, 1));
        assert_eq!(dispatch(&q, Duration::from_millis(1), false), (2, 0));
        assert_eq!(q.snapshot().live_tasks, 0);
    }

    #[test]
    fn retired_group_cancellation_wakes_waiter_in_same_pool() {
        use super::super::{DefaultTicker, YatpPoolBuilder};

        let q = queue(&[10_000, 10_000]);
        let pool = YatpPoolBuilder::new(DefaultTicker::default())
            .thread_count(4, 4, 4)
            .build_fair_pool(q.clone());
        let (senders, receivers): (Vec<_>, Vec<_>) = (0..128)
            .map(|_| futures::channel::oneshot::channel::<()>())
            .unzip();
        let permit = q.try_acquire(&TaskMetadata::default()).unwrap();
        let mut extras = Extras::multilevel_default();
        extras.set_metadata(permit.metadata(vec![]));
        let (started_tx, started_rx) = std::sync::mpsc::channel();
        let (done_tx, done_rx) = std::sync::mpsc::channel();
        pool.spawn(TaskCell::new(
            permit.wrap(async move {
                let mut waiting = Box::pin(futures::future::join_all(receivers));
                let mut started = Some(started_tx);
                futures::future::poll_fn(move |cx| {
                    let result = waiting.as_mut().poll(cx);
                    if let Some(started) = started.take() {
                        // The receivers now hold real wakers for this pool.
                        started.send(()).unwrap();
                    }
                    result
                })
                .await;
                done_tx.send(()).unwrap();
            }),
            extras,
        ));
        started_rx.recv_timeout(Duration::from_secs(5)).unwrap();
        // Keep the sender tasks queued until their group has been retired.
        q.shared.cpu.lock().unwrap().balance_ns = -1e15;
        let metadata = TaskMetadata::from_ctx(&ResourceControlContext {
            resource_group_name: "g1".into(),
            ..Default::default()
        });
        let mut cancellations = Vec::new();
        for sender in senders {
            let permit = q.try_acquire(&metadata).unwrap();
            cancellations.push(permit.cancellation());
            let mut extras = Extras::multilevel_default();
            extras.set_metadata(permit.metadata(metadata.clone().to_vec()));
            pool.spawn(TaskCell::new(
                permit.wrap(async move { drop(sender) }),
                extras,
            ));
        }
        q.shared.provider.group("g1").retire();
        let cancelling = thread::spawn(move || {
            for cancellation in cancellations {
                cancellation.cancel();
                thread::yield_now();
            }
        });
        q.shared.cpu.lock().unwrap().balance_ns = 1e10;
        done_rx.recv_timeout(Duration::from_secs(5)).unwrap();
        cancelling.join().unwrap();
        pool.shutdown();
        let stats = q.snapshot();
        assert_eq!(stats.queued, 0);
        assert_eq!(stats.live_tasks, 0);
        assert_eq!(stats.active_dispatches, 0);
        assert_eq!(stats.reserved_vt, 0);
    }

    #[test]
    fn dispatch_parallelism_tracks_lifetime_and_online_changes() {
        let q = queue(&[10_000]);
        q.set_parallelism(1);
        let (first, cancel_first) = task(&q, 0, 0, None);
        q.push(first);
        q.push(task(&q, 0, 0, None).0);
        q.push(task(&q, 0, 0, None).0);

        let PopResult::Ready(first) = q.pop() else {
            panic!("first dispatch should be available");
        };
        let first_guard = DispatchGuard::install();
        assert_eq!(q.snapshot().active_dispatches, 1);
        assert!(matches!(q.pop(), PopResult::Pending { .. }));
        // A full parallelism limit must not suppress the preemption hint.
        assert!(q.has_ready_task());

        // Settling one poll and beginning an inline repoll retain the same
        // dispatch slot. Only returning to the scheduler releases it.
        ACTIVE.with(|active| {
            let active = active.borrow();
            active.as_ref().unwrap().borrow_mut().settle();
        });
        assert_eq!(q.snapshot().active_dispatches, 1);
        ACTIVE.with(|active| {
            let active = active.borrow();
            active.as_ref().unwrap().borrow_mut().begin_poll();
        });
        assert!(matches!(q.pop(), PopResult::Pending { .. }));

        q.set_parallelism(2);
        let PopResult::Ready(second) = q.pop() else {
            panic!("raising parallelism should permit another dispatch");
        };
        let second_guard = DispatchGuard::install();
        assert_eq!(q.snapshot().active_dispatches, 2);
        q.set_parallelism(1);
        assert!(matches!(q.pop(), PopResult::Pending { .. }));
        drop(second.task_cell);
        drop(second_guard);
        assert_eq!(q.snapshot().active_dispatches, 1);
        assert!(matches!(q.pop(), PopResult::Pending { .. }));

        // Cancellation may release live-task capacity, but cannot release the
        // slot of a dispatch that still owns a task cell or a running poll.
        cancel_first.cancel();
        drop(first.task_cell);
        assert_eq!(q.snapshot().active_dispatches, 1);
        assert!(matches!(q.pop(), PopResult::Pending { .. }));
        drop(first_guard);
        assert_eq!(q.snapshot().active_dispatches, 0);
        dispatch(&q, Duration::from_micros(1), false);
        assert_eq!(q.snapshot().active_dispatches, 0);
        assert_eq!(q.snapshot().live_tasks, 0);
        assert!(matches!(q.pop(), PopResult::Empty));
    }

    #[test]
    fn concurrent_admit_dispatch_cancel_and_drain() {
        let q = queue(&[10_000; 16]);
        q.set_max_tasks(64);
        q.set_parallelism(2);
        let barrier = Arc::new(Barrier::new(9));
        let finished = Arc::new(AtomicUsize::new(0));
        let mut threads = Vec::new();
        for worker in 0..8 {
            let (q, barrier, finished) = (q.clone(), barrier.clone(), finished.clone());
            threads.push(thread::spawn(move || {
                barrier.wait();
                for i in 0..1_000 {
                    let ctx = ResourceControlContext {
                        resource_group_name: format!("g{}", (worker + i) % 16),
                        ..Default::default()
                    };
                    if let Some(permit) = q.try_acquire(&TaskMetadata::from_ctx(&ctx)) {
                        let cancel = permit.cancellation();
                        let mut extras = Extras::multilevel_default();
                        extras.set_metadata(permit.metadata(TaskMetadata::from_ctx(&ctx).to_vec()));
                        q.push(TaskCell::new(permit.wrap(async {}), extras));
                        if i % 3 == 0 {
                            cancel.cancel();
                        }
                    }
                    if let PopResult::Ready(pop) = q.pop() {
                        let guard = DispatchGuard::install();
                        assert!(q.snapshot().active_dispatches <= 2);
                        thread::yield_now();
                        drop(pop.task_cell);
                        drop(guard);
                        finished.fetch_add(1, Ordering::Relaxed);
                    }
                }
            }));
        }
        barrier.wait();
        for thread in threads {
            thread.join().unwrap();
        }
        q.drain();
        let stats = q.snapshot();
        assert_eq!(stats.queued, 0);
        assert_eq!(stats.live_tasks, 0);
        assert_eq!(stats.active_dispatches, 0);
        assert_eq!(stats.reserved_vt, 0);
        assert!(finished.load(Ordering::Relaxed) > 0);
    }

    #[test]
    fn real_pool_requeue_cancel_and_cpu_tail() {
        use super::super::{DefaultTicker, YatpPoolBuilder};
        let q = queue(&[10_000, 10_000]);
        let pool = YatpPoolBuilder::new(DefaultTicker::default())
            .thread_count(2, 2, 2)
            .build_fair_pool(q.clone());
        let (tx, rx) = std::sync::mpsc::channel();
        for group in 0..2 {
            let ctx = ResourceControlContext {
                resource_group_name: format!("g{group}"),
                ..Default::default()
            };
            let permit = q.try_acquire(&TaskMetadata::from_ctx(&ctx)).unwrap();
            let mut extras = Extras::multilevel_default();
            extras.set_metadata(permit.metadata(TaskMetadata::from_ctx(&ctx).to_vec()));
            let tx = tx.clone();
            pool.spawn(TaskCell::new(
                permit.wrap(async move {
                    for _ in 0..10 {
                        let start = Instant::now();
                        while start.elapsed() < Duration::from_micros(200) {
                            std::hint::spin_loop();
                        }
                        yatp::task::future::yield_to_scheduler().await;
                    }
                    tx.send(()).unwrap();
                }),
                extras,
            ));
        }
        for _ in 0..2 {
            rx.recv_timeout(Duration::from_secs(5)).unwrap();
        }
        pool.shutdown();
        assert_eq!(q.snapshot().reserved_vt, 0);
        assert_eq!(q.snapshot().live_tasks, 0);
        // Every slice is shorter than the checkpoint interval: tail accounting
        // must still consume tokens (no refill during this short workload).
        assert!(q.shared.cpu.lock().unwrap().balance_ns < 128.0 * BURST_WINDOW.as_nanos() as f64);
    }

    #[test]
    fn short_inline_polls_cannot_bypass_cpu_gate() {
        use super::super::{DefaultTicker, YatpPoolBuilder};
        let q = queue(&[10_000]);
        let pool = YatpPoolBuilder::new(DefaultTicker::default()).build_fair_pool(q.clone());
        let permit = q.try_acquire(&TaskMetadata::default()).unwrap();
        let cancel = permit.cancellation();
        let mut extras = Extras::multilevel_default();
        extras.set_metadata(permit.metadata(vec![]));
        let polls = Arc::new(AtomicUsize::new(0));
        let (q2, polls2) = (q.clone(), polls.clone());
        let (tx, rx) = std::sync::mpsc::channel();
        pool.spawn(TaskCell::new(
            permit.wrap(futures::future::poll_fn(move |cx| {
                if polls2.fetch_add(1, Ordering::SeqCst) == 0 {
                    q2.shared.cpu.lock().unwrap().balance_ns = -1e15;
                    tx.send(()).unwrap();
                }
                cx.waker().wake_by_ref();
                Poll::Pending
            })),
            extras,
        ));
        rx.recv_timeout(Duration::from_secs(5)).unwrap();
        thread::sleep(Duration::from_millis(30));
        assert_eq!(polls.load(Ordering::SeqCst), 1);
        cancel.cancel();
        pool.shutdown();
        assert_eq!(q.snapshot().live_tasks, 0);
        assert_eq!(q.snapshot().reserved_vt, 0);
    }

    #[test]
    fn shutdown_releases_async_waiters_with_live_queue_handle() {
        use super::super::{DefaultTicker, YatpPoolBuilder};
        let q = queue(&[10_000]);
        let pool = YatpPoolBuilder::new(DefaultTicker::default()).build_fair_pool(q.clone());
        let permit = q.try_acquire(&TaskMetadata::default()).unwrap();
        let mut extras = Extras::multilevel_default();
        extras.set_metadata(permit.metadata(vec![]));
        let (tx, rx) = std::sync::mpsc::channel();
        pool.spawn(TaskCell::new(
            permit.wrap(async move {
                tx.send(()).unwrap();
                futures::future::pending::<()>().await;
            }),
            extras,
        ));
        rx.recv_timeout(Duration::from_secs(5)).unwrap();
        pool.shutdown();
        assert_eq!(q.snapshot().live_tasks, 0);
        assert_eq!(q.snapshot().reserved_vt, 0);
        assert!(q.shared.state.lock().unwrap().records.is_empty());
    }

    #[test]
    fn poll_panic_releases_record_and_reservation() {
        let q = queue(&[10_000]);
        q.push(task(&q, 0, 0, None).0);
        let PopResult::Ready(pop) = q.pop() else {
            panic!();
        };
        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            let _dispatch = DispatchGuard::install();
            let record = ACTIVE.with(|a| a.borrow().as_ref().unwrap().borrow().record.clone());
            // An additional test-only permit exercises the exact poll wrapper
            // on an installed dispatch, without letting a worker panic escape.
            let permit = FairTaskPermit {
                record: Arc::downgrade(&record),
                id: record.id,
            };
            futures::executor::block_on(permit.wrap(async {
                panic!("test poll unwind");
            }));
        }));
        assert!(result.is_err());
        drop(pop.task_cell);
        assert_eq!(q.snapshot().active_dispatches, 0);
        assert_eq!(q.snapshot().reserved_vt, 0);
        assert_eq!(q.snapshot().live_tasks, 0);
        assert!(q.shared.state.lock().unwrap().records.is_empty());
    }

    struct CancelDropProbe(std::sync::mpsc::Sender<()>);

    impl Drop for CancelDropProbe {
        fn drop(&mut self) {
            let _ = self.0.send(());
        }
    }

    fn spawn_external_io_waiter(
        q: &Arc<FairQueue>,
        pool: &yatp::ThreadPool<TaskCell>,
    ) -> (
        FairCancellation,
        futures::channel::oneshot::Sender<()>,
        std::sync::mpsc::Receiver<()>,
    ) {
        let permit = q.try_acquire(&TaskMetadata::default()).unwrap();
        let cancellation = permit.cancellation();
        let mut extras = Extras::multilevel_default();
        extras.set_metadata(permit.metadata(vec![]));
        let (io_tx, mut io_rx) = futures::channel::oneshot::channel::<()>();
        let (polled_tx, polled_rx) = std::sync::mpsc::channel();
        let (dropped_tx, dropped_rx) = std::sync::mpsc::channel();
        let probe = CancelDropProbe(dropped_tx);
        pool.spawn(TaskCell::new(
            permit.wrap(async move {
                let _probe = probe;
                let mut polled_tx = Some(polled_tx);
                let _ = futures::future::poll_fn(move |cx| {
                    // The oneshot receiver stores the real YATP waker here.
                    let result = Pin::new(&mut io_rx).poll(cx);
                    assert!(result.is_pending());
                    if let Some(tx) = polled_tx.take() {
                        tx.send(()).unwrap();
                    }
                    result
                })
                .await;
            }),
            extras,
        ));
        polled_rx.recv_timeout(Duration::from_secs(5)).unwrap();
        // Ensure the first poll and dispatch have exited; this exercises an actual
        // IO-waiting task, rather than only cancellation racing its first poll.
        let deadline = Instant::now() + Duration::from_secs(5);
        loop {
            let idle = {
                let s = q.shared.state.lock().unwrap();
                s.locations.is_empty() && s.lanes.values().all(|lane| lane.in_flight == 0)
            };
            if idle {
                break;
            }
            assert!(Instant::now() < deadline, "IO waiter never became idle");
            thread::sleep(Duration::from_millis(1));
        }
        assert_eq!(q.snapshot().live_tasks, 1);
        (cancellation, io_tx, dropped_rx)
    }

    #[test]
    fn cancellation_drops_external_io_future_without_completing_io() {
        use super::super::{DefaultTicker, YatpPoolBuilder};

        let q = queue(&[10_000]);
        let pool = YatpPoolBuilder::new(DefaultTicker::default())
            .thread_count(1, 1, 1)
            .build_fair_pool(q.clone());
        let (cancel, io_tx, dropped_rx) = spawn_external_io_waiter(&q, &pool);
        // Cleanup must not require normal CPU-budget admission.
        q.shared.cpu.lock().unwrap().balance_ns = -1e15;
        cancel.cancel();
        dropped_rx.recv_timeout(Duration::from_secs(5)).unwrap();
        assert_eq!(q.snapshot().live_tasks, 0);
        assert_eq!(q.snapshot().reserved_vt, 0);
        assert_eq!(q.snapshot().queued, 0);
        assert!(q.shared.state.lock().unwrap().records.is_empty());
        // Keep the IO sender alive until after the DropProbe assertion. Its
        // receiver must already have been dropped by cancellation itself.
        assert!(io_tx.send(()).is_err());
        pool.shutdown();
    }

    #[test]
    fn shutdown_drops_external_io_future_with_live_queue_and_sender() {
        use super::super::{DefaultTicker, YatpPoolBuilder};

        let q = queue(&[10_000]);
        let pool = YatpPoolBuilder::new(DefaultTicker::default())
            .thread_count(1, 1, 1)
            .build_fair_pool(q.clone());
        let (_cancel, io_tx, dropped_rx) = spawn_external_io_waiter(&q, &pool);
        pool.shutdown();
        dropped_rx.recv_timeout(Duration::from_secs(5)).unwrap();
        assert_eq!(q.snapshot().live_tasks, 0);
        assert_eq!(q.snapshot().reserved_vt, 0);
        assert_eq!(q.snapshot().queued, 0);
        assert!(q.shared.state.lock().unwrap().records.is_empty());
        assert!(io_tx.send(()).is_err());
    }
    #[test]
    #[ignore = "manual full-poll contention measurement"]
    fn fair_pool_poll_benchmark() {
        use super::super::{DefaultTicker, YatpPoolBuilder};
        for workers in [1, 8, 32] {
            for groups in [1, 16, 256] {
                let q = queue(&vec![10_000; groups]);
                let pool = YatpPoolBuilder::new(DefaultTicker::default())
                    .thread_count(workers, workers, workers)
                    .build_fair_pool(q.clone());
                let tasks = groups.max(workers);
                let iterations = 20_000 / tasks;
                let (tx, rx) = std::sync::mpsc::channel();
                let start = Instant::now();
                for task in 0..tasks {
                    let ctx = ResourceControlContext {
                        resource_group_name: format!("g{}", task % groups),
                        ..Default::default()
                    };
                    let permit = q.try_acquire(&TaskMetadata::from_ctx(&ctx)).unwrap();
                    let mut extras = Extras::multilevel_default();
                    extras.set_metadata(permit.metadata(TaskMetadata::from_ctx(&ctx).to_vec()));
                    let tx = tx.clone();
                    pool.spawn(TaskCell::new(
                        permit.wrap(async move {
                            for _ in 0..iterations {
                                yatp::task::future::yield_to_scheduler().await;
                            }
                            tx.send(()).unwrap();
                        }),
                        extras,
                    ));
                }
                for _ in 0..tasks {
                    rx.recv_timeout(Duration::from_secs(30)).unwrap();
                }
                eprintln!(
                    "full-poll workers={workers} groups={groups} polls/s={:.0}",
                    (iterations + 1) as f64 * tasks as f64 / start.elapsed().as_secs_f64()
                );
                pool.shutdown();
                assert_eq!(q.snapshot().reserved_vt, 0);
                assert_eq!(q.snapshot().live_tasks, 0);
            }
        }
    }

    #[test]
    #[ignore = "manual queue contention measurement"]
    fn fair_queue_contention_benchmark() {
        for workers in [1, 8, 32] {
            for groups in [1, 16, 256] {
                let q = queue(&vec![10_000; groups]);
                for group in 0..groups {
                    for _ in 0..workers {
                        q.push(task(&q, group, 0, None).0);
                    }
                }
                let start = Instant::now();
                let mut threads = Vec::new();
                for _ in 0..workers {
                    let q = q.clone();
                    threads.push(thread::spawn(move || {
                        for _ in 0..10_000 {
                            dispatch(&q, Duration::from_millis(1), true);
                        }
                    }));
                }
                for thread in threads {
                    thread.join().unwrap();
                }
                eprintln!(
                    "workers={workers} groups={groups} dispatches/s={:.0}",
                    workers as f64 * 10_000.0 / start.elapsed().as_secs_f64()
                );
                q.drain();
            }
        }
    }
}
