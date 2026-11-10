// Copyright 2026 TiKV Project Authors. Licensed under Apache-2.0.

//! Serialized scheduling transitions: indexes, VT, capacity and task locations.
//! Task ownership may leave this module while locked; task destructors must not
//! run in these critical sections, nor in the YATP parking callback.

use std::{
    cmp::Reverse,
    collections::{BTreeSet, HashMap, HashSet},
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, Ordering},
    },
    time::{Duration, Instant},
};

use yatp::{queue::multilevel::TaskLevelManager, task::future::TaskCell};

use super::{
    FairQueueConfig, FairQueueStats, GroupPolicy, MAX_PRIORITY, Shared, effective_priority,
    task::{FairTaskPermit, TaskRecord},
    task_queues::{LEVEL_TIME_THRESHOLDS, TaskQueues},
};

// Fixed-point weighted nanoseconds, separate from the CPU gate's token units.
// Scaling preserves small charges even when a group has a very large weight.
const VT_SCALE: u128 = 1 << 32;
// A group at this weight receives the configured base task cap. Smaller
// weights scale it down, subject to the floor and the configured maximum.
const GROUP_CAP_BASE_WEIGHT: u128 = 10_000;
// Applied before the configured maximum, which may itself be below this floor.
const MIN_SCALED_GROUP_TASKS: u128 = 64;
// Index directly by business priority; slot zero is unused.
const PRIORITY_SLOTS: usize = MAX_PRIORITY as usize + 1;
// Dispatch completion can free a slot without a new enqueue. Return Pending
// with a short retry so surplus YATP workers wait instead of spinning or
// treating runnable work as an empty queue.
const PARALLELISM_RETRY_INTERVAL: Duration = Duration::from_millis(1);
// (Effective business priority, stable resource-group identity).
// One resource group can participate in multiple priority domains.
pub(super) type QueueKey = (u32, u64);
type ReadyKey = (Reverse<u32>, u128, u64);

// Reservation and settlement use the same captured weight even if
// resource-group metadata changes during execution.
pub(super) fn weighted_vt(cost: Duration, weight: u64) -> u128 {
    cost.as_nanos() * VT_SCALE / weight.max(1) as u128
}

/// Shared policy, task history and admission count for one resource group.
struct GroupState {
    policy: Arc<GroupPolicy>,
    // Reusable task-id history shared across this RG generation's priorities.
    history: Arc<TaskLevelManager>,
    // Admitted tasks, including running and IO-waiting tasks. Retirement
    // redirects queued work but leaves this admission ownership unchanged.
    live_tasks: usize,
}

/// Original YATP task cell plus its admission and cancellation record.
pub(super) struct FairTaskCell {
    pub(super) task_cell: TaskCell,
    pub(super) record: Arc<TaskRecord>,
}

/// Queue and scheduling progress for one (priority, resource-group id).
/// It owns VT and queued tasks for that priority; admission capacity and task
/// history remain shared in GroupState.
#[derive(Default)]
struct GroupQueue {
    // Settled weighted wall time plus reference placement; excludes precharge.
    committed_vt: u128,
    // Refundable precharges for polls that have not settled yet.
    reserved_vt: u128,
    // Runner-owned dispatches, retained across inline polls until Dispatch Drop.
    active_dispatches: usize,
    task_queues: TaskQueues,
}

impl GroupQueue {
    fn is_empty(&self) -> bool {
        self.task_queues.is_empty()
    }
    fn ready_key(&self, id: QueueKey) -> ReadyKey {
        (
            Reverse(id.0),
            self.committed_vt.saturating_add(self.reserved_vt),
            id.1,
        )
    }
    fn committed_key(&self, id: QueueKey) -> ReadyKey {
        (Reverse(id.0), self.committed_vt, id.1)
    }
}

struct State {
    // Business RG capacity and MLFQ history are shared across priorities.
    group_states: HashMap<u64, GroupState>,
    group_queues: HashMap<QueueKey, GroupQueue>,
    // Nonempty group queues ordered by strict priority, committed + reserved VT, id.
    ready: BTreeSet<ReadyKey>,
    // The same nonempty group queues indexed without refundable reservations.
    committed_index: BTreeSet<ReadyKey>,
    // Monotonic placement floor per priority; idle groups cannot bank credit.
    reference_vt: [u128; PRIORITY_SLOTS],
    // Owns live TaskRecords even while their tasks wait outside the runnable queue.
    records: HashMap<u64, Arc<TaskRecord>>,
    // Only queued tasks: scheduler id -> (group queue, MLFQ level, enqueue sequence).
    locations: HashMap<u64, (QueueKey, usize, u64)>,
    next_id: u64,
    sequence: u64,
    // Counts admitted tasks only; internal unadmitted tasks also have TaskRecords.
    live_tasks: usize,
    max_tasks: usize,
    parallelism: usize,
    // Bounds dispatches, independently of YATP's prestarted physical workers.
    active_dispatches: usize,
    evicted: u64,
    closed: bool,
}

impl State {
    // Remove both index entries before changing a key or queue emptiness.
    // Every caller restores the group queue with put_group_queue before releasing
    // the lock.
    fn take_group_queue(&mut self, id: QueueKey) -> GroupQueue {
        let group_queue = self.group_queues.remove(&id).unwrap_or_else(|| GroupQueue {
            committed_vt: self.reference_vt[id.0 as usize],
            ..GroupQueue::default()
        });
        if !group_queue.is_empty() {
            self.ready.remove(&group_queue.ready_key(id));
            self.committed_index.remove(&group_queue.committed_key(id));
        }
        group_queue
    }
    // Empty group queues retain their debt and in-flight ledger but have no ready
    // key.
    fn put_group_queue(&mut self, id: QueueKey, group_queue: GroupQueue) {
        if !group_queue.is_empty() {
            self.ready.insert(group_queue.ready_key(id));
            self.committed_index.insert(group_queue.committed_key(id));
        }
        self.group_queues.insert(id, group_queue);
    }
    // Advance from the minimum committed VT in this priority only. Precharge
    // must not move this floor: a later refund cannot roll the floor back.
    fn advance_reference(&mut self, priority: u32) {
        if let Some(&(Reverse(p), vt, _)) = self
            .committed_index
            .range((Reverse(priority), 0, 0)..)
            .next()
            && p == priority
        {
            self.reference_vt[priority as usize] = self.reference_vt[priority as usize].max(vt);
        }
    }
    /// Inserts an owned entry without resolving policies, touching task
    /// history, or invoking task destructors. The destination group must
    /// already exist.
    fn enqueue(&mut self, id: QueueKey, level: usize, entry: FairTaskCell, continuation: bool) {
        let mut group_queue = self.take_group_queue(id);
        if group_queue.is_empty() && group_queue.active_dispatches == 0 && !continuation {
            group_queue.committed_vt = group_queue
                .committed_vt
                .max(self.reference_vt[id.0 as usize]);
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
        group_queue.task_queues.push(level, sequence, entry);
        self.put_group_queue(id, group_queue);
        self.advance_reference(id.0);
    }
    fn remove_queued(&mut self, task_id: u64) -> Option<FairTaskCell> {
        let (id, level, seq) = self.locations.remove(&task_id)?;
        let mut group_queue = self.take_group_queue(id);
        let entry = group_queue.task_queues.remove(level, seq);
        self.put_group_queue(id, group_queue);
        self.advance_reference(id.0);
        entry
    }
    fn cleanup_retired(&mut self, id: u64) {
        if self
            .group_states
            .get(&id)
            .is_some_and(|g| g.policy.is_retired() && g.live_tasks == 0)
            && self.group_queues.iter().all(|(queue_key, group_queue)| {
                queue_key.1 != id || (group_queue.is_empty() && group_queue.active_dispatches == 0)
            })
        {
            self.group_queues.retain(|queue_key, _| queue_key.1 != id);
            self.group_states.remove(&id);
        }
    }

    fn get_or_create_group_state(&mut self, policy: &Arc<GroupPolicy>) -> &mut GroupState {
        if !self.group_states.contains_key(&policy.id()) {
            // Sweep retired idle identities on group creation, not per pop.
            // One pass avoids quadratic work after a bulk metadata deletion.
            let active: HashSet<_> = self
                .group_queues
                .iter()
                .filter(|(_, group_queue)| {
                    !group_queue.is_empty() || group_queue.active_dispatches != 0
                })
                .map(|(id, _)| id.1)
                .collect();
            let retired: HashSet<_> = self
                .group_states
                .iter()
                .filter(|(id, g)| {
                    g.policy.is_retired() && g.live_tasks == 0 && !active.contains(id)
                })
                .map(|(id, _)| *id)
                .collect();
            self.group_states.retain(|id, _| !retired.contains(id));
            self.group_queues.retain(|id, _| !retired.contains(&id.1));
        }
        self.group_states
            .entry(policy.id())
            .or_insert_with(|| GroupState {
                policy: policy.clone(),
                history: Arc::new(TaskLevelManager::new(
                    LEVEL_TIME_THRESHOLDS,
                    Some(super::super::DEFAULT_CLEANUP_INTERVAL),
                )),
                live_tasks: 0,
            })
    }
}

impl State {
    fn new_record(
        &mut self,
        shared: &Arc<Shared>,
        group: Arc<GroupPolicy>,
        history: Arc<TaskLevelManager>,
        override_priority: u32,
        counted: bool,
    ) -> Arc<TaskRecord> {
        let id = self.next_id;
        self.next_id = self
            .next_id
            .checked_add(1)
            .expect("fair task identity exhausted");
        let record = TaskRecord::new(
            id,
            group,
            history,
            override_priority,
            Arc::downgrade(shared),
            counted,
        );
        self.records.insert(id, record.clone());
        record
    }
}

pub(super) struct ScheduledTask {
    pub(super) entry: FairTaskCell,
    pub(super) policy: Arc<GroupPolicy>,
    pub(super) queue_key: QueueKey,
    pub(super) weight: u64,
    pub(super) reserved_vt: u128,
}

pub(super) enum Selection {
    Empty,
    Pending {
        retry_at: Instant,
        cpu_limited: bool,
    },
    // A retired entry moved to default. Recheck CPU and choose again.
    Retry,
    Ready(ScheduledTask),
}

pub(super) struct Scheduler {
    state: Mutex<State>,
    queued_nonempty: AtomicBool,
}

impl Scheduler {
    pub(super) fn new(default_policy: &Arc<GroupPolicy>, config: FairQueueConfig) -> Self {
        let mut state = State {
            group_states: HashMap::new(),
            group_queues: HashMap::new(),
            ready: BTreeSet::new(),
            committed_index: BTreeSet::new(),
            reference_vt: [0; PRIORITY_SLOTS],
            records: HashMap::new(),
            locations: HashMap::new(),
            next_id: 1,
            sequence: 0,
            live_tasks: 0,
            max_tasks: config.max_tasks,
            parallelism: usize::MAX,
            active_dispatches: 0,
            evicted: 0,
            closed: false,
        };
        // pop() can run under YATP's parking lock. Resolve and register default
        // before starting workers so retirement never invokes the provider or
        // creates/sweeps group histories from that context.
        state.get_or_create_group_state(default_policy);
        Self {
            state: Mutex::new(state),
            queued_nonempty: AtomicBool::new(false),
        }
    }

    // Call after one successful insertion/removal, under the scheduler lock.
    // Derive transitions from locations instead of reading the atomic on every
    // queue operation. Retirement transfers keep the hint unchanged.
    fn publish_added(&self, state: &State) {
        if state.locations.len() == 1 {
            self.queued_nonempty.store(true, Ordering::Release);
        }
    }

    fn publish_removed(&self, state: &State) {
        if state.locations.is_empty() {
            self.queued_nonempty.store(false, Ordering::Release);
        }
    }

    pub(super) fn has_queued_task(&self) -> bool {
        self.queued_nonempty.load(Ordering::Acquire)
    }

    pub(super) fn set_max_tasks(&self, limit: usize) {
        self.state.lock().unwrap().max_tasks = limit;
    }
    pub(super) fn set_parallelism(&self, limit: usize) {
        self.state.lock().unwrap().parallelism = limit;
    }
    pub(super) fn snapshot(&self) -> FairQueueStats {
        let s = self.state.lock().unwrap();
        FairQueueStats {
            queued: s.locations.len(),
            live_tasks: s.live_tasks,
            active_dispatches: s.active_dispatches,
            reserved_vt: s.group_queues.values().map(|queue| queue.reserved_vt).sum(),
            evicted_tasks: s.evicted,
        }
    }
    pub(super) fn record(&self, id: u64) -> Option<Arc<TaskRecord>> {
        self.state.lock().unwrap().records.get(&id).cloned()
    }

    pub(super) fn try_acquire(
        &self,
        shared: &Arc<Shared>,
        policy: Arc<GroupPolicy>,
        override_priority: u32,
    ) -> Option<FairTaskPermit> {
        let priority = effective_priority(&policy, override_priority);
        let cap = ((shared.config.max_tasks_per_group as u128 * policy.weight() as u128
            / GROUP_CAP_BASE_WEIGHT)
            .max(MIN_SCALED_GROUP_TASKS)
            .min(shared.config.max_tasks_per_group as u128)) as usize;
        // One eviction attempt: contention may consume the freed slot. Never
        // overcommit either cap, or repeatedly evict unrelated work to chase it.
        for attempt in 0..2 {
            let mut s = self.state.lock().unwrap();
            if s.closed {
                return None;
            }
            let group_full = s.get_or_create_group_state(&policy).live_tasks >= cap;
            if !group_full && s.live_tasks < s.max_tasks {
                s.live_tasks += 1;
                let group = s.get_or_create_group_state(&policy);
                group.live_tasks += 1;
                let history = group.history.clone();
                let record = s.new_record(shared, policy.clone(), history, override_priority, true);
                return Some(FairTaskPermit::new(&record));
            }
            if attempt != 0 {
                if group_full {
                    shared.metrics.rejected_group.inc();
                } else {
                    shared.metrics.rejected_global.inc();
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
                    shared.metrics.rejected_group.inc();
                } else {
                    shared.metrics.rejected_global.inc();
                }
                return None;
            };
            let victim_id = s.group_queues[&(p, group)]
                .task_queues
                .eviction_candidate()
                .unwrap();
            let victim = s.remove_queued(victim_id);
            if victim.is_some() {
                self.publish_removed(&s);
            }
            s.evicted += 1;
            shared.metrics.evicted_tasks.inc();
            drop(s);
            // Task destructors release permits and can reenter this queue.
            if let Some(victim) = victim {
                victim.record.cancel();
                drop(victim);
            }
        }
        None
    }

    pub(super) fn create_internal_record(
        &self,
        shared: &Arc<Shared>,
        policy: Arc<GroupPolicy>,
        override_priority: u32,
    ) -> Arc<TaskRecord> {
        let mut s = self.state.lock().unwrap();
        let history = s.get_or_create_group_state(&policy).history.clone();
        s.new_record(shared, policy, history, override_priority, false)
    }

    pub(super) fn enqueue(
        &self,
        policy: &Arc<GroupPolicy>,
        level: usize,
        entry: FairTaskCell,
        continuation: bool,
    ) -> Result<(), FairTaskCell> {
        let mut s = self.state.lock().unwrap();
        if s.closed
            || entry.record.cancelled.load(Ordering::Acquire)
            || entry.record.released.load(Ordering::Acquire)
        {
            return Err(entry);
        }
        let queue_key = (
            effective_priority(policy, entry.record.override_priority),
            policy.id(),
        );
        s.get_or_create_group_state(policy);
        s.enqueue(queue_key, level, entry, continuation);
        self.publish_added(&s);
        Ok(())
    }

    pub(super) fn pop(
        &self,
        retry_at: Option<Instant>,
        default_policy: &Arc<GroupPolicy>,
        precharge: Duration,
    ) -> Selection {
        let mut s = self.state.lock().unwrap();
        let Some(&(Reverse(priority), _, group)) = s.ready.first() else {
            return Selection::Empty;
        };
        if let Some(retry_at) = retry_at {
            return Selection::Pending {
                retry_at,
                cpu_limited: true,
            };
        }
        if s.active_dispatches >= s.parallelism {
            return Selection::Pending {
                retry_at: Instant::now() + PARALLELISM_RETRY_INTERVAL,
                cpu_limited: false,
            };
        }
        let queue_key = (priority, group);
        let mut group_queue = s.take_group_queue(queue_key);
        let (level, entry) = group_queue.task_queues.pop().unwrap();
        s.locations.remove(&entry.record.id);
        if entry.record.group.is_retired() && group == entry.record.group.id() {
            s.put_group_queue(queue_key, group_queue);
            s.advance_reference(priority);
            let target = (
                effective_priority(default_policy, entry.record.override_priority),
                default_policy.id(),
            );
            // Do not call push/cancel/history or destroy the task here: the
            // caller may still be holding YATP's outer parking lock.
            s.enqueue(target, level, entry, false);
            // Migration keeps the queue nonempty; publish no intermediate state.
            return Selection::Retry;
        }
        let policy = s.group_states[&group].policy.clone();
        let weight = policy.weight();
        let reserved_vt = weighted_vt(precharge, weight);
        group_queue.reserved_vt = group_queue.reserved_vt.saturating_add(reserved_vt);
        group_queue.active_dispatches += 1;
        s.active_dispatches += 1;
        s.put_group_queue(queue_key, group_queue);
        s.advance_reference(priority);
        self.publish_removed(&s);
        Selection::Ready(ScheduledTask {
            entry,
            policy,
            queue_key,
            weight,
            reserved_vt,
        })
    }

    pub(super) fn reserve(&self, queue_key: QueueKey, reserved_vt: u128) {
        let mut s = self.state.lock().unwrap();
        let mut group_queue = s.take_group_queue(queue_key);
        group_queue.reserved_vt = group_queue.reserved_vt.saturating_add(reserved_vt);
        s.put_group_queue(queue_key, group_queue);
    }

    pub(super) fn settle(&self, queue_key: QueueKey, reserved_vt: u128, actual_vt: u128) {
        let mut s = self.state.lock().unwrap();
        let mut group_queue = s.take_group_queue(queue_key);
        // Remove this poll's exact precharge, then commit its measured cost.
        // Other workers' reservations remain; settlement does not free a slot.
        group_queue.reserved_vt = group_queue
            .reserved_vt
            .checked_sub(reserved_vt)
            .expect("unbalanced fair reservation");
        group_queue.committed_vt = group_queue.committed_vt.saturating_add(actual_vt);
        s.put_group_queue(queue_key, group_queue);
        s.advance_reference(queue_key.0);
    }

    // Called after final poll settlement, once per Dispatch Drop. Cancellation
    // and inline repolls must not release the slot while the runner still owns it.
    // The returned flag requests internal-record release outside this lock.
    pub(super) fn finish_dispatch(&self, queue_key: QueueKey, record: &TaskRecord) -> bool {
        let mut s = self.state.lock().unwrap();
        s.active_dispatches = s
            .active_dispatches
            .checked_sub(1)
            .expect("unbalanced fair dispatch");
        let mut group_queue = s.take_group_queue(queue_key);
        group_queue.active_dispatches -= 1;
        // The final completion must advance an empty domain as well. Never
        // advance using a reservation, which is refundable rather than service.
        let no_ready = s
            .committed_index
            .range((Reverse(queue_key.0), 0, 0)..)
            .next()
            .is_none_or(|k| k.0.0 != queue_key.0);
        if group_queue.is_empty() && no_ready {
            s.reference_vt[queue_key.0 as usize] =
                s.reference_vt[queue_key.0 as usize].max(group_queue.committed_vt);
        }
        s.put_group_queue(queue_key, group_queue);
        s.advance_reference(queue_key.0);
        s.cleanup_retired(queue_key.1);
        !record.counted && !s.locations.contains_key(&record.id)
    }

    pub(super) fn release(&self, record: &TaskRecord) {
        let mut s = self.state.lock().unwrap();
        s.records.remove(&record.id);
        if record.counted {
            s.live_tasks -= 1;
            s.group_states
                .get_mut(&record.group.id())
                .unwrap()
                .live_tasks -= 1;
        }
        s.cleanup_retired(record.group.id());
    }

    pub(super) fn remove_queued(&self, id: u64) -> Option<FairTaskCell> {
        let mut s = self.state.lock().unwrap();
        let entry = s.remove_queued(id);
        if entry.is_some() {
            self.publish_removed(&s);
        }
        entry
    }

    pub(super) fn close_and_drain(&self) -> (Vec<FairTaskCell>, Vec<Arc<TaskRecord>>) {
        let mut s = self.state.lock().unwrap();
        s.closed = true;
        let mut entries = Vec::with_capacity(s.locations.len());
        for group_queue in s.group_queues.values_mut() {
            entries.extend(group_queue.task_queues.drain());
        }
        s.ready.clear();
        s.committed_index.clear();
        s.locations.clear();
        self.queued_nonempty.store(false, Ordering::Release);
        (entries, s.records.values().cloned().collect())
    }
}

#[cfg(test)]
mod tests {
    use std::{
        future::Future,
        pin::Pin,
        sync::{Barrier, atomic::AtomicUsize},
        task::Poll,
        thread,
        time::Instant,
    };

    use kvproto::kvrpcpb::ResourceControlContext;
    use yatp::queue::{Extras, Pop};

    use super::{
        super::{execution::with_active_for_test, *},
        weighted_vt,
    };

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
        let group = with_active_for_test(|a| {
            let active = a.borrow();
            let mut d = active.as_ref().unwrap().borrow_mut();
            d.shared
                .scheduler
                .settle(d.queue_key, d.reserved_vt, weighted_vt(cost, d.weight));
            d.reserved_vt = 0;
            d.needs_settlement = false;
            d.queue_key.1
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
        let reference_vt = q.shared.scheduler.state.lock().unwrap().reference_vt[8];
        assert_eq!(reference_vt, weighted_vt(Duration::from_millis(10), 10_000));
        q.push(task(&q, 1, 0, None).0);
        assert_eq!(
            q.shared.scheduler.state.lock().unwrap().group_queues[&(8, 1)].committed_vt,
            reference_vt
        );
        let PopResult::Ready(pop) = q.pop() else {
            panic!();
        };
        let guard = DispatchGuard::install();
        q.push(task(&q, 2, 0, None).0);
        assert_eq!(
            q.shared.scheduler.state.lock().unwrap().group_queues[&(8, 2)].committed_vt,
            reference_vt
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
        let original = with_active_for_test(|a| a.borrow().as_ref().unwrap().borrow().weight);
        q.shared.provider.group("g0").update(8, 40_000);
        with_active_for_test(|a| {
            let active = a.borrow();
            let mut d = active.as_ref().unwrap().borrow_mut();
            assert_eq!(d.weight, original);
            d.shared.scheduler.settle(
                d.queue_key,
                d.reserved_vt,
                weighted_vt(Duration::from_millis(1), d.weight),
            );
            d.reserved_vt = 0;
            d.needs_settlement = false;
        });
        let before = q.shared.scheduler.state.lock().unwrap().group_queues[&(8, 0)].committed_vt;
        q.push(pop.task_cell);
        drop(guard);
        assert_eq!(
            q.shared.scheduler.state.lock().unwrap().group_queues[&(8, 0)].committed_vt,
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
        q.shared.cpu_budget.set_balance_ns(-1e15);
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
    fn deleted_group_migrates_and_old_generation_releases() {
        let q = queue(&[10_000, 10_000]);
        q.push(task(&q, 1, 0, None).0);
        q.shared.provider.group("g1").retire();
        assert_eq!(dispatch(&q, Duration::from_millis(1), false).0, 0);
        let state = q.shared.scheduler.state.lock().unwrap();
        assert!(!state.group_states.contains_key(&1));
        assert_eq!(state.live_tasks, 0);
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
        let metadata = permit.metadata(metadata.to_vec());
        let record = q
            .shared
            .scheduler
            .record(
                TaskMetadata::from(metadata.as_slice())
                    .scheduler_id()
                    .unwrap(),
            )
            .unwrap();
        let mut extras = Extras::multilevel_default();
        extras.set_metadata(metadata);
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
        // default's priority-4 group queue must allow priority 8 to run before it.
        assert_eq!(dispatch(&q, Duration::from_millis(1), false), (2, 0));
        {
            let state = q.shared.scheduler.state.lock().unwrap();
            assert_eq!(state.live_tasks, 1);
            assert_eq!(state.group_states[&1].live_tasks, 1);
            assert_eq!(state.group_states[&0].live_tasks, 0);
        }
        default.update(4, 40_000);
        let PopResult::Ready(mut pop) = q.pop() else {
            panic!("the migrated task should remain ready");
        };
        let guard = DispatchGuard::install();
        assert_eq!(pop.task_cell.mut_extras().current_level(), 2);
        let queue_key = (4, 0);
        let expected_reservation = weighted_vt(q.shared.config.slice_precharge, 40_000);
        let expected_charge = weighted_vt(Duration::from_millis(3), 40_000);
        let before = q.shared.scheduler.state.lock().unwrap().group_queues[&queue_key].committed_vt;
        with_active_for_test(|active| {
            let active = active.borrow();
            let mut d = active.as_ref().unwrap().borrow_mut();
            assert_eq!(d.queue_key, queue_key);
            assert_eq!(d.weight, 40_000);
            assert_eq!(d.reserved_vt, expected_reservation);
            assert_eq!(d.record.group.id(), 1);
            assert!(Arc::ptr_eq(&d.policy, &default));
            d.shared
                .scheduler
                .settle(d.queue_key, d.reserved_vt, expected_charge);
            d.reserved_vt = 0;
            d.needs_settlement = false;
        });
        assert_eq!(
            q.shared.scheduler.state.lock().unwrap().group_queues[&queue_key].committed_vt,
            before + expected_charge
        );
        drop(pop.task_cell);
        drop(guard);
        {
            let state = q.shared.scheduler.state.lock().unwrap();
            assert!(!state.group_states.contains_key(&1));
            assert_eq!(state.group_states[&0].live_tasks, 0);
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
        use crate::yatp_pool::{DefaultTicker, YatpPoolBuilder};

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
        q.shared.cpu_budget.set_balance_ns(-1e15);
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
        q.shared.cpu_budget.set_balance_ns(1e10);
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
        with_active_for_test(|active| {
            let active = active.borrow();
            active.as_ref().unwrap().borrow_mut().settle();
        });
        assert_eq!(q.snapshot().active_dispatches, 1);
        with_active_for_test(|active| {
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
        use crate::yatp_pool::{DefaultTicker, YatpPoolBuilder};
        let q = queue(&[10_000, 10_000]);
        let initial_balance = q.shared.cpu_budget.balance_ns();
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
        assert!(q.shared.cpu_budget.balance_ns() < initial_balance);
    }

    #[test]
    fn short_inline_polls_cannot_bypass_cpu_gate() {
        use crate::yatp_pool::{DefaultTicker, YatpPoolBuilder};
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
                    q2.shared.cpu_budget.set_balance_ns(-1e15);
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
        use crate::yatp_pool::{DefaultTicker, YatpPoolBuilder};
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
        assert!(q.shared.scheduler.state.lock().unwrap().records.is_empty());
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
            let record =
                with_active_for_test(|a| a.borrow().as_ref().unwrap().borrow().record.clone());
            // An additional test-only permit exercises the exact poll wrapper
            // on an installed dispatch, without letting a worker panic escape.
            let permit = FairTaskPermit::new(&record);
            futures::executor::block_on(permit.wrap(async {
                panic!("test poll unwind");
            }));
        }));
        assert!(result.is_err());
        drop(pop.task_cell);
        assert_eq!(q.snapshot().active_dispatches, 0);
        assert_eq!(q.snapshot().reserved_vt, 0);
        assert_eq!(q.snapshot().live_tasks, 0);
        assert!(q.shared.scheduler.state.lock().unwrap().records.is_empty());
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
                let s = q.shared.scheduler.state.lock().unwrap();
                s.locations.is_empty()
                    && s.group_queues
                        .values()
                        .all(|group_queue| group_queue.active_dispatches == 0)
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
        use crate::yatp_pool::{DefaultTicker, YatpPoolBuilder};

        let q = queue(&[10_000]);
        let pool = YatpPoolBuilder::new(DefaultTicker::default())
            .thread_count(1, 1, 1)
            .build_fair_pool(q.clone());
        let (cancel, io_tx, dropped_rx) = spawn_external_io_waiter(&q, &pool);
        // Cleanup must not require normal CPU-budget admission.
        q.shared.cpu_budget.set_balance_ns(-1e15);
        cancel.cancel();
        dropped_rx.recv_timeout(Duration::from_secs(5)).unwrap();
        assert_eq!(q.snapshot().live_tasks, 0);
        assert_eq!(q.snapshot().reserved_vt, 0);
        assert_eq!(q.snapshot().queued, 0);
        assert!(q.shared.scheduler.state.lock().unwrap().records.is_empty());
        // Keep the IO sender alive until after the DropProbe assertion. Its
        // receiver must already have been dropped by cancellation itself.
        assert!(io_tx.send(()).is_err());
        pool.shutdown();
    }

    #[test]
    fn shutdown_drops_external_io_future_with_live_queue_and_sender() {
        use crate::yatp_pool::{DefaultTicker, YatpPoolBuilder};

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
        assert!(q.shared.scheduler.state.lock().unwrap().records.is_empty());
        assert!(io_tx.send(()).is_err());
    }
    #[test]
    #[ignore = "manual full-poll contention measurement"]
    fn fair_pool_poll_benchmark() {
        use crate::yatp_pool::{DefaultTicker, YatpPoolBuilder};
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

    #[test]
    fn continuation_record_is_scoped_to_the_active_pool() {
        let a = queue(&[10_000]);
        let b = queue(&[10_000, 10_000]);

        // Keep B's first task alive outside its queue, with an idle group queue at VT
        // 0.
        b.push(task(&b, 0, 0, None).0);
        let PopResult::Ready(held_b) = b.pop() else {
            panic!("B's first task should be ready");
        };
        let b_guard = DispatchGuard::install();
        let record_b = with_active_for_test(|active| {
            let active = active.borrow();
            let mut d = active.as_ref().unwrap().borrow_mut();
            d.shared.scheduler.settle(d.queue_key, d.reserved_vt, 0);
            d.reserved_vt = 0;
            d.needs_settlement = false;
            d.record.clone()
        });
        drop(b_guard);
        b.push(task(&b, 1, 0, None).0);
        dispatch(&b, Duration::from_millis(10), false);
        let reference_vt = b.shared.scheduler.state.lock().unwrap().reference_vt[8];
        assert!(reference_vt > 0);
        assert_eq!(
            b.shared.scheduler.state.lock().unwrap().group_queues[&(8, 0)].committed_vt,
            0
        );

        // The two pools allocate the same first ID. Requeueing B from A's runner
        // must use B's registry and clamp B's idle group queue to its own reference.
        a.push(task(&a, 0, 0, None).0);
        let PopResult::Ready(held_a) = a.pop() else {
            panic!("A's first task should be ready");
        };
        let a_guard = DispatchGuard::install();
        let record_a = with_active_for_test(|active| {
            active.borrow().as_ref().unwrap().borrow().record.clone()
        });
        assert_eq!(record_a.id, record_b.id);
        assert!(Arc::ptr_eq(
            &execution::continuation_record(&a.shared, record_a.id).unwrap(),
            &record_a,
        ));
        assert!(execution::continuation_record(&b.shared, record_b.id).is_none());
        b.push(held_b.task_cell);
        {
            let state = b.shared.scheduler.state.lock().unwrap();
            assert_eq!(state.group_queues[&(8, 0)].committed_vt, reference_vt);
            assert!(Arc::ptr_eq(&state.records[&record_b.id], &record_b));
            assert_eq!(state.locations.len(), 1);
        }
        // The ordinary same-pool continuation still requeues successfully.
        a.push(held_a.task_cell);
        drop(a_guard);
        assert_eq!(a.snapshot().queued, 1);
        assert_eq!(b.snapshot().queued, 1);
        a.drain();
        b.drain();
        for q in [&a, &b] {
            assert_eq!(q.snapshot().live_tasks, 0);
            assert_eq!(q.snapshot().reserved_vt, 0);
        }
    }

    #[test]
    fn continuation_rechecks_cancel_and_close_after_saved_waker_drop() {
        use std::task::{Wake, Waker};

        struct ReenterOnDrop {
            queue: Arc<FairQueue>,
            cancel: FairCancellation,
            close: bool,
            drops: Arc<AtomicUsize>,
        }
        impl Wake for ReenterOnDrop {
            fn wake(self: Arc<Self>) {}
        }
        impl Drop for ReenterOnDrop {
            fn drop(&mut self) {
                // A waker destructor may reenter the runner and the scheduler.
                // Neither ACTIVE's borrow nor its dispatch borrow may escape the
                // continuation lookup into this callout.
                with_active_for_test(|active| {
                    let active = active
                        .try_borrow_mut()
                        .expect("ACTIVE borrow escaped lookup");
                    let _dispatch = active
                        .as_ref()
                        .unwrap()
                        .try_borrow_mut()
                        .expect("dispatch borrow escaped lookup");
                });
                if self.close {
                    self.queue.drain();
                } else {
                    self.cancel.cancel();
                }
                self.drops.fetch_add(1, Ordering::Relaxed);
            }
        }

        for close in [false, true] {
            let q = queue(&[10_000]);
            let (cell, cancel) = task(&q, 0, 0, None);
            q.push(cell);
            let PopResult::Ready(pop) = q.pop() else {
                panic!("task should be ready");
            };
            let guard = DispatchGuard::install();
            let record = with_active_for_test(|active| {
                active.borrow().as_ref().unwrap().borrow().record.clone()
            });
            let drops = Arc::new(AtomicUsize::new(0));
            let waker = Waker::from(Arc::new(ReenterOnDrop {
                queue: q.clone(),
                cancel,
                close,
                drops: drops.clone(),
            }));
            record.waker.register(&waker);
            drop(waker);

            // push first obtains the live Arc from ACTIVE, then takes and drops
            // the saved waker. Its callback releases that same TaskRecord before the
            // final enqueue, which must reject rather than resurrect the task.
            q.push(pop.task_cell);
            drop(guard);
            assert_eq!(drops.load(Ordering::Relaxed), 1);
            assert!(record.cancelled.load(Ordering::Acquire));
            assert!(record.released.load(Ordering::Acquire));
            let stats = q.snapshot();
            assert_eq!(stats.queued, 0);
            assert_eq!(stats.live_tasks, 0);
            assert_eq!(stats.active_dispatches, 0);
            assert_eq!(stats.reserved_vt, 0);
            assert!(!q.shared.scheduler.has_queued_task());
            assert!(!q.has_ready_task());
            assert!(matches!(q.pop(), PopResult::Empty));
            q.drain();
        }
    }

    #[test]
    fn ready_hint_tracks_pop_cancel_eviction_drain_and_cpu_gate() {
        let q = queue(&[10_000, 10_000]);
        q.set_max_tasks(1);
        assert!(!q.has_ready_task());
        q.push(task(&q, 0, 1, None).0);
        assert!(q.has_ready_task());
        let (high, cancel) = task(&q, 1, 16, None);
        // Admission evicted the last queued task, but the new permit has not
        // published its TaskCell yet. Live count is not a readiness hint.
        assert_eq!(q.snapshot().live_tasks, 1);
        assert_eq!(q.snapshot().evicted_tasks, 1);
        assert!(!q.shared.scheduler.has_queued_task());
        assert!(!q.has_ready_task());
        q.push(high);
        assert!(q.has_ready_task());
        q.shared.cpu_budget.set_balance_ns(-1e15);
        assert!(q.shared.scheduler.has_queued_task());
        assert!(!q.has_ready_task());
        assert!(matches!(q.pop(), PopResult::Pending { .. }));
        q.shared.cpu_budget.set_balance_ns(1e10);
        assert!(q.has_ready_task());
        cancel.cancel();
        assert!(!q.shared.scheduler.has_queued_task());
        assert!(!q.has_ready_task());

        q.push(task(&q, 0, 0, None).0);
        let PopResult::Ready(pop) = q.pop() else {
            panic!("the last queued task should be ready");
        };
        let guard = DispatchGuard::install();
        assert!(!q.shared.scheduler.has_queued_task());
        assert!(!q.has_ready_task());
        drop(pop.task_cell);
        drop(guard);
        q.push(task(&q, 0, 0, None).0);
        assert!(q.has_ready_task());
        q.drain();
        assert!(!q.shared.scheduler.has_queued_task());
        assert!(!q.has_ready_task());
        assert_eq!(q.snapshot().live_tasks, 0);
    }

    #[test]
    fn ready_hint_survives_retirement_and_full_parallelism() {
        let q = queue(&[10_000, 10_000]);
        q.set_parallelism(1);
        q.push(task(&q, 1, 0, None).0);
        q.shared.provider.group("g1").retire();
        assert!(q.has_ready_task());
        // Observe the intermediate scheduler transition before FairQueue::pop
        // retries: transferring the sole entry must publish a nonempty state.
        assert!(matches!(
            q.shared.scheduler.pop(
                None,
                &q.shared.default_policy,
                q.shared.config.slice_precharge
            ),
            Selection::Retry,
        ));
        assert!(q.shared.scheduler.has_queued_task());
        assert!(q.has_ready_task());
        assert_eq!(q.snapshot().queued, 1);
        q.push(task(&q, 0, 0, None).0);
        let PopResult::Ready(first) = q.pop() else {
            panic!("migrated task should be ready");
        };
        let guard = DispatchGuard::install();
        assert_eq!(q.snapshot().active_dispatches, 1);
        assert!(q.has_ready_task());
        assert!(matches!(q.pop(), PopResult::Pending { .. }));
        drop(first.task_cell);
        drop(guard);
        dispatch(&q, Duration::from_micros(1), false);
        assert!(!q.has_ready_task());
        assert_eq!(q.snapshot().live_tasks, 0);
        assert_eq!(q.snapshot().reserved_vt, 0);
    }
}
