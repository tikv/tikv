// Copyright 2026 TiKV Project Authors. Licensed under Apache-2.0.

//! Strict business priority, weighted group scheduling and per-group MLFQ.
//!
//! A dispatch first chooses the highest business priority with queued work,
//! then the group with the least committed + reserved VT within that priority,
//! and finally an MLFQ task in that group queue.
//! Selection and VT precharge share one scheduler lock so concurrent workers
//! see each other's reservations. Poll completion replaces the reservation
//! with weighted wall time; the separate CPU budget charges thread CPU time.
//!
//! Live admission permits also cover tasks suspended on IO. Queue length,
//! live requests and active dispatches therefore describe different states.
//! Scheduling transitions live in `scheduler`; task ownership and cancellation
//! in `task`; per-poll/dispatch accounting in `execution`. The YATP adapter
//! must keep provider/history/destructors outside scheduler and parking locks.

mod cpu_budget;
mod execution;
mod scheduler;
mod task;
mod task_queues;

use std::{
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicU32, AtomicU64, Ordering},
    },
    time::Duration,
};

use cpu_budget::CpuBudget;
pub(super) use execution::DispatchGuard;
pub use execution::{is_fair_cpu_dispatch_active, yield_if_fair_cpu_exhausted};
use scheduler::{FairTaskCell, Scheduler, Selection};
pub use task::{FairCancellation, FairTaskFuture, FairTaskPermit};
use yatp::{
    queue::{PopResult, TaskCell as _, TaskQueue},
    task::future::TaskCell,
};

use crate::resource_control::TaskMetadata;

// Larger business priorities run first. Zero is reserved in request metadata
// for inheriting the resource group's priority, never an effective scheduling
// priority.
const MIN_PRIORITY: u32 = 1;
const MAX_PRIORITY: u32 = 16;

/// Stable identity provided by resource control. Metadata reloads update in
/// place; deleting and recreating a group must allocate a new identity.
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
            priority: AtomicU32::new(priority.clamp(MIN_PRIORITY, MAX_PRIORITY)),
            weight: AtomicU64::new(weight),
            retired: AtomicBool::new(false),
        }
    }
    /// Updates independent policy values. A dispatch captures its charge
    /// weight so a later update cannot change that poll's settlement.
    pub fn update(&self, priority: u32, weight: u64) {
        assert!(weight > 0);
        self.weight.store(weight, Ordering::Relaxed);
        self.priority.store(
            priority.clamp(MIN_PRIORITY, MAX_PRIORITY),
            Ordering::Relaxed,
        );
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
    /// Maximum admitted live requests, including running and IO-waiting ones.
    pub max_tasks: usize,
    /// Upper bound of each group's weight-scaled live-request cap.
    pub max_tasks_per_group: usize,
    /// Base CPU capacity in core-equivalents; one core supplies 1 CPU ns/wall
    /// ns.
    pub cpu_cores: f64,
    /// Fraction of that capacity available to this pool, in (0, 1].
    pub cpu_ratio: f64,
    /// Wall time reserved in VT at selection, reconciled at poll completion.
    /// This reservation does not consume CPU tokens.
    pub slice_precharge: Duration,
    /// Cooperative wall-time slice, observed at task yield points rather than
    /// enforced by interrupting arbitrary synchronous work.
    pub max_time_slice: Duration,
}

#[derive(Debug, Default)]
pub struct FairQueueStats {
    /// Task cells currently waiting across all group queues.
    pub queued: usize,
    /// Admitted requests still owning capacity, including IO waiters.
    pub live_tasks: usize,
    /// Dispatch slots held until the runner returns, including inline repolls.
    pub active_dispatches: usize,
    /// Sum of outstanding weighted VT reservations, not a CPU-time measurement.
    pub reserved_vt: u128,
    /// Queued tasks evicted to admit strictly higher-priority requests.
    pub evicted_tasks: u64,
}

struct Shared {
    provider: Arc<dyn GroupProvider>,
    default_policy: Arc<GroupPolicy>,
    config: FairQueueConfig,
    scheduler: Scheduler,
    cpu_budget: CpuBudget,
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
        let scheduler = Scheduler::new(&default_policy, config);
        Arc::new(Self {
            shared: Arc::new(Shared {
                provider,
                default_policy,
                config,
                scheduler,
                cpu_budget: CpuBudget::new(config.cpu_cores * config.cpu_ratio),
                metrics: super::metrics::FairQueueMetrics::default(),
            }),
        })
    }

    pub fn cpu_budget_cores(&self) -> f64 {
        self.shared.config.cpu_cores * self.shared.config.cpu_ratio
    }

    pub fn set_max_tasks(&self, limit: usize) {
        self.shared.scheduler.set_max_tasks(limit);
    }

    /// Limits simultaneous dispatches independently of YATP's physical worker
    /// count. Lowering the limit lets existing dispatches finish before more
    /// work starts; it never interrupts a poll or an inline repoll.
    pub fn set_parallelism(&self, limit: usize) {
        assert!(limit > 0);
        self.shared.scheduler.set_parallelism(limit);
    }

    pub fn snapshot(&self) -> FairQueueStats {
        self.shared.scheduler.snapshot()
    }

    /// The permit covers the entire future, including asynchronous waiting.
    /// Only a strictly higher business priority can evict a queued task.
    pub fn try_acquire(self: &Arc<Self>, metadata: &TaskMetadata<'_>) -> Option<FairTaskPermit> {
        let policy = self
            .shared
            .provider
            .group(std::str::from_utf8(metadata.group_name()).unwrap_or("default"));
        self.shared
            .scheduler
            .try_acquire(&self.shared, policy, metadata.override_priority())
    }
}

fn effective_priority(policy: &GroupPolicy, override_priority: u32) -> u32 {
    if override_priority == 0 {
        policy.priority()
    } else {
        override_priority.clamp(MIN_PRIORITY, MAX_PRIORITY)
    }
}

impl TaskQueue<TaskCell> for FairQueue {
    fn push(&self, mut task: TaskCell) {
        let metadata = TaskMetadata::from(task.mut_extras().metadata());
        let id = metadata.scheduler_id();
        // A synchronous requeue already owns the same live TaskRecord. Pool
        // identity is essential: different pools can allocate the same task ID.
        let continuation = id.and_then(|id| execution::continuation_record(&self.shared, id));
        let is_continuation = continuation.is_some();
        let record = continuation.or_else(|| id.and_then(|id| self.shared.scheduler.record(id)));
        let record = match record {
            Some(record) => record,
            None if id.is_some() => {
                // A late wake after completion/cancellation is not a new task.
                drop(task);
                return;
            }
            None => {
                // Internal tasks have no admission wrapper, but still receive
                // a TaskRecord and the dispatch's fallback CPU/VT accounting.
                let policy = self
                    .shared
                    .provider
                    .group(std::str::from_utf8(metadata.group_name()).unwrap_or("default"));
                self.shared.scheduler.create_internal_record(
                    &self.shared,
                    policy,
                    metadata.override_priority(),
                )
            }
        };
        // TLS borrows and scheduler guards have ended before any reentrant
        // destruction or task-history work.
        drop(record.waker.take());
        let policy = if record.group.is_retired() {
            self.shared.default_policy.clone()
        } else {
            record.group.clone()
        };
        record.history.adjust_task_level(&mut task);
        let level = task.mut_extras().current_level() as usize;
        if let Err(entry) = self.shared.scheduler.enqueue(
            &policy,
            level,
            FairTaskCell {
                task_cell: task,
                record,
            },
            is_continuation,
        ) {
            entry.record.cancel();
            drop(entry);
        }
    }

    fn pop(&self) -> PopResult<TaskCell> {
        // May run inside YATP park.validate's parking_lot bucket lock. Do not
        // call the provider, history helpers, cancellation or task destructors,
        // even after the scheduler mutex has been released.
        loop {
            let retry_at = self.shared.cpu_budget.retry_at();
            let scheduled = match self.shared.scheduler.pop(
                retry_at,
                &self.shared.default_policy,
                self.shared.config.slice_precharge,
            ) {
                Selection::Empty => return PopResult::Empty,
                Selection::Pending {
                    retry_at,
                    cpu_limited,
                } => {
                    if cpu_limited {
                        self.shared.metrics.gate_pending.inc();
                    }
                    return PopResult::Pending { retry_at };
                }
                Selection::Retry => continue,
                Selection::Ready(scheduled) => scheduled,
            };
            return PopResult::Ready(execution::prepare_dispatch(&self.shared, scheduled));
        }
    }

    fn drain(&self) {
        let (entries, records) = self.shared.scheduler.close_and_drain();
        // Include async waiters; cancel/drop only after releasing scheduler.
        for record in records {
            record.cancel();
        }
        drop(entries);
    }

    fn has_ready_task(&self) -> bool {
        // Keep CPU-first lazy refill. A running task can occupy the last slot
        // and must still be able to yield to queued work; only pop gates slots.
        self.shared.cpu_budget.retry_at().is_none() && self.shared.scheduler.has_queued_task()
    }
}
