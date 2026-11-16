// Copyright 2026 TiKV Project Authors. Licensed under Apache-2.0.

//! Per-poll accounting and the longer-lived YATP dispatch context.

use std::{
    cell::RefCell,
    rc::Rc,
    sync::Arc,
    time::{Duration, Instant},
};

use cpu_time::ThreadTime;
use yatp::{
    queue::{Pop, TaskCell as _},
    task::future::TaskCell,
};

use super::{
    GroupPolicy, Shared,
    scheduler::{QueueKey, ScheduledTask, weighted_vt},
    task::TaskRecord,
};
use crate::{task_reschedule::PollRescheduleContextGuard, time::LazyInstant};

// The first mid-poll CPU sample becomes eligible after this much poll wall
// time. Later checkpoint calls in the same poll also sample; they do not reset
// this threshold or wait another interval since the previous checkpoint.
const CPU_CHECKPOINT_MIN_POLL_TIME: Duration = Duration::from_millis(1);

thread_local! {
    static PREPARED: RefCell<Option<Dispatch>> = const { RefCell::new(None) };
    static ACTIVE: RefCell<Option<Rc<RefCell<Dispatch>>>> = const { RefCell::new(None) };
}

#[cfg(test)]
pub(super) fn with_active_for_test<R>(
    f: impl FnOnce(&RefCell<Option<Rc<RefCell<Dispatch>>>>) -> R,
) -> R {
    ACTIVE.with(f)
}

// Exposed fields support white-box scheduler tests. Production code accesses
// dispatch state through this module's execution APIs.
pub(super) struct Dispatch {
    pub(super) policy: Arc<GroupPolicy>,
    pub(super) shared: Arc<Shared>,
    pub(super) record: Arc<TaskRecord>,
    pub(super) queue_key: QueueKey,
    // Capture the weight for one poll. Reservations are weighted VT, replaced
    // by weighted actual wall time at settlement; they do not charge CPU tokens.
    pub(super) weight: u64,
    pub(super) reserved_vt: u128,
    // Wall time charges group VT; thread CPU time charges the shared budget.
    wall_start: Instant,
    cpu_start: ThreadTime,
    // CPU already charged since cpu_start, excluding it from subsequent
    // checkpoints and the final tail charge.
    cpu_charged: Duration,
    // A poll or the pre-poll fallback still needs settlement.
    pub(super) needs_settlement: bool,
}

pub(super) fn prepare_dispatch(shared: &Arc<Shared>, scheduled: ScheduledTask) -> Pop<TaskCell> {
    let mut entry = scheduled.entry;
    let schedule_time = entry
        .task_cell
        .mut_extras()
        .schedule_time()
        .unwrap_or_else(Instant::now);
    PREPARED.with(|p| {
        assert!(
            p.borrow().is_none(),
            "fair dispatch was not consumed by runner"
        );
        *p.borrow_mut() = Some(Dispatch {
            shared: shared.clone(),
            record: entry.record,
            policy: scheduled.policy,
            queue_key: scheduled.queue_key,
            weight: scheduled.weight,
            reserved_vt: scheduled.reserved_vt,
            // Account from pop when no PollChargeGuard starts: internal tasks or
            // early cancellation. begin_poll resets these fallback clocks at
            // the wrapped poll's accounting boundary.
            wall_start: Instant::now(),
            cpu_start: ThreadTime::now(),
            cpu_charged: Duration::ZERO,
            needs_settlement: true,
        });
    });
    Pop {
        task_cell: entry.task_cell,
        schedule_time,
        from_local: false,
    }
}

impl Dispatch {
    fn take_cpu_delta(&mut self) -> Duration {
        let elapsed = self.cpu_start.elapsed();
        let delta = elapsed.saturating_sub(self.cpu_charged);
        self.cpu_charged = elapsed;
        delta
    }
    fn charge_cpu(&mut self) {
        let delta = self.take_cpu_delta();
        self.shared.cpu_budget.consume(delta);
    }
    pub(super) fn settle(&mut self) {
        if !self.needs_settlement {
            return;
        }
        self.charge_cpu();
        let wall = self.wall_start.elapsed();
        self.shared.metrics.record_poll(wall, self.cpu_charged);
        self.shared.scheduler.settle(
            self.queue_key,
            self.reserved_vt,
            weighted_vt(wall, self.weight),
        );
        self.reserved_vt = 0;
        self.needs_settlement = false;
    }
    pub(super) fn begin_poll(&mut self) {
        if !self.needs_settlement {
            // FutureRunner can repoll inline. Capture a new slice weight while
            // retaining the dispatch group queue, including after a metadata update.
            self.weight = self.policy.weight();
            self.reserved_vt = weighted_vt(self.shared.config.slice_precharge, self.weight);
            self.shared
                .scheduler
                .reserve(self.queue_key, self.reserved_vt);
        }
        self.wall_start = Instant::now();
        self.cpu_start = ThreadTime::now();
        self.cpu_charged = Duration::ZERO;
        self.needs_settlement = true;
    }
}

impl Drop for Dispatch {
    fn drop(&mut self) {
        self.settle();
        let release = self
            .shared
            .scheduler
            .finish_dispatch(self.queue_key, &self.record);
        if release {
            self.record.release();
        }
    }
}

/// Owns one runner dispatch across inline repolls and synchronous re-enqueue,
/// so a cooperative yield is not mistaken for an idle-group wakeup.
/// PollChargeGuard settles each poll; Dispatch drop releases the
/// parallelism slot.
pub(in crate::yatp_pool) struct DispatchGuard {
    previous: Option<Rc<RefCell<Dispatch>>>,
    installed: bool,
}

impl DispatchGuard {
    pub(in crate::yatp_pool) fn install() -> Self {
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

/// Settles one synchronous wrapped poll, including Pending and unwinding.
/// Async waiting is outside this guard. Settlement retains the dispatch slot
/// for any inline repoll or re-enqueue still performed by the runner.
pub(super) struct PollChargeGuard {
    dispatch: Option<Rc<RefCell<Dispatch>>>,
    _slice: Option<PollRescheduleContextGuard>,
}

impl PollChargeGuard {
    pub(super) fn new(id: u64) -> Self {
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

impl Drop for PollChargeGuard {
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
        if now.now().saturating_duration_since(dispatch.wall_start) < CPU_CHECKPOINT_MIN_POLL_TIME {
            return false;
        }
        let delta = dispatch.take_cpu_delta();
        dispatch
            .shared
            .cpu_budget
            .consume_and_retry_at(delta)
            .is_some()
    });
    if exhausted {
        yatp::task::future::yield_to_scheduler().await;
    }
    exhausted
}

/// Resolve only the task being requeued by this pool's current runner.
/// Release all TLS borrows before a caller drops a waker or consults history.
pub(super) fn continuation_record(shared: &Arc<Shared>, id: u64) -> Option<Arc<TaskRecord>> {
    ACTIVE.with(|active| {
        let active = active.borrow();
        let dispatch = active.as_ref()?.borrow();
        (dispatch.record.id == id && Arc::ptr_eq(&dispatch.shared, shared))
            .then(|| dispatch.record.clone())
    })
}

pub(super) fn cpu_exhausted() -> bool {
    ACTIVE.with(|active| {
        active
            .borrow()
            .as_ref()
            .is_some_and(|d| d.borrow().shared.cpu_budget.retry_at().is_some())
    })
}
