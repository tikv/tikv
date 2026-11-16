// Copyright 2026 TiKV Project Authors. Licensed under Apache-2.0.

//! Admission permits, task ownership and cancellation.

use std::{
    future::Future,
    pin::Pin,
    sync::{
        Arc, Mutex, Weak,
        atomic::{AtomicBool, Ordering},
    },
    task::{Context, Poll},
};

use futures::task::AtomicWaker;
use yatp::queue::multilevel::TaskLevelManager;

use super::{
    GroupPolicy, Shared,
    execution::{self, PollChargeGuard},
};
use crate::resource_control::TaskMetadata;

/// Registry-owned task identity. Backreferences to the pool and future slot
/// stay weak; cancellation must still drop IO futures to break waker cycles.
pub(super) struct TaskRecord {
    pub(super) id: u64,
    pub(super) group: Arc<GroupPolicy>,
    pub(super) history: Arc<TaskLevelManager>,
    pub(super) override_priority: u32,
    shared: Weak<Shared>,
    pub(super) cancelled: AtomicBool,
    pub(super) released: AtomicBool,
    // Admitted tasks occupy global/group live capacity through async waits.
    // Internal tasks are uncounted; ready-queue membership is tracked separately.
    pub(super) counted: bool,
    pub(super) waker: AtomicWaker,
    // Serialize publication in wrap with cancellation. The post-publication
    // cancelled check covers cancellation before the slot was published.
    future: Mutex<Option<Weak<dyn CancelTask>>>,
}

impl TaskRecord {
    pub(super) fn new(
        id: u64,
        group: Arc<GroupPolicy>,
        history: Arc<TaskLevelManager>,
        override_priority: u32,
        shared: Weak<Shared>,
        counted: bool,
    ) -> Arc<Self> {
        Arc::new(Self {
            id,
            group,
            history,
            override_priority,
            shared,
            counted,
            cancelled: AtomicBool::new(false),
            released: AtomicBool::new(false),
            waker: AtomicWaker::new(),
            future: Mutex::new(None),
        })
    }
}

trait CancelTask: Send + Sync {
    // False means the future slot is busy; cancellation is completed by the
    // current owner or a later poll. Never wait for or recursively lock a poll.
    fn try_cancel(&self) -> bool;
}

struct FutureSlot<F> {
    // Polling and cancellation must have exclusive access to the pinned future.
    // Cancel only tries this lock; extracted futures are dropped after unlock
    // because their destructors may wake or cancel other tasks.
    future: Mutex<Option<Pin<Box<F>>>>,
}

impl<F: Future<Output = ()> + Send> CancelTask for FutureSlot<F> {
    fn try_cancel(&self) -> bool {
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

impl TaskRecord {
    pub(super) fn cancel(&self) {
        self.cancelled.store(true, Ordering::Release);
        let future = self.future.lock().unwrap().as_ref().and_then(Weak::upgrade);
        if future.is_none_or(|f| f.try_cancel()) {
            self.release();
        }
        self.waker.wake();
    }

    pub(super) fn release(&self) {
        if self.released.swap(true, Ordering::AcqRel) {
            return;
        }
        if let Some(shared) = self.shared.upgrade() {
            shared.scheduler.release(self);
        }
        // The registry owns TaskRecord; the future holds only a Weak reference.
        // Clear the last saved task waker outside the scheduler lock.
        drop(self.waker.take());
    }
}

impl Drop for TaskRecord {
    fn drop(&mut self) {
        self.release();
    }
}

pub struct FairTaskPermit {
    record: Weak<TaskRecord>,
    id: u64,
}

impl FairTaskPermit {
    pub(super) fn new(record: &Arc<TaskRecord>) -> Self {
        Self {
            record: Arc::downgrade(record),
            id: record.id,
        }
    }

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
    record: Weak<TaskRecord>,
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
            .and_then(|shared| shared.scheduler.remove_queued(record.id));
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
        let _account = PollChargeGuard::new(record.id);
        // Inline repolls bypass queue.pop(). Force a real scheduler yield when
        // debt appears, including for short self-waking futures.
        let exhausted = execution::cpu_exhausted();
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
