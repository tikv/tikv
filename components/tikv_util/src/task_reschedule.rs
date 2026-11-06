// Copyright 2026 TiKV Project Authors. Licensed under Apache-2.0.
// Modifications Copyright 2026 PingCAP, Inc.

//! Cooperative rescheduling for fairness-enabled storage tasks.

use std::{
    cell::RefCell,
    marker::PhantomData,
    rc::Rc,
    time::{Duration, Instant},
};

use crate::time::LazyInstant;

/// The wall-time slice used when no other slice is configured.
///
/// A shorter slice offers resource groups more frequent opportunities to
/// compete for workers, at the cost of rescheduling long tasks more often.
/// This is cooperative: synchronous work between checkpoints can exceed it.
pub const DEFAULT_MAX_TIME_SLICE_US: u64 = 1_000;

struct RescheduleContext {
    poll_started_at: Instant,
    max_time_slice: Duration,
}

impl RescheduleContext {
    fn new(poll_started_at: Instant, max_time_slice: Duration) -> Self {
        Self {
            poll_started_at,
            max_time_slice,
        }
    }
}

thread_local! {
    static RESCHEDULE_CONTEXT: RefCell<Option<RescheduleContext>> =
        const { RefCell::new(None) };
}

/// Installs cooperative-rescheduling state for one future poll.
///
/// The guard must cover only the synchronous invocation of [`Future::poll`].
/// Dropping it restores an outer context, if any, so the state cannot follow a
/// future across a `Pending` boundary or to another worker thread.
///
/// [`Future::poll`]: std::future::Future::poll
#[must_use = "the guard must stay alive for the duration of the future poll"]
pub struct PollRescheduleContextGuard {
    previous: Option<RescheduleContext>,
    // A poll context is thread local and must be dropped on the installing
    // thread.
    _not_send: PhantomData<Rc<()>>,
}

impl PollRescheduleContextGuard {
    /// Installs a cooperative-rescheduling context for the current poll.
    ///
    /// `max_time_slice` is fixed for this poll, so a configuration change can
    /// never shorten or extend a slice that is already being measured.
    pub fn new(poll_started_at: Instant, max_time_slice: Duration) -> Self {
        let previous = RESCHEDULE_CONTEXT.with(|slot| {
            slot.borrow_mut()
                .replace(RescheduleContext::new(poll_started_at, max_time_slice))
        });
        Self {
            previous,
            _not_send: PhantomData,
        }
    }
}

impl Drop for PollRescheduleContextGuard {
    fn drop(&mut self) {
        RESCHEDULE_CONTEXT.with(|slot| {
            let mut slot = slot.borrow_mut();
            debug_assert!(
                slot.is_some(),
                "cooperative reschedule context must remain installed"
            );
            *slot = self.previous.take();
        });
    }
}

/// Returns whether the current poll supports cooperative
/// rescheduling.
#[inline]
pub fn is_reschedule_context_active() -> bool {
    RESCHEDULE_CONTEXT.with(|slot| slot.borrow().is_some())
}

/// Reschedules the current future when its poll exceeds the wall-time
/// slice.
///
/// This function is a no-op outside a poll with an active reschedule context.
/// Callers own the cadence at which they invoke this check.
///
/// Returns whether rescheduling was requested. Callers must not reuse `now`
/// after this function returns `true`, because the await may resume in a new
/// poll.
#[inline]
pub async fn reschedule_if_time_slice_exhausted(now: &mut LazyInstant) -> bool {
    if is_time_slice_exhausted_with_now(|| now.now()) {
        yatp::task::future::reschedule().await;
        true
    } else {
        false
    }
}

fn is_time_slice_exhausted_with_now(now: impl FnOnce() -> Instant) -> bool {
    RESCHEDULE_CONTEXT.with(|slot| {
        slot.borrow().as_ref().is_some_and(|context| {
            now().saturating_duration_since(context.poll_started_at) >= context.max_time_slice
        })
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    const DEFAULT_MAX_TIME_SLICE: Duration = Duration::from_micros(DEFAULT_MAX_TIME_SLICE_US);

    #[test]
    fn test_context_scope_restores_after_unwind_and_no_context_is_inert() {
        assert!(!is_reschedule_context_active());
        assert!(!is_time_slice_exhausted_with_now(|| panic!(
            "a missing context must not sample time"
        )));

        let outer = PollRescheduleContextGuard::new(Instant::now(), DEFAULT_MAX_TIME_SLICE);
        assert!(is_reschedule_context_active());

        let result = std::panic::catch_unwind(|| {
            let _inner = PollRescheduleContextGuard::new(Instant::now(), DEFAULT_MAX_TIME_SLICE);
            assert!(is_reschedule_context_active());
            panic!("exercise context guard unwind");
        });

        assert!(result.is_err());
        assert!(is_reschedule_context_active());
        drop(outer);
        assert!(!is_reschedule_context_active());
    }

    #[test]
    fn test_yield_returns_pending_and_does_not_keep_poll_context() {
        use std::{
            future::Future,
            task::{Context, Poll},
        };

        let mut now = LazyInstant::default();
        let mut checkpoint = Box::pin(reschedule_if_time_slice_exhausted(&mut now));
        let mut cx = Context::from_waker(futures::task::noop_waker_ref());
        {
            let _guard = PollRescheduleContextGuard::new(Instant::now(), Duration::ZERO);
            assert!(checkpoint.as_mut().poll(&mut cx).is_pending());
        }
        assert!(!is_reschedule_context_active());
        {
            let _guard = PollRescheduleContextGuard::new(Instant::now(), Duration::from_secs(60));
            assert_eq!(checkpoint.as_mut().poll(&mut cx), Poll::Ready(true));
            // The resuming poll starts with a fresh slice. Its checkpoint must
            // use a fresh timestamp rather than the one held across the yield.
            assert!(!is_time_slice_exhausted_with_now(Instant::now));
        }
        assert!(!is_reschedule_context_active());
    }

    #[test]
    fn test_poll_start_is_fixed_across_checks() {
        let poll_started_at = Instant::now();
        let _guard = PollRescheduleContextGuard::new(poll_started_at, DEFAULT_MAX_TIME_SLICE);

        assert!(!is_time_slice_exhausted_with_now(|| {
            poll_started_at + DEFAULT_MAX_TIME_SLICE - Duration::from_nanos(1)
        }));
        assert!(is_time_slice_exhausted_with_now(|| {
            poll_started_at + DEFAULT_MAX_TIME_SLICE
        }));
        // A check never resets the poll start time. The first awaited
        // reschedule returns `Pending`, and the read-pool wrapper installs a
        // fresh context for the next outer poll.
        assert!(is_time_slice_exhausted_with_now(|| {
            poll_started_at + DEFAULT_MAX_TIME_SLICE + Duration::from_millis(1)
        }));
    }

    #[test]
    fn test_each_context_keeps_its_own_slice() {
        let poll_started_at = Instant::now();
        let outer = PollRescheduleContextGuard::new(poll_started_at, Duration::from_micros(1_000));
        assert!(is_time_slice_exhausted_with_now(
            || poll_started_at + Duration::from_micros(1_000)
        ));

        let inner = PollRescheduleContextGuard::new(poll_started_at, Duration::from_micros(5_000));
        assert!(!is_time_slice_exhausted_with_now(
            || poll_started_at + Duration::from_micros(1_000)
        ));
        assert!(is_time_slice_exhausted_with_now(
            || poll_started_at + Duration::from_micros(5_000)
        ));

        // Restoring the outer context restores its own slice.
        drop(inner);
        assert!(is_time_slice_exhausted_with_now(
            || poll_started_at + Duration::from_micros(1_000)
        ));
        drop(outer);
    }
}
