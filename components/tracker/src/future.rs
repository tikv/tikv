// Copyright 2020 TiKV Project Authors. Licensed under Apache-2.0.

use std::{
    future::Future,
    pin::Pin,
    task::{Context, Poll},
    time::Instant,
};

use pin_project::pin_project;

use crate::slab::{GLOBAL_TRACKERS, TrackerToken};

/// A trait for tracking the polling of a future.
/// It is used to do some work before and after polling the inner future.
/// For example, it can be used to record the start and end time of polling.
///
/// The trait is used in [`track`] function.
pub trait FutureTrack {
    fn on_poll_begin(&mut self);
    fn on_poll_finish(&mut self);
}

/// A future that tracks the polling of the inner future.
pub fn track<F: Future, T: FutureTrack>(fut: F, fut_tracker: T) -> impl Future<Output = F::Output> {
    Tracker::new(fut, fut_tracker)
}

/// Attributes a tracked future's poll time to the slab tracker addressed by
/// a token: busy poll time accumulates into
/// [`crate::RequestMetrics::future_process_nanos`] and the gaps between polls,
/// plus the wait before the first poll when that poll begins, accumulate into
/// [`crate::RequestMetrics::future_suspend_nanos`], which
/// [`crate::Tracker::merge_time_detail`] folds into the request's
/// `TimeDetailV2`. Unlike `TlsFutureTracker` in the storage layer it keeps no
/// thread local state, so it can track a future handed to a foreign executor;
/// an invalid or removed token makes it a no-op. An instance dropped before its
/// first poll has no poll callback and therefore cannot flush that initial
/// wait.
#[derive(Debug)]
pub struct PollTimeTracker {
    token: TrackerToken,
    poll_began: Instant,
    last_finished: Instant,
}

impl PollTimeTracker {
    /// Creates a poll-time tracker for the slab tracker addressed by `token`.
    pub fn new(token: TrackerToken) -> Self {
        let now = Instant::now();
        Self {
            token,
            poll_began: now,
            last_finished: now,
        }
    }
}

impl FutureTrack for PollTimeTracker {
    fn on_poll_begin(&mut self) {
        self.poll_began = Instant::now();
    }

    fn on_poll_finish(&mut self) {
        let now = Instant::now();
        let process_nanos = now.saturating_duration_since(self.poll_began).as_nanos() as u64;
        let suspend_nanos = self
            .poll_began
            .saturating_duration_since(self.last_finished)
            .as_nanos() as u64;
        GLOBAL_TRACKERS.with_tracker(self.token, |tracker| {
            tracker.metrics.future_process_nanos += process_nanos;
            tracker.metrics.future_suspend_nanos += suspend_nanos;
        });
        self.last_finished = now;
    }
}

#[pin_project]
struct Tracker<F, T>
where
    F: Future,
    T: FutureTrack,
{
    #[pin]
    fut: F,
    fut_tracker: T,
}

impl<F, T> Tracker<F, T>
where
    F: Future,
    T: FutureTrack,
{
    fn new(fut: F, fut_tracker: T) -> Self {
        Tracker { fut, fut_tracker }
    }
}

impl<F, T> Future for Tracker<F, T>
where
    F: Future,
    T: FutureTrack,
{
    type Output = F::Output;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.project();

        this.fut_tracker.on_poll_begin();

        let res = this.fut.poll(cx);

        this.fut_tracker.on_poll_finish();

        res
    }
}
