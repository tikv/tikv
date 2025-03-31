// Copyright 2020 TiKV Project Authors. Licensed under Apache-2.0.

use std::{
    future::Future,
    pin::Pin,
    task::{Context, Poll},
};

use pin_project::pin_project;

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
