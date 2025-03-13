// Copyright 2020 TiKV Project Authors. Licensed under Apache-2.0.

use std::{
    future::Future,
    pin::Pin,
    task::{Context, Poll},
};

use pin_project::pin_project;

pub trait FutureTrack {
    fn on_poll_begin(&mut self);
    fn on_poll_finish(&mut self);
}

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
