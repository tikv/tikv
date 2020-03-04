// Copyright 2020 TiKV Project Authors. Licensed under Apache-2.0.

use pin_project::pin_project;
use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll};

use crate::coprocessor::tracker::Tracker as CopTracker;

pub fn track<'a, F: Future + 'a>(
    fut: F,
    cop_tracker: &'a mut CopTracker,
) -> impl Future<Output = F::Output> + 'a {
    Tracker::new(fut, cop_tracker)
}

#[pin_project]
struct Tracker<'a, F>
where
    F: Future,
{
    #[pin]
    fut: F,
    cop_tracker: &'a mut CopTracker,
}

impl<'a, F> Tracker<'a, F>
where
    F: Future,
{
    fn new(fut: F, cop_tracker: &'a mut CopTracker) -> Self {
        Tracker { fut, cop_tracker }
    }
}

impl<'a, F: Future> Future for Tracker<'a, F>
where
    F: Future,
{
    type Output = F::Output;

    fn poll(self: Pin<&mut Self>, cx: &mut Context) -> Poll<Self::Output> {
        let this = self.project();
        this.cop_tracker.on_begin_item();
        let res = this.fut.poll(cx);
        this.cop_tracker.on_finish_item(None);
        res
    }
}
