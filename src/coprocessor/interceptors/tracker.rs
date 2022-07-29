// Copyright 2020 TiKV Project Authors. Licensed under Apache-2.0.

use std::{
    future::Future,
    pin::Pin,
    task::{Context, Poll},
};

use pin_project::pin_project;
use tikv_kv::Engine;

use crate::coprocessor::tracker::Tracker as CopTracker;

pub fn track<'a, F: Future + 'a, E: Engine>(
    fut: F,
    cop_tracker: &'a mut CopTracker<E>,
) -> impl Future<Output = F::Output> + 'a {
    Tracker::new(fut, cop_tracker)
}

#[pin_project]
struct Tracker<'a, F, E>
where
    F: Future,
    E: Engine,
{
    #[pin]
    fut: F,
    cop_tracker: &'a mut CopTracker<E>,
}

impl<'a, F, E> Tracker<'a, F, E>
where
    F: Future,
    E: Engine,
{
    fn new(fut: F, cop_tracker: &'a mut CopTracker<E>) -> Self {
        Tracker { fut, cop_tracker }
    }
}

impl<'a, F, E> Future for Tracker<'a, F, E>
where
    F: Future,
    E: Engine,
{
    type Output = F::Output;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.project();

        this.cop_tracker.on_begin_item();

        let res = this.fut.poll(cx);

        this.cop_tracker.on_finish_item(None);

        res
    }
}
