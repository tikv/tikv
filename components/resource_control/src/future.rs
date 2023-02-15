// Copyright 2022 TiKV Project Authors. Licensed under Apache-2.0.

use std::{
    future::Future,
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
};

use pin_project::pin_project;
use tikv_util::time::Instant;

use crate::resource_group::{ResourceConsumeType, ResourceController};

#[pin_project]
pub struct ControlledFuture<F> {
    #[pin]
    future: F,
    controller: Arc<ResourceController>,
    group_name: Vec<u8>,
}

impl<F> ControlledFuture<F> {
    pub fn new(future: F, controller: Arc<ResourceController>, group_name: Vec<u8>) -> Self {
        Self {
            future,
            controller,
            group_name,
        }
    }
}

impl<F: Future> Future for ControlledFuture<F> {
    type Output = F::Output;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.project();
        let now = Instant::now();
        let res = this.future.poll(cx);
        this.controller.consume(
            this.group_name,
            ResourceConsumeType::CpuTime(now.saturating_elapsed()),
        );
        res
    }
}
