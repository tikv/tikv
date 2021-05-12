// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

use crate::RequestTags;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

impl<T: std::future::Future> FutureExt for T {}

pub trait FutureExt: Sized {
    #[inline]
    fn in_tags(self, tags: Arc<RequestTags>) -> InTags<Self> {
        InTags { inner: self, tags }
    }
}

#[pin_project::pin_project]
pub struct InTags<T> {
    #[pin]
    inner: T,
    tags: Arc<RequestTags>,
}

impl<T: std::future::Future> std::future::Future for InTags<T> {
    type Output = T::Output;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.project();
        let _guard = this.tags.attach();
        this.inner.poll(cx)
    }
}
