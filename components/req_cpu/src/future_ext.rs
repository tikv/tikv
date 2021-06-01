// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

use crate::ResourceMeteringTag;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

impl<T: std::future::Future> FutureExt for T {}

pub trait FutureExt: Sized {
    #[inline]
    fn in_tag(self, tag: Arc<ResourceMeteringTag>) -> InTags<Self> {
        InTags { inner: self, tag }
    }
}

#[pin_project::pin_project]
pub struct InTags<T> {
    #[pin]
    inner: T,
    tag: Arc<ResourceMeteringTag>,
}

impl<T: std::future::Future> std::future::Future for InTags<T> {
    type Output = T::Output;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.project();
        let _guard = this.tag.attach();
        this.inner.poll(cx)
    }
}
