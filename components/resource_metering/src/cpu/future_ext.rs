// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

use crate::ResourceMeteringTag;

use std::pin::Pin;
use std::task::{Context, Poll};

#[pin_project::pin_project]
pub struct InTags<T> {
    #[pin]
    inner: T,
    tag: ResourceMeteringTag,
}

impl<T: std::future::Future> FutureExt for T {}

pub trait FutureExt: Sized {
    #[inline]
    fn in_resource_metering_tag(self, tag: ResourceMeteringTag) -> InTags<Self> {
        InTags { inner: self, tag }
    }
}

impl<T: std::future::Future> std::future::Future for InTags<T> {
    type Output = T::Output;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.project();
        let _guard = this.tag.attach();
        this.inner.poll(cx)
    }
}

impl<T: futures::Stream> StreamExt for T {}

pub trait StreamExt: Sized {
    #[inline]
    fn in_resource_metering_tag(self, tag: ResourceMeteringTag) -> InTags<Self> {
        InTags { inner: self, tag }
    }
}

impl<T: futures::Stream> futures::Stream for InTags<T> {
    type Item = T::Item;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.project();
        let _guard = this.tag.attach();
        this.inner.poll_next(cx)
    }
}
