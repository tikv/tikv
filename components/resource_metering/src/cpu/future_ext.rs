// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

use crate::ResourceMeteringTag;

use crate::row::recorder::{on_poll_begin, on_poll_finish};
use std::pin::Pin;
use std::task::{Context, Poll};

impl<T: std::future::Future> FutureExt for T {}

pub trait FutureExt: Sized {
    #[inline]
    fn in_resource_metering_tag(self, tag: ResourceMeteringTag) -> InTags<Self> {
        InTags { inner: self, tag }
    }
}

#[pin_project::pin_project]
pub struct InTags<T> {
    #[pin]
    inner: T,
    tag: ResourceMeteringTag,
}

impl<T: std::future::Future> std::future::Future for InTags<T> {
    type Output = T::Output;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.project();
        let _guard = this.tag.attach();
        on_poll_begin();
        let result = this.inner.poll(cx);
        on_poll_finish(this.tag.infos.extra_attachment.clone());
        result
    }
}
