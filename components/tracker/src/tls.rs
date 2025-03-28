// Copyright 2022 TiKV Project Authors. Licensed under Apache-2.0.

use std::{
    cell::Cell,
    future::Future,
    pin::Pin,
    task::{Context, Poll},
};

use pin_project::pin_project;

use crate::{slab::TrackerToken, Tracker, GLOBAL_TRACKERS, INVALID_TRACKER_TOKEN};

thread_local! {
    static TLS_TRACKER_TOKEN: Cell<TrackerToken> = const { Cell::new(INVALID_TRACKER_TOKEN) };
}

pub fn set_tls_tracker_token(token: TrackerToken) {
    TLS_TRACKER_TOKEN.with(|c| {
        c.set(token);
    })
}

pub fn clear_tls_tracker_token() {
    set_tls_tracker_token(INVALID_TRACKER_TOKEN);
}

pub fn get_tls_tracker_token() -> TrackerToken {
    TLS_TRACKER_TOKEN.with(|c| c.get())
}

pub fn with_tls_tracker<F>(mut f: F)
where
    F: FnMut(&mut Tracker),
{
    TLS_TRACKER_TOKEN.with(|c| {
        GLOBAL_TRACKERS.with_tracker(c.get(), &mut f);
    });
}

/// A future that sets the TLS tracker token before polling the inner future.
/// It is used to propagate the tracker token to the inner future.
/// The tracker token will be cleared after polling the inner future.
#[pin_project]
pub struct TlsTrackedFuture<F> {
    #[pin]
    future: F,
    token: TrackerToken,
}

impl<F> TlsTrackedFuture<F> {
    pub fn new(future: F) -> TlsTrackedFuture<F> {
        TlsTrackedFuture {
            future,
            token: get_tls_tracker_token(),
        }
    }
}

impl<F: Future> Future for TlsTrackedFuture<F> {
    type Output = F::Output;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.project();
        TLS_TRACKER_TOKEN.with(|c| {
            c.set(*this.token);
            let res = this.future.poll(cx);
            c.set(INVALID_TRACKER_TOKEN);
            res
        })
    }
}
