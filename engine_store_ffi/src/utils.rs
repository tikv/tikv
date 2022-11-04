// Copyright 2022 TiKV Project Authors. Licensed under Apache-2.0.

use std::time;

use futures_util::{compat::Future01CompatExt, future::BoxFuture, FutureExt};
use tikv_util::timer::start_global_timer;
use tokio_timer::timer::Handle;

use crate::lazy_static;

lazy_static! {
    pub static ref PROXY_TIMER_HANDLE: Handle = start_global_timer("proxy-timer");
}

pub type ArcNotifyWaker = std::sync::Arc<NotifyWaker>;

pub struct NotifyWaker {
    pub inner: Box<dyn Fn() + Send + Sync>,
}

impl futures::task::ArcWake for NotifyWaker {
    fn wake_by_ref(arc_self: &std::sync::Arc<Self>) {
        (arc_self.inner)();
    }
}

pub struct TimerTask {
    future: BoxFuture<'static, ()>,
}

pub fn make_timer_task(millis: u64) -> TimerTask {
    let deadline = time::Instant::now() + time::Duration::from_millis(millis);
    let delay = PROXY_TIMER_HANDLE.delay(deadline).compat().map(|_| {});
    TimerTask {
        future: Box::pin(delay),
    }
}

#[allow(clippy::explicit_auto_deref)]
pub fn poll_timer_task(task: &mut TimerTask, waker: Option<&ArcNotifyWaker>) -> Option<()> {
    let mut func = |cx: &mut std::task::Context| {
        let fut = &mut task.future;
        match fut.as_mut().poll(cx) {
            std::task::Poll::Pending => None,
            std::task::Poll::Ready(e) => Some(e),
        }
    };
    if let Some(waker) = waker {
        let waker = futures::task::waker_ref(waker);
        let cx = &mut std::task::Context::from_waker(&*waker);
        func(cx)
    } else {
        let waker = futures::task::noop_waker();
        let cx = &mut std::task::Context::from_waker(&waker);
        func(cx)
    }
}
