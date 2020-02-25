// Copyright 2020 TiKV Project Authors. Licensed under Apache-2.0.

use pin_project::pin_project;
use std::future::Future;
use std::marker::PhantomData;
use std::pin::Pin;
use std::task::{Context, Poll};
use std::time::{Duration, Instant};
use tokio::sync::{Semaphore, SemaphorePermit};

use super::metrics::*;

/// Limits the time used by `fut` before a semaphore permit is acquired.
///
/// The future `fut` can always run for at least `time_limit_without_permit`,
/// but it needs to acquire a permit from the semaphore before it can continue.
pub fn limit_time<'a, F: Future + 'a>(
    fut: F,
    semaphore: &'a Semaphore,
    time_limit_without_permit: Duration,
) -> impl Future<Output = F::Output> + 'a {
    TimeLimiter::new(semaphore.acquire(), fut, time_limit_without_permit)
}

#[pin_project]
struct TimeLimiter<'a, PF, F>
where
    PF: Future<Output = SemaphorePermit<'a>>,
    F: Future,
{
    #[pin]
    permit_fut: PF,
    #[pin]
    fut: F,
    time_limit_without_permit: Duration,
    execution_time: Duration,
    permit: Option<SemaphorePermit<'a>>,
    // Whether the task is waiting for a semaphore. Used for tracking metrics only.
    waiting: bool,
    _phantom: PhantomData<&'a ()>,
}

impl<'a, PF, F> TimeLimiter<'a, PF, F>
where
    PF: Future<Output = SemaphorePermit<'a>>,
    F: Future,
{
    fn new(permit_fut: PF, fut: F, time_limit_without_permit: Duration) -> Self {
        TimeLimiter {
            permit_fut,
            fut,
            time_limit_without_permit,
            execution_time: Duration::default(),
            permit: None,
            waiting: false,
            _phantom: PhantomData,
        }
    }
}

impl<'a, PF, F> Future for TimeLimiter<'a, PF, F>
where
    PF: Future<Output = SemaphorePermit<'a>>,
    F: Future,
{
    type Output = F::Output;

    fn poll(self: Pin<&mut Self>, cx: &mut Context) -> Poll<Self::Output> {
        let this = self.project();
        if this.permit.is_none() && this.execution_time > this.time_limit_without_permit {
            match this.permit_fut.poll(cx) {
                Poll::Ready(permit) => {
                    *this.permit = Some(permit);
                    COPR_ACQUIRE_SEMAPHORE_TYPE.acquired.inc();
                    if *this.waiting {
                        COPR_WAITING_SEMAPHORE.dec();
                        *this.waiting = false; // not necessary, but keep its meaning valid
                    }
                }
                Poll::Pending => {
                    if !*this.waiting {
                        *this.waiting = true;
                        COPR_WAITING_SEMAPHORE.inc();
                    }
                    return Poll::Pending;
                }
            }
        }
        let now = Instant::now();
        match this.fut.poll(cx) {
            Poll::Ready(res) => {
                if this.permit.is_none() {
                    COPR_ACQUIRE_SEMAPHORE_TYPE.unacquired.inc();
                }
                Poll::Ready(res)
            }
            Poll::Pending => {
                *this.execution_time += now.elapsed();
                Poll::Pending
            }
        }
    }
}
