// Copyright 2020 TiKV Project Authors. Licensed under Apache-2.0.

use pin_project::pin_project;
use std::future::Future;
use std::marker::PhantomData;
use std::pin::Pin;
use std::task::{Context, Poll};
use std::time::{Duration, Instant};
use tokio::sync::{Semaphore, SemaphorePermit};

use crate::coprocessor::metrics::*;

/// Limits the concurrency of heavy tasks by limiting the time spent on executing `fut`
/// before forcing to acquire a semaphore permit.
///
/// The future `fut` can always run for at least `time_limit_without_permit`,
/// but it needs to acquire a permit from the semaphore before it can continue.
pub fn limit_concurrency<'a, F: Future + 'a>(
    fut: F,
    semaphore: &'a Semaphore,
    time_limit_without_permit: Duration,
) -> impl Future<Output = F::Output> + 'a {
    ConcurrencyLimiter::new(semaphore.acquire(), fut, time_limit_without_permit)
}

#[pin_project]
struct ConcurrencyLimiter<'a, PF, F>
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
    state: LimitationState<'a>,
    _phantom: PhantomData<&'a ()>,
}

enum LimitationState<'a> {
    NotLimited,
    Acquiring,
    Acuqired(SemaphorePermit<'a>),
}

impl<'a, PF, F> ConcurrencyLimiter<'a, PF, F>
where
    PF: Future<Output = SemaphorePermit<'a>>,
    F: Future,
{
    fn new(permit_fut: PF, fut: F, time_limit_without_permit: Duration) -> Self {
        ConcurrencyLimiter {
            permit_fut,
            fut,
            time_limit_without_permit,
            execution_time: Duration::default(),
            state: LimitationState::NotLimited,
            _phantom: PhantomData,
        }
    }
}

impl<'a, PF, F> Future for ConcurrencyLimiter<'a, PF, F>
where
    PF: Future<Output = SemaphorePermit<'a>>,
    F: Future,
{
    type Output = F::Output;

    fn poll(self: Pin<&mut Self>, cx: &mut Context) -> Poll<Self::Output> {
        let this = self.project();
        match this.state {
            LimitationState::NotLimited if this.execution_time > this.time_limit_without_permit => {
                match this.permit_fut.poll(cx) {
                    Poll::Ready(permit) => {
                        *this.state = LimitationState::Acuqired(permit);
                        COPR_ACQUIRE_SEMAPHORE_TYPE.acquired.inc();
                    }
                    Poll::Pending => {
                        *this.state = LimitationState::Acquiring;
                        COPR_WAITING_FOR_SEMAPHORE.inc();
                        return Poll::Pending;
                    }
                }
            }
            LimitationState::Acquiring => match this.permit_fut.poll(cx) {
                Poll::Ready(permit) => {
                    *this.state = LimitationState::Acuqired(permit);
                    COPR_WAITING_FOR_SEMAPHORE.dec();
                    COPR_ACQUIRE_SEMAPHORE_TYPE.acquired.inc();
                }
                Poll::Pending => {
                    return Poll::Pending;
                }
            },
            _ => {}
        }
        let now = Instant::now();
        match this.fut.poll(cx) {
            Poll::Ready(res) => {
                if let LimitationState::NotLimited = this.state {
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

#[cfg(test)]
mod tests {
    use super::*;
    use futures03::future::FutureExt;
    use std::sync::Arc;
    use std::thread;
    use tokio::task::yield_now;
    use tokio::time::{delay_for, timeout};

    #[tokio::test(basic_scheduler)]
    async fn test_limit_concurrency() {
        async fn work(iter: i32) {
            for i in 0..iter {
                thread::sleep(Duration::from_millis(50));
                if i < iter - 1 {
                    yield_now().await;
                }
            }
        }

        let smp = Arc::new(Semaphore::new(0));

        // Light tasks should run without any semaphore permit
        let smp2 = smp.clone();
        assert!(
            tokio::spawn(timeout(Duration::from_millis(250), async move {
                limit_concurrency(work(2), &*smp2, Duration::from_millis(500)).await
            }))
            .await
            .is_ok()
        );

        // Both t1 and t2 need a semaphore permit to finish. Although t2 is much shorter than t1,
        // it starts with t1
        smp.add_permits(1);
        let smp2 = smp.clone();
        let mut t1 =
            tokio::spawn(
                async move { limit_concurrency(work(8), &*smp2, Duration::default()).await },
            )
            .fuse();

        delay_for(Duration::from_millis(100)).await;
        let smp2 = smp.clone();
        let mut t2 =
            tokio::spawn(
                async move { limit_concurrency(work(2), &*smp2, Duration::default()).await },
            )
            .fuse();

        let mut deadline = delay_for(Duration::from_millis(1500)).fuse();
        let mut t1_finished = false;
        loop {
            futures_util::select! {
                _ = t1 => {
                    t1_finished = true;
                },
                _ = t2 => {
                    if t1_finished {
                        return;
                    } else {
                        panic!("t2 should finish later than t1");
                    }
                },
                _ = deadline => {
                    panic!("test timeout");
                }
            }
        }
    }
}
