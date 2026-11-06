// Copyright 2020 TiKV Project Authors. Licensed under Apache-2.0.

use std::{
    future::Future,
    marker::PhantomData,
    pin::Pin,
    task::{Context, Poll},
    time::Duration,
};

use futures::future::FutureExt;
use pin_project::pin_project;
use prometheus::{Histogram, IntGauge};
use tikv_util::time::Instant;
use tokio::sync::{Semaphore, SemaphorePermit};

use crate::coprocessor::metrics::*;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SemaphoreGroup {
    /// Ordinary coprocessor requests that contend on the shared heavy-task
    /// budget.
    Shared,
    /// Request classes that intentionally report quota samples to the
    /// background quota limiter and should not be fully blocked behind the
    /// shared heavy-task queue.
    BackgroundLimited,
}

impl SemaphoreGroup {
    fn waiting_metric(self) -> &'static IntGauge {
        match self {
            Self::Shared => &COPR_WAITING_FOR_SEMAPHORE.shared,
            Self::BackgroundLimited => &COPR_WAITING_FOR_SEMAPHORE.background_limited,
        }
    }

    fn wait_time_metric(self) -> &'static Histogram {
        match self {
            Self::Shared => &COPR_SEMAPHORE_WAIT_TIME.shared,
            Self::BackgroundLimited => &COPR_SEMAPHORE_WAIT_TIME.background_limited,
        }
    }
}

/// Limits the concurrency of heavy tasks by limiting the time spent on
/// executing `fut` before forcing to acquire a semaphore permit.
///
/// The future `fut` can always run for at least `time_limit_without_permit`,
/// but it needs to acquire a permit from the semaphore before it can continue.
pub fn limit_concurrency<'a, F: Future + 'a>(
    fut: F,
    semaphore: &'a Semaphore,
    semaphore_group: SemaphoreGroup,
    time_limit_without_permit: Duration,
) -> impl Future<Output = F::Output> + 'a {
    ConcurrencyLimiter::new(
        semaphore
            .acquire()
            .map(|permit| permit.expect("the semaphore never be closed")),
        fut,
        semaphore_group,
        time_limit_without_permit,
    )
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
    semaphore_group: SemaphoreGroup,
    time_limit_without_permit: Duration,
    execution_time: Duration,
    state: LimitationState<'a>,
    _phantom: PhantomData<&'a ()>,
}

enum LimitationState<'a> {
    NotLimited,
    Acquiring(Instant),
    Acquired { _permit: SemaphorePermit<'a> },
}

impl<'a, PF, F> ConcurrencyLimiter<'a, PF, F>
where
    PF: Future<Output = SemaphorePermit<'a>>,
    F: Future,
{
    fn new(
        permit_fut: PF,
        fut: F,
        semaphore_group: SemaphoreGroup,
        time_limit_without_permit: Duration,
    ) -> Self {
        ConcurrencyLimiter {
            permit_fut,
            fut,
            semaphore_group,
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

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.project();
        match this.state {
            LimitationState::NotLimited if this.execution_time > this.time_limit_without_permit => {
                match this.permit_fut.poll(cx) {
                    Poll::Ready(permit) => {
                        *this.state = LimitationState::Acquired { _permit: permit };
                        COPR_ACQUIRE_SEMAPHORE_TYPE.acquired.inc();
                    }
                    Poll::Pending => {
                        *this.state = LimitationState::Acquiring(Instant::now());
                        this.semaphore_group.waiting_metric().inc();
                        return Poll::Pending;
                    }
                }
            }
            LimitationState::Acquiring(wait_start) => match this.permit_fut.poll(cx) {
                Poll::Ready(permit) => {
                    this.semaphore_group
                        .wait_time_metric()
                        .observe(wait_start.saturating_elapsed().as_secs_f64());
                    this.semaphore_group.waiting_metric().dec();
                    COPR_ACQUIRE_SEMAPHORE_TYPE.acquired.inc();
                    *this.state = LimitationState::Acquired { _permit: permit };
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
                *this.execution_time += now.saturating_elapsed();
                Poll::Pending
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::{
        sync::{
            Arc,
            atomic::{AtomicBool, Ordering},
        },
        thread,
    };

    use futures::future::FutureExt;
    use prometheus::core::Collector;
    use tokio::{
        task::yield_now,
        time::{sleep, timeout},
    };

    use super::*;

    fn metric_group(collector: &impl Collector) -> String {
        collector.collect()[0].get_metric()[0]
            .get_label()
            .iter()
            .find(|label| label.get_name() == "group")
            .unwrap()
            .get_value()
            .to_owned()
    }

    #[test]
    fn test_semaphore_group_metric_mapping() {
        assert_eq!(
            metric_group(SemaphoreGroup::Shared.waiting_metric()),
            "shared"
        );
        assert_eq!(
            metric_group(SemaphoreGroup::Shared.wait_time_metric()),
            "shared"
        );
        assert_eq!(
            metric_group(SemaphoreGroup::BackgroundLimited.waiting_metric()),
            "background_limited"
        );
        assert_eq!(
            metric_group(SemaphoreGroup::BackgroundLimited.wait_time_metric()),
            "background_limited"
        );
    }

    #[tokio::test(flavor = "current_thread")]
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
        tokio::spawn(timeout(Duration::from_millis(250), async move {
            limit_concurrency(
                work(2),
                &smp2,
                SemaphoreGroup::Shared,
                Duration::from_millis(500),
            )
            .await
        }))
        .await
        .unwrap()
        .unwrap();

        // Both t1 and t2 need a semaphore permit to finish. Although t2 is much shorter
        // than t1, it starts with t1
        smp.add_permits(1);
        let smp2 = smp.clone();

        let t1_finished = Arc::new(AtomicBool::new(false));

        let t1_finished_cloned = t1_finished.clone();
        let mut t1 = tokio::spawn(async move {
            limit_concurrency(work(8), &smp2, SemaphoreGroup::Shared, Duration::default()).await;
            t1_finished_cloned.store(true, Ordering::Release);
        })
        .fuse();

        sleep(Duration::from_millis(100)).await;
        let smp2 = smp.clone();
        let mut t2 = tokio::spawn(async move {
            limit_concurrency(work(2), &smp2, SemaphoreGroup::Shared, Duration::default()).await
        })
        .fuse();

        let deadline = sleep(Duration::from_millis(1500)).fuse();
        futures::pin_mut!(deadline);
        loop {
            futures_util::select! {
                _ = t1 => {},
                _ = t2 => {
                    if t1_finished.load(Ordering::Acquire) {
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
