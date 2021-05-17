// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

use pin_project::pin_project;
use std::future::Future;
use std::marker::PhantomData;
use std::pin::Pin;
use std::task::{Context, Poll};
use tikv_util::deadline::{Deadline, DeadlineError};

/// Checks the deadline before every poll of the future. If the deadline is exceeded,
/// `DeadlineError` is returned.
pub fn check_deadline<'a, F: Future + 'a>(
    fut: F,
    deadline: Deadline,
) -> impl Future<Output = Result<F::Output, DeadlineError>> + 'a {
    DeadlineChecker {
        fut,
        deadline,
        _phantom: PhantomData,
    }
}

#[pin_project]
struct DeadlineChecker<'a, F: Future + 'a> {
    #[pin]
    fut: F,
    deadline: Deadline,
    _phantom: PhantomData<&'a ()>,
}

impl<'a, F> Future for DeadlineChecker<'a, F>
where
    F: Future + 'a,
{
    type Output = Result<F::Output, DeadlineError>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context) -> Poll<Self::Output> {
        self.deadline.check()?;
        let this = self.project();
        this.fut.poll(cx).map(Ok)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::{thread, time::Duration};
    use tokio::task::yield_now;

    #[tokio::test(basic_scheduler)]
    async fn test_deadline_checker() {
        async fn work(iter: i32) {
            for i in 0..iter {
                thread::sleep(Duration::from_millis(50));
                if i < iter - 1 {
                    yield_now().await;
                }
            }
        }

        let res = check_deadline(work(5), Deadline::from_now(Duration::from_millis(500))).await;
        assert!(res.is_ok());

        let res = check_deadline(work(100), Deadline::from_now(Duration::from_millis(500))).await;
        assert!(res.is_err());
    }
}
