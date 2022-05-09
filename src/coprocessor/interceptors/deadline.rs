// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

use std::{
    future::Future,
    pin::Pin,
    task::{Context, Poll},
};

use pin_project::pin_project;
use tikv_util::deadline::{Deadline, DeadlineError};

/// Checks the deadline before every poll of the future. If the deadline is exceeded,
/// `DeadlineError` is returned.
pub fn check_deadline<F: Future>(
    fut: F,
    deadline: Deadline,
) -> impl Future<Output = Result<F::Output, DeadlineError>> {
    DeadlineChecker { fut, deadline }
}

#[pin_project]
struct DeadlineChecker<F: Future> {
    #[pin]
    fut: F,
    deadline: Deadline,
}

impl<F> Future for DeadlineChecker<F>
where
    F: Future,
{
    type Output = Result<F::Output, DeadlineError>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        self.deadline.check()?;
        let this = self.project();
        this.fut.poll(cx).map(Ok)
    }
}

#[cfg(test)]
mod tests {
    use std::{thread, time::Duration};

    use tokio::task::yield_now;

    use super::*;

    #[tokio::test(flavor = "current_thread")]
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
