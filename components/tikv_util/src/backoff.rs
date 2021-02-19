// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

use crate::timer::GLOBAL_TIMER_HANDLE;
use futures::{
    compat::Compat01As03,
    stream::{Stream, StreamExt},
    FutureExt,
};
use std::{
    cmp,
    pin::Pin,
    task::{Context, Poll},
    time::{Duration, Instant},
};
use tokio_timer::Delay;

pub struct BackoffBuilder {
    // Initial backoff duration
    pub base: Duration,
    // Maximum time of a single sleep
    pub cap: Duration,
    // Maximum total sleep time. An error will be returned if the limit is exceeded.
    pub total_limit: Duration,
    // It is now an expotential backoff with no jitter.
    // It is possible to add more options in the future.
}

impl BackoffBuilder {
    // Creates an async stream of the backoff. Before each retry, await the next item
    // of the stream, it will be ready after sleeping for exponential time.
    pub fn create_async_backoff(&self) -> AsyncBackoff {
        AsyncBackoff {
            next_sleep: self.base,
            cap: self.cap,
            sleep_remain: self.total_limit,
            sleep_instant: None,
            delay_fut: None,
        }
    }
}

pub struct AsyncBackoff {
    next_sleep: Duration,
    cap: Duration,
    sleep_remain: Duration,
    sleep_instant: Option<Instant>,
    delay_fut: Option<Compat01As03<Delay>>,
}

impl Stream for AsyncBackoff {
    type Item = ();

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        if self.delay_fut.is_none() {
            if self.sleep_remain == Duration::default() {
                return Poll::Ready(None);
            }
            if self.next_sleep > self.sleep_remain {
                self.next_sleep = self.sleep_remain;
            }
            let sleep_instant = Instant::now();
            // TODO: GLOBAL_TIMER_HANDLE is a low-precesion timer which is not suitable for
            // sub-millisecond durations. Maybe switch to other timer implementations?
            self.delay_fut = Some(Compat01As03::new(
                GLOBAL_TIMER_HANDLE.delay(sleep_instant + self.next_sleep),
            ));
            self.sleep_instant = Some(sleep_instant);
            self.next_sleep = cmp::min(self.next_sleep * 2, self.cap);
        }

        match self.delay_fut.as_mut().unwrap().poll_unpin(cx) {
            Poll::Pending => Poll::Pending,
            Poll::Ready(Ok(())) => {
                let real_sleep = self.sleep_instant.take().unwrap().elapsed();
                self.sleep_remain = self.sleep_remain.saturating_sub(real_sleep);
                self.delay_fut = None;
                Poll::Ready(Some(()))
            }
            _ => Poll::Ready(None),
        }
    }
}

impl AsyncBackoff {
    pub async fn retry<T, E>(mut self, mut f: impl FnMut() -> Result<T, E>) -> Result<T, E> {
        loop {
            match f() {
                Ok(t) => return Ok(t),
                Err(e) => {
                    if self.next().await.is_none() {
                        return Err(e);
                    }
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use futures::executor::{block_on, block_on_stream};
    use std::thread;

    // 10ms as the duration of an iteration. Backoff duration shouldn't be affected by it.
    const ITER_DUR: Duration = Duration::from_millis(10);

    const BACKOFF_BUILDER: BackoffBuilder = BackoffBuilder {
        base: Duration::from_micros(100),
        cap: Duration::from_millis(100),
        total_limit: Duration::from_millis(250),
    };

    const SLEEP_DURATION: [Duration; 12] = [
        Duration::from_micros(100),
        Duration::from_micros(200),
        Duration::from_micros(400),
        Duration::from_micros(800),
        Duration::from_micros(1600),
        Duration::from_micros(3200),
        Duration::from_micros(6400),
        Duration::from_micros(12800),
        Duration::from_micros(25600),
        Duration::from_micros(51200),
        Duration::from_millis(100),
        Duration::from_micros(47700),
    ];

    fn check_instants(instants: Vec<Instant>) {
        let intervals: Vec<_> = (1..instants.len())
            .map(|i| instants[i] - instants[i - 1] - ITER_DUR)
            .collect();
        for (act, exp) in intervals[..intervals.len() - 1].iter().zip(&SLEEP_DURATION) {
            // allow at most 5ms difference
            assert!((act.as_micros() as i64 - exp.as_micros() as i64).abs() < 5000);
        }
    }

    #[test]
    fn test_async_backoff() {
        // 10ms as the duration of an iteration. Backoff duration shouldn't be affected by it.
        const ITER_DUR: Duration = Duration::from_millis(10);

        let mut instants = vec![Instant::now()];
        thread::sleep(ITER_DUR);
        for _ in block_on_stream(BACKOFF_BUILDER.create_async_backoff()) {
            instants.push(Instant::now());
            thread::sleep(ITER_DUR);
        }
        assert_eq!(instants.len(), SLEEP_DURATION.len() + 1);
        // No sleep after total_limit is reached.
        assert!(instants.last().unwrap().elapsed() - ITER_DUR < Duration::from_millis(5));
        check_instants(instants);
    }

    #[test]
    fn test_async_backoff_retry_success() {
        // 10ms as the duration of an iteration. Backoff duration shouldn't be affected by it.
        const ITER_DUR: Duration = Duration::from_millis(10);

        let backoff = BACKOFF_BUILDER.create_async_backoff();
        let initial = Instant::now();
        let mut i = 0;
        let mut instants = Vec::new();
        let res = block_on(backoff.retry(|| {
            instants.push(Instant::now());
            thread::sleep(ITER_DUR);
            i += 1;
            if i < 10 {
                Err(())
            } else {
                Ok(())
            }
        }));
        assert_eq!(res, Ok(()));
        assert_eq!(instants.len(), 10);
        // No sleep before first iteration
        assert!(instants[0] - initial < Duration::from_millis(5));
        // No sleep after Ok is returned.
        assert!(instants.last().unwrap().elapsed() - ITER_DUR < Duration::from_millis(5));
        check_instants(instants);
    }

    #[test]
    fn test_async_backoff_retry_timeout() {
        // 10ms as the duration of an iteration. Backoff duration shouldn't be affected by it.
        const ITER_DUR: Duration = Duration::from_millis(10);

        let backoff = BACKOFF_BUILDER.create_async_backoff();
        let initial = Instant::now();
        let mut instants = Vec::new();
        let res = block_on(backoff.retry(|| {
            instants.push(Instant::now());
            thread::sleep(ITER_DUR);
            Err::<(), _>(())
        }));
        assert_eq!(res, Err(()));
        assert_eq!(instants.len(), SLEEP_DURATION.len() + 1);
        // No sleep before first iteration
        assert!(instants[0] - initial < Duration::from_millis(5));
        // No sleep after total_limit is reached.
        assert!(instants.last().unwrap().elapsed() - ITER_DUR < Duration::from_millis(5));
        check_instants(instants);
    }
}
