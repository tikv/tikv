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
    base: Duration,
    cap: Duration,
    max_sleep: Duration,
}

impl BackoffBuilder {
    pub const fn new(base: Duration, cap: Duration, max_sleep: Duration) -> BackoffBuilder {
        BackoffBuilder {
            base,
            cap,
            max_sleep,
        }
    }

    pub fn create_async_backoff(&self) -> AsyncBackoff {
        AsyncBackoff {
            next_sleep: self.base,
            cap: self.cap,
            sleep_remain: self.max_sleep,
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

    use futures::executor::block_on_stream;

    #[test]
    fn test_async_backoff() {
        let mut instants = vec![Instant::now()];
        for _ in block_on_stream(
            BackoffBuilder::new(
                Duration::from_micros(100),
                Duration::from_millis(100),
                Duration::from_millis(300),
            )
            .create_async_backoff(),
        ) {
            instants.push(Instant::now());
        }
        // No sleep after max_sleep is reached.
        assert!(instants.pop().unwrap().elapsed() < Duration::from_millis(5));
        let intervals: Vec<_> = (1..instants.len())
            .map(|i| instants[i] - instants[i - 1])
            .collect();
        let expected = vec![
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
            Duration::from_millis(100),
        ];
        for (act, exp) in intervals[..intervals.len() - 1].iter().zip(expected) {
            // allow at most 5ms difference
            assert!((act.as_micros() as i64 - exp.as_micros() as i64).abs() < 5000);
        }
    }
}
