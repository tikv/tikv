// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

use crate::metrics::*;
use crossbeam::channel::TrySendError;
use std::cmp::min;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use futures::SinkExt;
use crossbeam::channel::{unbounded, Sender, Receiver};
use tokio::sync::mpsc::channel as async_channel;

#[derive(Debug)]
pub enum RateLimiterError<E> {
    SenderError(TrySendError<E>),
    SinkClosedError(usize),
    TryAgainError,
}

#[derive(Debug)]
pub enum DrainerError<E> {
    RateLimitExceededError,
}

pub struct RateLimiter<E> {
    sink: Sender<E>,
    state: Arc<State>,
}

pub struct Drainer<E> {
    receiver: Receiver<E>,
    state: Arc<State>,
}

struct State {
    is_sink_closed: AtomicBool,
    block_scan_threshold: usize,
    close_sink_threshold: usize,
    #[cfg(test)]
    has_blocked: AtomicBool,
}

pub fn new_pair<E>(block_scan_threshold: usize, close_sink_threshold: usize) -> (RateLimiter<E>, Drainer<E>) {
    let (sender, receiver) = unbounded::<E>();
    let state = Arc::new(State {
        is_sink_closed: AtomicBool::new(false),
        block_scan_threshold,
        close_sink_threshold,
        #[cfg(test)]
        has_blocked: AtomicBool::new(false),
    });
    let rate_limiter = RateLimiter::new(sender, state.clone());
    let drainer = Drainer::new(receiver, state_clone);
}

impl<E> RateLimiter<E> {
    fn new(
        sink: Sender<E>,
        state: Arc<State>,
    ) -> RateLimiter<E> {
        return RateLimiter {
            sink,
            state,
        };
    }

    pub fn send_realtime_event(&self, event: E) -> Result<(), RateLimiterError<E>> {
        if self.state.is_sink_closed.load(Ordering::SeqCst) {
            return Err(RateLimiterError::SinkClosedError(0));
        }

        let queue_size = self.sink.len();
        CDC_SINK_QUEUE_SIZE_HISTOGRAM.observe(queue_size as f64);
        if queue_size >= self.state.close_sink_threshold {
            warn!("cdc send_realtime_event queue length reached threshold"; "queue_size" => queue_size);
            self.state.is_sink_closed.store(true, Ordering::SeqCst);
            return Err(RateLimiterError::SinkClosedError(queue_size));
        }

        self.sink.try_send(event).map_err(|e| {
            warn!("cdc send_realtime_event error"; "err" => ?e);
            self.state.is_sink_closed.store(true, Ordering::SeqCst);
            RateLimiterError::SenderError(e)
        })?;

        Ok(())
    }

    pub async fn send_scan_event(&self, event: E) -> Result<(), RateLimiterError<E>> {
        let state_clone = self.state.clone();
        let sink_clone = self.sink.clone();
        let mut attempts: u64 = 0;

        let timer = CDC_SCAN_BLOCK_DURATION_HISTOGRAM.start_coarse_timer();
        loop {
            if state_clone.is_sink_closed.load(Ordering::SeqCst) {
                return Err(RateLimiterError::SinkClosedError(0));
            }

            let queue_size = sink_clone.len();
            CDC_SINK_QUEUE_SIZE_HISTOGRAM.observe(queue_size as f64);

            if queue_size >= state_clone.block_scan_threshold {
                // used for unit testing
                #[cfg(test)]
                self.state.has_blocked.store(true, Ordering::SeqCst);

                info!("cdc send_scan_event backoff"; "queue_size" => queue_size, "attempts" => attempts);
                let backoff_ms: u64 = 32 << min(attempts, 10);
                tokio::time::delay_for(std::time::Duration::from_millis(backoff_ms)).await;
                attempts += 1;

                continue;
            }
            break;
        }

        timer.observe_duration();
        sink_clone.try_send(event).map_err(|e| {
            warn!("cdc send_scan_event error"; "err" => ?e);
            match e {
                crossbeam::TrySendError::Disconnected(_) => {
                    state_clone.is_sink_closed.store(true, Ordering::SeqCst);
                    info!("cdc sink closed");
                }
                _ => {}
            }
            RateLimiterError::SenderError(e)
        })
    }

    pub fn notify_close(self) {
        self.state.is_sink_closed.store(true, Ordering::SeqCst);
    }
}

impl<E> Clone for RateLimiter<E> {
    fn clone(&self) -> Self {
        RateLimiter {
            sink: self.sink.clone(),
            state: self.state.clone(),
        }
    }
}

impl<E> Drainer<E> {
    fn new(receiver: Receiver<E>, state: Arc<State>) -> Drainer<E> {
        Drainer {
            receiver,
            state
        }
    }

    pub async fn drain<F: Copy, S: SinkExt<(E, F)>>(self, mut rpc_sink: S, flag: F) -> Result<(), DrainerError<E>> {

    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tikv_util::mpsc::batch::unbounded;

    type MockCdcEvent = u64;

    #[test]
    fn test_basic_realtime() -> Result<(), RateLimiterError<MockCdcEvent>> {
        let (tx, rx) = unbounded::<MockCdcEvent>(1);
        let rate_limiter = RateLimiter::new(tx, 1024, 1024);

        for i in 0..10u64 {
            rate_limiter.send_realtime_event(i)?;
        }

        for i in 0..10u64 {
            assert_eq!(rx.recv().unwrap(), i);
        }

        Ok(())
    }

    #[tokio::test]
    async fn test_basic_scan() -> Result<(), RateLimiterError<MockCdcEvent>> {
        let (tx, rx) = unbounded::<MockCdcEvent>(1);
        let rate_limiter = RateLimiter::new(tx, 1024, 1024);

        for i in 0..10u64 {
            rate_limiter.send_scan_event(i).await?;
        }

        for i in 0..10u64 {
            assert_eq!(rx.recv().unwrap(), i);
        }

        Ok(())
    }

    #[test]
    fn test_realtime_disconnected() -> Result<(), RateLimiterError<MockCdcEvent>> {
        let (tx, rx) = unbounded::<MockCdcEvent>(1);
        let rate_limiter = RateLimiter::new(tx, 1024, 1024);

        rate_limiter.send_realtime_event(1)?;
        rate_limiter.send_realtime_event(2)?;
        rate_limiter.send_realtime_event(3)?;
        drop(rx);
        match rate_limiter.send_realtime_event(4) {
            Ok(_) => panic!("expected error"),
            Err(RateLimiterError::SenderError(e)) => {}
            _ => panic!("expected SenderError"),
        }

        match rate_limiter.send_realtime_event(5) {
            Ok(_) => panic!("expected error"),
            Err(RateLimiterError::SinkClosedError(len)) => assert_eq!(len, 0),
            _ => panic!("expected SinkClosedError"),
        }

        Ok(())
    }

    #[tokio::test]
    async fn test_scan_disconnected() -> Result<(), RateLimiterError<MockCdcEvent>> {
        let (tx, rx) = unbounded::<MockCdcEvent>(1);
        let rate_limiter = RateLimiter::new(tx, 1024, 1024);

        rate_limiter.send_scan_event(1).await?;
        rate_limiter.send_scan_event(2).await?;
        rate_limiter.send_scan_event(3).await?;
        drop(rx);
        match rate_limiter.send_scan_event(4).await {
            Ok(_) => panic!("expected error"),
            Err(RateLimiterError::SenderError(e)) => {}
            _ => panic!("expected SenderError"),
        }

        match rate_limiter.send_scan_event(5).await {
            Ok(_) => panic!("expected error"),
            Err(RateLimiterError::SinkClosedError(len)) => assert_eq!(len, 0),
            _ => panic!("expected SinkClosedError"),
        }

        Ok(())
    }

    #[test]
    fn test_realtime_congested() -> Result<(), RateLimiterError<MockCdcEvent>> {
        let (tx, rx) = unbounded::<MockCdcEvent>(1);
        let rate_limiter = RateLimiter::new(tx, 1024, 5);

        rate_limiter.send_realtime_event(1)?;
        rate_limiter.send_realtime_event(2)?;
        rate_limiter.send_realtime_event(3)?;
        rate_limiter.send_realtime_event(4)?;
        rate_limiter.send_realtime_event(5)?;
        match rate_limiter.send_realtime_event(6) {
            Ok(_) => panic!("expected error"),
            Err(RateLimiterError::SinkClosedError(len)) => assert_eq!(len, 5),
            _ => panic!("expected SinkClosedError"),
        }

        match rate_limiter.send_realtime_event(6) {
            Ok(_) => panic!("expected error"),
            Err(RateLimiterError::SinkClosedError(len)) => assert_eq!(len, 0),
            _ => panic!("expected SinkClosedError"),
        }

        Ok(())
    }

    #[tokio::test]
    async fn test_scan_backoff_normal() -> Result<(), RateLimiterError<MockCdcEvent>> {
        let (tx, rx) = unbounded::<MockCdcEvent>(1);
        let rate_limiter = RateLimiter::new(tx, 5, 1024);

        rate_limiter.send_realtime_event(1)?;
        rate_limiter.send_realtime_event(2)?;
        rate_limiter.send_scan_event(3).await?;
        rate_limiter.send_scan_event(4).await?;
        rate_limiter.send_scan_event(5).await?;
        assert_eq!(rate_limiter.state.has_blocked.load(Ordering::SeqCst), false);

        let rate_limiter_clone = rate_limiter.clone();
        tokio::spawn(async move {
            rate_limiter_clone.send_scan_event(6).await.unwrap();
            assert_eq!(rate_limiter.state.has_blocked.load(Ordering::SeqCst), true);
        });

        tokio::time::delay_for(std::time::Duration::from_millis(200)).await;
        let finished = Arc::new(AtomicBool::new(false));
        let finished_clone = finished.clone();
        std::thread::spawn(move || {
            assert_eq!(rx.recv().unwrap(), 1);
            assert_eq!(rx.recv().unwrap(), 2);
            assert_eq!(rx.recv().unwrap(), 3);
            assert_eq!(rx.recv().unwrap(), 4);
            assert_eq!(rx.recv().unwrap(), 5);
            assert_eq!(rx.recv().unwrap(), 6);
            finished_clone.store(true, Ordering::SeqCst);
        });

        while !finished.load(Ordering::SeqCst) {
            tokio::task::yield_now().await;
        }

        Ok(())
    }

    #[tokio::test]
    async fn test_scan_backoff_disconnected() -> Result<(), RateLimiterError<MockCdcEvent>> {
        let (tx, rx) = unbounded::<MockCdcEvent>(1);
        let rate_limiter = RateLimiter::new(tx, 5, 1024);

        rate_limiter.send_realtime_event(1)?;
        rate_limiter.send_realtime_event(2)?;
        rate_limiter.send_scan_event(3).await?;
        rate_limiter.send_scan_event(4).await?;
        rate_limiter.send_scan_event(5).await?;
        assert_eq!(rate_limiter.state.has_blocked.load(Ordering::SeqCst), false);

        let rate_limiter_clone = rate_limiter.clone();
        let finished = Arc::new(AtomicBool::new(false));
        let finished_clone = finished.clone();

        tokio::spawn(async move {
            match rate_limiter_clone.send_scan_event(6).await {
                Ok(_) => panic!("expected error"),
                Err(RateLimiterError::SinkClosedError(len)) => assert_eq!(len, 0),
                _ => panic!("expected SinkClosedError"),
            }
            assert_eq!(
                rate_limiter_clone.state.has_blocked.load(Ordering::SeqCst),
                true
            );
            finished_clone.store(true, Ordering::SeqCst);
        });

        tokio::time::delay_for(std::time::Duration::from_millis(200)).await;
        drop(rx);
        rate_limiter.notify_close();

        while !finished.load(Ordering::SeqCst) {
            tokio::task::yield_now().await;
        }

        Ok(())
    }
}
