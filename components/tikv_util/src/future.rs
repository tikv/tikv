// Copyright 2018 TiKV Project Authors. Licensed under Apache-2.0.

use std::{
    cell::UnsafeCell,
    sync::{
        Arc,
        atomic::{AtomicU8, Ordering},
    },
};

use futures::{
    channel::{mpsc, oneshot as futures_oneshot},
    future::{self, BoxFuture, Future, FutureExt, TryFutureExt},
    stream::{Stream, StreamExt},
    task::{self, ArcWake, Context, Poll},
};

use crate::{
    callback::must_call,
    time::{Duration, Instant},
    timer::GLOBAL_TIMER_HANDLE,
};

/// Generates a paired future and callback so that when callback is being
/// called, its result is automatically passed as a future result.
pub fn paired_future_callback<T>() -> (Box<dyn FnOnce(T) + Send>, futures_oneshot::Receiver<T>)
where
    T: Send + 'static,
{
    let (tx, future) = futures_oneshot::channel::<T>();
    let callback = Box::new(move |result| {
        let r = tx.send(result);
        if r.is_err() {
            warn!("paired_future_callback: Failed to send result to the future rx, discarded.");
        }
    });
    (callback, future)
}

pub fn paired_must_called_future_callback<T>(
    arg_on_drop: impl FnOnce() -> T + Send + 'static,
) -> (Box<dyn FnOnce(T) + Send>, futures_oneshot::Receiver<T>)
where
    T: Send + 'static,
{
    let (tx, future) = futures_oneshot::channel::<T>();
    let callback = must_call(
        move |result| {
            let r = tx.send(result);
            if r.is_err() {
                warn!("paired_future_callback: Failed to send result to the future rx, discarded.");
            }
        },
        arg_on_drop,
    );
    (callback, future)
}

/// Create a stream proxy with buffer representing the remote stream. The
/// returned task will receive messages from the remote stream as much as
/// possible.
pub fn create_stream_with_buffer<T, S>(
    s: S,
    size: usize,
) -> (
    impl Stream<Item = T> + Send + 'static,
    impl Future<Output = ()> + Send + 'static,
)
where
    S: Stream<Item = T> + Send + 'static,
    T: Send + 'static,
{
    let (tx, rx) = mpsc::channel::<T>(size);
    let driver = s
        .then(future::ok::<T, mpsc::SendError>)
        .forward(tx)
        .map_err(|e| warn!("stream with buffer send error"; "error" => %e))
        .map(|_| ());
    (rx, driver)
}

/// Polls the provided future immediately. If the future is not ready,
/// it will register the waker. When the event is ready, the waker will
/// be notified, then the internal future is immediately polled in the
/// thread calling `wake()`.
pub fn poll_future_notify<F: Future<Output = ()> + Send + 'static>(f: F) {
    let f: BoxFuture<'static, ()> = Box::pin(f);
    let poller = Arc::new(PollAtWake {
        f: UnsafeCell::new(Some(f)),
        state: AtomicU8::new(IDLE),
    });
    PollAtWake::poll(&poller)
}

/// The future is not processed by any one.
const IDLE: u8 = 0;
/// The future is being polled by some thread.
const POLLING: u8 = 1;
/// The future is woken when being polled.
const NOTIFIED: u8 = 2;

/// A waker that will poll the future immediately when waking up.
struct PollAtWake {
    f: UnsafeCell<Option<BoxFuture<'static, ()>>>,
    state: AtomicU8,
}

impl PollAtWake {
    fn poll(arc_self: &Arc<PollAtWake>) {
        let mut state = arc_self.state.load(Ordering::Relaxed);
        loop {
            match state {
                IDLE => {
                    match arc_self.state.compare_exchange_weak(
                        IDLE,
                        POLLING,
                        Ordering::SeqCst,
                        Ordering::SeqCst,
                    ) {
                        Ok(_) => break,
                        Err(s) => state = s,
                    }
                }
                POLLING => {
                    match arc_self.state.compare_exchange_weak(
                        POLLING,
                        NOTIFIED,
                        Ordering::SeqCst,
                        Ordering::SeqCst,
                    ) {
                        // The polling thread should poll the future again.
                        Ok(_) => return,
                        Err(s) => state = s,
                    }
                }
                NOTIFIED => {
                    // It will be polled again, so we don't need to do anything here.
                    return;
                }
                _ => panic!("unexpected state {}", state),
            }
        }

        let f = unsafe { &mut *arc_self.f.get() };
        let fut = match f {
            Some(f) => f,
            None => {
                // It can't be `None` for the moment. But it's not a big mistake, just ignore.
                return;
            }
        };

        let waker = task::waker_ref(arc_self);
        let cx = &mut Context::from_waker(&waker);
        loop {
            match fut.as_mut().poll(cx) {
                // Likely pending
                Poll::Pending => (),
                Poll::Ready(()) => {
                    // We skip updating states here as all future wake should be ignored once
                    // a future is resolved.
                    f.take();
                    return;
                }
            }
            match arc_self
                .state
                .compare_exchange(POLLING, IDLE, Ordering::SeqCst, Ordering::SeqCst)
            {
                Ok(_) => return,
                Err(s) => {
                    if s == NOTIFIED {
                        // Only this thread can change the state from NOTIFIED, so it has to
                        // succeed.
                        match arc_self.state.compare_exchange(
                            NOTIFIED,
                            POLLING,
                            Ordering::SeqCst,
                            Ordering::SeqCst,
                        ) {
                            Ok(_) => continue,
                            Err(s) => panic!("unexpected state {}", s),
                        }
                    } else {
                        panic!("unexpcted state {}", s);
                    }
                }
            }
        }
    }
}

// `BoxFuture` is Send, so `PollAtWake` is Send and Sync.
unsafe impl Send for PollAtWake {}
unsafe impl Sync for PollAtWake {}

impl ArcWake for PollAtWake {
    #[inline]
    fn wake_by_ref(arc_self: &Arc<Self>) {
        PollAtWake::poll(arc_self)
    }
}

/// Poll the future immediately. If the future is ready, returns the result.
/// Otherwise just ignore the future.
#[inline]
pub fn try_poll<T>(f: impl Future<Output = T>) -> Option<T> {
    futures::executor::block_on(async move {
        futures::select_biased! {
            res = f.fuse() => Some(res),
            _ = futures::future::ready(()).fuse() => None,
        }
    })
}

// Run a future with a timeout on the current thread. Returns Err if times out.
#[allow(clippy::result_unit_err)]
pub fn block_on_timeout<F>(fut: F, dur: std::time::Duration) -> Result<F::Output, ()>
where
    F: std::future::Future,
{
    use futures_util::compat::Future01CompatExt;

    let mut timeout = GLOBAL_TIMER_HANDLE
        .delay(std::time::Instant::now() + dur)
        .compat()
        .fuse();
    futures::pin_mut!(fut);
    let mut f = fut.fuse();
    futures::executor::block_on(async {
        futures::select! {
            _ = timeout => Err(()),
            item = f => Ok(item),
        }
    })
}

/// Runs an async future with a timeout. Returns `Ok(result)` if the future
/// completes before the timeout, or returns an error if it times out.
///
/// This function uses lazy timer initialization: it only creates a timer if
/// the future doesn't complete immediately. This optimization avoids timer
/// creation overhead in the common case where futures complete quickly
/// (e.g., local snapshot reads).
///
/// This function works in yatp FuturePool contexts and doesn't require
/// a Tokio runtime.
pub async fn async_timeout<F>(
    fut: F,
    dur: std::time::Duration,
) -> Result<F::Output, Box<dyn std::error::Error + Send + Sync>>
where
    F: std::future::Future + Send,
{
    use futures_util::compat::Future01CompatExt;

    // Use select_biased! to check if future completes immediately
    // If the future completes first, return without creating a timer (fast path)
    // If ready() completes first, the future is pending, so create timer (slow
    // path)
    futures::pin_mut!(fut);
    let mut fut = fut.fuse();

    futures::select_biased! {
        result = fut => {
            // Fast path: future completed immediately, no timer needed
            Ok(result)
        }
        _ = futures::future::ready(()) => {
            // Slow path: future is pending, create timer and use select!
            let timeout_fut = GLOBAL_TIMER_HANDLE
                .delay(std::time::Instant::now() + dur)
                .compat()
                .fuse();
            futures::pin_mut!(timeout_fut);

            futures::select! {
                result = fut => Ok(result),
                _ = timeout_fut => Err(format!("future timeout after {:?}", dur).into()),
            }
        }
    }
}

pub struct RescheduleChecker<B> {
    duration: Duration,
    start: Instant,
    future_builder: B,
}

impl<T: Future, B: Fn() -> T> RescheduleChecker<B> {
    pub fn new(future_builder: B, duration: Duration) -> Self {
        Self {
            duration,
            start: Instant::now_coarse(),
            future_builder,
        }
    }

    pub async fn check(&mut self) {
        if self.start.saturating_elapsed() >= self.duration {
            (self.future_builder)().await;
            self.start = Instant::now_coarse();
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::AtomicUsize;

    use futures::task::Poll;

    use super::*;

    #[test]
    fn test_in_place_wake() {
        let poll_times = Arc::new(AtomicUsize::new(0));
        let times = poll_times.clone();
        let f = futures::future::poll_fn(move |cx| {
            cx.waker().wake_by_ref();
            let last_time = times.fetch_add(1, Ordering::SeqCst);
            if last_time == 0 {
                Poll::Pending
            } else {
                Poll::Ready(())
            }
        });
        poll_future_notify(f);
        // The future will be woken twice, but only polled twice.
        // The sequence should be:
        // 1. future gets polled
        //   1.1 future gets woken
        //      1.1.1 future marks NOTIFIED
        //   1.2 future returns Poll::Pending
        // 2. future finishes polling, then re-poll
        //   2.1 future gets woken
        //     2.1.1 future marks NOTIFIED
        //   2.2 future returns Poll::Ready
        // 3. future gets ready, ignore NOTIFIED
        assert_eq!(poll_times.load(Ordering::SeqCst), 2);
    }

    #[test]
    fn test_try_poll() {
        let f = futures::future::ready(1);
        assert_eq!(try_poll(f), Some(1));
        let f = futures::future::pending::<()>();
        assert_eq!(try_poll(f), None);
    }

    #[test]
    fn test_async_timeout_success() {
        use futures::executor::block_on;
        use futures_util::compat::Future01CompatExt;

        // Test case: future completes before timeout
        let fast_future = async {
            GLOBAL_TIMER_HANDLE
                .delay(std::time::Instant::now() + Duration::from_millis(10))
                .compat()
                .await
                .unwrap();
            42
        };
        let result = block_on(async_timeout(
            fast_future,
            std::time::Duration::from_millis(100),
        ));
        assert!(result.is_ok(), "future should complete before timeout");
        assert_eq!(result.unwrap(), 42);
    }

    #[test]
    fn test_async_timeout_timeout() {
        use futures::executor::block_on;
        use futures_util::compat::Future01CompatExt;

        // Test case: future times out before completing
        let slow_future = async {
            GLOBAL_TIMER_HANDLE
                .delay(std::time::Instant::now() + Duration::from_millis(100))
                .compat()
                .await
                .unwrap();
            42
        };
        let result = block_on(async_timeout(
            slow_future,
            std::time::Duration::from_millis(10),
        ));
        assert!(result.is_err(), "future should timeout");
        let err_msg = result.unwrap_err().to_string();
        assert!(
            err_msg.contains("future timeout after"),
            "error message should mention timeout"
        );
    }

    #[test]
    fn test_async_timeout_immediate_completion() {
        use futures::executor::block_on;

        // Test case: future completes immediately
        let immediate_future = async { 123 };
        let result = block_on(async_timeout(
            immediate_future,
            std::time::Duration::from_millis(100),
        ));
        assert!(result.is_ok(), "immediate future should succeed");
        assert_eq!(result.unwrap(), 123);
    }

    #[test]
    fn test_async_timeout_with_result() {
        use futures::executor::block_on;

        // Test case: future returns Result
        let success_future = async { Ok::<i32, &str>(100) };
        let result = block_on(async_timeout(
            success_future,
            std::time::Duration::from_millis(50),
        ));
        assert!(result.is_ok(), "success result should propagate");
        assert_eq!(result.unwrap(), Ok(100));

        let error_future = async { Err::<i32, &str>("test error") };
        let result = block_on(async_timeout(
            error_future,
            std::time::Duration::from_millis(50),
        ));
        assert!(
            result.is_ok(),
            "error result should propagate (not timeout)"
        );
        assert_eq!(result.unwrap(), Err("test error"));
    }

    #[test]
    fn test_async_timeout_lazy_timer_optimization() {
        use futures::executor::block_on;
        use futures_util::compat::Future01CompatExt;

        // Test case: verify that fast futures complete without creating timer
        // (lazy optimization: timer is only created if future doesn't complete
        // immediately)
        let fast_future = async { 42 };
        let result = block_on(async_timeout(
            fast_future,
            std::time::Duration::from_millis(100),
        ));
        assert!(result.is_ok(), "fast future should complete successfully");
        assert_eq!(result.unwrap(), 42);

        // Test case: verify that slow futures still work correctly with timer
        // This verifies the lazy timer is created when needed
        let slow_future = async {
            GLOBAL_TIMER_HANDLE
                .delay(std::time::Instant::now() + Duration::from_millis(50))
                .compat()
                .await
                .unwrap();
            100
        };
        let result = block_on(async_timeout(
            slow_future,
            std::time::Duration::from_millis(200),
        ));
        assert!(result.is_ok(), "slow future should complete before timeout");
        assert_eq!(result.unwrap(), 100);
    }
}
