// Copyright 2022 TiKV Project Authors. Licensed under Apache-2.0.

use std::{
    cell::Cell,
    future::Future,
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
    time::Duration,
};

#[cfg(not(test))]
use file_system::get_thread_io_bytes_total;
use file_system::IoBytes;
use futures::compat::{Compat01As03, Future01CompatExt};
use pin_project::pin_project;
use tikv_util::{time::Instant, timer::GLOBAL_TIMER_HANDLE, warn};
use tokio_timer::Delay;

use crate::{
    resource_group::{ResourceConsumeType, ResourceController},
    resource_limiter::ResourceLimiter,
};

const MAX_WAIT_DURATION: Duration = Duration::from_secs(10);

thread_local! {
    static THREAD_TOTAL_IO_BYTES: Cell<CachedIoBytes> = Cell::new(CachedIoBytes::default());
}

#[derive(Copy, Clone)]
struct CachedIoBytes {
    io_bytes: IoBytes,
    state: LocalIoState,
}

impl Default for CachedIoBytes {
    fn default() -> Self {
        Self {
            state: LocalIoState::Outdated,
            io_bytes: IoBytes::default(),
        }
    }
}

impl CachedIoBytes {
    #[cfg(test)]
    fn new(state: LocalIoState, read: u64, write: u64) -> Self {
        Self {
            io_bytes: IoBytes { read, write },
            state,
        }
    }
}

#[cfg(not(test))]
fn get_thread_io_bytes_stats() -> Result<IoBytes, String> {
    get_thread_io_bytes_total()
}

#[cfg(test)]
fn get_thread_io_bytes_stats() -> Result<IoBytes, String> {
    fail::fail_point!("failed_to_get_thread_io_bytes_stats", |_| {
        Err("get_thread_io_bytes_total failed".into())
    });
    thread_local! {
        static TOTAL_BYTES: Cell<IoBytes> = Cell::new(IoBytes::default());
    }

    let mut new_bytes = TOTAL_BYTES.get();
    new_bytes.read += 100;
    new_bytes.write += 50;
    TOTAL_BYTES.set(new_bytes);
    Ok(new_bytes)
}

// `LocalIoState` is a flag used to track the state of thread variable
// `THREAD_TOTAL_IO_BYTES`. typically, for a `LimitedFuture`, we need to call
// `get_thread_io_bytes_total` before and after `poll` so we can calcualte the
// io bytes delta. But if the previous task is also `LimitedFuture`, then we can
// directly use the threadlocal value. We use this state to avoid a extra call
// of `get_thread_io_bytes_total`.
#[derive(Copy, Clone, PartialEq, Eq, Debug)]
enum LocalIoState {
    // the thread local is valid, should be set after poll of `LimitedFuture`.
    Fresh,
    // the thread local value is to outdated if incoming future is not background type.
    // should be set at the beginning of `TrackIoStateFuture`.
    MaybeOutdated,
    // local io stats is outdated, should be set at the end of `TrackIoStateFuture` if the
    // previous state is `MaybeOutdated`.
    Outdated,
}

#[pin_project]
pub struct ControlledFuture<F> {
    #[pin]
    future: F,
    controller: Arc<ResourceController>,
    group_name: Vec<u8>,
}

impl<F> ControlledFuture<F> {
    pub fn new(future: F, controller: Arc<ResourceController>, group_name: Vec<u8>) -> Self {
        Self {
            future,
            controller,
            group_name,
        }
    }
}

impl<F: Future> Future for ControlledFuture<F> {
    type Output = F::Output;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.project();
        let now = Instant::now();
        let res = this.future.poll(cx);
        this.controller.consume(
            this.group_name,
            ResourceConsumeType::CpuTime(now.saturating_elapsed()),
        );
        res
    }
}

// `TrackIoStateFuture` is used to update the thread local variable
// `THREAD_TOTAL_IO_BYTES`. it should be used pair with `LimitedFuture` so the
// `THREAD_TOTAL_IO_BYTES` state is valid.
#[pin_project]
pub struct TrackIoStateFuture<F> {
    #[pin]
    f: F,
}

impl<F: Future> TrackIoStateFuture<F> {
    pub fn new(f: F) -> Self {
        Self { f }
    }
}

impl<F: Future> Future for TrackIoStateFuture<F> {
    type Output = F::Output;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.project();
        THREAD_TOTAL_IO_BYTES.with(|s| {
            let mut stat = s.get();
            stat.state = LocalIoState::MaybeOutdated;
            s.set(stat);
        });
        let res = this.f.poll(cx);
        THREAD_TOTAL_IO_BYTES.with(|s| {
            let mut stat = s.get();
            if stat.state == LocalIoState::MaybeOutdated {
                stat.state = LocalIoState::Outdated;
                s.set(stat);
            }
        });
        res
    }
}

// `LimitedFuture` wraps a Future with ResourceLimiter, it will automically
// statistics the cpu time and io bytes consumed by the future, and do async
// waiting according the configuration of the ResourceLimiter.
#[pin_project]
pub struct LimitedFuture<F: Future> {
    #[pin]
    f: F,
    #[pin]
    pre_delay: OptionalFuture<Compat01As03<Delay>>,
    #[pin]
    post_delay: OptionalFuture<Compat01As03<Delay>>,
    resource_limiter: Arc<ResourceLimiter>,
    res: Poll<F::Output>,
}

impl<F: Future> LimitedFuture<F> {
    pub fn new(f: F, resource_limiter: Arc<ResourceLimiter>) -> Self {
        Self {
            f,
            pre_delay: None.into(),
            post_delay: None.into(),
            resource_limiter,
            res: Poll::Pending,
        }
    }
}

impl<F: Future> Future for LimitedFuture<F> {
    type Output = F::Output;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let mut this = self.project();
        if !this.post_delay.is_done() {
            assert!(this.pre_delay.is_done());
            std::mem::swap(&mut *this.pre_delay, &mut *this.post_delay);
        }
        if !this.pre_delay.is_done() {
            let res = this.pre_delay.poll(cx);
            if res.is_pending() {
                return Poll::Pending;
            }
        }
        if this.res.is_ready() {
            return std::mem::replace(this.res, Poll::Pending);
        }
        let last_io_bytes = THREAD_TOTAL_IO_BYTES.with(|s| {
            let mut stat = s.get();
            if stat.state == LocalIoState::Outdated {
                if let Ok(io_bytes) = get_thread_io_bytes_stats() {
                    stat.io_bytes = io_bytes;
                    stat.state = LocalIoState::Fresh;
                    s.set(stat);
                }
            }
            stat
        });
        let start = Instant::now();
        let res = this.f.poll(cx);
        let dur = start.saturating_elapsed();
        let io_bytes = if last_io_bytes.state == LocalIoState::Outdated {
            0
        } else {
            THREAD_TOTAL_IO_BYTES.with(|s| match get_thread_io_bytes_stats() {
                Ok(io_bytes) => {
                    s.set(CachedIoBytes {
                        io_bytes,
                        state: LocalIoState::Fresh,
                    });
                    let delta = io_bytes - last_io_bytes.io_bytes;
                    delta.read + delta.write
                }
                Err(msg) => {
                    warn!("load thread io bytes failed"; "err" => msg);
                    0
                }
            })
        };
        let mut wait_dur = this.resource_limiter.consume(dur, io_bytes);
        if wait_dur == Duration::ZERO {
            return res;
        }
        if wait_dur > MAX_WAIT_DURATION {
            warn!("limiter future wait too long"; "wait" => ?wait_dur, "io" => io_bytes, "cpu" => ?dur);
            wait_dur = MAX_WAIT_DURATION;
        }
        *this.post_delay = Some(
            GLOBAL_TIMER_HANDLE
                .delay(std::time::Instant::now() + wait_dur)
                .compat(),
        )
        .into();
        if this.post_delay.poll(cx).is_ready() {
            return res;
        }
        *this.res = res;
        Poll::Pending
    }
}

/// `OptionalFuture` is similar to futures::OptionFuture, but provide an extra
/// `is_done` method.
#[pin_project]
struct OptionalFuture<F> {
    #[pin]
    f: Option<F>,
    done: bool,
}

impl<F> OptionalFuture<F> {
    fn new(f: Option<F>) -> Self {
        let done = f.is_none();
        Self { f, done }
    }

    fn is_done(&self) -> bool {
        self.done
    }
}

impl<F> From<Option<F>> for OptionalFuture<F> {
    fn from(f: Option<F>) -> Self {
        Self::new(f)
    }
}

impl<F: Future> Future for OptionalFuture<F> {
    type Output = Option<F::Output>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.project();
        match this.f.as_pin_mut() {
            Some(x) => x.poll(cx).map(|r| {
                *this.done = true;
                Some(r)
            }),
            None => Poll::Ready(None),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::mpsc::{channel, Sender};

    use tikv_util::yatp_pool::{DefaultTicker, FuturePool, YatpPoolBuilder};

    use super::*;

    #[pin_project]
    struct NotifyFuture<F> {
        #[pin]
        f: F,
        sender: Sender<CachedIoBytes>,
    }

    impl<F> NotifyFuture<F> {
        fn new(f: F, sender: Sender<CachedIoBytes>) -> Self {
            Self { f, sender }
        }
    }

    impl<F: Future> Future for NotifyFuture<F> {
        type Output = F::Output;

        fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
            let this = self.project();
            this.f.poll(cx).map(|r| {
                this.sender.send(THREAD_TOTAL_IO_BYTES.get()).unwrap();
                r
            })
        }
    }

    struct TestFuture(Duration);
    impl Future for TestFuture {
        type Output = ();

        fn poll(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Self::Output> {
            std::thread::sleep(self.0);
            Poll::Ready(())
        }
    }

    struct CheckFuture(CachedIoBytes);

    impl Future for CheckFuture {
        type Output = ();

        fn poll(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Self::Output> {
            let current = THREAD_TOTAL_IO_BYTES.get();
            assert_eq!(current.state, self.0.state);
            assert_eq!(current.io_bytes, self.0.io_bytes);
            Poll::Ready(())
        }
    }

    async fn empty() {}

    #[test]
    fn test_limited_future() {
        let pool = YatpPoolBuilder::new(DefaultTicker::default())
            .thread_count(1, 1, 1)
            .name_prefix("test")
            .build_future_pool();

        let resource_limiter = Arc::new(ResourceLimiter::new(f64::INFINITY, f64::INFINITY));

        fn spawn_and_wait<F>(
            pool: &FuturePool,
            f: F,
            limiter: Option<Arc<ResourceLimiter>>,
        ) -> CachedIoBytes
        where
            F: Future + Send + 'static,
            <F as Future>::Output: Send,
        {
            let (sender, receiver) = channel();
            if let Some(limiter) = limiter {
                let fut = TrackIoStateFuture::new(LimitedFuture::new(f, limiter));
                pool.spawn(NotifyFuture::new(fut, sender)).unwrap();
            } else {
                let fut = TrackIoStateFuture::new(f);
                pool.spawn(NotifyFuture::new(fut, sender)).unwrap();
            }
            receiver.recv().unwrap()
        }
        let check_eq = |stats: CachedIoBytes, expected: CachedIoBytes| {
            assert_eq!(stats.state, expected.state);
            assert_eq!(stats.io_bytes, expected.io_bytes);
            let limit_stats = resource_limiter.io_limiter.get_statistics();
            assert_eq!(
                limit_stats.total_consumed,
                expected.io_bytes.read + expected.io_bytes.write
            );
        };

        let stats = spawn_and_wait(&pool, empty(), None);
        check_eq(stats, CachedIoBytes::default());

        let stats = spawn_and_wait(
            &pool,
            TestFuture(Duration::from_millis(1)),
            Some(resource_limiter.clone()),
        );
        check_eq(stats, CachedIoBytes::new(LocalIoState::Fresh, 100, 50));

        let stats = spawn_and_wait(&pool, TestFuture(Duration::from_millis(1)), None);
        check_eq(stats, CachedIoBytes::new(LocalIoState::Outdated, 100, 50));

        let stats = spawn_and_wait(
            &pool,
            TestFuture(Duration::from_millis(1)),
            Some(resource_limiter.clone()),
        );
        check_eq(stats, CachedIoBytes::new(LocalIoState::Fresh, 200, 100));

        fail::cfg("failed_to_get_thread_io_bytes_stats", "1*return").unwrap();
        let stats = spawn_and_wait(
            &pool,
            TestFuture(Duration::from_millis(1)),
            Some(resource_limiter.clone()),
        );
        check_eq(stats, CachedIoBytes::new(LocalIoState::Outdated, 200, 100));
        fail::remove("failed_to_get_thread_io_bytes_stats");
    }
}
