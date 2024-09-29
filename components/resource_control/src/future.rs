// Copyright 2022 TiKV Project Authors. Licensed under Apache-2.0.

use std::{
    future::Future,
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
    time::Duration,
};

use file_system::IoBytes;
use futures::compat::{Compat01As03, Future01CompatExt};
use pin_project::pin_project;
use tikv_util::{time::Instant, timer::GLOBAL_TIMER_HANDLE, warn};
use tokio_timer::Delay;

use crate::{
    resource_group::{ResourceConsumeType, ResourceController},
    resource_limiter::{ResourceLimiter, ResourceType},
};

const MAX_WAIT_DURATION: Duration = Duration::from_secs(10);

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

#[cfg(not(test))]
fn get_thread_io_bytes_stats() -> Result<IoBytes, String> {
    file_system::get_thread_io_bytes_total()
}

#[cfg(test)]
fn get_thread_io_bytes_stats() -> Result<IoBytes, String> {
    use std::cell::Cell;

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

// `LimitedFuture` wraps a Future with ResourceLimiter, it will automically
// statistics the cpu time and io bytes consumed by the future, and do async
// waiting according the configuration of the ResourceLimiter.
#[pin_project]
pub struct LimitedFuture<F: Future> {
    #[pin]
    f: F,
    // `pre_delay` and `post_delay` is used to delay this task, at any time, at most one of the two
    // is valid. A future can only be polled once in one round, so we uses two field here to
    // workaround this restriction of the rust compiler.
    #[pin]
    pre_delay: OptionalFuture<Compat01As03<Delay>>,
    #[pin]
    post_delay: OptionalFuture<Compat01As03<Delay>>,
    resource_limiter: Arc<ResourceLimiter>,
    // if the future is first polled, we need to let it consume a 0 value
    // to compensate the debt of previously finished tasks.
    is_first_poll: bool,
}

impl<F: Future> LimitedFuture<F> {
    pub fn new(f: F, resource_limiter: Arc<ResourceLimiter>) -> Self {
        Self {
            f,
            pre_delay: None.into(),
            post_delay: None.into(),
            resource_limiter,
            is_first_poll: true,
        }
    }
}

impl<F: Future> Future for LimitedFuture<F> {
    type Output = F::Output;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let mut this = self.project();
        if *this.is_first_poll {
            debug_assert!(this.pre_delay.finished && this.post_delay.finished);
            *this.is_first_poll = false;
            let wait_dur = this
                .resource_limiter
                .consume(Duration::ZERO, IoBytes::default(), true)
                .min(MAX_WAIT_DURATION);
            if wait_dur > Duration::ZERO {
                *this.pre_delay = Some(
                    GLOBAL_TIMER_HANDLE
                        .delay(std::time::Instant::now() + wait_dur)
                        .compat(),
                )
                .into();
            }
        }
        if !this.post_delay.finished {
            assert!(this.pre_delay.finished);
            std::mem::swap(&mut *this.pre_delay, &mut *this.post_delay);
        }
        if !this.pre_delay.finished {
            let res = this.pre_delay.poll(cx);
            if res.is_pending() {
                return Poll::Pending;
            }
        }
        // get io stats is very expensive, so we only do so if only io control is
        // enabled.
        let mut last_io_bytes = None;
        if this
            .resource_limiter
            .get_limiter(ResourceType::Io)
            .get_rate_limit()
            .is_finite()
        {
            match get_thread_io_bytes_stats() {
                Ok(b) => {
                    last_io_bytes = Some(b);
                }
                Err(e) => {
                    warn!("load thread io bytes failed"; "err" => e);
                }
            }
        }
        let start = Instant::now();
        let res = this.f.poll(cx);
        let dur = start.saturating_elapsed();
        let io_bytes = if let Some(last_io_bytes) = last_io_bytes {
            match get_thread_io_bytes_stats() {
                Ok(io_bytes) => io_bytes - last_io_bytes,
                Err(e) => {
                    warn!("load thread io bytes failed"; "err" => e);
                    IoBytes::default()
                }
            }
        } else {
            IoBytes::default()
        };
        let mut wait_dur = this
            .resource_limiter
            .consume(dur, io_bytes, res.is_pending());
        if wait_dur == Duration::ZERO || res.is_ready() {
            return res;
        }
        if wait_dur > MAX_WAIT_DURATION {
            warn!("limiter future wait too long"; "wait" => ?wait_dur, "io_read" => io_bytes.read, "io_write" => io_bytes.write, "cpu" => ?dur);
            wait_dur = MAX_WAIT_DURATION;
        }
        *this.post_delay = Some(
            GLOBAL_TIMER_HANDLE
                .delay(std::time::Instant::now() + wait_dur)
                .compat(),
        )
        .into();
        _ = this.post_delay.poll(cx);
        Poll::Pending
    }
}

/// `OptionalFuture` is similar to futures::OptionFuture, but provide an extra
/// `finished` flag to determine if the future requires poll.
#[pin_project]
struct OptionalFuture<F> {
    #[pin]
    f: Option<F>,
    finished: bool,
}

impl<F> OptionalFuture<F> {
    fn new(f: Option<F>) -> Self {
        let finished = f.is_none();
        Self { f, finished }
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
                *this.finished = true;
                Some(r)
            }),
            None => Poll::Ready(None),
        }
    }
}

pub async fn with_resource_limiter<F: Future>(
    f: F,
    limiter: Option<Arc<ResourceLimiter>>,
) -> F::Output {
    if let Some(limiter) = limiter {
        LimitedFuture::new(f, limiter).await
    } else {
        f.await
    }
}

#[cfg(test)]
mod tests {
    use std::sync::mpsc::{channel, Sender};

    use tikv_util::yatp_pool::{DefaultTicker, FuturePool, YatpPoolBuilder};

    use super::*;
    use crate::resource_limiter::{GroupStatistics, ResourceType::Io};

    #[pin_project]
    struct NotifyFuture<F> {
        #[pin]
        f: F,
        sender: Sender<()>,
    }

    impl<F> NotifyFuture<F> {
        fn new(f: F, sender: Sender<()>) -> Self {
            Self { f, sender }
        }
    }

    impl<F: Future> Future for NotifyFuture<F> {
        type Output = F::Output;

        fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
            let this = self.project();
            this.f.poll(cx).map(|r| {
                this.sender.send(()).unwrap();
                r
            })
        }
    }

    #[allow(clippy::unused_async)]
    async fn empty() {}

    #[test]
    fn test_limited_future() {
        let pool = YatpPoolBuilder::new(DefaultTicker::default())
            .thread_count(1, 1, 1)
            .name_prefix("test")
            .build_future_pool();

        let resource_limiter = Arc::new(ResourceLimiter::new(
            "".into(),
            f64::INFINITY,
            1000.0,
            0,
            true,
        ));

        fn spawn_and_wait<F>(pool: &FuturePool, f: F, limiter: Arc<ResourceLimiter>)
        where
            F: Future + Send + 'static,
            <F as Future>::Output: Send,
        {
            let (sender, receiver) = channel::<()>();
            let fut = NotifyFuture::new(LimitedFuture::new(f, limiter), sender);
            pool.spawn(fut).unwrap();
            receiver.recv().unwrap();
        }

        let mut i = 0;
        let mut stats: GroupStatistics;
        // consume the remain free limit quota.
        loop {
            i += 1;
            spawn_and_wait(&pool, empty(), resource_limiter.clone());
            stats = resource_limiter.get_limit_statistics(Io);
            assert_eq!(stats.total_consumed, i * 150);
            if stats.total_wait_dur_us > 0 {
                break;
            }
        }

        let start = Instant::now();
        spawn_and_wait(&pool, empty(), resource_limiter.clone());
        let new_stats = resource_limiter.get_limit_statistics(Io);
        let delta = new_stats - stats;
        let dur = start.saturating_elapsed();
        assert_eq!(delta.total_consumed, 150);
        assert!(delta.total_wait_dur_us >= 140_000 && delta.total_wait_dur_us <= 160_000);
        assert!(dur >= Duration::from_millis(140) && dur <= Duration::from_millis(160), "dur: {:?}", dur);

        // fetch io bytes failed, consumed value is 0.
        #[cfg(feature = "failpoints")]
        {
            fail::cfg("failed_to_get_thread_io_bytes_stats", "1*return").unwrap();
            spawn_and_wait(&pool, empty(), resource_limiter.clone());
            assert_eq!(
                resource_limiter.get_limit_statistics(Io).total_consumed,
                new_stats.total_consumed
            );
            fail::remove("failed_to_get_thread_io_bytes_stats");
        }
    }
}
