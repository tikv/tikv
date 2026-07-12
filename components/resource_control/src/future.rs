// Copyright 2022 TiKV Project Authors. Licensed under Apache-2.0.

use std::{
    future::Future,
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
    time::Duration,
};

use file_system::{IoBytes, IoBytesTracker};
use futures::compat::{Compat01As03, Future01CompatExt};
use pin_project::pin_project;
use tikv_util::{time::Instant, timer::GLOBAL_TIMER_HANDLE, warn};
use tokio_timer::Delay;

use crate::{
    resource_group::{ResourceConsumeType, ResourceController, ResourceGroupManager},
    resource_limiter::{ResourceLimiter, ResourceType},
};

#[inline]
fn cpu_timer() -> impl FnOnce() -> Duration {
    let start = std::time::Instant::now();
    move || start.elapsed()
}

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

// `LimitedFuture` wraps a Future with ResourceLimiter, it will automatically
// track the cpu time and io bytes consumed by the future.
//
// Two modes controlled by `measure_only`:
//
// `measure_only = false` (default, used by background jobs like backup/import):
//   Measures CPU+IO per poll and sleeps proportionally to token-bucket debt.
//   Pre-delay pays off existing debt before starting; post-delay sleeps after
//   each pending poll. This throttles the task inside the thread pool.
//
// `measure_only = true` (used by read/write pools):
//   Measures CPU+IO per poll and builds token-bucket debt, but NEVER sleeps.
//   Throttling is handled pre-pool by `admission_decision` which reads the
//   accumulated debt before submitting the next request.
#[pin_project]
pub struct LimitedFuture<F: Future> {
    #[pin]
    f: F,
    // `pre_delay` and `post_delay` is used to delay this task, at any time, at most one of the two
    // is valid. A future can only be polled once in one round, so we use two fields here to
    // workaround this restriction of the rust compiler.
    #[pin]
    pre_delay: OptionalFuture<Compat01As03<Delay>>,
    #[pin]
    post_delay: OptionalFuture<Compat01As03<Delay>>,
    resource_limiter: Arc<ResourceLimiter>,
    skip_compaction_pressure: bool,
    // if the future is first polled, we need to let it consume a 0 value
    // to compensate the debt of previously finished tasks.
    is_first_poll: bool,
    // When true: measure CPU/IO and build token-bucket debt, but never sleep.
    // When false: existing behavior — sleep inside the pool to throttle.
    measure_only: bool,
    // For measure-only mode: feed RuTracker + token-bucket via record_ru_consumption.
    resource_mgr: Option<Arc<ResourceGroupManager>>,
    // Known write bytes for this request (from cmd.write_bytes()), used as the
    // write component of io_bytes passed to consume() instead of /proc iostat.
    // Avoids syscall overhead and thread-level IO noise from compaction on the
    // same thread. Zero means fall back to IoBytesTracker (e.g. reads, background).
    write_bytes: u64,
}

impl<F: Future> LimitedFuture<F> {
    pub fn new(
        f: F,
        resource_limiter: Arc<ResourceLimiter>,
        skip_compaction_pressure: bool,
        measure_only: bool,
        resource_mgr: Option<Arc<ResourceGroupManager>>,
        write_bytes: u64,
    ) -> Self {
        Self {
            f,
            pre_delay: None.into(),
            post_delay: None.into(),
            resource_limiter,
            skip_compaction_pressure,
            is_first_poll: true,
            measure_only,
            resource_mgr,
            write_bytes,
        }
    }
}

impl<F: Future> Future for LimitedFuture<F> {
    type Output = F::Output;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let mut this = self.project();

        // Pre-delay: pay off existing token-bucket debt before starting work.
        // Skipped in measure-only mode — foreground pools never sleep in-pool.
        if *this.is_first_poll && !*this.measure_only {
            debug_assert!(this.pre_delay.finished && this.post_delay.finished);
            *this.is_first_poll = false;
            let wait_dur = this
                .resource_limiter
                .consume(
                    Duration::ZERO,
                    IoBytes::default(),
                    true,
                    *this.skip_compaction_pressure,
                )
                .min(MAX_WAIT_DURATION);
            if wait_dur > Duration::ZERO {
                *this.pre_delay = Some(
                    GLOBAL_TIMER_HANDLE
                        .delay(std::time::Instant::now() + wait_dur)
                        .compat(),
                )
                .into();
            }
        } else if *this.is_first_poll {
            *this.is_first_poll = false;
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
        // get io stats is very expensive, so we only do so if io control is enabled.
        let mut io_tracker = if this
            .resource_limiter
            .get_limiter(ResourceType::Io)
            .get_rate_limit()
            .is_finite()
        {
            Some(IoBytesTracker::new())
        } else {
            None
        };
        let elapsed = cpu_timer();
        let res = this.f.poll(cx);
        let dur = elapsed();
        let mut io_bytes = io_tracker
            .as_mut()
            .and_then(|tracker| tracker.update())
            .unwrap_or_else(IoBytes::default);
        // Only count write_bytes when the future completes to avoid
        // double-counting across multiple pending polls.
        if *this.write_bytes > 0 && res.is_ready() {
            io_bytes.write = *this.write_bytes;
        }
        let mut wait_dur = this.resource_limiter.consume(
            dur,
            io_bytes,
            res.is_pending(),
            *this.skip_compaction_pressure,
        );
        // Feed RuTracker so admission_decision has baseline data.
        // Only for foreground limiters — background jobs shouldn't shift the baseline.
        if !this.resource_limiter.is_background()
            && let Some(mgr) = &this.resource_mgr
        {
            mgr.record_ru_consumption(this.resource_limiter.name(), dur.as_micros() as u64);
        }
        if wait_dur == Duration::ZERO || res.is_ready() || *this.measure_only {
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

/// `OptionalFuture` is similar to futures::OptionFuture, but provides an extra
/// `finished` flag to determine if the future requires polling.
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
    skip_compaction_pressure: bool,
    measure_only: bool,
    resource_mgr: Option<Arc<ResourceGroupManager>>,
    write_bytes: u64,
) -> F::Output {
    if let Some(limiter) = limiter {
        LimitedFuture::new(
            f,
            limiter,
            skip_compaction_pressure,
            measure_only,
            resource_mgr,
            write_bytes,
        )
        .await
    } else {
        f.await
    }
}

#[cfg(all(test, feature = "failpoints"))]
mod tests {
    use std::sync::mpsc::{Sender, channel};

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
            let fut = NotifyFuture::new(
                LimitedFuture::new(f, limiter, false, false, None, 0),
                sender,
            );
            pool.spawn(fut).unwrap();
            receiver.recv().unwrap();
        }

        fail::cfg("delta_read_io_bytes", "return(100)").unwrap();
        fail::cfg("delta_write_io_bytes", "return(50)").unwrap();

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
        assert!(
            dur >= Duration::from_millis(140) && dur <= Duration::from_millis(160),
            "dur: {:?}",
            dur
        );

        // fetch io bytes failed, consumed value is 0.
        fail::cfg("failed_to_get_thread_io_bytes_stats", "1*return").unwrap();
        spawn_and_wait(&pool, empty(), resource_limiter.clone());
        assert_eq!(
            resource_limiter.get_limit_statistics(Io).total_consumed,
            new_stats.total_consumed
        );
        fail::remove("failed_to_get_thread_io_bytes_stats");
    }
}
