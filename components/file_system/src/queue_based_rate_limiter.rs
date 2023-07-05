// Copyright 2023 TiKV Project Authors. Licensed under Apache-2.0.

use std::{
    collections::VecDeque,
    future::Future,
    intrinsics::unlikely,
    pin::Pin,
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc, Mutex,
    },
    task::{Context, Poll, Waker},
    time::Duration,
};

use crossbeam_utils::CachePadded;
use rand::{distributions::Uniform, prelude::Distribution, rngs::ThreadRng};
use strum::EnumCount;
use tikv_util::{config::ReadableSize, sys::thread::StdThreadBuildWrapper, thd_name};

use crate::{IoBudgetAdjustor, IoPriority};

const DEFAULT_REFILL_PERIOD: Duration = Duration::from_millis(50);
const INITIAL_THRESHOLD: u64 =
    (ReadableSize::mb(20).0 as f64 * DEFAULT_REFILL_PERIOD.as_secs_f64()) as u64;

#[derive(Default)]
struct MinuteWindow<const N: u64> {
    sample_count: usize,
    sum: u64,
}

impl<const N: u64> MinuteWindow<N> {
    const ELAPSE: u64 = N * 60 * 1000 / DEFAULT_REFILL_PERIOD.as_millis() as u64;

    /// Should be called before pushing new value into `values`.
    #[inline]
    fn add(&mut self, elapsed: u64, val: u64, values: &VecDeque<(u64, u64)>) {
        self.sum += val;
        if !values.is_empty() {
            let (off_elapsed, off_val) = values[values.len() - self.sample_count];
            if elapsed - off_elapsed > Self::ELAPSE {
                self.sum -= off_val;
                return;
            }
        }
        self.sample_count += 1;
    }

    #[inline]
    fn avg(&self) -> u64 {
        self.sum / self.sample_count as u64
    }
}

#[derive(PartialEq)]
enum Trending {
    Accelerating,
    Decelerating,
    Stable,
}

#[derive(Default)]
struct TimeWindow {
    one: MinuteWindow<1>,
    two: MinuteWindow<2>,
    five: MinuteWindow<5>,
    // interval, score
    values: VecDeque<(u64, u64)>,
}

impl TimeWindow {
    #[inline]
    fn add(&mut self, elapsed: u64, val: u64) {
        self.one.add(elapsed, val, &self.values);
        self.two.add(elapsed, val, &self.values);
        self.five.add(elapsed, val, &self.values);
        if self.values.len() == self.five.sample_count {
            self.values.pop_front();
        }
        self.values.push_back((elapsed, val));
    }

    #[inline]
    fn trending(&self) -> Trending {
        let avg_one = self.one.avg();
        let avg_two = self.two.avg();
        let avg_five = self.five.avg();
        if avg_one > avg_two && avg_two > avg_five + 1 {
            Trending::Accelerating
        } else if avg_one < avg_two && avg_two + 1 < avg_five {
            Trending::Decelerating
        } else {
            Trending::Stable
        }
    }
}

struct AllocatedRequest {
    amount: u64,
    seq: u64,
}

struct Request {
    waker: Waker,
    req: AllocatedRequest,
}

struct QuotaBase {
    window: TimeWindow,
    last_backlog: u64,
    /// Shows the backlog degree. Used to adjust soft limit.
    backlog: Option<Arc<dyn IoBudgetAdjustor>>,
    allocated_lots: [VecDeque<AllocatedRequest>; IoPriority::COUNT],
    parking_lots: [VecDeque<Request>; IoPriority::COUNT],
    seq: u64,
    allocated_step: u64,
}

impl Default for QuotaBase {
    fn default() -> Self {
        Self {
            window: Default::default(),
            last_backlog: 100,
            backlog: None,
            allocated_lots: Default::default(),
            parking_lots: Default::default(),
            seq: 0,
            allocated_step: INITIAL_THRESHOLD,
        }
    }
}

impl QuotaBase {
    fn tun(&mut self, elapsed: u64, left: u64, hard_limit: u64) {
        // Reach 95%.
        if let Some(backlog) = self.backlog.as_ref().and_then(|j| j.pressure()) {
            self.last_backlog = backlog;
            self.window.add(elapsed, backlog);
        }
        let trending = self.window.trending();
        let almost_used_up = left * 100 / self.allocated_step <= 5;
        let factor = if trending == Trending::Accelerating && almost_used_up {
            if self.last_backlog < 20 {
                1.01
            } else if self.last_backlog < 40 {
                1.1
            } else if self.last_backlog < 60 {
                1.2
            } else if self.last_backlog < 80 {
                1.3
            } else {
                1.4
            }
        } else if trending == Trending::Decelerating {
            if self.last_backlog < 20 {
                0.8
            } else if self.last_backlog < 40 {
                0.9
            } else if self.last_backlog < 60 {
                0.99
            } else if self.last_backlog < 80 {
                0.995
            } else {
                0.999
            }
        } else {
            1.0
        };
        self.allocated_step =
            ((self.allocated_step as f64 * factor) as u64).clamp(INITIAL_THRESHOLD, hard_limit);
    }
}

enum TokenStatus {
    Waiting(u64),
    Requested(u64),
}

struct RequestToken<'a> {
    priority: IoPriority,
    status: TokenStatus,
    limiter: &'a RateLimiter,
}

impl<'a> Future for RequestToken<'a> {
    type Output = u64;

    #[inline]
    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<u64> {
        let token = self.get_mut();
        let mut guard = token.limiter.core.base.lock().unwrap();
        let off = token.priority as usize;
        match token.status {
            TokenStatus::Waiting(amount) => {
                // Some quota may be requested by others when waiting for lock.
                if let Some(amount) = token.limiter.request_no_wait_throttle(amount) {
                    return Poll::Ready(amount);
                }
                guard.seq += 1;
                let seq = guard.seq;
                guard.parking_lots[off].push_back(Request {
                    req: AllocatedRequest { amount, seq },
                    waker: cx.waker().clone(),
                });

                token.status = TokenStatus::Requested(seq);
            }
            TokenStatus::Requested(seq) => {
                let idx = guard.allocated_lots[off]
                    .iter_mut()
                    .position(|req| req.seq == seq);
                if let Some(idx) = idx {
                    let amount = guard.allocated_lots[off][idx].amount;
                    guard.allocated_lots[off].swap_remove_front(idx);
                    return Poll::Ready(amount);
                }
                let req = guard.parking_lots[off]
                    .iter_mut()
                    .find(|req| req.req.seq == seq)
                    .unwrap();
                if !req.waker.will_wake(cx.waker()) {
                    req.waker = cx.waker().clone();
                }
            }
        }
        Poll::Pending
    }
}

struct Quota {
    available_bytes: CachePadded<AtomicU64>,
    /// All bytes through this quota per period will unlikely exceed this limit.
    ///
    /// 0 means no limit.
    hard_limit: CachePadded<AtomicU64>,
}

#[inline]
fn fair_choose(
    parking_lots: &[VecDeque<Request>],
    rng: &mut ThreadRng,
    dist: &Uniform<u8>,
) -> Option<IoPriority> {
    let mut chosen = None;
    for pri in [IoPriority::High, IoPriority::Medium, IoPriority::Low] {
        let lot = &parking_lots[pri as usize];
        if !lot.is_empty() {
            chosen = Some(pri);
            if dist.sample(rng) > 10 {
                break;
            }
        } else if chosen.is_some() && dist.sample(rng) > 10 {
            break;
        }
    }
    chosen
}

async fn refill(core: Arc<RateLimiterCore>, mut last_hard_limit: u64, interval: Duration) {
    let mut timer = tokio::time::interval(interval);
    let dist = Uniform::from(1..=100);
    let mut elapsed = 0;
    loop {
        timer.tick().await;
        elapsed += 1;

        let hard_limit = core.quota.hard_limit.load(Ordering::Relaxed);
        if hard_limit == 0 {
            if unlikely(last_hard_limit != 0) {
                // Change to no limit, we need to wake up all pending requests.
                last_hard_limit = 0;
                let mut guard = core.base.lock().unwrap();
                let base = &mut *guard;
                for (pending, allocated) in base
                    .parking_lots
                    .iter_mut()
                    .zip(base.allocated_lots.iter_mut())
                {
                    while let Some(req) = pending.pop_front() {
                        allocated.push_back(req.req);
                        req.waker.wake();
                    }
                }
                guard.allocated_step = INITIAL_THRESHOLD;
            }
            continue;
        }
        last_hard_limit = hard_limit;

        let mut available = core.quota.available_bytes.load(Ordering::Relaxed);
        let mut rng = rand::thread_rng();
        let mut guard = core.base.lock().unwrap();
        guard.tun(elapsed, available, hard_limit);
        let mut next_allocated = guard.allocated_step;
        while available != 0 {
            match core.quota.available_bytes.compare_exchange_weak(
                available,
                next_allocated,
                Ordering::Relaxed,
                Ordering::Relaxed,
            ) {
                Ok(_) => break,
                Err(val) => available = val,
            }
        }
        if available == 0 {
            while next_allocated > 0 {
                let priority = match fair_choose(&guard.parking_lots, &mut rng, &dist) {
                    Some(pri) => pri,
                    None => break,
                };
                let req = guard.parking_lots[priority as usize].pop_front().unwrap();
                req.waker.wake();
                let mut req = req.req;
                req.amount = req.amount.min(next_allocated);
                next_allocated -= req.amount;
                guard.allocated_lots[priority as usize].push_back(req);
            }
            if next_allocated > 0 {
                core.quota
                    .available_bytes
                    .store(next_allocated, Ordering::Relaxed);
            }
        }
    }
}

struct RateLimiterCore {
    quota: Quota,
    strict: bool,
    base: Mutex<QuotaBase>,
}

pub struct RateLimiter {
    core: Arc<RateLimiterCore>,
}

impl RateLimiter {
    pub fn new(strict: bool) -> Self {
        let core = Arc::new(RateLimiterCore {
            strict,
            quota: Quota {
                available_bytes: Default::default(),
                hard_limit: Default::default(),
            },
            base: Mutex::default(),
        });
        let core0 = core.clone();
        std::thread::Builder::new()
            .name(thd_name!("rl-refill"))
            .spawn_wrapper(move || {
                futures::executor::block_on(refill(core0, 0, DEFAULT_REFILL_PERIOD));
            })
            .unwrap();
        RateLimiter { core }
    }

    /// Adjust the hard rate limit. The new value will only take effect in next
    /// backfill.
    pub fn set_bytes_per_sec(&self, bytes_per_sec: u64) {
        let new_value = (bytes_per_sec as f64 * DEFAULT_REFILL_PERIOD.as_secs_f64()) as u64;
        self.core
            .quota
            .hard_limit
            .store(new_value, Ordering::Relaxed);
    }

    /// Set a new IoBudgetAdjustor. The new value will only take effect in next
    /// backfill.
    pub fn set_io_budget_adjustor(&self, adjustor: Option<Arc<dyn IoBudgetAdjustor>>) {
        let mut quota = self.core.base.lock().unwrap();
        quota.backlog = adjustor;
    }

    /// Check if we can request `amount` bytes with `priority` without wait.
    #[inline]
    pub fn request_no_wait(&self, priority: IoPriority, amount: u64) -> Option<u64> {
        if priority == IoPriority::High && !self.core.strict {
            return Some(amount);
        }
        if self.core.quota.hard_limit.load(Ordering::Relaxed) == 0 {
            return Some(amount);
        }
        self.request_no_wait_throttle(amount)
    }

    #[inline]
    fn request_no_wait_throttle(&self, amount: u64) -> Option<u64> {
        let available_bytes = &self.core.quota.available_bytes;
        let mut value = available_bytes.load(Ordering::Relaxed);
        loop {
            if unlikely(value == 0) {
                break;
            }
            let new_value = value.saturating_sub(amount);
            match available_bytes.compare_exchange_weak(
                value,
                new_value,
                Ordering::Relaxed,
                Ordering::Relaxed,
            ) {
                Ok(_) => return Some(value - new_value),
                Err(v) => value = v,
            }
        }
        None
    }

    #[inline]
    pub async fn request(&self, priority: IoPriority, amount: u64) -> u64 {
        if let Some(got) = self.request_no_wait(priority, amount) {
            return got;
        }
        RequestToken {
            priority,
            status: TokenStatus::Waiting(amount),
            limiter: self,
        }
        .await
    }

    #[cfg(test)]
    pub fn reset(&self) {}
}
