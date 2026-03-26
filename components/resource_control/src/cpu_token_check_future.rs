// Copyright 2026 TiKV Project Authors. Licensed under Apache-2.0.

use std::{
    future::Future,
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
    time::{Duration, Instant},
};

use cpu_time::ThreadTime;
use futures::future::BoxFuture;
use pin_project::pin_project;

use crate::{CpuTokenHandle, ThrottleError};

#[derive(Debug)]
pub enum CpuTokenError {
    Timeout,
}

impl std::fmt::Display for CpuTokenError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            CpuTokenError::Timeout => write!(f, "request timeout waiting for runtime cpu tokens"),
        }
    }
}

impl std::error::Error for CpuTokenError {}

#[derive(Clone, Copy)]
struct RuntimeTokenConfig {
    check_interval: Duration,
    additional_allocation_threshold: f64,
    per_allocation_us: u64,
}

enum CheckState {
    Executing {
        last_check_time: Instant,
    },
    AllocatingTokens {
        allocation_future: BoxFuture<'static, Result<u64, ThrottleError>>,
    },
    Completed,
    TimedOut,
}

#[pin_project]
pub struct CpuTokenCheckFuture<F> {
    #[pin]
    delegate: F,
    token_handle: Arc<CpuTokenHandle>,
    state: CheckState,
    runtime_config: Option<RuntimeTokenConfig>,
    total_duration: Duration,
    timer: Option<ThreadTime>,
}

impl<F> CpuTokenCheckFuture<F> {
    pub fn new(inner: F, token_handle: Arc<CpuTokenHandle>) -> Self {
        Self {
            delegate: inner,
            runtime_config: token_handle.get_runtime_config().map(
                |(check_interval, additional_allocation_threshold, per_allocation_us)| {
                    RuntimeTokenConfig {
                        check_interval,
                        additional_allocation_threshold,
                        per_allocation_us,
                    }
                },
            ),
            token_handle,
            state: CheckState::Executing {
                last_check_time: Instant::now(),
            },
            total_duration: Duration::ZERO,
            timer: None,
        }
    }
}

impl<F: Future> Future for CpuTokenCheckFuture<F> {
    type Output = Result<F::Output, CpuTokenError>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let mut this = self.project();

        loop {
            match this.state {
                CheckState::Executing { last_check_time } => {
                    *this.timer = Some(ThreadTime::now());
                    let result = this.delegate.as_mut().poll(cx);
                    if let Some(timer) = this.timer.take() {
                        *this.total_duration += timer.elapsed();
                    }

                    match result {
                        Poll::Ready(result) => {
                            this.token_handle
                                .record_actual_usage(this.total_duration.as_micros() as u64);
                            *this.state = CheckState::Completed;
                            return Poll::Ready(Ok(result));
                        }
                        Poll::Pending => {
                            let Some(runtime_config) = this.runtime_config.as_ref() else {
                                return Poll::Pending;
                            };
                            let now = Instant::now();
                            if now.duration_since(*last_check_time) < runtime_config.check_interval
                            {
                                return Poll::Pending;
                            }

                            let current_cpu_us = this.total_duration.as_micros() as u64;
                            let threshold_us = (this.token_handle.allocated() as f64
                                * runtime_config.additional_allocation_threshold)
                                as u64;
                            if current_cpu_us <= threshold_us {
                                *last_check_time = now;
                                return Poll::Pending;
                            }

                            let token_handle = this.token_handle.clone();
                            let per_allocation_us = runtime_config.per_allocation_us;
                            this.token_handle.log_runtime_check_trigger(
                                current_cpu_us,
                                threshold_us,
                                per_allocation_us,
                            );
                            *this.state = CheckState::AllocatingTokens {
                                allocation_future: Box::pin(async move {
                                    token_handle
                                        .allocate_more_with_wait(per_allocation_us)
                                        .await
                                }),
                            };
                            continue;
                        }
                    }
                }
                CheckState::AllocatingTokens { allocation_future } => {
                    match allocation_future.as_mut().poll(cx) {
                        Poll::Ready(Ok(_)) => {
                            *this.state = CheckState::Executing {
                                last_check_time: Instant::now(),
                            };
                            cx.waker().wake_by_ref();
                            continue;
                        }
                        Poll::Ready(Err(_)) => {
                            this.token_handle
                                .record_actual_usage(this.total_duration.as_micros() as u64);
                            *this.state = CheckState::TimedOut;
                            return Poll::Ready(Err(CpuTokenError::Timeout));
                        }
                        Poll::Pending => return Poll::Pending,
                    }
                }
                CheckState::Completed => unreachable!(),
                CheckState::TimedOut => return Poll::Ready(Err(CpuTokenError::Timeout)),
            }
        }
    }
}

// SAFETY: `ThreadTime` is not `Send`, so this future must never carry a live
// `ThreadTime` across thread boundaries. We uphold that invariant by creating
// `ThreadTime::now()` only inside `CheckState::Executing` and always taking it
// back out before `poll` returns `Pending` or `Ready`. That means moving the
// future between polls never moves a `ThreadTime` to another thread.
//
// If a wake causes the next poll to run on a different worker thread, that poll
// creates a fresh `ThreadTime` for the current thread, which is the intended
// behavior because we want to measure CPU time consumed by whichever thread is
// executing the future at that moment.
unsafe impl<F: Future + Send> Send for CpuTokenCheckFuture<F> {}
