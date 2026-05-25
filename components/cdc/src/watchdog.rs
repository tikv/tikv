// Copyright 2020 TiKV Project Authors. Licensed under Apache-2.0.

use std::{
    future,
    sync::Arc,
    time::{Duration, Instant},
};

use crossbeam::atomic::AtomicCell;
use futures::{compat::Stream01CompatExt, stream::StreamExt};
use tikv_util::{
    error, info, memory::MemoryQuota, timer::GLOBAL_TIMER_HANDLE, warn, worker::Worker,
};
use tokio::sync::oneshot;

use crate::service::ConnId;

// CDC connection monitoring constants in seconds.
const CDC_WATCHDOG_INTERVAL_SECS: u64 = 60;
const CDC_IDLE_DEREGISTER_THRESHOLD_SECS: u64 = 60 * 5; // 5 minutes
const CDC_MEMORY_QUOTA_ABORT_THRESHOLD: f64 = 0.999;

pub(crate) struct CancelSinks {
    recv: oneshot::Sender<()>,
    send: oneshot::Sender<()>,
}

impl CancelSinks {
    fn cancel(self) {
        let _ = self.recv.send(());
        let _ = self.send.send(());
    }
}

pub(crate) fn cancel_channels() -> (CancelSinks, oneshot::Receiver<()>, oneshot::Receiver<()>) {
    let (recv_tx, recv_rx) = oneshot::channel();
    let (send_tx, send_rx) = oneshot::channel();
    (
        CancelSinks {
            recv: recv_tx,
            send: send_tx,
        },
        recv_rx,
        send_rx,
    )
}

pub(crate) async fn wait_cancel(rx: oneshot::Receiver<()>) {
    if rx.await.is_err() {
        future::pending::<()>().await;
    }
}

/// Start a watchdog to monitor CDC connection activity.
///
/// The watchdog periodically checks whether the connection has been idle for
/// too long. When the idle threshold and memory pressure threshold are both
/// reached, it notifies both the receive task and the send task to terminate.
pub(crate) fn start(
    pool: &Worker,
    last_flush_time: Arc<AtomicCell<Instant>>,
    peer: String,
    conn_id: ConnId,
    cancel_sinks: CancelSinks,
    mut forward_exit_rx: oneshot::Receiver<()>,
    memory_quota: Arc<MemoryQuota>,
) {
    let mut cancel_sinks = Some(cancel_sinks);

    let _ = pool.pool().spawn(async move {
        let mut interval = GLOBAL_TIMER_HANDLE
            .interval(
                Instant::now(),
                Duration::from_secs(watchdog_check_interval_secs()),
            )
            .compat();

        loop {
            tokio::select! {
                _ = &mut forward_exit_rx => {
                    info!("cdc connection forward exit signal received, stopping watchdog");
                    break;
                }
                _ = interval.next() => {
                    let elapsed = last_flush_time.load().elapsed();

                    if elapsed > Duration::from_secs(CDC_WATCHDOG_INTERVAL_SECS) {
                        warn!("cdc connection idle too long";
                            "seconds_since_last_flush" => elapsed.as_secs(),
                            "downstream" => peer.as_str(),
                            "conn_id" => ?conn_id);
                    }

                    let idle_threshold = idle_deregister_threshold_secs();
                    let memory_quota_abort_threshold_reached =
                        memory_quota_abort_threshold_reached(&memory_quota);

                    // Check if last flush was more than the deregister threshold.
                    // To prevent the case that the connection idle since there are a lot of
                    // incremental scan tasks queueing so won't send events, also check on the
                    // memory usage. The failpoint can skip the memory check for manual testing.
                    if elapsed > Duration::from_secs(idle_threshold)
                        && memory_quota_abort_threshold_reached
                    {
                        error!("cdc connection idle for too long, aborting connection";
                            "seconds_since_last_flush" => elapsed.as_secs(),
                            "downstream" => peer.as_str(),
                            "conn_id" => ?conn_id);
                        if let Some(cancel_sinks) = cancel_sinks.take() {
                            cancel_sinks.cancel();
                        }
                        break;
                    }
                }
            }
        }
    });
}

fn watchdog_check_interval_secs() -> u64 {
    #[cfg(feature = "failpoints")]
    {
        if use_short_idle_deregister_threshold() {
            return 1;
        }
    }

    CDC_WATCHDOG_INTERVAL_SECS
}

fn idle_deregister_threshold_secs() -> u64 {
    #[cfg(feature = "failpoints")]
    {
        if use_short_idle_deregister_threshold() {
            return 5;
        }
    }

    CDC_IDLE_DEREGISTER_THRESHOLD_SECS
}

fn memory_quota_abort_threshold_reached(memory_quota: &MemoryQuota) -> bool {
    let threshold_reached = memory_quota.used_ratio() >= CDC_MEMORY_QUOTA_ABORT_THRESHOLD;

    #[cfg(feature = "failpoints")]
    {
        let should_ignore_memory_quota = || {
            fail::fail_point!("cdc_watchdog_ignore_memory_quota", |_| true);
            false
        };
        if should_ignore_memory_quota() {
            return true;
        }
    }

    threshold_reached
}

#[cfg(feature = "failpoints")]
fn use_short_idle_deregister_threshold() -> bool {
    let should_adjust = || {
        fail::fail_point!("cdc_idle_deregister_threshold", |_| true);
        false
    };
    should_adjust()
}

#[cfg(test)]
mod tests {
    use std::{sync::Arc, time::Duration};

    use crossbeam::atomic::AtomicCell;
    use tikv_util::{future::block_on_timeout, memory::MemoryQuota, worker::Builder};

    use super::*;

    #[test]
    fn test_connection_watchdog_cancels_send_and_receive() {
        let pool = Arc::new(Builder::new("cdc-watchdog-test").thread_count(1).create());
        let last_flush_time = Arc::new(AtomicCell::new(
            Instant::now() - Duration::from_secs(CDC_IDLE_DEREGISTER_THRESHOLD_SECS + 1),
        ));
        let memory_quota = Arc::new(MemoryQuota::new(1));
        memory_quota.alloc_force(1);

        let (cancel_sinks, recv_cancel_rx, send_cancel_rx) = cancel_channels();
        let (_forward_exit_tx, forward_exit_rx) = oneshot::channel::<()>();

        start(
            &pool,
            last_flush_time,
            "127.0.0.1:0".to_owned(),
            ConnId::new(),
            cancel_sinks,
            forward_exit_rx,
            memory_quota,
        );

        let timeout = Duration::from_secs(CDC_WATCHDOG_INTERVAL_SECS + 3);
        block_on_timeout(wait_cancel(send_cancel_rx), timeout)
            .expect("watchdog should cancel send");
        block_on_timeout(wait_cancel(recv_cancel_rx), timeout)
            .expect("watchdog should cancel receive");
        pool.stop();
    }
}
