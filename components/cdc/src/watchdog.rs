// Copyright 2026 TiKV Project Authors. Licensed under Apache-2.0.

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

use crate::types::ConnId;

// CDC connection monitoring constants in seconds.
const CDC_WATCHDOG_INTERVAL_SECS: u64 = 60;
const CDC_IDLE_DEREGISTER_THRESHOLD_SECS: u64 = 60 * 5; // 5 minutes
const CDC_MEMORY_QUOTA_ABORT_THRESHOLD: f64 = 0.999;

struct WatchdogConfig {
    check_interval: Duration,
    idle_deregister_threshold: Duration,
    memory_quota_abort_threshold: f64,
}

impl WatchdogConfig {
    fn new() -> WatchdogConfig {
        WatchdogConfig::default()
    }

    fn idle_deregister_threshold(&self) -> Duration {
        fail::fail_point!("cdc_idle_deregister_threshold", |_| Duration::from_secs(5));
        self.idle_deregister_threshold
    }

    fn memory_quota_abort_threshold(&self) -> f64 {
        fail::fail_point!("cdc_idle_deregister_threshold", |_| 0.0);
        self.memory_quota_abort_threshold
    }
}

impl Default for WatchdogConfig {
    fn default() -> WatchdogConfig {
        WatchdogConfig {
            check_interval: Duration::from_secs(CDC_WATCHDOG_INTERVAL_SECS),
            idle_deregister_threshold: Duration::from_secs(CDC_IDLE_DEREGISTER_THRESHOLD_SECS),
            memory_quota_abort_threshold: CDC_MEMORY_QUOTA_ABORT_THRESHOLD,
        }
    }
}

#[derive(Clone)]
pub(crate) struct ActivityHandle {
    last_flush: Arc<AtomicCell<Instant>>,
}

impl ActivityHandle {
    fn new() -> ActivityHandle {
        ActivityHandle {
            last_flush: Arc::new(AtomicCell::new(Instant::now())),
        }
    }

    #[cfg(test)]
    fn with_last_flush(last_flush: Instant) -> ActivityHandle {
        ActivityHandle {
            last_flush: Arc::new(AtomicCell::new(last_flush)),
        }
    }

    #[inline]
    pub(crate) fn record_flush(&self) {
        self.last_flush.store(Instant::now());
    }

    #[inline]
    fn idle_elapsed(&self) -> Duration {
        self.last_flush.load().elapsed()
    }
}

pub(crate) struct WatchdogHandle {
    pub(crate) activity: ActivityHandle,
    pub(crate) recv_abort: oneshot::Receiver<()>,
    pub(crate) send_abort: oneshot::Receiver<()>,
    pub(crate) forward_exit: ForwardExitGuard,
}

/// Keeps the watchdog alive while the send task is forwarding events.
///
/// Dropping this guard drops the underlying oneshot sender, which wakes the
/// watchdog and lets it stop polling the connection.
pub(crate) struct ForwardExitGuard {
    _tx: oneshot::Sender<()>,
}

impl ForwardExitGuard {
    fn new(tx: oneshot::Sender<()>) -> ForwardExitGuard {
        ForwardExitGuard { _tx: tx }
    }
}

struct AbortHandle {
    recv: Option<oneshot::Sender<()>>,
    send: Option<oneshot::Sender<()>>,
}

impl AbortHandle {
    fn new() -> (AbortHandle, oneshot::Receiver<()>, oneshot::Receiver<()>) {
        let (recv_tx, recv_rx) = oneshot::channel();
        let (send_tx, send_rx) = oneshot::channel();
        (
            AbortHandle {
                recv: Some(recv_tx),
                send: Some(send_tx),
            },
            recv_rx,
            send_rx,
        )
    }

    fn abort(&mut self) {
        if let Some(send) = self.send.take() {
            let _ = send.send(());
        }
        if let Some(recv) = self.recv.take() {
            let _ = recv.send(());
        }
    }
}

/// Waits for an explicit watchdog abort signal.
///
/// This only returns when the sender side sends `Ok(())`. If the sender is
/// dropped without aborting, this future stays pending. That is intentional:
/// when watchdog exits normally, the receive/send tasks should complete via
/// their non-abort `select!` branches instead of being cancelled here.
pub(crate) async fn wait_for_abort(rx: oneshot::Receiver<()>) {
    if rx.await.is_err() {
        future::pending::<()>().await;
    }
}

pub(crate) struct Watchdog {
    config: WatchdogConfig,
    activity: ActivityHandle,
    peer: String,
    conn_id: ConnId,
    abort: AbortHandle,
    forward_exit_rx: oneshot::Receiver<()>,
    memory_quota: Arc<MemoryQuota>,
}

impl Watchdog {
    pub(crate) fn spawn(
        pool: &Worker,
        peer: String,
        conn_id: ConnId,
        memory_quota: Arc<MemoryQuota>,
    ) -> WatchdogHandle {
        Self::spawn_with_activity(
            pool,
            peer,
            conn_id,
            memory_quota,
            ActivityHandle::new(),
            WatchdogConfig::new(),
        )
    }

    fn spawn_with_activity(
        pool: &Worker,
        peer: String,
        conn_id: ConnId,
        memory_quota: Arc<MemoryQuota>,
        activity: ActivityHandle,
        config: WatchdogConfig,
    ) -> WatchdogHandle {
        let (abort, recv_abort, send_abort) = AbortHandle::new();
        let (forward_exit_tx, forward_exit_rx) = oneshot::channel();
        let watchdog = Watchdog {
            config,
            activity: activity.clone(),
            peer,
            conn_id,
            abort,
            forward_exit_rx,
            memory_quota,
        };

        if let Err(e) = pool.pool().spawn(async move { watchdog.run().await }) {
            error!("cdc watchdog failed to spawn"; "error" => ?e, "conn_id" => ?conn_id);
        }

        WatchdogHandle {
            activity,
            recv_abort,
            send_abort,
            forward_exit: ForwardExitGuard::new(forward_exit_tx),
        }
    }

    async fn run(mut self) {
        let mut interval = GLOBAL_TIMER_HANDLE
            .interval(Instant::now(), self.config.check_interval)
            .compat();

        loop {
            tokio::select! {
                _ = &mut self.forward_exit_rx => {
                    info!("cdc connection forward exit signal received, stopping watchdog";
                        "downstream" => self.peer.as_str(),
                        "conn_id" => ?self.conn_id);
                    break;
                }
                _ = interval.next() => {
                    if self.check_and_maybe_abort() {
                        break;
                    }
                }
            }
        }
    }

    fn check_and_maybe_abort(&mut self) -> bool {
        let elapsed = self.activity.idle_elapsed();

        if elapsed > self.config.check_interval {
            warn!("cdc connection idle too long";
                "seconds_since_last_flush" => elapsed.as_secs(),
                "downstream" => self.peer.as_str(),
                "conn_id" => ?self.conn_id);
        }

        let memory_quota_abort_threshold_reached =
            self.memory_quota.used_ratio() >= self.config.memory_quota_abort_threshold();

        // Check if last flush was more than the deregister threshold.
        // To prevent the case that the connection idle since there are a lot of
        // incremental scan tasks queueing so won't send events, also check on the
        // memory usage. The failpoint can adjust the memory threshold for manual
        // testing.
        if elapsed > self.config.idle_deregister_threshold() && memory_quota_abort_threshold_reached
        {
            error!("cdc connection idle for too long, aborting connection";
                "seconds_since_last_flush" => elapsed.as_secs(),
                "downstream" => self.peer.as_str(),
                "conn_id" => ?self.conn_id);
            self.abort.abort();
            return true;
        }

        false
    }
}

#[cfg(test)]
mod tests {
    use std::{sync::Arc, time::Duration};

    use tikv_util::{future::block_on_timeout, memory::MemoryQuota, worker::Builder};

    use super::*;

    #[test]
    fn test_connection_watchdog_cancels_send_and_receive() {
        let pool = Arc::new(Builder::new("cdc-watchdog-test").thread_count(1).create());
        let activity = ActivityHandle::with_last_flush(Instant::now() - Duration::from_secs(1));
        let memory_quota = Arc::new(MemoryQuota::new(1));
        memory_quota.alloc_force(1);

        let config = WatchdogConfig {
            check_interval: Duration::from_millis(50),
            idle_deregister_threshold: Duration::from_millis(100),
            ..Default::default()
        };

        let handle = Watchdog::spawn_with_activity(
            &pool,
            "127.0.0.1:0".to_owned(),
            ConnId::new(),
            memory_quota,
            activity,
            config,
        );

        let timeout = Duration::from_secs(1);
        block_on_timeout(wait_for_abort(handle.send_abort), timeout)
            .expect("watchdog should cancel send");
        block_on_timeout(wait_for_abort(handle.recv_abort), timeout)
            .expect("watchdog should cancel receive");
        pool.stop();
    }

    #[test]
    fn test_abort_handle_idempotent() {
        let (mut abort, recv_abort, send_abort) = AbortHandle::new();

        abort.abort();
        abort.abort();

        block_on_timeout(wait_for_abort(send_abort), Duration::from_secs(1))
            .expect("watchdog should cancel send");
        block_on_timeout(wait_for_abort(recv_abort), Duration::from_secs(1))
            .expect("watchdog should cancel receive");
    }

    #[test]
    fn test_wait_for_abort_hangs_on_sender_drop() {
        let (tx, rx) = oneshot::channel();
        drop(tx);

        block_on_timeout(wait_for_abort(rx), Duration::from_millis(100))
            .expect_err("wait_for_abort should not finish when sender is dropped");
    }
}
