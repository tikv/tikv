// Copyright 2026 TiKV Project Authors. Licensed under Apache-2.0.

use std::{
    fs,
    path::PathBuf,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    time::Duration,
};

use health_controller::HealthController;
use prometheus::{IntCounter, IntGauge, register_int_counter, register_int_gauge};
use tikv_util::{config::VersionTrack, logger, warn, worker::Worker};

use super::{Config, metrics::STORE_INSPECT_DURATION_HISTOGRAM};
use crate::store::disk_probe::ProbeRunner;

const PROBE_INTERVAL: Duration = Duration::from_secs(1);
const PROBE_FILENAME: &str = ".raft_disk_fail_fast_probe.tmp";
const PROBE_PAYLOAD: &[u8] = b"tikv raft disk fail fast probe";
const SLOW_SCORE_GATE_THRESHOLD: f64 = 95.0;

lazy_static::lazy_static! {
    pub static ref RAFT_DISK_PROBE_SUCCESS: IntCounter = register_int_counter!(
        "tikv_raftstore_raft_disk_probe_success_total",
        "Total successful raft disk fail-fast probes"
    ).unwrap();
    pub static ref RAFT_DISK_PROBE_FAILURE: IntCounter = register_int_counter!(
        "tikv_raftstore_raft_disk_probe_failure_total",
        "Total failed raft disk fail-fast probes"
    ).unwrap();
    pub static ref RAFT_DISK_PROBE_TIME_SINCE_LAST_SUCCESS_SECONDS: IntGauge = register_int_gauge!(
        "tikv_raftstore_raft_disk_probe_time_since_last_success_seconds",
        "Seconds since last successful raft disk fail-fast probe"
    ).unwrap();
    pub static ref RAFT_DISK_PROBE_CURRENT_PROBE_ELAPSED_SECONDS: IntGauge = register_int_gauge!(
        "tikv_raftstore_raft_disk_probe_current_probe_elapsed_seconds",
        "Elapsed seconds of the current raft disk fail-fast probe"
    ).unwrap();
    pub static ref FAIL_FAST_TRIGGER_TOTAL: IntCounter = register_int_counter!(
        "tikv_fail_fast_trigger_total",
        "Total fail-fast exits triggered by raft disk hang"
    ).unwrap();
}

fn record_probe_duration(duration: Duration) {
    STORE_INSPECT_DURATION_HISTOGRAM
        .with_label_values(&["raft-disk-probe"])
        .observe(duration.as_secs_f64());
}

fn probe_once(probe: &ProbeRunner) -> std::io::Result<Duration> {
    probe.probe_once()
}

pub struct FailFastMonitor {
    stop: Arc<AtomicBool>,
    probe: ProbeRunner,
}

impl FailFastMonitor {
    pub fn new(
        cfg: Arc<VersionTrack<Config>>,
        health_controller: HealthController,
        probe_dir: PathBuf,
        background_worker: Worker,
    ) -> Self {
        let stop = Arc::new(AtomicBool::new(false));
        let probe = ProbeRunner::new(probe_dir.join(PROBE_FILENAME), PROBE_PAYLOAD);

        let stop_for_probe = stop.clone();
        let probe_for_probe = probe.clone();
        background_worker.spawn_interval_task(PROBE_INTERVAL, move || {
            if stop_for_probe.load(Ordering::Acquire) {
                return;
            }
            if !probe_for_probe.try_start_probe() {
                return;
            }
            match probe_once(&probe_for_probe) {
                Ok(duration) => {
                    record_probe_duration(duration);
                    probe_for_probe.finish_probe_success();
                    RAFT_DISK_PROBE_SUCCESS.inc();
                }
                Err(e) => {
                    warn!("fail-fast probe failed"; "err" => ?e, "path" => %probe_for_probe.path().display());
                    record_probe_duration(Duration::ZERO);
                    probe_for_probe.finish_probe_failure();
                    RAFT_DISK_PROBE_FAILURE.inc();
                }
            }
        });

        let stop_for_check = stop.clone();
        let probe_for_check = probe.clone();
        background_worker.spawn_interval_task(PROBE_INTERVAL, move || {
            if stop_for_check.load(Ordering::Acquire) {
                return;
            }

            let timeout = cfg.value().raft_disk_hang_timeout;
            let Some(timeout) = timeout else {
                RAFT_DISK_PROBE_CURRENT_PROBE_ELAPSED_SECONDS.set(0);
                RAFT_DISK_PROBE_TIME_SINCE_LAST_SUCCESS_SECONDS.set(0);
                return;
            };
            if timeout.0.is_zero() {
                RAFT_DISK_PROBE_CURRENT_PROBE_ELAPSED_SECONDS.set(0);
                RAFT_DISK_PROBE_TIME_SINCE_LAST_SUCCESS_SECONDS.set(0);
                return;
            }

            let current_probe_elapsed = probe_for_check.current_probe_elapsed();
            let time_since_last_success = probe_for_check.time_since_last_success();
            RAFT_DISK_PROBE_CURRENT_PROBE_ELAPSED_SECONDS.set(
                current_probe_elapsed
                    .map(|d| d.as_secs() as i64)
                    .unwrap_or(0),
            );
            RAFT_DISK_PROBE_TIME_SINCE_LAST_SUCCESS_SECONDS
                .set(time_since_last_success.as_secs() as i64);

            if let Some(current_probe_elapsed) = current_probe_elapsed {
                if current_probe_elapsed >= timeout.0
                    && health_controller.get_raftstore_slow_score() > SLOW_SCORE_GATE_THRESHOLD
                {
                    FAIL_FAST_TRIGGER_TOTAL.inc();
                    health_controller.set_is_serving(false);
                    eprintln!("raft disk hung for configured timeout");
                    logger::exit_process_gracefully(1);
                }
            }
        });

        Self { stop, probe }
    }

    pub fn start(&mut self) {}

    pub fn stop(&mut self) {
        self.stop.store(true, Ordering::Release);
        self.cleanup_probe_file();
    }

    fn cleanup_probe_file(&self) {
        if let Err(e) = fs::remove_file(self.probe.path()) {
            if e.kind() != std::io::ErrorKind::NotFound {
                warn!("failed to remove fail-fast probe file"; "err" => ?e, "path" => %self.probe.path().display());
            }
        }
    }
}

impl Drop for FailFastMonitor {
    fn drop(&mut self) {
        self.cleanup_probe_file();
    }
}

#[cfg(test)]
mod tests {
    use tempfile::tempdir;

    use super::*;

    #[test]
    fn test_probe_once() {
        let dir = tempdir().unwrap();
        let probe = ProbeRunner::new(dir.path().join(PROBE_FILENAME), PROBE_PAYLOAD);
        let duration = probe_once(&probe).unwrap();
        assert!(duration > Duration::ZERO);
        assert!(probe.path().exists());
    }
}
