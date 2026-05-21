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
use prometheus::{IntCounterVec, register_int_counter_vec};
use tikv_util::{
    config::VersionTrack,
    logger, warn,
    worker::{Builder as WorkerBuilder, Worker},
};

use super::{Config, metrics::STORE_INSPECT_DURATION_HISTOGRAM};
use crate::store::disk_probe::ProbeRunner;

const PROBE_INTERVAL: Duration = Duration::from_secs(1);
const ANOMALY_THRESHOLD: Duration = Duration::from_secs(1);

const RAFT_PROBE_FILENAME: &str = ".raft_disk_fail_fast_probe.tmp";
const RAFT_PROBE_PAYLOAD: &[u8] = b"tikv raft disk fail fast probe";
const KV_PROBE_FILENAME: &str = ".kv_disk_fail_fast_probe.tmp";
const KV_PROBE_PAYLOAD: &[u8] = b"tikv kv disk fail fast probe";
const SLOW_SCORE_GATE_THRESHOLD: f64 = 95.0;

lazy_static::lazy_static! {
    pub static ref DISK_PROBE_SUCCESS: IntCounterVec = register_int_counter_vec!(
        "tikv_raftstore_disk_probe_success_total",
        "Total successful disk fail-fast probes",
        &["disk"]
    ).unwrap();
    pub static ref DISK_PROBE_FAILURE: IntCounterVec = register_int_counter_vec!(
        "tikv_raftstore_disk_probe_failure_total",
        "Total failed disk fail-fast probes",
        &["disk"]
    ).unwrap();
}

fn record_probe_duration(metric_label: &str, duration: Duration) {
    STORE_INSPECT_DURATION_HISTOGRAM
        .with_label_values(&[metric_label])
        .observe(duration.as_secs_f64());
}

fn probe_once(probe: &ProbeRunner) -> std::io::Result<Duration> {
    probe.probe_once()
}

pub struct FailFastMonitor {
    stop: Arc<AtomicBool>,
    raft_probe: ProbeRunner,
    kv_probe: ProbeRunner,
    #[allow(dead_code)]
    probe_worker: Worker,
    #[allow(dead_code)]
    check_worker: Worker,
}

impl FailFastMonitor {
    pub fn new(
        cfg: Arc<VersionTrack<Config>>,
        health_controller: HealthController,
        raft_probe_dir: PathBuf,
        kv_probe_dir: PathBuf,
    ) -> Self {
        let stop = Arc::new(AtomicBool::new(false));
        let raft_probe =
            ProbeRunner::new(raft_probe_dir.join(RAFT_PROBE_FILENAME), RAFT_PROBE_PAYLOAD);
        let kv_probe = ProbeRunner::new(kv_probe_dir.join(KV_PROBE_FILENAME), KV_PROBE_PAYLOAD);

        let probe_worker = WorkerBuilder::new("fail-fast-probe")
            .thread_count(1)
            .create();
        let check_worker = WorkerBuilder::new("fail-fast-check")
            .thread_count(1)
            .create();

        let make_probe_task = |disk: &'static str,
                               metric_label: &'static str,
                               probe: ProbeRunner,
                               stop: Arc<AtomicBool>| {
            move || {
                if stop.load(Ordering::Acquire) {
                    return;
                }
                if !probe.try_start_probe() {
                    if let Some(elapsed) = probe.current_probe_elapsed() {
                        if elapsed >= ANOMALY_THRESHOLD {
                            warn!(
                                "fail-fast probe still in flight";
                                "disk" => disk,
                                "path" => %probe.path().display(),
                                "elapsed" => ?elapsed,
                            );
                        }
                    }
                    return;
                }

                match probe_once(&probe) {
                    Ok(duration) => {
                        if duration >= ANOMALY_THRESHOLD {
                            warn!(
                                "fail-fast probe slow";
                                "disk" => disk,
                                "path" => %probe.path().display(),
                                "elapsed" => ?duration,
                            );
                        }
                        record_probe_duration(metric_label, duration);
                        probe.finish_probe_success();
                        DISK_PROBE_SUCCESS.with_label_values(&[disk]).inc();
                    }
                    Err(e) => {
                        warn!(
                            "fail-fast probe failed";
                            "disk" => disk,
                            "path" => %probe.path().display(),
                            "err" => ?e,
                        );
                        record_probe_duration(metric_label, Duration::ZERO);
                        probe.finish_probe_failure();
                        DISK_PROBE_FAILURE.with_label_values(&[disk]).inc();
                    }
                }
            }
        };

        {
            let stop_for_probe = stop.clone();
            let raft_probe_for_probe = raft_probe.clone();
            probe_worker.spawn_interval_task(
                PROBE_INTERVAL,
                make_probe_task(
                    "raft",
                    "raft-disk-probe",
                    raft_probe_for_probe,
                    stop_for_probe,
                ),
            );
        }

        {
            let stop_for_probe = stop.clone();
            let kv_probe_for_probe = kv_probe.clone();
            probe_worker.spawn_interval_task(
                PROBE_INTERVAL,
                make_probe_task("kv", "kv-disk-probe", kv_probe_for_probe, stop_for_probe),
            );
        }

        let stop_for_check = stop.clone();
        let raft_probe_for_check = raft_probe.clone();
        let kv_probe_for_check = kv_probe.clone();
        check_worker.spawn_interval_task(PROBE_INTERVAL, move || {
            if stop_for_check.load(Ordering::Acquire) {
                return;
            }

            let timeout = cfg.value().disk_hang_timeout;
            let Some(timeout) = timeout else { return };
            if timeout.0.is_zero() {
                return;
            }

            let slow_score_ok =
                health_controller.get_raftstore_slow_score() > SLOW_SCORE_GATE_THRESHOLD;
            if !slow_score_ok {
                return;
            }

            let raft_elapsed = raft_probe_for_check.current_probe_elapsed();
            let kv_elapsed = kv_probe_for_check.current_probe_elapsed();
            if raft_elapsed.is_some_and(|d| d >= timeout.0)
                || kv_elapsed.is_some_and(|d| d >= timeout.0)
            {
                health_controller.set_is_serving(false);
                eprintln!("disk hung for configured timeout");
                logger::exit_process_gracefully(1);
            }
        });

        Self {
            stop,
            raft_probe,
            kv_probe,
            probe_worker,
            check_worker,
        }
    }

    pub fn start(&mut self) {}

    pub fn stop(&mut self) {
        self.stop.store(true, Ordering::Release);
        self.cleanup_probe_file();
    }

    fn cleanup_probe_file(&self) {
        for path in [self.raft_probe.path(), self.kv_probe.path()] {
            if let Err(e) = fs::remove_file(path) {
                if e.kind() != std::io::ErrorKind::NotFound {
                    warn!("failed to remove fail-fast probe file"; "err" => ?e, "path" => %path.display());
                }
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
        let raft_probe = ProbeRunner::new(dir.path().join(RAFT_PROBE_FILENAME), RAFT_PROBE_PAYLOAD);
        let duration = probe_once(&raft_probe).unwrap();
        assert!(duration > Duration::ZERO);
        assert!(raft_probe.path().exists());

        let kv_probe = ProbeRunner::new(dir.path().join(KV_PROBE_FILENAME), KV_PROBE_PAYLOAD);
        let duration = probe_once(&kv_probe).unwrap();
        assert!(duration > Duration::ZERO);
        assert!(kv_probe.path().exists());
    }
}
