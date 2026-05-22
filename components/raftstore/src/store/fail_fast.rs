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
use tikv_util::{config::VersionTrack, error, logger, warn, worker::Worker};

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

fn spawn_probe_loop(
    worker: &Worker,
    cfg: Arc<VersionTrack<Config>>,
    stop: Arc<AtomicBool>,
    disk: &'static str,
    metric_label: &'static str,
    probe: ProbeRunner,
) {
    // The probe loop is intentionally simple: it only tries to start a probe,
    // then performs a blocking `write + sync_all`, and reports success/failure.
    //
    // If the disk is hung, this task can block indefinitely. That is why the
    // checker below must run on an independent worker thread: otherwise the
    // checker could be starved by the probe it supervises.
    worker.spawn_interval_task(PROBE_INTERVAL, move || {
        if stop.load(Ordering::Acquire) {
            return;
        }
        // Avoid issuing any IO when fail-fast is disabled. Otherwise the default
        // config (None) still creates background fsync traffic, and a stuck probe
        // would also make graceful shutdown harder without an exit path.
        let Some(timeout) = cfg.value().disk_hang_timeout else {
            return;
        };
        if timeout.0.is_zero() {
            return;
        }
        if !probe.try_start_probe() {
            return;
        }

        match probe.probe_once() {
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
                probe.finish_probe_failure();
                DISK_PROBE_FAILURE.with_label_values(&[disk]).inc();
            }
        }
    });
}

/// A best-effort "fail-fast" monitor for cases where TiKV can become
/// effectively unavailable without crashing.
///
/// In some failure modes, TiKV may remain alive and even keep Raft leadership,
/// but it is no longer able to make forward progress or provide service (a
/// "zombie" state). In such cases, keeping the process running can prolong
/// unavailability and hide the real failure from external orchestration.
///
/// This monitor chooses to fail fast (exit the process) when it detects such a
/// condition. Today it covers disk hang detection via a blocking `write +
/// sync_all` probe on both raft and kv disks. The checker runs on an
/// independent worker thread so it can still trigger even if the probe thread
/// is stuck. The scope is expected to expand to cover more zombie-like failure
/// modes in the future.
pub struct FailFastMonitor {
    stop: Arc<AtomicBool>,
    raft_probe: ProbeRunner,
    kv_probe: Option<ProbeRunner>,
}

impl FailFastMonitor {
    pub fn new(
        cfg: Arc<VersionTrack<Config>>,
        health_controller: HealthController,
        raft_probe_dir: PathBuf,
        kv_probe_dir: Option<PathBuf>,
        probe_worker: Worker,
        check_worker: Worker,
    ) -> Self {
        let stop = Arc::new(AtomicBool::new(false));
        let raft_probe =
            ProbeRunner::new(raft_probe_dir.join(RAFT_PROBE_FILENAME), RAFT_PROBE_PAYLOAD);
        let kv_probe =
            kv_probe_dir.map(|dir| ProbeRunner::new(dir.join(KV_PROBE_FILENAME), KV_PROBE_PAYLOAD));

        spawn_probe_loop(
            &probe_worker,
            cfg.clone(),
            stop.clone(),
            "raft",
            "raft-disk-probe",
            raft_probe.clone(),
        );
        if let Some(kv_probe) = kv_probe.as_ref() {
            spawn_probe_loop(
                &probe_worker,
                cfg.clone(),
                stop.clone(),
                "kv",
                "kv-disk-probe",
                kv_probe.clone(),
            );
        }

        let stop_for_check = stop.clone();
        let raft_probe_for_check = raft_probe.clone();
        let kv_probe_for_check = kv_probe.clone();
        check_worker.spawn_interval_task(PROBE_INTERVAL, move || {
            if stop_for_check.load(Ordering::Acquire) {
                return;
            }

            // An in-flight probe means the previous `write + sync_all` has not
            // returned yet, which is a strong signal of disk unresponsiveness.
            // We log this early (>= 1s) for observability even if it's below
            // the configured fail-fast timeout.
            if let Some(elapsed) = raft_probe_for_check.current_probe_elapsed() {
                if elapsed >= ANOMALY_THRESHOLD {
                    warn!(
                        "fail-fast probe: disk still not responsive after elapsed";
                        "disk" => "raft",
                        "path" => %raft_probe_for_check.path().display(),
                        "elapsed" => ?elapsed,
                    );
                }
            }
            if let Some(kv_probe_for_check) = kv_probe_for_check.as_ref() {
                if let Some(elapsed) = kv_probe_for_check.current_probe_elapsed() {
                    if elapsed >= ANOMALY_THRESHOLD {
                        warn!(
                            "fail-fast probe: disk still not responsive after elapsed";
                            "disk" => "kv",
                            "path" => %kv_probe_for_check.path().display(),
                            "elapsed" => ?elapsed,
                        );
                    }
                }
            }

            let timeout = cfg.value().disk_hang_timeout;
            let Some(timeout) = timeout else { return };
            if timeout.0.is_zero() {
                return;
            }

            // Gate on slow score to avoid exiting due to transient IO jitter.
            // When the store is healthy, a slow/overlapped probe is more likely
            // to be benign (e.g. short fs stalls). When slow score is high, we
            // treat a stuck probe as a stronger indication of a real hang.
            let slow_score_ok =
                health_controller.get_raftstore_slow_score() > SLOW_SCORE_GATE_THRESHOLD;
            if !slow_score_ok {
                return;
            }

            let raft_elapsed = raft_probe_for_check.current_probe_elapsed();
            let kv_elapsed = kv_probe_for_check
                .as_ref()
                .and_then(|probe| probe.current_probe_elapsed());
            if raft_elapsed.is_some_and(|d| d >= timeout.0)
                || kv_elapsed.is_some_and(|d| d >= timeout.0)
            {
                // We intentionally hard-exit here. Continuing to serve on a
                // hung disk can cause prolonged unavailability and data loss
                // risks (e.g. stuck raft/kv writes). We first flip is_serving
                // to false to stop new traffic, then exit.
                error!(
                    "fail-fast: disk probe timed out, shutting down to avoid serving on an unhealthy disk";
                    "timeout" => ?timeout.0,
                    "raft_current_probe_elapsed" => ?raft_elapsed,
                    "kv_current_probe_elapsed" => ?kv_elapsed,
                );
                health_controller.set_is_serving(false);
                eprintln!("disk hung for configured timeout");
                logger::exit_process_gracefully(1);
            }
        });

        Self {
            stop,
            raft_probe,
            kv_probe,
        }
    }

    pub fn stop(&self) {
        self.stop.store(true, Ordering::Release);
        self.cleanup_probe_file();
    }

    fn cleanup_probe_file(&self) {
        let kv_path = self.kv_probe.as_ref().map(|probe| probe.path());
        for path in std::iter::once(self.raft_probe.path()).chain(kv_path.into_iter()) {
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
        let duration = raft_probe.probe_once().unwrap();
        assert!(duration > Duration::ZERO);
        assert!(raft_probe.path().exists());

        let kv_probe = ProbeRunner::new(dir.path().join(KV_PROBE_FILENAME), KV_PROBE_PAYLOAD);
        let duration = kv_probe.probe_once().unwrap();
        assert!(duration > Duration::ZERO);
        assert!(kv_probe.path().exists());
    }
}
