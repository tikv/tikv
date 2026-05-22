// Copyright 2026 TiKV Project Authors. Licensed under Apache-2.0.

use std::{
    fs,
    path::PathBuf,
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicU64, Ordering},
    },
    time::Duration,
};

use health_controller::HealthController;
use prometheus::{HistogramVec, exponential_buckets, register_histogram_vec};
use tikv_util::{
    config::VersionTrack,
    error, logger,
    sys::thread::StdThreadBuildWrapper,
    time::{monotonic_raw_now, timespec_to_ns},
    warn,
    worker::Worker,
};

use super::Config;
use crate::store::disk_probe::ProbeRunner;

const PROBE_INTERVAL: Duration = Duration::from_secs(1);
const IN_FLIGHT_LOG_THRESHOLD: Duration = Duration::from_secs(30);

const RAFT_PROBE_FILENAME: &str = ".raft_disk_fail_fast_probe.tmp";
const RAFT_PROBE_PAYLOAD: &[u8] = b"tikv raft disk fail fast probe";
const KV_PROBE_FILENAME: &str = ".kv_disk_fail_fast_probe.tmp";
const KV_PROBE_PAYLOAD: &[u8] = b"tikv kv disk fail fast probe";

lazy_static::lazy_static! {
    pub static ref DISK_PROBE_DURATION_HISTOGRAM: HistogramVec = register_histogram_vec!(
        "tikv_raftstore_disk_probe_duration_seconds",
        "Bucketed histogram of fail-fast disk probe duration.",
        &["disk", "outcome"],
        exponential_buckets(0.00001, 2.0, 26).unwrap()
    ).unwrap();
}

fn record_probe_duration(disk: &str, outcome: &str, duration: Duration) {
    DISK_PROBE_DURATION_HISTOGRAM
        .with_label_values(&[disk, outcome])
        .observe(duration.as_secs_f64());
}

fn run_probe(stop: &AtomicBool, disk: &'static str, probe: &ProbeRunner) -> bool {
    let started = probe.try_start_probe();
    debug_assert!(started, "fail-fast probe must not overlap");
    if !started {
        return true;
    }

    match probe.probe_once() {
        Ok(duration) => {
            probe.finish_probe_success();
            if stop.load(Ordering::Acquire) {
                return false;
            }
            record_probe_duration(disk, "success", duration);
        }
        Err(e) => {
            let duration = probe.current_probe_elapsed().unwrap_or_default();
            probe.finish_probe_failure();
            if stop.load(Ordering::Acquire) {
                return false;
            }
            record_probe_duration(disk, "failure", duration);
            warn!(
                "fail-fast probe failed";
                "disk" => disk,
                "path" => %probe.path().display(),
                "elapsed" => ?duration,
                "err" => ?e,
            );
        }
    }

    true
}

fn log_stuck_probe(disk: &'static str, probe: &ProbeRunner) {
    if let Some(elapsed) = probe.current_probe_elapsed() {
        if elapsed >= IN_FLIGHT_LOG_THRESHOLD {
            warn!(
                "fail-fast probe: disk still not responsive after elapsed";
                "disk" => disk,
                "path" => %probe.path().display(),
                "elapsed" => ?elapsed,
            );
        }
    }
}

fn spawn_probe_thread(
    cfg: Arc<VersionTrack<Config>>,
    stop: Arc<AtomicBool>,
    raft_probe: ProbeRunner,
    kv_probe: Option<ProbeRunner>,
) -> std::io::Result<()> {
    // The blocking `write + sync_all` probe can hang indefinitely on an
    // unresponsive disk. Run it on a dedicated OS thread that is not joined
    // during shutdown, so manual stop/restart is not blocked by a stuck probe.
    std::thread::Builder::new()
        .name("fail-fast-probe".to_owned())
        .spawn_wrapper(move || {
            while !stop.load(Ordering::Acquire) {
                // Avoid issuing any IO when fail-fast is disabled. Otherwise the
                // default config (None) still creates background fsync traffic.
                let Some(timeout) = cfg.value().disk_hang_timeout else {
                    std::thread::sleep(PROBE_INTERVAL);
                    continue;
                };
                if timeout.0.is_zero() {
                    std::thread::sleep(PROBE_INTERVAL);
                    continue;
                }

                if !run_probe(&stop, "raft", &raft_probe) {
                    return;
                }
                if let Some(kv_probe) = kv_probe.as_ref() {
                    if !run_probe(&stop, "kv", kv_probe) {
                        return;
                    }
                }

                std::thread::sleep(PROBE_INTERVAL);
            }
        })?;
    Ok(())
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
        check_worker: Worker,
        last_raft_append_success_at_millis: Arc<AtomicU64>,
    ) -> std::io::Result<Self> {
        let stop = Arc::new(AtomicBool::new(false));
        let raft_probe =
            ProbeRunner::new(raft_probe_dir.join(RAFT_PROBE_FILENAME), RAFT_PROBE_PAYLOAD);
        let kv_probe =
            kv_probe_dir.map(|dir| ProbeRunner::new(dir.join(KV_PROBE_FILENAME), KV_PROBE_PAYLOAD));

        spawn_probe_thread(
            cfg.clone(),
            stop.clone(),
            raft_probe.clone(),
            kv_probe.clone(),
        )?;

        let stop_for_check = stop.clone();
        let raft_probe_for_check = raft_probe.clone();
        let kv_probe_for_check = kv_probe.clone();
        check_worker.spawn_interval_task(PROBE_INTERVAL, move || {
            if stop_for_check.load(Ordering::Acquire) {
                return;
            }

            let timeout = cfg.value().disk_hang_timeout;
            let Some(timeout) = timeout else {
                return;
            };
            if timeout.0.is_zero() {
                return;
            }

            log_stuck_probe("raft", &raft_probe_for_check);
            if let Some(probe) = kv_probe_for_check.as_ref() {
                log_stuck_probe("kv", probe);
            }

            let raft_elapsed = raft_probe_for_check.current_probe_elapsed();
            let kv_elapsed = kv_probe_for_check
                .as_ref()
                .and_then(|probe| probe.current_probe_elapsed());
            // Raft has one extra anti-false-positive guard: do not fail fast on
            // a single slow probe if normal raft-log appends have still made
            // progress within the same timeout window.
            let raft_last_append_success_elapsed =
                last_raft_append_success_elapsed(&last_raft_append_success_at_millis);
            let raft_recent_append_progress = raft_last_append_success_elapsed
                .is_some_and(|elapsed| elapsed < timeout.0);
            // Fail-fast is armed by either:
            // 1. the current probe staying in flight for >= timeout, or
            // 2. repeated probe failures for >= timeout after a prior success.
            let raft_should_fail_fast =
                should_fail_fast_for_raft_probe(&raft_probe_for_check, timeout.0, raft_recent_append_progress);
            let kv_should_fail_fast = kv_elapsed.is_some_and(|elapsed| elapsed >= timeout.0)
                || kv_probe_for_check
                    .as_ref()
                    .is_some_and(|probe| should_fail_fast_on_repeated_failures(probe, timeout.0));
            if !raft_should_fail_fast && !kv_should_fail_fast {
                return;
            }

            // We intentionally hard-exit here. Continuing to serve on a
            // hung disk can cause prolonged unavailability and data loss
            // risks (e.g. stuck raft/kv writes). We first flip is_serving
            // to false to stop new traffic, then exit.
            error!(
                "fail-fast: disk probe timed out, shutting down to avoid serving on an unhealthy disk";
                "timeout" => ?timeout.0,
                "raft_current_probe_elapsed" => ?raft_elapsed,
                "kv_current_probe_elapsed" => ?kv_elapsed,
                "raft_time_since_last_success" => ?raft_probe_for_check.time_since_last_success(),
                "kv_time_since_last_success" => ?kv_probe_for_check
                    .as_ref()
                    .and_then(|probe| probe.time_since_last_success()),
                "raft_failure_count_since_last_success" => raft_probe_for_check
                    .failure_count_since_last_success(),
                "kv_failure_count_since_last_success" => kv_probe_for_check
                    .as_ref()
                    .map_or(0, |probe| probe.failure_count_since_last_success()),
                "raft_last_append_success_elapsed" => ?raft_last_append_success_elapsed,
            );
            health_controller.set_is_serving(false);
            eprintln!("disk hung for configured timeout");
            logger::exit_process_gracefully(1);
        });

        Ok(Self {
            stop,
            raft_probe,
            kv_probe,
        })
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

fn should_fail_fast_for_raft_probe(
    probe: &ProbeRunner,
    timeout: Duration,
    has_recent_append_progress: bool,
) -> bool {
    // For raft, a slow probe alone is not enough if real raft appends are
    // still succeeding recently. That treats actual raft progress as a veto on
    // top of the generic probe-timeout rule.
    probe
        .current_probe_elapsed()
        .is_some_and(|elapsed| elapsed >= timeout && !has_recent_append_progress)
        || should_fail_fast_on_repeated_failures(probe, timeout)
}

// This catches the "not one forever-stuck probe, but no successful probe for a
// full timeout window" case. We only arm it after a prior success so obvious
// startup/configuration mistakes do not immediately turn into fail-fast exits.
fn should_fail_fast_on_repeated_failures(probe: &ProbeRunner, timeout: Duration) -> bool {
    probe.current_probe_elapsed().is_none()
        && probe.failure_count_since_last_success() > 0
        && probe
            .time_since_last_success()
            .is_some_and(|elapsed| elapsed >= timeout)
}

// Returns how long it has been since the last successful raft-log append,
// using the same monotonic raw clock as the write thread.
//
// This is intentionally only a coarse veto signal for the raft probe path; kv
// fail-fast currently relies only on the probe state itself.
fn last_raft_append_success_elapsed(
    last_raft_append_success_at_millis: &AtomicU64,
) -> Option<Duration> {
    let last = last_raft_append_success_at_millis.load(Ordering::Relaxed);
    if last == 0 {
        return None;
    }
    let now = timespec_to_ns(monotonic_raw_now()) / 1_000_000;
    Some(Duration::from_millis(now.saturating_sub(last)))
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
