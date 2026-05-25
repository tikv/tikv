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
    metrics::CRITICAL_ERROR,
    sys::thread::StdThreadBuildWrapper,
    time::{monotonic_raw_now, timespec_to_ns},
    warn,
    worker::Worker,
};

use super::Config;
use crate::store::disk_probe::ProbeRunner;

const PROBE_INTERVAL: Duration = Duration::from_secs(1);
const IN_FLIGHT_LOG_THRESHOLD: Duration = Duration::from_secs(30);
const FAIL_FAST_LOG_FLUSH_TIMEOUT: Duration = Duration::from_secs(2);

const RAFT_PROBE_FILENAME: &str = ".raft_disk_fail_fast_probe.tmp";
const RAFT_PROBE_PAYLOAD: &[u8] = b"tikv raft disk fail fast probe";
const KV_PROBE_FILENAME: &str = ".kv_disk_fail_fast_probe.tmp";
const KV_PROBE_PAYLOAD: &[u8] = b"tikv kv disk fail fast probe";
const CRITICAL_ERROR_DISK_PROBE_STUCK_RAFT: &str = "disk_probe_stuck_raft";
const CRITICAL_ERROR_DISK_PROBE_STUCK_KV: &str = "disk_probe_stuck_kv";
const CRITICAL_ERROR_DISK_PROBE_FAIL_FAST_RAFT: &str = "disk_probe_fail_fast_raft";
const CRITICAL_ERROR_DISK_PROBE_FAIL_FAST_KV: &str = "disk_probe_fail_fast_kv";

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

fn fail_fast_timeout(cfg: &Config) -> Option<Duration> {
    // `0s` is treated as explicitly disabled.
    cfg.disk_hang_timeout
        .and_then(|timeout| (!timeout.0.is_zero()).then_some(timeout.0))
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
        if elapsed < IN_FLIGHT_LOG_THRESHOLD {
            return;
        }
        let critical_error_kind = match disk {
            "raft" => CRITICAL_ERROR_DISK_PROBE_STUCK_RAFT,
            "kv" => CRITICAL_ERROR_DISK_PROBE_STUCK_KV,
            _ => return,
        };
        CRITICAL_ERROR
            .with_label_values(&[critical_error_kind])
            .inc();
        error!(
            "fail-fast probe: disk still not responsive after elapsed";
            "disk" => disk,
            "path" => %probe.path().display(),
            "elapsed" => ?elapsed,
        );
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
                let Some(_) = fail_fast_timeout(&cfg.value()) else {
                    std::thread::sleep(PROBE_INTERVAL);
                    continue;
                };

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

/// Detects one class of TiKV "zombie" failure and chooses to hard-exit.
///
/// The target failure mode here is: the process is still alive and may still
/// hold Raft leadership, but it is no longer truly serviceable because disk IO
/// on the critical path has stopped making progress. In that state, staying
/// alive can prolong unavailability and hide the failure from orchestration.
///
/// The monitor has two parts:
/// 1. A dedicated probe thread periodically runs a blocking `write + sync_all`
///    probe against the raft disk, and against the kv disk as well when raft
///    and kv are deployed on different mount points.
/// 2. A separate checker worker periodically inspects the probe state, so a
///    hung blocking probe cannot prevent fail-fast from being triggered.
///
/// The checker treats a disk as hung under either of these rules:
/// 1. The current probe is still in flight and its elapsed time has reached
///    `raftstore.disk-hang-timeout`.
/// 2. There is no current in-flight probe, but probes have kept failing for at
///    least `raftstore.disk-hang-timeout` after a prior successful probe.
///
/// Raft has one extra anti-false-positive guard: even if the dedicated raft
/// probe is slow, the checker will not fail fast on that signal alone if real
/// raft-log appends have still succeeded within the same timeout window.
///
/// Once any disk meets the fail-fast rule, TiKV emits the final error, gives
/// the async logger a brief best-effort flush window, and then aborts. The
/// scope is intentionally narrow today: disk hang is the first zombie-failure
/// scenario covered by this monitor.
pub struct FailFastMonitor {
    stop: Arc<AtomicBool>,
    raft_probe: ProbeRunner,
    kv_probe: Option<ProbeRunner>,
}

impl FailFastMonitor {
    pub fn new(
        cfg: Arc<VersionTrack<Config>>,
        _health_controller: HealthController,
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

            let Some(timeout) = fail_fast_timeout(&cfg.value()) else {
                return;
            };

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
            let raft_recent_append_progress =
                raft_last_append_success_elapsed.is_some_and(|elapsed| elapsed < timeout);
            // Fail-fast is armed by either:
            // 1. the current probe staying in flight for >= timeout, or
            // 2. repeated probe failures for >= timeout after a prior success.
            let raft_should_fail_fast = should_fail_fast_for_raft_probe(
                &raft_probe_for_check,
                timeout,
                raft_recent_append_progress,
            );
            let kv_should_fail_fast = kv_elapsed.is_some_and(|elapsed| elapsed >= timeout)
                || kv_probe_for_check
                    .as_ref()
                    .is_some_and(|probe| should_fail_fast_on_repeated_failures(probe, timeout));
            if !raft_should_fail_fast && !kv_should_fail_fast {
                return;
            }

            if raft_should_fail_fast {
                CRITICAL_ERROR
                    .with_label_values(&[CRITICAL_ERROR_DISK_PROBE_FAIL_FAST_RAFT])
                    .inc();
            }
            if kv_should_fail_fast {
                CRITICAL_ERROR
                    .with_label_values(&[CRITICAL_ERROR_DISK_PROBE_FAIL_FAST_KV])
                    .inc();
            }

            error!(
                "fail-fast: disk probe timed out, shutting down to avoid serving on an unhealthy disk";
                "timeout" => ?timeout,
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
            logger::abort_process_after_best_effort_flush(FAIL_FAST_LOG_FLUSH_TIMEOUT);
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

    #[test]
    fn test_fail_fast_timeout_none_disables() {
        let mut cfg = Config::default();
        cfg.disk_hang_timeout = None;
        assert_eq!(fail_fast_timeout(&cfg), None);
    }

    #[test]
    fn test_should_fail_fast_for_raft_probe_with_recent_progress_veto() {
        let dir = tempdir().unwrap();
        let probe = ProbeRunner::new(dir.path().join(RAFT_PROBE_FILENAME), RAFT_PROBE_PAYLOAD);
        let timeout = Duration::from_millis(10);

        assert!(probe.try_start_probe());
        std::thread::sleep(timeout + Duration::from_millis(5));
        assert!(!should_fail_fast_for_raft_probe(&probe, timeout, true));
        assert!(should_fail_fast_for_raft_probe(&probe, timeout, false));
    }

    #[test]
    fn test_should_fail_fast_on_repeated_failures_after_prior_success() {
        let dir = tempdir().unwrap();
        let probe = ProbeRunner::new(dir.path().join(RAFT_PROBE_FILENAME), RAFT_PROBE_PAYLOAD);
        let timeout = Duration::from_millis(10);

        assert!(!should_fail_fast_on_repeated_failures(&probe, timeout));
        probe.finish_probe_success();
        assert!(probe.try_start_probe());
        probe.finish_probe_failure();
        std::thread::sleep(timeout + Duration::from_millis(5));
        assert!(should_fail_fast_on_repeated_failures(&probe, timeout));
    }
}
