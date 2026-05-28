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
    crit, error, logger,
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
const PROBE_PAYLOAD: &[u8] = b"tikv disk fail fast probe";

#[derive(Clone, Copy)]
enum DiskKind {
    Raft,
    Kv,
}

impl DiskKind {
    fn name(self) -> &'static str {
        match self {
            Self::Raft => "raft",
            Self::Kv => "kv",
        }
    }

    fn probe_filename(self) -> &'static str {
        match self {
            Self::Raft => ".raft_disk_fail_fast_probe.tmp",
            Self::Kv => ".kv_disk_fail_fast_probe.tmp",
        }
    }
}

const fn stuck_critical_error_kind(disk: DiskKind) -> &'static str {
    match disk {
        DiskKind::Raft => "disk_probe_stuck_raft",
        DiskKind::Kv => "disk_probe_stuck_kv",
    }
}

const fn fail_fast_critical_error_kind(disk: DiskKind) -> &'static str {
    match disk {
        DiskKind::Raft => "disk_probe_fail_fast_raft",
        DiskKind::Kv => "disk_probe_fail_fast_kv",
    }
}

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

// Returns false when shutdown is requested.
fn run_probe(stop: &AtomicBool, disk: DiskKind, probe: &ProbeRunner) -> bool {
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
            record_probe_duration(disk.name(), "success", duration);
        }
        Err(e) => {
            let duration = probe.current_probe_elapsed().unwrap_or_default();
            probe.finish_probe_failure();
            if stop.load(Ordering::Acquire) {
                return false;
            }
            record_probe_duration(disk.name(), "failure", duration);
            warn!(
                "fail-fast probe failed";
                "disk" => disk.name(),
                "path" => %probe.path().display(),
                "elapsed" => ?duration,
                "err" => ?e,
            );
        }
    }

    true
}

fn spawn_probe_thread(
    name: &'static str,
    cfg: Arc<VersionTrack<Config>>,
    stop: Arc<AtomicBool>,
    raft_probe: ProbeRunner,
    kv_probe: Option<ProbeRunner>,
) -> std::io::Result<()> {
    // A single probe thread alternates raft -> kv.
    std::thread::Builder::new()
        .name(name.to_owned())
        .spawn_wrapper(move || {
            while !stop.load(Ordering::Acquire) {
                // Avoid issuing background IO when fail-fast is disabled.
                let Some(_) = fail_fast_timeout(&cfg.value()) else {
                    std::thread::sleep(PROBE_INTERVAL);
                    continue;
                };

                if !run_probe(&stop, DiskKind::Raft, &raft_probe) {
                    return;
                }
                if let Some(kv_probe) = kv_probe.as_ref() {
                    if !run_probe(&stop, DiskKind::Kv, kv_probe) {
                        return;
                    }
                }

                std::thread::sleep(PROBE_INTERVAL);
            }
        })?;
    Ok(())
}

// Returns the fail-fast decision for one disk probe.
//
// The base rule comes from the probe itself:
// 1. an in-flight `write + sync_all` probe reaches the timeout, or
// 2. probes have kept failing for a full timeout window after a prior success.
//
// Recent successful sync-backed progress on the same disk vetoes that decision
// to reduce false positives. In other words, if the direct probe looks bad but
// real sync-backed work has still completed recently on that disk, TiKV treats
// the disk as still making forward progress and does not fail fast yet.
fn should_fail_fast(
    probe: Option<&ProbeRunner>,
    disk: DiskKind,
    timeout: Duration,
    last_sync_success_at_millis: &AtomicU64,
) -> bool {
    let Some(probe) = probe else {
        return false;
    };
    let current_probe_elapsed = probe.current_probe_elapsed();
    if let Some(elapsed) = current_probe_elapsed {
        if elapsed >= IN_FLIGHT_LOG_THRESHOLD {
            CRITICAL_ERROR
                .with_label_values(&[stuck_critical_error_kind(disk)])
                .inc();
            error!(
                "fail-fast probe: disk still not responsive after elapsed";
                "disk" => disk.name(),
                "path" => %probe.path().display(),
                "elapsed" => ?elapsed,
            );
        }
    }

    let last_sync_success_elapsed = last_sync_success_elapsed(last_sync_success_at_millis);
    let base_signal = current_probe_elapsed.is_some_and(|elapsed| elapsed >= timeout)
        || should_fail_fast_on_repeated_failures(probe, timeout);
    if !base_signal {
        return false;
    }

    let vetoed_by_recent_progress =
        last_sync_success_elapsed.is_some_and(|elapsed| elapsed < timeout);
    if vetoed_by_recent_progress {
        warn!(
            "fail-fast vetoed by recent sync-backed progress";
            "disk" => disk.name(),
            "timeout" => ?timeout,
            "current_probe_elapsed" => ?current_probe_elapsed,
            "last_sync_success_elapsed" => ?last_sync_success_elapsed,
        );
        return false;
    }

    CRITICAL_ERROR
        .with_label_values(&[fail_fast_critical_error_kind(disk)])
        .inc();
    crit!(
        "fail-fast: disk probe timed out, shutting down to avoid serving on an unhealthy disk";
        "disk" => disk.name(),
        "timeout" => ?timeout,
        "current_probe_elapsed" => ?current_probe_elapsed,
        "last_sync_success_elapsed" => ?last_sync_success_elapsed,
    );
    true
}

/// Detects one class of TiKV "zombie" failure and chooses to hard-exit.
///
/// The target failure mode here is: the process is still alive and may still
/// hold Raft leadership, but it is no longer truly serviceable because disk IO
/// on the critical path has stopped making progress. In that state, staying
/// alive can prolong unavailability and hide the failure from orchestration.
///
/// The monitor has two parts:
/// 1. One dedicated probe thread periodically runs blocking `write + sync_all`
///    probes, alternating raft -> kv when both disks are configured.
/// 2. A separate checker worker periodically inspects the probe state, so a
///    hung blocking probe cannot prevent fail-fast from being triggered.
///
/// The checker applies that same rule directly. Raft may veto a probe timeout
/// if real raft-log appends have still succeeded recently, and kv may veto a
/// probe timeout if a real kv WAL sync has still succeeded recently.
///
/// Once any disk meets the fail-fast rule, TiKV emits the final error, gives
/// the async logger a brief best-effort flush window, and then panics so the
/// panic hook can emit the fatal crash log before the process dies. The scope
/// is intentionally narrow today: disk hang is the first zombie-failure
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
        last_kv_sync_success_at_millis: Arc<AtomicU64>,
    ) -> std::io::Result<Self> {
        let stop = Arc::new(AtomicBool::new(false));
        let raft_probe = ProbeRunner::new(
            raft_probe_dir.join(DiskKind::Raft.probe_filename()),
            PROBE_PAYLOAD,
        );
        let kv_probe = kv_probe_dir
            .map(|dir| ProbeRunner::new(dir.join(DiskKind::Kv.probe_filename()), PROBE_PAYLOAD));

        spawn_probe_thread(
            "fail-fast-probe",
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

            let raft_should_fail_fast = should_fail_fast(
                Some(&raft_probe_for_check),
                DiskKind::Raft,
                timeout,
                &last_raft_append_success_at_millis,
            );
            let kv_should_fail_fast = should_fail_fast(
                kv_probe_for_check.as_ref(),
                DiskKind::Kv,
                timeout,
                &last_kv_sync_success_at_millis,
            );
            if raft_should_fail_fast || kv_should_fail_fast {
                logger::panic_after_best_effort_flush(
                    FAIL_FAST_LOG_FLUSH_TIMEOUT,
                    "fail-fast: TiKV self-killed due to disk unavailability",
                );
            }
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
        let kv_path = self.kv_probe.as_ref().map(ProbeRunner::path);
        for path in std::iter::once(self.raft_probe.path()).chain(kv_path.into_iter()) {
            if let Err(e) = fs::remove_file(path) {
                if e.kind() != std::io::ErrorKind::NotFound {
                    warn!("failed to remove fail-fast probe file"; "err" => ?e, "path" => %path.display());
                }
            }
        }
    }
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

// Returns how long it has been since the last successful sync-backed progress
// signal on a disk, using the same monotonic raw clock as the producer side.
fn last_sync_success_elapsed(last_sync_success_at_millis: &AtomicU64) -> Option<Duration> {
    let last = last_sync_success_at_millis.load(Ordering::Relaxed);
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
        let raft_probe = ProbeRunner::new(
            dir.path().join(DiskKind::Raft.probe_filename()),
            PROBE_PAYLOAD,
        );
        let duration = raft_probe.probe_once().unwrap();
        assert!(duration > Duration::ZERO);
        assert!(raft_probe.path().exists());

        let kv_probe = ProbeRunner::new(
            dir.path().join(DiskKind::Kv.probe_filename()),
            PROBE_PAYLOAD,
        );
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
    fn test_should_fail_fast_on_timeout() {
        let dir = tempdir().unwrap();
        let probe = ProbeRunner::new(
            dir.path().join(DiskKind::Raft.probe_filename()),
            PROBE_PAYLOAD,
        );
        let timeout = Duration::from_millis(10);

        assert!(probe.try_start_probe());
        std::thread::sleep(timeout + Duration::from_millis(5));
        assert!(should_fail_fast(
            Some(&probe),
            DiskKind::Raft,
            timeout,
            &AtomicU64::new(0),
        ));
    }

    #[test]
    fn test_should_fail_fast_on_repeated_failures_after_prior_success() {
        let dir = tempdir().unwrap();
        let probe = ProbeRunner::new(
            dir.path().join(DiskKind::Raft.probe_filename()),
            PROBE_PAYLOAD,
        );
        let timeout = Duration::from_millis(10);

        assert!(!should_fail_fast_on_repeated_failures(&probe, timeout));
        probe.finish_probe_success();
        assert!(probe.try_start_probe());
        probe.finish_probe_failure();
        std::thread::sleep(timeout + Duration::from_millis(5));
        assert!(should_fail_fast_on_repeated_failures(&probe, timeout));
    }

    #[test]
    fn test_should_fail_fast_repeated_failures_respect_raft_progress_veto() {
        let dir = tempdir().unwrap();
        let probe = ProbeRunner::new(
            dir.path().join(DiskKind::Raft.probe_filename()),
            PROBE_PAYLOAD,
        );
        let timeout = Duration::from_millis(10);

        probe.finish_probe_success();
        assert!(probe.try_start_probe());
        probe.finish_probe_failure();
        std::thread::sleep(timeout + Duration::from_millis(5));

        assert!(!should_fail_fast(
            Some(&probe),
            DiskKind::Raft,
            timeout,
            &AtomicU64::new(timespec_to_ns(monotonic_raw_now()) / 1_000_000),
        ));
        assert!(should_fail_fast(
            Some(&probe),
            DiskKind::Raft,
            timeout,
            &AtomicU64::new(
                (timespec_to_ns(monotonic_raw_now()) / 1_000_000)
                    .saturating_sub(timeout.as_millis() as u64 + 1)
            ),
        ));
        assert!(should_fail_fast(
            Some(&probe),
            DiskKind::Raft,
            timeout,
            &AtomicU64::new(0),
        ));
    }

    #[test]
    fn test_should_fail_fast_respects_kv_sync_veto() {
        let dir = tempdir().unwrap();
        let kv_probe = ProbeRunner::new(
            dir.path().join(DiskKind::Kv.probe_filename()),
            PROBE_PAYLOAD,
        );
        let timeout = Duration::from_millis(10);

        assert!(kv_probe.try_start_probe());
        std::thread::sleep(timeout + Duration::from_millis(5));

        assert!(!should_fail_fast(
            Some(&kv_probe),
            DiskKind::Kv,
            timeout,
            &AtomicU64::new(timespec_to_ns(monotonic_raw_now()) / 1_000_000),
        ));
        assert!(should_fail_fast(
            Some(&kv_probe),
            DiskKind::Kv,
            timeout,
            &AtomicU64::new(
                (timespec_to_ns(monotonic_raw_now()) / 1_000_000)
                    .saturating_sub(timeout.as_millis() as u64 + 1)
            ),
        ));
        assert!(should_fail_fast(
            Some(&kv_probe),
            DiskKind::Kv,
            timeout,
            &AtomicU64::new(0),
        ));
    }
}
