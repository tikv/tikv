// Copyright 2026 TiKV Project Authors. Licensed under Apache-2.0.

use std::{
    fs::OpenOptions,
    io::Write,
    path::{Path, PathBuf},
    sync::Arc,
    time::Duration,
};

use parking_lot::Mutex;
use tikv_util::time::Instant;

/// A tiny disk probe that performs `write + sync` on a dedicated file.
///
/// It is intentionally minimal because it is used by multiple subsystems:
/// - KV disk latency inspection (`disk_check`)
/// - Raft disk hang fail-fast (`fail_fast`)
pub(crate) fn write_sync_once(path: &Path, payload: &[u8]) -> std::io::Result<Duration> {
    let start = Instant::now();
    let mut file = OpenOptions::new()
        .create(true)
        .write(true)
        .truncate(true)
        .open(path)?;
    file.write_all(payload)?;
    file.sync_all()?;
    Ok(start.saturating_elapsed())
}

#[derive(Debug)]
struct ProbeState {
    last_success_at: Instant,
    current_probe_started_at: Option<Instant>,
}

impl ProbeState {
    fn new() -> Self {
        Self {
            last_success_at: Instant::now(),
            current_probe_started_at: None,
        }
    }

    fn mark_probe_start(&mut self) {
        self.current_probe_started_at = Some(Instant::now());
    }

    fn try_start_probe(&mut self) -> bool {
        if self.current_probe_started_at.is_some() {
            return false;
        }
        self.mark_probe_start();
        true
    }

    fn finish_probe_success(&mut self) {
        self.last_success_at = Instant::now();
        self.current_probe_started_at = None;
    }

    fn finish_probe_failure(&mut self) {
        self.current_probe_started_at = None;
    }

    fn current_probe_elapsed(&self) -> Option<Duration> {
        self.current_probe_started_at
            .map(|start| start.saturating_elapsed())
    }

    fn time_since_last_success(&self) -> Duration {
        self.last_success_at.saturating_elapsed()
    }
}

#[derive(Clone, Debug)]
pub(crate) struct ProbeRunner {
    path: PathBuf,
    payload: &'static [u8],
    state: Arc<Mutex<ProbeState>>,
}

impl ProbeRunner {
    pub(crate) fn new(path: PathBuf, payload: &'static [u8]) -> Self {
        Self {
            path,
            payload,
            state: Arc::new(Mutex::new(ProbeState::new())),
        }
    }

    pub(crate) fn path(&self) -> &Path {
        &self.path
    }

    pub(crate) fn probe_once(&self) -> std::io::Result<Duration> {
        write_sync_once(self.path(), self.payload)
    }

    pub(crate) fn try_start_probe(&self) -> bool {
        self.state.lock().try_start_probe()
    }

    pub(crate) fn finish_probe_success(&self) {
        self.state.lock().finish_probe_success();
    }

    pub(crate) fn finish_probe_failure(&self) {
        self.state.lock().finish_probe_failure();
    }

    pub(crate) fn current_probe_elapsed(&self) -> Option<Duration> {
        self.state.lock().current_probe_elapsed()
    }

    pub(crate) fn time_since_last_success(&self) -> Duration {
        self.state.lock().time_since_last_success()
    }
}

#[cfg(test)]
mod tests {
    use tempfile::tempdir;

    use super::*;

    #[test]
    fn test_probe_runner_probe_once() {
        let dir = tempdir().unwrap();
        let runner = ProbeRunner::new(dir.path().join("probe.tmp"), b"payload");

        let duration = runner.probe_once().unwrap();
        assert!(duration > Duration::ZERO);
        assert!(runner.path().exists());
    }

    #[test]
    fn test_probe_runner_state() {
        let dir = tempdir().unwrap();
        let runner = ProbeRunner::new(dir.path().join("probe.tmp"), b"payload");

        assert!(runner.current_probe_elapsed().is_none());
        assert!(runner.try_start_probe());
        assert!(!runner.try_start_probe());
        assert!(runner.current_probe_elapsed().is_some());
        runner.finish_probe_failure();
        assert!(runner.current_probe_elapsed().is_none());

        assert!(runner.try_start_probe());
        assert!(runner.current_probe_elapsed().is_some());
        runner.finish_probe_success();
        assert!(runner.current_probe_elapsed().is_none());
        assert!(runner.time_since_last_success() <= Duration::from_secs(1));
    }
}
