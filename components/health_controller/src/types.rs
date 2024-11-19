// Copyright 2024 TiKV Project Authors. Licensed under Apache-2.0.

use std::{fmt::Debug, u64};

/// Represent the duration of all stages of raftstore recorded by one
/// inspecting.
#[derive(Default, Debug)]
pub struct RaftstoreDuration {
    pub store_wait_duration: Option<std::time::Duration>,
    pub store_process_duration: Option<std::time::Duration>,
    pub store_write_duration: Option<std::time::Duration>,
    pub store_commit_duration: Option<std::time::Duration>,
    pub apply_wait_duration: Option<std::time::Duration>,
    pub apply_process_duration: Option<std::time::Duration>,
    pub disk_health_check_duration: Option<std::time::Duration>,
}

impl RaftstoreDuration {
    #[inline]
    pub fn sum(&self) -> std::time::Duration {
        self.delays_on_disk_io(true) + self.delays_on_net_io()
    }

    #[inline]
    /// Returns the delayed duration on Disk I/O.
    pub fn delays_on_disk_io(&self, include_wait_duration: bool) -> std::time::Duration {
        let duration = self.store_process_duration.unwrap_or_default()
            + self.store_write_duration.unwrap_or_default()
            + self.apply_process_duration.unwrap_or_default()
            + self.disk_health_check_duration.unwrap_or_default();
        if include_wait_duration {
            duration
                + self.store_wait_duration.unwrap_or_default()
                + self.apply_wait_duration.unwrap_or_default()
        } else {
            duration
        }
    }

    #[inline]
    /// Returns the delayed duration on Network I/O.
    ///
    /// Normally, it can be reflected by the duraiton on
    /// `store_commit_duraiton`.
    pub fn delays_on_net_io(&self) -> std::time::Duration {
        // The `store_commit_duration` serves as an indicator for latency
        // during the duration of transferring Raft logs to peers and appending
        // logs. In most scenarios, instances of latency fluctuations in the
        // network are reflected by this duration. Hence, it is selected as a
        // representative of network latency.
        self.store_commit_duration.unwrap_or_default()
    }
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum InspectFactor {
    RaftDisk = 0,
    KvDisk,
    // TODO: Add more factors, like network io.
}

impl InspectFactor {
    pub fn as_str(&self) -> &str {
        match *self {
            InspectFactor::RaftDisk => "raft",
            InspectFactor::KvDisk => "kvdb",
        }
    }
}

/// Used to inspect the latency of all stages of raftstore.
pub struct LatencyInspector {
    id: u64,
    duration: RaftstoreDuration,
    cb: Box<dyn FnOnce(u64, RaftstoreDuration) + Send>,
}

impl Debug for LatencyInspector {
    fn fmt(&self, fmt: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            fmt,
            "LatencyInspector: id {} duration: {:?}",
            self.id, self.duration
        )
    }
}

impl LatencyInspector {
    pub fn new(id: u64, cb: Box<dyn FnOnce(u64, RaftstoreDuration) + Send>) -> Self {
        Self {
            id,
            cb,
            duration: RaftstoreDuration::default(),
        }
    }

    pub fn record_store_wait(&mut self, duration: std::time::Duration) {
        self.duration.store_wait_duration = Some(duration);
    }

    pub fn record_store_process(&mut self, duration: std::time::Duration) {
        self.duration.store_process_duration = Some(duration);
    }

    pub fn record_store_write(&mut self, duration: std::time::Duration) {
        self.duration.store_write_duration = Some(duration);
    }

    pub fn record_store_commit(&mut self, duration: std::time::Duration) {
        self.duration.store_commit_duration = Some(duration);
    }

    pub fn record_apply_wait(&mut self, duration: std::time::Duration) {
        self.duration.apply_wait_duration = Some(duration);
    }

    pub fn record_apply_process(&mut self, duration: std::time::Duration) {
        self.duration.apply_process_duration = Some(duration);
    }

    pub fn record_disk_health_check(&mut self, duration: std::time::Duration) {
        self.duration.disk_health_check_duration = Some(duration);
    }

    /// Call the callback.
    pub fn finish(self) {
        (self.cb)(self.id, self.duration);
    }
}
