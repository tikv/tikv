// Copyright 2020 TiKV Project Authors. Licensed under Apache-2.0.

#[derive(Copy, Clone, Debug, PartialEq)]
pub enum PerfLevel {
    Uninitialized,
    Disable,
    EnableCount,
    EnableTimeExceptForMutex,
    EnableTimeAndCPUTimeExceptForMutex,
    EnableTime,
    OutOfBounds,
}

/// Extensions for measuring engine performance.
///
/// A PerfContext is created with a specific measurement level,
/// and a 'kind' which represents wich tikv subsystem measurements are being
/// collected for.
///
/// In rocks, `PerfContext` uses global state, and does not require
/// access through an engine. Thus perf data is not per-engine.
/// This doesn't seem like a reasonable assumption for engines generally,
/// so this abstraction follows the existing pattern in this crate and
/// requires `PerfContext` to be accessed through the engine.
pub trait PerfContextExt {
    type PerfContext: PerfContext;

    fn get_perf_context(&self, level: PerfLevel, kind: PerfContextKind) -> Self::PerfContext;
}

/// The raftstore subsystem the PerfContext is being created for.
///
/// This is a leaky abstraction that supports the encapsulation of metrics
/// reporting by the two raftstore subsystems that use `report_metrics`.
#[derive(Eq, PartialEq, Copy, Clone)]
pub enum PerfContextKind {
    RaftstoreApply,
    RaftstoreStore,
}

/// Reports metrics to prometheus
///
/// For alternate engines, it is reasonable to make `start_observe`
/// and `report_metrics` no-ops.
pub trait PerfContext: Send {
    /// Reinitializes statistics and the perf level
    fn start_observe(&mut self);

    /// Reports the current collected metrics to prometheus
    fn report_metrics(&mut self);
}
