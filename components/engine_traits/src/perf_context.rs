// Copyright 2020 TiKV Project Authors. Licensed under Apache-2.0.
use tikv_util::numeric_enum_serializing_mod;
use tracker::TrackerToken;

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

numeric_enum_serializing_mod! {perf_level_serde PerfLevel {
    Uninitialized = 0,
    Disable = 1,
    EnableCount = 2,
    EnableTimeExceptForMutex = 3,
    EnableTimeAndCPUTimeExceptForMutex = 4,
    EnableTime = 5,
    OutOfBounds = 6,
}}

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

/// The subsystem the PerfContext is being created for.
///
/// This is a leaky abstraction that supports the encapsulation of metrics
/// reporting by the subsystems that use PerfContext.
#[derive(Eq, PartialEq, Copy, Clone, Debug)]
pub enum PerfContextKind {
    RaftstoreApply,
    RaftstoreStore,
    /// Commands in tikv::storage, the inner str is the command tag.
    Storage(&'static str),
    /// Coprocessor requests in tikv::coprocessor, the inner str is the request type.
    Coprocessor(&'static str),
}

/// Reports metrics to prometheus
///
/// For alternate engines, it is reasonable to make `start_observe`
/// and `report_metrics` no-ops.
pub trait PerfContext: Send {
    /// Reinitializes statistics and the perf level
    fn start_observe(&mut self);

    /// Reports the current collected metrics to prometheus and trackers
    fn report_metrics(&mut self, trackers: &[TrackerToken]);
}
