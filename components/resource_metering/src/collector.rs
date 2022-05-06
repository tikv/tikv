// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

use std::sync::Arc;

use crate::RawRecords;

/// `Collector` is used to connect [Recorder] and [Reporter].
///
/// The `Recorder` is mainly responsible for collecting data, and it is
/// only responsible for passing the collected data to the `Collector`.
/// The `Recorder` does not know anything about "scheduling" or "uploading".
///
/// Typically, constructing a `Collector` instance requires passing in a
/// [Scheduler], The `Collector` will send the data passed by the recorder
/// to the `Scheduler` for processing.
///
/// `Reporter` implements [Runnable] and [RunnableWithTimer], aggregates the
/// data sent by the `Collector` internally, and reports it regularly through RPC.
///
/// [Recorder]: crate::recorder::Recorder
/// [Reporter]: crate::reporter::Reporter
/// [Scheduler]: tikv_util::worker::Scheduler
/// [Runnable]: tikv_util::worker::Runnable
/// [RunnableWithTimer]: tikv_util::worker::RunnableWithTimer
pub trait Collector: Send {
    fn collect(&self, records: Arc<RawRecords>);
}
