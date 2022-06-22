// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

use collections::HashMap;
use tikv_util::sys::thread::Pid;

use crate::{recorder::localstorage::LocalStorage, RawRecords};

pub mod cpu;
pub mod summary;

/// This trait defines a general framework that works at a certain frequency. Typically,
/// it describes the recorder(sampler) framework for a specific resource.
///
/// [Recorder] will maintain a list of sub-recorders, driving all sub-recorders to work
/// according to the behavior described in this trait.
pub trait SubRecorder: Send {
    /// This function is called at a fixed frequency. (A typical frequency is 99hz.)
    ///
    /// The [RawRecords] and [LocalStorage] map of all threads will be passed in through
    /// parameters. We need to collect resources (may be from each `LocalStorage`) and
    /// write them into `RawRecords`.
    ///
    /// The implementation needs to sample the resource in this function (in general).
    ///
    /// [RawRecords]: crate::model::RawRecords
    /// [LocalStorage]: crate::localstorage::LocalStorage
    fn tick(&mut self, _records: &mut RawRecords, _thread_stores: &mut HashMap<Pid, LocalStorage>) {
    }

    /// This function is called every time before reporting to Collector.
    /// The default period is 1 second.
    ///
    /// The [RawRecords] and [LocalStorage] map of all threads will be passed in through parameters.
    /// `usize` is thread_id without platform dependency.
    ///
    /// [RawRecords]: crate::model::RawRecords
    /// [LocalStorage]: crate::localstorage::LocalStorage
    fn collect(
        &mut self,
        _records: &mut RawRecords,
        _thread_stores: &mut HashMap<Pid, LocalStorage>,
    ) {
    }

    /// This function is called when we need to clean up data.
    /// The default period is 5 minutes.
    fn cleanup(
        &mut self,
        _records: &mut RawRecords,
        _thread_stores: &mut HashMap<Pid, LocalStorage>,
    ) {
    }

    /// This function is called before the [Recorder] thread pause.
    fn pause(
        &mut self,
        _records: &mut RawRecords,
        _thread_stores: &mut HashMap<Pid, LocalStorage>,
    ) {
    }

    /// This function is called when the [Recorder] thread is resume execution.
    fn resume(
        &mut self,
        _records: &mut RawRecords,
        _thread_stores: &mut HashMap<Pid, LocalStorage>,
    ) {
    }

    /// This function is called when a new thread accesses thread-local-storage.
    ///
    /// This function exists because the sampling work of `SubRecorder` may need
    /// to be performed on all functions, and `SubRecorder` may wish to maintain
    /// a thread-related data structure by itself.
    fn thread_created(&mut self, _id: Pid, _store: &LocalStorage) {}
}
