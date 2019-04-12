// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

use std::sync::Mutex;

use callgrind::CallgrindClientRequest;

#[derive(Debug, PartialEq)]
enum Profiler {
    None,
    GPerfTools,
    CallGrind,
}

lazy_static::lazy_static! {
    #[derive(Debug)]
    static ref ACTIVE_PROFILER: Mutex<Profiler> = Mutex::new(Profiler::None);
}

/// Start profiling.
///
/// When `profiling` feature is not enabled, this function will do nothing and there is totally
/// zero cost.
///
/// When running in Callgrind, Callgrind instrumentation will be started
/// (`CALLGRIND_START_INSTRUMENTATION`). Otherwise, the CPU Profiler will be started and profile
/// will be generated to the file specified by `name`.
// TODO: Better multi-thread support.
#[inline]
pub fn start(name: impl AsRef<str>) {
    let mut profiler = ACTIVE_PROFILER.lock().unwrap();

    // Profiling in progress.
    if *profiler != Profiler::None {
        return;
    }

    if valgrind_request::running_on_valgrind() != 0 {
        *profiler = Profiler::CallGrind;
        CallgrindClientRequest::start();
    } else {
        *profiler = Profiler::GPerfTools;
        cpuprofiler::PROFILER
            .lock()
            .unwrap()
            .start(name.as_ref())
            .unwrap();
    }
}

/// Stop profiling if it is started previously.
///
/// When `profiling` feature is not enabled, this function will do nothing and there is totally
/// zero cost.
#[inline]
pub fn stop() {
    let mut profiler = ACTIVE_PROFILER.lock().unwrap();
    match *profiler {
        Profiler::None => {}
        Profiler::CallGrind => {
            CallgrindClientRequest::stop(None);
            *profiler = Profiler::None;
        }
        Profiler::GPerfTools => {
            cpuprofiler::PROFILER.lock().unwrap().stop().unwrap();
            *profiler = Profiler::None;
        }
    }
}
