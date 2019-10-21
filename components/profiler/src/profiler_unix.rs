// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

#[cfg(target_os = "linux")]
use gperftools_static as gperftools;

use std::sync::Mutex;

use callgrind::CallgrindClientRequest;

use path_abs::PathInfo;
use std::io;

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

/// Start profiling. Returns false if failed, i.e. there is already a profiling in progress.
///
/// When `profiling` feature is not enabled, this function will do nothing and there is totally
/// zero cost.
///
/// When running in Callgrind, Callgrind instrumentation will be started
/// (`CALLGRIND_START_INSTRUMENTATION`). Otherwise, the CPU Profiler will be started and profile
/// will be generated to the file specified by `name`.
// TODO: Better multi-thread support.
#[inline]
pub fn start(name: impl AsRef<str>) -> io::Result<bool> {
    let mut profiler = ACTIVE_PROFILER.lock().unwrap();

    // Profiling in progress.
    if *profiler != Profiler::None {
        return Ok(false);
    }

    if valgrind_request::running_on_valgrind() != 0 {
        *profiler = Profiler::CallGrind;
        CallgrindClientRequest::start();
    } else {
        let path = path_abs::PathAbs::new(name.as_ref())?;
        let path_str = match path.to_str() {
            Some(path) => path,
            None => {
                return Err(io::Error::new(
                    io::ErrorKind::Other,
                    "check for UTF-8 validity failed",
                ))
            }
        };

        *profiler = Profiler::GPerfTools;
        gperftools::PROFILER
            .lock()
            .unwrap()
            .start(path_str)
            .unwrap();
    }

    Ok(true)
}

/// Stop profiling. Returns false if failed, i.e. there is no profiling in progress.
///
/// When `profiling` feature is not enabled, this function will do nothing and there is totally
/// zero cost.
#[inline]
pub fn stop() -> bool {
    let mut profiler = ACTIVE_PROFILER.lock().unwrap();
    match *profiler {
        Profiler::None => false,
        Profiler::CallGrind => {
            CallgrindClientRequest::stop(None);
            *profiler = Profiler::None;
            true
        }
        Profiler::GPerfTools => {
            gperftools::PROFILER.lock().unwrap().stop().unwrap();
            *profiler = Profiler::None;
            true
        }
    }
}
