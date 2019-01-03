// Copyright 2018 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

use std::env;
use std::fs::File;
use std::io;
use std::io::prelude::*;
use std::sync::Mutex;

use slog::{self, Drain, OwnedKVList, Record};
use slog_scope::GlobalLoggerGuard;
use time;

use tikv;

/// A logger that add a test case tag before each line of log.
struct CaseTraceLogger {
    f: Option<Mutex<File>>,
}

impl Drain for CaseTraceLogger {
    type Ok = ();
    type Err = slog::Never;
    fn log(&self, record: &Record, _: &OwnedKVList) -> Result<Self::Ok, Self::Err> {
        let tag = tikv::util::get_tag_from_thread_name().map_or_else(|| "".into(), |s| s + " ");

        let t = time::now();
        let time_str = time::strftime("%Y/%m/%d %H:%M:%S.%f", &t).unwrap();
        // todo allow formatter to be configurable.
        let message = format!(
            "{}{} {}:{}: [{}] {}\n",
            tag,
            &time_str[..time_str.len() - 6],
            record.file().rsplit('/').nth(0).unwrap(),
            record.line(),
            record.level(),
            record.msg(),
        );

        if let Some(ref out) = self.f {
            let mut w = out.lock().unwrap();
            let _ = w.write(message.as_bytes());
        } else {
            let mut w = io::stderr();
            let _ = w.write(message.as_bytes());
        }
        Ok(())
    }
}

impl Drop for CaseTraceLogger {
    fn drop(&mut self) {
        if let Some(ref w) = self.f {
            w.lock().unwrap().flush().unwrap();
        }
    }
}

// A help function to initial logger.
pub fn init_log_for_test() -> GlobalLoggerGuard {
    let output = env::var("LOG_FILE").ok();
    let level = tikv::util::logger::get_level_by_string(
        &env::var("LOG_LEVEL").unwrap_or_else(|_| "debug".to_owned()),
    ).unwrap();
    let writer = output.map(|f| Mutex::new(File::create(f).unwrap()));
    // we don't mind set it multiple times.
    let drain = CaseTraceLogger { f: writer };

    // Collects following targes.
    const ENABLED_TARGETS: &[&str] = &[
        "tikv::",
        "tests::",
        "benches::",
        "integrations::",
        "failpoints::",
        "raft::",
        // Collects logs for test components.
        "test_",
    ];
    let filtered = drain.filter(|record| {
        ENABLED_TARGETS
            .iter()
            .any(|target| record.module().starts_with(target))
    });

    // CaseTraceLogger relies on test's thread name, however slog_async has
    // its own thread, and the name is "".
    // TODO: Enable the slog_async when the [Custom test frameworks][1] is mature,
    //       and hook the slog_async logger to every test cases.
    //
    // [1]: https://github.com/rust-lang/rfcs/blob/master/text/2318-custom-test-frameworks.md
    tikv::util::logger::init_log(
        filtered, level, false, // disable async drainer
        true,  // init std log
    ).unwrap()
}
