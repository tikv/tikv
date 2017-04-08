// Copyright 2016 PingCAP, Inc.
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

use std::io::{self, Write};
use std::fmt::Arguments;

use time;
use log::{self, Log, LogMetadata, LogRecord, SetLoggerError};

pub use log::LogLevelFilter;

pub fn init_log<W: LogWriter + Sync + Send + 'static>(writer: W,
                                                      level: LogLevelFilter)
                                                      -> Result<(), SetLoggerError> {
    log::set_logger(|filter| {
                        filter.set(level);
                        Box::new(Logger {
                                     level: level,
                                     writer: writer,
                                 })
                    })
}

pub trait LogWriter {
    fn write(&self, args: Arguments);
}

struct Logger<W: LogWriter> {
    level: LogLevelFilter,
    writer: W,
}

impl<W: LogWriter + Sync + Send> Log for Logger<W> {
    fn enabled(&self, meta: &LogMetadata) -> bool {
        meta.level() <= self.level
    }

    fn log(&self, record: &LogRecord) {
        if self.enabled(record.metadata()) {
            let t = time::now();
            let time_str = time::strftime("%Y/%m/%d %H:%M:%S.%f", &t).unwrap();
            // TODO allow formatter to be configurable.
            self.writer
                .write(format_args!("{} {}:{}: [{}] {}\n",
                                    &time_str[..time_str.len() - 6],
                                    record.location().file().rsplit('/').nth(0).unwrap(),
                                    record.location().line(),
                                    record.level(),
                                    record.args()));
        }
    }
}

pub struct StderrLogger;

impl LogWriter for StderrLogger {
    #[inline]
    fn write(&self, args: Arguments) {
        let _ = io::stderr().write_fmt(args);
    }
}

pub fn get_level_by_string(lv: &str) -> LogLevelFilter {
    #![allow(match_same_arms)]
    match &*lv.to_owned().to_lowercase() {
        "trace" => LogLevelFilter::Trace,
        "debug" => LogLevelFilter::Debug,
        "info" => LogLevelFilter::Info,
        "warn" => LogLevelFilter::Warn,
        "error" => LogLevelFilter::Error,
        "off" => LogLevelFilter::Off,
        _ => LogLevelFilter::Info,
    }
}
