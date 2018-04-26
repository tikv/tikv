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
use std::marker::PhantomData;
use std::thread;

use crossbeam_channel;
use grpc;
use log::{self, Log, LogMetadata, LogRecord, SetLoggerError};
use time;

pub use log::LogLevelFilter;

const ENABLED_TARGETS: &[&str] = &[
    "tikv::",
    "tests::",
    "benches::",
    "integrations::",
    "failpoints::",
    "raft::",
];
const LOGGER_CHANNEL_BUFFER: usize = 4096;

pub fn init_log<W: LogWriter + Sync + Send + 'static>(
    writer: W,
    level: LogLevelFilter,
) -> Result<(), SetLoggerError> {
    log::set_logger(|filter| {
        filter.set(level);
        grpc::redirect_log();
        Box::new(Logger::new(level, writer, false))
    })
}

pub fn init_log_for_tikv_only<W: LogWriter + Sync + Send + 'static>(
    writer: W,
    level: LogLevelFilter,
) -> Result<(), SetLoggerError> {
    log::set_logger(|filter| {
        filter.set(level);
        grpc::redirect_log();
        Box::new(Logger::new(level, writer, true))
    })
}

pub trait LogWriter {
    fn write(&self, args: String);
}

struct Logger<W>
where
    W: LogWriter + Send + Sync,
{
    level: LogLevelFilter,
    tikv_only: bool,
    /// When the logger is sent a `None` it will return cleanly, allowing the thread to join.
    sender: crossbeam_channel::Sender<Option<String>>,
    handle: Option<thread::JoinHandle<()>>,
    marker: PhantomData<W>,
}

impl<W> Logger<W>
where
    W: 'static + LogWriter + Send + Sync,
{
    fn new(level: LogLevelFilter, writer: W, tikv_only: bool) -> Self {
        let (sender, receiver): (_, crossbeam_channel::Receiver<Option<String>>) =
            crossbeam_channel::bounded(LOGGER_CHANNEL_BUFFER);
        let handle = Some(thread::spawn(move || match receiver.recv() {
            Ok(Some(message)) => {
                writer.write(message);
            }
            Ok(None) => return,
            _ => panic!("Logging thread unexpectedly became disconnected."),
        }));
        Self {
            sender,
            handle,
            marker: PhantomData,
            level,
            tikv_only,
        }
    }
}

impl<W> Log for Logger<W>
where
    W: LogWriter + Send + Sync,
{
    fn enabled(&self, meta: &LogMetadata) -> bool {
        meta.level() <= self.level
    }

    fn log(&self, record: &LogRecord) {
        if self.tikv_only
            && ENABLED_TARGETS
                .iter()
                .all(|target| !record.target().starts_with(target))
        {
            return;
        };

        if self.enabled(record.metadata()) {
            let t = time::now();
            let time_str = time::strftime("%Y/%m/%d %H:%M:%S.%f", &t).unwrap();
            let message = format!(
                "{} {}:{}: [{}] {}\n",
                &time_str[..time_str.len() - 6],
                record.location().file().rsplit('/').nth(0).unwrap(),
                record.location().line(),
                record.level(),
                record.args()
            );

            let _ = self.sender.send(Some(message));
        }
    }
}

impl<W> Drop for Logger<W>
where
    W: LogWriter + Send + Sync,
{
    fn drop(&mut self) {
        // Make sure we stop the logger thread cleanly.
        let _ = self.sender.send(None);
        let _ = self.handle.take().map(|h| h.join());
    }
}

pub struct StderrLogger;

impl LogWriter for StderrLogger {
    #[inline]
    fn write(&self, args: String) {
        let _ = io::stderr().write(args.as_bytes());
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
