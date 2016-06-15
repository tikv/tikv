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

use rand::{self, Rng, ThreadRng};
use std::ops::RangeFrom;
use std::io::{self, Write};
use std::env;
use std::fs::File;
use std::sync::Mutex;

use tikv::storage::mvcc::TEST_TS_BASE;
use tikv::util::{self, logger};

use time;
use log::{self, LogLevelFilter, Log, LogMetadata, LogRecord};

/// A random generator of kv.
/// Every iter should be taken in µs. See also `benches::bench_kv_iter`.
pub struct KvGenerator {
    key_len: usize,
    value_len: usize,
    rng: ThreadRng,
}

impl KvGenerator {
    pub fn new(key_len: usize, value_len: usize) -> KvGenerator {
        KvGenerator {
            key_len: key_len,
            value_len: value_len,
            rng: rand::thread_rng(),
        }
    }
}

impl Iterator for KvGenerator {
    type Item = (Vec<u8>, Vec<u8>);

    fn next(&mut self) -> Option<(Vec<u8>, Vec<u8>)> {
        let mut k = vec![0; self.key_len];
        self.rng.fill_bytes(&mut k);
        let mut v = vec![0; self.value_len];
        self.rng.fill_bytes(&mut v);

        Some((k, v))
    }
}

/// Generate n pair of kvs.
#[allow(dead_code)]
pub fn generate_random_kvs(n: usize, key_len: usize, value_len: usize) -> Vec<(Vec<u8>, Vec<u8>)> {
    let kv_generator = KvGenerator::new(key_len, value_len);
    kv_generator.take(n).collect()
}

pub struct TsGenerator {
    ts_pool: RangeFrom<u64>,
}

impl TsGenerator {
    pub fn new() -> TsGenerator {
        TsGenerator { ts_pool: TEST_TS_BASE.. }
    }

    pub fn gen(&mut self) -> u64 {
        self.ts_pool.next().unwrap()
    }
}

/// A logger that add a test case tag before each line of log.
struct CaseTraceLogger {
    level: LogLevelFilter,
    f: Option<Mutex<File>>,
}

impl Log for CaseTraceLogger {
    fn enabled(&self, meta: &LogMetadata) -> bool {
        meta.level() <= self.level
    }

    fn log(&self, record: &LogRecord) {
        if self.enabled(record.metadata()) {
            let tag = util::get_tag_from_thread_name().unwrap_or("".into());
            let t = time::now();
            // TODO allow formatter to be configurable.
            let _ = if let Some(ref out) = self.f {
                write!(out.lock().unwrap(),
                       "{} {},{:03} {}:{} - {:5} - {}\n",
                       tag,
                       time::strftime("%Y-%m-%d %H:%M:%S", &t).unwrap(),
                       t.tm_nsec / 1000_000,
                       record.location().file().rsplit('/').nth(0).unwrap(),
                       record.location().line(),
                       record.level(),
                       record.args())
            } else {
                write!(io::stderr(),
                       "{} {},{:03} {}:{} - {:5} - {}\n",
                       tag,
                       time::strftime("%Y-%m-%d %H:%M:%S", &t).unwrap(),
                       t.tm_nsec / 1000_000,
                       record.location().file().rsplit('/').nth(0).unwrap(),
                       record.location().line(),
                       record.level(),
                       record.args())
            };
        }
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
pub fn init_log() {
    let output = env::var("LOG_FILE").ok();

    // don't treat stderr as a filename.
    let output = output.iter().filter(|f| f.to_lowercase() != "stderr").next();
    let level = logger::get_level_by_string(&env::var("LOG_LEVEL").unwrap_or("debug".to_owned()));

    // we don't mind set it multiple times.
    let _ = log::set_logger(move |filter| {
        filter.set(level);
        let writer = output.map(|f| Mutex::new(File::create(f).unwrap()));
        Box::new(CaseTraceLogger {
            level: level,
            f: writer,
        })
    });
}
