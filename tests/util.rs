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
use std::io::{self, Write};
use std::env;
use std::fmt::Arguments;
use std::fs::File;
use std::sync::Mutex;

use tikv::util;
use tikv::util::logger::{self, LogWriter};


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

/// A logger that add a test case tag before each line of log.
struct CaseTraceLogger {
    f: Option<Mutex<File>>,
}

impl LogWriter for CaseTraceLogger {
    fn write(&self, args: Arguments) {
        let tag = util::get_tag_from_thread_name().unwrap_or("".into());
        let _ = if let Some(ref out) = self.f {
            let mut w = out.lock().unwrap();
            write!(w, "{} {}", tag, args)
        } else {
            let mut w = io::stderr();
            write!(w, "{} {}", tag, args)
        };
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
    let level = logger::get_level_by_string(&env::var("LOG_LEVEL").unwrap_or("debug".to_owned()));
    let writer = output.map(|f| Mutex::new(File::create(f).unwrap()));
    // we don't mind set it multiple times.
    let _ = logger::init_log(CaseTraceLogger { f: writer }, level);
}
