// Copyright 2017 PingCAP, Inc.
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

use rocksdb::DB;
use std::sync::{Arc, RwLock};
use super::super::engine_metrics::*;
use std::thread::{self, JoinHandle, Builder};
use std::io;
use std::time;

pub struct MetricsFlusher {
    engine: Arc<DB>,
    flag: Arc<RwLock<bool>>,
    handle: Option<JoinHandle<()>>,
}

impl MetricsFlusher {
    pub fn new(engine: Arc<DB>) -> MetricsFlusher {
        MetricsFlusher {
            engine: engine,
            flag: Arc::new(RwLock::new(false)),
            handle: None,
        }
    }

    pub fn start(&mut self) -> Result<(), io::Error> {
        let db = self.engine.clone();
        let flag = self.flag.clone();
        let h = try!(Builder::new()
            .name(thd_name!("flush metrics"))
            .spawn(move || {
                loop {
                    if *(flag.read().unwrap()) {
                        break;
                    }
                    for t in ENGINE_TICKER_TYPES {
                        let v = db.get_statistics_ticker_count(*t);
                        flush_engine_ticker_metrics(*t, v);
                    }
                    for t in ENGINE_HIST_TYPES {
                        if let Some(v) = db.get_statistics_histogram(*t) {
                            flush_engine_histogram_metrics(*t, v);
                        }
                    }
                    thread::sleep(time::Duration::from_millis(1000));
                }
            }));

        self.handle = Some(h);
        Ok(())
    }

    pub fn stop(&mut self) -> Option<::std::thread::JoinHandle<()>> {
        *(self.flag.write().unwrap()) = true;
        if self.handle.is_none() {
            return None;
        }
        self.handle.take()
    }
}
