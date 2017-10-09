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
use raftstore::store::Engines;
use util::rocksdb::engine_metrics::*;
use std::thread::{Builder, JoinHandle};
use std::io;
use std::sync::mpsc::{self, Sender};
use std::time::Duration;

pub const DEFAULT_FLUSER_INTERVAL: u64 = 10000;

pub struct MetricsFlusher {
    engines: Engines,
    handle: Option<JoinHandle<()>>,
    sender: Option<Sender<bool>>,
    interval: Duration,
}

impl MetricsFlusher {
    pub fn new(engines: Engines, interval: Duration) -> MetricsFlusher {
        MetricsFlusher {
            engines: engines,
            handle: None,
            sender: None,
            interval: interval,
        }
    }

    pub fn start(&mut self) -> Result<(), io::Error> {
        let db = self.engines.kv_engine.clone();
        let raft_db = self.engines.raft_engine.clone();
        let (tx, rx) = mpsc::channel();
        let interval = self.interval;
        self.sender = Some(tx);
        let h = Builder::new()
            .name(thd_name!("rocksb-metrics-flusher"))
            .spawn(move || {
                while let Err(mpsc::RecvTimeoutError::Timeout) = rx.recv_timeout(interval) {
                    flush_metrics(&db, "kv");
                    flush_metrics(&raft_db, "raft");
                }
            })?;

        self.handle = Some(h);
        Ok(())
    }

    pub fn stop(&mut self) {
        let h = self.handle.take();
        if h.is_none() {
            return;
        }
        drop(self.sender.take().unwrap());
        if let Err(e) = h.unwrap().join() {
            error!("join metrics flusher failed {:?}", e);
            return;
        }
    }
}

fn flush_metrics(db: &DB, name: &str) {
    for t in ENGINE_TICKER_TYPES {
        let v = db.get_and_reset_statistics_ticker_count(*t);
        flush_engine_ticker_metrics(*t, v, name);
    }
    for t in ENGINE_HIST_TYPES {
        if let Some(v) = db.get_statistics_histogram(*t) {
            flush_engine_histogram_metrics(*t, v, name);
        }
    }
    flush_engine_properties(db, name);
}

#[cfg(test)]
mod tests {
    use tempdir::TempDir;
    use std::path::Path;
    use std::time::Duration;
    use std::sync::Arc;
    use super::*;
    use rocksdb::{ColumnFamilyOptions, DBOptions};
    use util::rocksdb::{self, CFOptions};
    use storage::{CF_DEFAULT, CF_LOCK, CF_WRITE};
    use std::thread::sleep;

    #[test]
    fn test_metrics_flusher() {
        let path = TempDir::new("_test_metrics_flusher").unwrap();
        let raft_path = path.path().join(Path::new("raft"));
        let db_opt = DBOptions::new();
        let cf_opts = ColumnFamilyOptions::new();
        let cfs_opts = vec![
            CFOptions::new(CF_DEFAULT, rocksdb::ColumnFamilyOptions::new()),
            CFOptions::new(CF_LOCK, rocksdb::ColumnFamilyOptions::new()),
            CFOptions::new(CF_WRITE, cf_opts),
        ];
        let engine = Arc::new(
            rocksdb::new_engine_opt(path.path().to_str().unwrap(), db_opt, cfs_opts).unwrap(),
        );

        let cfs_opts = vec![CFOptions::new(CF_DEFAULT, ColumnFamilyOptions::new())];
        let raft_engine = Arc::new(
            rocksdb::new_engine_opt(raft_path.to_str().unwrap(), DBOptions::new(), cfs_opts)
                .unwrap(),
        );
        let engines = Engines::new(engine, raft_engine);
        let mut metrics_flusher = MetricsFlusher::new(engines, Duration::from_millis(100));

        if let Err(e) = metrics_flusher.start() {
            error!("failed to start metrics flusher, error = {:?}", e);
        }

        let rtime = Duration::from_millis(300);
        sleep(rtime);

        metrics_flusher.stop();
    }
}
