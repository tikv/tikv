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
use std::sync::Arc;
use util::rocksdb::engine_metrics::*;
use std::thread::{JoinHandle, Builder};
use std::io;
use std::sync::mpsc::{self, Sender};
use std::time::Duration;

pub struct MetricsFlusher {
    engine: Arc<DB>,
    handle: Option<JoinHandle<()>>,
    sender: Option<Sender<bool>>,
    interval: u64,
}

impl MetricsFlusher {
    pub fn new(engine: Arc<DB>, interval: u64) -> MetricsFlusher {
        MetricsFlusher {
            engine: engine,
            handle: None,
            sender: None,
            interval: interval,
        }
    }

    pub fn start(&mut self) -> Result<(), io::Error> {
        let db = self.engine.clone();
        let (tx, rx) = mpsc::channel();
        let interval = self.interval;
        self.sender = Some(tx);
        let h = try!(Builder::new()
            .name(thd_name!("flush metrics"))
            .spawn(move || {
                loop {
                    match rx.recv_timeout(Duration::from_millis(interval)) {
                        Ok(_) => {
                            break;
                        }
                        Err(mpsc::RecvTimeoutError::Timeout) => {
                            for t in ENGINE_TICKER_TYPES {
                                let v = db.get_statistics_ticker_count(*t);
                                flush_engine_ticker_metrics(*t, v);
                            }
                            for t in ENGINE_HIST_TYPES {
                                if let Some(v) = db.get_statistics_histogram(*t) {
                                    flush_engine_histogram_metrics(*t, v);
                                }
                            }
                            flush_engine_properties(db.clone());
                        }
                        Err(mpsc::RecvTimeoutError::Disconnected) => {
                            error!("The channel's sending half has become disconnected");
                            break;
                        }
                    }
                }
            }));

        self.handle = Some(h);
        Ok(())
    }

    pub fn stop(&mut self) -> Option<::std::thread::JoinHandle<()>> {
        if self.handle.is_none() {
            return None;
        }
        self.sender.as_ref().unwrap().send(true).unwrap();;
        self.handle.take()
    }
}


#[cfg(test)]
mod tests {
    use tempdir::TempDir;
    use std::time::{Duration, Instant};
    use storage::Config as StorageConfig;
    use std::sync::{Arc, Mutex};
    use std::sync::mpsc;
    use server::Config;
    use std::process;
    use server::{Server, create_raft_storage};
    use raftstore::store::SnapManager;
    use std::error::Error;
    use super::*;
    use rocksdb::{DBOptions, ColumnFamilyOptions};
    use util::rocksdb::{self, CFOptions};
    use storage::CF_DEFAULT;
    use server::server::tests::{MockResolver, TestRaftStoreRouter};

    fn exit_with_err<E: Error>(e: E) -> ! {
        exit_with_msg(format!("{:?}", e))
    }

    fn exit_with_msg(msg: String) -> ! {
        error!("{}", msg);
        process::exit(1)
    }

    #[test]
    fn test_metrics_flusher() {
        let path = TempDir::new("_test_metrics_flusher").unwrap();
        let db_opt = DBOptions::new();
        let cf_opts = ColumnFamilyOptions::new();
        let mut cfg = Config::default();
        let storage_cfg = StorageConfig::default();
        cfg.addr = "127.0.0.1:0".to_owned();
        let (tx, _) = mpsc::channel();
        let router = TestRaftStoreRouter::new(tx);
        let engine = Arc::new(rocksdb::new_engine_opt(path.path().to_str().unwrap(),
                                                      db_opt,
                                                      vec![CFOptions::new(CF_DEFAULT, cf_opts)])
            .unwrap());

        let mut storage = create_raft_storage(router.clone(), engine.clone(), &storage_cfg)
            .unwrap_or_else(|e| exit_with_err(e));
        storage.start(&storage_cfg).unwrap();

        let (snapshot_status_sender, _) = mpsc::channel();

        let addr = Arc::new(Mutex::new(None));

        let mut metrics_flusher = MetricsFlusher::new(engine.clone(), 10000);

        let mut server = Server::new(&cfg,
                                     1024,
                                     storage,
                                     router,
                                     snapshot_status_sender,
                                     MockResolver { addr: addr.clone() },
                                     SnapManager::new("", None, true))
            .unwrap();
        if let Err(e) = metrics_flusher.start() {
            error!("failed to start metrics flusher, error = {:?}", e);
        }
        server.start(&cfg).unwrap();

        let rtime = Duration::from_millis(20000);
        let now = Instant::now();

        while now.elapsed() >= rtime {
            server.stop().unwrap();
        }

        metrics_flusher.stop();
    }
}