// Copyright 2020 TiKV Project Authors. Licensed under Apache-2.0.

use std::io;
use std::result::Result;
use std::sync::mpsc::{self, Sender};
use std::thread::{Builder as ThreadBuilder, JoinHandle};
use std::time::{Duration, Instant};

use crate::*;

const DEFAULT_FLUSH_INTERVAL: Duration = Duration::from_millis(10_000);
const FLUSHER_RESET_INTERVAL: Duration = Duration::from_millis(60_000);

pub struct MetricsFlusher<K: KvEngine, R: KvEngine> {
    pub engines: KvEngines<K, R>,
    interval: Duration,
    handle: Option<JoinHandle<()>>,
    sender: Option<Sender<bool>>,
}

impl<K: KvEngine, R: KvEngine> MetricsFlusher<K, R> {
    pub fn new(engines: KvEngines<K, R>) -> Self {
        MetricsFlusher {
            engines,
            interval: DEFAULT_FLUSH_INTERVAL,
            handle: None,
            sender: None,
        }
    }

    pub fn set_flush_interval(&mut self, interval: Duration) {
        self.interval = interval;
    }

    pub fn start(&mut self) -> Result<(), io::Error> {
        let (kv_db, raft_db) = (self.engines.kv.clone(), self.engines.raft.clone());
        let shared_block_cache = self.engines.shared_block_cache;
        let interval = self.interval;
        let (tx, rx) = mpsc::channel();
        self.sender = Some(tx);
        let h = ThreadBuilder::new()
            .name("metrics-flusher".to_owned())
            .spawn(move || {
                let mut last_reset = Instant::now();
                while let Err(mpsc::RecvTimeoutError::Timeout) = rx.recv_timeout(interval) {
                    kv_db.flush_metrics("kv", shared_block_cache);
                    raft_db.flush_metrics("raft", shared_block_cache);
                    if last_reset.elapsed() >= FLUSHER_RESET_INTERVAL {
                        kv_db.reset_statistics();
                        raft_db.reset_statistics();
                        last_reset = Instant::now();
                    }
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
            error!("join metrics flusher failed"; "err" => ?e);
            return;
        }
    }
}
