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

use std::sync::{Arc, Mutex};
use std::sync::mpsc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Instant;
use std::thread::{self, JoinHandle};

use rocksdb::SeekKey;
use kvproto::kvrpcpb::*;

use pd::RegionInfo;
use storage::types::Key;

use super::{Client, Config, Engine, Error, Result};
use super::region::*;

pub struct PrepareJob {
    tag: String,
    cfg: Config,
    client: Arc<Client>,
    engine: Arc<Engine>,
    cf_name: String,
    job_counter: Arc<AtomicUsize>,
}

impl PrepareJob {
    pub fn new(
        cfg: Config,
        client: Arc<Client>,
        engine: Arc<Engine>,
        cf_name: String,
    ) -> PrepareJob {
        PrepareJob {
            tag: format!("[PrepareJob {}:{}]", engine.uuid(), cf_name),
            cfg: cfg,
            client: client,
            engine: engine,
            cf_name: cf_name,
            job_counter: Arc::new(AtomicUsize::new(0)),
        }
    }

    pub fn run(&self) -> Vec<RangeInfo> {
        let start = Instant::now();
        info!("{} start", self.tag);

        let (tx, rx) = mpsc::channel();
        let handles = self.run_prepare_threads(rx);
        let ranges = self.run_prepare_stream(tx);
        for h in handles {
            h.join().unwrap();
        }

        info!(
            "{} with {} ranges takes {:?}",
            self.tag,
            ranges.len(),
            start.elapsed(),
        );
        ranges
    }

    fn run_prepare_stream(&self, tx: mpsc::Sender<RangeInfo>) -> Vec<RangeInfo> {
        let mut ranges = Vec::new();

        let mut ctx = RegionContext::new(self.client.clone(), self.cfg.region_split_size);
        let mut iter = self.engine.new_iter(&self.cf_name, false);
        iter.seek(SeekKey::Start);

        while iter.valid() {
            let start = iter.key().to_owned();

            ctx.reset(iter.key());
            loop {
                ctx.add(iter.key(), iter.value());
                if !iter.next() || ctx.should_stop_before(iter.key()) {
                    break;
                }
            }

            let end = if iter.valid() {
                iter.key().to_owned()
            } else {
                RANGE_MAX.to_owned()
            };

            let range = RangeInfo::new(start, end, ctx.raw_size());
            ranges.push(range.clone());
            tx.send(range).unwrap();
        }

        ranges
    }

    fn new_prepare_thread(&self, rx: Arc<Mutex<mpsc::Receiver<RangeInfo>>>) -> JoinHandle<()> {
        let cfg = self.cfg.clone();
        let client = self.client.clone();
        let engine = self.engine.clone();
        let cf_name = self.cf_name.clone();
        let job_counter = self.job_counter.clone();

        thread::Builder::new()
            .name("prepare-job".to_owned())
            .spawn(move || while let Ok(info) = rx.lock().unwrap().recv() {
                let id = job_counter.fetch_add(1, Ordering::SeqCst);
                let tag = format!("[PrepareJob {}:{}:{}]", engine.uuid(), cf_name, id);
                let job = PrepareRangeJob::new(tag, cfg.clone(), info, client.clone());
                let _ = job.run(); // Don't care about error here.
            })
            .unwrap()
    }

    fn run_prepare_threads(&self, rx: mpsc::Receiver<RangeInfo>) -> Vec<JoinHandle<()>> {
        let mut handles = Vec::new();
        let rx = Arc::new(Mutex::new(rx));
        for _ in 0..self.cfg.max_import_jobs {
            handles.push(self.new_prepare_thread(rx.clone()));
        }
        handles
    }
}

struct PrepareRangeJob {
    tag: String,
    cfg: Config,
    info: RangeInfo,
    client: Arc<Client>,
}

impl PrepareRangeJob {
    fn new(tag: String, cfg: Config, info: RangeInfo, client: Arc<Client>) -> PrepareRangeJob {
        PrepareRangeJob {
            tag: tag,
            cfg: cfg,
            info: info,
            client: client,
        }
    }

    fn run(&self) -> Result<()> {
        let start = Instant::now();
        info!("{} start", self.tag);

        let mut region = self.client.get_region(&self.info.start)?;

        // No need to split if the file is not large enough.
        if self.info.size > self.cfg.region_split_size / 2 &&
            RangeEnd(&self.info.end) < RangeEnd(region.get_end_key())
        {
            region = self.split_region(&region, &self.info.end)?;
        }

        self.relocate_region(&region)?;

        info!("{} takes {:?}", self.tag, start.elapsed());
        Ok(())
    }

    fn split_region(&self, region: &RegionInfo, split_key: &[u8]) -> Result<RegionInfo> {
        let ctx = new_context(region);
        let store_id = ctx.get_peer().get_store_id();
        // The SplitRegion API accepts a raw key.
        let raw_key = Key::from_encoded(split_key.to_owned()).raw()?;

        let mut req = SplitRegionRequest::new();
        req.set_context(ctx);
        req.set_split_key(raw_key);

        let res = match self.client.split_region(store_id, req) {
            Ok(mut resp) => if resp.has_region_error() {
                Err(Error::SplitRegion(resp.take_region_error()))
            } else {
                Ok(resp)
            },
            Err(e) => Err(e),
        };

        match res {
            Ok(mut resp) => {
                info!(
                    "{} split {:?} to {:?} and {:?}",
                    self.tag,
                    region,
                    resp.get_left(),
                    resp.get_right(),
                );
                let region = resp.take_left();
                // Just assume that the leader will be at the same store.
                let leader = region
                    .get_peers()
                    .iter()
                    .find(|p| p.get_store_id() == store_id)
                    .cloned();
                Ok(RegionInfo::new(region, leader))
            }
            Err(e) => {
                error!("{} split {:?}: {:?}", self.tag, region, e);
                Err(e)
            }
        }
    }

    fn relocate_region(&self, _: &RegionInfo) -> Result<()> {
        // TODO
        Ok(())
    }
}
