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
use std::time::{Duration, Instant};
use std::thread::{self, JoinHandle};

use kvproto::kvrpcpb::*;

use pd::RegionInfo;
use storage::types::Key;
use util::escape;

use super::{Client, Config, Engine, Error, Result};
use super::common::*;
use super::stream::*;

const MAX_RETRY_TIMES: u64 = 3;
const RETRY_INTERVAL_SECS: u64 = 1;

pub struct PrepareJob<C> {
    tag: String,
    cfg: Config,
    client: Arc<C>,
    engine: Arc<Engine>,
    counter: Arc<AtomicUsize>,
}

impl<C: Client + Send + Sync + 'static> PrepareJob<C> {
    pub fn new(cfg: Config, client: Arc<C>, engine: Arc<Engine>) -> PrepareJob<C> {
        PrepareJob {
            tag: format!("[PrepareJob {}]", engine.uuid()),
            cfg: cfg,
            client: client,
            engine: engine,
            counter: Arc::new(AtomicUsize::new(0)),
        }
    }

    pub fn run(&self) -> Result<Vec<RangeInfo>> {
        let start = Instant::now();
        info!("{} start", self.tag);

        let ranges = self.engine
            .get_approximate_ranges(self.cfg.max_import_jobs)?;
        let handles = self.run_prepare_threads(ranges);

        let mut res = Vec::new();
        for h in handles {
            res.extend(h.join().unwrap());
        }
        res.sort_by(|a, b| a.get_start().cmp(b.get_start()));

        info!(
            "{} prepare {} ranges takes {:?}",
            self.tag,
            res.len(),
            start.elapsed()
        );
        Ok(res)
    }

    fn new_prepare_thread(&self, id: u64, range: RangeInfo) -> JoinHandle<Vec<RangeInfo>> {
        let cfg = self.cfg.clone();
        let client = self.client.clone();
        let engine = self.engine.clone();
        let counter = self.counter.clone();

        thread::Builder::new()
            .name("prepare-job".to_owned())
            .spawn(move || {
                let job = SubPrepareJob::new(id, cfg, range, client, engine, counter);
                job.run()
            })
            .unwrap()
    }

    fn run_prepare_threads(&self, ranges: Vec<RangeInfo>) -> Vec<JoinHandle<Vec<RangeInfo>>> {
        let mut handles = Vec::new();
        for (i, range) in ranges.into_iter().enumerate() {
            handles.push(self.new_prepare_thread(i as u64, range));
        }
        handles
    }
}

struct SubPrepareJob<C> {
    id: u64,
    tag: String,
    cfg: Config,
    range: RangeInfo,
    client: Arc<C>,
    engine: Arc<Engine>,
    counter: Arc<AtomicUsize>,
}

impl<C: Client + Send + Sync + 'static> SubPrepareJob<C> {
    pub fn new(
        id: u64,
        cfg: Config,
        range: RangeInfo,
        client: Arc<C>,
        engine: Arc<Engine>,
        counter: Arc<AtomicUsize>,
    ) -> SubPrepareJob<C> {
        SubPrepareJob {
            id: id,
            tag: format!("[SubPrepareJob {}:{}]", engine.uuid(), id),
            cfg: cfg,
            range: range,
            client: client,
            engine: engine,
            counter: counter,
        }
    }

    pub fn run(&self) -> Vec<RangeInfo> {
        let start = Instant::now();
        info!("{} start {:?}", self.tag, self.range);

        let (tx, rx) = mpsc::sync_channel(self.cfg.max_import_sst_jobs);
        let handles = self.run_prepare_threads(rx);
        let ranges = self.run_prepare_stream(tx);
        for h in handles {
            h.join().unwrap();
        }

        info!(
            "{} prepare {} ranges takes {:?}",
            self.tag,
            ranges.len(),
            start.elapsed(),
        );
        ranges
    }

    fn run_prepare_stream(&self, tx: mpsc::SyncSender<RangeInfo>) -> Vec<RangeInfo> {
        let mut ranges = Vec::new();

        let mut ctx = RangeContext::new(self.client.clone(), self.cfg.region_split_size);
        let engine_iter = self.engine.new_iter(false);
        let mut iter = RangeIterator::new(engine_iter, self.range.range.clone(), Vec::new());

        while iter.valid() {
            let start = iter.key().to_owned();
            ctx.reset(&start);

            loop {
                ctx.add(iter.key(), iter.value());
                if !iter.next() || ctx.should_stop_before(iter.key()) {
                    break;
                }
            }

            let end = if iter.valid() {
                iter.key()
            } else {
                self.range.get_end()
            };

            let range = RangeInfo::new(&start, end, ctx.raw_size());
            ranges.push(range.clone());
            tx.send(range).unwrap();
        }

        ranges
    }

    fn new_prepare_thread(&self, rx: Arc<Mutex<mpsc::Receiver<RangeInfo>>>) -> JoinHandle<()> {
        let sub_id = self.id;
        let client = self.client.clone();
        let engine = self.engine.clone();
        let counter = self.counter.clone();

        thread::Builder::new()
            .name("prepare-range".to_owned())
            .spawn(move || while let Ok(range) = rx.lock().unwrap().recv() {
                let id = counter.fetch_add(1, Ordering::SeqCst);
                let tag = format!("[PrepareRangeJob {}:{}:{}]", engine.uuid(), sub_id, id);
                let job = PrepareRangeJob::new(tag, range, client.clone());
                // We don't care whether the preparation succeeded or not.
                let _ = job.run();
            })
            .unwrap()
    }

    fn run_prepare_threads(&self, rx: mpsc::Receiver<RangeInfo>) -> Vec<JoinHandle<()>> {
        let mut handles = Vec::new();
        let rx = Arc::new(Mutex::new(rx));
        for _ in 0..self.cfg.max_import_sst_jobs {
            handles.push(self.new_prepare_thread(rx.clone()));
        }
        handles
    }
}

struct PrepareRangeJob<C> {
    tag: String,
    range: RangeInfo,
    client: Arc<C>,
}

impl<C: Client> PrepareRangeJob<C> {
    fn new(tag: String, range: RangeInfo, client: Arc<C>) -> PrepareRangeJob<C> {
        PrepareRangeJob {
            tag: tag,
            range: range,
            client: client,
        }
    }

    fn run(&self) -> Result<()> {
        let start = Instant::now();
        info!("{} start {:?}", self.tag, self.range);

        for i in 0..MAX_RETRY_TIMES {
            if i != 0 {
                thread::sleep(Duration::from_secs(RETRY_INTERVAL_SECS));
            }

            if self.prepare().is_ok() {
                info!("{} takes {:?}", self.tag, start.elapsed());
                return Ok(());
            }
        }

        Err(Error::PrepareRangeJobFailed(self.tag.clone()))
    }

    fn prepare(&self) -> Result<()> {
        let split_key = self.range.get_end().to_owned();
        let mut region = self.client.get_region(self.range.get_start())?;

        for _ in 0..MAX_RETRY_TIMES {
            if split_key.is_empty() || !before_end(&split_key, region.get_end_key()) {
                break;
            }
            match self.split_region(&region, &split_key) {
                Ok(new_region) => region = new_region,
                Err(Error::NotLeader(new_leader)) => region.leader = new_leader,
                Err(Error::StaleEpoch(new_regions)) => for new_region in new_regions {
                    if before_end(&split_key, new_region.get_end_key()) {
                        let new_leader = region
                            .leader
                            .and_then(|p| find_region_peer(&new_region, p.get_store_id()));
                        region = RegionInfo::new(new_region, new_leader);
                        break;
                    }
                },
                Err(e) => return Err(e),
            }
        }

        self.scatter_region(&region)
    }

    fn split_region(&self, region: &RegionInfo, split_key: &[u8]) -> Result<RegionInfo> {
        let ctx = new_context(region);
        let store_id = ctx.get_peer().get_store_id();
        // The SplitRegion API accepts a raw key.
        let raw_key = Key::from_encoded(split_key.to_owned()).raw()?;

        let mut split = SplitRegionRequest::new();
        split.set_context(ctx);
        split.set_split_key(raw_key);

        let res = match self.client.split_region(store_id, split) {
            Ok(mut resp) => if !resp.has_region_error() {
                Ok(resp)
            } else {
                match Error::from(resp.take_region_error()) {
                    e @ Error::NotLeader(_) | e @ Error::StaleEpoch(_) => return Err(e),
                    e => Err(e),
                }
            },
            Err(e) => Err(e),
        };

        match res {
            Ok(mut resp) => {
                info!(
                    "{} split at {:?} to left {{{:?}}} and right {{{:?}}}",
                    self.tag,
                    escape(split_key),
                    resp.get_left(),
                    resp.get_right(),
                );
                // Just assume that the leader will be at the same store.
                let new_region = resp.take_left();
                let new_leader = find_region_peer(&new_region, store_id);
                Ok(RegionInfo::new(new_region, new_leader))
            }
            Err(e) => {
                warn!("{} split {:?}: {:?}", self.tag, region, e);
                Err(e)
            }
        }
    }

    fn scatter_region(&self, region: &RegionInfo) -> Result<()> {
        match self.client.scatter_region(region.clone()) {
            Ok(_) => {
                info!("{} scatter region {}", self.tag, region.get_id());
                Ok(())
            }
            Err(e) => {
                warn!("{} scatter region {}: {:?}", self.tag, region.get_id(), e);
                Err(e)
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use uuid::Uuid;
    use tempdir::TempDir;
    use rocksdb::Writable;

    use config::DbConfig;

    use import::client::tests::MockClient;

    #[test]
    fn test_prepare_job() {
        let dir = TempDir::new("test_import_prepare_job").unwrap();
        let uuid = Uuid::new_v4();
        let opts = DbConfig::default();
        let engine = Arc::new(Engine::new(dir.path(), uuid, opts).unwrap());

        // Generate entries to prepare.
        let num_files = 4;
        let num_entries = 4;
        // This size if pre-calculated.
        let entry_size = 10;
        for i in 0..num_files {
            for j in 0..num_entries {
                let v = &[i + j * num_files];
                let k = Key::from_raw(v);
                engine.put(k.encoded(), v).unwrap();
                assert_eq!(k.encoded().len() + v.len(), entry_size);
            }
            engine.flush(true).unwrap();
        }

        let mut cfg = Config::default();
        cfg.max_import_jobs = 4;
        cfg.region_split_size = entry_size * 3; // A region contains at most 3 entries.

        // Test with continuous region ranges.
        {
            let mut client = MockClient::new();
            let k = Key::from_raw(&[0]);
            client.add_region_range(b"", k.encoded());
            client.add_region_range(k.encoded(), b"");
            let client = Arc::new(client);
            run_and_check_prepare_job(cfg.clone(), client.clone(), engine.clone());
        }

        // Test with some segmented region ranges.
        {
            let mut client = MockClient::new();
            let keys = vec![
                0,
                // [0, 3), [3, 5)
                5,
                // [5, 7)
                7,
                // [7, 10), [10, 13), [13, 15)
                15,
            ];
            let mut last = Vec::new();
            for i in keys {
                let k = Key::from_raw(&[i]);
                client.add_region_range(&last, k.encoded());
                last = k.encoded().clone();
            }
            client.add_region_range(&last, b"");

            let client = Arc::new(client);
            run_and_check_prepare_job(cfg.clone(), client.clone(), engine.clone());
        }
    }

    fn run_and_check_prepare_job(cfg: Config, client: Arc<MockClient>, engine: Arc<Engine>) {
        let job = PrepareJob::new(cfg, client.clone(), engine.clone());
        let ranges = job.run().unwrap();
        let mut last_end: Option<Vec<u8>> = None;
        for range in ranges {
            let region = client.get_region(range.get_start()).unwrap();
            assert_eq!(range.get_start(), region.get_start_key());
            assert_eq!(range.get_end(), region.get_end_key());
            if let Some(last_end) = last_end {
                assert_eq!(last_end.as_slice(), region.get_start_key());
            }
            last_end = Some(region.get_end_key().to_owned());
            client.get_scatter_region(region.get_id()).unwrap();
        }
    }
}
