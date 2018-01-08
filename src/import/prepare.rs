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

use rocksdb::SeekKey;
use kvproto::kvrpcpb::*;

use pd::RegionInfo;
use storage::types::Key;
use util::escape;

use super::{Config, Engine, Error, ImportClient, Result};
use super::region::*;

const MAX_RETRY_TIMES: u64 = 3;
const RETRY_INTERVAL_SECS: u64 = 1;

pub struct PrepareJob<C> {
    tag: String,
    cfg: Config,
    client: Arc<C>,
    engine: Arc<Engine>,
    cf_name: String,
    counter: Arc<AtomicUsize>,
}

impl<C: ImportClient + Send + Sync + 'static> PrepareJob<C> {
    pub fn new(cfg: Config, client: Arc<C>, engine: Arc<Engine>, cf_name: String) -> PrepareJob<C> {
        PrepareJob {
            tag: format!("[PrepareJob {}:{}]", engine.uuid(), cf_name),
            cfg: cfg,
            client: client,
            engine: engine,
            cf_name: cf_name,
            counter: Arc::new(AtomicUsize::new(0)),
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
            "{} prepare {} ranges takes {:?}",
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

        if iter.valid() {
            // Make a start range for split.
            let range = RangeInfo::new(iter.key(), iter.key(), 0);
            tx.send(range).unwrap();
        }

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
                iter.key()
            } else {
                ctx.end_key()
            };
            let range = RangeInfo::new(&start, end, ctx.raw_size());
            ranges.push(range.clone());

            tx.send(range).unwrap();
        }

        ranges
    }

    fn new_prepare_thread(&self, rx: Arc<Mutex<mpsc::Receiver<RangeInfo>>>) -> JoinHandle<()> {
        let client = self.client.clone();
        let engine = self.engine.clone();
        let cf_name = self.cf_name.clone();
        let counter = self.counter.clone();

        thread::Builder::new()
            .name("prepare-job".to_owned())
            .spawn(move || while let Ok(range) = rx.lock().unwrap().recv() {
                let id = counter.fetch_add(1, Ordering::SeqCst);
                let tag = format!("[PrepareJob {}:{}:{}]", engine.uuid(), cf_name, id);
                let job = PrepareRangeJob::new(tag, range, client.clone());
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

struct PrepareRangeJob<C> {
    tag: String,
    range: RangeInfo,
    client: Arc<C>,
}

impl<C: ImportClient> PrepareRangeJob<C> {
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
                warn!("{} retry #{}", self.tag, i);
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
        let mut region = self.client.get_region(self.range.get_start())?;

        for _ in 0..MAX_RETRY_TIMES {
            if !is_before_end(self.range.get_end(), region.get_end_key()) {
                break;
            }
            match self.split_region(&region, self.range.get_end()) {
                Ok(new_region) => region = new_region,
                Err(Error::NotLeader(new_leader)) => region.leader = new_leader,
                Err(Error::StaleEpoch(new_regions)) => for new_region in new_regions {
                    if is_before_end(self.range.get_end(), new_region.get_end_key()) {
                        let new_leader = region
                            .leader
                            .and_then(|p| find_peer_in_store(&new_region, p.get_store_id()));
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
                let new_leader = find_peer_in_store(&new_region, store_id);
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
        let engine = Arc::new(Engine::new(uuid, dir.path(), opts).unwrap());
        let cf_name = "default".to_owned();

        // Generate entries to prepare.
        // Entry size is 16 + 27 = 43.
        for i in 0..16 {
            let s = format!("{:016}", i);
            let k = Key::from_raw(s.as_bytes());
            assert_eq!(k.encoded().len(), 27); // This size is pre-calculated.
            engine.put(k.encoded(), s.as_bytes()).unwrap();
        }

        let mut cfg = Config::default();
        cfg.max_import_jobs = 4;
        cfg.region_split_size = 128; // A region contains at most 3 entries.

        // Test with an empty region range.
        {
            let mut client = MockClient::new();
            client.add_region_range(b"", b"");
            let client = Arc::new(client);
            run_and_check_prepare_job(cfg.clone(), client, engine.clone(), cf_name.clone());
        }

        // Test with some region ranges.
        {
            let mut client = MockClient::new();
            let keys = vec![
                // [0, 3), [3, 5)
                5,
                // [5, 7)
                7,
                // [7, 10), [10, 13), [13, 15)
                15,
            ];
            let mut last = Vec::new();
            for i in keys {
                let k = Key::from_raw(format!("{:016}", i).as_bytes());
                client.add_region_range(&last, k.encoded());
                last = k.encoded().clone();
            }
            // Add an unrelated range.
            client.add_region_range(&last, b"abc");
            client.add_region_range(b"abc", b"");

            let client = Arc::new(client);
            run_and_check_prepare_job(cfg.clone(), client, engine.clone(), cf_name.clone());
        }
    }

    fn run_and_check_prepare_job(
        cfg: Config,
        client: Arc<MockClient>,
        engine: Arc<Engine>,
        cf_name: String,
    ) {
        let job = PrepareJob::new(cfg, client.clone(), engine, cf_name);
        let ranges = job.run();
        for range in ranges {
            let region = client.get_region(range.get_start()).unwrap();
            assert_eq!(range.get_start(), region.get_start_key());
            assert_eq!(range.get_end(), region.get_end_key());
            client.get_scatter_region(region.get_id()).unwrap();
        }
    }
}
