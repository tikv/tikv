// Copyright 2018 PingCAP, Inc.
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

use std::cmp;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::thread;
use std::time::{Duration, Instant};

use kvproto::metapb::*;

use pd::RegionInfo;
use util::escape;
use util::rocksdb::properties::SizeProperties;

use super::client::*;
use super::common::*;
use super::engine::*;
use super::{Config, Error, Result};

const MAX_RETRY_TIMES: u64 = 3;
const RETRY_INTERVAL_SECS: u64 = 1;

/// PrepareJob is responsible for improving cluster data balance
///
/// The main job is:
/// 1. split data into ranges according to region size and region distribution
/// 2. split and scatter regions of a cluster before we import a large amount of data
pub struct PrepareJob<Client> {
    tag: String,
    cfg: Config,
    client: Arc<Client>,
    engine: Arc<Engine>,
    counter: AtomicUsize,
}

impl<Client: ImportClient> PrepareJob<Client> {
    pub fn new(cfg: Config, client: Client, engine: Arc<Engine>) -> PrepareJob<Client> {
        PrepareJob {
            tag: format!("[PrepareJob {}]", engine.uuid()),
            cfg,
            client: Arc::new(client),
            engine,
            counter: AtomicUsize::new(0),
        }
    }

    pub fn run(&self) -> Result<Vec<RangeInfo>> {
        let start = Instant::now();
        info!("{} start", self.tag);

        let props = match self.engine.get_size_properties() {
            Ok(v) => {
                info!("{} approximate size {}", self.tag, v.total_size);
                v
            }
            Err(e) => {
                error!("{} get size properties: {:?}", self.tag, e);
                return Err(e);
            }
        };

        let num_prepares = self.prepare(&props);

        // PD needs some time to scatter regions. But we don't know how much
        // time it should take, so we just calculate an approximate duration.
        let wait_duration = Duration::from_millis(num_prepares as u64 * 100);
        let wait_duration = cmp::min(wait_duration, self.cfg.max_prepare_duration.0);
        info!(
            "{} prepare {} ranges waits {:?}",
            self.tag, num_prepares, wait_duration,
        );
        thread::sleep(wait_duration);

        info!(
            "{} prepare {} ranges takes {:?}",
            self.tag,
            num_prepares,
            start.elapsed(),
        );

        // One `SubImportJob` is responsible for one range, the max number of `SubImportJob`
        // is `num_import_jobs`.
        Ok(get_approximate_ranges(
            &props,
            self.cfg.num_import_jobs,
            self.cfg.region_split_size.0 as usize,
        ))
    }

    fn prepare(&self, props: &SizeProperties) -> usize {
        let split_size = self.cfg.region_split_size.0 as usize;
        let mut ctx = RangeContext::new(Arc::clone(&self.client), split_size);

        let mut num_prepares = 0;
        let mut start = Vec::new();
        for (k, v) in props.index_handles.iter() {
            ctx.add(v.size as usize);
            if !ctx.should_stop_before(k) {
                continue;
            }

            let range = RangeInfo::new(&start, k, ctx.raw_size());
            if let Ok(true) = self.run_prepare_job(range) {
                num_prepares += 1;
            }

            start = k.to_owned();
            ctx.reset(k);
        }

        num_prepares
    }

    fn run_prepare_job(&self, range: RangeInfo) -> Result<bool> {
        let id = self.counter.fetch_add(1, Ordering::SeqCst);
        let tag = format!("[PrepareRangeJob {}:{}]", self.engine.uuid(), id);
        let job = PrepareRangeJob::new(tag, range, Arc::clone(&self.client));
        job.run()
    }
}

/// PrepareRangeJob is responsible for helping to split and scatter regions.
/// according to range of data we are going to import
struct PrepareRangeJob<Client> {
    tag: String,
    range: RangeInfo,
    client: Arc<Client>,
}

impl<Client: ImportClient> PrepareRangeJob<Client> {
    fn new(tag: String, range: RangeInfo, client: Arc<Client>) -> PrepareRangeJob<Client> {
        PrepareRangeJob { tag, range, client }
    }

    fn run(&self) -> Result<bool> {
        let start = Instant::now();
        info!("{} start {:?}", self.tag, self.range);

        for i in 0..MAX_RETRY_TIMES {
            if i != 0 {
                thread::sleep(Duration::from_secs(RETRY_INTERVAL_SECS));
            }

            let mut region = match self.client.get_region(self.range.get_start()) {
                Ok(region) => region,
                Err(e) => {
                    warn!("{}: {:?}", self.tag, e);
                    continue;
                }
            };

            for _ in 0..MAX_RETRY_TIMES {
                match self.prepare(region) {
                    Ok(v) => {
                        info!("{} takes {:?}", self.tag, start.elapsed());
                        return Ok(v);
                    }
                    Err(Error::UpdateRegion(new_region)) => {
                        region = new_region;
                        continue;
                    }
                    Err(_) => break,
                }
            }
        }

        error!("{} run out of time", self.tag);
        Err(Error::PrepareRangeJobFailed(self.tag.clone()))
    }

    fn prepare(&self, mut region: RegionInfo) -> Result<bool> {
        if !self.need_split(&region) {
            return Ok(false);
        }
        match self.split_region(&region) {
            Ok(new_region) => {
                self.scatter_region(&new_region)?;
                Ok(true)
            }
            Err(Error::NotLeader(new_leader)) => {
                region.leader = new_leader;
                Err(Error::UpdateRegion(region))
            }
            Err(Error::StaleEpoch(new_regions)) => {
                let new_region = new_regions.iter().find(|&r| self.need_split(r)).cloned();
                match new_region {
                    Some(new_region) => {
                        let new_leader = region
                            .leader
                            .and_then(|p| find_region_peer(&new_region, p.get_store_id()));
                        Err(Error::UpdateRegion(RegionInfo::new(new_region, new_leader)))
                    }
                    None => {
                        warn!("{} stale epoch {:?}", self.tag, new_regions);
                        Err(Error::StaleEpoch(new_regions))
                    }
                }
            }
            Err(e) => Err(e),
        }
    }

    /// Judges if we need to split the region.
    fn need_split(&self, region: &Region) -> bool {
        let split_key = self.range.get_end();
        if split_key.is_empty() {
            return false;
        }
        if split_key <= region.get_start_key() {
            return false;
        }
        before_end(split_key, region.get_end_key())
    }

    fn split_region(&self, region: &RegionInfo) -> Result<RegionInfo> {
        let split_key = self.range.get_end();
        let res = match self.client.split_region(region, split_key) {
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
                info!("{} split {:?} at {:?}", self.tag, region, escape(split_key));
                // Just assume that the leader will be at the same store.
                let left = resp.take_left();
                let leader = match region.leader {
                    Some(ref p) => find_region_peer(&left, p.get_store_id()),
                    None => None,
                };
                Ok(RegionInfo::new(left, leader))
            }
            Err(e) => {
                warn!(
                    "{} split {:?} at {:?}: {:?}",
                    self.tag,
                    region,
                    escape(split_key),
                    e
                );
                Err(e)
            }
        }
    }

    fn scatter_region(&self, region: &RegionInfo) -> Result<()> {
        match self.client.scatter_region(region) {
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
    use import::test_helpers::*;

    use rocksdb::Writable;
    use tempdir::TempDir;
    use uuid::Uuid;

    use config::DbConfig;
    use storage::types::Key;

    fn new_encoded_key(k: &[u8]) -> Vec<u8> {
        if k.is_empty() {
            vec![]
        } else {
            Key::from_raw(k).into_encoded()
        }
    }

    #[test]
    fn test_prepare_job() {
        let dir = TempDir::new("test_import_prepare_job").unwrap();
        let uuid = Uuid::new_v4();
        let opts = DbConfig::default();
        let engine = Arc::new(Engine::new(dir.path(), uuid, opts).unwrap());

        // Generate entries to prepare.
        let (n, m) = (4, 4);
        // This size if pre-calculated.
        let index_size = 10;
        for i in 0..n {
            for j in 0..m {
                let v = &[i + j * n + 1];
                let k = new_encoded_key(v);
                engine.put(&k, v).unwrap();
                assert_eq!(k.len() + v.len(), index_size);
                engine.flush(true).unwrap();
            }
        }

        let mut cfg = Config::default();
        // We have n * m = 16 entries and 4 jobs.
        cfg.num_import_jobs = 4;
        // Each region contains at most 3 entries.
        cfg.region_split_size.0 = index_size as u64 * 3;

        // Expected ranges returned by the prepare job.
        let ranges = vec![
            (vec![], vec![4]),
            (vec![4], vec![8]),
            (vec![8], vec![12]),
            (vec![12], vec![]),
        ];

        // Test with an empty range.
        {
            let mut client = MockClient::new();
            client.add_region_range(b"", b"");
            // Expected region ranges returned by the prepare job.
            let region_ranges = vec![
                (vec![], vec![3], true),
                (vec![3], vec![6], true),
                (vec![6], vec![9], true),
                (vec![9], vec![12], true),
                (vec![12], vec![15], true),
                (vec![15], vec![], false),
            ];
            run_and_check_prepare_job(
                cfg.clone(),
                client,
                Arc::clone(&engine),
                &ranges,
                &region_ranges,
            );
        }

        // Test with some segmented region ranges.
        {
            let mut client = MockClient::new();
            let keys = vec![
                // [0, 3), [3, 5)
                5, // [5, 7)
                7, // [7, 10), [10, 13), [13, 15)
                15,
            ];
            let mut last = Vec::new();
            for i in keys {
                let k = new_encoded_key(&[i]);
                client.add_region_range(&last, &k);
                last = k.clone();
            }
            client.add_region_range(&last, b"");
            // Expected region ranges returned by the prepare job.
            let region_ranges = vec![
                (vec![], vec![3], true),
                (vec![3], vec![5], false),
                (vec![5], vec![7], false),
                (vec![7], vec![10], true),
                (vec![10], vec![13], true),
                (vec![13], vec![15], false),
                (vec![15], vec![], false),
            ];
            run_and_check_prepare_job(
                cfg.clone(),
                client,
                Arc::clone(&engine),
                &ranges,
                &region_ranges,
            );
        }
    }

    fn run_and_check_prepare_job(
        cfg: Config,
        client: MockClient,
        engine: Arc<Engine>,
        expected_ranges: &[(Vec<u8>, Vec<u8>)],
        expected_region_ranges: &[(Vec<u8>, Vec<u8>, bool)],
    ) {
        let job = PrepareJob::new(cfg, client.clone(), Arc::clone(&engine));

        let ranges = job.run().unwrap();
        assert_eq!(ranges.len(), expected_ranges.len());
        for (range, &(ref start, ref end)) in ranges.iter().zip(expected_ranges.iter()) {
            let start_key = new_encoded_key(start);
            let end_key = new_encoded_key(end);
            assert_eq!(range.get_start(), start_key.as_slice());
            assert_eq!(range.get_end(), end_key.as_slice());
        }

        for &(ref start, ref end, should_scatter) in expected_region_ranges {
            let start_key = new_encoded_key(start);
            let end_key = new_encoded_key(end);
            let region = client.get_region(&start_key).unwrap();
            assert_eq!(region.get_start_key(), start_key.as_slice());
            assert_eq!(region.get_end_key(), end_key.as_slice());
            if should_scatter {
                client.get_scatter_region(region.get_id()).unwrap();
            }
        }
    }
}
