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

use std::cmp::Ordering;
use std::collections::BinaryHeap;
use std::fmt::{self, Display, Formatter};
use std::mem;
use std::sync::Arc;

use kvproto::metapb::Region;
use kvproto::metapb::RegionEpoch;
use kvproto::pdpb::CheckPolicy;
use rocksdb::{DBIterator, DB};

use raftstore::coprocessor::CoprocessorHost;
use raftstore::coprocessor::SplitCheckerHost;
use raftstore::store::engine::{IterOption, Iterable};
use raftstore::store::{keys, Callback, Msg};
use raftstore::Result;
use storage::{CfName, CF_WRITE, LARGE_CFS};
use util::escape;
use util::transport::{RetryableSendCh, Sender};
use util::worker::Runnable;

use super::metrics::*;

#[derive(PartialEq, Eq)]
pub struct KeyEntry {
    key: Vec<u8>,
    pos: usize,
    value_size: usize,
    cf: CfName,
}

impl KeyEntry {
    pub fn new(key: Vec<u8>, pos: usize, value_size: usize, cf: CfName) -> KeyEntry {
        KeyEntry {
            key,
            pos,
            value_size,
            cf,
        }
    }

    pub fn key(&self) -> &[u8] {
        self.key.as_ref()
    }

    pub fn is_commit_version(&self) -> bool {
        self.cf == CF_WRITE
    }

    pub fn entry_size(&self) -> usize {
        self.value_size + self.key.len()
    }
}

impl PartialOrd for KeyEntry {
    fn partial_cmp(&self, rhs: &KeyEntry) -> Option<Ordering> {
        // BinaryHeap is max heap, so we have to reverse order to get a min heap.
        Some(self.key.cmp(&rhs.key).reverse())
    }
}

impl Ord for KeyEntry {
    fn cmp(&self, rhs: &KeyEntry) -> Ordering {
        self.partial_cmp(rhs).unwrap()
    }
}

struct MergedIterator<'a> {
    iters: Vec<(CfName, DBIterator<&'a DB>)>,
    heap: BinaryHeap<KeyEntry>,
}

impl<'a> MergedIterator<'a> {
    fn new(
        db: &'a DB,
        cfs: &[CfName],
        start_key: &[u8],
        end_key: &[u8],
        fill_cache: bool,
    ) -> Result<MergedIterator<'a>> {
        let mut iters = Vec::with_capacity(cfs.len());
        let mut heap = BinaryHeap::with_capacity(cfs.len());
        for (pos, cf) in cfs.into_iter().enumerate() {
            let iter_opt =
                IterOption::new(Some(start_key.to_vec()), Some(end_key.to_vec()), fill_cache);
            let mut iter = db.new_iterator_cf(cf, iter_opt)?;
            if iter.seek(start_key.into()) {
                heap.push(KeyEntry::new(
                    iter.key().to_vec(),
                    pos,
                    iter.value().len(),
                    *cf,
                ));
            }
            iters.push((*cf, iter));
        }
        Ok(MergedIterator { iters, heap })
    }

    fn next(&mut self) -> Option<KeyEntry> {
        let pos = match self.heap.peek() {
            None => return None,
            Some(e) => e.pos,
        };
        let (cf, iter) = &mut self.iters[pos];
        if iter.next() {
            // TODO: avoid copy key.
            let mut e = KeyEntry::new(iter.key().to_vec(), pos, iter.value().len(), cf);
            let mut front = self.heap.peek_mut().unwrap();
            mem::swap(&mut e, &mut front);
            Some(e)
        } else {
            self.heap.pop()
        }
    }
}

/// Split checking task.
pub struct Task {
    region: Region,
    auto_split: bool,
    policy: CheckPolicy,
}

impl Task {
    pub fn new(region: Region, auto_split: bool, policy: CheckPolicy) -> Task {
        Task {
            region,
            auto_split,
            policy,
        }
    }
}

impl Display for Task {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        write!(
            f,
            "Split Check Task for {}, auto_split: {:?}",
            self.region.get_id(),
            self.auto_split
        )
    }
}

pub struct Runner<C> {
    engine: Arc<DB>,
    ch: RetryableSendCh<Msg, C>,
    coprocessor: Arc<CoprocessorHost>,
}

impl<C: Sender<Msg>> Runner<C> {
    pub fn new(
        engine: Arc<DB>,
        ch: RetryableSendCh<Msg, C>,
        coprocessor: Arc<CoprocessorHost>,
    ) -> Runner<C> {
        Runner {
            engine,
            ch,
            coprocessor,
        }
    }

    fn check_split(&mut self, task: Task) {
        let region = &task.region;
        let region_id = region.get_id();
        let start_key = keys::enc_start_key(region);
        let end_key = keys::enc_end_key(region);
        debug!(
            "[region {}] executing task {} {}",
            region_id,
            escape(&start_key),
            escape(&end_key)
        );
        CHECK_SPILT_COUNTER_VEC.with_label_values(&["all"]).inc();

        let mut host = self.coprocessor.new_split_checker_host(
            region,
            &self.engine,
            task.auto_split,
            task.policy,
        );
        if host.skip() {
            debug!("[region {}] skip split check", region.get_id());
            return;
        }

        let split_keys = match host.policy() {
            CheckPolicy::SCAN => {
                match self.scan_split_keys(&mut host, region, &start_key, &end_key) {
                    Ok(keys) => keys,
                    Err(e) => {
                        error!("[region {}] failed to scan split key: {}", region_id, e);
                        return;
                    }
                }
            }
            CheckPolicy::APPROXIMATE => match host.approximate_split_keys(region, &self.engine) {
                Ok(keys) => keys
                    .into_iter()
                    .map(|k| keys::origin_key(&k).to_vec())
                    .collect(),
                Err(e) => {
                    error!(
                        "[region {}] failed to get approxiamte split key: {}, try scan way",
                        region_id, e
                    );
                    match self.scan_split_keys(&mut host, region, &start_key, &end_key) {
                        Ok(keys) => keys,
                        Err(e) => {
                            error!("[region {}] failed to scan split key: {}", region_id, e);
                            return;
                        }
                    }
                }
            },
        };

        if !split_keys.is_empty() {
            let region_epoch = region.get_region_epoch().clone();
            let res = self
                .ch
                .try_send(new_split_region(region_id, region_epoch, split_keys));
            if let Err(e) = res {
                warn!("[region {}] failed to send check result: {}", region_id, e);
            }

            CHECK_SPILT_COUNTER_VEC
                .with_label_values(&["success"])
                .inc();
        } else {
            debug!(
                "[region {}] no need to send, split key not found",
                region_id,
            );

            CHECK_SPILT_COUNTER_VEC.with_label_values(&["ignore"]).inc();
        }
    }

    fn scan_split_keys(
        &mut self,
        host: &mut SplitCheckerHost,
        region: &Region,
        start_key: &[u8],
        end_key: &[u8],
    ) -> Result<Vec<Vec<u8>>> {
        let timer = CHECK_SPILT_HISTOGRAM.start_coarse_timer();
        MergedIterator::new(self.engine.as_ref(), LARGE_CFS, start_key, end_key, false).map(
            |mut iter| {
                while let Some(e) = iter.next() {
                    if host.on_kv(region, &e) {
                        break;
                    }
                }
            },
        )?;
        timer.observe_duration();

        Ok(host.split_keys())
    }
}

impl<C: Sender<Msg>> Runnable<Task> for Runner<C> {
    fn run(&mut self, task: Task) {
        self.check_split(task);
    }
}

fn new_split_region(region_id: u64, region_epoch: RegionEpoch, split_keys: Vec<Vec<u8>>) -> Msg {
    Msg::SplitRegion {
        region_id,
        region_epoch,
        split_keys,
        callback: Callback::None,
    }
}
