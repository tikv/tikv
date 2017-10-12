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

use std::sync::Arc;
use std::fmt::{self, Display, Formatter};
use std::collections::BinaryHeap;
use std::cmp::Ordering;

use rocksdb::DB;

use kvproto::metapb::RegionEpoch;
use kvproto::metapb::Region;

use raftstore::store::{keys, Msg};
use raftstore::store::engine::{IterOption, Iterable};
use raftstore::store::util;
use raftstore::Result;
use rocksdb::DBIterator;
use util::escape;
use util::transport::{RetryableSendCh, Sender};
use util::worker::Runnable;
use util::codec::table;
use storage::types::Key;
use storage::{CfName, LARGE_CFS};

use super::metrics::*;

#[derive(PartialEq, Eq)]
struct KeyEntry {
    key: Option<Vec<u8>>,
    pos: usize,
    value_size: usize,
}

impl KeyEntry {
    fn new(key: Vec<u8>, pos: usize, value_size: usize) -> KeyEntry {
        KeyEntry {
            key: Some(key),
            pos: pos,
            value_size: value_size,
        }
    }

    fn take(&mut self) -> KeyEntry {
        KeyEntry::new(self.key.take().unwrap(), self.pos, self.value_size)
    }

    fn len(&self) -> usize {
        self.key.as_ref().unwrap().len() + self.value_size
    }
}

impl PartialOrd for KeyEntry {
    fn partial_cmp(&self, rhs: &KeyEntry) -> Option<Ordering> {
        // BinaryHeap is max heap, so we have to reverse order to get a min heap.
        Some(
            self.key
                .as_ref()
                .unwrap()
                .cmp(rhs.key.as_ref().unwrap())
                .reverse(),
        )
    }
}

impl Ord for KeyEntry {
    fn cmp(&self, rhs: &KeyEntry) -> Ordering {
        self.partial_cmp(rhs).unwrap()
    }
}

struct MergedIterator<'a> {
    iters: Vec<DBIterator<&'a DB>>,
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
            let iter_opt = IterOption::new(Some(end_key.to_vec()), fill_cache);
            let mut iter = db.new_iterator_cf(cf, iter_opt)?;
            if iter.seek(start_key.into()) {
                heap.push(KeyEntry::new(iter.key().to_vec(), pos, iter.value().len()));
            }
            iters.push(iter);
        }
        Ok(MergedIterator {
            iters: iters,
            heap: heap,
        })
    }

    fn next(&mut self) -> Option<KeyEntry> {
        let pos = match self.heap.peek() {
            None => return None,
            Some(e) => e.pos,
        };
        let iter = &mut self.iters[pos];
        if iter.next() {
            // TODO: avoid copy key.
            let e = KeyEntry::new(iter.key().to_vec(), pos, iter.value().len());
            let mut front = self.heap.peek_mut().unwrap();
            let res = front.take();
            *front = e;
            Some(res)
        } else {
            self.heap.pop()
        }
    }
}

/// Split checking task.
pub struct Task {
    region: Region,
}

impl Task {
    pub fn new(region: &Region) -> Task {
        Task {
            region: region.clone(),
        }
    }
}

impl Display for Task {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        write!(f, "Split Check Task for {}", self.region.get_id())
    }
}

pub struct Runner<C> {
    engine: Arc<DB>,
    ch: RetryableSendCh<Msg, C>,
    region_max_size: u64,
    split_size: u64,
    split_table: bool,
}

impl<C> Runner<C> {
    pub fn new(
        engine: Arc<DB>,
        ch: RetryableSendCh<Msg, C>,
        region_max_size: u64,
        split_size: u64,
        split_table: bool,
    ) -> Runner<C> {
        Runner {
            engine: engine,
            ch: ch,
            region_max_size: region_max_size,
            split_size: split_size,
            split_table: split_table,
        }
    }
}

impl<C: Sender<Msg>> Runnable<Task> for Runner<C> {
    fn run(&mut self, task: Task) {
        let region = &task.region;
        let region_id = region.get_id();

        // Check approximate size before scanning region.
        match util::get_region_approximate_size(&self.engine, region) {
            Ok(size) => {
                if size < self.region_max_size {
                    CHECK_SPILT_COUNTER_VEC.with_label_values(&["skip"]).inc();
                    return;
                }
                info!(
                    "[region {}] approximate size {} >= {}, need to scan region",
                    region_id,
                    size,
                    self.region_max_size
                );
            }
            Err(e) => error!(
                "[region {}] failed to get approximate size: {}",
                region_id,
                e
            ),
        }

        let start_key = keys::enc_start_key(region);
        let end_key = keys::enc_end_key(region);
        debug!(
            "[region {}] executing task {} {}",
            region_id,
            escape(&start_key),
            escape(&end_key)
        );
        CHECK_SPILT_COUNTER_VEC.with_label_values(&["all"]).inc();

        let mut size = 0;
        let mut split_key = vec![];
        let mut prev_key: Option<Vec<u8>> = None;
        let timer = CHECK_SPILT_HISTOGRAM.start_coarse_timer();
        let res = MergedIterator::new(self.engine.as_ref(), LARGE_CFS, &start_key, &end_key, false)
            .map(|mut iter| while let Some(e) = iter.next() {
                if self.split_table {
                    if let (Some(ref prev_key), &Some(ref current_key)) = (prev_key, &e.key) {
                        if let Some(key) = cross_table(prev_key, current_key) {
                            info!("[region {}] split table, split key {:?}", region_id, key);
                            split_key = key;
                            break;
                        }
                    }
                    prev_key = e.key.clone();
                }

                size += e.len() as u64;
                if split_key.is_empty() && size > self.split_size {
                    split_key = e.key.unwrap();
                }
                if size >= self.region_max_size {
                    break;
                }
            });

        if let Err(e) = res {
            error!("[region {}] failed to scan split key: {}", region_id, e);
            return;
        }

        timer.observe_duration();

        if size < self.region_max_size && !self.split_table {
            debug!(
                "[region {}] no need to send for {} < {}",
                region_id,
                size,
                self.region_max_size
            );

            CHECK_SPILT_COUNTER_VEC.with_label_values(&["ignore"]).inc();
            return;
        }

        let region_epoch = region.get_region_epoch().clone();
        let res = self.ch
            .try_send(new_split_region(region_id, region_epoch, split_key));
        if let Err(e) = res {
            warn!("[region {}] failed to send check result: {}", region_id, e);
        }

        CHECK_SPILT_COUNTER_VEC
            .with_label_values(&["success"])
            .inc();
    }
}

fn cross_table(prev_key: &[u8], current_key: &[u8]) -> Option<Vec<u8>> {
    let origin_current_key = keys::origin_key(current_key);
    let raw_current_key = match Key::from_encoded(origin_current_key.to_vec()).raw() {
        Ok(k) => k,
        Err(_) => return None,
    };

    let origin_prev_key = keys::origin_key(prev_key);
    let raw_prev_key = match Key::from_encoded(origin_prev_key.to_vec()).raw() {
        Ok(k) => k,
        Err(_) => return None,
    };

    if let Ok(false) = table::is_same_table(&raw_prev_key, &raw_current_key) {
        Some(keys::data_key(
            Key::from_raw(&table::gen_table_prefix(
                table::decode_table_id(&raw_current_key),
            )).encoded(),
        ))
    } else {
        None
    }
}

fn new_split_region(region_id: u64, epoch: RegionEpoch, split_key: Vec<u8>) -> Msg {
    let key = keys::origin_key(split_key.as_slice()).to_vec();
    Msg::SplitRegion {
        region_id: region_id,
        region_epoch: epoch,
        split_key: key,
        callback: None,
    }
}

#[cfg(test)]
mod tests {
    use std::sync::mpsc::{self, TryRecvError};
    use std::sync::Arc;

    use tempdir::TempDir;
    use rocksdb::Writable;
    use kvproto::metapb::Peer;

    use storage::ALL_CFS;
    use util::rocksdb;
    use super::*;

    #[test]
    fn test_split_check() {
        let path = TempDir::new("test-raftstore").unwrap();
        let engine = Arc::new(
            rocksdb::new_engine(path.path().to_str().unwrap(), ALL_CFS).unwrap(),
        );

        let mut region = Region::new();
        region.set_id(1);
        region.set_start_key(vec![]);
        region.set_end_key(vec![]);
        region.mut_peers().push(Peer::new());
        region.mut_region_epoch().set_version(2);
        region.mut_region_epoch().set_conf_ver(5);

        let (tx, rx) = mpsc::sync_channel(100);
        let ch = RetryableSendCh::new(tx, "test-split");
        let mut runnable = Runner::new(engine.clone(), ch, 100, 60, false);

        // so split key will be z0006
        for i in 0..7 {
            let s = keys::data_key(format!("{:04}", i).as_bytes());
            engine.put(&s, &s).unwrap();
        }

        runnable.run(Task::new(&region));
        // size has not reached the max_size 100 yet.
        match rx.try_recv() {
            Err(TryRecvError::Empty) => {}
            others => panic!("expect recv empty, but got {:?}", others),
        }

        for i in 7..11 {
            let s = keys::data_key(format!("{:04}", i).as_bytes());
            engine.put(&s, &s).unwrap();
        }

        // Approximate size of memtable is inaccurate for small data,
        // we flush it to SST so we can use the size properties instead.
        engine.flush(true).unwrap();

        runnable.run(Task::new(&region));
        match rx.try_recv() {
            Ok(Msg::SplitRegion {
                region_id,
                region_epoch,
                split_key,
                ..
            }) => {
                assert_eq!(region_id, region.get_id());
                assert_eq!(&region_epoch, region.get_region_epoch());
                assert_eq!(split_key, b"0006");
            }
            others => panic!("expect split check result, but got {:?}", others),
        }

        // So split key will be z0003
        for i in 0..6 {
            let s = keys::data_key(format!("{:04}", i).as_bytes());
            for cf in ALL_CFS {
                let handle = engine.cf_handle(cf).unwrap();
                engine.put_cf(handle, &s, &s).unwrap();
            }
        }
        for cf in ALL_CFS {
            let handle = engine.cf_handle(cf).unwrap();
            engine.flush_cf(handle, true).unwrap();
        }

        runnable.run(Task::new(&region));
        match rx.try_recv() {
            Ok(Msg::SplitRegion {
                region_id,
                region_epoch,
                split_key,
                ..
            }) => {
                assert_eq!(region_id, region.get_id());
                assert_eq!(&region_epoch, region.get_region_epoch());
                assert_eq!(split_key, b"0003");
            }
            others => panic!("expect split check result, but got {:?}", others),
        }

        drop(rx);
        // It should be safe even the result can't be sent back.
        runnable.run(Task::new(&region));
    }

    #[test]
    fn test_split_table() {
        let path = TempDir::new("test-raftstore").unwrap();
        let engine = Arc::new(
            rocksdb::new_engine(path.path().to_str().unwrap(), ALL_CFS).unwrap(),
        );

        let mut region = Region::new();
        region.set_id(1);
        region.mut_peers().push(Peer::new());
        region.mut_region_epoch().set_version(2);
        region.mut_region_epoch().set_conf_ver(5);

        let (table_tx, table_rx) = mpsc::sync_channel(100);
        let table_ch = RetryableSendCh::new(table_tx, "test-split-table");
        let mut table_runnable = Runner::new(engine.clone(), table_ch, 100, 60, true);

        let check = |msg: Msg, key: Vec<u8>| match msg {
            Msg::SplitRegion { split_key, .. } => {
                assert_eq!(&split_key, Key::from_raw(&key).encoded())
            }
            others => panic!("expect split check result, but got {:?}", others),
        };

        // arbitrary padding.
        let padding = b"_r00000005";

        // Put some data
        // t1_xx, t3_xx, t5_xx
        for i in 1..6 {
            if i % 2 == 0 {
                // leave some space.
                continue;
            }

            let mut key = table::gen_table_prefix(i);
            key.extend_from_slice(padding);
            let s = keys::data_key(Key::from_raw(&key).encoded());
            engine.put(&s, &s).unwrap();
        }
        engine.flush(true).unwrap();

        // ["", "") => t3
        region.set_start_key(vec![]);
        region.set_end_key(vec![]);
        table_runnable.run(Task::new(&region));
        check(table_rx.try_recv().unwrap(), table::gen_table_prefix(3));

        // ["t1", "") => t3
        region.set_start_key(Key::from_raw(&table::gen_table_prefix(1)).encoded().clone());
        region.set_end_key(vec![]);
        table_runnable.run(Task::new(&region));
        check(table_rx.try_recv().unwrap(), table::gen_table_prefix(3));

        // ["t1", "t5") => t3
        region.set_start_key(Key::from_raw(&table::gen_table_prefix(1)).encoded().clone());
        region.set_end_key(Key::from_raw(&table::gen_table_prefix(5)).encoded().clone());
        table_runnable.run(Task::new(&region));
        check(table_rx.try_recv().unwrap(), table::gen_table_prefix(3));

        // Put some data to table 3.
        for i in 0..3 {
            // Each kv entry takes about 56 bytes.
            let mut key = table::gen_table_prefix(3);
            key.extend_from_slice(format!("_r0000000{}", i).as_bytes());
            let s = keys::data_key(Key::from_raw(&key).encoded());
            engine.put(&s, &s).unwrap();
        }
        // Since region_max_size = 100, split_size = 60
        // for ["t3", ""), the split key should be in table 3.
        region.set_start_key(Key::from_raw(&table::gen_table_prefix(3)).encoded().clone());
        region.set_end_key(vec![]);
        table_runnable.run(Task::new(&region));
        match table_rx.try_recv() {
            Ok(Msg::SplitRegion { split_key, .. }) => {
                let key = Key::from_encoded(split_key).raw().unwrap();
                assert_eq!(table::decode_table_id(&key), 3);
            }
            others => panic!("expect split check result, but got {:?}", others),
        }
    }
}
