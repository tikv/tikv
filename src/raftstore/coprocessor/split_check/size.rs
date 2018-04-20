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

use raftstore::store::{util, Msg};
use rocksdb::DB;
use util::transport::{RetryableSendCh, Sender};

use super::super::metrics::*;
use super::super::{Coprocessor, ObserverContext, SplitCheckObserver};
use super::Status;

#[derive(Default)]
pub struct SizeStatus {
    current_size: u64,
    split_key: Option<Vec<u8>>,
}

pub struct SizeCheckObserver<C> {
    region_max_size: u64,
    split_size: u64,
    ch: RetryableSendCh<Msg, C>,
}

impl<C: Sender<Msg>> SizeCheckObserver<C> {
    pub fn new(
        region_max_size: u64,
        split_size: u64,
        ch: RetryableSendCh<Msg, C>,
    ) -> SizeCheckObserver<C> {
        SizeCheckObserver {
            region_max_size,
            split_size,
            ch,
        }
    }
}

impl<C> Coprocessor for SizeCheckObserver<C> {}

impl<C: Sender<Msg> + Send> SplitCheckObserver for SizeCheckObserver<C> {
    fn new_split_check_status(&self, ctx: &mut ObserverContext, status: &mut Status, engine: &DB) {
        let size_status = SizeStatus::default();
        let region = ctx.region();
        let region_id = region.get_id();
        let region_size = match util::get_region_approximate_size(engine, region) {
            Ok(size) => size,
            Err(e) => {
                error!(
                    "[region {}] failed to get approximate size: {}",
                    region_id, e
                );
                // Need to check size.
                status.size = Some(size_status);
                return;
            }
        };

        let res = Msg::ApproximateRegionSize {
            region_id,
            region_size,
        };
        if let Err(e) = self.ch.try_send(res) {
            error!(
                "[region {}] failed to send approximate region size: {}",
                region_id, e
            );
        }

        REGION_SIZE_HISTOGRAM.observe(region_size as f64);
        if region_size >= self.region_max_size {
            info!(
                "[region {}] approximate size {} >= {}, need to do split check",
                region.get_id(),
                region_size,
                self.region_max_size
            );
            // Need to check size.
            status.size = Some(size_status);
        } else {
            // Does not need to check size.
            debug!(
                "[region {}] approximate size {} < {}, does not need to do split check",
                region.get_id(),
                region_size,
                self.region_max_size
            );
        }
    }

    fn on_split_check(
        &self,
        _: &mut ObserverContext,
        status: &mut Status,
        key: &[u8],
        value_size: u64,
    ) -> Option<Vec<u8>> {
        if let Some(size_status) = status.size.as_mut() {
            size_status.current_size += key.len() as u64 + value_size;
            if size_status.current_size > self.split_size && size_status.split_key.is_none() {
                size_status.split_key = Some(key.to_vec());
            }
            if size_status.current_size >= self.region_max_size {
                size_status.split_key.take()
            } else {
                None
            }
        } else {
            None
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::sync::mpsc;

    use kvproto::metapb::Peer;
    use kvproto::metapb::Region;
    use rocksdb::Writable;
    use rocksdb::{ColumnFamilyOptions, DBOptions};
    use tempdir::TempDir;

    use raftstore::store::{keys, Msg, SplitCheckRunner, SplitCheckTask};
    use storage::ALL_CFS;
    use util::config::ReadableSize;
    use util::properties::SizePropertiesCollectorFactory;
    use util::rocksdb::{new_engine_opt, CFOptions};
    use util::transport::RetryableSendCh;
    use util::worker::Runnable;

    use raftstore::coprocessor::{Config, CoprocessorHost};

    #[test]
    fn test_split_check() {
        let path = TempDir::new("test-raftstore").unwrap();
        let path_str = path.path().to_str().unwrap();
        let db_opts = DBOptions::new();
        let mut cf_opts = ColumnFamilyOptions::new();
        let f = Box::new(SizePropertiesCollectorFactory::default());
        cf_opts.add_table_properties_collector_factory("tikv.size-collector", f);
        let cfs_opts = ALL_CFS
            .iter()
            .map(|cf| CFOptions::new(cf, cf_opts.clone()))
            .collect();
        let engine = Arc::new(new_engine_opt(path_str, db_opts, cfs_opts).unwrap());

        let mut region = Region::new();
        region.set_id(1);
        region.set_start_key(vec![]);
        region.set_end_key(vec![]);
        region.mut_peers().push(Peer::new());
        region.mut_region_epoch().set_version(2);
        region.mut_region_epoch().set_conf_ver(5);

        let (tx, rx) = mpsc::sync_channel(100);
        let ch = RetryableSendCh::new(tx, "test-split");
        let mut cfg = Config::default();
        cfg.region_max_size = ReadableSize(100);
        cfg.region_split_size = ReadableSize(60);

        let mut runnable = SplitCheckRunner::new(
            Arc::clone(&engine),
            ch.clone(),
            Arc::new(CoprocessorHost::new(cfg, ch.clone())),
        );

        // so split key will be z0006
        for i in 0..7 {
            let s = keys::data_key(format!("{:04}", i).as_bytes());
            engine.put(&s, &s).unwrap();
        }

        runnable.run(SplitCheckTask::new(&region, true));
        // size has not reached the max_size 100 yet.
        match rx.try_recv() {
            Ok(Msg::ApproximateRegionSize { region_id, .. }) => {
                assert_eq!(region_id, region.get_id());
            }
            others => panic!("expect recv empty, but got {:?}", others),
        }

        for i in 7..11 {
            let s = keys::data_key(format!("{:04}", i).as_bytes());
            engine.put(&s, &s).unwrap();
        }

        // Approximate size of memtable is inaccurate for small data,
        // we flush it to SST so we can use the size properties instead.
        engine.flush(true).unwrap();

        runnable.run(SplitCheckTask::new(&region, true));
        match rx.try_recv() {
            Ok(Msg::ApproximateRegionSize { region_id, .. }) => {
                assert_eq!(region_id, region.get_id());
            }
            others => panic!("expect approximate region size, but got {:?}", others),
        }
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

        runnable.run(SplitCheckTask::new(&region, true));
        match rx.try_recv() {
            Ok(Msg::ApproximateRegionSize { region_id, .. }) => {
                assert_eq!(region_id, region.get_id());
            }
            others => panic!("expect approximate region size, but got {:?}", others),
        }
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
        runnable.run(SplitCheckTask::new(&region, true));
    }
}
