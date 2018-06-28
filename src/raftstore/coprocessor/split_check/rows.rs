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

use raftstore::store::{util, Msg};
use rocksdb::DB;
use util::transport::{RetryableSendCh, Sender};

use super::super::metrics::*;
use super::super::{Coprocessor, KeyEntry, ObserverContext, SplitCheckObserver, SplitChecker};
use super::Host;

pub struct Checker {
    max_rows: u64,
    split_rows: u64,
    current_rows: u64,
    split_key: Option<Vec<u8>>,
}

impl Checker {
    pub fn new(max_rows: u64, split_rows: u64) -> Checker {
        Checker {
            max_rows,
            split_rows,
            current_rows: 0,
            split_key: None,
        }
    }
}

impl SplitChecker for Checker {
    fn on_kv(&mut self, _: &mut ObserverContext, row: &KeyEntry) -> bool {
        if row.is_from_write_cf() {
            self.current_rows += 1;
        }
        if self.current_rows > self.split_rows && self.split_key.is_none() {
            self.split_key = Some(row.key().to_vec());
        }
        self.current_rows > self.max_rows
    }

    fn split_key(&mut self) -> Option<Vec<u8>> {
        if self.current_rows >= self.max_rows {
            self.split_key.take()
        } else {
            None
        }
    }
}

pub struct RowsCheckObserver<C> {
    region_max_rows: u64,
    split_rows: u64,
    ch: RetryableSendCh<Msg, C>,
}

impl<C: Sender<Msg>> RowsCheckObserver<C> {
    pub fn new(
        region_max_rows: u64,
        split_rows: u64,
        ch: RetryableSendCh<Msg, C>,
    ) -> RowsCheckObserver<C> {
        RowsCheckObserver {
            region_max_rows,
            split_rows,
            ch,
        }
    }
}

impl<C> Coprocessor for RowsCheckObserver<C> {}

impl<C: Sender<Msg> + Send> SplitCheckObserver for RowsCheckObserver<C> {
    fn add_checker(&self, ctx: &mut ObserverContext, host: &mut Host, engine: &DB) {
        let region = ctx.region();
        let region_id = region.get_id();
        let region_rows = match util::get_region_approximate_rows(engine, region) {
            Ok(rows) => rows,
            Err(e) => {
                warn!(
                    "[region {}] failed to get approximate rows: {}",
                    region_id, e
                );
                // Need to check rows.
                host.add_checker(Box::new(Checker::new(
                    self.region_max_rows,
                    self.split_rows,
                )));
                return;
            }
        };

        let res = Msg::RegionApproximateRows {
            region_id,
            rows: region_rows,
        };
        if let Err(e) = self.ch.try_send(res) {
            warn!(
                "[region {}] failed to send approximate region rows: {}",
                region_id, e
            );
        }

        REGION_ROWS_HISTOGRAM.observe(region_rows as f64);
        if region_rows >= self.region_max_rows {
            info!(
                "[region {}] approximate rows {} >= {}, need to do split check",
                region.get_id(),
                region_rows,
                self.region_max_rows
            );
            // Need to check rows.
            host.add_checker(Box::new(Checker::new(
                self.region_max_rows,
                self.split_rows,
            )));
        } else {
            // Does not need to check rows.
            debug!(
                "[region {}] approximate rows {} < {}, does not need to do split check",
                region.get_id(),
                region_rows,
                self.region_max_rows
            );
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::mpsc;
    use std::sync::Arc;

    use kvproto::metapb::Peer;
    use kvproto::metapb::Region;
    use rocksdb::Writable;
    use rocksdb::{ColumnFamilyOptions, DBOptions};
    use tempdir::TempDir;

    use raftstore::store::{keys, Msg, SplitCheckRunner, SplitCheckTask};
    use storage::mvcc::{Write, WriteType};
    use storage::{Key, ALL_CFS, CF_DEFAULT, CF_WRITE};
    use util::properties::MvccPropertiesCollectorFactory;
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
        let f = Box::new(MvccPropertiesCollectorFactory::default());
        cf_opts.add_table_properties_collector_factory("tikv.mvcc-properties-collector", f);

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
        cfg.region_max_rows = 100;
        cfg.region_split_rows = 80;

        let mut runnable = SplitCheckRunner::new(
            Arc::clone(&engine),
            ch.clone(),
            Arc::new(CoprocessorHost::new(cfg, ch.clone())),
        );

        // so split key will be z0080
        for i in 0..90 {
            let key = keys::data_key(
                Key::from_raw(format!("{:04}", i).as_bytes())
                    .append_ts(2)
                    .encoded(),
            );
            let write_value = Write::new(WriteType::Put, 0, None).to_bytes();
            let write_cf = engine.cf_handle(CF_WRITE).unwrap();
            engine.put_cf(write_cf, &key, &write_value).unwrap();
            engine.flush_cf(write_cf, true).unwrap();
            let default_cf = engine.cf_handle(CF_DEFAULT).unwrap();
            engine.put_cf(default_cf, &key, &[0; 1024]).unwrap();
            engine.flush_cf(default_cf, true).unwrap();
        }

        runnable.run(SplitCheckTask::new(region.clone(), true));
        // rows has not reached the max_rows 100 yet.
        match rx.try_recv() {
            Ok(Msg::RegionApproximateSize { region_id, .. })
            | Ok(Msg::RegionApproximateRows { region_id, .. }) => {
                assert_eq!(region_id, region.get_id());
            }
            others => panic!("expect recv empty, but got {:?}", others),
        }

        for i in 90..160 {
            let key = keys::data_key(
                Key::from_raw(format!("{:04}", i).as_bytes())
                    .append_ts(2)
                    .encoded(),
            );

            let write_value =
                Write::new(WriteType::Put, 0, Some(b"shortvalue".to_vec())).to_bytes();
            let write_cf = engine.cf_handle(CF_WRITE).unwrap();
            engine.put_cf(write_cf, &key, &write_value).unwrap();
            engine.flush_cf(write_cf, true).unwrap();
            let default_cf = engine.cf_handle(CF_DEFAULT).unwrap();
            engine.put_cf(default_cf, &key, &[0; 1024]).unwrap();
            engine.flush_cf(default_cf, true).unwrap();
        }

        runnable.run(SplitCheckTask::new(region.clone(), true));
        loop {
            match rx.try_recv() {
                Ok(Msg::SplitRegion {
                    region_id,
                    region_epoch,
                    split_key,
                    ..
                }) => {
                    assert_eq!(region_id, region.get_id());
                    assert_eq!(&region_epoch, region.get_region_epoch());
                    assert_eq!(&split_key, Key::from_raw(b"0080").append_ts(2).encoded());
                    break;
                }
                Ok(Msg::RegionApproximateSize { region_id, .. })
                | Ok(Msg::RegionApproximateRows { region_id, .. }) => {
                    assert_eq!(region_id, region.get_id());
                }
                others => panic!(
                    "expect split check result or region's stat, but got {:?}",
                    others
                ),
            }
        }

        drop(rx);
        // It should be safe even the result can't be sent back.
        runnable.run(SplitCheckTask::new(region, true));
    }
}
