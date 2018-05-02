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

use std::cmp::Ordering;

use kvproto::metapb::Region;
use rocksdb::{SeekKey, DB};

use coprocessor::codec::table as table_codec;
use raftstore::store::engine::{IterOption, Iterable};
use raftstore::store::keys;
use storage::CF_WRITE;
use storage::types::Key;
use util::escape;

use super::super::{Coprocessor, ObserverContext, Result, SplitCheckObserver};
use super::Status;

#[derive(Default)]
pub struct TableStatus {
    first_encoded_table_prefix: Option<Vec<u8>>,
    last_encoded_table_prefix: Option<Vec<u8>>,
}

#[derive(Default)]
pub struct TableCheckObserver;

impl Coprocessor for TableCheckObserver {}

impl SplitCheckObserver for TableCheckObserver {
    fn new_split_check_status(&self, ctx: &mut ObserverContext, status: &mut Status, engine: &DB) {
        let mut table_status = TableStatus::default();
        let skip = before_check(&mut table_status, engine, ctx.region());
        if !skip {
            status.table = Some(table_status);
        }
    }

    fn on_split_check(
        &self,
        _: &mut ObserverContext,
        status: &mut Status,
        key: &[u8],
        _: u64,
    ) -> Option<Vec<u8>> {
        if let Some(ref mut table_status) = status.table {
            check_key(table_status, key)
        } else {
            None
        }
    }
}

/// Do some quick checks, true for skipping `check_key`.
fn before_check(status: &mut TableStatus, engine: &DB, region: &Region) -> bool {
    if is_same_table(region.get_start_key(), region.get_end_key()) {
        // Region is inside a table, skip for saving IO.
        return true;
    }

    let end_key = match last_key_of_region(engine, region) {
        Ok(Some(end_key)) => end_key,
        Ok(None) => return true,
        Err(err) => {
            error!(
                "[region {}] failed to get region last key: {}",
                region.get_id(),
                err
            );
            return true;
        }
    };

    let encoded_start_key = region.get_start_key();
    let encoded_end_key = keys::origin_key(&end_key);

    if encoded_start_key.len() < table_codec::TABLE_PREFIX_KEY_LEN
        || encoded_end_key.len() < table_codec::TABLE_PREFIX_KEY_LEN
    {
        // For now, let us scan region if encoded_start_key or encoded_end_key
        // is less than TABLE_PREFIX_KEY_LEN.
        return false;
    }

    // Table data starts with `TABLE_PREFIX`.
    // Find out the actual range of this region by comparing with `TABLE_PREFIX`.
    match (
        encoded_start_key[..table_codec::TABLE_PREFIX_LEN].cmp(table_codec::TABLE_PREFIX),
        encoded_end_key[..table_codec::TABLE_PREFIX_LEN].cmp(table_codec::TABLE_PREFIX),
    ) {
        // The range does not cover table data.
        (Ordering::Less, Ordering::Less) | (Ordering::Greater, Ordering::Greater) => true,

        // Following arms matches when the region contains table data.
        // Covers all table data.
        (Ordering::Less, Ordering::Greater) => false,
        // The later part contains table data.
        (Ordering::Less, Ordering::Equal) => {
            // It starts from non-table area to table area,
            // try to extract a split key from `encoded_end_key`, and save it in status.
            status.last_encoded_table_prefix = to_encoded_table_prefix(encoded_end_key);
            false
        }
        // Region is in table area.
        (Ordering::Equal, Ordering::Equal) => {
            if is_same_table(encoded_start_key, encoded_end_key) {
                // Same table.
                true
            } else {
                // Different tables.
                // Note that table id does not grow by 1, so have to use
                // `encoded_end_key` to extract a table prefix.
                // See more: https://github.com/pingcap/tidb/issues/4727
                status.last_encoded_table_prefix = to_encoded_table_prefix(encoded_end_key);
                false
            }
        }
        // The region starts from tabel area to non-table area.
        (Ordering::Equal, Ordering::Greater) => {
            // As the comment above, outside needs scan for finding a split key.
            status.first_encoded_table_prefix = to_encoded_table_prefix(encoded_start_key);
            false
        }
        _ => panic!(
            "start_key {:?} and end_key {:?} out of order",
            escape(encoded_start_key),
            escape(encoded_end_key)
        ),
    }
}

/// Feed keys in order to find the split key.
/// If `current_data_key` does not belong to `status.first_encoded_table_prefix`.
/// it returns the encoded table prefix of `current_data_key`.
fn check_key(status: &mut TableStatus, current_data_key: &[u8]) -> Option<Vec<u8>> {
    if let Some(last_encoded_table_prefix) = status.last_encoded_table_prefix.take() {
        // `before_check` found a split key.
        return Some(keys::data_key(&last_encoded_table_prefix));
    }

    let current_encoded_key = keys::origin_key(current_data_key);

    let split_key = if status.first_encoded_table_prefix.is_some() {
        if !is_same_table(
            status.first_encoded_table_prefix.as_ref().unwrap(),
            current_encoded_key,
        ) {
            // Different tables.
            Some(current_encoded_key)
        } else {
            None
        }
    } else if is_table_key(current_encoded_key) {
        // Now we meet the very first table key of this region.
        Some(current_encoded_key)
    } else {
        None
    };
    if let Some(key) = split_key {
        to_encoded_table_prefix(key).map(|k| keys::data_key(&k))
    } else {
        None
    }
}

fn last_key_of_region(db: &DB, region: &Region) -> Result<Option<Vec<u8>>> {
    let start_key = keys::enc_start_key(region);
    let end_key = keys::enc_end_key(region);
    let mut last_key = None;

    let iter_opt = IterOption::new(Some(start_key), Some(end_key), false);
    let mut iter = box_try!(db.new_iterator_cf(CF_WRITE, iter_opt));

    // the last key
    if iter.seek(SeekKey::End) {
        let key = iter.key().to_vec();
        last_key = Some(key);
    } // else { No data in this CF }

    match last_key {
        Some(lk) => Ok(Some(lk)),
        None => Ok(None),
    }
}

fn to_encoded_table_prefix(encoded_key: &[u8]) -> Option<Vec<u8>> {
    if let Ok(raw_key) = Key::from_encoded(encoded_key.to_vec()).raw() {
        table_codec::extract_table_prefix(&raw_key)
            .map(|k| Key::from_raw(k).encoded().to_vec())
            .ok()
    } else {
        None
    }
}

// Encode a key like `t{i64}` will append some unnecessary bytes to the output,
// The first 10 bytes are enough to find out which table this key belongs to.
const ENCODED_TABLE_TABLE_PREFIX: usize = table_codec::TABLE_PREFIX_KEY_LEN + 1;

fn is_table_key(encoded_key: &[u8]) -> bool {
    encoded_key.starts_with(table_codec::TABLE_PREFIX)
        && encoded_key.len() >= ENCODED_TABLE_TABLE_PREFIX
}

fn is_same_table(left_key: &[u8], right_key: &[u8]) -> bool {
    is_table_key(left_key) && is_table_key(right_key)
        && left_key[..ENCODED_TABLE_TABLE_PREFIX] == right_key[..ENCODED_TABLE_TABLE_PREFIX]
}

#[cfg(test)]
mod test {
    use std::io::Write;
    use std::sync::Arc;
    use std::sync::mpsc;

    use kvproto::metapb::Peer;
    use rocksdb::Writable;
    use tempdir::TempDir;

    use coprocessor::codec::table::{TABLE_PREFIX, TABLE_PREFIX_KEY_LEN};
    use raftstore::store::{Msg, SplitCheckRunner, SplitCheckTask};
    use storage::ALL_CFS;
    use storage::types::Key;
    use util::codec::number::NumberEncoder;
    use util::config::ReadableSize;
    use util::rocksdb::new_engine;
    use util::transport::RetryableSendCh;
    use util::worker::Runnable;

    use super::*;
    use raftstore::coprocessor::{Config, CoprocessorHost};

    /// Composes table record and index prefix: `t[table_id]`.
    // Port from TiDB
    fn gen_table_prefix(table_id: i64) -> Vec<u8> {
        let mut buf = Vec::with_capacity(TABLE_PREFIX_KEY_LEN);
        buf.write_all(TABLE_PREFIX).unwrap();
        buf.encode_i64(table_id).unwrap();
        buf
    }

    #[test]
    fn test_last_key_of_region() {
        let path = TempDir::new("test_last_key_of_region").unwrap();
        let engine = Arc::new(new_engine(path.path().to_str().unwrap(), ALL_CFS, None).unwrap());
        let write_cf = engine.cf_handle(CF_WRITE).unwrap();

        let mut region = Region::new();
        region.set_id(1);
        region.mut_peers().push(Peer::new());

        // arbitrary padding.
        let padding = b"_r00000005";
        // Put keys, t1_xxx, t2_xxx
        let mut data_keys = vec![];
        for i in 1..3 {
            let mut key = gen_table_prefix(i);
            key.extend_from_slice(padding);
            let k = keys::data_key(Key::from_raw(&key).encoded());
            engine.put_cf(write_cf, &k, &k).unwrap();
            data_keys.push(k)
        }

        type Case = (Option<i64>, Option<i64>, Option<Vec<u8>>);
        let mut check_cases = |cases: Vec<Case>| {
            for (start_id, end_id, want) in cases {
                region.set_start_key(
                    start_id
                        .map(|id| Key::from_raw(&gen_table_prefix(id)).encoded().to_vec())
                        .unwrap_or_else(Vec::new),
                );
                region.set_end_key(
                    end_id
                        .map(|id| Key::from_raw(&gen_table_prefix(id)).encoded().to_vec())
                        .unwrap_or_else(Vec::new),
                );
                assert_eq!(last_key_of_region(&engine, &region).unwrap(), want);
            }
        };

        check_cases(vec![
            // ["", "") => t2_xx
            (None, None, data_keys.get(1).cloned()),
            // ["", "t1") => None
            (None, Some(1), None),
            // ["t1", "") => t2_xx
            (Some(1), None, data_keys.get(1).cloned()),
            // ["t1", "t2") => t1_xx
            (Some(1), Some(2), data_keys.get(0).cloned()),
        ]);
    }

    #[test]
    fn test_table_check_observer() {
        let path = TempDir::new("test_table_check_observer").unwrap();
        let engine = Arc::new(new_engine(path.path().to_str().unwrap(), ALL_CFS, None).unwrap());
        let write_cf = engine.cf_handle(CF_WRITE).unwrap();

        let mut region = Region::new();
        region.set_id(1);
        region.mut_peers().push(Peer::new());
        region.mut_region_epoch().set_version(2);
        region.mut_region_epoch().set_conf_ver(5);

        let (tx, rx) = mpsc::sync_channel(100);
        let ch = RetryableSendCh::new(tx, "test-split-table");
        let (stx, _rx) = mpsc::sync_channel::<Msg>(100);
        let sch = RetryableSendCh::new(stx, "test-split-size");

        let mut cfg = Config::default();
        // Enable table split.
        cfg.split_region_on_table = true;

        // Try to "disable" size split.
        cfg.region_max_size = ReadableSize::gb(2);
        cfg.region_split_size = ReadableSize::gb(1);

        // Try to ignore the ApproximateRegionSize
        let coprocessor = CoprocessorHost::new(cfg, sch);
        let mut runnable =
            SplitCheckRunner::new(Arc::clone(&engine), ch.clone(), Arc::new(coprocessor));

        type Case = (Option<Vec<u8>>, Option<Vec<u8>>, Option<i64>);
        let mut check_cases = |cases: Vec<Case>| {
            for (encoded_start_key, encoded_end_key, table_id) in cases {
                region.set_start_key(encoded_start_key.unwrap_or_else(Vec::new));
                region.set_end_key(encoded_end_key.unwrap_or_else(Vec::new));
                runnable.run(SplitCheckTask::new(&region, true));

                if let Some(id) = table_id {
                    let key = Key::from_raw(&gen_table_prefix(id));
                    match rx.try_recv() {
                        Ok(Msg::SplitRegion { split_key, .. }) => {
                            assert_eq!(&split_key, key.encoded());
                        }
                        others => panic!("expect {:?}, but got {:?}", key, others),
                    }
                } else {
                    match rx.try_recv() {
                        Err(mpsc::TryRecvError::Empty) => (),
                        others => panic!("expect empty, but got {:?}", others),
                    }
                }
            }
        };

        let gen_encoded_table_prefix = |table_id| {
            let key = Key::from_raw(&gen_table_prefix(table_id));
            key.encoded().to_vec()
        };

        // arbitrary padding.
        let padding = b"_r00000005";

        // Put some tables
        // t1_xx, t3_xx
        for i in 1..4 {
            if i % 2 == 0 {
                // leave some space.
                continue;
            }

            let mut key = gen_table_prefix(i);
            key.extend_from_slice(padding);
            let s = keys::data_key(Key::from_raw(&key).encoded());
            engine.put_cf(write_cf, &s, &s).unwrap();
        }

        check_cases(vec![
            // ["", "") => t1
            (None, None, Some(1)),
            // ["t1", "") => t3
            (Some(gen_encoded_table_prefix(1)), None, Some(3)),
            // ["t1", "t5") => t3
            (
                Some(gen_encoded_table_prefix(1)),
                Some(gen_encoded_table_prefix(5)),
                Some(3),
            ),
            // ["t2", "t4") => t3
            (
                Some(gen_encoded_table_prefix(2)),
                Some(gen_encoded_table_prefix(4)),
                Some(3),
            ),
        ]);

        // Put some data to t3
        for i in 1..4 {
            let mut key = gen_table_prefix(3);
            key.extend_from_slice(format!("{:?}{}", padding, i).as_bytes());
            let s = keys::data_key(Key::from_raw(&key).encoded());
            engine.put_cf(write_cf, &s, &s).unwrap();
        }

        check_cases(vec![
            // ["t1", "") => t3
            (Some(gen_encoded_table_prefix(1)), None, Some(3)),
            // ["t3", "") => skip
            (Some(gen_encoded_table_prefix(3)), None, None),
            // ["t3", "t5") => skip
            (
                Some(gen_encoded_table_prefix(3)),
                Some(gen_encoded_table_prefix(5)),
                None,
            ),
        ]);

        // Put some data before t and after t.
        for i in 0..3 {
            // m is less than t and is the prefix of meta keys.
            let key = format!("m{:?}{}", padding, i);
            let s = keys::data_key(Key::from_raw(key.as_bytes()).encoded());
            engine.put_cf(write_cf, &s, &s).unwrap();
            let key = format!("u{:?}{}", padding, i);
            let s = keys::data_key(Key::from_raw(key.as_bytes()).encoded());
            engine.put_cf(write_cf, &s, &s).unwrap();
        }

        check_cases(vec![
            // ["", "") => t1
            (None, None, Some(1)),
            // ["", "t1"] => skip
            (None, Some(gen_encoded_table_prefix(1)), None),
            // ["", "t3"] => t1
            (None, Some(gen_encoded_table_prefix(3)), Some(1)),
            // ["", "s"] => skip
            (None, Some(b"s".to_vec()), None),
            // ["u", ""] => skip
            (Some(b"u".to_vec()), None, None),
            // ["t3", ""] => None
            (Some(gen_encoded_table_prefix(3)), None, None),
            // ["t1", ""] => t3
            (Some(gen_encoded_table_prefix(1)), None, Some(3)),
        ]);
    }
}
