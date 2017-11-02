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

use std::result::Result as StdResult;
use std::cmp::Ordering;

use rocksdb::{SeekKey, DB};
use kvproto::metapb::Region;

use storage::CF_WRITE;
use storage::types::Key;
use raftstore::store::keys;
use raftstore::store::engine::{IterOption, Iterable};
use coprocessor::codec::table as table_codec;

use super::super::{Coprocessor, ObserverContext, RegionObserver, Result};
use super::Status;

#[derive(Default)]
pub struct TableStatus {
    first_encoded_table_prefix: Option<Vec<u8>>,
    last_encoded_table_prefix: Option<Vec<u8>>,
}

#[derive(Default)]
pub struct TableCheckObserver;

impl Coprocessor for TableCheckObserver {}

impl RegionObserver for TableCheckObserver {
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
    if let Ok(true) = is_same_table(region.get_start_key(), region.get_end_key()) {
        // Region is inside a table, skip for saving IO.
        return true;
    }

    let (start_key, end_key) = match bound_keys(engine, region) {
        Ok(Some((start_key, end_key))) => (start_key, end_key),
        Ok(None) => return true,
        Err(err) => {
            error!(
                "[region {}] failed to get region bound: {}",
                region.get_id(),
                err
            );
            return true;
        }
    };

    if !keys::validate_data_key(&start_key) || !keys::validate_data_key(&end_key) {
        return true;
    }

    let encoded_start_key = keys::origin_key(&start_key);
    let encoded_end_key = keys::origin_key(&end_key);

    if encoded_start_key.len() < table_codec::TABLE_PREFIX_KEY_LEN ||
        encoded_end_key.len() < table_codec::TABLE_PREFIX_KEY_LEN
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
            if let Ok(true) = is_same_table(encoded_start_key, encoded_end_key) {
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
            start_key,
            end_key
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

    if !keys::validate_data_key(current_data_key) {
        return None;
    }
    let current_encoded_key = keys::origin_key(current_data_key);

    let split_key = if status.first_encoded_table_prefix.is_some() {
        if let Ok(false) = is_same_table(
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

#[allow(collapsible_if)]
fn bound_keys(db: &DB, region: &Region) -> Result<Option<(Vec<u8>, Vec<u8>)>> {
    let start_key = keys::enc_start_key(region);
    let end_key = keys::enc_end_key(region);
    let mut first_key = None;
    let mut last_key = None;

    let iter_opt = IterOption::new(Some(end_key), false);
    let mut iter = box_try!(db.new_iterator_cf(CF_WRITE, iter_opt));

    // the first key
    if iter.seek(start_key.as_slice().into()) {
        let key = iter.key().to_vec();
        first_key = Some(key);
    } // else { No data in this CF }

    // the last key
    if iter.seek(SeekKey::End) {
        let key = iter.key().to_vec();
        last_key = Some(key);
    } // else { No data in this CF }

    match (first_key, last_key) {
        (Some(fk), Some(lk)) => Ok(Some((fk, lk))),
        (None, None) => Ok(None),
        (first_key, last_key) => Err(box_err!(
            "invalid bound, first key: {:?}, last key: {:?}",
            first_key,
            last_key
        )),
    }
}

fn to_encoded_table_prefix(encoded_key: &[u8]) -> Option<Vec<u8>> {
    table_codec::decode_table_id(&Key::from_encoded(encoded_key.to_vec()).raw().unwrap())
        .map(table_codec::gen_table_prefix)
        .map(|k| Key::from_raw(&k).encoded().to_vec())
        .ok()
}

// Encode a key like `t{i64}` will append some unecessary bytes to the output,
// This function extracts the first 10 bytes, they are enough to find out which
// table this key belongs to.
const ENCODED_TABLE_TABLE_PREFIX: usize = table_codec::TABLE_PREFIX_KEY_LEN + 1;

fn is_table_key(encoded_key: &[u8]) -> bool {
    encoded_key.starts_with(table_codec::TABLE_PREFIX) &&
        encoded_key.len() >= ENCODED_TABLE_TABLE_PREFIX
}

fn is_same_table<'a>(left_key: &'a [u8], right_key: &'a [u8]) -> StdResult<bool, &'a [u8]> {
    if !is_table_key(left_key) {
        Err(left_key)
    } else if !is_table_key(right_key) {
        Err(right_key)
    } else {
        Ok(
            left_key[..ENCODED_TABLE_TABLE_PREFIX] == right_key[..ENCODED_TABLE_TABLE_PREFIX],
        )
    }
}

#[cfg(test)]
mod test {
    use std::sync::Arc;
    use std::sync::mpsc;

    use tempdir::TempDir;
    use rocksdb::Writable;
    use kvproto::metapb::Peer;

    use storage::ALL_CFS;
    use storage::types::Key;
    use raftstore::store::{Msg, SplitCheckRunner, SplitCheckTask};
    use util::rocksdb::new_engine;
    use util::worker::Runnable;
    use util::transport::RetryableSendCh;
    use util::config::ReadableSize;
    use coprocessor::codec::table::gen_table_prefix;

    use raftstore::coprocessor::{Config, CoprocessorHost};
    use super::*;

    #[test]
    fn test_bound_keys() {
        let path = TempDir::new("test-split-table").unwrap();
        let engine = Arc::new(new_engine(path.path().to_str().unwrap(), ALL_CFS).unwrap());
        let write_cf = engine.cf_handle(CF_WRITE).unwrap();

        let mut region = Region::new();
        region.set_id(1);
        region.mut_peers().push(Peer::new());
        region.mut_region_epoch().set_version(2);
        region.mut_region_epoch().set_conf_ver(5);

        assert_eq!(bound_keys(&engine, &region).unwrap(), None);

        // arbitrary padding.
        let padding = b"_r00000005";
        // Put t1_xxx
        let mut key = gen_table_prefix(1);
        key.extend_from_slice(padding);
        let s1 = keys::data_key(Key::from_raw(&key).encoded());
        engine.put_cf(write_cf, &s1, &s1).unwrap();

        let mut check = |start_id: Option<i64>, end_id: Option<i64>, result| {
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
            assert_eq!(bound_keys(&engine, &region).unwrap(), result);
        };

        // ["", "") => {t1_xx, t1_xx}
        check(None, None, Some((s1.clone(), s1.clone())));

        // ["", "t1") => None
        check(None, Some(1), None);

        // ["t1", "") => {t1_xx, t1_xx}
        check(Some(1), None, Some((s1.clone(), s1.clone())));

        // ["t1", "t2") => {t1_xx, t1_xx}
        check(Some(1), Some(2), Some((s1.clone(), s1.clone())));

        // Put t2_xx
        let mut key = gen_table_prefix(2);
        key.extend_from_slice(padding);
        let s2 = keys::data_key(Key::from_raw(&key).encoded());
        engine.put_cf(write_cf, &s2, &s2).unwrap();

        // ["t1", "") => {t1_xx, t2_xx}
        check(Some(1), None, Some((s1.clone(), s2.clone())));

        // ["", "t2") => {t1_xx, t1_xx}
        check(None, Some(2), Some((s1.clone(), s1.clone())));

        // ["t1", "t2") => {t1_xx, t1_xx}
        check(Some(1), Some(2), Some((s1.clone(), s1.clone())));

        // Put t3_xx
        let mut key = gen_table_prefix(3);
        key.extend_from_slice(padding);
        let s3 = keys::data_key(Key::from_raw(&key).encoded());
        engine.put_cf(write_cf, &s3, &s3).unwrap();

        // ["", "t3") => {t1_xx, t2_xx}
        check(None, Some(3), Some((s1.clone(), s2.clone())));

        // ["t1", "t3") => {t1_xx, t2_xx}
        check(Some(1), Some(3), Some((s1.clone(), s2.clone())));
    }

    #[test]
    fn test_table_check_observer() {
        let path = TempDir::new("test-raftstore").unwrap();
        let engine = Arc::new(new_engine(path.path().to_str().unwrap(), ALL_CFS).unwrap());
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
        let mut runnable = SplitCheckRunner::new(engine.clone(), ch.clone(), Arc::new(coprocessor));

        let mut check = |encoded_start_key: Option<Vec<u8>>,
                         encoded_end_key: Option<Vec<u8>>,
                         table_id: Option<i64>| {
            region.set_start_key(encoded_start_key.unwrap_or_else(Vec::new));
            region.set_end_key(encoded_end_key.unwrap_or_else(Vec::new));
            runnable.run(SplitCheckTask::new(&region));

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
        };

        let gen_encoded_table_prefix = |table_id| {
            let key = Key::from_raw(&table_codec::gen_table_prefix(table_id));
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

        // ["", "") => t3
        check(None, None, Some(3));

        // ["t1", "") => t3
        check(Some(gen_encoded_table_prefix(1)), None, Some(3));


        // ["t1", "t5") => t3
        check(
            Some(gen_encoded_table_prefix(1)),
            Some(gen_encoded_table_prefix(5)),
            Some(3),
        );


        // Put some data to t3
        for i in 1..4 {
            let mut key = gen_table_prefix(3);
            key.extend_from_slice(format!("{:?}{}", padding, i).as_bytes());
            let s = keys::data_key(Key::from_raw(&key).encoded());
            engine.put_cf(write_cf, &s, &s).unwrap();
        }

        // ["t1", "") => t3
        check(Some(gen_encoded_table_prefix(1)), None, Some(3));

        // ["t3", "") => skip
        check(Some(gen_encoded_table_prefix(3)), None, None);

        // ["t3", "t5") => skip
        check(
            Some(gen_encoded_table_prefix(3)),
            Some(gen_encoded_table_prefix(5)),
            None,
        );

        // Put some data before t and after t.
        for i in 0..3 {
            {
                // m is less than t and is the prefix of meta keys.
                let key = format!("m{:?}{}", padding, i);
                let s = keys::data_key(Key::from_raw(key.as_bytes()).encoded());
                engine.put_cf(write_cf, &s, &s).unwrap();
            }
            {
                let key = format!("u{:?}{}", padding, i);
                let s = keys::data_key(Key::from_raw(key.as_bytes()).encoded());
                engine.put_cf(write_cf, &s, &s).unwrap();
            }
        }

        // ["", "") => t1
        check(None, None, Some(1));

        // ["", "t1"] => skip
        check(None, Some(gen_encoded_table_prefix(1)), None);

        // ["", "t3"] => t1
        check(None, Some(gen_encoded_table_prefix(3)), Some(1));

        // ["", "s"] => skip
        check(None, Some(b"s".to_vec()), None);

        // ["u", ""] => skip
        check(Some(b"u".to_vec()), None, None);

        // ["t3", ""] => None
        check(Some(gen_encoded_table_prefix(3)), None, None);

        // ["t1", ""] => t3
        check(Some(gen_encoded_table_prefix(1)), None, Some(3));
    }
}
