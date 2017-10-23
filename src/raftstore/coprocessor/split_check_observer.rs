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
use rocksdb::{SeekKey, DB};
use kvproto::metapb::Region;

use coprocessor::codec::table as table_codec;
use storage::types::Key;
use storage::CF_WRITE;

use super::super::store::keys;
use super::super::store::engine::{IterOption, Iterable};

use super::{Coprocessor, ObserverContext, RegionObserver, Result};

#[derive(Default)]
pub struct Context {
    prev_table_id: i64,
    last_table_prefix: Option<Vec<u8>>,
    skip: bool,
}

impl Context {
    pub fn skip(&self) -> bool {
        self.skip
    }
}

#[derive(Default)]
pub struct TableCheckObserver;

impl Coprocessor for TableCheckObserver {}

impl RegionObserver for TableCheckObserver {
    /// Hook to call before handle split region task. If it returns a None,
    /// then `each_split_check` can be skippped.
    fn pre_split_check(
        &self,
        ob_ctx: &mut ObserverContext,
        engine: &DB,
    ) -> Result<Option<Context>> {
        ob_ctx.bypass = true;

        let mut ctx = Context::default();
        ctx.skip = before_check(&mut ctx, engine, ob_ctx.region());

        Ok(Some(ctx))
    }

    /// Hook to call for every check during split.
    fn each_split_check(
        &self,
        ob_ctx: &mut ObserverContext,
        ctx: &mut Context,
        key: &[u8],
    ) -> Option<Vec<u8>> {
        ob_ctx.bypass = true;

        check_key(ctx, key)
    }
}

/// Do some quick checks, true for skipping `check_key`.
fn before_check(ctx: &mut Context, engine: &DB, region: &Region) -> bool {
    let (actual_start_key, actual_end_key) = match bound_keys(engine, region) {
        Ok(Some((actual_start_key, actual_end_key))) => (actual_start_key, actual_end_key),
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

    if !keys::validate_data_key(&actual_start_key) || !keys::validate_data_key(&actual_end_key) {
        return true;
    }

    let raw_start_key =
        match Key::from_encoded(keys::origin_key(&actual_start_key).to_vec()).raw() {
            Ok(k) => k,
            Err(_) => return true,
        };

    let raw_end_key = match Key::from_encoded(keys::origin_key(&actual_end_key).to_vec()).raw() {
        Ok(k) => k,
        Err(_) => return true,
    };

    let start_table_id = table_codec::decode_table_id(&raw_start_key).unwrap_or(0);
    let end_table_id = table_codec::decode_table_id(&raw_end_key).unwrap_or(0);

    match (
        raw_start_key[..table_codec::TABLE_PREFIX_LEN]
            .cmp(&table_codec::TABLE_PREFIX[..table_codec::TABLE_PREFIX_LEN]),
        raw_end_key[..table_codec::TABLE_PREFIX_LEN]
            .cmp(&table_codec::TABLE_PREFIX[..table_codec::TABLE_PREFIX_LEN]),
    ) {
        (Ordering::Less, Ordering::Less) | (Ordering::Greater, Ordering::Greater) => true,
        (Ordering::Less, Ordering::Equal) => {
            if end_table_id != 0 {
                ctx.last_table_prefix = Some(table_codec::gen_table_prefix(end_table_id));
            }
            false
        }
        (Ordering::Less, Ordering::Greater) => false,
        (Ordering::Equal, Ordering::Equal) => {
            if start_table_id == end_table_id {
                // Same table.
                // TODO: what if start_table_id equals to 0.
                true
            } else {
                ctx.last_table_prefix = Some(table_codec::gen_table_prefix(end_table_id));
                false
            }
        }
        (Ordering::Equal, Ordering::Greater) => {
            ctx.prev_table_id = start_table_id;
            false
        }
        _ => panic!(
            "start_key {:?} and end_key {:?} out of order",
            actual_end_key,
            actual_end_key
        ),
    }
}

/// Feed keys and value sizes in order to find the split key.
fn check_key(ctx: &mut Context, key: &[u8]) -> Option<Vec<u8>> {
    if let Some(last_table_prefix) = ctx.last_table_prefix.take() {
        Some(keys::data_key(Key::from_raw(&last_table_prefix).encoded()))
    } else if let Some(current_table_prefix) = cross_table(ctx.prev_table_id, key) {
        Some(keys::data_key(
            Key::from_raw(&current_table_prefix).encoded(),
        ))
    } else {
        None
    }
}

/// If `current_key` is not in the table `table_id`,
/// it returns the `current_key`'s table prefix.
fn cross_table(table_id: i64, current_key: &[u8]) -> Option<Vec<u8>> {
    if !keys::validate_data_key(current_key) {
        return None;
    }
    let origin_current_key = keys::origin_key(current_key);
    let raw_current_key = match Key::from_encoded(origin_current_key.to_vec()).raw() {
        Ok(k) => k,
        Err(_) => return None,
    };

    let current_table_id = match table_codec::decode_table_id(&raw_current_key) {
        Ok(id) => id,
        _ => return None,
    };

    if table_id != current_table_id {
        Some(table_codec::gen_table_prefix(current_table_id))
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

#[cfg(test)]
mod test {
    use std::sync::Arc;
    use std::sync::mpsc;

    use tempdir::TempDir;
    use rocksdb::Writable;
    use kvproto::metapb::Peer;

    use storage::ALL_CFS;
    use storage::types::Key;
    use util::rocksdb::new_engine;
    use util::worker::Runnable;
    use raftstore::store::{Msg, SplitCheckRunner, SplitCheckTask};
    use util::transport::RetryableSendCh;

    use super::super::{Config, CoprocessorHost};
    use super::*;

    #[test]
    fn test_cross_table() {
        let t2 = table_codec::gen_table_prefix(2);
        let data_t2 = keys::data_key(Key::from_raw(&t2).encoded());

        assert_eq!(cross_table(1, &data_t2).unwrap(), t2);
        assert_eq!(cross_table(2, &data_t2), None);
        assert_eq!(cross_table(2, b"bar"), None);
    }

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
        let mut key = table_codec::gen_table_prefix(1);
        key.extend_from_slice(padding);
        let s1 = keys::data_key(Key::from_raw(&key).encoded());
        engine.put_cf(write_cf, &s1, &s1).unwrap();

        let mut check = |start_id: Option<i64>, end_id: Option<i64>, result| {
            region.set_start_key(
                start_id
                    .map(|id| {
                        Key::from_raw(&table_codec::gen_table_prefix(id))
                            .encoded()
                            .to_vec()
                    })
                    .unwrap_or_else(Vec::new),
            );
            region.set_end_key(
                end_id
                    .map(|id| {
                        Key::from_raw(&table_codec::gen_table_prefix(id))
                            .encoded()
                            .to_vec()
                    })
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
        let mut key = table_codec::gen_table_prefix(2);
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
        let mut key = table_codec::gen_table_prefix(3);
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
        let ch = RetryableSendCh::new(tx, "test-split");

        let mut cfg = Config::default();
        cfg.split_region_on_table = true;
        let coprocessor = CoprocessorHost::new(cfg);
        let mut runnable = SplitCheckRunner::new(
            engine.clone(),
            ch.clone(),
            10000, // Disable size split.
            4000,
            Arc::new(coprocessor),
        );

        let mut check = |encoded_start_key: Option<Vec<u8>>,
                         encoded_end_key: Option<Vec<u8>>,
                         table_id: Option<i64>| {
            region.set_start_key(encoded_start_key.unwrap_or_else(Vec::new));
            region.set_end_key(encoded_end_key.unwrap_or_else(Vec::new));
            runnable.run(SplitCheckTask::new(&region));

            // Try to ignore the ApproximateRegionSize
            if let Ok(Msg::ApproximateRegionSize { .. }) = rx.try_recv() {}
            if let Some(id) = table_id {
                let key = Key::from_raw(&table_codec::gen_table_prefix(id));
                match rx.try_recv() {
                    Ok(Msg::SplitRegion { split_key, .. }) => {
                        assert_eq!(&split_key, key.encoded());
                    }
                    others => panic!("expect split check result, but got {:?}", others),
                }
            } else {
                match rx.try_recv() {
                    Err(mpsc::TryRecvError::Empty) => (),
                    others => panic!("expect empty, but got {:?}", others),
                }
            }
        };

        let gen_data_table_prefix = |table_id| {
            Key::from_raw(&table_codec::gen_table_prefix(table_id))
                .encoded()
                .clone()
        };

        // arbitrary padding.
        let padding = b"_r00000005";
        let mut array = vec![];

        // Put some tables
        // t1_xx, t3_xx
        for i in 1..4 {
            if i % 2 == 0 {
                // leave some space.
                continue;
            }

            let mut key = table_codec::gen_table_prefix(i);
            key.extend_from_slice(padding);
            let s = keys::data_key(Key::from_raw(&key).encoded());
            engine.put_cf(write_cf, &s, &s).unwrap();
            array.push(s);
        }

        // ["", "") => t3
        check(None, None, Some(3));

        // ["t1", "") => t3
        check(Some(gen_data_table_prefix(1)), None, Some(3));


        // ["t1", "t5") => t3
        check(
            Some(gen_data_table_prefix(1)),
            Some(gen_data_table_prefix(5)),
            Some(3),
        );


        // Put some data to t3
        for i in 1..4 {
            let mut key = table_codec::gen_table_prefix(3);
            key.extend_from_slice(format!("{:?}{}", padding, i).as_bytes());
            let s = keys::data_key(Key::from_raw(&key).encoded());
            engine.put_cf(write_cf, &s, &s).unwrap();
            array.push(s);
        }

        // ["t1", "") => t3
        check(Some(gen_data_table_prefix(1)), None, Some(3));

        // ["t3", "") => skip
        check(Some(gen_data_table_prefix(3)), None, None);

        // ["t3", "t5") => skip
        check(
            Some(gen_data_table_prefix(3)),
            Some(gen_data_table_prefix(5)),
            None,
        );

        // Put some data before t and after t.
        for i in 0..3 {
            {
                let key = format!("s{:?}{}", padding, i);
                let s = keys::data_key(Key::from_raw(key.as_bytes()).encoded());
                engine.put_cf(write_cf, &s, &s).unwrap();
                array.push(s);
            }
            {
                let key = format!("u{:?}{}", padding, i);
                let s = keys::data_key(Key::from_raw(key.as_bytes()).encoded());
                engine.put_cf(write_cf, &s, &s).unwrap();
                array.push(s);
            }
        }
        array.sort();

        // ["", "") => t1
        check(None, None, Some(1));

        // ["", "t1"] => skip
        check(None, Some(gen_data_table_prefix(1)), None);

        // ["", "t3"] => t1
        check(None, Some(gen_data_table_prefix(3)), Some(1));

        // ["", "s"] => skip
        check(None, Some(b"s".to_vec()), None);

        // ["u", ""] => skip
        check(Some(b"u".to_vec()), None, None);

        // ["t3", ""] => None
        check(Some(gen_data_table_prefix(3)), None, None);

        // ["t1", ""] => t3
        check(Some(gen_data_table_prefix(1)), None, Some(3));
    }
}
