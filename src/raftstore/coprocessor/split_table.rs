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

use coprocessor::codec::table;
use storage::types::Key;
use storage::{CfName, LARGE_CFS};
use raftstore::store::{keys, SplitChecker};
use raftstore::store::engine::{IterOption, Iterable};

use rocksdb::{SeekKey, DB};
use kvproto::metapb::Region;

use super::Result;

pub struct Checker {
    prev_key: Vec<u8>,
}

impl Checker {
    pub fn new() -> Checker {
        Checker {
            prev_key: Vec::new(),
        }
    }
}

impl SplitChecker for Checker {
    fn name(&self) -> &str {
        "TableSplitChecker"
    }

    fn prev_check(&self, engine: &DB, region: &Region) -> bool {
        let (actual_start_key, actual_end_key) = match bound_keys(engine, LARGE_CFS, region) {
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
        match table::in_same_table(
            &Key::from_encoded(keys::origin_key(&actual_start_key).to_vec())
                .raw()
                .unwrap(),
            &Key::from_encoded(keys::origin_key(&actual_end_key).to_vec())
                .raw()
                .unwrap(),
        ) {
            // This region's actual start_key and actual end_key are valid table keys,
            // and they come from different tables. So false for splitting at table bound.
            Ok(false) => false,
            _ => true,
        }
    }

    fn find_split_key(&mut self, key: &[u8], _: u64) -> Option<Vec<u8>> {
        let split_key = cross_table(&self.prev_key, key);

        // Avoid allocation.
        self.prev_key.clear();
        self.prev_key.extend_from_slice(key);

        split_key
    }

    fn finish(&mut self) {
        self.prev_key.clear();
    }
}

/// If `left_key` and `right_key` are in different tables,
/// it returns the `right_key`'s table prefix.
fn cross_table(left_key: &[u8], right_key: &[u8]) -> Option<Vec<u8>> {
    if !keys::validate_data_key(left_key) || !keys::validate_data_key(right_key) {
        return None;
    }
    let origin_right_key = keys::origin_key(right_key);
    let raw_right_key = match Key::from_encoded(origin_right_key.to_vec()).raw() {
        Ok(k) => k,
        Err(_) => return None,
    };

    let origin_left_key = keys::origin_key(left_key);
    let raw_left_key = match Key::from_encoded(origin_left_key.to_vec()).raw() {
        Ok(k) => k,
        Err(_) => return None,
    };

    if let Ok(false) = table::in_same_table(&raw_left_key, &raw_right_key) {
        let table_id = match table::decode_table_id(&raw_right_key) {
            Ok(id) => id,
            _ => return None,
        };

        Some(keys::data_key(
            Key::from_raw(&table::gen_table_prefix(table_id)).encoded(),
        ))
    } else {
        None
    }
}

#[allow(collapsible_if)]
fn bound_keys(db: &DB, cfs: &[CfName], region: &Region) -> Result<Option<(Vec<u8>, Vec<u8>)>> {
    let start_key = keys::enc_start_key(region);
    let end_key = keys::enc_end_key(region);
    let mut first_key = None;
    let mut last_key = None;

    for cf in cfs {
        let iter_opt = IterOption::new(Some(end_key.clone()), false);
        let mut iter = box_try!(db.new_iterator_cf(cf, iter_opt));

        // the first key
        if iter.seek(start_key.as_slice().into()) {
            let key = iter.key().to_vec();
            if first_key.is_some() {
                if &key < first_key.as_ref().unwrap() {
                    first_key = Some(key);
                }
            } else {
                first_key = Some(key);
            }
        } // else { No data in this CF }

        // the last key
        if iter.seek(SeekKey::End) {
            let key = iter.key().to_vec();
            if last_key.is_some() {
                if &key < last_key.as_ref().unwrap() {
                    last_key = Some(key);
                }
            } else {
                last_key = Some(key);
            }
        } // else { No data in this CF }
    }

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

    use tempdir::TempDir;
    use rocksdb::Writable;
    use kvproto::metapb::Peer;

    use storage::ALL_CFS;
    use storage::types::Key;
    use util::rocksdb::new_engine;

    use super::*;

    #[test]
    fn test_cross_table() {
        let t1 = keys::data_key(Key::from_raw(&table::gen_table_prefix(1)).encoded());
        let t5 = keys::data_key(Key::from_raw(&table::gen_table_prefix(5)).encoded());

        assert_eq!(cross_table(&t1, &t5).unwrap(), t5);
        assert_eq!(cross_table(&t5, &t5), None);
        assert_eq!(cross_table(b"foo", b"bar"), None);
    }

    #[test]
    fn test_bound_keys() {
        let path = TempDir::new("test-split-table").unwrap();
        let engine = Arc::new(new_engine(path.path().to_str().unwrap(), ALL_CFS).unwrap());

        let mut region = Region::new();
        region.set_id(1);
        region.mut_peers().push(Peer::new());
        region.mut_region_epoch().set_version(2);
        region.mut_region_epoch().set_conf_ver(5);

        assert_eq!(bound_keys(&engine, LARGE_CFS, &region).unwrap(), None);

        // arbitrary padding.
        let padding = b"_r00000005";
        // Put t1_xxx
        let mut key = table::gen_table_prefix(1);
        key.extend_from_slice(padding);
        let s1 = keys::data_key(Key::from_raw(&key).encoded());
        engine.put(&s1, &s1).unwrap();

        // ["", "") => {t1_xx, t1_xx}
        assert_eq!(
            bound_keys(&engine, LARGE_CFS, &region).unwrap(),
            Some((s1.clone(), s1.clone()))
        );

        // ["", "t1") => None
        region.set_start_key(vec![]);
        region.set_end_key(
            Key::from_raw(&table::gen_table_prefix(1))
                .encoded()
                .to_vec(),
        );
        assert_eq!(bound_keys(&engine, LARGE_CFS, &region).unwrap(), None);

        // ["t1", "") => {t1_xx, t1_xx}
        region.set_start_key(
            Key::from_raw(&table::gen_table_prefix(1))
                .encoded()
                .to_vec(),
        );
        region.set_end_key(vec![]);
        assert_eq!(
            bound_keys(&engine, LARGE_CFS, &region).unwrap(),
            Some((s1.clone(), s1.clone()))
        );

        // ["t1", "t2") => {t1_xx, t1_xx}
        region.set_start_key(
            Key::from_raw(&table::gen_table_prefix(1))
                .encoded()
                .to_vec(),
        );
        region.set_end_key(
            Key::from_raw(&table::gen_table_prefix(2))
                .encoded()
                .to_vec(),
        );
        assert_eq!(
            bound_keys(&engine, LARGE_CFS, &region).unwrap(),
            Some((s1.clone(), s1.clone()))
        );

        // Put t2_xx
        let mut key = table::gen_table_prefix(2);
        key.extend_from_slice(padding);
        let s2 = keys::data_key(Key::from_raw(&key).encoded());
        engine.put(&s2, &s2).unwrap();

        // ["t1", "") => {t1_xx, t2_xx}
        region.set_start_key(
            Key::from_raw(&table::gen_table_prefix(1))
                .encoded()
                .to_vec(),
        );
        region.set_end_key(vec![]);
        assert_eq!(
            bound_keys(&engine, LARGE_CFS, &region).unwrap(),
            Some((s1.clone(), s2.clone()))
        );

        // ["", "t2") => {t1_xx, t1_xx}
        region.set_start_key(vec![]);
        region.set_end_key(
            Key::from_raw(&table::gen_table_prefix(2))
                .encoded()
                .to_vec(),
        );
        assert_eq!(
            bound_keys(&engine, LARGE_CFS, &region).unwrap(),
            Some((s1.clone(), s1.clone()))
        );

        // ["t1", "t2") => {t1_xx, t1_xx}
        region.set_start_key(
            Key::from_raw(&table::gen_table_prefix(1))
                .encoded()
                .to_vec(),
        );
        region.set_end_key(
            Key::from_raw(&table::gen_table_prefix(2))
                .encoded()
                .to_vec(),
        );
        assert_eq!(
            bound_keys(&engine, LARGE_CFS, &region).unwrap(),
            Some((s1.clone(), s1.clone()))
        );

        // Put t3_xx
        let mut key = table::gen_table_prefix(3);
        key.extend_from_slice(padding);
        let s3 = keys::data_key(Key::from_raw(&key).encoded());
        engine.put(&s3, &s3).unwrap();

        // ["", "t3") => {t1_xx, t2_xx}
        region.set_start_key(vec![]);
        region.set_end_key(
            Key::from_raw(&table::gen_table_prefix(3))
                .encoded()
                .to_vec(),
        );
        assert_eq!(
            bound_keys(&engine, LARGE_CFS, &region).unwrap(),
            Some((s1.clone(), s2.clone()))
        );

        // ["t1", "t3") => {t1_xx, t2_xx}
        region.set_start_key(
            Key::from_raw(&table::gen_table_prefix(1))
                .encoded()
                .to_vec(),
        );
        region.set_end_key(
            Key::from_raw(&table::gen_table_prefix(3))
                .encoded()
                .to_vec(),
        );
        assert_eq!(
            bound_keys(&engine, LARGE_CFS, &region).unwrap(),
            Some((s1.clone(), s2.clone()))
        );
    }
}
