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
use raftstore::store::{keys, SplitChecker};

use kvproto::metapb::Region;

pub struct Checker {
    prev_key: Vec<u8>,
}

impl Default for Checker {
    fn default() -> Checker {
        Checker {
            prev_key: Vec::new(),
        }
    }
}

impl SplitChecker for Checker {
    fn name(&self) -> &str {
        "TableSplitChecker"
    }

    fn skip_check(&self, _: &Region, actual_keys: &Option<(Vec<u8>, Vec<u8>)>) -> bool {
        if actual_keys.is_none() {
            return true;
        }
        let &(ref actual_start_key, ref actual_end_key) = actual_keys.as_ref().unwrap();
        if !keys::validate_data_key(actual_start_key) || !keys::validate_data_key(actual_end_key) {
            return true;
        }
        match table::in_same_table(
            &Key::from_encoded(keys::origin_key(actual_start_key).to_vec())
                .raw()
                .unwrap(),
            &Key::from_encoded(keys::origin_key(actual_end_key).to_vec())
                .raw()
                .unwrap(),
        ) {
            // This region's actual start_key and actual end_key are valid table keys,
            // and they come from different tables. So false for splitting at table bound.
            Ok(false) => false,
            _ => true,
        }
    }

    fn find_gap(&mut self, key: &[u8], _: u64) -> Option<Vec<u8>> {
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

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn test_cross_table() {
        let t1 = keys::data_key(Key::from_raw(&table::gen_table_prefix(1)).encoded());
        let t5 = keys::data_key(Key::from_raw(&table::gen_table_prefix(5)).encoded());

        assert_eq!(cross_table(&t1, &t5).unwrap(), t5);
        assert_eq!(cross_table(&t5, &t5), None);
        assert_eq!(cross_table(b"foo", b"bar"), None);
    }
}
