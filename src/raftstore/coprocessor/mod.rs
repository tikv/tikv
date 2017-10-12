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

mod region_snapshot;
pub mod dispatcher;
pub mod split_observer;
mod error;

pub use self::region_snapshot::{RegionIterator, RegionSnapshot};
pub use self::dispatcher::{CoprocessorHost, Registry};

use raftstore::store::keys;
use coprocessor::codec::table;
use storage::types::Key;

use kvproto::raft_cmdpb::{AdminRequest, Request};
use kvproto::metapb::Region;
use protobuf::RepeatedField;

pub use self::error::{Error, Result};


/// Coprocessor is used to provide a convient way to inject code to
/// KV processing.
pub trait Coprocessor {
    fn start(&self) {}
    fn stop(&self) {}
}

/// Context of observer.
pub struct ObserverContext<'a> {
    region: &'a Region,
    /// Whether to bypass following observer hook.
    pub bypass: bool,
}

impl<'a> ObserverContext<'a> {
    pub fn new(region: &Region) -> ObserverContext {
        ObserverContext {
            region: region,
            bypass: false,
        }
    }

    pub fn region(&self) -> &Region {
        self.region
    }
}

/// Observer hook of region level.
pub trait RegionObserver: Coprocessor {
    /// Hook to call before execute admin request.
    fn pre_admin(&self, _: &mut ObserverContext, _: &mut AdminRequest) -> Result<()> {
        Ok(())
    }

    /// Hook to call before execute read/write request.
    fn pre_query(&self, _: &mut ObserverContext, _: &mut RepeatedField<Request>) -> Result<()> {
        Ok(())
    }

    /// Hook to call before apply read/write request.
    ///
    /// Please note that improper implementation can lead to data inconsistency.
    fn pre_apply_query(&self, _: &mut ObserverContext, _: &mut RepeatedField<Request>) {}
}

/// If `left_key` and `right_key` are in different tables,
/// it returns the `right_key`'s table prefix.
pub fn cross_table(left_key: &[u8], right_key: &[u8]) -> Option<Vec<u8>> {
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
