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

use rocksdb::DB;
use raftstore::store::util;

use super::super::{Coprocessor, ObserverContext, SplitCheckObserver};
use super::Status;

#[derive(Default)]
pub struct HalfStatus {
    region_size: u64,
    current_size: u64,
}

impl HalfStatus {
    pub fn on_split_check(&mut self, key: &[u8], value_size: u64) -> bool {
        self.current_size += key.len() as u64 + value_size;
        self.current_size >= self.region_size / 2
    }
}

#[derive(Default)]
pub struct HalfCheckObserver;

impl Coprocessor for HalfCheckObserver {}

impl SplitCheckObserver for HalfCheckObserver {
    fn new_split_check_status(&self, ctx: &mut ObserverContext, status: &mut Status, engine: &DB) {
        if status.auto_split {
            return;
        }
        let mut half_status = HalfStatus::default();
        let region = ctx.region();
        let region_id = region.get_id();
        half_status.region_size = match util::get_region_approximate_size(engine, region) {
            Ok(size) => size,
            Err(e) => {
                error!(
                    "[region {}] failed to get approximate size: {}",
                    region_id, e
                );
                // Need to check size.
                //TODO:
                return;
            }
        };
        status.half = Some(half_status);
    }

    fn on_split_check(
        &self,
        _: &mut ObserverContext,
        status: &mut Status,
        key: &[u8],
        value_size: u64,
    ) -> Option<Vec<u8>> {
        if let Some(status) = status.half.as_mut() {
            if status.on_split_check(key, value_size) {
                return Some(key.to_vec());
            }
        }
        None
    }
}
