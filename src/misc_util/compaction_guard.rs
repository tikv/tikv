// Copyright 2019 PingCAP, Inc.
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

use std::cell::RefCell;
use std::sync::atomic::{AtomicBool, Ordering};

use super::super::raftstore::store::keys::{data_end_key, origin_key, validate_data_key};
use super::super::storage::kv::RegionInfoProvider;
use engine::rocks::CompactionGuard;

pub struct RegionCompactionGuard<R: RegionInfoProvider> {
    initialized: AtomicBool,
    region_info_provider: RefCell<Option<R>>,
}

impl<R: RegionInfoProvider> RegionCompactionGuard<R> {
    pub fn new() -> Self {
        Self {
            initialized: AtomicBool::new(false),
            region_info_provider: RefCell::new(None),
        }
    }

    pub fn set_region_info_provider(&self, region_info_provider: R) {
        self.region_info_provider
            .replace(Some(region_info_provider));
        self.initialized.store(true, Ordering::SeqCst);
    }
}

impl<R: RegionInfoProvider> CompactionGuard for RegionCompactionGuard<R> {
    fn get_guards_in_range(&self, start: &[u8], end: &[u8]) -> Vec<Vec<u8>> {
        if self.initialized.load(Ordering::SeqCst) {
            let provider = self.region_info_provider.borrow();

            let s = if !start.is_empty() && validate_data_key(start) {
                origin_key(start)
            } else {
                &[]
            };
            let e = if !start.is_empty() && validate_data_key(end) {
                origin_key(end)
            } else {
                &[]
            };

            let regions = provider
                .as_ref()
                .unwrap()
                .get_regions_in_range(s, e)
                .unwrap_or_else(|e| panic!("fail to get regions in range, err: {:?}", e));
            let mut guards = Vec::with_capacity(regions.len());
            for region in regions {
                guards.push(data_end_key(region.get_end_key()));
            }
            guards
        } else {
            vec![]
        }
    }
}
