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

mod half;
mod size;
mod table;

use rocksdb::DB;

use kvproto::metapb::Region;
use super::error::Result;
use super::{ObserverContext, SplitChecker};

pub use self::half::HalfCheckObserver;
pub use self::size::SizeCheckObserver;
pub use self::table::TableCheckObserver;

#[derive(Default)]
pub struct Host {
    checkers: Vec<Box<SplitChecker>>,
    auto_split: bool,
}

impl Host {
    pub fn new(auto_split: bool) -> Host {
        Host {
            auto_split,
            checkers: vec![],
        }
    }

    #[inline]
    pub fn auto_split(&self) -> bool {
        self.auto_split
    }

    #[inline]
    pub fn skip(&self) -> bool {
        self.checkers.is_empty()
    }

    /// Hook to call for every check during split.
    ///
    /// Return true means abort early.
    pub fn on_kv(&mut self, region: &Region, key: &[u8], value_size: u64) -> bool {
        let mut ob_ctx = ObserverContext::new(region);
        for checker in &mut self.checkers {
            if checker.on_kv(&mut ob_ctx, key, value_size) {
                return true;
            }
        }
        false
    }

    pub fn split_key(mut self) -> Option<Vec<u8>> {
        self.checkers
            .drain(..)
            .flat_map(|mut c| c.split_key())
            .next()
    }

    pub fn approximate_split_key(mut self, region: &Region, engine: &DB) -> Result<Option<Vec<u8>>> {
        for checker in &mut self.checkers {
            match box_try!(checker.approximate_split_key(region, engine)) {
                Some(split_key) => return Ok(Some(split_key)),
                None => continue,
            }
        }
        Ok(None)
    }

    #[inline]
    pub fn add_checker(&mut self, checker: Box<SplitChecker>) {
        self.checkers.push(checker);
    }
}
