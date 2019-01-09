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
mod keys;
mod size;
mod table;

use rocksdb::DB;

use super::error::Result;
use super::{KeyEntry, ObserverContext, SplitChecker};
use kvproto::metapb::Region;
use kvproto::pdpb::CheckPolicy;

pub use self::half::HalfCheckObserver;
pub use self::keys::KeysCheckObserver;
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

    pub fn policy(&self) -> CheckPolicy {
        for checker in &self.checkers {
            if checker.policy() == CheckPolicy::APPROXIMATE {
                return CheckPolicy::APPROXIMATE;
            }
        }
        CheckPolicy::SCAN
    }

    /// Hook to call for every check during split.
    ///
    /// Return true means abort early.
    pub fn on_kv(&mut self, region: &Region, entry: &KeyEntry) -> bool {
        let mut ob_ctx = ObserverContext::new(region);
        for checker in &mut self.checkers {
            if checker.on_kv(&mut ob_ctx, entry) {
                return true;
            }
        }
        false
    }

    pub fn split_keys(&mut self) -> Vec<Vec<u8>> {
        for checker in &mut self.checkers {
            let keys = checker.split_keys();
            if !keys.is_empty() {
                return keys;
            }
        }
        vec![]
    }

    pub fn approximate_split_keys(&mut self, region: &Region, engine: &DB) -> Result<Vec<Vec<u8>>> {
        for checker in &mut self.checkers {
            let keys = box_try!(checker.approximate_split_keys(region, engine));
            if !keys.is_empty() {
                return Ok(keys);
            }
        }
        Ok(vec![])
    }

    #[inline]
    pub fn add_checker(&mut self, checker: Box<SplitChecker>) {
        self.checkers.push(checker);
    }
}
