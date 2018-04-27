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

use self::half::HalfStatus;
use self::size::SizeStatus;
use self::table::TableStatus;

pub use self::size::SizeCheckObserver;
pub const SIZE_CHECK_OBSERVER_PRIORITY: u32 = 200;
pub use self::table::TableCheckObserver;
// TableCheckObserver has higher priority than SizeCheckObserver.
// Note that higher means less.
pub const TABLE_CHECK_OBSERVER_PRIORITY: u32 = SIZE_CHECK_OBSERVER_PRIORITY - 1;

pub use self::half::HalfCheckObserver;
pub const HALF_SPLIT_OBSERVER_PRIORITY: u32 = 400;

#[derive(Default)]
pub struct Status {
    // For TableCheckObserver
    table: Option<TableStatus>,
    // For SizeCheckObserver
    size: Option<SizeStatus>,
    // For HalfCheckObserver
    half: Option<HalfStatus>,
    // Whether it's called by auto_split
    auto_split: bool,
}

impl Status {
    pub fn new(auto_split: bool) -> Status {
        let mut status = Status::default();
        status.auto_split = auto_split;
        status
    }

    pub fn skip(&self) -> bool {
        self.table.is_none() && self.size.is_none() && self.half.is_none()
    }

    pub fn split_key(self) -> Option<Vec<u8>> {
        if let Some(status) = self.half {
            status.split_key()
        } else {
            None
        }
    }
}
