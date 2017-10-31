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

mod size;

use self::size::SizeStatus;

pub use self::size::SizeCheckObserver;
pub const SIZE_CHECK_OBSERVER_PRIORITY: u32 = 200;

#[derive(Default)]
pub struct Status {
    // For SizeCheckObserver
    size: Option<SizeStatus>,
}

impl Status {
    pub fn skip(&self) -> bool {
        self.size.is_none()
    }
}
