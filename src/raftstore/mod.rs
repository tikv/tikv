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

// Assert the operation must succeed.
macro_rules! assert_op {
    ($tag:expr, $op:expr) => ({
        match $op {
            Ok(r) => r,
            Err(e) => panic!("{} op is failed: {:?}", $tag, e),
        }
    })
}

pub mod store;
pub mod errors;
pub mod coprocessor;
pub use self::errors::{Result, Error};
