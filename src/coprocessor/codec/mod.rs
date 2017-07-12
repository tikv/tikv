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

pub use util::codec::{Result, Error};

const TEN_POW: &'static [u32] =
    &[1, 10, 100, 1_000, 10_000, 100_000, 1_000_000, 10_000_000, 100_000_000, 1_000_000_000];

/// A shortcut to box an error.
macro_rules! invalid_type {
    ($e:expr) => ({
        use util::codec::Error;
        Error::InvalidDataType(($e).into())
    });
    ($f:tt, $($arg:expr),+) => ({
        use util::codec::Error;
        Error::InvalidDataType(format!($f, $($arg),+))
    });
}

pub mod datum;
pub mod table;
pub mod convert;
pub mod mysql;

pub use self::datum::Datum;
