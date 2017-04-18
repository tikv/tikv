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


use std::str::FromStr;
use std::cmp::{PartialOrd, Ordering};
use std::error::Error;
use std::num::ParseIntError;
use std::fmt;
use std::time::Duration;
use std::net::SocketAddrV4;
use std::iter::FromIterator;

use regex::RegexBuilder;
use serde::{Serialize, Serializer, Deserialize, Deserializer};
use serde::de::{self, Visitor, SeqVisitor};

pub use self::labels::ServerLabels;
pub use self::size::Size;
pub use self::rocksdb::WalRecoveryMode;

pub mod labels;
pub mod size;
pub mod duration;
pub mod addrs;
pub mod rocksdb;


pub fn deserialize_opt_addr<D>(deserializer: D) -> Result<Option<SocketAddrV4>, D::Error>
    where D: Deserializer
{
    let opt_str = try!(Option::<String>::deserialize(deserializer));
    // TODO: handle parsing error
    Ok(opt_str.and_then(|s| s.parse().ok()))
}
