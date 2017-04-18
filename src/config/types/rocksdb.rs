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

use std::fmt;

use serde::{Serialize, Serializer, Deserialize, Deserializer};
use serde::de::{self, Visitor, SeqVisitor};
use rocksdb::{DBCompressionType, DBRecoveryMode};

// https://github.com/facebook/rocksdb/wiki/WAL-Recovery-Modes
#[derive(Debug, PartialEq, Eq)]
pub enum WalRecoveryMode {
    TolerateCorruptedTailRecords,
    AbsoluteConsistency,
    PointInTimeConsistency,
    SkipAnyCorruptedRecords,
}

impl Serialize for WalRecoveryMode {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
        where S: Serializer
    {
        match *self {
            WalRecoveryMode::TolerateCorruptedTailRecords => serializer.serialize_i32(0),
            WalRecoveryMode::AbsoluteConsistency => serializer.serialize_i32(1),
            WalRecoveryMode::PointInTimeConsistency => serializer.serialize_i32(2),
            WalRecoveryMode::SkipAnyCorruptedRecords => serializer.serialize_i32(3),
        }
    }
}

struct WalRecoveryModeVisitor;

impl Visitor for WalRecoveryModeVisitor {
    type Value = WalRecoveryMode;

    fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
        formatter.write_str("an integer between 0 and 3")
    }

    fn visit_i64<E>(self, value: i64) -> Result<WalRecoveryMode, E>
        where E: de::Error
    {
        match value {
            0 => Ok(WalRecoveryMode::TolerateCorruptedTailRecords),
            1 => Ok(WalRecoveryMode::AbsoluteConsistency),
            2 => Ok(WalRecoveryMode::PointInTimeConsistency),
            3 => Ok(WalRecoveryMode::SkipAnyCorruptedRecords),
            _ => Err(E::custom(format!("value out of range: {}", value))),
        }
    }
}

impl Deserialize for WalRecoveryMode {
    fn deserialize<D>(deserializer: D) -> Result<WalRecoveryMode, D::Error>
        where D: Deserializer
    {
        deserializer.deserialize_i32(WalRecoveryModeVisitor)
    }
}
