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
use serde::de::{self, Visitor, SeqVisitor, Error};
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



pub mod db_compression_types {
    use std::fmt;
    use std::iter::FromIterator;

    use rocksdb::DBCompressionType;
    use serde::{Serialize, Serializer, Deserialize, Deserializer};
    use serde::de::{self, Visitor, SeqVisitor, Error};


    pub struct CompressionPerLevel(Vec<DBCompressionType>);

    impl CompressionPerLevel {
        fn no_compression() -> CompressionPerLevel {
            CompressionPerLevel(vec![DBCompressionType::DBNo; 7])
        }
    }

    impl Default for CompressionPerLevel {
        fn default() -> CompressionPerLevel {
            CompressionPerLevel(vec![DBCompressionType::DBLz4; 7])
        }
    }

    impl fmt::Display for CompressionPerLevel {
        fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
            let raw = self.0
                .iter()
                .map(|tp| {
                    match *tp {
                        DBCompressionType::DBNo => "no",
                        DBCompressionType::DBLz4 => "lz4",
                        DBCompressionType::DBSnappy => "snappy",
                        DBCompressionType::DBZlib => "zlib",
                        DBCompressionType::DBBz2 => "bzip2",
                        DBCompressionType::DBLz4hc => "lz4hc",
                    }
                })
                .collect::<Vec<_>>()
                .join(":");
            write!(f, "{}", raw)
        }
    }

    impl fmt::Debug for CompressionPerLevel {
        fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
            write!(f, "CompressionPerLevel({})", self)
        }
    }

    /// serde serialize interface for CompressionPerLevel
    impl Serialize for CompressionPerLevel {
        fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
            where S: Serializer
        {
            serializer.serialize_str(&format!("{}", self))
        }
    }

    /// serde deserialize interface for CompressionPerLevel
    impl Deserialize for CompressionPerLevel {
        fn deserialize<D>(deserializer: D) -> Result<CompressionPerLevel, D::Error>
            where D: Deserializer
        {
            let raw = try!(String::deserialize(deserializer));
            let it = raw.split(':')
                .map(|tp| {
                    match tp.to_lowercase().as_ref() {
                        "no" => Ok(DBCompressionType::DBNo),
                        "snappy" => Ok(DBCompressionType::DBSnappy),
                        "zlib" => Ok(DBCompressionType::DBZlib),
                        "bzip2" => Ok(DBCompressionType::DBBz2),
                        "lz4" => Ok(DBCompressionType::DBLz4),
                        "lz4hc" => Ok(DBCompressionType::DBLz4hc),
                        _ => Err(D::Error::custom(format!("unknown db compression type {}", tp))),
                    }
                });
            Result::from_iter(it).map(CompressionPerLevel)
        }
    }
}
