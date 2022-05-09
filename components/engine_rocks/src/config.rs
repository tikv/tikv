// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

use std::str::FromStr;

use online_config::ConfigValue;
use rocksdb::{
    CompactionPriority, DBCompactionStyle, DBCompressionType, DBInfoLogLevel, DBRateLimiterMode,
    DBRecoveryMode, DBTitanDBBlobRunMode,
};
use serde::{Deserialize, Serialize};
use tikv_util::numeric_enum_serializing_mod;

#[derive(Copy, Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum LogLevel {
    Error,
    Fatal,
    Info,
    Warn,
    Debug,
}

impl From<LogLevel> for DBInfoLogLevel {
    fn from(compression_type: LogLevel) -> DBInfoLogLevel {
        match compression_type {
            LogLevel::Error => DBInfoLogLevel::Error,
            LogLevel::Fatal => DBInfoLogLevel::Fatal,
            LogLevel::Info => DBInfoLogLevel::Info,
            LogLevel::Warn => DBInfoLogLevel::Warn,
            LogLevel::Debug => DBInfoLogLevel::Debug,
        }
    }
}

#[derive(Copy, Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum CompressionType {
    No,
    Snappy,
    Zlib,
    Bz2,
    Lz4,
    Lz4hc,
    Zstd,
    ZstdNotFinal,
}

impl From<CompressionType> for DBCompressionType {
    fn from(compression_type: CompressionType) -> DBCompressionType {
        match compression_type {
            CompressionType::No => DBCompressionType::No,
            CompressionType::Snappy => DBCompressionType::Snappy,
            CompressionType::Zlib => DBCompressionType::Zlib,
            CompressionType::Bz2 => DBCompressionType::Bz2,
            CompressionType::Lz4 => DBCompressionType::Lz4,
            CompressionType::Lz4hc => DBCompressionType::Lz4hc,
            CompressionType::Zstd => DBCompressionType::Zstd,
            CompressionType::ZstdNotFinal => DBCompressionType::ZstdNotFinal,
        }
    }
}

pub mod compression_type_level_serde {
    use std::fmt;

    use rocksdb::DBCompressionType;
    use serde::{
        de::{Error, SeqAccess, Unexpected, Visitor},
        ser::SerializeSeq,
        Deserializer, Serializer,
    };

    pub fn serialize<S>(ts: &[DBCompressionType; 7], serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut s = serializer.serialize_seq(Some(ts.len()))?;
        for t in ts {
            let name = match *t {
                DBCompressionType::No => "no",
                DBCompressionType::Snappy => "snappy",
                DBCompressionType::Zlib => "zlib",
                DBCompressionType::Bz2 => "bzip2",
                DBCompressionType::Lz4 => "lz4",
                DBCompressionType::Lz4hc => "lz4hc",
                DBCompressionType::Zstd => "zstd",
                DBCompressionType::ZstdNotFinal => "zstd-not-final",
                DBCompressionType::Disable => "disable",
            };
            s.serialize_element(name)?;
        }
        s.end()
    }

    pub fn deserialize<'de, D>(deserializer: D) -> Result<[DBCompressionType; 7], D::Error>
    where
        D: Deserializer<'de>,
    {
        struct SeqVisitor;
        impl<'de> Visitor<'de> for SeqVisitor {
            type Value = [DBCompressionType; 7];

            fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
                write!(formatter, "a compression type vector")
            }

            fn visit_seq<S>(self, mut seq: S) -> Result<[DBCompressionType; 7], S::Error>
            where
                S: SeqAccess<'de>,
            {
                let mut seqs = [DBCompressionType::No; 7];
                let mut i = 0;
                while let Some(value) = seq.next_element::<String>()? {
                    if i == 7 {
                        return Err(S::Error::invalid_value(
                            Unexpected::Str(&value),
                            &"only 7 compression types",
                        ));
                    }
                    seqs[i] = match &*value.trim().to_lowercase() {
                        "no" => DBCompressionType::No,
                        "snappy" => DBCompressionType::Snappy,
                        "zlib" => DBCompressionType::Zlib,
                        "bzip2" => DBCompressionType::Bz2,
                        "lz4" => DBCompressionType::Lz4,
                        "lz4hc" => DBCompressionType::Lz4hc,
                        "zstd" => DBCompressionType::Zstd,
                        "zstd-not-final" => DBCompressionType::ZstdNotFinal,
                        "disable" => DBCompressionType::Disable,
                        _ => {
                            return Err(S::Error::invalid_value(
                                Unexpected::Str(&value),
                                &"invalid compression type",
                            ));
                        }
                    };
                    i += 1;
                }
                if i < 7 {
                    return Err(S::Error::invalid_length(i, &"7 compression types"));
                }
                Ok(seqs)
            }
        }

        deserializer.deserialize_seq(SeqVisitor)
    }
}

pub mod compression_type_serde {
    use std::fmt;

    use rocksdb::DBCompressionType;
    use serde::{
        de::{Error, Unexpected, Visitor},
        Deserializer, Serializer,
    };

    pub fn serialize<S>(t: &DBCompressionType, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let name = match *t {
            DBCompressionType::No => "no",
            DBCompressionType::Snappy => "snappy",
            DBCompressionType::Zlib => "zlib",
            DBCompressionType::Bz2 => "bzip2",
            DBCompressionType::Lz4 => "lz4",
            DBCompressionType::Lz4hc => "lz4hc",
            DBCompressionType::Zstd => "zstd",
            DBCompressionType::ZstdNotFinal => "zstd-not-final",
            DBCompressionType::Disable => "disable",
        };
        serializer.serialize_str(name)
    }

    pub fn deserialize<'de, D>(deserializer: D) -> Result<DBCompressionType, D::Error>
    where
        D: Deserializer<'de>,
    {
        struct StrVistor;
        impl<'de> Visitor<'de> for StrVistor {
            type Value = DBCompressionType;

            fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
                write!(formatter, "a compression type")
            }

            fn visit_str<E>(self, value: &str) -> Result<DBCompressionType, E>
            where
                E: Error,
            {
                let str = match &*value.trim().to_lowercase() {
                    "no" => DBCompressionType::No,
                    "snappy" => DBCompressionType::Snappy,
                    "zlib" => DBCompressionType::Zlib,
                    "bzip2" => DBCompressionType::Bz2,
                    "lz4" => DBCompressionType::Lz4,
                    "lz4hc" => DBCompressionType::Lz4hc,
                    "zstd" => DBCompressionType::Zstd,
                    "zstd-not-final" => DBCompressionType::ZstdNotFinal,
                    "disable" => DBCompressionType::Disable,
                    _ => {
                        return Err(E::invalid_value(
                            Unexpected::Other("invalid compression type"),
                            &self,
                        ));
                    }
                };
                Ok(str)
            }
        }

        deserializer.deserialize_str(StrVistor)
    }
}

#[derive(Copy, Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum BlobRunMode {
    Normal,
    ReadOnly,
    Fallback,
}

impl From<BlobRunMode> for ConfigValue {
    fn from(mode: BlobRunMode) -> ConfigValue {
        ConfigValue::BlobRunMode(format!("k{:?}", mode))
    }
}

impl From<ConfigValue> for BlobRunMode {
    fn from(c: ConfigValue) -> BlobRunMode {
        if let ConfigValue::BlobRunMode(s) = c {
            match s.as_str() {
                "kNormal" => BlobRunMode::Normal,
                "kReadOnly" => BlobRunMode::ReadOnly,
                "kFallback" => BlobRunMode::Fallback,
                m => panic!("expect: kNormal, kReadOnly or kFallback, got: {:?}", m),
            }
        } else {
            panic!("expect: ConfigValue::BlobRunMode, got: {:?}", c);
        }
    }
}

impl FromStr for BlobRunMode {
    type Err = String;
    fn from_str(s: &str) -> Result<BlobRunMode, String> {
        match s {
            "normal" => Ok(BlobRunMode::Normal),
            "read-only" => Ok(BlobRunMode::ReadOnly),
            "fallback" => Ok(BlobRunMode::Fallback),
            m => Err(format!(
                "expect: normal, read-only or fallback, got: {:?}",
                m
            )),
        }
    }
}

impl From<BlobRunMode> for DBTitanDBBlobRunMode {
    fn from(m: BlobRunMode) -> DBTitanDBBlobRunMode {
        match m {
            BlobRunMode::Normal => DBTitanDBBlobRunMode::Normal,
            BlobRunMode::ReadOnly => DBTitanDBBlobRunMode::ReadOnly,
            BlobRunMode::Fallback => DBTitanDBBlobRunMode::Fallback,
        }
    }
}

numeric_enum_serializing_mod! {compaction_pri_serde CompactionPriority {
    ByCompensatedSize = 0,
    OldestLargestSeqFirst = 1,
    OldestSmallestSeqFirst = 2,
    MinOverlappingRatio = 3,
}}

numeric_enum_serializing_mod! {rate_limiter_mode_serde DBRateLimiterMode {
    ReadOnly = 1,
    WriteOnly = 2,
    AllIo = 3,
}}

numeric_enum_serializing_mod! {compaction_style_serde DBCompactionStyle {
    Level = 0,
    Universal = 1,
    Fifo = 2,
    None = 3,
}}

numeric_enum_serializing_mod! {recovery_mode_serde DBRecoveryMode {
    TolerateCorruptedTailRecords = 0,
    AbsoluteConsistency = 1,
    PointInTime = 2,
    SkipAnyCorruptedRecords = 3,
}}

#[cfg(test)]
mod tests {
    use rocksdb::DBCompressionType;

    use super::*;

    #[test]
    fn test_parse_compression_type() {
        #[derive(Serialize, Deserialize)]
        struct CompressionTypeHolder {
            #[serde(with = "compression_type_level_serde")]
            tp: [DBCompressionType; 7],
        }

        let all_tp = vec![
            (DBCompressionType::No, "no"),
            (DBCompressionType::Snappy, "snappy"),
            (DBCompressionType::Zlib, "zlib"),
            (DBCompressionType::Bz2, "bzip2"),
            (DBCompressionType::Lz4, "lz4"),
            (DBCompressionType::Lz4hc, "lz4hc"),
            (DBCompressionType::Zstd, "zstd"),
            (DBCompressionType::ZstdNotFinal, "zstd-not-final"),
            (DBCompressionType::Disable, "disable"),
        ];
        for i in 0..all_tp.len() - 7 {
            let mut src = [DBCompressionType::No; 7];
            let mut exp = ["no"; 7];
            for (i, &t) in all_tp[i..i + 7].iter().enumerate() {
                src[i] = t.0;
                exp[i] = t.1;
            }
            let holder = CompressionTypeHolder { tp: src };
            let res_str = toml::to_string(&holder).unwrap();
            let exp_str = format!("tp = [\"{}\"]\n", exp.join("\", \""));
            assert_eq!(res_str, exp_str);
            let h: CompressionTypeHolder = toml::from_str(&exp_str).unwrap();
            assert_eq!(h.tp, holder.tp);
        }

        // length is wrong.
        assert!(toml::from_str::<CompressionTypeHolder>("tp = [\"no\"]").is_err());
        assert!(
            toml::from_str::<CompressionTypeHolder>(
                r#"tp = [
            "no", "no", "no", "no", "no", "no", "no", "no"
        ]"#
            )
            .is_err()
        );
        // value is wrong.
        assert!(
            toml::from_str::<CompressionTypeHolder>(
                r#"tp = [
            "no", "no", "no", "no", "no", "no", "yes"
        ]"#
            )
            .is_err()
        );
    }
}
