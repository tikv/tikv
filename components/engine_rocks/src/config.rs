// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

use configuration::ConfigValue;
pub use rocksdb::PerfLevel;
use rocksdb::{DBCompressionType, DBTitanDBBlobRunMode};
use std::str::FromStr;

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

    use serde::de::{Error, SeqAccess, Unexpected, Visitor};
    use serde::ser::SerializeSeq;
    use serde::{Deserializer, Serializer};

    use rocksdb::DBCompressionType;

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

impl Into<BlobRunMode> for ConfigValue {
    fn into(self) -> BlobRunMode {
        if let ConfigValue::BlobRunMode(s) = self {
            match s.as_str() {
                "kNormal" => BlobRunMode::Normal,
                "kReadOnly" => BlobRunMode::ReadOnly,
                "kFallback" => BlobRunMode::Fallback,
                m => panic!("expect: kNormal, kReadOnly or kFallback, got: {:?}", m),
            }
        } else {
            panic!("expect: ConfigValue::BlobRunMode, got: {:?}", self);
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

impl Into<DBTitanDBBlobRunMode> for BlobRunMode {
    fn into(self) -> DBTitanDBBlobRunMode {
        match self {
            BlobRunMode::Normal => DBTitanDBBlobRunMode::Normal,
            BlobRunMode::ReadOnly => DBTitanDBBlobRunMode::ReadOnly,
            BlobRunMode::Fallback => DBTitanDBBlobRunMode::Fallback,
        }
    }
}

macro_rules! numeric_enum_mod {
    ($name:ident $enum:ident { $($variant:ident = $value:expr, )* }) => {
        pub mod $name {
            use std::fmt;

            use serde::{Serializer, Deserializer};
            use serde::de::{self, Unexpected, Visitor};
            use rocksdb::$enum;

            pub fn serialize<S>(mode: &$enum, serializer: S) -> Result<S::Ok, S::Error>
                where S: Serializer
            {
                serializer.serialize_i64(*mode as i64)
            }

            pub fn deserialize<'de, D>(deserializer: D) -> Result<$enum, D::Error>
                where D: Deserializer<'de>
            {
                struct EnumVisitor;

                impl<'de> Visitor<'de> for EnumVisitor {
                    type Value = $enum;

                    fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
                        write!(formatter, concat!("valid ", stringify!($enum)))
                    }

                    fn visit_i64<E>(self, value: i64) -> Result<$enum, E>
                        where E: de::Error
                    {
                        match value {
                            $( $value => Ok($enum::$variant), )*
                            _ => Err(E::invalid_value(Unexpected::Signed(value), &self))
                        }
                    }
                }

                deserializer.deserialize_i64(EnumVisitor)
            }

            #[cfg(test)]
            mod tests {
                use toml;
                use rocksdb::$enum;

                #[test]
                fn test_serde() {
                    #[derive(Serialize, Deserialize, PartialEq)]
                    struct EnumHolder {
                        #[serde(with = "super")]
                        e: $enum,
                    }

                    let cases = vec![
                        $(($enum::$variant, $value), )*
                    ];
                    for (e, v) in cases {
                        let holder = EnumHolder { e };
                        let res = toml::to_string(&holder).unwrap();
                        let exp = format!("e = {}\n", v);
                        assert_eq!(res, exp);
                        let h: EnumHolder = toml::from_str(&exp).unwrap();
                        assert!(h == holder);
                    }
                }
            }
        }
    }
}

numeric_enum_mod! {compaction_pri_serde CompactionPriority {
    ByCompensatedSize = 0,
    OldestLargestSeqFirst = 1,
    OldestSmallestSeqFirst = 2,
    MinOverlappingRatio = 3,
}}

numeric_enum_mod! {rate_limiter_mode_serde DBRateLimiterMode {
    ReadOnly = 1,
    WriteOnly = 2,
    AllIo = 3,
}}

numeric_enum_mod! {compaction_style_serde DBCompactionStyle {
    Level = 0,
    Universal = 1,
}}

numeric_enum_mod! {recovery_mode_serde DBRecoveryMode {
    TolerateCorruptedTailRecords = 0,
    AbsoluteConsistency = 1,
    PointInTime = 2,
    SkipAnyCorruptedRecords = 3,
}}

numeric_enum_mod! {perf_level_serde PerfLevel {
    Uninitialized = 0,
    Disable = 1,
    EnableCount = 2,
    EnableTimeExceptForMutex = 3,
    EnableTimeAndCPUTimeExceptForMutex = 4,
    EnableTime = 5,
    OutOfBounds = 6,
}}

#[cfg(test)]
mod tests {
    use super::*;
    use rocksdb::DBCompressionType;

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
        assert!(toml::from_str::<CompressionTypeHolder>(
            r#"tp = [
            "no", "no", "no", "no", "no", "no", "no", "no"
        ]"#
        )
        .is_err());
        // value is wrong.
        assert!(toml::from_str::<CompressionTypeHolder>(
            r#"tp = [
            "no", "no", "no", "no", "no", "no", "yes"
        ]"#
        )
        .is_err());
    }
}
