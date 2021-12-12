// Copyright 2020 TiKV Project Authors. Licensed under Apache-2.0.

pub use crate::perf_context::PerfLevel;

// FIXME This is duplicated from engine_rocks
macro_rules! numeric_enum_mod {
    ($name:ident $enum:ident { $($variant:ident = $value:expr, )* }) => {
        pub mod $name {
            use std::fmt;

            use serde::{Serializer, Deserializer};
            use serde::de::{self, Unexpected, Visitor};
            use crate::$enum;

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
                use crate::$enum;

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

numeric_enum_mod! {perf_level_serde PerfLevel {
    Uninitialized = 0,
    Disable = 1,
    EnableCount = 2,
    EnableTimeExceptForMutex = 3,
    EnableTimeAndCPUTimeExceptForMutex = 4,
    EnableTime = 5,
    OutOfBounds = 6,
}}
