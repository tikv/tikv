// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

#![feature(min_specialization)]

mod api_v1;
mod api_v1ttl;
pub mod api_v2;

use engine_traits::Result;
use kvproto::kvrpcpb::ApiVersion;

pub use match_template::match_template;

pub trait APIVersion: Clone + Copy + 'static + Send + Sync {
    const TAG: ApiVersion;
    const IS_TTL_ENABLED: bool;

    /// Parse the key prefix and infer key mode. It's safe to parse either raw key or encoded key.
    fn parse_key_mode(key: &[u8]) -> KeyMode;
    fn parse_range_mode(range: (Option<&[u8]>, Option<&[u8]>)) -> KeyMode;
    /// Parse from the bytes from storage.
    fn decode_raw_value(bytes: &[u8]) -> Result<RawValue<&[u8]>>;
    /// This is equivalent to `decode_raw_value()` but returns the owned user value.
    fn decode_raw_value_owned(mut bytes: Vec<u8>) -> Result<RawValue<Vec<u8>>> {
        let (len, expire_ts) = {
            let raw_value = Self::decode_raw_value(&bytes)?;
            (raw_value.user_value.len(), raw_value.expire_ts)
        };
        // The user value are always the first part in encoded bytes.
        bytes.truncate(len);
        Ok(RawValue {
            user_value: bytes,
            expire_ts,
        })
    }
    /// Encode the raw value and it's metadata into bytes.
    fn encode_raw_value(value: RawValue<&[u8]>) -> Vec<u8>;
    /// This is equivalent to `encode_raw_value` but reduced an allocation.
    fn encode_raw_value_owned(value: RawValue<Vec<u8>>) -> Vec<u8>;
}

#[derive(Default, Clone, Copy)]
pub struct APIV1;
#[derive(Default, Clone, Copy)]
pub struct APIV1TTL;
#[derive(Default, Clone, Copy)]
pub struct APIV2;

#[macro_export]
macro_rules! match_template_api_version {
     ($t:tt, $($tail:tt)*) => {{
         $crate::match_template! {
             $t = [
                V1 => $crate::APIV1,
                V1ttl => $crate::APIV1TTL,
                V2 => $crate::APIV2,
            ],
            $($tail)*
         }
     }}
}

/// The key mode infered from the key prefix.
#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub enum KeyMode {
    /// Raw key.
    Raw,
    /// Transaction key.
    Txn,
    /// TiDB key.
    ///
    /// It doesn't mean that the key is certainly written by
    /// TiDB, but instead, it means that the key matches the definition of
    /// TiDB key in API V2, therefore, the key is treated as TiDB data in
    /// order to fulfill compatibility.
    TiDB,
    /// Unrecognised key mode.
    Unknown,
}

/// A RawKV value and it's metadata.
///
/// ### ApiVersion::V1
///
/// This is the plain user value.
///
/// ### ApiVersion::V1ttl
///
/// 8 bytes representing the unix timestamp in seconds for expiring time will be append
/// to the value of all RawKV kv pairs.
///
/// ```text
/// ------------------------------------------------------------
/// | User value     | Expire Ts                               |
/// ------------------------------------------------------------
/// | 0x12 0x34 0x56 | 0x00 0x00 0x00 0x00 0x00 0x00 0xff 0xff |
/// ------------------------------------------------------------
/// ```
///
/// ### ApiVersion::V2
///
/// The last byte in the raw value must be a meta flag. For example:
///
/// ```text
/// --------------------------------------
/// | User value     | Meta flags        |
/// --------------------------------------
/// | 0x12 0x34 0x56 | 0x00 (0b00000000) |
/// --------------------------------------
/// ```
///
/// As shown in the example below, the least significant bit of the meta flag
/// indicates whether the value contains 8 bytes expire ts at the very left to the
/// meta flags.
///
/// ```text
/// --------------------------------------------------------------------------------
/// | User value     | Expire Ts                               | Meta flags        |
/// --------------------------------------------------------------------------------
/// | 0x12 0x34 0x56 | 0x00 0x00 0x00 0x00 0x00 0x00 0xff 0xff | 0x01 (0b00000001) |
/// --------------------------------------------------------------------------------
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RawValue<T: AsRef<[u8]>> {
    /// The user value.
    pub user_value: T,
    /// The unix timestamp in seconds indicating the point of time that this key will be deleted.
    pub expire_ts: Option<u64>,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::api_v2::*;

    #[test]
    fn test_parse() {
        assert_eq!(APIV1::parse_key_mode(&b"t_a"[..]), KeyMode::Unknown);
        assert_eq!(APIV1TTL::parse_key_mode(&b"ot"[..]), KeyMode::Raw);
        assert_eq!(
            APIV2::parse_key_mode(&[RAW_KEY_PREFIX, b'a', b'b']),
            KeyMode::Raw
        );
        assert_eq!(APIV2::parse_key_mode(&[RAW_KEY_PREFIX]), KeyMode::Raw);
        assert_eq!(APIV2::parse_key_mode(&[TXN_KEY_PREFIX]), KeyMode::Txn);
        assert_eq!(APIV2::parse_key_mode(&b"t_a"[..]), KeyMode::TiDB);
        assert_eq!(APIV2::parse_key_mode(&b"m"[..]), KeyMode::TiDB);
        assert_eq!(APIV2::parse_key_mode(&b"ot"[..]), KeyMode::Unknown);
    }

    #[test]
    fn test_parse_range() {
        assert_eq!(APIV1::parse_range_mode((None, None)), KeyMode::Unknown);
        assert_eq!(
            APIV1::parse_range_mode((Some(b"x"), None)),
            KeyMode::Unknown
        );
        assert_eq!(
            APIV1TTL::parse_range_mode((Some(b"m_a"), Some(b"na"))),
            KeyMode::Raw
        );
        assert_eq!(
            APIV2::parse_range_mode((Some(b"t_a"), Some(b"t_z"))),
            KeyMode::TiDB
        );
        assert_eq!(
            APIV2::parse_range_mode((Some(b"t"), Some(b"u"))),
            KeyMode::TiDB
        );
        assert_eq!(
            APIV2::parse_range_mode((Some(b"m"), Some(b"n"))),
            KeyMode::TiDB
        );
        assert_eq!(
            APIV2::parse_range_mode((Some(b"m_a"), Some(b"m_z"))),
            KeyMode::TiDB
        );
        assert_eq!(
            APIV2::parse_range_mode((Some(b"x\0a"), Some(b"x\0z"))),
            KeyMode::Txn
        );
        assert_eq!(
            APIV2::parse_range_mode((Some(b"x"), Some(b"y"))),
            KeyMode::Txn
        );
        assert_eq!(
            APIV2::parse_range_mode((Some(b"r\0a"), Some(b"r\0z"))),
            KeyMode::Raw
        );
        assert_eq!(
            APIV2::parse_range_mode((Some(b"r"), Some(b"s"))),
            KeyMode::Raw
        );
        assert_eq!(
            APIV2::parse_range_mode((Some(b"t_a"), Some(b"ua"))),
            KeyMode::Unknown
        );
        assert_eq!(
            APIV2::parse_range_mode((Some(b"t"), None)),
            KeyMode::Unknown
        );
        assert_eq!(
            APIV2::parse_range_mode((None, Some(b"t_z"))),
            KeyMode::Unknown
        );
        assert_eq!(
            APIV2::parse_range_mode((Some(b"m_a"), Some(b"na"))),
            KeyMode::Unknown
        );
        assert_eq!(
            APIV2::parse_range_mode((Some(b"m"), None)),
            KeyMode::Unknown
        );
        assert_eq!(
            APIV2::parse_range_mode((None, Some(b"m_z"))),
            KeyMode::Unknown
        );
        assert_eq!(
            APIV2::parse_range_mode((Some(b"x\0a"), Some(b"ya"))),
            KeyMode::Unknown
        );
        assert_eq!(
            APIV2::parse_range_mode((Some(b"x"), None)),
            KeyMode::Unknown
        );
        assert_eq!(
            APIV2::parse_range_mode((None, Some(b"x\0z"))),
            KeyMode::Unknown
        );
        assert_eq!(
            APIV2::parse_range_mode((Some(b"r\0a"), Some(b"sa"))),
            KeyMode::Unknown
        );
        assert_eq!(
            APIV2::parse_range_mode((Some(b"r"), None)),
            KeyMode::Unknown
        );
        assert_eq!(
            APIV2::parse_range_mode((None, Some(b"r\0z"))),
            KeyMode::Unknown
        );
    }

    #[test]
    fn test_no_ttl() {
        // (user_value, encoded_bytes_V1, encoded_bytes_V1ttl, encoded_bytes_V2)
        let cases = vec![
            (&b""[..], &b""[..], &[0, 0, 0, 0, 0, 0, 0, 0][..], &[0][..]),
            (
                &b"a"[..],
                &b"a"[..],
                &[b'a', 0, 0, 0, 0, 0, 0, 0, 0][..],
                &[b'a', 0][..],
            ),
        ];
        for case in &cases {
            assert_encode_decode_identity(case.0, None, case.1, ApiVersion::V1);
        }
        for case in &cases {
            assert_encode_decode_identity(case.0, None, case.2, ApiVersion::V1ttl);
        }
        for case in &cases {
            assert_encode_decode_identity(case.0, None, case.3, ApiVersion::V2);
        }
    }

    #[test]
    fn test_ttl() {
        // (user_value, expire_ts, encoded_bytes_V1ttl, encoded_bytes_V2)
        let cases = vec![
            (
                &b""[..],
                2,
                &[0, 0, 0, 0, 0, 0, 0, 2][..],
                &[0, 0, 0, 0, 0, 0, 0, 2, 1][..],
            ),
            (
                &b"a"[..],
                2,
                &[b'a', 0, 0, 0, 0, 0, 0, 0, 2][..],
                &[b'a', 0, 0, 0, 0, 0, 0, 0, 2, 1][..],
            ),
        ];

        for case in &cases {
            assert_encode_decode_identity(case.0, Some(case.1), case.2, ApiVersion::V1ttl);
        }
        for case in &cases {
            assert_encode_decode_identity(case.0, Some(case.1), case.3, ApiVersion::V2);
        }
    }

    #[test]
    fn test_decode_err() {
        let cases = vec![
            // At least 8 bytes for expire_ts.
            (vec![], ApiVersion::V1ttl),
            (vec![1, 2, 3, 4, 5, 6, 7], ApiVersion::V1ttl),
            // At least 1 byte for flags.
            (vec![], ApiVersion::V2),
            // The last byte indicates that expire_ts is set, therefore 8 more bytes for
            // expire_ts is expected.
            (vec![1], ApiVersion::V2),
            (vec![1, 2, 3, 4, 5, 6, 7, 1], ApiVersion::V2),
            // Undefined flag.
            (vec![2], ApiVersion::V2),
            (vec![1, 2, 3, 4, 5, 6, 7, 8, 2], ApiVersion::V2),
        ];

        for (bytes, api_version) in cases {
            match_template_api_version!(
                API,
                match api_version {
                    ApiVersion::API => {
                        assert!(API::decode_raw_value(&bytes).is_err());
                        assert!(API::decode_raw_value_owned(bytes).is_err());
                    }
                }
            )
        }
    }

    fn assert_encode_decode_identity(
        user_value: &[u8],
        expire_ts: Option<u64>,
        encoded_bytes: &[u8],
        api_version: ApiVersion,
    ) {
        match_template_api_version!(
            API,
            match api_version {
                ApiVersion::API => {
                    let raw_value = RawValue {
                        user_value,
                        expire_ts,
                    };
                    assert_eq!(&API::encode_raw_value(raw_value), encoded_bytes);
                    assert_eq!(API::decode_raw_value(encoded_bytes).unwrap(), raw_value);

                    let raw_value = RawValue {
                        user_value: user_value.to_vec(),
                        expire_ts,
                    };
                    assert_eq!(
                        API::encode_raw_value_owned(raw_value.clone()),
                        encoded_bytes
                    );
                    assert_eq!(
                        API::decode_raw_value_owned(encoded_bytes.to_vec()).unwrap(),
                        raw_value
                    );
                }
            }
        )
    }
}
