// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

#![feature(min_specialization)]

mod api_v1;
mod api_v1ttl;
pub mod api_v2;

use engine_traits::Result;
use kvproto::kvrpcpb::ApiVersion;

pub use match_template::match_template;
use txn_types::{Key, TimeStamp};

pub trait KvFormat: Clone + Copy + 'static + Send + Sync {
    const TAG: ApiVersion;
    /// Corresponding TAG of client requests. For test only.
    #[cfg(any(test, feature = "testexport"))]
    const CLIENT_TAG: ApiVersion;
    const IS_TTL_ENABLED: bool;

    /// Parse the key prefix and infer key mode. It's safe to parse either raw key or encoded key.
    fn parse_key_mode(key: &[u8]) -> KeyMode;
    fn parse_range_mode(range: (Option<&[u8]>, Option<&[u8]>)) -> KeyMode;

    /// Parse from the bytes from storage.
    fn decode_raw_value(bytes: &[u8]) -> Result<RawValue<&[u8]>>;
    /// This is equivalent to `decode_raw_value()` but returns the owned user value.
    fn decode_raw_value_owned(mut bytes: Vec<u8>) -> Result<RawValue<Vec<u8>>> {
        let (len, expire_ts, is_delete) = {
            let raw_value = Self::decode_raw_value(&bytes)?;
            (
                raw_value.user_value.len(),
                raw_value.expire_ts,
                raw_value.is_delete,
            )
        };
        // The user value are always the first part in encoded bytes.
        bytes.truncate(len);
        Ok(RawValue {
            user_value: bytes,
            expire_ts,
            is_delete,
        })
    }
    /// Encode the raw value and it's metadata into bytes.
    fn encode_raw_value(value: RawValue<&[u8]>) -> Vec<u8>;
    /// This is equivalent to `encode_raw_value` but reduced an allocation.
    fn encode_raw_value_owned(value: RawValue<Vec<u8>>) -> Vec<u8>;

    /// Parse from the txn_types::Key from storage. Default implementation for API V1|V1TTL.
    /// Return: (user key, optional timestamp)
    fn decode_raw_key(encoded_key: &Key, _with_ts: bool) -> Result<(Vec<u8>, Option<TimeStamp>)> {
        Ok((encoded_key.as_encoded().clone(), None))
    }
    /// This is equivalent to `decode_raw_key()` but returns the owned user key.
    fn decode_raw_key_owned(
        encoded_key: Key,
        _with_ts: bool,
    ) -> Result<(Vec<u8>, Option<TimeStamp>)> {
        Ok((encoded_key.into_encoded(), None))
    }
    /// Encode the user key & optional timestamp into txn_types::Key. Default implementation for API V1|V1TTL.
    fn encode_raw_key(user_key: &[u8], _ts: Option<TimeStamp>) -> Key {
        Key::from_encoded_slice(user_key)
    }
    /// This is equivalent to `encode_raw_key` but reduced an allocation.
    fn encode_raw_key_owned(user_key: Vec<u8>, _ts: Option<TimeStamp>) -> Key {
        Key::from_encoded(user_key)
    }

    // Convert the encoded key from src_api version to Self::TAG version
    fn convert_raw_encoded_key_version_from(
        src_api: ApiVersion,
        key: &[u8],
        ts: Option<TimeStamp>,
    ) -> Result<Key>;

    // Convert the user key range from src_api version to Self::TAG version
    fn convert_raw_user_key_range_version_from(
        src_api: ApiVersion,
        start_key: Vec<u8>,
        end_key: Vec<u8>,
    ) -> (Vec<u8>, Vec<u8>);

    /// Convert the encoded value from src_api version to Self::TAG version
    fn convert_raw_encoded_value_version_from(
        src_api: ApiVersion,
        value: &[u8],
    ) -> Result<Vec<u8>> {
        if src_api == Self::TAG {
            return Ok(value.to_owned());
        }
        dispatch_api_version!(src_api, {
            let raw_value = API::decode_raw_value(value)?;
            Ok(Self::encode_raw_value(raw_value))
        })
    }
}

#[derive(Default, Clone, Copy)]
pub struct ApiV1;
#[derive(Default, Clone, Copy)]
pub struct ApiV1Ttl;
#[derive(Default, Clone, Copy)]
pub struct ApiV2;

#[macro_export]
macro_rules! test_kv_format_impl {
    ($func:ident<$ver:ident $($left_ver:ident)*> $(($($arg:expr),*))?) => {
        $crate::test_kv_format_impl!(__imp $func<$ver> $(($($arg),*))?);
        $crate::test_kv_format_impl!($func<$($left_ver)*> $(($($arg),*))?);
    };
    ($func:ident<> $(($($arg:expr),*))?) => {
    };
    ($func:ident $(($($arg:expr),*))?) => {
        $crate::test_kv_format_impl!($func<ApiV1 ApiV1Ttl ApiV2>$(($($arg),*))?);
    };
    (__imp $func:ident<$ver:ident>) => {
        $func::<$crate::$ver>();
    };
    (__imp $func:ident<$ver:ident>($($arg:expr),*)) => {
        $func::<$crate::$ver>($($arg),*);
    };
}

// TODO: move `match_template_api_version!` usage to `dispatch_api_version!`.
#[macro_export]
macro_rules! match_template_api_version {
     ($t:tt, $($tail:tt)*) => {{
         $crate::match_template! {
             $t = [
                V1 => $crate::ApiV1,
                V1ttl => $crate::ApiV1Ttl,
                V2 => $crate::ApiV2,
            ],
            $($tail)*
         }
     }}
}

/// Dispatch an expression with type `kvproto::kvrpcpb::ApiVersion` to corresponding concrete type of `KvFormat`
///
/// For example, the following code
///
/// ```ignore
/// let encoded_key = dispatch_api_version(api_version, API::encode_raw_key(key));
/// ```
///
/// generates
///
/// ```ignore
/// let encoded_key = match api_version {
///     ApiVersion::V1 => ApiV1::encode_raw_key(key),
///     ApiVersion::V1ttl => ApiV1Ttl::encode_raw_key(key),
///     ApiVersion::V2 => ApiV2::encode_raw_key(key),
/// };
/// ```
#[macro_export]
macro_rules! dispatch_api_version {
    ($api_version:expr, $e:expr) => {{
        $crate::match_template! {
            API = [
                V1 => $crate::ApiV1,
                V1ttl => $crate::ApiV1Ttl,
                V2 => $crate::ApiV2,
            ],
            match $api_version {
                kvproto::kvrpcpb::ApiVersion::API => $e,
            }
        }
    }};
}

/// The key mode inferred from the key prefix.
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
    /// Logical deletion flag in ApiV2, should be `false` in ApiV1 and ApiV1Ttl
    pub is_delete: bool,
}

impl<T: AsRef<[u8]>> RawValue<T> {
    #[inline]
    pub fn is_valid(&self, current_ts: u64) -> bool {
        !self.is_delete
            && self
                .expire_ts
                .map_or(true, |expire_ts| expire_ts > current_ts)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::api_v2::*;

    #[test]
    fn test_parse() {
        assert_eq!(ApiV1::parse_key_mode(&b"t_a"[..]), KeyMode::Unknown);
        assert_eq!(ApiV1Ttl::parse_key_mode(&b"ot"[..]), KeyMode::Raw);
        assert_eq!(
            ApiV2::parse_key_mode(&[RAW_KEY_PREFIX, b'a', b'b']),
            KeyMode::Raw
        );
        assert_eq!(ApiV2::parse_key_mode(&[RAW_KEY_PREFIX]), KeyMode::Raw);
        assert_eq!(ApiV2::parse_key_mode(&[TXN_KEY_PREFIX]), KeyMode::Txn);
        assert_eq!(ApiV2::parse_key_mode(&b"t_a"[..]), KeyMode::TiDB);
        assert_eq!(ApiV2::parse_key_mode(&b"m"[..]), KeyMode::TiDB);
        assert_eq!(ApiV2::parse_key_mode(&b"ot"[..]), KeyMode::Unknown);
    }

    #[test]
    fn test_parse_range() {
        assert_eq!(ApiV1::parse_range_mode((None, None)), KeyMode::Unknown);
        assert_eq!(
            ApiV1::parse_range_mode((Some(b"x"), None)),
            KeyMode::Unknown
        );
        assert_eq!(
            ApiV1Ttl::parse_range_mode((Some(b"m_a"), Some(b"na"))),
            KeyMode::Raw
        );
        assert_eq!(
            ApiV2::parse_range_mode((Some(b"t_a"), Some(b"t_z"))),
            KeyMode::TiDB
        );
        assert_eq!(
            ApiV2::parse_range_mode((Some(b"t"), Some(b"u"))),
            KeyMode::TiDB
        );
        assert_eq!(
            ApiV2::parse_range_mode((Some(b"m"), Some(b"n"))),
            KeyMode::TiDB
        );
        assert_eq!(
            ApiV2::parse_range_mode((Some(b"m_a"), Some(b"m_z"))),
            KeyMode::TiDB
        );
        assert_eq!(
            ApiV2::parse_range_mode((Some(b"x\0a"), Some(b"x\0z"))),
            KeyMode::Txn
        );
        assert_eq!(
            ApiV2::parse_range_mode((Some(b"x"), Some(b"y"))),
            KeyMode::Txn
        );
        assert_eq!(
            ApiV2::parse_range_mode((Some(b"r\0a"), Some(b"r\0z"))),
            KeyMode::Raw
        );
        assert_eq!(
            ApiV2::parse_range_mode((Some(b"r"), Some(b"s"))),
            KeyMode::Raw
        );
        assert_eq!(
            ApiV2::parse_range_mode((Some(b"t_a"), Some(b"ua"))),
            KeyMode::Unknown
        );
        assert_eq!(
            ApiV2::parse_range_mode((Some(b"t"), None)),
            KeyMode::Unknown
        );
        assert_eq!(
            ApiV2::parse_range_mode((None, Some(b"t_z"))),
            KeyMode::Unknown
        );
        assert_eq!(
            ApiV2::parse_range_mode((Some(b"m_a"), Some(b"na"))),
            KeyMode::Unknown
        );
        assert_eq!(
            ApiV2::parse_range_mode((Some(b"m"), None)),
            KeyMode::Unknown
        );
        assert_eq!(
            ApiV2::parse_range_mode((None, Some(b"m_z"))),
            KeyMode::Unknown
        );
        assert_eq!(
            ApiV2::parse_range_mode((Some(b"x\0a"), Some(b"ya"))),
            KeyMode::Unknown
        );
        assert_eq!(
            ApiV2::parse_range_mode((Some(b"x"), None)),
            KeyMode::Unknown
        );
        assert_eq!(
            ApiV2::parse_range_mode((None, Some(b"x\0z"))),
            KeyMode::Unknown
        );
        assert_eq!(
            ApiV2::parse_range_mode((Some(b"r\0a"), Some(b"sa"))),
            KeyMode::Unknown
        );
        assert_eq!(
            ApiV2::parse_range_mode((Some(b"r"), None)),
            KeyMode::Unknown
        );
        assert_eq!(
            ApiV2::parse_range_mode((None, Some(b"r\0z"))),
            KeyMode::Unknown
        );
    }

    #[test]
    fn test_no_meta() {
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
            assert_raw_value_encode_decode_identity(case.0, None, false, case.1, ApiVersion::V1);
        }
        for case in &cases {
            assert_raw_value_encode_decode_identity(case.0, None, false, case.2, ApiVersion::V1ttl);
        }
        for case in &cases {
            assert_raw_value_encode_decode_identity(case.0, None, false, case.3, ApiVersion::V2);
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
            assert_raw_value_encode_decode_identity(
                case.0,
                Some(case.1),
                false,
                case.2,
                ApiVersion::V1ttl,
            );
        }
        for case in &cases {
            assert_raw_value_encode_decode_identity(
                case.0,
                Some(case.1),
                false,
                case.3,
                ApiVersion::V2,
            );
        }
    }

    #[test]
    fn test_meta_api_v2() {
        // (user_value, expire_ts, is_delete, ecoded_bytes_v2)
        let cases = vec![
            // only deletion flag.
            (&b""[..], None, true, &[2][..]),
            (&b""[..], None, false, &[0][..]),
            // deletion flag with value.
            (&b""[..], Some(2), true, &[0, 0, 0, 0, 0, 0, 0, 2, 3][..]),
            (
                &b"a"[..],
                Some(2),
                true,
                &[b'a', 0, 0, 0, 0, 0, 0, 0, 2, 3][..],
            ),
            (&b""[..], Some(2), false, &[0, 0, 0, 0, 0, 0, 0, 2, 1][..]),
            (
                &b"a"[..],
                Some(2),
                false,
                &[b'a', 0, 0, 0, 0, 0, 0, 0, 2, 1][..],
            ),
        ];

        for case in cases {
            assert_raw_value_encode_decode_identity(case.0, case.1, case.2, case.3, ApiVersion::V2);
        }
    }

    #[test]
    fn test_value_decode_err() {
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
            (vec![4], ApiVersion::V2),
            (vec![1, 2, 3, 4, 5, 6, 7, 8, 4], ApiVersion::V2),
        ];

        for (bytes, api_version) in cases {
            dispatch_api_version!(api_version, {
                assert!(API::decode_raw_value(&bytes).is_err());
                assert!(API::decode_raw_value_owned(bytes).is_err());
            })
        }
    }

    #[test]
    fn test_value_valid() {
        let cases = vec![
            // expire_ts, is_delete, expect_is_valid
            (None, false, true),
            (None, true, false),
            (Some(5), false, false),
            (Some(5), true, false),
            (Some(100), false, true),
            (Some(100), true, false),
        ];

        for (idx, (expire_ts, is_delete, expect_is_valid)) in cases.into_iter().enumerate() {
            let raw_value = RawValue {
                user_value: b"value",
                expire_ts,
                is_delete,
            };
            assert_eq!(raw_value.is_valid(10), expect_is_valid, "case {}", idx);
        }
    }

    fn assert_raw_value_encode_decode_identity(
        user_value: &[u8],
        expire_ts: Option<u64>,
        is_delete: bool,
        encoded_bytes: &[u8],
        api_version: ApiVersion,
    ) {
        dispatch_api_version!(api_version, {
            let raw_value = RawValue {
                user_value,
                expire_ts,
                is_delete,
            };
            assert_eq!(&API::encode_raw_value(raw_value), encoded_bytes);
            assert_eq!(API::decode_raw_value(encoded_bytes).unwrap(), raw_value);

            let raw_value = RawValue {
                user_value: user_value.to_vec(),
                expire_ts,
                is_delete,
            };
            assert_eq!(
                API::encode_raw_value_owned(raw_value.clone()),
                encoded_bytes
            );
            assert_eq!(
                API::decode_raw_value_owned(encoded_bytes.to_vec()).unwrap(),
                raw_value
            );
        })
    }

    #[test]
    fn test_raw_key() {
        // (user_key, ts, encoded_bytes_V1, encoded_bytes_V1ttl, encoded_bytes_V2)
        let cases = vec![
            (
                &b""[..],
                None,
                &[][..],
                &[][..],
                &[0, 0, 0, 0, 0, 0, 0, 0, 0xf7][..],
            ),
            (
                &b"r"[..],
                None,
                &[b'r'][..],
                &[b'r'][..],
                &[b'r', 0, 0, 0, 0, 0, 0, 0, 0xf8][..],
            ),
            (
                &b"r"[..],
                Some(2.into()),
                &[b'r'][..],
                &[b'r'][..],
                &[
                    b'r', 0, 0, 0, 0, 0, 0, 0, 0xf8, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xfd,
                ][..],
            ),
            (
                &b"r234567890"[..],
                Some(3.into()),
                &b"r234567890"[..],
                &b"r234567890"[..],
                &[
                    b'r', b'2', b'3', b'4', b'5', b'6', b'7', b'8', 0xff, b'9', b'0', 0, 0, 0, 0,
                    0, 0, 0xf9, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xfc,
                ][..],
            ),
        ];

        for case in &cases {
            assert_raw_key_encode_decode_identity(case.0, case.1, case.2, ApiVersion::V1, None);
        }
        for case in &cases {
            assert_raw_key_encode_decode_identity(case.0, case.1, case.3, ApiVersion::V1ttl, None);
        }
        for case in &cases {
            if !case.0.is_empty() {
                assert_raw_key_encode_decode_identity(
                    case.0,
                    case.1,
                    case.4,
                    ApiVersion::V2,
                    case.1,
                );
            }
        }
    }

    fn assert_raw_key_encode_decode_identity(
        user_key: &[u8],
        ts: Option<TimeStamp>,
        encoded_bytes: &[u8],
        api_version: ApiVersion,
        expected_ts: Option<TimeStamp>,
    ) {
        dispatch_api_version!(api_version, {
            let encoded_key = Key::from_encoded_slice(encoded_bytes);

            assert_eq!(
                &API::encode_raw_key(user_key, ts).into_encoded(),
                encoded_bytes
            );
            assert_eq!(
                API::decode_raw_key(&encoded_key, expected_ts.is_some()).unwrap(),
                (user_key.to_vec(), expected_ts)
            );

            assert_eq!(
                &API::encode_raw_key_owned(user_key.to_vec(), ts).into_encoded(),
                encoded_bytes
            );
            assert_eq!(
                API::decode_raw_key_owned(encoded_key, expected_ts.is_some()).unwrap(),
                (user_key.to_vec(), expected_ts)
            );
        })
    }

    #[test]
    fn test_raw_key_convert() {
        let timestamp = 30;
        let apiv1_keys = vec![
            b""[..].to_owned(),
            b"abc"[..].to_owned(),
            b"api_ver_test"[..].to_owned(),
        ];
        let apiv2_keys: Vec<Vec<u8>> = apiv1_keys
            .clone()
            .into_iter()
            .map(|key| {
                let mut v2_key = key;
                v2_key.insert(0, RAW_KEY_PREFIX);
                ApiV2::encode_raw_key_owned(v2_key, Some(TimeStamp::from(timestamp))).into_encoded()
            })
            .collect();
        // src_api_ver, dst_api_ver, src_data, dst_data
        let test_cases = vec![
            (ApiVersion::V1, ApiVersion::V2, &apiv1_keys, &apiv2_keys),
            (ApiVersion::V1ttl, ApiVersion::V2, &apiv1_keys, &apiv2_keys),
            (ApiVersion::V2, ApiVersion::V1, &apiv2_keys, &apiv1_keys),
            (ApiVersion::V2, ApiVersion::V1ttl, &apiv2_keys, &apiv1_keys),
        ];
        for i in 0..apiv1_keys.len() {
            for (src_api_ver, dst_api_ver, src_data, dst_data) in test_cases.clone() {
                let dst_key = dispatch_api_version!(dst_api_ver, {
                    API::convert_raw_encoded_key_version_from(
                        src_api_ver,
                        &src_data[i],
                        Some(TimeStamp::from(timestamp)),
                    )
                });
                assert_eq!(dst_key.unwrap().into_encoded(), dst_data[i]);
            }
        }
    }

    #[test]
    fn test_raw_value_convert() {
        let apiv1_values = vec![
            b""[..].to_owned(),
            b"abc"[..].to_owned(),
            b"api_ver_test"[..].to_owned(),
        ];
        let apiv1ttl_values: Vec<Vec<u8>> = apiv1_values
            .clone()
            .into_iter()
            .map(|value| {
                let raw_value = RawValue {
                    user_value: value,
                    expire_ts: None,
                    is_delete: false,
                };
                ApiV1Ttl::encode_raw_value_owned(raw_value)
            })
            .collect();
        let apiv2_values: Vec<Vec<u8>> = apiv1_values
            .clone()
            .into_iter()
            .map(|value| {
                let raw_value = RawValue {
                    user_value: value,
                    expire_ts: None,
                    is_delete: false,
                };
                ApiV2::encode_raw_value_owned(raw_value)
            })
            .collect();
        // src_api_ver, dst_api_ver, src_data, dst_data
        let test_cases = vec![
            (ApiVersion::V1, ApiVersion::V2, &apiv1_values, &apiv2_values),
            (
                ApiVersion::V1ttl,
                ApiVersion::V2,
                &apiv1ttl_values,
                &apiv2_values,
            ),
            (ApiVersion::V2, ApiVersion::V1, &apiv2_values, &apiv1_values),
            (
                ApiVersion::V2,
                ApiVersion::V1ttl,
                &apiv2_values,
                &apiv1ttl_values,
            ),
        ];
        for i in 0..apiv1_values.len() {
            for (src_api_ver, dst_api_ver, src_data, dst_data) in test_cases.clone() {
                let dst_value = dispatch_api_version!(dst_api_ver, {
                    API::convert_raw_encoded_value_version_from(src_api_ver, &src_data[i])
                });
                assert_eq!(dst_value.unwrap(), dst_data[i]);
            }
        }
    }

    #[test]
    fn test_convert_raw_user_key_range() {
        let apiv1_key_ranges = vec![
            (b""[..].to_owned(), b""[..].to_owned()),
            (b"abc"[..].to_owned(), b"abz"[..].to_owned()),
            (
                b"api_ver_test"[..].to_owned(),
                b"bpi_ver_test"[..].to_owned(),
            ),
        ];
        let apiv2_key_ranges: Vec<(Vec<u8>, Vec<u8>)> = apiv1_key_ranges
            .clone()
            .into_iter()
            .map(|(start_key, end_key)| {
                let mut v2_start_key = start_key;
                let mut v2_end_key = end_key;
                v2_start_key.insert(0, RAW_KEY_PREFIX);
                if v2_end_key.is_empty() {
                    v2_end_key.insert(0, RAW_KEY_PREFIX_END);
                } else {
                    v2_end_key.insert(0, RAW_KEY_PREFIX);
                }
                (v2_start_key, v2_end_key)
            })
            .collect();
        // src_api_ver, dst_api_ver, src_data, dst_data
        let test_cases = vec![
            (
                ApiVersion::V1,
                ApiVersion::V2,
                &apiv1_key_ranges,
                &apiv2_key_ranges,
            ),
            (
                ApiVersion::V1ttl,
                ApiVersion::V2,
                &apiv1_key_ranges,
                &apiv2_key_ranges,
            ),
            (
                ApiVersion::V2,
                ApiVersion::V1,
                &apiv2_key_ranges,
                &apiv1_key_ranges,
            ),
            (
                ApiVersion::V2,
                ApiVersion::V1ttl,
                &apiv2_key_ranges,
                &apiv1_key_ranges,
            ),
        ];
        for (src_api_ver, dst_api_ver, src_data, dst_data) in test_cases {
            for i in 0..apiv1_key_ranges.len() {
                let dst_key_range = dispatch_api_version!(dst_api_ver, {
                    let (src_start, src_end) = src_data[i].clone();
                    API::convert_raw_user_key_range_version_from(src_api_ver, src_start, src_end)
                });
                assert_eq!(dst_key_range, dst_data[i]);
            }
        }
    }
}
