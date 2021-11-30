// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

use super::{Error, Result};
use kvproto::kvrpcpb::ApiVersion;
use tikv_util::codec;
use tikv_util::codec::number::{self, NumberEncoder};

pub fn ttl_current_ts() -> u64 {
    fail_point!("ttl_current_ts", |r| r.map_or(2, |e| e.parse().unwrap()));
    tikv_util::time::UnixSecs::now().into_inner()
}

pub fn ttl_to_expire_ts(ttl: u64) -> Option<u64> {
    if ttl == 0 {
        None
    } else {
        Some(ttl.saturating_add(ttl_current_ts()))
    }
}

bitflags::bitflags! {
    struct ValueMeta: u8 {
        const EXPIRE_TS = 0b00000001;
    }
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

impl<'a> RawValue<&'a [u8]> {
    /// Parse from the bytes from storage.
    pub fn from_bytes(bytes: &'a [u8], api_version: ApiVersion) -> Result<Self> {
        match api_version {
            ApiVersion::V1 => Ok(RawValue {
                user_value: bytes,
                expire_ts: None,
            }),
            ApiVersion::V1ttl => {
                let rest_len = bytes
                    .len()
                    .checked_sub(number::U64_SIZE)
                    .ok_or(Error::Codec(codec::Error::ValueLength))?;
                let mut expire_ts_slice = &bytes[rest_len..];
                let expire_ts = number::decode_u64(&mut expire_ts_slice)?;
                let expire_ts = if expire_ts == 0 {
                    None
                } else {
                    Some(expire_ts)
                };
                Ok(RawValue {
                    user_value: &bytes[..rest_len],
                    expire_ts,
                })
            }
            ApiVersion::V2 => {
                let mut rest_len = bytes
                    .len()
                    .checked_sub(1)
                    .ok_or(Error::Codec(codec::Error::ValueLength))?;
                let flags = ValueMeta::from_bits(bytes[rest_len])
                    .ok_or(Error::Codec(codec::Error::ValueMeta))?;
                let expire_ts = if flags.contains(ValueMeta::EXPIRE_TS) {
                    rest_len = rest_len
                        .checked_sub(number::U64_SIZE)
                        .ok_or(Error::Codec(codec::Error::ValueLength))?;
                    let mut expire_ts_slice = &bytes[rest_len..rest_len + number::U64_SIZE];
                    Some(number::decode_u64(&mut expire_ts_slice)?)
                } else {
                    None
                };
                Ok(RawValue {
                    user_value: &bytes[..rest_len],
                    expire_ts,
                })
            }
        }
    }

    /// Encode the raw value and it's metadata into bytes.
    pub fn to_bytes(self, api_version: ApiVersion) -> Vec<u8> {
        match api_version {
            ApiVersion::V1 => self.user_value.to_vec(),
            ApiVersion::V1ttl => {
                let mut buf = Vec::with_capacity(self.user_value.len() + number::U64_SIZE);
                buf.extend_from_slice(self.user_value);
                buf.encode_u64(self.expire_ts.unwrap_or(0)).unwrap();
                buf
            }
            ApiVersion::V2 => {
                let mut flags = ValueMeta::empty();
                let mut meta_size = 1;
                if self.expire_ts.is_some() {
                    flags.insert(ValueMeta::EXPIRE_TS);
                    meta_size += number::U64_SIZE;
                }
                let mut buf = Vec::with_capacity(self.user_value.len() + meta_size);
                buf.extend_from_slice(self.user_value);
                if let Some(expire_ts) = self.expire_ts {
                    buf.encode_u64(expire_ts).unwrap();
                }
                buf.push(flags.bits());
                buf
            }
        }
    }
}

impl RawValue<Vec<u8>> {
    /// This is equivalent to `RawValue::from_bytes()` but returns the owned user value.
    pub fn from_owned_bytes(mut bytes: Vec<u8>, api_version: ApiVersion) -> Result<Self> {
        let (len, expire_ts) = {
            let raw_value = RawValue::from_bytes(&bytes, api_version)?;
            (raw_value.user_value.len(), raw_value.expire_ts)
        };
        // The user value are always the first part in encoded bytes.
        bytes.truncate(len);
        Ok(Self {
            user_value: bytes,
            expire_ts,
        })
    }

    /// This is equivalent to `RawValue::<&[u8]>::to_bytes()` but reduced an allocation.
    pub fn to_bytes(mut self, api_version: ApiVersion) -> Vec<u8> {
        match api_version {
            ApiVersion::V1 => self.user_value,
            ApiVersion::V1ttl => {
                self.user_value.reserve(number::U64_SIZE);
                self.user_value
                    .encode_u64(self.expire_ts.unwrap_or(0))
                    .unwrap();
                self.user_value
            }
            ApiVersion::V2 => {
                let mut flags = ValueMeta::empty();
                let mut meta_size = 1;
                if self.expire_ts.is_some() {
                    flags.insert(ValueMeta::EXPIRE_TS);
                    meta_size += number::U64_SIZE;
                }
                self.user_value.reserve(meta_size);
                if let Some(expire_ts) = self.expire_ts {
                    self.user_value.encode_u64(expire_ts).unwrap();
                }
                self.user_value.push(flags.bits());
                self.user_value
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::assert_matches::assert_matches;

    use super::*;

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
            assert_matches!(RawValue::from_bytes(&bytes, api_version), Err(_));
            assert_matches!(RawValue::from_owned_bytes(bytes, api_version), Err(_));
        }
    }

    fn assert_encode_decode_identity(
        user_value: &[u8],
        expire_ts: Option<u64>,
        encoded_bytes: &[u8],
        api_version: ApiVersion,
    ) {
        let raw_value = RawValue {
            user_value,
            expire_ts,
        };
        assert_eq!(&raw_value.to_bytes(api_version), encoded_bytes);
        assert_eq!(
            RawValue::from_bytes(encoded_bytes, api_version).unwrap(),
            raw_value
        );

        let raw_value = RawValue {
            user_value: user_value.to_vec(),
            expire_ts,
        };
        assert_eq!(raw_value.clone().to_bytes(api_version), encoded_bytes);
        assert_eq!(
            RawValue::from_owned_bytes(encoded_bytes.to_vec(), api_version).unwrap(),
            raw_value
        );
    }
}
