// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

use codec::byte::MemComparableByteCodec;
use engine_traits::Result;
use tikv_util::codec::{
    bytes,
    number::{self, NumberEncoder},
    Error,
};
use txn_types::{Key, TimeStamp};

use super::*;

pub const RAW_KEY_PREFIX: u8 = b'r';
pub const RAW_KEY_PREFIX_END: u8 = RAW_KEY_PREFIX + 1;
pub const TXN_KEY_PREFIX: u8 = b'x';
pub const TIDB_META_KEY_PREFIX: u8 = b'm';
pub const TIDB_TABLE_KEY_PREFIX: u8 = b't';

pub const TIDB_RANGES: &[(&[u8], &[u8])] = &[
    (&[TIDB_META_KEY_PREFIX], &[TIDB_META_KEY_PREFIX + 1]),
    (&[TIDB_TABLE_KEY_PREFIX], &[TIDB_TABLE_KEY_PREFIX + 1]),
];
pub const TIDB_RANGES_COMPLEMENT: &[(&[u8], &[u8])] = &[
    (&[], &[TIDB_META_KEY_PREFIX]),
    (&[TIDB_META_KEY_PREFIX + 1], &[TIDB_TABLE_KEY_PREFIX]),
    (&[TIDB_TABLE_KEY_PREFIX + 1], &[]),
];

bitflags::bitflags! {
    struct ValueMeta: u8 {
        const EXPIRE_TS = 0b0000_0001;
        const DELETE_FLAG = 0b0000_0010;
    }
}

impl KvFormat for ApiV2 {
    const TAG: ApiVersion = ApiVersion::V2;
    #[cfg(any(test, feature = "testexport"))]
    const CLIENT_TAG: ApiVersion = ApiVersion::V2;
    const IS_TTL_ENABLED: bool = true;

    fn parse_key_mode(key: &[u8]) -> KeyMode {
        if key.is_empty() {
            return KeyMode::Unknown;
        }

        match key[0] {
            RAW_KEY_PREFIX => KeyMode::Raw,
            TXN_KEY_PREFIX => KeyMode::Txn,
            TIDB_META_KEY_PREFIX | TIDB_TABLE_KEY_PREFIX => KeyMode::TiDB,
            _ => KeyMode::Unknown,
        }
    }

    fn parse_range_mode(range: (Option<&[u8]>, Option<&[u8]>)) -> KeyMode {
        match range {
            (Some(start), Some(end))
                if !start.is_empty()
                    && !end.is_empty()
                    && (start[0] == end[0] ||
                        // Special case to represent "".."" within a key mode
                        (end == [start[0] + 1])) =>
            {
                Self::parse_key_mode(start)
            }
            _ => KeyMode::Unknown,
        }
    }

    fn decode_raw_value(bytes: &[u8]) -> Result<RawValue<&[u8]>> {
        let mut rest_len = bytes.len().checked_sub(1).ok_or(Error::ValueLength)?;
        let flags = ValueMeta::from_bits(bytes[rest_len]).ok_or(Error::ValueMeta)?;
        let is_delete = flags.contains(ValueMeta::DELETE_FLAG);
        let expire_ts = if flags.contains(ValueMeta::EXPIRE_TS) {
            rest_len = rest_len
                .checked_sub(number::U64_SIZE)
                .ok_or(Error::ValueLength)?;
            let mut expire_ts_slice = &bytes[rest_len..rest_len + number::U64_SIZE];
            Some(number::decode_u64(&mut expire_ts_slice)?)
        } else {
            None
        };
        Ok(RawValue {
            user_value: &bytes[..rest_len],
            expire_ts,
            is_delete,
        })
    }

    fn encode_raw_value(value: RawValue<&[u8]>) -> Vec<u8> {
        let mut flags = ValueMeta::empty();
        let mut meta_size = 1;
        if value.expire_ts.is_some() {
            flags.insert(ValueMeta::EXPIRE_TS);
            meta_size += number::U64_SIZE;
        }
        if value.is_delete {
            flags.insert(ValueMeta::DELETE_FLAG);
        };
        let mut buf = Vec::with_capacity(value.user_value.len() + meta_size);
        buf.extend_from_slice(value.user_value);
        if let Some(expire_ts) = value.expire_ts {
            buf.encode_u64(expire_ts).unwrap();
        }
        buf.push(flags.bits());
        buf
    }

    fn encode_raw_value_owned(mut value: RawValue<Vec<u8>>) -> Vec<u8> {
        let mut flags = ValueMeta::empty();
        let mut meta_size = 1;
        if value.expire_ts.is_some() {
            flags.insert(ValueMeta::EXPIRE_TS);
            meta_size += number::U64_SIZE;
        }
        if value.is_delete {
            flags.insert(ValueMeta::DELETE_FLAG);
        };
        value.user_value.reserve(meta_size);
        if let Some(expire_ts) = value.expire_ts {
            value.user_value.encode_u64(expire_ts).unwrap();
        }
        value.user_value.push(flags.bits());
        value.user_value
    }

    fn decode_raw_key(encoded_key: &Key, with_ts: bool) -> Result<(Vec<u8>, Option<TimeStamp>)> {
        debug_assert!(is_valid_encoded_key(encoded_key, with_ts));
        let ts = decode_raw_key_timestamp(encoded_key, with_ts)?;
        Ok((encoded_key.to_raw()?, ts))
    }

    fn decode_raw_key_owned(
        encoded_key: Key,
        with_ts: bool,
    ) -> Result<(Vec<u8>, Option<TimeStamp>)> {
        debug_assert!(is_valid_encoded_key(&encoded_key, with_ts));
        let ts = decode_raw_key_timestamp(&encoded_key, with_ts)?;
        Ok((encoded_key.into_raw()?, ts))
    }

    // Note: `user_key` may not be `KeyMode::Raw`.
    // E.g., `raw_xxx_range` interfaces accept an exclusive end key just beyond the scope of raw keys.
    // The validity is ensured by client & Storage interfaces.
    fn encode_raw_key(user_key: &[u8], ts: Option<TimeStamp>) -> Key {
        let encoded_key = Key::from_raw(user_key);
        if let Some(ts) = ts {
            debug_assert!(is_valid_ts(ts));
            encoded_key.append_ts(ts)
        } else {
            encoded_key
        }
    }

    // Note: `user_key` may not be `KeyMode::Raw`.
    // E.g., `raw_xxx_range` interfaces accept an exclusive end key just beyond the scope of raw keys.
    // The validity is ensured by client & Storage interfaces.
    fn encode_raw_key_owned(mut user_key: Vec<u8>, ts: Option<TimeStamp>) -> Key {
        let src_len = user_key.len();
        let encoded_len = MemComparableByteCodec::encoded_len(src_len);

        // always reserve more U64_SIZE for ts, as it's likely to "append_ts" later, especially in raw write procedures.
        user_key.reserve(encoded_len - src_len + number::U64_SIZE);
        user_key.resize(encoded_len, 0u8);
        MemComparableByteCodec::encode_all_in_place(&mut user_key, src_len);

        let encoded_key = Key::from_encoded(user_key);
        if let Some(ts) = ts {
            debug_assert!(is_valid_ts(ts));
            encoded_key.append_ts(ts)
        } else {
            encoded_key
        }
    }

    // add prefix RAW_KEY_PREFIX
    fn convert_raw_encoded_key_version_from(
        src_api: ApiVersion,
        key: &[u8],
        ts: Option<TimeStamp>,
    ) -> Result<Key> {
        match src_api {
            ApiVersion::V1 | ApiVersion::V1ttl => {
                let mut apiv2_key = Vec::with_capacity(ApiV2::get_encode_len(key.len() + 1));
                apiv2_key.push(RAW_KEY_PREFIX);
                apiv2_key.extend(key);
                Ok(Self::encode_raw_key_owned(apiv2_key, ts))
            }
            ApiVersion::V2 => Ok(Key::from_encoded_slice(key)),
        }
    }

    fn convert_raw_user_key_range_version_from(
        src_api: ApiVersion,
        mut start_key: Vec<u8>,
        mut end_key: Vec<u8>,
    ) -> (Vec<u8>, Vec<u8>) {
        match src_api {
            ApiVersion::V1 | ApiVersion::V1ttl => {
                start_key.insert(0, RAW_KEY_PREFIX);
                if end_key.is_empty() {
                    end_key.insert(0, RAW_KEY_PREFIX_END);
                } else {
                    end_key.insert(0, RAW_KEY_PREFIX);
                }
                (start_key, end_key)
            }
            ApiVersion::V2 => (start_key, end_key),
        }
    }
}

impl ApiV2 {
    pub fn append_ts_on_encoded_bytes(encoded_bytes: &mut Vec<u8>, ts: TimeStamp) {
        debug_assert!(is_valid_encoded_bytes(encoded_bytes, false));
        debug_assert!(is_valid_ts(ts));
        encoded_bytes.encode_u64_desc(ts.into_inner()).unwrap();
    }

    fn get_encode_len(src_len: usize) -> usize {
        MemComparableByteCodec::encoded_len(src_len) + number::U64_SIZE
    }

    pub fn get_rawkv_range() -> (u8, u8) {
        (RAW_KEY_PREFIX, RAW_KEY_PREFIX_END)
    }

    pub fn decode_ts_from(key: &[u8]) -> Result<TimeStamp> {
        let ts = Key::decode_ts_from(key)?;
        Ok(ts)
    }

    pub fn split_ts(key: &[u8]) -> Result<(&[u8], TimeStamp)> {
        Ok(Key::split_on_ts_for(key)?)
    }

    pub const ENCODED_LOGICAL_DELETE: [u8; 1] = [ValueMeta::DELETE_FLAG.bits];
}

// Note: `encoded_bytes` may not be `KeyMode::Raw`.
// E.g., backup service accept an exclusive end key just beyond the scope of raw keys.
// The validity is ensured by client & Storage interfaces.
#[inline]
fn is_valid_encoded_bytes(mut encoded_bytes: &[u8], with_ts: bool) -> bool {
    bytes::decode_bytes(&mut encoded_bytes, false).is_ok()
        && encoded_bytes.len() == number::U64_SIZE * (with_ts as usize)
}

#[inline]
fn is_valid_encoded_key(encoded_key: &Key, with_ts: bool) -> bool {
    is_valid_encoded_bytes(encoded_key.as_encoded(), with_ts)
}

/// TimeStamp::zero is not acceptable, as such entries can not be retrieved by RawKV MVCC.
/// See `RawMvccSnapshot::seek_first_key_value_cf`.
#[inline]
fn is_valid_ts(ts: TimeStamp) -> bool {
    !ts.is_zero()
}

#[inline]
fn decode_raw_key_timestamp(encoded_key: &Key, with_ts: bool) -> Result<Option<TimeStamp>> {
    let ts = if with_ts {
        Some(encoded_key.decode_ts()?)
    } else {
        None
    };
    Ok(ts)
}

#[cfg(test)]
mod tests {
    use txn_types::{Key, TimeStamp};

    use crate::{ApiV2, KvFormat, RawValue};

    #[test]
    fn test_key_decode_err() {
        let cases: Vec<(Vec<u8>, bool)> = vec![
            // Invalid prefix
            (vec![1, 2, 3, 4, 5, 6, 7, 8, 9], false),
            // Memcomparable-encoded padding: n * 9 + Optional 8
            (vec![b'r', 2, 3, 4, 5, 6, 7, 8], false),
            (vec![b'r', 2, 3, 4, 5, 6, 7, 8, 9, 10], false),
            (vec![b'r', 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12], true),
            (
                vec![
                    b'r', 2, 3, 4, 5, 6, 7, 8, 0xff, 1, 2, 3, 4, 0, 0, 0, 0, 0xfb, 0,
                ],
                true,
            ),
            (
                vec![
                    b'r', 2, 3, 4, 5, 6, 7, 8, 0xff, 1, 2, 3, 4, 0, 0, 0, 0, 0xfb, 0, 0, 0, 0, 0,
                    0, 0, 1, 0,
                ],
                true,
            ),
            // Memcomparable-encoded padding pattern: [.., 0, 0, 0, 0, 0xff - padding-len]
            (vec![b'r', 2, 3, 4, 0, 0, 1, 0, 0xfb], false),
            (vec![b'r', 2, 3, 4, 5, 6, 7, 8, 0xf6], false),
        ];

        for (idx, (bytes, with_ts)) in cases.into_iter().enumerate() {
            let res = vec![
                panic_hook::recover_safe(|| {
                    let _ = ApiV2::decode_raw_key(&Key::from_encoded_slice(&bytes), with_ts);
                }),
                panic_hook::recover_safe(|| {
                    let _ = ApiV2::decode_raw_key_owned(Key::from_encoded(bytes), with_ts);
                }),
            ];
            for r in res {
                assert!(r.is_err(), "case {}: {:?}", idx, r);
            }
        }
    }

    #[test]
    fn test_key_encode_err() {
        let cases: Vec<(Vec<u8>, Option<TimeStamp>)> = vec![
            (vec![b'r', 2, 3, 4, 0, 0, 0, 0, 0xfb], Some(0.into())), // ts 0 is invalid.
        ];

        for (idx, (bytes, ts)) in cases.into_iter().enumerate() {
            let res = vec![
                panic_hook::recover_safe(|| {
                    let _ = ApiV2::encode_raw_key(&bytes, ts);
                }),
                panic_hook::recover_safe(|| {
                    let _ = ApiV2::encode_raw_key_owned(bytes, ts);
                }),
            ];
            for r in res {
                assert!(r.is_err(), "case {}: {:?}", idx, r);
            }
        }
    }

    #[test]
    fn test_key_split_ts() {
        let user_key = b"r\0aaaaaaaaaaa";
        let ts = 10;
        let key = Key::from_raw(user_key).append_ts(ts.into()).into_encoded();

        let encoded_key = ApiV2::encode_raw_key(user_key, None);

        let (split_key, split_ts) = ApiV2::split_ts(key.as_slice()).unwrap();

        assert_eq!(encoded_key.into_encoded(), split_key.to_vec());
        assert_eq!(ts, split_ts.into_inner());
    }

    #[test]
    fn test_append_ts_on_encoded_bytes() {
        let cases = vec![
            (true, vec![b'r', 2, 3, 4, 0, 0, 0, 0, 0xfb], 10),
            (
                true,
                vec![
                    b'r', 2, 3, 4, 5, 6, 7, 8, 0xff, 1, 2, 3, 4, 0, 0, 0, 0, 0xfb,
                ],
                20,
            ),
            (false, vec![b'r', 2, 3, 4, 0, 0, 0, 0, 0xfb], 0), // ts 0 is invalid.
            (false, vec![1, 2, 3, 4, 5, 6, 7, 8, 9], 1),
            (false, vec![b'r', 2, 3, 4, 5, 6, 7, 8], 2),
            (false, vec![b'r', 2, 3, 4, 5, 6, 7, 8, 9, 10], 3),
            (false, vec![b'r', 2, 3, 4, 0, 0, 1, 0, 0xfb], 4),
            (false, vec![b'r', 2, 3, 4, 5, 6, 7, 8, 0xf6], 5),
        ];

        for (idx, (is_valid, mut bytes, ts)) in cases.into_iter().enumerate() {
            if is_valid {
                let expected = Key::from_encoded(bytes.clone()).append_ts(ts.into());
                ApiV2::append_ts_on_encoded_bytes(&mut bytes, ts.into());
                assert_eq!(&bytes, expected.as_encoded(), "case {}", idx);
            } else {
                let r = panic_hook::recover_safe(|| {
                    ApiV2::append_ts_on_encoded_bytes(&mut bytes, ts.into());
                });
                assert!(r.is_err(), "case {}: {:?}", idx, r);
            }
        }
    }

    #[test]
    fn test_encoded_logical_delete() {
        {
            let v = RawValue {
                user_value: vec![],
                expire_ts: None,
                is_delete: true,
            };
            let encoded = ApiV2::encode_raw_value_owned(v);
            assert_eq!(encoded, ApiV2::ENCODED_LOGICAL_DELETE);
        }
        {
            let v = ApiV2::decode_raw_value(&ApiV2::ENCODED_LOGICAL_DELETE).unwrap();
            assert!(v.is_delete);
        }
    }

    #[test]
    fn test_decode_ts_from() {
        let test_cases: Vec<(Vec<u8>, TimeStamp)> = vec![
            (b"rkey1".to_vec(), 1.into()),
            (b"rkey2".to_vec(), 2.into()),
            (b"rkey3".to_vec(), 3.into()),
            (b"rkey4".to_vec(), 4.into()),
        ];
        for (idx, (key, ts)) in test_cases.into_iter().enumerate() {
            let key_with_ts = ApiV2::encode_raw_key(&key, Some(ts)).into_encoded();
            let (decoded_key, decoded_ts1) =
                ApiV2::decode_raw_key_owned(Key::from_encoded(key_with_ts.clone()), true).unwrap();
            let decoded_ts2 = ApiV2::decode_ts_from(&key_with_ts).unwrap();

            assert_eq!(key, decoded_key, "case {}", idx);
            assert_eq!(ts, decoded_ts1.unwrap(), "case {}", idx);
            assert_eq!(ts, decoded_ts2, "case {}", idx);
        }
    }
}
