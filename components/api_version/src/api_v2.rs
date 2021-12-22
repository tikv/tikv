// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

use engine_traits::Result;
use tikv_util::codec::number::{self, NumberEncoder};
use tikv_util::codec::Error;

use super::*;

pub const RAW_KEY_PREFIX: u8 = b'r';
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
        const EXPIRE_TS = 0b00000001;
    }
}

impl APIVersion for APIV2 {
    const TAG: ApiVersion = ApiVersion::V2;
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
        })
    }

    fn encode_raw_value(value: RawValue<&[u8]>) -> Vec<u8> {
        let mut flags = ValueMeta::empty();
        let mut meta_size = 1;
        if value.expire_ts.is_some() {
            flags.insert(ValueMeta::EXPIRE_TS);
            meta_size += number::U64_SIZE;
        }
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
        value.user_value.reserve(meta_size);
        if let Some(expire_ts) = value.expire_ts {
            value.user_value.encode_u64(expire_ts).unwrap();
        }
        value.user_value.push(flags.bits());
        value.user_value
    }
}
