// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

use engine_traits::Result;
use tikv_util::codec::number::{self, NumberEncoder};
use tikv_util::codec::Error;

use super::*;

impl APIVersion for APIV1TTL {
    const TAG: ApiVersion = ApiVersion::V1ttl;
    const IS_TTL_ENABLED: bool = true;

    #[inline]
    fn parse_key_mode(_: &[u8]) -> KeyMode {
        // In V1TTL, txnkv is disabled, so all data keys are raw keys.
        KeyMode::Raw
    }

    fn parse_range_mode(_: (Option<&[u8]>, Option<&[u8]>)) -> KeyMode {
        // In V1TTL, txnkv is disabled, so all data keys are raw keys.
        KeyMode::Raw
    }

    fn decode_raw_value(bytes: &[u8]) -> Result<RawValue<&[u8]>> {
        let rest_len = bytes
            .len()
            .checked_sub(number::U64_SIZE)
            .ok_or(Error::ValueLength)?;
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

    fn encode_raw_value(value: RawValue<&[u8]>) -> Vec<u8> {
        let mut buf = Vec::with_capacity(value.user_value.len() + number::U64_SIZE);
        buf.extend_from_slice(value.user_value);
        buf.encode_u64(value.expire_ts.unwrap_or(0)).unwrap();
        buf
    }

    fn encode_raw_value_owned(mut value: RawValue<Vec<u8>>) -> Vec<u8> {
        value.user_value.reserve(number::U64_SIZE);
        value
            .user_value
            .encode_u64(value.expire_ts.unwrap_or(0))
            .unwrap();
        value.user_value
    }
}
