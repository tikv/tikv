// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

use super::*;

impl APIVersion for APIV1 {
    const TAG: ApiVersion = ApiVersion::V1;
    const IS_TTL_ENABLED: bool = false;

    #[inline]
    fn parse_key_mode(_: &[u8]) -> KeyMode {
        KeyMode::Unknown
    }

    fn parse_range_mode(_: (Option<&[u8]>, Option<&[u8]>)) -> KeyMode {
        KeyMode::Unknown
    }

    #[inline]
    fn decode_raw_value(bytes: &[u8]) -> Result<RawValue<&[u8]>> {
        Ok(RawValue {
            user_value: bytes,
            expire_ts: None,
        })
    }

    #[inline]
    fn encode_raw_value(value: RawValue<&[u8]>) -> Vec<u8> {
        value.user_value.to_vec()
    }

    #[inline]
    fn encode_raw_value_owned(value: RawValue<Vec<u8>>) -> Vec<u8> {
        value.user_value
    }
}
