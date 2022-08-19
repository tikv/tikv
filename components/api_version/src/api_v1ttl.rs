// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

use engine_traits::Result;
use tikv_util::{
    box_err,
    codec::{
        number::{self, NumberEncoder},
        Error,
    },
};

use super::*;

impl KvFormat for ApiV1Ttl {
    const TAG: ApiVersion = ApiVersion::V1ttl;
    #[cfg(any(test, feature = "testexport"))]
    const CLIENT_TAG: ApiVersion = ApiVersion::V1;
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
            is_delete: false,
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

    fn convert_raw_encoded_key_version_from(
        src_api: ApiVersion,
        key: &[u8],
        _ts: Option<TimeStamp>,
    ) -> Result<Key> {
        match src_api {
            ApiVersion::V1 | ApiVersion::V1ttl => Ok(Key::from_encoded_slice(key)),
            ApiVersion::V2 => Err(box_err!("unsupported conversion from v2 to v1ttl")), // reject apiv2 -> apiv1ttl conversion
        }
    }

    fn convert_raw_user_key_range_version_from(
        src_api: ApiVersion,
        start_key: Vec<u8>,
        end_key: Vec<u8>,
    ) -> Result<(Vec<u8>, Vec<u8>)> {
        match src_api {
            ApiVersion::V1 | ApiVersion::V1ttl => Ok((start_key, end_key)),
            ApiVersion::V2 => Err(box_err!("unsupported conversion from v2 to v1ttl")), // reject apiv2 -> apiv1ttl conversion
        }
    }
}
