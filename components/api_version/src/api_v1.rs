// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

use super::*;

impl APIVersion for APIV1 {
    const TAG: ApiVersion = ApiVersion::V1;
    #[cfg(any(test, feature = "testexport"))]
    const CLIENT_TAG: ApiVersion = ApiVersion::V1;
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
            is_delete: false,
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

    fn convert_raw_key_from(
        src_api: ApiVersion,
        key: &[u8],
        _ts: Option<TimeStamp>,
    ) -> Result<Key> {
        match src_api {
            ApiVersion::V1 | ApiVersion::V1ttl => Ok(Key::from_encoded_slice(key)),
            ApiVersion::V2 => {
                assert_eq!(APIV2::parse_key_mode(key), KeyMode::Raw);
                let (mut user_key, _) = APIV2::decode_raw_key(&Key::from_encoded_slice(key), true)?;
                user_key.remove(0); // remove first byte `RAW_KEY_PREFIX`
                Ok(Self::encode_raw_key_owned(user_key, None))
            }
        }
    }
}
