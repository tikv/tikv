// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

use tikv_util::box_err;

use super::*;

impl KvFormat for ApiV1 {
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

    fn convert_raw_encoded_key_version_from(
        src_api: ApiVersion,
        key: &[u8],
        _ts: Option<TimeStamp>,
    ) -> Result<Key> {
        match src_api {
            ApiVersion::V1 | ApiVersion::V1ttl => Ok(Key::from_encoded_slice(key)),
            ApiVersion::V2 => Err(box_err!("unsupported conversion from v2 to v1")), // reject apiv2 -> apiv1 conversion
        }
    }

    fn convert_raw_user_key_range_version_from(
        src_api: ApiVersion,
        start_key: Vec<u8>,
        end_key: Vec<u8>,
    ) -> Result<(Vec<u8>, Vec<u8>)> {
        match src_api {
            ApiVersion::V1 | ApiVersion::V1ttl => Ok((start_key, end_key)),
            ApiVersion::V2 => Err(box_err!("unsupported conversion from v2 to v1")), // reject apiv2 -> apiv1 conversion
        }
    }
}
