// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

use super::{Error, Result};
use kvproto::kvrpcpb::ApiVersion;
use tikv_util::codec;
use tikv_util::codec::number::{self, NumberEncoder};

bitflags::bitflags! {
    struct ValueMeta: u8 {
        const EXPIRE_TS = 0b00000001;
    }
}

/// An raw key and it's metadata.
/// TODO: Describe the raw value encode in API V1 and API V2.
#[derive(Debug, Clone, Copy)]
pub struct RawValue<T: AsRef<[u8]>> {
    /// The user value.
    pub user_value: T,
    /// The unix timestamp in seconds indicating the point of time that this key will be deleted.
    pub expire_ts: Option<u64>,
}

impl<'a> RawValue<&'a [u8]> {
    /// Parse from the bytes from storage.
    pub fn from_bytes(bytes: &'a [u8], api_version: ApiVersion, enable_ttl: bool) -> Result<Self> {
        match api_version {
            ApiVersion::V1 if !enable_ttl => Ok(RawValue {
                user_value: bytes,
                expire_ts: None,
            }),
            ApiVersion::V1 => {
                let len = bytes.len();
                if len < number::U64_SIZE {
                    return Err(Error::Codec(codec::Error::ValueLength));
                }
                let mut expire_ts_slice = &bytes[len - number::U64_SIZE..];
                let expire_ts = number::decode_u64(&mut expire_ts_slice)?;
                let expire_ts = if expire_ts == 0 {
                    None
                } else {
                    Some(expire_ts)
                };
                Ok(RawValue {
                    user_value: &bytes[..len - number::U64_SIZE],
                    expire_ts,
                })
            }
            ApiVersion::V2 => {
                let len = bytes.len();
                if len == 0 {
                    return Err(Error::Codec(codec::Error::ValueLength));
                }
                let flags = ValueMeta::from_bits(bytes[len - 1])
                    .ok_or_else(|| Error::Codec(codec::Error::ValueMeta))?;
                let mut meta_size = 1;
                let expire_ts = if flags.contains(ValueMeta::EXPIRE_TS) {
                    let mut expire_ts_slice = &bytes[len - meta_size - number::U64_SIZE..];
                    meta_size += number::U64_SIZE;
                    Some(number::decode_u64(&mut expire_ts_slice)?)
                } else {
                    None
                };
                Ok(RawValue {
                    user_value: &bytes[..len - meta_size],
                    expire_ts,
                })
            }
        }
    }

    /// Encode the raw value and it's metadata into bytes.
    pub fn to_bytes(self, api_version: ApiVersion, enable_ttl: bool) -> Vec<u8> {
        match api_version {
            ApiVersion::V1 if !enable_ttl => self.user_value.to_vec(),
            ApiVersion::V1 => {
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
    pub fn from_owned_bytes(
        mut bytes: Vec<u8>,
        api_version: ApiVersion,
        enable_ttl: bool,
    ) -> Result<Self> {
        let (len, expire_ts) = {
            let raw_value = RawValue::from_bytes(&bytes, api_version, enable_ttl)?;
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
    pub fn to_bytes(mut self, api_version: ApiVersion, enable_ttl: bool) -> Vec<u8> {
        match api_version {
            ApiVersion::V1 if !enable_ttl => self.user_value,
            ApiVersion::V1 => {
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

// TODO: Add test
