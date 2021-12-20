// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

use super::*;
use encoding_rs::GBK;

#[derive(Debug)]
pub struct EncodingGBK;

impl Encoding for EncodingGBK {
    #[inline]
    fn decode(data: BytesRef) -> Result<Bytes> {
        match GBK.decode_without_bom_handling_and_without_replacement(data) {
            Some(v) => Ok(Bytes::from(v.as_bytes())),
            None => Err(Error::cannot_convert_string("gbk")),
        }
    }

    #[inline]
    fn encode(data: BytesRef) -> Result<Bytes> {
        Ok(Bytes::from(GBK.encode(str::from_utf8(data)?).0))
    }
}
