// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

use super::*;

#[derive(Debug)]
pub struct EncodingLatin1;

impl Encoding for EncodingLatin1 {
    #[inline]
    fn decode(data: BytesRef) -> Result<Bytes> {
        match str::from_utf8(data) {
            Ok(v) => Ok(Bytes::from(v)),
            Err(_) => Err(Error::cannot_convert_string("latin1")),
        }
    }

    #[inline]
    fn encode(data: BytesRef) -> Result<Bytes> {
        Ok(Bytes::from(data))
    }
}
