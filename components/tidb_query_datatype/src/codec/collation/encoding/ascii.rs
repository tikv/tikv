// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

use super::*;

#[derive(Debug)]
pub struct EncodingBinary;

impl Encoding for EncodingBinary {
    #[inline]
    fn decode(data: BytesRef<'_>) -> Result<Bytes> {
        Ok(Bytes::from(data))
    }
}

#[derive(Debug)]
pub struct EncodingAscii;

impl Encoding for EncodingAscii {
    #[inline]
    fn decode(data: BytesRef<'_>) -> Result<Bytes> {
        for x in data {
            if !x.is_ascii() {
                return Err(Error::cannot_convert_string(
                    format_invalid_char(data).as_str(),
                    "ascii",
                ));
            }
        }
        Ok(Bytes::from(data))
    }
}
