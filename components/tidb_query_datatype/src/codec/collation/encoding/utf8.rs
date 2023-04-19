// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

use super::*;

pub trait Utf8CompatibleEncoding {
    const NAME: &'static str;
}

impl<T: Utf8CompatibleEncoding> Encoding for T {
    #[inline]
    fn decode(data: BytesRef<'_>) -> Result<Bytes> {
        match str::from_utf8(data) {
            Ok(v) => Ok(Bytes::from(v)),
            Err(_) => Err(Error::cannot_convert_string(
                format_invalid_char(data).as_str(),
                T::NAME,
            )),
        }
    }
}

#[derive(Debug)]
pub struct EncodingUtf8Mb4;

impl Utf8CompatibleEncoding for EncodingUtf8Mb4 {
    const NAME: &'static str = "utf8mb4";
}

#[derive(Debug)]
pub struct EncodingUtf8;

impl Utf8CompatibleEncoding for EncodingUtf8 {
    const NAME: &'static str = "utf8";
}

#[derive(Debug)]
pub struct EncodingLatin1;

impl Utf8CompatibleEncoding for EncodingLatin1 {
    const NAME: &'static str = "latin1";
}
