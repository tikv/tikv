// Copyright 2020 TiKV Project Authors. Licensed under Apache-2.0.

use std::str;

use super::*;

#[derive(PartialEq)]
pub enum Charset {
    Binary,
    Utf8Mb4,
}

pub struct CharsetBinary;

impl super::Charset for CharsetBinary {
    type Char = u8;

    #[inline]
    fn validate(_: &[u8]) -> Result<()> {
        Ok(())
    }

    #[inline]
    fn decode_one(data: &[u8]) -> Option<(Self::Char, usize)> {
        if data.is_empty() {
            None
        } else {
            Some((data[0], 1))
        }
    }

    fn charset() -> Charset {
        Charset::Binary
    }
}

pub struct CharsetUtf8mb4;

impl super::Charset for CharsetUtf8mb4 {
    type Char = char;

    #[inline]
    fn validate(bstr: &[u8]) -> Result<()> {
        str::from_utf8(bstr)?;
        Ok(())
    }

    #[inline]
    fn decode_one(data: &[u8]) -> Option<(Self::Char, usize)> {
        let mut it = data.iter();
        let start = it.as_slice().as_ptr();
        core::str::next_code_point(&mut it).map(|c| unsafe {
            (
                std::char::from_u32_unchecked(c),
                it.as_slice().as_ptr().offset_from(start) as usize,
            )
        })
    }

    fn charset() -> Charset {
        Charset::Utf8Mb4
    }
}
