// Copyright 2020 TiKV Project Authors. Licensed under Apache-2.0.

use std::str;

use super::*;

pub struct CharsetBinary;

impl Charset for CharsetBinary {
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

    fn charset() -> crate::Charset {
        crate::Charset::Binary
    }
}

pub struct CharsetUtf8mb4;

impl Charset for CharsetUtf8mb4 {
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
        unsafe {
            core::str::next_code_point(&mut it).map(|c| {
                (
                    std::char::from_u32_unchecked(c),
                    it.as_slice().as_ptr().offset_from(start) as usize,
                )
            })
        }
    }

    fn charset() -> crate::Charset {
        crate::Charset::Utf8Mb4
    }
}

// gbk character data actually stored with utf8mb4 character encoding.
pub type CharsetGbk = CharsetUtf8mb4;
