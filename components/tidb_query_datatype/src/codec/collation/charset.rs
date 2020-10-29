// Copyright 2020 TiKV Project Authors. Licensed under Apache-2.0.
use super::Charset;

pub struct CharsetBinary;

impl Charset for CharsetBinary {
    type Char = u8;

    #[inline]
    fn decode_one(data: &[u8]) -> Option<(Self::Char, usize)> {
        if data.is_empty() {
            None
        } else {
            Some((data[0], 1))
        }
    }
}

pub struct CharsetUtf8mb4;

impl Charset for CharsetUtf8mb4 {
    type Char = char;

    #[inline]
    fn decode_one(data: &[u8]) -> Option<(Self::Char, usize)> {
        let mut it = data.iter();
        let start = it.as_slice().as_ptr();
        if let Some(c) = core::str::next_code_point(&mut it) {
            unsafe {
                Some((
                    std::char::from_u32_unchecked(c),
                    it.as_slice().as_ptr().offset_from(start) as usize,
                ))
            }
        } else {
            None
        }
    }
}
