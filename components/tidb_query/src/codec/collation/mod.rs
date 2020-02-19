// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

mod utf8mb4;

pub use self::utf8mb4::*;

use std::cmp::Ordering;
use std::hash::Hasher;

use codec::prelude::*;

use crate::codec::Result;

pub macro match_template_collator($t:tt, $($tail:tt)*) {
    match_template::match_template! {
        $t = [
            Utf8Bin => CollatorUtf8Mb4Bin,
            Utf8Mb4Bin => CollatorUtf8Mb4Bin,
            Utf8GeneralCi => CollatorUtf8Mb4GeneralCi,
            Utf8Mb4GeneralCi => CollatorUtf8Mb4GeneralCi,
            Binary => CollatorBinary,
        ],
        $($tail)*
    }
}

pub trait Charset {
    type Char: Copy + Into<u32>;

    fn decode_one(data: &[u8]) -> Option<(Self::Char, usize)>;
}

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

pub trait Collator {
    type Charset: Charset;

    /// Writes the SortKey of `bstr` into `writer`.
    fn write_sort_key<W: BufferWriter>(bstr: &[u8], writer: &mut W) -> Result<usize>;

    /// Returns the SortKey of `bstr` as an owned byte vector.
    fn sort_key(bstr: &[u8]) -> Result<Vec<u8>> {
        let mut v = Vec::default();
        Self::write_sort_key(bstr, &mut v)?;
        Ok(v)
    }

    fn validate(bstr: &[u8]) -> Result<()>;

    /// Compares `a` and `b` based on their SortKey.
    fn sort_compare(a: &[u8], b: &[u8]) -> Result<Ordering>;

    /// Hashes `bstr` based on its SortKey directly.
    ///
    /// WARN: `sort_hash(str) != hash(sort_key(str))`.
    fn sort_hash<H: Hasher>(bstr: &[u8], state: &mut H) -> Result<()>;
}

pub struct CollatorBinary;

impl Collator for CollatorBinary {
    type Charset = CharsetBinary;

    #[inline]
    fn write_sort_key<W: BufferWriter>(bstr: &[u8], writer: &mut W) -> Result<usize> {
        writer.write_bytes(bstr)?;
        Ok(bstr.len())
    }

    #[inline]
    fn validate(_bstr: &[u8]) -> Result<()> {
        Ok(())
    }

    #[inline]
    fn sort_compare(a: &[u8], b: &[u8]) -> Result<Ordering> {
        Ok(a.cmp(b))
    }

    #[inline]
    fn sort_hash<H: Hasher>(bstr: &[u8], state: &mut H) -> Result<()> {
        use std::hash::Hash;

        bstr.hash(state);
        Ok(())
    }
}
