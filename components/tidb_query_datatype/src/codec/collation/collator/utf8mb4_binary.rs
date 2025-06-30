// Copyright 2020 TiKV Project Authors. Licensed under Apache-2.0.

use super::*;

/// Collator for utf8mb4_bin collation with padding behavior (trims right
/// spaces).
#[derive(Debug)]
pub struct CollatorUtf8Mb4Bin;

impl Collator for CollatorUtf8Mb4Bin {
    type Charset = CharsetUtf8mb4;
    type Weight = u32;

    const IS_CASE_INSENSITIVE: bool = false;

    #[inline]
    fn char_weight(ch: char) -> Self::Weight {
        ch as u32
    }

    #[inline]
    fn write_sort_key<W: BufferWriter>(writer: &mut W, bstr: &[u8]) -> Result<usize> {
        let bstr = trim_end_padding(bstr);
        writer.write_bytes(bstr)?;
        Ok(bstr.len())
    }

    #[inline]
    fn sort_compare(a: &[u8], b: &[u8], force_no_pad: bool) -> Result<Ordering> {
        let a = if force_no_pad { a } else { trim_end_padding(a) };
        let b = if force_no_pad { b } else { trim_end_padding(b) };
        Ok(a.cmp(b))
    }

    #[inline]
    fn sort_hash<H: Hasher>(state: &mut H, bstr: &[u8]) -> Result<()> {
        let bstr = trim_end_padding(bstr);
        bstr.hash(state);
        Ok(())
    }
}

/// Collator for utf8mb4_bin collation without padding.
#[derive(Debug)]
pub struct CollatorUtf8Mb4BinNoPadding;

impl Collator for CollatorUtf8Mb4BinNoPadding {
    type Charset = CharsetUtf8mb4;
    type Weight = u32;

    const IS_CASE_INSENSITIVE: bool = false;

    #[inline]
    fn char_weight(ch: char) -> Self::Weight {
        ch as u32
    }

    #[inline]
    fn write_sort_key<W: BufferWriter>(writer: &mut W, bstr: &[u8]) -> Result<usize> {
        writer.write_bytes(bstr)?;
        Ok(bstr.len())
    }

    #[inline]
    fn sort_compare(a: &[u8], b: &[u8], _force_no_pad: bool) -> Result<Ordering> {
        Ok(a.cmp(b))
    }

    #[inline]
    fn sort_hash<H: Hasher>(state: &mut H, bstr: &[u8]) -> Result<()> {
        bstr.hash(state);
        Ok(())
    }
}
