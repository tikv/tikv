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
        let s = str::from_utf8(bstr)?.trim_end_matches(PADDING_SPACE);
        writer.write_bytes(s.as_bytes())?;
        Ok(s.len())
    }

    #[inline]
    fn sort_compare(a: &[u8], b: &[u8]) -> Result<Ordering> {
        let sa = str::from_utf8(a)?.trim_end_matches(PADDING_SPACE);
        let sb = str::from_utf8(b)?.trim_end_matches(PADDING_SPACE);
        Ok(sa.as_bytes().cmp(sb.as_bytes()))
    }

    #[inline]
    fn sort_hash<H: Hasher>(state: &mut H, bstr: &[u8]) -> Result<()> {
        let s = str::from_utf8(bstr)?.trim_end_matches(PADDING_SPACE);
        s.hash(state);
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
        str::from_utf8(bstr)?;
        writer.write_bytes(bstr)?;
        Ok(bstr.len())
    }

    #[inline]
    fn sort_compare(a: &[u8], b: &[u8]) -> Result<Ordering> {
        str::from_utf8(a)?;
        str::from_utf8(b)?;
        Ok(a.cmp(b))
    }

    #[inline]
    fn sort_hash<H: Hasher>(state: &mut H, bstr: &[u8]) -> Result<()> {
        str::from_utf8(bstr)?;
        bstr.hash(state);
        Ok(())
    }
}
