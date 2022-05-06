// Copyright 2020 TiKV Project Authors. Licensed under Apache-2.0.

use bstr::{ByteSlice, B};

use super::*;

/// Collator for latin1_bin collation with padding behavior (trims right spaces).
#[derive(Debug)]
pub struct CollatorLatin1Bin;

impl Collator for CollatorLatin1Bin {
    type Charset = CharsetBinary;
    type Weight = u8;

    const IS_CASE_INSENSITIVE: bool = false;

    #[inline]
    fn char_weight(ch: u8) -> Self::Weight {
        ch
    }

    #[inline]
    fn write_sort_key<W: BufferWriter>(writer: &mut W, bstr: &[u8]) -> Result<usize> {
        let s = B(bstr).trim_end_with(|c| c == PADDING_SPACE);
        writer.write_bytes(s)?;
        Ok(s.len())
    }

    #[inline]
    fn sort_compare(a: &[u8], b: &[u8]) -> Result<Ordering> {
        Ok(B(a)
            .trim_end_with(|c| c == PADDING_SPACE)
            .cmp(B(b).trim_end_with(|c| c == PADDING_SPACE)))
    }

    #[inline]
    fn sort_hash<H: Hasher>(state: &mut H, bstr: &[u8]) -> Result<()> {
        B(bstr).trim_end_with(|c| c == PADDING_SPACE).hash(state);
        Ok(())
    }
}
