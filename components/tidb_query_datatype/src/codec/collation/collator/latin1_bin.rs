// Copyright 2020 TiKV Project Authors. Licensed under Apache-2.0.

use bstr::{ByteSlice, B};

use super::*;

/// Collator for latin1_bin collation with padding behavior (trims right
/// spaces).
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
    fn sort_compare(mut a: &[u8], mut b: &[u8], force_no_pad: bool) -> Result<Ordering> {
        if !force_no_pad {
            a = a.trim_end_with(|c| c == PADDING_SPACE);
        }
        if !force_no_pad {
            b = b.trim_end_with(|c| c == PADDING_SPACE);
        }
        Ok(a.cmp(b))
    }

    #[inline]
    fn sort_hash<H: Hasher>(state: &mut H, bstr: &[u8]) -> Result<()> {
        B(bstr).trim_end_with(|c| c == PADDING_SPACE).hash(state);
        Ok(())
    }
}
