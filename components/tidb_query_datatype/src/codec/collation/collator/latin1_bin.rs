// Copyright 2020 TiKV Project Authors. Licensed under Apache-2.0.

use super::*;

/// Collator for latin1_bin collation with padding behavior (trims right spaces).
#[derive(Debug)]
pub struct CollatorLatin1Bin;

impl Collator for CollatorLatin1Bin {
    type Charset = CharsetBinary;

    #[inline]
    fn validate(_bstr: &[u8]) -> Result<()> {
        Ok(())
    }

    #[inline]
    fn write_sort_key<W: BufferWriter>(writer: &mut W, bstr: &[u8]) -> Result<usize> {
        let str = String::from_utf8_lossy(bstr);
        let s = str.trim_end_matches(TRIM_PADDING_SPACE);
        writer.write_bytes(s.as_bytes())?;
        Ok(s.len())
    }

    #[inline]
    fn sort_compare(a: &[u8], b: &[u8]) -> Result<Ordering> {
        let str_a = String::from_utf8_lossy(a);
        let str_b = String::from_utf8_lossy(b);
        let sa = str_a.trim_end_matches(TRIM_PADDING_SPACE);
        let sb = str_b.trim_end_matches(TRIM_PADDING_SPACE);
        Ok(sa.as_bytes().cmp(sb.as_bytes()))
    }

    #[inline]
    fn sort_hash<H: Hasher>(state: &mut H, bstr: &[u8]) -> Result<()> {
        let str = String::from_utf8_lossy(bstr);
        let s = str.trim_end_matches(TRIM_PADDING_SPACE);
        s.hash(state);
        Ok(())
    }
}
