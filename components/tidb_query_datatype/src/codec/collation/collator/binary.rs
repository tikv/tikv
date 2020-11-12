// Copyright 2020 TiKV Project Authors. Licensed under Apache-2.0.

use super::*;

/// Collator for binary collation without padding.
#[derive(Debug)]
pub struct CollatorBinary;

impl Collator for CollatorBinary {
    type Charset = CharsetBinary;

    #[inline]
    fn validate(_bstr: &[u8]) -> Result<()> {
        Ok(())
    }

    #[inline]
    fn write_sort_key<W: BufferWriter>(writer: &mut W, bstr: &[u8]) -> Result<usize> {
        writer.write_bytes(bstr)?;
        Ok(bstr.len())
    }

    #[inline]
    fn sort_compare(a: &[u8], b: &[u8]) -> Result<Ordering> {
        Ok(a.cmp(b))
    }

    #[inline]
    fn sort_hash<H: Hasher>(state: &mut H, bstr: &[u8]) -> Result<()> {
        bstr.hash(state);
        Ok(())
    }
}
