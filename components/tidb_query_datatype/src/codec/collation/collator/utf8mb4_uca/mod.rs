// Copyright 2020 TiKV Project Authors. Licensed under Apache-2.0.

mod data_0400;
mod data_0900;

use std::{fmt::Debug, marker::PhantomData};

use super::*;

/// Collator for `utf8mb4_unicode_ci` collation with padding behavior (trims
/// right spaces).
pub type CollatorUtf8Mb4UnicodeCi = CollatorUca<data_0400::Unicode0400>;

/// Collator for `utf8mb4_0900_ai_ci` collation without padding
pub type CollatorUtf8Mb40900AiCi = CollatorUca<data_0900::Unicode0900>;

pub trait UnicodeVersion: 'static + Send + Sync + Debug {
    fn preprocess(s: &str) -> &str;

    fn char_weight(ch: char) -> u128;
}

#[derive(Debug)]
pub struct CollatorUca<T: UnicodeVersion> {
    _impl: PhantomData<T>,
}

impl<T: UnicodeVersion> Collator for CollatorUca<T> {
    type Charset = CharsetUtf8mb4;
    type Weight = u128;

    const IS_CASE_INSENSITIVE: bool = true;

    #[inline]
    fn char_weight(ch: char) -> Self::Weight {
        T::char_weight(ch)
    }

    #[inline]
    fn write_sort_key<W: BufferWriter>(writer: &mut W, bstr: &[u8]) -> Result<usize> {
        let s = T::preprocess(str::from_utf8(bstr)?);

        let mut n = 0;
        for ch in s.chars() {
            let mut weight = Self::char_weight(ch);
            while weight != 0 {
                writer.write_u16_be((weight & 0xFFFF) as u16)?;
                n += 1;
                weight >>= 16
            }
        }
        Ok(n * std::mem::size_of::<u16>())
    }

    #[inline]
    fn sort_compare(a: &[u8], b: &[u8], force_no_pad: bool) -> Result<Ordering> {
        let mut sa = str::from_utf8(a)?;
        let mut sb = str::from_utf8(b)?;
        if !force_no_pad {
            sa = T::preprocess(sa);
            sb = T::preprocess(sb);
        }

        let mut ca = sa.chars();
        let mut cb = sb.chars();
        let mut an = 0;
        let mut bn = 0;

        loop {
            if an == 0 {
                for ach in &mut ca {
                    an = Self::char_weight(ach);
                    if an != 0 {
                        break;
                    }
                }
            }

            if bn == 0 {
                for bch in &mut cb {
                    bn = Self::char_weight(bch);
                    if bn != 0 {
                        break;
                    }
                }
            }

            if an == 0 || bn == 0 {
                return Ok(an.cmp(&bn));
            }

            if an == bn {
                an = 0;
                bn = 0;
                continue;
            }

            while an != 0 && bn != 0 {
                if (an ^ bn) & 0xFFFF == 0 {
                    an >>= 16;
                    bn >>= 16;
                } else {
                    return Ok((an & 0xFFFF).cmp(&(bn & 0xFFFF)));
                }
            }
        }
    }

    #[inline]
    fn sort_hash<H: Hasher>(state: &mut H, bstr: &[u8]) -> Result<()> {
        let s = T::preprocess(str::from_utf8(bstr)?);
        for ch in s.chars() {
            let mut weight = Self::char_weight(ch);
            while weight != 0 {
                (weight & 0xFFFF).hash(state);
                weight >>= 16;
            }
        }
        Ok(())
    }
}
