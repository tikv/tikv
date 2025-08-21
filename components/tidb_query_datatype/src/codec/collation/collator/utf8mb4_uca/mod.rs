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
    fn preprocess(s: &[u8]) -> &[u8];

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
        let mut bstr_rest = T::preprocess(bstr);

        let mut n = 0;

        while !bstr_rest.is_empty() {
            match next_utf8_char(bstr_rest) {
                Some((ch_b, b_next)) => {
                    let mut weight = Self::char_weight(ch_b);
                    while weight != 0 {
                        writer.write_u16_be((weight & 0xFFFF) as u16)?;
                        n += 1;
                        weight >>= 16
                    }
                    bstr_rest = b_next
                }
                _ => break,
            }
        }
        Ok(n * std::mem::size_of::<u16>())
    }

    #[inline]
    fn sort_compare(a: &[u8], b: &[u8], force_no_pad: bool) -> Result<Ordering> {
        let mut sa = if force_no_pad { a } else { T::preprocess(a) };
        let mut sb = if force_no_pad { b } else { T::preprocess(b) };

        let mut an = 0;
        let mut bn = 0;

        loop {
            while an == 0 && !sa.is_empty() {
                match next_utf8_char(sa) {
                    Some((ch_a, a_next)) => {
                        an = Self::char_weight(ch_a);
                        sa = a_next;
                    }
                    _ => return Ok(Ordering::Equal),
                }
            }

            while bn == 0 && !sb.is_empty() {
                match next_utf8_char(sb) {
                    Some((ch_b, b_next)) => {
                        bn = Self::char_weight(ch_b);
                        sb = b_next;
                    }
                    _ => return Ok(Ordering::Equal),
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
                if (an ^ bn) & 0xFFFF != 0 {
                    return Ok((an & 0xFFFF).cmp(&(bn & 0xFFFF)));
                }
                an >>= 16;
                bn >>= 16;
            }
        }
    }

    #[inline]
    fn sort_hash<H: Hasher>(state: &mut H, bstr: &[u8]) -> Result<()> {
        let mut bstr_rest = T::preprocess(bstr);
        while !bstr_rest.is_empty() {
            match next_utf8_char(bstr_rest) {
                Some((ch_b, b_next)) => {
                    let mut weight = Self::char_weight(ch_b);
                    while weight != 0 {
                        (weight & 0xFFFF).hash(state);
                        weight >>= 16;
                    }
                    bstr_rest = b_next
                }
                _ => break,
            }
        }
        Ok(())
    }
}
