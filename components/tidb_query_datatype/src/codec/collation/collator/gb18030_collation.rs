// Copyright 2024 TiKV Project Authors. Licensed under Apache-2.0.

use super::*;

/// Collator for `gb18030_bin`
#[derive(Debug)]
pub struct CollatorGb18030Bin;

impl Collator for CollatorGb18030Bin {
    type Charset = CharsetGb18030;
    type Weight = u32;
    const IS_CASE_INSENSITIVE: bool = false;

    #[inline]
    fn char_weight(ch: char) -> u32 {
        // If the incoming character is not, convert it to '?'. This should not
        // happened.
        let r = ch as usize;
        if r > 0x10FFFF {
            return '?' as u32;
        }

        (&GB18030_BIN_TABLE[r * 4..r * 4 + 4])
            .read_u32_le()
            .unwrap()
    }

    #[inline]
    fn write_sort_key<W: BufferWriter>(writer: &mut W, bstr: &[u8]) -> Result<usize> {
        let mut bstr_rest = trim_end_padding(bstr);
        let mut n = 0;
        while !bstr_rest.is_empty() {
            match next_utf8_char(bstr_rest) {
                Some((ch, b_next)) => {
                    let weight = Self::char_weight(ch);
                    if weight > 0xFFFF {
                        writer.write_u32_be(weight)?;
                        n += 4;
                    } else if weight > 0xFF {
                        writer.write_u16_be(weight as u16)?;
                        n += 2;
                    } else {
                        writer.write_u8(weight as u8)?;
                        n += 1;
                    }
                    bstr_rest = b_next
                }
                None => {
                    writer.write_u8(b'?')?;
                    n += 1;
                    bstr_rest = &bstr_rest[1..]
                }
            }
        }
        Ok(n * std::mem::size_of::<u8>())
    }

    #[inline]
    fn sort_compare(a: &[u8], b: &[u8], force_no_pad: bool) -> Result<Ordering> {
        let sa = if force_no_pad { a } else { trim_end_padding(a) };
        let sb = if force_no_pad { b } else { trim_end_padding(b) };
        let mut a_rest = sa;
        let mut b_rest = sb;

        while !a_rest.is_empty() && !b_rest.is_empty() {
            let (ch_a, a_next) = next_utf8_char(a_rest).unwrap_or(('?', &a_rest[1..]));
            let (ch_b, b_next) = next_utf8_char(b_rest).unwrap_or(('?', &b_rest[1..]));

            let ord = Self::char_weight(ch_a).cmp(&Self::char_weight(ch_b));
            if ord != Ordering::Equal {
                return Ok(ord);
            }

            a_rest = a_next;
            b_rest = b_next;
        }

        Ok(a_rest.len().cmp(&b_rest.len()))
    }

    #[inline]
    fn sort_hash<H: Hasher>(state: &mut H, bstr: &[u8]) -> Result<()> {
        let mut bstr_rest = trim_end_padding(bstr);
        while !bstr_rest.is_empty() {
            match next_utf8_char(bstr_rest) {
                Some((ch_b, b_next)) => {
                    Self::char_weight(ch_b).hash(state);
                    bstr_rest = b_next
                }
                None => {
                    Self::char_weight('?').hash(state);
                    bstr_rest = &bstr_rest[1..];
                }
            }
        }
        Ok(())
    }
}

/// Collator for `gb18030_chinese_ci`
#[derive(Debug)]
pub struct CollatorGb18030ChineseCi;

impl Collator for CollatorGb18030ChineseCi {
    type Charset = CharsetGb18030;
    type Weight = u32;
    const IS_CASE_INSENSITIVE: bool = true;

    #[inline]
    fn char_weight(ch: char) -> u32 {
        // If the incoming character is not, convert it to '?'. This should not
        // happened.
        let r = ch as usize;
        if r > 0x10FFFF {
            return '?' as u32;
        }

        (&GB18030_CHINESE_CI_TABLE[r * 4..r * 4 + 4])
            .read_u32_le()
            .unwrap()
    }

    #[inline]
    fn write_sort_key<W: BufferWriter>(writer: &mut W, bstr: &[u8]) -> Result<usize> {
        let mut bstr_rest = trim_end_padding(bstr);
        let mut n = 0;
        while !bstr_rest.is_empty() {
            match next_utf8_char(bstr_rest) {
                Some((ch, b_next)) => {
                    let weight = Self::char_weight(ch);
                    if weight > 0xFFFF {
                        writer.write_u32_be(weight)?;
                        n += 4;
                    } else if weight > 0xFF {
                        writer.write_u16_be(weight as u16)?;
                        n += 2;
                    } else {
                        writer.write_u8(weight as u8)?;
                        n += 1;
                    }
                    bstr_rest = b_next
                }
                _ => break,
            }
        }
        Ok(n * std::mem::size_of::<u8>())
    }

    #[inline]
    fn sort_compare(a: &[u8], b: &[u8], force_no_pad: bool) -> Result<Ordering> {
        let sa = if force_no_pad { a } else { trim_end_padding(a) };
        let sb = if force_no_pad { b } else { trim_end_padding(b) };
        let mut a_rest = sa;
        let mut b_rest = sb;

        while !a_rest.is_empty() && !b_rest.is_empty() {
            match (next_utf8_char(a_rest), next_utf8_char(b_rest)) {
                (Some((ch_a, a_next)), Some((ch_b, b_next))) => {
                    let ord = Self::char_weight(ch_a).cmp(&Self::char_weight(ch_b));
                    if ord != Ordering::Equal {
                        return Ok(ord);
                    }
                    a_rest = a_next;
                    b_rest = b_next;
                }
                _ => return Ok(Ordering::Equal),
            }
        }

        Ok(a_rest.len().cmp(&b_rest.len()))
    }

    #[inline]
    fn sort_hash<H: Hasher>(state: &mut H, bstr: &[u8]) -> Result<()> {
        let mut bstr_rest = trim_end_padding(bstr);
        while !bstr_rest.is_empty() {
            match next_utf8_char(bstr_rest) {
                Some((ch_b, b_next)) => {
                    Self::char_weight(ch_b).hash(state);
                    bstr_rest = b_next
                }
                _ => break,
            }
        }
        Ok(())
    }
}

const TABLE_SIZE_FOR_GB18030: usize = 4 * (0x10FFFF + 1);

// GB18030_BIN_TABLE are the encoding tables from Unicode to GB18030 code.
const GB18030_BIN_TABLE: &[u8; TABLE_SIZE_FOR_GB18030] = include_bytes!("gb18030_bin.data");

// GB18030_CHINESE_CI_TABLE are the sort key tables for GB18030 codepoint.
const GB18030_CHINESE_CI_TABLE: &[u8; TABLE_SIZE_FOR_GB18030] =
    include_bytes!("gb18030_chinese_ci.data");

#[cfg(test)]
mod tests {
    use crate::codec::collation::{
        Collator,
        collator::{CollatorGb18030Bin, CollatorGb18030ChineseCi},
    };

    #[test]
    fn test_weight() {
        let cases: Vec<(char, u32, u32)> = vec![
            ('中', 0xFFA09BC1, 0xD6D0),
            ('€', 0xA2E3, 0xA2E3),
            ('', 0xFF001D21, 0x8135F437),
            ('ḿ', 0xFF001D20, 0xA8BC),
            ('ǹ', 0xFF000154, 0xA8BF),
            ('䦃', 0xFFA09E8A, 0xFE89),
        ];

        for (case, exp_chinese_ci, exp_bin) in cases {
            let chinese_ci = CollatorGb18030ChineseCi::char_weight(case);
            let bin = CollatorGb18030Bin::char_weight(case);
            assert_eq!(
                exp_bin, bin,
                "{} expected:{:02X?}, but got:{:02X?}",
                case, exp_bin, bin
            );
            assert_eq!(
                exp_chinese_ci, chinese_ci,
                "{} expected:{:02X?}, but got:{:02X?}",
                case, exp_chinese_ci, chinese_ci
            );
        }
    }
}
