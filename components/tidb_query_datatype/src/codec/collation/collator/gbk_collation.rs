// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

use super::*;

pub trait GbkCollator: 'static + std::marker::Send + std::marker::Sync + std::fmt::Debug {
    const IS_CASE_INSENSITIVE: bool;
    const WEIGHT_TABLE: &'static [u8; (0xffff + 1) * 2];
}

impl<T: GbkCollator> Collator for T {
    type Charset = CharsetGbk;
    type Weight = u16;

    const IS_CASE_INSENSITIVE: bool = T::IS_CASE_INSENSITIVE;

    #[inline]
    fn char_weight(ch: char) -> Self::Weight {
        // All GBK code point are in BMP, if the incoming character is not, convert it to '?'.
        // This should not happened.
        let r = ch as usize;
        if r > 0xFFFF {
            return '?' as u16;
        }

        (&Self::WEIGHT_TABLE[r * 2..r * 2 + 2]).read_u16().unwrap()
    }

    #[inline]
    fn write_sort_key<W: BufferWriter>(writer: &mut W, bstr: &[u8]) -> Result<usize> {
        let s = str::from_utf8(bstr)?.trim_end_matches(PADDING_SPACE);
        let mut n = 0;
        for ch in s.chars() {
            let weight = Self::char_weight(ch);
            if weight > 0xFF {
                writer.write_u16_be(weight)?;
                n += 2;
            } else {
                writer.write_u8(weight as u8)?;
                n += 1;
            }
        }
        Ok(n * std::mem::size_of::<u8>())
    }

    #[inline]
    fn sort_compare(a: &[u8], b: &[u8]) -> Result<Ordering> {
        let sa = str::from_utf8(a)?.trim_end_matches(PADDING_SPACE);
        let sb = str::from_utf8(b)?.trim_end_matches(PADDING_SPACE);
        Ok(sa
            .chars()
            .map(Self::char_weight)
            .cmp(sb.chars().map(Self::char_weight)))
    }

    #[inline]
    fn sort_hash<H: Hasher>(state: &mut H, bstr: &[u8]) -> Result<()> {
        let s = str::from_utf8(bstr)?.trim_end_matches(PADDING_SPACE);
        for ch in s.chars().map(Self::char_weight) {
            ch.hash(state);
        }
        Ok(())
    }
}

/// Collator for `gbk_bin` collation with padding behavior (trims right spaces).
#[derive(Debug)]
pub struct CollatorGbkBin;

impl GbkCollator for CollatorGbkBin {
    const IS_CASE_INSENSITIVE: bool = false;
    const WEIGHT_TABLE: &'static [u8; (0xffff + 1) * 2] = GBK_BIN_TABLE;
}

/// Collator for `gbk_chinese_ci` collation with padding behavior (trims right spaces).
#[derive(Debug)]
pub struct CollatorGbkChineseCi;

impl GbkCollator for CollatorGbkChineseCi {
    const IS_CASE_INSENSITIVE: bool = true;
    const WEIGHT_TABLE: &'static [u8; (0xffff + 1) * 2] = GBK_CHINESE_CI_TABLE;
}

// GBK_BIN_TABLE are the encoding tables from Unicode to GBK code, it is totally the same with golang's GBK encoding.
// If there is no mapping code in GBK, use 0x3F(?) instead. It should not happened.
const GBK_BIN_TABLE: &[u8; (0xffff + 1) * 2] = include_bytes!("gbk_bin.data");

// GBK_CHINESE_CI_TABLE are the sort key tables for GBK codepoint.
// If there is no mapping code in GBK, use 0x3F(?) instead. It should not happened.
const GBK_CHINESE_CI_TABLE: &[u8; (0xffff + 1) * 2] = include_bytes!("gbk_chinese_ci.data");
