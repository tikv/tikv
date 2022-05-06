// Copyright 2020 TiKV Project Authors. Licensed under Apache-2.0.

mod binary;
mod gbk_collation;
mod latin1_bin;
mod utf8mb4_binary;
mod utf8mb4_general_ci;
mod utf8mb4_unicode_ci;

use std::{
    cmp::Ordering,
    hash::{Hash, Hasher},
    str,
};

pub use binary::*;
use codec::prelude::*;
pub use gbk_collation::*;
pub use latin1_bin::*;
pub use utf8mb4_binary::*;
pub use utf8mb4_general_ci::*;
pub use utf8mb4_unicode_ci::*;

use super::{charset::*, Collator};
use crate::codec::Result;

pub const PADDING_SPACE: char = 0x20 as char;

#[cfg(test)]
mod tests {
    use crate::{codec::collation::Collator, match_template_collator, Collation};

    #[test]
    #[allow(clippy::string_lit_as_bytes)]
    fn test_compare() {
        use std::{cmp::Ordering, collections::hash_map::DefaultHasher};

        let collations = [
            (Collation::Utf8Mb4Bin, 0),
            (Collation::Utf8Mb4BinNoPadding, 1),
            (Collation::Utf8Mb4GeneralCi, 2),
            (Collation::Utf8Mb4UnicodeCi, 3),
            (Collation::Latin1Bin, 4),
            (Collation::GbkBin, 5),
            (Collation::GbkChineseCi, 6),
        ];
        let cases = vec![
            // (sa, sb, [Utf8Mb4Bin, Utf8Mb4BinNoPadding, Utf8Mb4GeneralCi, Utf8Mb4UnicodeCi, Latin1, GBKBin, GbkChineseCi])
            (
                "a".as_bytes(),
                "a".as_bytes(),
                [
                    Ordering::Equal,
                    Ordering::Equal,
                    Ordering::Equal,
                    Ordering::Equal,
                    Ordering::Equal,
                    Ordering::Equal,
                    Ordering::Equal,
                ],
            ),
            (
                "a".as_bytes(),
                "a ".as_bytes(),
                [
                    Ordering::Equal,
                    Ordering::Less,
                    Ordering::Equal,
                    Ordering::Equal,
                    Ordering::Equal,
                    Ordering::Equal,
                    Ordering::Equal,
                ],
            ),
            (
                "a".as_bytes(),
                "A ".as_bytes(),
                [
                    Ordering::Greater,
                    Ordering::Greater,
                    Ordering::Equal,
                    Ordering::Equal,
                    Ordering::Greater,
                    Ordering::Greater,
                    Ordering::Equal,
                ],
            ),
            (
                "aa ".as_bytes(),
                "a a".as_bytes(),
                [
                    Ordering::Greater,
                    Ordering::Greater,
                    Ordering::Greater,
                    Ordering::Greater,
                    Ordering::Greater,
                    Ordering::Greater,
                    Ordering::Greater,
                ],
            ),
            (
                "A".as_bytes(),
                "a\t".as_bytes(),
                [
                    Ordering::Less,
                    Ordering::Less,
                    Ordering::Less,
                    Ordering::Less,
                    Ordering::Less,
                    Ordering::Less,
                    Ordering::Less,
                ],
            ),
            (
                "cAfe".as_bytes(),
                "café".as_bytes(),
                [
                    Ordering::Less,
                    Ordering::Less,
                    Ordering::Equal,
                    Ordering::Equal,
                    Ordering::Less,
                    Ordering::Less,
                    Ordering::Less,
                ],
            ),
            (
                "cAfe ".as_bytes(),
                "café".as_bytes(),
                [
                    Ordering::Less,
                    Ordering::Less,
                    Ordering::Equal,
                    Ordering::Equal,
                    Ordering::Less,
                    Ordering::Less,
                    Ordering::Less,
                ],
            ),
            (
                "ß".as_bytes(),
                "ss".as_bytes(),
                [
                    Ordering::Greater,
                    Ordering::Greater,
                    Ordering::Less,
                    Ordering::Equal,
                    Ordering::Greater,
                    Ordering::Less,
                    Ordering::Less,
                ],
            ),
            (
                "中文".as_bytes(),
                "汉字".as_bytes(),
                [
                    Ordering::Less,
                    Ordering::Less,
                    Ordering::Less,
                    Ordering::Less,
                    Ordering::Less,
                    Ordering::Greater,
                    Ordering::Greater,
                ],
            ),
            (
                "啊".as_bytes(),
                "把".as_bytes(),
                [
                    Ordering::Less,
                    Ordering::Less,
                    Ordering::Less,
                    Ordering::Less,
                    Ordering::Less,
                    Ordering::Less,
                    Ordering::Less,
                ],
            ),
        ];

        for (sa, sb, expected) in cases {
            for (collation, order_in_expected) in &collations {
                let (cmp, ha, hb) = match_template_collator! {
                    TT, match collation {
                        Collation::TT => {
                            let eval_hash = |s| {
                                let mut hasher = DefaultHasher::default();
                                TT::sort_hash(&mut hasher, s).unwrap();
                                hasher.finish()
                            };

                            let cmp = TT::sort_compare(sa, sb).unwrap();
                            let ha = eval_hash(sa);
                            let hb = eval_hash(sb);
                            (cmp, ha, hb)
                        }
                    }
                };

                assert_eq!(
                    cmp, expected[*order_in_expected],
                    "when comparing {:?} and {:?} by {:?}",
                    sa, sb, collation
                );

                if expected[*order_in_expected] == Ordering::Equal {
                    assert_eq!(
                        ha, hb,
                        "when comparing the hash of {:?} and {:?} by {:?}, which should be equal",
                        sa, sb, collation
                    );
                } else {
                    assert_ne!(
                        ha, hb,
                        "when comparing the hash of {:?} and {:?} by {:?}, which should not be equal",
                        sa, sb, collation
                    );
                }
            }
        }
    }

    #[test]
    fn test_utf8mb4_sort_key() {
        let collations = [
            (Collation::Utf8Mb4Bin, 0),
            (Collation::Utf8Mb4BinNoPadding, 1),
            (Collation::Utf8Mb4GeneralCi, 2),
            (Collation::Utf8Mb4UnicodeCi, 3),
            (Collation::Latin1Bin, 4),
            (Collation::GbkBin, 5),
            (Collation::GbkChineseCi, 6),
        ];
        let cases = vec![
            // (str, [Utf8Mb4Bin, Utf8Mb4BinNoPadding, Utf8Mb4GeneralCi, Utf8Mb4UnicodeCi, Latin1, GBKBin, GbkChineseCi])
            (
                "a",
                [
                    vec![0x61],
                    vec![0x61],
                    vec![0x00, 0x41],
                    vec![0x0E, 0x33],
                    vec![0x61],
                    vec![0x61],
                    vec![0x41],
                ],
            ),
            (
                "A ",
                [
                    vec![0x41],
                    vec![0x41, 0x20],
                    vec![0x00, 0x41],
                    vec![0x0E, 0x33],
                    vec![0x41],
                    vec![0x41],
                    vec![0x41],
                ],
            ),
            (
                "A",
                [
                    vec![0x41],
                    vec![0x41],
                    vec![0x00, 0x41],
                    vec![0x0E, 0x33],
                    vec![0x41],
                    vec![0x41],
                    vec![0x41],
                ],
            ),
            (
                "😃",
                [
                    vec![0xF0, 0x9F, 0x98, 0x83],
                    vec![0xF0, 0x9F, 0x98, 0x83],
                    vec![0xff, 0xfd],
                    vec![0xff, 0xfd],
                    vec![0xF0, 0x9F, 0x98, 0x83],
                    vec![0x3F],
                    vec![0x3F],
                ],
            ),
            (
                "Foo © bar 𝌆 baz ☃ qux",
                [
                    vec![
                        0x46, 0x6F, 0x6F, 0x20, 0xC2, 0xA9, 0x20, 0x62, 0x61, 0x72, 0x20, 0xF0,
                        0x9D, 0x8C, 0x86, 0x20, 0x62, 0x61, 0x7A, 0x20, 0xE2, 0x98, 0x83, 0x20,
                        0x71, 0x75, 0x78,
                    ],
                    vec![
                        0x46, 0x6F, 0x6F, 0x20, 0xC2, 0xA9, 0x20, 0x62, 0x61, 0x72, 0x20, 0xF0,
                        0x9D, 0x8C, 0x86, 0x20, 0x62, 0x61, 0x7A, 0x20, 0xE2, 0x98, 0x83, 0x20,
                        0x71, 0x75, 0x78,
                    ],
                    vec![
                        0x00, 0x46, 0x00, 0x4f, 0x00, 0x4f, 0x00, 0x20, 0x00, 0xa9, 0x00, 0x20,
                        0x00, 0x42, 0x00, 0x41, 0x00, 0x52, 0x00, 0x20, 0xff, 0xfd, 0x00, 0x20,
                        0x00, 0x42, 0x00, 0x41, 0x00, 0x5a, 0x00, 0x20, 0x26, 0x3, 0x00, 0x20,
                        0x00, 0x51, 0x00, 0x55, 0x00, 0x58,
                    ],
                    vec![
                        0x0E, 0xB9, 0x0F, 0x82, 0x0F, 0x82, 0x02, 0x09, 0x02, 0xC5, 0x02, 0x09,
                        0x0E, 0x4A, 0x0E, 0x33, 0x0F, 0xC0, 0x02, 0x09, 0xFF, 0xFD, 0x02, 0x09,
                        0x0E, 0x4A, 0x0E, 0x33, 0x10, 0x6A, 0x02, 0x09, 0x06, 0xFF, 0x02, 0x09,
                        0x0F, 0xB4, 0x10, 0x1F, 0x10, 0x5A,
                    ],
                    vec![
                        0x46, 0x6F, 0x6F, 0x20, 0xC2, 0xA9, 0x20, 0x62, 0x61, 0x72, 0x20, 0xF0,
                        0x9D, 0x8C, 0x86, 0x20, 0x62, 0x61, 0x7A, 0x20, 0xE2, 0x98, 0x83, 0x20,
                        0x71, 0x75, 0x78,
                    ],
                    vec![
                        0x46, 0x6f, 0x6f, 0x20, 0x3f, 0x20, 0x62, 0x61, 0x72, 0x20, 0x3f, 0x20,
                        0x62, 0x61, 0x7a, 0x20, 0x3f, 0x20, 0x71, 0x75, 0x78,
                    ],
                    vec![
                        0x46, 0x4f, 0x4f, 0x20, 0x3f, 0x20, 0x42, 0x41, 0x52, 0x20, 0x3f, 0x20,
                        0x42, 0x41, 0x5a, 0x20, 0x3f, 0x20, 0x51, 0x55, 0x58,
                    ],
                ],
            ),
            (
                "ﷻ",
                [
                    vec![0xEF, 0xB7, 0xBB],
                    vec![0xEF, 0xB7, 0xBB],
                    vec![0xFD, 0xFB],
                    vec![
                        0x13, 0x5E, 0x13, 0xAB, 0x02, 0x09, 0x13, 0x5E, 0x13, 0xAB, 0x13, 0x50,
                        0x13, 0xAB, 0x13, 0xB7,
                    ],
                    vec![0xEF, 0xB7, 0xBB],
                    vec![0x3f],
                    vec![0x3f],
                ],
            ),
            (
                "中文",
                [
                    vec![0xE4, 0xB8, 0xAD, 0xE6, 0x96, 0x87],
                    vec![0xE4, 0xB8, 0xAD, 0xE6, 0x96, 0x87],
                    vec![0x4E, 0x2D, 0x65, 0x87],
                    vec![0xFB, 0x40, 0xCE, 0x2D, 0xFB, 0x40, 0xE5, 0x87],
                    vec![0xE4, 0xB8, 0xAD, 0xE6, 0x96, 0x87],
                    vec![0xD6, 0xD0, 0xCE, 0xC4],
                    vec![0xD3, 0x21, 0xC1, 0xAD],
                ],
            ),
        ];
        for (s, expected) in cases {
            for (collation, order_in_expected) in &collations {
                let code = match_template_collator! {
                    TT, match collation {
                        Collation::TT => TT::sort_key(s.as_bytes()).unwrap()
                    }
                };
                assert_eq!(
                    code, expected[*order_in_expected],
                    "when testing {} by {:?}",
                    s, collation
                );
            }
        }
    }

    #[test]
    fn test_latin1_bin() {
        use std::{cmp::Ordering, collections::hash_map::DefaultHasher, hash::Hasher};

        use crate::codec::collation::collator::CollatorLatin1Bin;

        let cases = vec![
            (
                vec![0xFF, 0x88, 0x00, 0x13],
                vec![0xFF, 0x88, 0x00, 0x13],
                Ordering::Equal,
            ),
            (
                vec![0xFF, 0x88, 0x00, 0x13, 0x20, 0x20, 0x20],
                vec![0xFF, 0x88, 0x00, 0x13],
                Ordering::Equal,
            ),
            (
                vec![0xFF, 0x88, 0x00, 0x13, 0x09, 0x09, 0x09],
                vec![0xFF, 0x88, 0x00, 0x13],
                Ordering::Greater,
            ),
        ];

        for (sa, sb, od) in cases {
            let eval_hash = |s| {
                let mut hasher = DefaultHasher::default();
                CollatorLatin1Bin::sort_hash(&mut hasher, s).unwrap();
                hasher.finish()
            };

            let cmp = CollatorLatin1Bin::sort_compare(sa.as_slice(), sb.as_slice()).unwrap();
            let ha = eval_hash(sa.as_slice());
            let hb = eval_hash(sb.as_slice());

            assert_eq!(cmp, od, "when comparing {:?} and {:?}", sa, sb);

            if od == Ordering::Equal {
                assert_eq!(
                    ha, hb,
                    "when comparing the hash of {:?} and {:?}, which should be equal",
                    sa, sb
                );
            } else {
                assert_ne!(
                    ha, hb,
                    "when comparing the hash of {:?} and {:?}, which should not be equal",
                    sa, sb
                );
            }
        }
    }
}
