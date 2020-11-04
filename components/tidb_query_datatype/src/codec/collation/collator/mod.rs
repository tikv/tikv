// Copyright 2020 TiKV Project Authors. Licensed under Apache-2.0.

mod binary;
mod utf8mb4_binary;
mod utf8mb4_general_ci;
mod utf8mb4_unicode_ci;
mod utf8mb4_zh_pinyin_tidb_as_cs;

pub use binary::*;
pub use utf8mb4_binary::*;
pub use utf8mb4_general_ci::*;
pub use utf8mb4_unicode_ci::*;
pub use utf8mb4_zh_pinyin_tidb_as_cs::*;

use std::cmp::Ordering;
use std::hash::{Hash, Hasher};
use std::str;

use codec::prelude::*;

use super::charset::*;
use super::Collator;
use crate::codec::Result;

const TRIM_PADDING_SPACE: char = 0x20 as char;

#[cfg(test)]
mod tests {
    use crate::codec::collation::match_template_collator;
    use crate::codec::collation::Collator;
    use crate::Collation;

    #[test]
    #[allow(clippy::string_lit_as_bytes)]
    fn test_compare() {
        use std::cmp::Ordering;
        use std::collections::hash_map::DefaultHasher;
        use std::hash::Hasher;

        let collations = [
            (Collation::Utf8Mb4Bin, 0),
            (Collation::Utf8Mb4BinNoPadding, 1),
            (Collation::Utf8Mb4GeneralCi, 2),
            (Collation::Utf8Mb4UnicodeCi, 3),
            (Collation::Utf8mb4ZhPinyinAsCs, 4),
        ];
        let cases = vec![
            // (sa, sb, [Utf8Mb4Bin, Utf8Mb4BinNoPadding, Utf8Mb4GeneralCi, Utf8Mb4UnicodeCi, Utf8mb4ZhPinyinAsCs])
            (
                "a".as_bytes(),
                "a".as_bytes(),
                [
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
                ],
            ),
            (
                "设".as_bytes(),
                "置".as_bytes(),
                [
                    Ordering::Greater,
                    Ordering::Greater,
                    Ordering::Greater,
                    Ordering::Greater,
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
            (Collation::Utf8mb4ZhPinyinAsCs, 4),
        ];
        let cases = vec![
            // (str, [Utf8Mb4Bin, Utf8Mb4BinNoPadding, Utf8Mb4GeneralCi, Utf8Mb4UnicodeCi, Utf8mb4ZhPinyinAsCs])
            (
                "a",
                [vec![0x61], vec![0x61], vec![0x00, 0x41], vec![0x0E, 0x33], vec![0x61]],
            ),
            (
                "A ",
                [
                    vec![0x41],
                    vec![0x41, 0x20],
                    vec![0x00, 0x41],
                    vec![0x0E, 0x33],
                    vec![0x41]
                ],
            ),
            (
                "A",
                [vec![0x41], vec![0x41], vec![0x00, 0x41], vec![0x0E, 0x33], vec![0x41]],
            ),
            (
                "😃",
                [
                    vec![0xF0, 0x9F, 0x98, 0x83],
                    vec![0xF0, 0x9F, 0x98, 0x83],
                    vec![0xff, 0xfd],
                    vec![0xff, 0xfd],
                    vec![0xff, 0x03, 0xd8, 0x4b],
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
                    //
                    vec![
                        0x46, 0x6F, 0x6F, 0x20, 0xFF, 0x00, 0x00, 0x26, 0x20, 0x62, 0x61, 0x72, 0x20, 0xFF, 0x03, 0xB5, 0x4E, 0x20, 0x62, 0x61, 0x7A, 0x20, 0xFF, 0x00, 0x23, 0xC8, 0x20, 0x71, 0x75, 0x78,
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
                    vec![0xFF, 0x00, 0x98, 0x8F],
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
}
