// Copyright 2020 TiKV Project Authors. Licensed under Apache-2.0.

pub use binary::*;
pub use latin1_bin::*;
pub use utf8mb4_binary::*;
pub use utf8mb4_general_ci::*;

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
    fn test_latin1_bin() {
        use crate::codec::collation::collator::CollatorLatin1Bin;
        use std::cmp::Ordering;
        use std::collections::hash_map::DefaultHasher;
        use std::hash::Hasher;

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
