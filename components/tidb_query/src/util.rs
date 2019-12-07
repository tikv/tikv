// Copyright 2018 TiKV Project Authors. Licensed under Apache-2.0.

use kvproto::coprocessor as coppb;
use tipb::ColumnInfo;

use crate::codec::datum::Datum;

const MAX_RAND_VALUE: u32 = 0x3FFFFFFF;

pub struct MySQLRng {
    seed1: u32,
    seed2: u32,
}

impl MySQLRng {
    pub fn new() -> Self {
        let current_time = time::get_time();
        let nsec = i64::from(current_time.nsec);
        Self::new_with_seed(nsec)
    }

    pub fn new_with_seed(seed: i64) -> Self {
        let seed1 = (seed.wrapping_mul(0x10001).wrapping_add(55555555)) as u32 % MAX_RAND_VALUE;
        let seed2 = (seed.wrapping_mul(0x10000001)) as u32 % MAX_RAND_VALUE;
        MySQLRng { seed1, seed2 }
    }

    pub fn gen(&mut self) -> f64 {
        self.seed1 = (self.seed1 * 3 + self.seed2) % MAX_RAND_VALUE;
        self.seed2 = (self.seed1 + self.seed2 + 33) % MAX_RAND_VALUE;
        f64::from(self.seed1) / MAX_RAND_VALUE as f64
    }
}

/// Convert the key to the smallest key which is larger than the key given.
pub fn convert_to_prefix_next(key: &mut Vec<u8>) {
    if key.is_empty() {
        key.push(0);
        return;
    }
    let mut i = key.len() - 1;

    // Add 1 to the last byte that is not 255, and set it's following bytes to 0.
    loop {
        if key[i] == 255 {
            key[i] = 0;
        } else {
            key[i] += 1;
            return;
        }
        if i == 0 {
            // All bytes are 255. Append a 0 to the key.
            for byte in key.iter_mut() {
                *byte = 255;
            }
            key.push(0);
            return;
        }
        i -= 1;
    }
}

/// Check if `key`'s prefix next equals to `next`
pub fn is_prefix_next(key: &[u8], next: &[u8]) -> bool {
    let len = key.len();
    let next_len = next.len();

    if len == next_len {
        // Find the last non-255 byte
        let mut carry_pos = len;
        loop {
            if carry_pos == 0 {
                // All bytes of `key` are 255. `next` couldn't be `key`'s prefix_next since their
                // lengths are equal.
                return false;
            }

            carry_pos -= 1;
            if key[carry_pos] != 255 {
                break;
            }
        }

        // Now `carry_pos` is the index of the last byte that is not 255. For example:
        //   key: [1, 2, 3, 255, 255, 255]
        //               ^ carry_pos == 2

        // So they are equal when:
        // * `key`'s value at `carry_pos` is that of `next` - 1 and
        // * `next`'s part after carry_pos is all 0
        // * `key` and `next`'s parts before `carry_pos` are equal.
        // For example:
        //   key:  [1, 2, 3, 255, 255, 255]
        //   next: [1, 2, 4,   0,   0,   0]
        //                ^ carry_pos == 2
        // The part before `carry_pos` is all [1, 2],
        // the bytes at `carry_pos` differs by 1 (4 == 3 + 1), and
        // the remaining bytes of next ([0, 0, 0]) is all 0.
        // so [1, 2, 4, 0, 0, 0] is prefix_next of [1, 2, 3, 255, 255, 255]
        key[carry_pos] + 1 == next[carry_pos]
            && next[carry_pos + 1..].iter().all(|byte| *byte == 0)
            && key[..carry_pos] == next[..carry_pos]
    } else if len + 1 == next_len {
        // `next` must has one more 0 than `key`, and the first `len` bytes must be all 255.
        // The case that `len == 0` is also covered here.
        *next.last().unwrap() == 0
            && key.iter().all(|byte| *byte == 255)
            && next.iter().take(len).all(|byte| *byte == 255)
    } else {
        // Length not match.
        false
    }
}

/// `is_point` checks if the key range represents a point.
#[inline]
pub fn is_point(range: &coppb::KeyRange) -> bool {
    is_prefix_next(range.get_start(), range.get_end())
}

#[inline]
pub fn get_pk(col: &ColumnInfo, h: i64) -> Datum {
    use tidb_query_datatype::{FieldTypeAccessor, FieldTypeFlag};

    if col.as_accessor().flag().contains(FieldTypeFlag::UNSIGNED) {
        // PK column is unsigned
        Datum::U64(h as u64)
    } else {
        Datum::I64(h)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_prefix_next_once(key: &[u8], expected: &[u8]) {
        let mut key = key.to_vec();
        convert_to_prefix_next(&mut key);
        assert_eq!(key.as_slice(), expected);
    }

    #[test]
    fn test_prefix_next() {
        test_prefix_next_once(&[], &[0]);
        test_prefix_next_once(&[0], &[1]);
        test_prefix_next_once(&[1], &[2]);
        test_prefix_next_once(&[255], &[255, 0]);
        test_prefix_next_once(&[255, 255, 255], &[255, 255, 255, 0]);
        test_prefix_next_once(&[1, 255], &[2, 0]);
        test_prefix_next_once(&[0, 1, 255], &[0, 2, 0]);
        test_prefix_next_once(&[0, 1, 255, 5], &[0, 1, 255, 6]);
        test_prefix_next_once(&[0, 1, 5, 255], &[0, 1, 6, 0]);
        test_prefix_next_once(&[0, 1, 255, 255], &[0, 2, 0, 0]);
        test_prefix_next_once(&[0, 255, 255, 255], &[1, 0, 0, 0]);
    }

    fn test_is_prefix_next_case(lhs: &[u8], expected: &[u8], unexpected: &[&[u8]]) {
        assert!(is_prefix_next(lhs, expected));
        for rhs in unexpected {
            assert!(!is_prefix_next(lhs, rhs));
        }
    }

    #[test]
    fn test_is_prefix_next() {
        test_is_prefix_next_case(&[], &[0], &[&[], &[1], &[2]]);
        test_is_prefix_next_case(&[0], &[1], &[&[], &[0], &[0, 0], &[2], &[255]]);
        test_is_prefix_next_case(&[1], &[2], &[&[], &[1], &[3], &[1, 0]]);
        test_is_prefix_next_case(&[255], &[255, 0], &[&[0], &[255, 255, 0]]);
        test_is_prefix_next_case(
            &[255, 255, 255],
            &[255, 255, 255, 0],
            &[
                &[],
                &[0],
                &[0, 0, 0],
                &[255, 255, 0],
                &[255, 255, 255, 255, 0],
            ],
        );
        test_is_prefix_next_case(
            &[1, 255],
            &[2, 0],
            &[&[], &[1, 255, 0], &[2, 255], &[1, 255], &[2, 0, 0]],
        );
        test_is_prefix_next_case(
            &[0, 255],
            &[1, 0],
            &[&[], &[0, 255, 0], &[1, 255], &[0, 255], &[1, 0, 0]],
        );
        test_is_prefix_next_case(
            &[1, 2, 3, 4, 255, 255],
            &[1, 2, 3, 5, 0, 0],
            &[
                &[],
                &[1, 2, 3, 4, 255, 255],
                &[1, 2, 3, 4, 0, 0],
                &[1, 2, 3, 5, 255, 255],
                &[1, 2, 3, 5, 0, 1],
                &[1, 2, 3, 5, 1, 0],
                &[1, 2, 4, 0, 0, 0],
            ],
        );
    }

    #[test]
    #[allow(clippy::float_cmp)]
    fn test_rand_new() {
        let mut rng1 = MySQLRng::new();
        let mut rng2 = MySQLRng::new();
        let got1 = rng1.gen();
        let got2 = rng2.gen();
        assert!(got1 < 1.0);
        assert!(got1 >= 0.0);
        assert_ne!(got1, rng1.gen());
        assert!(got2 < 1.0);
        assert!(got2 >= 0.0);
        assert_ne!(got2, rng2.gen());
        assert_ne!(got1, got2);
    }

    #[test]
    #[allow(clippy::float_cmp)]
    fn test_rand_new_with_seed() {
        let tests = vec![
            (0, 0.15522042769493574, 0.620881741513388),
            (1, 0.40540353712197724, 0.8716141803857071),
            (-1, 0.9050373219931845, 0.37014932126752037),
            (9223372036854775807, 0.9050373219931845, 0.37014932126752037),
        ];
        for (seed, exp1, exp2) in tests {
            let mut rand = MySQLRng::new_with_seed(seed);
            let res1 = rand.gen();
            assert_eq!(res1, exp1);
            let res2 = rand.gen();
            assert_eq!(res2, exp2);
        }
    }
}
