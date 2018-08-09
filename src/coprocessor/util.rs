// Copyright 2018 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

use kvproto::coprocessor as coppb;
use tipb::schema::ColumnInfo;

use super::codec::datum::Datum;
use super::codec::mysql;

/// Convert the key to the smallest key which is larger than the key given.
pub fn prefix_next(key: &mut Vec<u8>) {
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

/// Check if `key1`'s prefix next equals to `key2`
pub fn prefix_next_equals(key1: &[u8], key2: &[u8]) -> bool {
    if key1.is_empty() {
        return key2 == [0];
    }
    if key2.is_empty() {
        return false;
    }

    let mut carry_pos = key1.len() - 1;
    loop {
        if key1[carry_pos] != 255 {
            break;
        }

        if carry_pos == 0 {
            // All bytes of `start` are 255. `end` should equals `start.append(0)`
            return &key2[..key2.len() - 1] == key1 && *key2.last().unwrap() == 0;
        }

        carry_pos -= 1;
    }

    // So they are equal when:
    // * lengths are equal and
    // * `key1`'s value at `carry_pos` is that of `key2` - 1 and
    // * `key2`'s part after carry_pos is all 0
    // * `key1` and `key2`'s parts before carry_pos are equal.
    key1.len() == key2.len()
        && key1[carry_pos] + 1 == key2[carry_pos]
        && key2[carry_pos + 1..].iter().all(|byte| *byte == 0)
        && key1[..carry_pos] == key2[..carry_pos]
}

/// `is_point` checks if the key range represents a point.
pub fn is_point(range: &coppb::KeyRange) -> bool {
    prefix_next_equals(range.get_start(), range.get_end())
}

#[inline]
pub fn get_pk(col: &ColumnInfo, h: i64) -> Datum {
    if mysql::has_unsigned_flag(col.get_flag() as u64) {
        // PK column is unsigned
        Datum::U64(h as u64)
    } else {
        Datum::I64(h)
    }
}

#[cfg(test)]
mod test {
    use super::*;

    fn test_prefix_next_once(key: &[u8], expected: &[u8]) {
        let mut key = key.to_vec();
        prefix_next(&mut key);
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

    fn test_prefix_next_equals_case(lhs: &[u8], expected: &[u8], unexpected: &[&[u8]]) {
        assert!(prefix_next_equals(lhs, expected));
        for rhs in unexpected {
            assert!(!prefix_next_equals(lhs, rhs));
        }
    }

    #[test]
    fn test_prefix_next_equals() {
        test_prefix_next_equals_case(&[], &[0], &[&[], &[1], &[2]]);
        test_prefix_next_equals_case(&[0], &[1], &[&[], &[0], &[00], &[2], &[255]]);
        test_prefix_next_equals_case(&[1], &[2], &[&[], &[1], &[3], &[10]]);
        test_prefix_next_equals_case(&[255], &[255, 0], &[&[0], &[255, 255, 0]]);
        test_prefix_next_equals_case(
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
        test_prefix_next_equals_case(
            &[1, 255],
            &[2, 0],
            &[&[], &[1, 255, 0], &[2, 255], &[1, 255], &[2, 0, 0]],
        );
        test_prefix_next_equals_case(
            &[0, 255],
            &[1, 0],
            &[&[], &[0, 255, 0], &[1, 255], &[0, 255], &[1, 0, 0]],
        );
        test_prefix_next_equals_case(
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
}
