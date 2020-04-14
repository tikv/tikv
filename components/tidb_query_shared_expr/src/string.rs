// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

use super::conv::i64_to_usize;

const MAX_BLOB_WIDTH: i32 = 16_777_216; // FIXME: Should be isize

// when target_len is 0, return Some(0), means the pad function should return empty string
// currently there are three conditions it return None, which means pad function should return Null
//   1. target_len is negative
//   2. target_len of type in byte is larger then MAX_BLOB_WIDTH
//   3. target_len is greater than length of input string, *and* pad string is empty
// otherwise return Some(target_len)
#[inline]
pub fn validate_target_len_for_pad(
    len_unsigned: bool,
    target_len: i64,
    input_len: usize,
    size_of_type: usize,
    pad_empty: bool,
) -> Option<usize> {
    if target_len == 0 {
        return Some(0);
    }
    let (target_len, target_len_positive) = i64_to_usize(target_len, len_unsigned);
    if !target_len_positive
        || target_len.saturating_mul(size_of_type) > MAX_BLOB_WIDTH as usize
        || (pad_empty && input_len < target_len)
    {
        return None;
    }
    Some(target_len)
}

#[cfg(test)]
mod tests {
    #[test]
    fn test_validate_target_len_for_pad() {
        let cases = vec![
            // target_len, input_len, size_of_type, pad_empty, result
            (0, 10, 1, false, Some(0)),
            (-1, 10, 1, false, None),
            (12, 10, 1, true, None),
            (i64::from(super::MAX_BLOB_WIDTH) + 1, 10, 1, false, None),
            (i64::from(super::MAX_BLOB_WIDTH) / 4 + 1, 10, 4, false, None),
            (12, 10, 1, false, Some(12)),
        ];
        for case in cases {
            let got = super::validate_target_len_for_pad(false, case.0, case.1, case.2, case.3);
            assert_eq!(got, case.4);
        }

        let unsigned_cases = vec![
            (u64::max_value(), 10, 1, false, None),
            (u64::max_value(), 10, 4, false, None),
            (u64::max_value(), 10, 1, true, None),
            (u64::max_value(), 10, 4, true, None),
            (12u64, 10, 4, false, Some(12)),
        ];
        for case in unsigned_cases {
            let got =
                super::validate_target_len_for_pad(true, case.0 as i64, case.1, case.2, case.3);
            assert_eq!(got, case.4);
        }
    }
}
