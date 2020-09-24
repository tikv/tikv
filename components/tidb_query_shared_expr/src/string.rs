// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

use super::conv::i64_to_usize;

const MAX_BLOB_WIDTH: i32 = 16_777_216; // FIXME: Should be isize

// see https://dev.mysql.com/doc/refman/5.7/en/string-functions.html#function_to-base64
// mysql base64 doc: A newline is added after each 76 characters of encoded output
const BASE64_LINE_WRAP_LENGTH: usize = 76;

// mysql base64 doc: Each 3 bytes of the input data are encoded using 4 characters.
pub const BASE64_INPUT_CHUNK_LENGTH: usize = 3;
pub const BASE64_ENCODED_CHUNK_LENGTH: usize = 4;
const BASE64_LINE_WRAP: u8 = b'\n';

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

#[inline]
pub fn encoded_size(len: usize) -> Option<usize> {
    if len == 0 {
        return Some(0);
    }
    // size_without_wrap = (len + (3 - 1)) / 3 * 4
    // size = size_without_wrap + (size_withou_wrap - 1) / 76
    len.checked_add(BASE64_INPUT_CHUNK_LENGTH - 1)
        .and_then(|r| r.checked_div(BASE64_INPUT_CHUNK_LENGTH))
        .and_then(|r| r.checked_mul(BASE64_ENCODED_CHUNK_LENGTH))
        .and_then(|r| r.checked_add((r - 1) / BASE64_LINE_WRAP_LENGTH))
}

// similar logic to crate `line-wrap`, since we had call `encoded_size` before,
// there is no need to use checked_xxx math operation like `line-wrap` does.
#[inline]
pub fn line_wrap(buf: &mut [u8], input_len: usize) {
    let line_len = BASE64_LINE_WRAP_LENGTH;
    if input_len <= line_len {
        return;
    }
    let last_line_len = if input_len % line_len == 0 {
        line_len
    } else {
        input_len % line_len
    };
    let lines_with_ending = (input_len - 1) / line_len;
    let line_with_ending_len = line_len + 1;
    let mut old_start = input_len - last_line_len;
    let mut new_start = buf.len() - last_line_len;
    safemem::copy_over(buf, old_start, new_start, last_line_len);
    for _ in 0..lines_with_ending {
        old_start -= line_len;
        new_start -= line_with_ending_len;
        safemem::copy_over(buf, old_start, new_start, line_len);
        buf[new_start + line_len] = BASE64_LINE_WRAP;
    }
}

pub enum TrimDirection {
    Both = 1,
    Leading,
    Trailing,
}

// FIXME: We could use `TryInto` here but then we need to introduce some ad-hoc error type?
//        Maybe once Rust have something like `MaybeInto`...
impl TrimDirection {
    pub fn from_i64(i: i64) -> Option<Self> {
        match i {
            1 => Some(TrimDirection::Both),
            2 => Some(TrimDirection::Leading),
            3 => Some(TrimDirection::Trailing),
            _ => None,
        }
    }

    pub fn into_i64(&self) -> i64 {
        match self {
            TrimDirection::Both => 1,
            TrimDirection::Leading => 2,
            TrimDirection::Trailing => 3
        }
    }
}

#[inline]
pub fn trim(s: &str, pat: &str, direction: TrimDirection) -> Vec<u8> {
    let r = match direction {
        TrimDirection::Leading => s.trim_start_matches(pat),
        TrimDirection::Trailing => s.trim_end_matches(pat),
        _ => s.trim_start_matches(pat).trim_end_matches(pat),
    };
    r.to_string().into_bytes()
}

#[inline]
pub fn strip_whitespace(input: &[u8]) -> Vec<u8> {
    let mut input_copy = Vec::<u8>::with_capacity(input.len());
    input_copy.extend(input.iter().filter(|b| !b" \n\t\r\x0b\x0c".contains(b)));
    input_copy
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
