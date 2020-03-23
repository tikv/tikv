// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

use crate::codec::data_type::*;
use base64;

// see https://dev.mysql.com/doc/refman/5.7/en/string-functions.html#function_to-base64
// mysql base64 doc: A newline is added after each 76 characters of encoded output
pub const BASE64_LINE_WRAP_LENGTH: usize = 76;

// mysql base64 doc: Each 3 bytes of the input data are encoded using 4 characters.
pub const BASE64_INPUT_CHUNK_LENGTH: usize = 3;
pub const BASE64_ENCODED_CHUNK_LENGTH: usize = 4;
pub const BASE64_LINE_WRAP: u8 = b'\n';

pub fn to_base64(s: &[u8]) -> Option<Bytes> {
    if let Some(size) = encoded_size(s.len()) {
        let mut result = vec![0; size];
        let len_without_wrap = base64::encode_config_slice(&s, base64::STANDARD, &mut result);
        line_wrap(&mut result, len_without_wrap);
        Some(result)
    } else {
        None
    }
}

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
fn line_wrap(buf: &mut [u8], input_len: usize) {
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

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn test_encoded_size() {
        assert_eq!(encoded_size(0).unwrap(), 0);
        assert_eq!(encoded_size(54).unwrap(), 72);
        assert_eq!(encoded_size(58).unwrap(), 81);
        assert!(encoded_size(usize::max_value()).is_none());
    }
}
