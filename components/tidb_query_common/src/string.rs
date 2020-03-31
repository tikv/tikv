// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

// see https://dev.mysql.com/doc/refman/5.7/en/string-functions.html#function_to-base64
// mysql base64 doc: A newline is added after each 76 characters of encoded output
pub const BASE64_LINE_WRAP_LENGTH: usize = 76;

// mysql base64 doc: Each 3 bytes of the input data are encoded using 4 characters.
pub const BASE64_INPUT_CHUNK_LENGTH: usize = 3;
pub const BASE64_ENCODED_CHUNK_LENGTH: usize = 4;
pub const BASE64_LINE_WRAP: u8 = b'\n';

#[inline]
pub fn strip_whitespace(input: &[u8]) -> Vec<u8> {
    let mut input_copy = Vec::<u8>::with_capacity(input.len());
    input_copy.extend(input.iter().filter(|b| !b" \n\t\r\x0b\x0c".contains(b)));
    input_copy
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn test_strip_whitespace() {
        let cases: Vec<(&str, &str)> = vec![(" \naa \nbc\n\t\r\x0b\x0c", "aabc")];
        for (s, expect) in cases {
            assert_eq!(
                strip_whitespace(&s.as_bytes().to_vec()),
                expect.as_bytes().to_vec()
            );
        }
    }
}
