// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

use std::{
    cmp::{Eq, Ord, Ordering, PartialEq, PartialOrd},
    string::ToString,
};

use crate::{
    codec::{error::Error, Result},
    expr::EvalContext,
};

/// BinaryLiteral is the internal type for storing bit / hex literal type.
#[derive(Debug)]
pub struct BinaryLiteral(Vec<u8>);

fn trim_leading_zero_bytes(bytes: &[u8]) -> &[u8] {
    if bytes.is_empty() {
        return bytes;
    }
    let (pos, _) = bytes
        .iter()
        .enumerate()
        .find(|(_, &x)| x != 0)
        .unwrap_or((bytes.len() - 1, &0));
    &bytes[pos..]
}

/// Returns the int value for the literal.
pub fn to_uint(ctx: &mut EvalContext, bytes: &[u8]) -> Result<u64> {
    let bytes = trim_leading_zero_bytes(bytes);
    if bytes.is_empty() {
        return Ok(0);
    }
    if bytes.len() > 8 {
        ctx.handle_truncate_err(Error::truncated_wrong_val(
            "BINARY",
            BinaryLiteral(bytes.to_owned()).to_string(),
        ))?;
        return Ok(u64::MAX);
    }
    let val = bytes.iter().fold(0, |acc, x| acc << 8 | (u64::from(*x)));
    Ok(val)
}

impl BinaryLiteral {
    /// from_u64 creates a new BinaryLiteral instance by the given uint value in BigEndian.
    /// byte size will be used as the length of the new BinaryLiteral, with leading bytes filled to zero.
    /// If byte size is -1, the leading zeros in new BinaryLiteral will be trimmed.
    pub fn from_u64(val: u64, byte_size: isize) -> Result<Self> {
        if byte_size != -1 && !(1..=8).contains(&byte_size) {
            return Err(box_err!("invalid byte size: {}", byte_size));
        }
        let bytes = val.to_be_bytes();
        let lit = if byte_size == -1 {
            Self(trim_leading_zero_bytes(&bytes[..]).to_vec())
        } else {
            let mut v = bytes[..].to_vec();
            v.drain(0..(8 - byte_size) as usize);
            Self(v)
        };
        Ok(lit)
    }

    /// Parses hexadecimal string literal.
    /// See <https://dev.mysql.com/doc/refman/5.7/en/hexadecimal-literals.html>
    pub fn from_hex_str(s: &str) -> Result<Self> {
        if s.is_empty() {
            return Err(box_err!(
                "invalid empty string for parsing hexadecimal literal"
            ));
        }

        let trimed = if s.starts_with('x') || s.starts_with('X') {
            // format is x'val' or X'val'
            let trimed = s[1..].trim_start_matches('\'');
            let trimed = trimed.trim_end_matches('\'');
            if trimed.len() % 2 != 0 {
                return Err(box_err!(
                    "invalid hexadecimal format, must even numbers, but {}",
                    s.len()
                ));
            }
            trimed
        } else if s.starts_with("0x") {
            s.trim_start_matches("0x")
        } else {
            // here means format is not x'val', X'val' or 0xval.
            return Err(box_err!("invalid hexadecimal format: {}", s));
        };
        if trimed.is_empty() {
            return Ok(BinaryLiteral(vec![]));
        }
        let v = if trimed.len() % 2 != 0 {
            let mut head = vec![b'0'];
            head.extend(trimed.as_bytes());
            box_try!(hex::decode(head))
        } else {
            box_try!(hex::decode(trimed.as_bytes()))
        };
        Ok(BinaryLiteral(v))
    }

    /// Parses bit string.
    /// The string format can be b'val', B'val' or 0bval, val must be 0 or 1.
    /// See <https://dev.mysql.com/doc/refman/5.7/en/bit-value-literals.html>
    pub fn from_bit_str(s: &str) -> Result<Self> {
        if s.is_empty() {
            return Err(box_err!("invalid empty string for parsing bit type"));
        }
        let trimed = if s.starts_with('b') || s.starts_with('B') {
            // format is b'val' or B'val'
            let trimed = s[1..].trim_start_matches('\'');
            trimed.trim_end_matches('\'')
        } else if s.starts_with("0b") {
            s.trim_start_matches("0b")
        } else {
            // here means format is not b'val', B'val' or 0bval.
            return Err(box_err!("invalid hexadecimal format: {}", s));
        };
        if trimed.is_empty() {
            return Ok(BinaryLiteral(vec![]));
        }

        // Align the length to 8
        let aligned_len = (trimed.len() + 7) & (!7);
        let mut padding_str = "0".repeat(8);
        padding_str.push_str(trimed);
        let start = trimed.len() + 8 - aligned_len;
        let substr = &padding_str[start..];
        let len = substr.len() >> 3;
        let mut buf = Vec::with_capacity(len);
        let mut i = 0;
        while i < len {
            let pos = i << 3;
            let val = box_try!(usize::from_str_radix(&substr[pos..(pos + 8)], 2));
            buf.push(val as u8);
            i += 1;
        }
        Ok(BinaryLiteral(buf))
    }

    /// Returns the bit literal representation for the literal.
    pub fn to_bit_string(&self, trim_zero: bool) -> String {
        if self.0.is_empty() {
            return "b''".to_string();
        }
        let mut s = String::with_capacity(self.0.len() * 2 + 1);
        for b in &self.0 {
            s.push_str(&format!("{:08b}", *b));
        }
        if trim_zero {
            let trimed = s.trim_start_matches('0');
            if trimed.is_empty() {
                s.clear();
                s.push_str("b'0'");
                return s;
            }
            let trimed_len = s.len() - trimed.len();
            s.drain(0..trimed_len);
        }
        s.insert_str(0, "b'");
        s.push('\'');
        s
    }

    /// Returns the int value for the literal.
    pub fn to_uint(&self, ctx: &mut EvalContext) -> Result<u64> {
        to_uint(ctx, &self.0)
    }
}

impl ToString for BinaryLiteral {
    fn to_string(&self) -> String {
        if self.0.is_empty() {
            return String::new();
        }
        format!("0x{}", hex::encode(self.0.as_slice()))
    }
}

impl Eq for BinaryLiteral {}

impl PartialEq for BinaryLiteral {
    fn eq(&self, rhs: &Self) -> bool {
        self.0 == rhs.0
    }
}

impl PartialOrd for BinaryLiteral {
    fn partial_cmp(&self, rhs: &Self) -> Option<Ordering> {
        Some(self.cmp(rhs))
    }
}

impl Ord for BinaryLiteral {
    fn cmp(&self, rhs: &Self) -> Ordering {
        let l = trim_leading_zero_bytes(&self.0);
        let r = trim_leading_zero_bytes(&rhs.0);
        if l.len() > r.len() {
            return Ordering::Greater;
        }
        if l.len() < r.len() {
            return Ordering::Less;
        }
        l.cmp(r)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_trim_leading_zero_bytes() {
        let cs: Vec<(Vec<u8>, Vec<u8>)> = vec![
            (vec![], vec![]),
            (vec![0x0], vec![0x0]),
            (vec![0x1], vec![0x1]),
            (vec![0x1, 0x0], vec![0x1, 0x0]),
            (vec![0x0, 0x1], vec![0x1]),
            (vec![0x0, 0x0, 0x0], vec![0x0]),
            (vec![0x1, 0x0, 0x0], vec![0x1, 0x0, 0x0]),
            (
                vec![0x0, 0x1, 0x0, 0x0, 0x1, 0x0, 0x0],
                vec![0x1, 0x0, 0x0, 0x1, 0x0, 0x0],
            ),
            (
                vec![0x0, 0x0, 0x0, 0x0, 0x0, 0x1, 0x0, 0x0, 0x1, 0x0, 0x0],
                vec![0x1, 0x0, 0x0, 0x1, 0x0, 0x0],
            ),
        ];
        for (input, expected) in cs {
            let result = trim_leading_zero_bytes(input.as_slice());
            assert_eq!(result, expected.as_slice())
        }
    }

    #[test]
    fn test_binary_literal_from_u64() {
        let cs: Vec<(u64, isize, Vec<u8>)> = vec![
            (0x0, -1, vec![0x0]),
            (0x0, 1, vec![0x0]),
            (0x0, 2, vec![0x0, 0x0]),
            (0x1, -1, vec![0x1]),
            (0x1, 1, vec![0x1]),
            (0x1, 2, vec![0x0, 0x1]),
            (0x1, 3, vec![0x0, 0x0, 0x1]),
            (0x10, -1, vec![0x10]),
            (0x123, -1, vec![0x1, 0x23]),
            (0x123, 2, vec![0x1, 0x23]),
            (0x123, 1, vec![0x23]),
            (0x123, 5, vec![0x0, 0x0, 0x0, 0x1, 0x23]),
            (0x4D7953514C, -1, vec![0x4D, 0x79, 0x53, 0x51, 0x4C]),
            (
                0x4D7953514C,
                8,
                vec![0x0, 0x0, 0x0, 0x4D, 0x79, 0x53, 0x51, 0x4C],
            ),
            (
                0x4920616D2061206C,
                -1,
                vec![0x49, 0x20, 0x61, 0x6D, 0x20, 0x61, 0x20, 0x6C],
            ),
            (
                0x4920616D2061206C,
                8,
                vec![0x49, 0x20, 0x61, 0x6D, 0x20, 0x61, 0x20, 0x6C],
            ),
            (0x4920616D2061206C, 5, vec![0x6D, 0x20, 0x61, 0x20, 0x6C]),
        ];

        for (n, byte_size, expected) in cs {
            let lit = BinaryLiteral::from_u64(n, byte_size).unwrap();
            assert_eq!(lit.0, expected);
        }

        let lit = BinaryLiteral::from_u64(100, -2);
        assert!(lit.is_err());
    }

    #[test]
    fn test_binary_literal_to_string() {
        let cs: Vec<(BinaryLiteral, &str)> = vec![
            (BinaryLiteral(vec![]), ""),
            (BinaryLiteral(vec![0x00]), "0x00"),
            (BinaryLiteral(vec![0x01]), "0x01"),
            (BinaryLiteral(vec![0xff, 0x01]), "0xff01"),
        ];
        for (lit, expected) in cs {
            let s = lit.to_string();
            assert_eq!(&s, expected);
        }
    }

    #[test]
    fn test_binary_literal_from_hex_str() {
        let cs: Vec<(&str, Vec<u8>, bool)> = vec![
            ("x'1'", vec![], true),
            ("x'01'", vec![0x1], false),
            ("X'01'", vec![0x1], false),
            ("0x1", vec![0x1], false),
            ("0x-1", vec![], true),
            ("0X11", vec![], true),
            ("x'01+'", vec![], true),
            ("0x123", vec![0x01, 0x23], false),
            ("0x10", vec![0x10], false),
            ("0x4D7953514C", b"MySQL".to_vec(), false),
            (
                "0x4920616D2061206C6F6E672068657820737472696E67",
                b"I am a long hex string".to_vec(),
                false,
            ),
            (
                "x'4920616D2061206C6F6E672068657820737472696E67'",
                b"I am a long hex string".to_vec(),
                false,
            ),
            (
                "X'4920616D2061206C6F6E672068657820737472696E67'",
                b"I am a long hex string".to_vec(),
                false,
            ),
            ("x''", vec![], false),
        ];
        for (input, exptected, err) in cs {
            if err {
                assert!(
                    BinaryLiteral::from_hex_str(input).is_err(),
                    "input: {}",
                    input
                );
            } else {
                let lit = BinaryLiteral::from_hex_str(input).unwrap();
                assert_eq!(lit.0, exptected);
            }
        }
    }

    #[test]
    fn test_binary_literal_from_bit_str() {
        let cs = vec![
            ("b''", vec![], false),
            ("B''", vec![], false),
            ("0b''", vec![], true),
            ("0b0", vec![0x0], false),
            ("b'0'", vec![0x0], false),
            ("B'0'", vec![0x0], false),
            ("0B0", vec![], true),
            ("0b123", vec![], true),
            ("b'123'", vec![], true),
            ("0b'1010'", vec![], true),
            ("0b0000000", vec![0x0], false),
            ("b'0000000'", vec![0x0], false),
            ("B'0000000'", vec![0x0], false),
            ("0b00000000", vec![0x0], false),
            ("b'00000000'", vec![0x0], false),
            ("B'00000000'", vec![0x0], false),
            ("0b000000000", vec![0x0, 0x0], false),
            ("b'000000000'", vec![0x0, 0x0], false),
            ("B'000000000'", vec![0x0, 0x0], false),
            ("0b1", vec![0x1], false),
            ("b'1'", vec![0x1], false),
            ("B'1'", vec![0x1], false),
            ("0b00000001", vec![0x1], false),
            ("b'00000001'", vec![0x1], false),
            ("B'00000001'", vec![0x1], false),
            ("0b000000010", vec![0x0, 0x2], false),
            ("b'000000010'", vec![0x0, 0x2], false),
            ("B'000000010'", vec![0x0, 0x2], false),
            ("0b000000001", vec![0x0, 0x1], false),
            ("b'000000001'", vec![0x0, 0x1], false),
            ("B'000000001'", vec![0x0, 0x1], false),
            ("0b11111111", vec![0xFF], false),
            ("b'11111111'", vec![0xFF], false),
            ("B'11111111'", vec![0xFF], false),
            ("0b111111111", vec![0x1, 0xFF], false),
            ("b'111111111'", vec![0x1, 0xFF], false),
            ("B'111111111'", vec![0x1, 0xFF], false),
            (
                "0b1101000011001010110110001101100011011110010000001110111011011110111001001101100011001000010000001100110011011110110111100100000011000100110000101110010",
                b"hello world foo bar".to_vec(),
                false,
            ),
            (
                "b'1101000011001010110110001101100011011110010000001110111011011110111001001101100011001000010000001100110011011110110111100100000011000100110000101110010'",
                b"hello world foo bar".to_vec(),
                false,
            ),
            (
                "B'1101000011001010110110001101100011011110010000001110111011011110111001001101100011001000010000001100110011011110110111100100000011000100110000101110010'",
                b"hello world foo bar".to_vec(),
                false,
            ),
            (
                "0b01101000011001010110110001101100011011110010000001110111011011110111001001101100011001000010000001100110011011110110111100100000011000100110000101110010",
                b"hello world foo bar".to_vec(),
                false,
            ),
            (
                "b'01101000011001010110110001101100011011110010000001110111011011110111001001101100011001000010000001100110011011110110111100100000011000100110000101110010'",
                b"hello world foo bar".to_vec(),
                false,
            ),
            (
                "B'01101000011001010110110001101100011011110010000001110111011011110111001001101100011001000010000001100110011011110110111100100000011000100110000101110010'",
                b"hello world foo bar".to_vec(),
                false,
            ),
        ];

        for (input, exptected, err) in cs {
            if err {
                let lit = BinaryLiteral::from_bit_str(input);
                assert!(lit.is_err(), "input: {}, lit: {:?}", input, lit);
            } else {
                let lit = BinaryLiteral::from_bit_str(input).unwrap();
                assert_eq!(lit.0, exptected);
            }
        }
    }

    #[test]
    fn test_binary_literal_to_bit_string() {
        let cs: Vec<(Vec<u8>, bool, &str)> = vec![
            (vec![], true, "b''"),
            (vec![], false, "b''"),
            (vec![0x0], true, "b'0'"),
            (vec![0x0], false, "b'00000000'"),
            (vec![0x0, 0x0], true, "b'0'"),
            (vec![0x0, 0x0], false, "b'0000000000000000'"),
            (vec![0x1], true, "b'1'"),
            (vec![0x1], false, "b'00000001'"),
            (vec![0xff, 0x01], true, "b'1111111100000001'"),
            (vec![0xff, 0x01], false, "b'1111111100000001'"),
            (vec![0x0, 0xff, 0x01], true, "b'1111111100000001'"),
            (vec![0x0, 0xff, 0x01], false, "b'000000001111111100000001'"),
        ];
        for (input, trim_zero, expected) in cs {
            let lit = BinaryLiteral(input);
            assert_eq!(
                lit.to_bit_string(trim_zero).as_str(),
                expected,
                "expect: {}",
                expected
            );
        }
    }

    #[test]
    fn test_binary_literal_to_uint() {
        let cs: Vec<(&str, u64, bool)> = vec![
            ("x''", 0, false),
            ("0x00", 0x0, false),
            ("0xff", 0xff, false),
            ("0x10ff", 0x10ff, false),
            ("0x1010ffff", 0x1010ffff, false),
            ("0x1010ffff8080", 0x1010ffff8080, false),
            ("0x1010ffff8080ff12", 0x1010ffff8080ff12, false),
            ("0x1010ffff8080ff12ff", 0xffffffffffffffff, true),
        ];
        let mut ctx = EvalContext::default();
        for (s, expected, err) in cs {
            if err {
                assert!(
                    BinaryLiteral::from_hex_str(s)
                        .unwrap()
                        .to_uint(&mut ctx)
                        .is_err()
                );
            } else {
                let lit = BinaryLiteral::from_hex_str(s).unwrap();
                assert_eq!(lit.to_uint(&mut ctx).unwrap(), expected)
            }
        }
    }

    #[test]
    fn test_binary_literal_cmp() {
        let cs = vec![
            (vec![0, 0, 1], vec![2], Ordering::Less),
            (vec![0, 1], vec![0, 0, 2], Ordering::Less),
            (vec![0, 1], vec![1], Ordering::Equal),
            (vec![0, 2, 1], vec![1, 2], Ordering::Greater),
        ];
        for (lhs, rhs, expected) in cs {
            let l = BinaryLiteral(lhs);
            let r = BinaryLiteral(rhs);
            let result = l.cmp(&r);
            assert_eq!(result, expected);
        }
    }
}
