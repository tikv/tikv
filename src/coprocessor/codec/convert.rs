// Copyright 2016 PingCAP, Inc.
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

use std::borrow::Cow;
use std::{self, i64, str, u64};

use super::mysql::Res;
use super::{Error, Result};
use coprocessor::dag::expr::EvalContext;
// `UNSPECIFIED_LENGTH` is unspecified length from FieldType
pub const UNSPECIFIED_LENGTH: i32 = -1;

pub fn truncate_binary(s: &mut Vec<u8>, flen: isize) {
    if flen != UNSPECIFIED_LENGTH as isize && s.len() > flen as usize {
        s.truncate(flen as usize);
    }
}

/// `truncate_f64` (`TruncateFloat` in tidb) tries to truncate f.
/// If the result exceeds the max/min float that flen/decimal
/// allowed, returns the max/min float allowed.
pub fn truncate_f64(mut f: f64, flen: u8, decimal: u8) -> Res<f64> {
    if f.is_nan() {
        return Res::Overflow(0f64);
    }
    let shift = 10u64.pow(u32::from(decimal)) as f64;
    let maxf = 10u64.pow(u32::from(flen - decimal)) as f64 - 1.0 / shift;
    if f.is_finite() {
        let tmp = f * shift;
        if tmp.is_finite() {
            f = tmp.round() / shift
        }
    };

    if f > maxf {
        return Res::Overflow(maxf);
    }

    if f < -maxf {
        return Res::Overflow(-maxf);
    }
    Res::Ok(f)
}

// `overflow` returns an overflowed error.
#[macro_export]
macro_rules! overflow {
    ($val:ident, $bound:ident) => {{
        Err(box_err!("constant {} overflows {}", $val, $bound))
    }};
}

// `convert_uint_to_int` converts an uint value to an int value.
pub fn convert_uint_to_int(val: u64, upper_bound: i64, tp: u8) -> Result<i64> {
    if val > upper_bound as u64 {
        return overflow!(val, tp);
    }
    Ok(val as i64)
}

pub fn convert_float_to_int(fval: f64, lower_bound: i64, upper_bound: i64, tp: u8) -> Result<i64> {
    // TODO any performance problem to use round directly?
    let val = fval.round();
    if val < lower_bound as f64 {
        return overflow!(val, tp);
    }

    if val > upper_bound as f64 {
        return overflow!(val, tp);
    }
    Ok(val as i64)
}

pub fn convert_float_to_uint(fval: f64, upper_bound: u64, tp: u8) -> Result<u64> {
    // TODO any performance problem to use round directly?
    let val = fval.round();
    if val < 0f64 {
        return overflow!(val, tp);
    }

    if val > upper_bound as f64 {
        return overflow!(val, tp);
    }
    Ok(val as u64)
}

/// `bytes_to_int_without_context` converts a byte arrays to an i64
/// in best effort, but without context.
pub fn bytes_to_int_without_context(bytes: &[u8]) -> Result<i64> {
    // trim
    let mut trimed = bytes.iter().skip_while(|&&b| b == b' ' || b == b'\t');
    let mut negative = false;
    let mut r = Some(0i64);
    if let Some(&c) = trimed.next() {
        if c == b'-' {
            negative = true;
        } else if c >= b'0' && c <= b'9' {
            r = Some(i64::from(c) - i64::from(b'0'));
        } else if c != b'+' {
            return Ok(0);
        }

        for c in trimed.take_while(|&&c| c >= b'0' && c <= b'9') {
            let cur = i64::from(*c - b'0');
            r = r.and_then(|r| r.checked_mul(10)).and_then(|r| {
                if negative {
                    r.checked_sub(cur)
                } else {
                    r.checked_add(cur)
                }
            });

            if r.is_none() {
                break;
            }
        }
    }
    r.ok_or_else(|| Error::overflow("BIGINT", ""))
}

/// `bytes_to_uint_without_context` converts a byte arrays to an iu64
/// in best effort, but without context.
pub fn bytes_to_uint_without_context(bytes: &[u8]) -> Result<u64> {
    // trim
    let mut trimed = bytes.iter().skip_while(|&&b| b == b' ' || b == b'\t');
    let mut r = Some(0u64);
    if let Some(&c) = trimed.next() {
        if c >= b'0' && c <= b'9' {
            r = Some(u64::from(c) - u64::from(b'0'));
        } else if c != b'+' {
            return Ok(0);
        }

        for c in trimed.take_while(|&&c| c >= b'0' && c <= b'9') {
            r = r
                .and_then(|r| r.checked_mul(10))
                .and_then(|r| r.checked_add(u64::from(*c - b'0')));
            if r.is_none() {
                break;
            }
        }
    }
    r.ok_or_else(|| Error::overflow("BIGINT UNSIGNED", ""))
}

/// `bytes_to_int` converts a byte arrays to an i64 in best effort.
pub fn bytes_to_int(ctx: &mut EvalContext, bytes: &[u8]) -> Result<i64> {
    let s = str::from_utf8(bytes)?.trim();
    let vs = get_valid_int_prefix(ctx, s)?;
    bytes_to_int_without_context(vs.as_bytes()).map_err(|_| Error::overflow("BIGINT", &vs))
}

/// `bytes_to_uint` converts a byte arrays to an u64 in best effort.
pub fn bytes_to_uint(ctx: &mut EvalContext, bytes: &[u8]) -> Result<u64> {
    let s = str::from_utf8(bytes)?.trim();
    let vs = get_valid_int_prefix(ctx, s)?;
    bytes_to_uint_without_context(vs.as_bytes())
        .map_err(|_| Error::overflow("BIGINT UNSIGNED", &vs))
}

fn bytes_to_f64_without_context(bytes: &[u8]) -> Result<f64> {
    let f = match std::str::from_utf8(bytes) {
        Ok(s) => match s.trim().parse::<f64>() {
            Ok(f) => f,
            Err(e) => {
                error!("failed to parse float from {}: {}", s, e);
                0.0
            }
        },
        Err(e) => {
            error!("failed to convert bytes to str: {:?}", e);
            0.0
        }
    };
    Ok(f)
}

/// `bytes_to_f64` converts a byte array to a float64 in best effort.
pub fn bytes_to_f64(ctx: &mut EvalContext, bytes: &[u8]) -> Result<f64> {
    let s = str::from_utf8(bytes)?.trim();
    let vs = get_valid_float_prefix(ctx, s)?;

    bytes_to_f64_without_context(vs.as_bytes())
}

fn get_valid_int_prefix<'a>(ctx: &mut EvalContext, s: &'a str) -> Result<Cow<'a, str>> {
    let vs = get_valid_float_prefix(ctx, s)?;
    float_str_to_int_string(vs)
}

fn get_valid_float_prefix<'a>(ctx: &mut EvalContext, s: &'a str) -> Result<&'a str> {
    let mut saw_dot = false;
    let mut saw_digit = false;
    let mut valid_len = 0;
    let mut e_idx = 0;
    for (i, c) in s.chars().enumerate() {
        if c == '+' || c == '-' {
            if i != 0 && i != e_idx + 1 {
                // "1e+1" is valid.
                break;
            }
        } else if c == '.' {
            if saw_dot || e_idx > 0 {
                // "1.1." or "1e1.1"
                break;
            }
            saw_dot = true;
            if saw_digit {
                // "123." is valid.
                valid_len = i + 1;
            }
        } else if c == 'e' || c == 'E' {
            if !saw_digit {
                // "+.e"
                break;
            }
            if e_idx != 0 {
                // "1e5e"
                break;
            }
            e_idx = i
        } else if c < '0' || c > '9' {
            break;
        } else {
            saw_digit = true;
            valid_len = i + 1;
        }
    }
    box_try!(ctx.handle_truncate(valid_len < s.len()));
    if valid_len == 0 {
        Ok("0")
    } else {
        Ok(&s[..valid_len])
    }
}

/// It converts a valid float string into valid integer string which can be
/// parsed by `i64::from_str`, we can't parse float first then convert it to string
/// because precision will be lost.
fn float_str_to_int_string<'a, 'b: 'a>(valid_float: &'b str) -> Result<Cow<'a, str>> {
    let mut dot_idx = None;
    let mut e_idx = None;
    let mut int_cnt: i64 = 0;
    let mut digits_cnt: i64 = 0;

    for (i, c) in valid_float.chars().enumerate() {
        match c {
            '.' => dot_idx = Some(i),
            'e' | 'E' => e_idx = Some(i),
            '0'...'9' => if e_idx.is_none() {
                if dot_idx.is_none() {
                    int_cnt += 1;
                }
                digits_cnt += 1;
            },
            _ => (),
        }
    }

    if dot_idx.is_none() && e_idx.is_none() {
        return Ok(Cow::Borrowed(valid_float));
    }

    if e_idx.is_none() {
        if int_cnt == 0 {
            return Ok(Cow::Borrowed("0"));
        }
        return Ok(Cow::Borrowed(&valid_float[..dot_idx.unwrap()]));
    }

    let exp = box_try!((&valid_float[e_idx.unwrap() + 1..]).parse::<i64>());
    if exp > 0 && int_cnt > (i64::MAX - exp) {
        // (exp + inc_cnt) overflows MaxInt64.
        // TODO: refactor errors
        return Err(box_err!("[1264] Data Out of Range"));
    }
    if int_cnt + exp <= 0 {
        return Ok(Cow::Borrowed("0"));
    }

    let mut valid_int = String::from(&valid_float[..e_idx.unwrap()]);
    if dot_idx.is_some() {
        valid_int.remove(dot_idx.unwrap());
    }

    let extra_zero_count = exp + int_cnt - digits_cnt;
    if extra_zero_count > MAX_ZERO_COUNT {
        // Return overflow to avoid allocating too much memory.
        // TODO: refactor errors
        return Err(box_err!("[1264] Data Out of Range"));
    }

    if extra_zero_count >= 0 {
        valid_int.extend((0..extra_zero_count).map(|_| '0'));
    } else {
        let len = valid_int.len();
        if extra_zero_count >= len as i64 {
            return Ok(Cow::Borrowed("0"));
        }
        valid_int.truncate((len as i64 + extra_zero_count) as usize);
    }

    Ok(Cow::Owned(valid_int))
}

const MAX_ZERO_COUNT: i64 = 20;

#[cfg(test)]
mod test {
    use std::f64::EPSILON;
    use std::sync::Arc;
    use std::{f64, i64, isize, u64};

    use coprocessor::codec::mysql::types;
    use coprocessor::dag::expr::{EvalConfig, EvalContext, FLAG_IGNORE_TRUNCATE};

    use super::*;

    #[test]
    fn test_bytes_to_i64() {
        let tests: Vec<(&'static [u8], i64)> = vec![
            (b"0", 0),
            (b" 23a", 23),
            (b"\t 23a", 23),
            (b"\r23a", 0),
            (b"1", 1),
            (b"2.1", 2),
            (b"23e10", 23),
            (b"ab", 0),
            (b"4a", 4),
            (b"+1024", 1024),
            (b"-231", -231),
            (b"", 0),
            (b"9223372036854775807", i64::MAX),
            (b"-9223372036854775808", i64::MIN),
        ];

        for (bs, n) in tests {
            let t = super::bytes_to_int_without_context(bs).unwrap();
            if t != n {
                panic!("expect convert {:?} to {}, but got {}", bs, n, t);
            }
        }

        let invalid_cases: Vec<&'static [u8]> =
            vec![b"9223372036854775809", b"-9223372036854775810"];
        for bs in invalid_cases {
            match super::bytes_to_int_without_context(bs) {
                Err(e) => assert!(e.is_overflow()),
                res => panic!("expect convert {:?} to overflow, but got {:?}", bs, res),
            };
        }
    }

    #[test]
    fn test_bytes_to_u64() {
        let tests: Vec<(&'static [u8], u64)> = vec![
            (b"0", 0),
            (b" 23a", 23),
            (b"\t 23a", 23),
            (b"\r23a", 0),
            (b"1", 1),
            (b"2.1", 2),
            (b"23e10", 23),
            (b"ab", 0),
            (b"4a", 4),
            (b"+1024", 1024),
            (b"231", 231),
            (b"18446744073709551615", u64::MAX),
        ];

        for (bs, n) in tests {
            let t = super::bytes_to_uint_without_context(bs).unwrap();
            if t != n {
                panic!("expect convert {:?} to {}, but got {}", bs, n, t);
            }
        }

        let invalid_cases: Vec<&'static [u8]> = vec![b"18446744073709551616"];
        for bs in invalid_cases {
            match super::bytes_to_uint_without_context(bs) {
                Err(e) => assert!(e.is_overflow()),
                res => panic!("expect convert {:?} to overflow, but got {:?}", bs, res),
            };
        }
    }

    #[test]
    fn test_bytes_to_f64() {
        let tests: Vec<(&'static [u8], f64)> = vec![
            (b"", 0.0),
            (b" 23", 23.0),
            (b"-1", -1.0),
            (b"1.11", 1.11),
            (b"1.11.00", 0.0),
            (b"xx", 0.0),
            (b"0x00", 0.0),
            (b"11.xx", 0.0),
            (b"xx.11", 0.0),
        ];

        for (v, f) in tests {
            let ff = super::bytes_to_f64_without_context(v).unwrap();
            if (ff - f).abs() > EPSILON {
                panic!("{:?} should be decode to {}, but got {}", v, f, ff);
            }
        }
    }

    #[test]
    fn test_get_valid_float_prefix() {
        let cases = vec![
            ("-100", "-100"),
            ("1abc", "1"),
            ("-1-1", "-1"),
            ("+1+1", "+1"),
            ("123..34", "123."),
            ("123.23E-10", "123.23E-10"),
            ("1.1e1.3", "1.1e1"),
            ("11e1.3", "11e1"),
            ("1.1e-13a", "1.1e-13"),
            ("1.", "1."),
            (".1", ".1"),
            ("", "0"),
            ("123e+", "123"),
            ("123.e", "123."),
        ];

        let cfg = EvalConfig::new(0, FLAG_IGNORE_TRUNCATE).unwrap();
        let mut ctx = EvalContext::new(Arc::new(cfg));
        for (i, o) in cases {
            assert_eq!(super::get_valid_float_prefix(&mut ctx, i).unwrap(), o);
        }
    }

    #[test]
    fn test_invalid_get_valid_int_prefix() {
        let cases = vec!["1e21", "1e9223372036854775807"];

        for i in cases {
            let o = super::float_str_to_int_string(i);
            assert!(o.is_err());
        }
    }

    #[test]
    fn test_valid_get_valid_int_prefix() {
        let cases = vec![
            (".1", "0"),
            (".0", "0"),
            ("123", "123"),
            ("123e1", "1230"),
            ("123.1e2", "12310"),
            ("123.45e5", "12345000"),
            ("123.45678e5", "12345678"),
            ("123.456789e5", "12345678"),
            ("-123.45678e5", "-12345678"),
            ("+123.45678e5", "+12345678"),
        ];

        for (i, e) in cases {
            let o = super::float_str_to_int_string(i);
            assert_eq!(o.unwrap(), *e);
        }
    }

    #[test]
    fn test_convert_uint_into_int() {
        assert!(convert_uint_to_int(u64::MAX, i64::MAX, types::LONG_LONG).is_err());
        let v = convert_uint_to_int(u64::MIN, i64::MAX, types::LONG_LONG).unwrap();
        assert_eq!(v, u64::MIN as i64);
        // TODO port tests from tidb(tidb haven't implemented now)
    }

    #[test]
    fn test_convert_float_to_int() {
        assert!(convert_float_to_int(f64::MIN, i64::MIN, i64::MAX, types::DOUBLE).is_err());
        assert!(convert_float_to_int(f64::MAX, i64::MIN, i64::MAX, types::DOUBLE).is_err());
        let v = convert_float_to_int(0.1, i64::MIN, i64::MAX, types::DOUBLE).unwrap();
        assert_eq!(v, 0);
        // TODO port tests from tidb(tidb haven't implemented now)
    }

    #[test]
    fn test_convert_float_to_uint() {
        assert!(convert_float_to_uint(f64::MIN, u64::MAX, types::DOUBLE).is_err());
        assert!(convert_float_to_uint(f64::MAX, u64::MAX, types::DOUBLE).is_err());
        let v = convert_float_to_uint(0.1, u64::MAX, types::DOUBLE).unwrap();
        assert_eq!(v, 0);
        // TODO port tests from tidb(tidb haven't implemented now)
    }

    #[test]
    fn test_truncate_binary() {
        let s = b"123456789".to_vec();
        let mut s1 = s.clone();
        truncate_binary(&mut s1, UNSPECIFIED_LENGTH as isize);
        assert_eq!(s1, s);
        let mut s2 = s.clone();
        truncate_binary(&mut s2, isize::MAX);
        assert_eq!(s2, s);
        let mut s3 = s;
        truncate_binary(&mut s3, 0);
        assert!(s3.is_empty());
        // TODO port tests from tidb(tidb haven't implemented now)
    }

    #[test]
    fn test_truncate_f64() {
        let cases = vec![
            (100.114, 10, 2, Res::Ok(100.11)),
            (100.115, 10, 2, Res::Ok(100.12)),
            (100.1156, 10, 3, Res::Ok(100.116)),
            (100.1156, 3, 1, Res::Overflow(99.9)),
            (1.36, 10, 2, Res::Ok(1.36)),
            (f64::NAN, 10, 1, Res::Overflow(0f64)),
        ];
        for (f, flen, decimal, exp) in cases {
            let res = truncate_f64(f, flen, decimal);
            assert_eq!(res, exp);
        }
    }
}
