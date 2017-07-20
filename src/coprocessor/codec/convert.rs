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

use std::{self, str, i64};
use std::borrow::Cow;

use coprocessor::xeval::EvalContext;
use super::Result;

/// `bytes_to_int_without_context` converts a byte arrays to an i64
/// in best effort, but without context.
/// Note that it does NOT handle overflow.
pub fn bytes_to_int_without_context(bytes: &[u8]) -> Result<i64> {
    // trim
    let mut trimed = bytes.iter().skip_while(|&&b| b == b' ' || b == b'\t');
    let mut negative = false;
    let mut r = 0i64;
    if let Some(&c) = trimed.next() {
        if c == b'-' {
            negative = true;
        } else if c >= b'0' && c <= b'9' {
            r = c as i64 - b'0' as i64;
        } else if c != b'+' {
            return Ok(0);
        }

        r = trimed.take_while(|&&c| c >= b'0' && c <= b'9')
            .fold(r, |l, &r| l * 10 + (r - b'0') as i64);
        if negative {
            r = -r;
        }
    }
    Ok(r)
}

/// `bytes_to_int` converts a byte arrays to an i64 in best effort.
/// TODO: handle overflow properly.
pub fn bytes_to_int(ctx: &EvalContext, bytes: &[u8]) -> Result<i64> {
    let s = try!(str::from_utf8(bytes)).trim();
    let vs = try!(get_valid_int_prefix(ctx, s));

    bytes_to_int_without_context(vs.as_bytes())
}

fn bytes_to_f64_without_context(bytes: &[u8]) -> Result<f64> {
    let f = match std::str::from_utf8(bytes) {
        Ok(s) => {
            match s.trim().parse::<f64>() {
                Ok(f) => f,
                Err(e) => {
                    error!("failed to parse float from {}: {}", s, e);
                    0.0
                }
            }
        }
        Err(e) => {
            error!("failed to convert bytes to str: {:?}", e);
            0.0
        }
    };
    Ok(f)
}

/// `bytes_to_f64` converts a byte array to a float64 in best effort.
pub fn bytes_to_f64(ctx: &EvalContext, bytes: &[u8]) -> Result<f64> {
    let s = try!(str::from_utf8(bytes)).trim();
    let vs = try!(get_valid_float_prefix(ctx, s));

    bytes_to_f64_without_context(vs.as_bytes())
}

#[inline]
fn handle_truncate(ctx: &EvalContext, is_truncated: bool) -> Result<()> {
    if is_truncated && !(ctx.ignore_truncate || ctx.truncate_as_warning) {
        Err(box_err!("[1265] Data Truncated"))
    } else {
        Ok(())
    }
}

fn get_valid_int_prefix<'a>(ctx: &EvalContext, s: &'a str) -> Result<Cow<'a, str>> {
    let vs = try!(get_valid_float_prefix(ctx, s));
    float_str_to_int_string(vs)
}

fn get_valid_float_prefix<'a>(ctx: &EvalContext, s: &'a str) -> Result<&'a str> {
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

    try!(handle_truncate(ctx, valid_len < s.len()));
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
            '0'...'9' => {
                if e_idx.is_none() {
                    if dot_idx.is_none() {
                        int_cnt += 1;
                    }
                    digits_cnt += 1;
                }
            }
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

    use chrono::FixedOffset;

    use coprocessor::xeval::EvalContext;

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
        ];

        for (bs, n) in tests {
            let t = super::bytes_to_int_without_context(bs).unwrap();
            if t != n {
                panic!("expect convert {:?} to {}, but got {}", bs, n, t);
            }
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
    fn test_handle_truncate() {
        let ctxs = vec![EvalContext {
                            tz: FixedOffset::east(0),
                            ignore_truncate: true,
                            truncate_as_warning: true,
                        },
                        EvalContext {
                            tz: FixedOffset::east(0),
                            ignore_truncate: true,
                            truncate_as_warning: false,
                        },
                        EvalContext {
                            tz: FixedOffset::east(0),
                            ignore_truncate: false,
                            truncate_as_warning: true,
                        },
                        EvalContext {
                            tz: FixedOffset::east(0),
                            ignore_truncate: false,
                            truncate_as_warning: false,
                        }];

        for ctx in &ctxs {
            assert!(super::handle_truncate(ctx, false).is_ok());
        }

        assert!(super::handle_truncate(&ctxs[0], true).is_ok());
        assert!(super::handle_truncate(&ctxs[1], true).is_ok());
        assert!(super::handle_truncate(&ctxs[2], true).is_ok());
        assert!(super::handle_truncate(&ctxs[3], true).is_err());
    }

    #[test]
    fn test_get_valid_float_prefix() {
        let cases = vec![("-100", "-100"),
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
                         ("123.e", "123.")];

        let ctx = EvalContext {
            tz: FixedOffset::east(0),
            ignore_truncate: true,
            truncate_as_warning: false,
        };
        for (i, o) in cases {
            assert_eq!(super::get_valid_float_prefix(&ctx, i).unwrap(), o);
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
        let cases = vec![(".1", "0"),
                         (".0", "0"),
                         ("123", "123"),
                         ("123e1", "1230"),
                         ("123.1e2", "12310"),
                         ("123.45e5", "12345000"),
                         ("123.45678e5", "12345678"),
                         ("123.456789e5", "12345678"),
                         ("-123.45678e5", "-12345678"),
                         ("+123.45678e5", "+12345678")];

        for (i, e) in cases {
            let o = super::float_str_to_int_string(i);
            assert_eq!(o.unwrap(), *e);
        }
    }
}
