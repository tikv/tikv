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

use std::{self, str};

use util::xeval::EvalContext;
use super::Result;

/// `bytes_to_int_without_context` converts a byte arrays to an i64
/// in best effort, but without context.
/// TODO: handle overflow.
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
/// TODO: handle overflow.
pub fn bytes_to_int(ctx: &EvalContext, bytes: &[u8]) -> Result<i64> {
    let s = try!(str::from_utf8(bytes)).trim();
    let vs = get_valid_int_prefix(s);
    try!(handle_truncate(ctx, s.len() > vs.len()));

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
    let vs = get_valid_float_prefix(s);
    try!(handle_truncate(ctx, s.len() > vs.len()));

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

// TODO: prot `floatStrToIntStr`.
fn get_valid_int_prefix(s: &str) -> &str {
    get_valid_float_prefix(s)
}

fn get_valid_float_prefix(s: &str) -> &str {
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
            if saw_dot {
                // "1.1."
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

    if valid_len == 0 { "0" } else { &s[..valid_len] }
}

#[cfg(test)]
mod test {
    use std::f64::EPSILON;
    use chrono::FixedOffset;
    use util::xeval::EvalContext;

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
                         ("1.1e-13a", "1.1e-13"),
                         ("1.", "1."),
                         (".1", ".1"),
                         ("", "0"),
                         ("123e+", "123"),
                         ("123.e", "123.")];

        for (i, o) in cases {
            assert_eq!(super::get_valid_float_prefix(i), o);
        }
    }
}
