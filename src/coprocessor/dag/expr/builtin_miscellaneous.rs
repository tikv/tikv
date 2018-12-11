// Copyright 2017 PingCAP, Inc.
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

use super::{EvalContext, Result, ScalarFunc};
use coprocessor::codec::Datum;
use std::borrow::Cow;
use std::convert::TryInto;
use std::net::{Ipv4Addr, Ipv6Addr};
use std::str::FromStr;

impl ScalarFunc {
    pub fn is_ipv4(&self, ctx: &mut EvalContext, row: &[Datum]) -> Result<Option<i64>> {
        let input = try_opt!(self.children[0].eval_string_and_decode(ctx, row));
        if Ipv4Addr::from_str(&input).is_ok() {
            Ok(Some(1))
        } else {
            Ok(Some(0))
        }
    }

    pub fn inet_aton<'a, 'b: 'a>(
        &'b self,
        ctx: &mut EvalContext,
        row: &'a [Datum],
    ) -> Result<Option<i64>> {
        let input = try_opt!(self.children[0].eval_string_and_decode(ctx, row));
        if input.len() == 0 || input.ends_with('.') {
            return Ok(None);
        }
        let (mut byte_result, mut result, mut dot_count): (u64, u64, usize) = (0, 0, 0);
        for c in input.chars() {
            if c >= '0' && c <= '9' {
                let digit = c as u64 - '0' as u64;
                byte_result = byte_result * 10 + digit;
                if byte_result > 255 {
                    return Ok(None);
                }
            } else if c == '.' {
                dot_count += 1;
                if dot_count > 3 {
                    return Ok(None);
                }
                result = (result << 8) + byte_result;
                byte_result = 0;
            } else {
                return Ok(None);
            }
        }

        if dot_count == 1 {
            result <<= 16;
        } else if dot_count == 2 {
            result <<= 8;
        }
        Ok(Some(((result << 8) + byte_result) as i64))
    }

    pub fn inet_ntoa<'a, 'b: 'a>(
        &'b self,
        ctx: &mut EvalContext,
        row: &'a [Datum],
    ) -> Result<Option<Cow<'a, [u8]>>> {
        let input = try_opt!(self.children[0].eval_int(ctx, row));
        if input < 0 || input > i64::from(u32::max_value()) {
            Ok(None)
        } else {
            let v = input as u32;
            let ipv4_addr =
                Ipv4Addr::new((v >> 24) as u8, (v >> 16) as u8, (v >> 8) as u8, v as u8);
            Ok(Some(Cow::Owned(format!("{}", ipv4_addr).into_bytes())))
        }
    }

    pub fn inet6_aton<'a, 'b: 'a>(
        &'b self,
        ctx: &mut EvalContext,
        row: &'a [Datum],
    ) -> Result<Option<Cow<'a, [u8]>>> {
        let input = try_opt!(self.children[0].eval_string_and_decode(ctx, row));
        let ipv6_addr = Ipv6Addr::from_str(&input).map(|t| Some(Cow::Owned(t.octets().to_vec())));
        let ipv4_addr_eval =
            |_t| Ipv4Addr::from_str(&input).map(|t| Some(Cow::Owned(t.octets().to_vec())));
        ipv6_addr.or_else(ipv4_addr_eval).or(Ok(None))
    }

    pub fn inet6_ntoa<'a, 'b: 'a>(
        &'b self,
        ctx: &mut EvalContext,
        row: &'a [Datum],
    ) -> Result<Option<Cow<'a, [u8]>>> {
        let s = try_opt!(self.children[0].eval_string(ctx, row));
        if s.len() == 16 {
            let v: &[u8; 16] = s.as_ref().try_into().unwrap();
            Ok(Some(Cow::Owned(
                format!("{}", Ipv6Addr::from(*v)).into_bytes(),
            )))
        } else if s.len() == 4 {
            let v: &[u8; 4] = s.as_ref().try_into().unwrap();
            Ok(Some(Cow::Owned(
                format!("{}", Ipv4Addr::from(*v)).into_bytes(),
            )))
        } else {
            Ok(None)
        }
    }

    pub fn is_ipv6(&self, ctx: &mut EvalContext, row: &[Datum]) -> Result<Option<i64>> {
        let input = try_opt!(self.children[0].eval_string_and_decode(ctx, row));
        if Ipv6Addr::from_str(&input).is_ok() {
            Ok(Some(1))
        } else {
            Ok(Some(0))
        }
    }
}

#[cfg(test)]
mod tests {
    use coprocessor::codec::Datum;
    use coprocessor::dag::expr::tests::{datum_expr, scalar_func_expr};
    use coprocessor::dag::expr::{EvalContext, Expression};
    use tipb::expression::ScalarFuncSig;

    #[test]
    fn test_is_ipv4() {
        let cases = vec![
            // input, expected
            ("127.0.0.1", 1i64),
            ("127.0.0.256", 0i64),
        ];

        let mut ctx = EvalContext::default();
        for (input_str, expected) in cases {
            let input = datum_expr(Datum::Bytes(input_str.as_bytes().to_vec()));

            let op = scalar_func_expr(ScalarFuncSig::IsIPv4, &[input]);
            let op = Expression::build(&ctx, op).unwrap();
            let got = op.eval(&mut ctx, &[]).unwrap();
            let exp = Datum::from(expected);
            assert_eq!(got, exp);
        }
    }

    #[test]
    fn test_is_ipv6() {
        let cases = vec![
            // input, expected
            ("::1", 1i64),
            ("1:2:3:4:5:6:7:10000", 0i64),
        ];

        let mut ctx = EvalContext::default();
        for (input_str, expected) in cases {
            let input = datum_expr(Datum::Bytes(input_str.as_bytes().to_vec()));

            let op = scalar_func_expr(ScalarFuncSig::IsIPv6, &[input]);
            let op = Expression::build(&ctx, op).unwrap();
            let got = op.eval(&mut ctx, &[]).unwrap();
            let exp = Datum::from(expected);
            assert_eq!(got, exp);
        }
    }

    #[test]
    fn test_inet_aton() {
        let cases = vec![
            (Datum::Bytes(b"0.0.0.0".to_vec()), Datum::I64(0)),
            (
                Datum::Bytes(b"255.255.255.255".to_vec()),
                Datum::I64(4294967295),
            ),
            (Datum::Bytes(b"127.0.0.1".to_vec()), Datum::I64(2130706433)),
            (
                Datum::Bytes(b"113.14.22.3".to_vec()),
                Datum::I64(1896748547),
            ),
            (Datum::Bytes(b"1".to_vec()), Datum::I64(1)),
            (Datum::Bytes(b"0.1.2".to_vec()), Datum::I64(65538)),
            (Datum::Bytes(b"0.1.2.3.4".to_vec()), Datum::Null),
            (Datum::Bytes(b"0.1.2..3".to_vec()), Datum::Null),
            (Datum::Bytes(b".0.1.2.3".to_vec()), Datum::Null),
            (Datum::Bytes(b"0.1.2.3.".to_vec()), Datum::Null),
            (Datum::Bytes(b"1.-2.3.4".to_vec()), Datum::Null),
            (Datum::Bytes(b"".to_vec()), Datum::Null),
            (Datum::Bytes(b"0.0.0.256".to_vec()), Datum::Null),
            (Datum::Bytes(b"127.0.0,1".to_vec()), Datum::Null),
            (Datum::Null, Datum::Null),
        ];

        let mut ctx = EvalContext::default();
        for (input, exp) in cases {
            let input = datum_expr(input);
            let op = scalar_func_expr(ScalarFuncSig::InetAton, &[input]);
            let op = Expression::build(&ctx, op).unwrap();
            let got = op.eval(&mut ctx, &[]).unwrap();
            assert_eq!(got, exp);
        }
    }

    #[test]
    fn test_inet_ntoa() {
        let cases = vec![
            (Datum::I64(167773449), Datum::Bytes(b"10.0.5.9".to_vec())),
            (Datum::I64(2063728641), Datum::Bytes(b"123.2.0.1".to_vec())),
            (Datum::I64(0), Datum::Bytes(b"0.0.0.0".to_vec())),
            (Datum::I64(545460846593), Datum::Null),
            (Datum::I64(-1), Datum::Null),
            (
                Datum::I64(i64::from(u32::max_value())),
                Datum::Bytes(b"255.255.255.255".to_vec()),
            ),
            (Datum::Null, Datum::Null),
        ];

        let mut ctx = EvalContext::default();
        for (input, exp) in cases {
            let input = datum_expr(input);
            let op = scalar_func_expr(ScalarFuncSig::InetNtoa, &[input]);
            let op = Expression::build(&ctx, op).unwrap();
            let got = op.eval(&mut ctx, &[]).unwrap();
            assert_eq!(got, exp);
        }
    }

    #[test]
    fn test_inet6_aton() {
        let cases = vec![
            (
                Datum::Bytes(b"0.0.0.0".to_vec()),
                Datum::Bytes(vec![0x00, 0x00, 0x00, 0x00]),
            ),
            (
                Datum::Bytes(b"10.0.5.9".to_vec()),
                Datum::Bytes(vec![0x0A, 0x00, 0x05, 0x09]),
            ),
            (
                Datum::Bytes(b"::1.2.3.4".to_vec()),
                Datum::Bytes(vec![
                    0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01,
                    0x02, 0x03, 0x04,
                ]),
            ),
            (
                Datum::Bytes(b"::FFFF:1.2.3.4".to_vec()),
                Datum::Bytes(vec![
                    0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0xFF, 0xFF, 0x01,
                    0x02, 0x03, 0x04,
                ]),
            ),
            (
                Datum::Bytes(b"::fdfe:5a55:caff:fefa:9089".to_vec()),
                Datum::Bytes(vec![
                    0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0xFD, 0xFE, 0x5A, 0x55, 0xCA, 0xFF, 0xFE,
                    0xFA, 0x90, 0x89,
                ]),
            ),
            (
                Datum::Bytes(b"fdfe::5a55:caff:fefa:9089".to_vec()),
                Datum::Bytes(vec![
                    0xFD, 0xFE, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x5A, 0x55, 0xCA, 0xFF, 0xFE,
                    0xFA, 0x90, 0x89,
                ]),
            ),
            (
                Datum::Bytes(b"2001:0db8:85a3:0000:0000:8a2e:0370:7334".to_vec()),
                Datum::Bytes(vec![
                    0x20, 0x01, 0x0d, 0xb8, 0x85, 0xa3, 0x00, 0x00, 0x00, 0x00, 0x8a, 0x2e, 0x03,
                    0x70, 0x73, 0x34,
                ]),
            ),
            (Datum::Bytes(b"".to_vec()), Datum::Null),
            (Datum::Null, Datum::Null),
        ];

        let mut ctx = EvalContext::default();
        for (input, exp) in cases {
            let input = datum_expr(input);
            let op = scalar_func_expr(ScalarFuncSig::Inet6Aton, &[input]);
            let op = Expression::build(&ctx, op).unwrap();
            let got = op.eval(&mut ctx, &[]).unwrap();
            assert_eq!(got, exp);
        }
    }

    #[test]
    fn test_inet6_ntoa() {
        let cases = vec![
            (
                Datum::Bytes(vec![0x00, 0x00, 0x00, 0x00]),
                Datum::Bytes(b"0.0.0.0".to_vec()),
            ),
            (
                Datum::Bytes(vec![0x0A, 0x00, 0x05, 0x09]),
                Datum::Bytes(b"10.0.5.9".to_vec()),
            ),
            (
                Datum::Bytes(vec![
                    0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01,
                    0x02, 0x03, 0x04,
                ]),
                Datum::Bytes(b"::1.2.3.4".to_vec()),
            ),
            (
                Datum::Bytes(vec![
                    0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0xFF, 0xFF, 0x01,
                    0x02, 0x03, 0x04,
                ]),
                Datum::Bytes(b"::ffff:1.2.3.4".to_vec()),
            ),
            (
                Datum::Bytes(vec![
                    0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0xFD, 0xFE, 0x5A, 0x55, 0xCA, 0xFF, 0xFE,
                    0xFA, 0x90, 0x89,
                ]),
                Datum::Bytes(b"::fdfe:5a55:caff:fefa:9089".to_vec()),
            ),
            (
                Datum::Bytes(vec![
                    0xFD, 0xFE, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x5A, 0x55, 0xCA, 0xFF, 0xFE,
                    0xFA, 0x90, 0x89,
                ]),
                Datum::Bytes(b"fdfe::5a55:caff:fefa:9089".to_vec()),
            ),
            (
                Datum::Bytes(vec![
                    0x20, 0x01, 0x0d, 0xb8, 0x85, 0xa3, 0x12, 0x34, 0x56, 0x78, 0x8a, 0x2e, 0x03,
                    0x70, 0x73, 0x34,
                ]),
                Datum::Bytes(b"2001:db8:85a3:1234:5678:8a2e:370:7334".to_vec()),
            ),
            // missing bytes
            (Datum::Bytes(b"".to_vec()), Datum::Null),
            // missing a byte ipv4
            (Datum::Bytes(vec![0x20, 0x01, 0x0d]), Datum::Null),
            // missing a byte ipv6
            (
                Datum::Bytes(vec![
                    0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0xFF, 0xFF, 0xFF,
                    0xFF, 0xFF,
                ]),
                Datum::Null,
            ),
            (Datum::Null, Datum::Null),
        ];

        let mut ctx = EvalContext::default();
        for (input, exp) in cases {
            let input = datum_expr(input);
            let op = scalar_func_expr(ScalarFuncSig::Inet6Ntoa, &[input]);
            let op = Expression::build(&ctx, op).unwrap();
            let got = op.eval(&mut ctx, &[]).unwrap();
            assert_eq!(got, exp);
        }
    }
}
