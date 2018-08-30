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
use std::net::{IpAddr, Ipv4Addr, Ipv6Addr};
use std::str;
use std::str::FromStr;

impl ScalarFunc {
    pub fn is_ipv4(&self, ctx: &mut EvalContext, row: &[Datum]) -> Result<Option<i64>> {
        let input = try_opt!(self.children[0].eval_string_and_decode(ctx, row));
        if is_given_string_ipv4(&input) {
            Ok(Some(1))
        } else {
            Ok(Some(0))
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
}

fn is_given_string_ipv4(some_str: &str) -> bool {
    let address = IpAddr::from_str(some_str);
    if let Ok(add) = address {
        return add.is_ipv4();
    }
    false
}

#[cfg(test)]
mod test {
    use super::is_given_string_ipv4;
    use coprocessor::codec::Datum;
    use coprocessor::dag::expr::test::{datum_expr, scalar_func_expr};
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
            let op = Expression::build(&mut ctx, op).unwrap();
            let got = op.eval(&mut ctx, &[]).unwrap();
            let exp = Datum::from(expected);
            assert_eq!(got, exp);
        }
    }

    #[test]
    fn test_is_given_string_ipv4_success() {
        let is_ipv4 = is_given_string_ipv4("127.0.0.1");
        assert_eq!(is_ipv4, true);
    }

    #[test]
    fn test_is_given_string_ipv4_fail() {
        let is_ipv4 = is_given_string_ipv4("A.123.a.X");
        assert_eq!(is_ipv4, false);
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
            let op = Expression::build(&mut ctx, op).unwrap();
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
            let op = Expression::build(&mut ctx, op).unwrap();
            let got = op.eval(&mut ctx, &[]).unwrap();
            assert_eq!(got, exp);
        }
    }
}
