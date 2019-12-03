// Copyright 2017 TiKV Project Authors. Licensed under Apache-2.0.

use super::{EvalContext, Result, ScalarFunc};
use crate::codec::data_type::Duration;
use crate::codec::mysql::{Decimal, Json, Time};
use crate::codec::Datum;
use crate::expr_util;

use std::borrow::Cow;
use std::convert::TryFrom;
use std::convert::TryInto;
use std::net::{Ipv4Addr, Ipv6Addr};
use std::str::FromStr;

const IPV6_LENGTH: usize = 16;
const IPV4_LENGTH: usize = 4;
const PREFIX_COMPAT: [u8; 12] = [0x00; 12];

impl ScalarFunc {
    pub fn int_any_value(&self, ctx: &mut EvalContext, row: &[Datum]) -> Result<Option<i64>> {
        self.children
            .first()
            .map_or(Ok(None), |child| child.eval_int(ctx, row))
    }

    pub fn real_any_value(&self, ctx: &mut EvalContext, row: &[Datum]) -> Result<Option<f64>> {
        self.children
            .first()
            .map_or(Ok(None), |child| child.eval_real(ctx, row))
    }

    pub fn string_any_value<'a, 'b: 'a>(
        &'b self,
        ctx: &mut EvalContext,
        row: &'a [Datum],
    ) -> Result<Option<Cow<'a, [u8]>>> {
        self.children
            .first()
            .map_or(Ok(None), |child| child.eval_string(ctx, row))
    }

    pub fn time_any_value<'a, 'b: 'a>(
        &'b self,
        ctx: &mut EvalContext,
        row: &'a [Datum],
    ) -> Result<Option<Cow<'a, Time>>> {
        self.children
            .first()
            .map_or(Ok(None), |child| child.eval_time(ctx, row))
    }

    pub fn decimal_any_value<'a, 'b: 'a>(
        &'b self,
        ctx: &mut EvalContext,
        row: &'a [Datum],
    ) -> Result<Option<Cow<'a, Decimal>>> {
        self.children
            .first()
            .map_or(Ok(None), |child| child.eval_decimal(ctx, row))
    }

    pub fn json_any_value<'a, 'b: 'a>(
        &'b self,
        ctx: &mut EvalContext,
        row: &'a [Datum],
    ) -> Result<Option<Cow<'a, Json>>> {
        self.children
            .first()
            .map_or(Ok(None), |child| child.eval_json(ctx, row))
    }

    pub fn duration_any_value<'a, 'b: 'a>(
        &'b self,
        ctx: &mut EvalContext,
        row: &'a [Datum],
    ) -> Result<Option<Duration>> {
        self.children
            .first()
            .map_or(Ok(None), |child| child.eval_duration(ctx, row))
    }

    pub fn is_ipv4(&self, ctx: &mut EvalContext, row: &[Datum]) -> Result<Option<i64>> {
        let input = try_opt!(self.children[0].eval_string_and_decode(ctx, row));
        if Ipv4Addr::from_str(&input).is_ok() {
            Ok(Some(1))
        } else {
            Ok(Some(0))
        }
    }

    pub fn is_ipv4_compat<'a, 'b: 'a>(
        &'b self,
        ctx: &mut EvalContext,
        row: &'a [Datum],
    ) -> Result<Option<i64>> {
        let input = try_opt!(self.children[0].eval_string(ctx, row));
        // Not an IPv6 address, return 0
        if input.len() != IPV6_LENGTH {
            return Ok(Some(0));
        }

        if !input.starts_with(&PREFIX_COMPAT) {
            return Ok(Some(0));
        }
        Ok(Some(1))
    }

    pub fn is_ipv4_mapped<'a, 'b: 'a>(
        &'b self,
        ctx: &mut EvalContext,
        row: &'a [Datum],
    ) -> Result<Option<i64>> {
        let input = try_opt!(self.children[0].eval_string(ctx, row));
        // Not an IPv6 address, return 0
        if input.len() != IPV6_LENGTH {
            return Ok(Some(0));
        }

        let mut prefix_mapped: [u8; 12] = [0x00; 12];
        prefix_mapped[10] = 0xff;
        prefix_mapped[11] = 0xff;
        if !input.starts_with(&prefix_mapped) {
            return Ok(Some(0));
        }
        Ok(Some(1))
    }

    pub fn inet_aton<'a, 'b: 'a>(
        &'b self,
        ctx: &mut EvalContext,
        row: &'a [Datum],
    ) -> Result<Option<i64>> {
        let addr = try_opt!(self.children[0].eval_string_and_decode(ctx, row));
        Ok(expr_util::miscellaneous::inet_aton(addr))
    }

    pub fn inet_ntoa<'a, 'b: 'a>(
        &'b self,
        ctx: &mut EvalContext,
        row: &'a [Datum],
    ) -> Result<Option<Cow<'a, [u8]>>> {
        let input = try_opt!(self.children[0].eval_int(ctx, row));
        Ok(u32::try_from(input)
            .ok()
            .map(|input| Cow::Owned(format!("{}", Ipv4Addr::from(input)).into_bytes())))
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
        if s.len() == IPV6_LENGTH {
            let v: &[u8; 16] = s.as_ref().try_into().unwrap();
            Ok(Some(Cow::Owned(
                format!("{}", Ipv6Addr::from(*v)).into_bytes(),
            )))
        } else if s.len() == IPV4_LENGTH {
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
    use crate::codec::data_type::Duration;
    use crate::codec::mysql::json::Json;
    use crate::codec::mysql::Decimal;
    use crate::codec::mysql::Time;
    use crate::codec::Datum;
    use crate::expr::tests::{datum_expr, scalar_func_expr};
    use crate::expr::{EvalContext, Expression};
    use tipb::ScalarFuncSig;

    macro_rules! test_any_value {
        ($cases: expr, $case_type: ty, Datum::$type_of_datum: ident, $maker_for_case_ele: expr, ScalarFuncSig::$sig_of_scalar_func: ident) => {{
            let cases: $case_type = $cases;

            let mut ctx = EvalContext::default();
            for (n1, n2, n3, n4, expected) in cases {
                let input1 = datum_expr(Datum::$type_of_datum($maker_for_case_ele(n1)));
                let input2 = datum_expr(Datum::$type_of_datum($maker_for_case_ele(n2)));
                let input3 = datum_expr(Datum::$type_of_datum($maker_for_case_ele(n3)));
                let input4 = datum_expr(Datum::$type_of_datum($maker_for_case_ele(n4)));

                let op = scalar_func_expr(
                    ScalarFuncSig::$sig_of_scalar_func,
                    &[input1, input2, input3, input4],
                );
                let op = Expression::build(&mut ctx, op).unwrap();
                let got = op.eval(&mut ctx, &[]).unwrap();
                let exp = Datum::from($maker_for_case_ele(expected));
                assert_eq!(got, exp);
            }
            let op = scalar_func_expr(ScalarFuncSig::$sig_of_scalar_func, &[]);
            let op = Expression::build(&mut ctx, op).unwrap();
            let got = op.eval(&mut ctx, &[]);
            match got {
                Ok(x) => assert_eq!(x, Datum::Null),
                _ => panic!("test failed, expect Ok(Datum::Null)"),
            }
        }};
    }

    #[test]
    fn test_int_any_value() {
        test_any_value!(
            vec![(1i64, 2i64, 3i64, 4i64, 1i64)],
            Vec<(i64, i64, i64, i64, i64)>,
            Datum::I64,
            |x| x,
            ScalarFuncSig::IntAnyValue
        );
    }

    #[test]
    fn test_real_any_value() {
        test_any_value!(
            vec![(1.2f64, 2.3f64, 3f64, 4f64, 1.2f64)],
            Vec<(f64, f64, f64, f64, f64)>,
            Datum::F64,
            |x| x,
            ScalarFuncSig::RealAnyValue
        );
    }

    #[test]
    fn test_string_any_value() {
        test_any_value!(
            vec![("abc", "def", "ojk", "hij", "abc")],
            Vec<(&str, &str, &str, &str, &str)>,
            Datum::Bytes,
            |x: &str| x.as_bytes().to_vec(),
            ScalarFuncSig::StringAnyValue
        );
    }

    #[test]
    fn test_duration_any_value() {
        test_any_value!(
            vec![(
                Duration::from_millis(10, 0).unwrap(),
                Duration::from_millis(11, 0).unwrap(),
                Duration::from_millis(12, 0).unwrap(),
                Duration::from_millis(13, 0).unwrap(),
                Duration::from_millis(10, 0).unwrap(),
            )],
            Vec<(Duration, Duration, Duration, Duration, Duration)>,
            Datum::Dur,
            |x| x,
            ScalarFuncSig::DurationAnyValue
        );
    }

    #[test]
    fn test_json_any_value() {
        test_any_value!(
            vec![(
                Json::U64(1),
                Json::U64(2),
                Json::U64(3),
                Json::U64(4),
                Json::U64(1),
            )],
            Vec<(Json, Json, Json, Json, Json)>,
            Datum::Json,
            |x| x,
            ScalarFuncSig::JsonAnyValue
        );
    }

    #[test]
    fn test_time_any_value() {
        let mut ctx = EvalContext::default();
        test_any_value!(
            vec![(
                Time::parse_datetime(&mut ctx, "1000-01-01 00:00:00", 0, false).unwrap(),
                Time::parse_datetime(&mut ctx, "1000-01-01 00:00:01", 0, false).unwrap(),
                Time::parse_datetime(&mut ctx, "1000-01-01 00:00:02", 0, false).unwrap(),
                Time::parse_datetime(&mut ctx, "1000-01-01 00:00:03", 0, false).unwrap(),
                Time::parse_datetime(&mut ctx, "1000-01-01 00:00:00", 0, false).unwrap(),
            )],
            Vec<(Time, Time, Time, Time, Time)>,
            Datum::Time,
            |x| x,
            ScalarFuncSig::TimeAnyValue
        );
    }

    #[test]
    fn test_decimal_any_value() {
        test_any_value!(
            vec![(10.into(), 20.into(), 30.into(), 40.into(), 10.into())],
            Vec<(Decimal, Decimal, Decimal, Decimal, Decimal)>,
            Datum::Dec,
            |x| x,
            ScalarFuncSig::DecimalAnyValue
        );
    }

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
    fn test_is_ipv4_compat() {
        let cases = vec![
            (Datum::Bytes(vec![]), Datum::I64(0)),
            (Datum::Bytes(vec![0x10, 0x10, 0x10, 0x10]), Datum::I64(0)),
            (
                Datum::Bytes(vec![
                    0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x1, 0x2, 0x3, 0x4,
                ]),
                Datum::I64(1),
            ),
            (
                Datum::Bytes(vec![
                    0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x1, 0x0, 0x0, 0x1, 0x2, 0x3, 0x4,
                ]),
                Datum::I64(0),
            ),
            (
                Datum::Bytes(vec![
                    0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x1, 0xff, 0xff, 0x1, 0x2, 0x3,
                    0x4,
                ]),
                Datum::I64(0),
            ),
            (
                Datum::Bytes(vec![0x0, 0x1, 0x2, 0x3, 0x4, 0x5, 0x6]),
                Datum::I64(0),
            ),
        ];

        let mut ctx = EvalContext::default();
        for (input, exp) in cases {
            let input = datum_expr(input);
            let op = scalar_func_expr(ScalarFuncSig::IsIPv4Compat, &[input]);
            let op = Expression::build(&mut ctx, op).unwrap();
            let got = op.eval(&mut ctx, &[]).unwrap();
            assert_eq!(got, exp);
        }
    }

    #[test]
    fn test_is_ipv4_mapped() {
        let cases = vec![
            (Datum::Bytes(vec![]), Datum::I64(0)),
            (Datum::Bytes(vec![0x10, 0x10, 0x10, 0x10]), Datum::I64(0)),
            (
                Datum::Bytes(vec![
                    0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0xff, 0xff, 0x1, 0x2, 0x3,
                    0x4,
                ]),
                Datum::I64(1),
            ),
            (
                Datum::Bytes(vec![
                    0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x1, 0xff, 0xff, 0x1, 0x2, 0x3,
                    0x4,
                ]),
                Datum::I64(0),
            ),
            (
                Datum::Bytes(vec![0x0, 0x1, 0x2, 0x3, 0x4, 0x5, 0x6]),
                Datum::I64(0),
            ),
        ];

        let mut ctx = EvalContext::default();
        for (input, exp) in cases {
            let input = datum_expr(input);
            let op = scalar_func_expr(ScalarFuncSig::IsIPv4Mapped, &[input]);
            let op = Expression::build(&mut ctx, op).unwrap();
            let got = op.eval(&mut ctx, &[]).unwrap();
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
            let op = Expression::build(&mut ctx, op).unwrap();
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
            let op = Expression::build(&mut ctx, op).unwrap();
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
            let op = Expression::build(&mut ctx, op).unwrap();
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
