// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

use std::{
    convert::{TryFrom, TryInto},
    net::{Ipv4Addr, Ipv6Addr},
    str::FromStr,
};

use tidb_query_codegen::rpn_fn;
use tidb_query_common::Result;
use tidb_query_datatype::codec::data_type::*;
use uuid::Uuid;

const IPV4_LENGTH: usize = 4;
const IPV6_LENGTH: usize = 16;
const PREFIX_COMPAT: [u8; 12] = [0x00; 12];
const PREFIX_MAPPED: [u8; 12] = [
    0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0xff, 0xff,
];

#[rpn_fn(nullable, varg)]
#[inline]
pub fn any_value<T: Evaluable + EvaluableRet>(args: &[Option<&T>]) -> Result<Option<T>> {
    if let Some(arg) = args.first() {
        Ok(arg.cloned())
    } else {
        Ok(None)
    }
}

#[rpn_fn(nullable, varg)]
#[inline]
pub fn any_value_json(args: &[Option<JsonRef>]) -> Result<Option<Json>> {
    if let Some(arg) = args.first() {
        Ok(arg.map(|x| x.to_owned()))
    } else {
        Ok(None)
    }
}

#[rpn_fn(nullable, varg)]
#[inline]
pub fn any_value_bytes(args: &[Option<BytesRef>]) -> Result<Option<Bytes>> {
    if let Some(arg) = args.first() {
        Ok(arg.map(|x| x.to_vec()))
    } else {
        Ok(None)
    }
}

#[rpn_fn]
#[inline]
pub fn inet_aton(addr: BytesRef) -> Result<Option<Int>> {
    let addr = String::from_utf8_lossy(addr);

    if addr.len() == 0 || addr.ends_with('.') {
        return Ok(None);
    }
    let (mut byte_result, mut result, mut dot_count): (u64, u64, usize) = (0, 0, 0);
    for c in addr.chars() {
        if ('0'..='9').contains(&c) {
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

#[rpn_fn(nullable)]
#[inline]
pub fn inet_ntoa(arg: Option<&Int>) -> Result<Option<Bytes>> {
    Ok(arg
        .cloned()
        .and_then(|arg| u32::try_from(arg).ok())
        .map(|arg| format!("{}", Ipv4Addr::from(arg)).into_bytes()))
}

#[rpn_fn(nullable)]
#[inline]
pub fn inet6_aton(input: Option<BytesRef>) -> Result<Option<Bytes>> {
    let input = match input {
        Some(input) => String::from_utf8_lossy(input),
        None => return Ok(None),
    };

    let ipv6_addr = Ipv6Addr::from_str(&input).map(|t| t.octets().to_vec());
    let ipv4_addr_eval = |_| Ipv4Addr::from_str(&input).map(|t| t.octets().to_vec());
    ipv6_addr
        .or_else(ipv4_addr_eval)
        .map(Option::Some)
        .or(Ok(None))
}

#[rpn_fn(nullable)]
#[inline]
pub fn inet6_ntoa(arg: Option<BytesRef>) -> Result<Option<Bytes>> {
    Ok(arg.and_then(|s| {
        if s.len() == IPV6_LENGTH {
            let v: &[u8; 16] = s.try_into().unwrap();
            Some(format!("{}", Ipv6Addr::from(*v)).into_bytes())
        } else if s.len() == IPV4_LENGTH {
            let v: &[u8; 4] = s.try_into().unwrap();
            Some(format!("{}", Ipv4Addr::from(*v)).into_bytes())
        } else {
            None
        }
    }))
}

#[rpn_fn(nullable)]
#[inline]
pub fn is_ipv4(addr: Option<BytesRef>) -> Result<Option<Int>> {
    Ok(match addr {
        Some(addr) => match std::str::from_utf8(addr) {
            Ok(addr) => {
                if Ipv4Addr::from_str(addr).is_ok() {
                    Some(1)
                } else {
                    Some(0)
                }
            }
            _ => Some(0),
        },
        None => Some(0),
    })
}

#[rpn_fn(nullable)]
#[inline]
pub fn is_ipv4_compat(addr: Option<BytesRef>) -> Result<Option<i64>> {
    Ok(addr.as_ref().map_or(Some(0), |addr| {
        if addr.len() != IPV6_LENGTH || !addr.starts_with(&PREFIX_COMPAT) {
            Some(0)
        } else {
            Some(1)
        }
    }))
}

#[rpn_fn(nullable)]
#[inline]
pub fn is_ipv4_mapped(addr: Option<BytesRef>) -> Result<Option<i64>> {
    Ok(addr.as_ref().map_or(Some(0), |addr| {
        if addr.len() != IPV6_LENGTH || !addr.starts_with(&PREFIX_MAPPED) {
            Some(0)
        } else {
            Some(1)
        }
    }))
}

#[rpn_fn(nullable)]
#[inline]
pub fn is_ipv6(addr: Option<BytesRef>) -> Result<Option<Int>> {
    Ok(match addr {
        Some(addr) => match std::str::from_utf8(addr) {
            Ok(addr) => {
                if Ipv6Addr::from_str(addr).is_ok() {
                    Some(1)
                } else {
                    Some(0)
                }
            }
            _ => Some(0),
        },
        None => Some(0),
    })
}

#[rpn_fn(nullable)]
#[inline]
pub fn uuid() -> Result<Option<Bytes>> {
    let result = Uuid::new_v4();
    let mut buf = vec![0; uuid::adapter::Hyphenated::LENGTH];
    result.to_hyphenated().encode_lower(&mut buf);
    Ok(Some(buf))
}

#[cfg(test)]
mod tests {
    use bstr::ByteVec;
    use tidb_query_datatype::expr::EvalContext;
    use tipb::ScalarFuncSig;

    use super::*;
    use crate::test_util::RpnFnScalarEvaluator;

    fn hex(data: impl AsRef<[u8]>) -> Vec<u8> {
        hex::decode(data).unwrap()
    }

    #[test]
    fn test_decimal_any_value() {
        let test_cases = vec![
            (vec![], None),
            (vec![Decimal::from(10)], Some(Decimal::from(10))),
            (
                vec![Decimal::from(10), Decimal::from(20)],
                Some(Decimal::from(10)),
            ),
            (
                vec![Decimal::from(10), Decimal::from(20), Decimal::from(30)],
                Some(Decimal::from(10)),
            ),
        ];

        for (args, expect_output) in test_cases {
            let output = RpnFnScalarEvaluator::new()
                .push_params(args)
                .evaluate::<Decimal>(ScalarFuncSig::DecimalAnyValue)
                .unwrap();
            assert_eq!(output, expect_output);
        }
    }

    #[test]
    fn test_duration_any_value() {
        let test_cases = vec![
            (vec![], None),
            (
                vec![Duration::from_millis(10, 0).unwrap()],
                Some(Duration::from_millis(10, 0).unwrap()),
            ),
            (
                vec![
                    Duration::from_millis(10, 0).unwrap(),
                    Duration::from_millis(11, 0).unwrap(),
                ],
                Some(Duration::from_millis(10, 0).unwrap()),
            ),
            (
                vec![
                    Duration::from_millis(10, 0).unwrap(),
                    Duration::from_millis(11, 0).unwrap(),
                    Duration::from_millis(12, 0).unwrap(),
                ],
                Some(Duration::from_millis(10, 0).unwrap()),
            ),
        ];

        for (args, expect_output) in test_cases {
            let output = RpnFnScalarEvaluator::new()
                .push_params(args)
                .evaluate::<Duration>(ScalarFuncSig::DurationAnyValue)
                .unwrap();
            assert_eq!(output, expect_output);
        }
    }

    #[test]
    fn test_int_any_value() {
        let test_cases = vec![
            (vec![], None),
            (vec![1i64], Some(1i64)),
            (vec![1i64, 2i64], Some(1i64)),
            (vec![1i64, 2i64, 3i64], Some(1i64)),
        ];

        for (args, expect_output) in test_cases {
            let output = RpnFnScalarEvaluator::new()
                .push_params(args)
                .evaluate::<Int>(ScalarFuncSig::IntAnyValue)
                .unwrap();
            assert_eq!(output, expect_output);
        }
    }

    #[test]
    fn test_json_any_value() {
        let test_cases = vec![
            (vec![], None),
            (
                vec![Json::from_u64(1).unwrap()],
                Some(Json::from_u64(1).unwrap()),
            ),
            (
                vec![Json::from_u64(1).unwrap(), Json::from_u64(2).unwrap()],
                Some(Json::from_u64(1).unwrap()),
            ),
            (
                vec![
                    Json::from_u64(1).unwrap(),
                    Json::from_u64(2).unwrap(),
                    Json::from_u64(3).unwrap(),
                ],
                Some(Json::from_u64(1).unwrap()),
            ),
        ];

        for (args, expect_output) in test_cases {
            let output = RpnFnScalarEvaluator::new()
                .push_params(args)
                .evaluate::<Json>(ScalarFuncSig::JsonAnyValue)
                .unwrap();
            assert_eq!(output, expect_output);
        }
    }

    #[test]
    fn test_real_any_value() {
        let test_cases = vec![
            (vec![], None),
            (vec![Real::from(1.2_f64)], Some(Real::from(1.2_f64))),
            (
                vec![Real::from(1.2_f64), Real::from(2.3_f64)],
                Some(Real::from(1.2_f64)),
            ),
            (
                vec![Real::from(1.2_f64), Real::from(2.3_f64), Real::from(3_f64)],
                Some(Real::from(1.2_f64)),
            ),
        ];

        for (args, expect_output) in test_cases {
            let output = RpnFnScalarEvaluator::new()
                .push_params(args)
                .evaluate::<Real>(ScalarFuncSig::RealAnyValue)
                .unwrap();
            assert_eq!(output, expect_output);
        }
    }

    #[test]
    fn test_string_any_value() {
        let test_cases = vec![
            (vec![], None),
            (vec![Bytes::from("abc")], Some(Bytes::from("abc"))),
            (
                vec![Bytes::from("abc"), Bytes::from("def")],
                Some(Bytes::from("abc")),
            ),
            (
                vec![Bytes::from("abc"), Bytes::from("def"), Bytes::from("ojk")],
                Some(Bytes::from("abc")),
            ),
        ];

        for (args, expect_output) in test_cases {
            let output = RpnFnScalarEvaluator::new()
                .push_params(args)
                .evaluate::<Bytes>(ScalarFuncSig::StringAnyValue)
                .unwrap();
            assert_eq!(output, expect_output);
        }
    }

    #[test]
    fn test_time_any_value() {
        let mut ctx = EvalContext::default();
        let test_cases = vec![
            (vec![], None),
            (
                vec![DateTime::parse_datetime(&mut ctx, "1000-01-01 00:00:00", 0, false).unwrap()],
                Some(DateTime::parse_datetime(&mut ctx, "1000-01-01 00:00:00", 0, false).unwrap()),
            ),
            (
                vec![
                    DateTime::parse_datetime(&mut ctx, "1000-01-01 00:00:00", 0, false).unwrap(),
                    DateTime::parse_datetime(&mut ctx, "1000-01-01 00:00:01", 0, false).unwrap(),
                ],
                Some(DateTime::parse_datetime(&mut ctx, "1000-01-01 00:00:00", 0, false).unwrap()),
            ),
            (
                vec![
                    DateTime::parse_datetime(&mut ctx, "1000-01-01 00:00:00", 0, false).unwrap(),
                    DateTime::parse_datetime(&mut ctx, "1000-01-01 00:00:01", 0, false).unwrap(),
                    DateTime::parse_datetime(&mut ctx, "1000-01-01 00:00:02", 0, false).unwrap(),
                ],
                Some(DateTime::parse_datetime(&mut ctx, "1000-01-01 00:00:00", 0, false).unwrap()),
            ),
        ];

        for (args, expect_output) in test_cases {
            let output = RpnFnScalarEvaluator::new()
                .push_params(args)
                .evaluate::<DateTime>(ScalarFuncSig::TimeAnyValue)
                .unwrap();
            assert_eq!(output, expect_output);
        }
    }

    #[test]
    fn test_inet_aton() {
        let test_cases = vec![
            (Some(b"0.0.0.0".to_vec()), Some(0)),
            (Some(b"255.255.255.255".to_vec()), Some(4294967295)),
            (Some(b"127.0.0.1".to_vec()), Some(2130706433)),
            (Some(b"113.14.22.3".to_vec()), Some(1896748547)),
            (Some(b"1".to_vec()), Some(1)),
            (Some(b"0.1.2".to_vec()), Some(65538)),
            (Some(b"0.1.2.3.4".to_vec()), None),
            (Some(b"0.1.2..3".to_vec()), None),
            (Some(b".0.1.2.3".to_vec()), None),
            (Some(b"0.1.2.3.".to_vec()), None),
            (Some(b"1.-2.3.4".to_vec()), None),
            (Some(b"".to_vec()), None),
            (Some(b"0.0.0.256".to_vec()), None),
            (Some(b"127.0.0,1".to_vec()), None),
            (None, None),
        ];

        for (input, expect_output) in test_cases {
            let output = RpnFnScalarEvaluator::new()
                .push_param(input)
                .evaluate::<Int>(ScalarFuncSig::InetAton)
                .unwrap();
            assert_eq!(output, expect_output);
        }
    }

    #[test]
    fn test_inet_ntoa() {
        let test_cases = vec![
            (Some(167773449), Some(Bytes::from("10.0.5.9"))),
            (Some(2063728641), Some(Bytes::from("123.2.0.1"))),
            (Some(0), Some(Bytes::from("0.0.0.0"))),
            (
                Some(i64::from(u32::max_value())),
                Some(Bytes::from("255.255.255.255")),
            ),
            (Some(545460846593), None),
            (Some(-1), None),
            (None, None),
        ];

        for (arg, expect_output) in test_cases {
            let output = RpnFnScalarEvaluator::new()
                .push_param(arg)
                .evaluate::<Bytes>(ScalarFuncSig::InetNtoa)
                .unwrap();
            assert_eq!(output, expect_output);
        }
    }

    #[test]
    fn test_inet6_aton() {
        let test_cases = vec![
            (Some(b"0.0.0.0".to_vec()), Some(hex("00000000"))),
            (Some(b"10.0.5.9".to_vec()), Some(hex("0A000509"))),
            (
                Some(b"::1.2.3.4".to_vec()),
                Some(hex("00000000000000000000000001020304")),
            ),
            (
                Some(b"::FFFF:1.2.3.4".to_vec()),
                Some(hex("00000000000000000000FFFF01020304")),
            ),
            (
                Some(b"::fdfe:5a55:caff:fefa:9089".to_vec()),
                Some(hex("000000000000FDFE5A55CAFFFEFA9089")),
            ),
            (
                Some(b"fdfe::5a55:caff:fefa:9089".to_vec()),
                Some(hex("FDFE0000000000005A55CAFFFEFA9089")),
            ),
            (
                Some(b"2001:0db8:85a3:0000:0000:8a2e:0370:7334".to_vec()),
                Some(hex("20010db885a3000000008a2e03707334")),
            ),
            (Some(b"".to_vec()), None),
            (None, None),
        ];

        for (input, expect_output) in test_cases {
            let output = RpnFnScalarEvaluator::new()
                .push_param(input)
                .evaluate::<Bytes>(ScalarFuncSig::Inet6Aton)
                .unwrap();
            assert_eq!(output, expect_output);
        }
    }

    #[test]
    fn test_inet6_ntoa() {
        let test_cases = vec![
            (Some(hex("00000000")), Some(b"0.0.0.0".to_vec())),
            (Some(hex("0A000509")), Some(b"10.0.5.9".to_vec())),
            (
                Some(hex("00000000000000000000000001020304")),
                Some(b"::1.2.3.4".to_vec()),
            ),
            (
                Some(hex("00000000000000000000FFFF01020304")),
                Some(b"::ffff:1.2.3.4".to_vec()),
            ),
            (
                Some(hex("000000000000FDFE5A55CAFFFEFA9089")),
                Some(b"::fdfe:5a55:caff:fefa:9089".to_vec()),
            ),
            (
                Some(hex("FDFE0000000000005A55CAFFFEFA9089")),
                Some(b"fdfe::5a55:caff:fefa:9089".to_vec()),
            ),
            (
                Some(hex("20010db885a3123456788a2e03707334")),
                Some(b"2001:db8:85a3:1234:5678:8a2e:370:7334".to_vec()),
            ),
            // missing bytes
            (Some(b"".to_vec()), None),
            // missing a byte ipv4
            (Some(hex("20010d")), None),
            // missing a byte ipv6
            (Some(hex("00000000000000000000FFFFFFFFFF")), None),
            (None, None),
        ];

        for (input, expect_output) in test_cases {
            let output = RpnFnScalarEvaluator::new()
                .push_param(input)
                .evaluate::<Bytes>(ScalarFuncSig::Inet6Ntoa)
                .unwrap();
            assert_eq!(output, expect_output);
        }
    }

    #[test]
    fn test_is_ipv4() {
        let test_cases = vec![
            (Some(b"127.0.0.1".to_vec()), Some(1)),
            (Some(b"127.0.0.256".to_vec()), Some(0)),
            (None, Some(0)),
        ];

        for (input, expect_output) in test_cases {
            let output = RpnFnScalarEvaluator::new()
                .push_param(input)
                .evaluate::<Int>(ScalarFuncSig::IsIPv4)
                .unwrap();
            assert_eq!(output, expect_output);
        }
    }

    #[test]
    fn test_is_ipv4_compat() {
        let test_cases = vec![
            (Some(hex("00000000000000000001000001020304")), Some(0)),
            (Some(hex("00000000000000000000000001020304")), Some(1)),
            (Some(hex("10101010")), Some(0)),
            (Some(hex("00000000000000000001ffff01020304")), Some(0)),
            (Some(hex("00010203040506")), Some(0)),
            (None, Some(0)),
        ];

        for (input, expect_output) in test_cases {
            let output = RpnFnScalarEvaluator::new()
                .push_param(input)
                .evaluate::<Int>(ScalarFuncSig::IsIPv4Compat)
                .unwrap();
            assert_eq!(output, expect_output);
        }
    }

    #[test]
    fn test_is_ipv4_mapped() {
        let test_cases = vec![
            (Some(hex("00000000000000000001000001020304")), Some(0)),
            (Some(hex("00000000000000000000000001020304")), Some(0)),
            (Some(hex("10101010")), Some(0)),
            (Some(hex("00000000000000000000ffff01020304")), Some(1)),
            (Some(hex("00010203040506")), Some(0)),
            (None, Some(0)),
        ];

        for (input, expect_output) in test_cases {
            let output = RpnFnScalarEvaluator::new()
                .push_param(input)
                .evaluate::<Int>(ScalarFuncSig::IsIPv4Mapped)
                .unwrap();
            assert_eq!(output, expect_output);
        }
    }

    #[test]
    fn test_is_ipv6() {
        let test_cases = vec![
            (Some(b"::1".to_vec()), Some(1)),
            (Some(b"1:2:3:4:5:6:7:10000".to_vec()), Some(0)),
            (None, Some(0)),
        ];

        for (input, expect_output) in test_cases {
            let output = RpnFnScalarEvaluator::new()
                .push_param(input)
                .evaluate::<Int>(ScalarFuncSig::IsIPv6)
                .unwrap();
            assert_eq!(output, expect_output);
        }
    }

    #[test]
    fn test_uuid() {
        let got = RpnFnScalarEvaluator::new()
            .evaluate::<Bytes>(ScalarFuncSig::Uuid)
            .unwrap();
        let r = got.unwrap().into_string().unwrap_or_default();
        let v: Vec<&str> = r.split('-').collect();
        assert_eq!(v.len(), 5);
        assert_eq!(v[0].len(), 8);
        assert_eq!(v[1].len(), 4);
        assert_eq!(v[2].len(), 4);
        assert_eq!(v[3].len(), 4);
        assert_eq!(v[4].len(), 12);
    }
}
