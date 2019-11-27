// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

use std::convert::{TryFrom, TryInto};
use std::net::{Ipv4Addr, Ipv6Addr};
use std::str::FromStr;
use tidb_query_codegen::rpn_fn;

use crate::codec::data_type::*;
use crate::expr_util;
use crate::Result;

const IPV4_LENGTH: usize = 4;
const IPV6_LENGTH: usize = 16;
const PREFIX_COMPAT: [u8; 12] = [0x00; 12];

#[rpn_fn]
#[inline]
pub fn is_ipv4(addr: &Option<Bytes>) -> Result<Option<Int>> {
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

#[rpn_fn]
#[inline]
pub fn inet_ntoa(arg: &Option<Int>) -> Result<Option<Bytes>> {
    Ok(arg
        .and_then(|arg| u32::try_from(arg).ok())
        .map(|arg| format!("{}", Ipv4Addr::from(arg)).into_bytes()))
}

#[rpn_fn]
#[inline]
pub fn is_ipv4_compat(addr: &Option<Bytes>) -> Result<Option<i64>> {
    match addr {
        Some(addr) => {
            if addr.len() != IPV6_LENGTH {
                return Ok(Some(0));
            }
            if !addr.starts_with(&PREFIX_COMPAT) {
                return Ok(Some(0));
            }
            Ok(Some(1))
        }
        None => Ok(Some(0)),
    }
}

#[rpn_fn]
#[inline]
pub fn is_ipv6(addr: &Option<Bytes>) -> Result<Option<Int>> {
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

#[rpn_fn]
#[inline]
pub fn inet6_ntoa(arg: &Option<Bytes>) -> Result<Option<Bytes>> {
    Ok(arg.as_ref().and_then(|s| {
        if s.len() == IPV6_LENGTH {
            let v: &[u8; 16] = s.as_slice().try_into().unwrap();
            Some(format!("{}", Ipv6Addr::from(*v)).into_bytes())
        } else if s.len() == IPV4_LENGTH {
            let v: &[u8; 4] = s.as_slice().try_into().unwrap();
            Some(format!("{}", Ipv4Addr::from(*v)).into_bytes())
        } else {
            None
        }
    }))
}

#[rpn_fn]
#[inline]
pub fn inet6_aton(input: &Option<Bytes>) -> Result<Option<Bytes>> {
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

#[rpn_fn]
#[inline]
pub fn inet_aton(addr: &Option<Bytes>) -> Result<Option<i64>> {
    Ok(addr
        .as_ref()
        .map(|addr| String::from_utf8_lossy(addr))
        .and_then(expr_util::miscellaneous::inet_aton))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::rpn_expr::test_util::RpnFnScalarEvaluator;
    use tipb::ScalarFuncSig;

    #[test]
    fn test_inet_ntoa() {
        let test_cases = vec![
            (Some(167773449), Some(Bytes::from("10.0.5.9"))),
            (Some(2063728641), Some(Bytes::from("123.2.0.1"))),
            (Some(0), Some(Bytes::from("0.0.0.0"))),
            (Some(545460846593), None),
            (Some(-1), None),
            (None, None),
            (
                Some(i64::from(u32::max_value())),
                Some(Bytes::from("255.255.255.255")),
            ),
        ];
        for (arg, expected) in test_cases {
            let output = RpnFnScalarEvaluator::new()
                .push_param(arg)
                .evaluate(ScalarFuncSig::InetNtoa)
                .unwrap();
            assert_eq!(output, expected);
        }
    }

    #[test]
    fn test_is_ipv4() {
        let cases = vec![(Some("127.0.0.1"), Some(1)), (Some("127.0.0.256"), Some(0))];

        for (input, expect) in cases {
            let input = input.map(|v| v.as_bytes().to_vec());
            let output = RpnFnScalarEvaluator::new()
                .push_param(input)
                .evaluate(ScalarFuncSig::IsIPv4)
                .unwrap();
            assert_eq!(output, expect);
        }

        let output = RpnFnScalarEvaluator::new()
            .push_param(None::<Bytes>)
            .evaluate(ScalarFuncSig::IsIPv4)
            .unwrap();
        assert_eq!(output, Some(0));
    }

    #[test]
    fn test_is_ipv4_compat() {
        let test_cases = vec![
            (
                Some(vec![
                    0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x1, 0x0, 0x0, 0x1, 0x2, 0x3, 0x4,
                ]),
                Some(0),
            ),
            (
                Some(vec![
                    0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x1, 0x2, 0x3, 0x4,
                ]),
                Some(1),
            ),
            (Some(vec![0x10, 0x10, 0x10, 0x10]), Some(0)),
            (
                Some(vec![
                    0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x1, 0xff, 0xff, 0x1, 0x2, 0x3,
                    0x4,
                ]),
                Some(0),
            ),
            (Some(vec![0x0, 0x1, 0x2, 0x3, 0x4, 0x5, 0x6]), Some(0)),
            (None, Some(0)),
        ];
        for (input, expect) in test_cases {
            let output = RpnFnScalarEvaluator::new()
                .push_param(input)
                .evaluate(ScalarFuncSig::IsIPv4Compat)
                .unwrap();
            assert_eq!(output, expect);
        }
    }

    #[test]
    fn test_is_ipv6() {
        let cases = vec![
            (Some("::1"), Some(1)),
            (Some("1:2:3:4:5:6:7:10000"), Some(0)),
        ];

        for (input, expect) in cases {
            let input = input.map(|v| v.as_bytes().to_vec());
            let output = RpnFnScalarEvaluator::new()
                .push_param(input)
                .evaluate(ScalarFuncSig::IsIPv6)
                .unwrap();
            assert_eq!(output, expect);
        }

        let output = RpnFnScalarEvaluator::new()
            .push_param(None::<Bytes>)
            .evaluate(ScalarFuncSig::IsIPv6)
            .unwrap();
        assert_eq!(output, Some(0));
    }

    #[test]
    fn test_inet6_ntoa() {
        let test_cases = vec![
            (
                Some(vec![0x00, 0x00, 0x00, 0x00]),
                Some(b"0.0.0.0".to_vec()),
            ),
            (
                Some(vec![0x0A, 0x00, 0x05, 0x09]),
                Some(b"10.0.5.9".to_vec()),
            ),
            (
                Some(vec![
                    0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01,
                    0x02, 0x03, 0x04,
                ]),
                Some(b"::1.2.3.4".to_vec()),
            ),
            (
                Some(vec![
                    0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0xFF, 0xFF, 0x01,
                    0x02, 0x03, 0x04,
                ]),
                Some(b"::ffff:1.2.3.4".to_vec()),
            ),
            (
                Some(vec![
                    0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0xFD, 0xFE, 0x5A, 0x55, 0xCA, 0xFF, 0xFE,
                    0xFA, 0x90, 0x89,
                ]),
                Some(b"::fdfe:5a55:caff:fefa:9089".to_vec()),
            ),
            (
                Some(vec![
                    0xFD, 0xFE, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x5A, 0x55, 0xCA, 0xFF, 0xFE,
                    0xFA, 0x90, 0x89,
                ]),
                Some(b"fdfe::5a55:caff:fefa:9089".to_vec()),
            ),
            (
                Some(vec![
                    0x20, 0x01, 0x0d, 0xb8, 0x85, 0xa3, 0x12, 0x34, 0x56, 0x78, 0x8a, 0x2e, 0x03,
                    0x70, 0x73, 0x34,
                ]),
                Some(b"2001:db8:85a3:1234:5678:8a2e:370:7334".to_vec()),
            ),
            // missing bytes
            (Some(b"".to_vec()), None),
            // missing a byte ipv4
            (Some(vec![0x20, 0x01, 0x0d]), None),
            // missing a byte ipv6
            (
                Some(vec![
                    0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0xFF, 0xFF, 0xFF,
                    0xFF, 0xFF,
                ]),
                None,
            ),
            (None, None),
        ];

        for (input, expect) in test_cases {
            let output = RpnFnScalarEvaluator::new()
                .push_param(input)
                .evaluate(ScalarFuncSig::Inet6Ntoa)
                .unwrap();
            assert_eq!(output, expect);
        }
    }

    #[test]
    fn test_inet6_aton() {
        let test_cases = vec![
            (
                Some(b"0.0.0.0".to_vec()),
                Some(vec![0x00, 0x00, 0x00, 0x00]),
            ),
            (
                Some(b"10.0.5.9".to_vec()),
                Some(vec![0x0A, 0x00, 0x05, 0x09]),
            ),
            (
                Some(b"::1.2.3.4".to_vec()),
                Some(vec![
                    0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01,
                    0x02, 0x03, 0x04,
                ]),
            ),
            (
                Some(b"::FFFF:1.2.3.4".to_vec()),
                Some(vec![
                    0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0xFF, 0xFF, 0x01,
                    0x02, 0x03, 0x04,
                ]),
            ),
            (
                Some(b"::fdfe:5a55:caff:fefa:9089".to_vec()),
                Some(vec![
                    0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0xFD, 0xFE, 0x5A, 0x55, 0xCA, 0xFF, 0xFE,
                    0xFA, 0x90, 0x89,
                ]),
            ),
            (
                Some(b"fdfe::5a55:caff:fefa:9089".to_vec()),
                Some(vec![
                    0xFD, 0xFE, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x5A, 0x55, 0xCA, 0xFF, 0xFE,
                    0xFA, 0x90, 0x89,
                ]),
            ),
            (
                Some(b"2001:0db8:85a3:0000:0000:8a2e:0370:7334".to_vec()),
                Some(vec![
                    0x20, 0x01, 0x0d, 0xb8, 0x85, 0xa3, 0x00, 0x00, 0x00, 0x00, 0x8a, 0x2e, 0x03,
                    0x70, 0x73, 0x34,
                ]),
            ),
            (Some(b"".to_vec()), None),
            (None, None),
        ];

        for (input, expect) in test_cases {
            let output = RpnFnScalarEvaluator::new()
                .push_param(input)
                .evaluate(ScalarFuncSig::Inet6Aton)
                .unwrap();
            assert_eq!(output, expect);
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

        for (input, expect) in test_cases {
            let output = RpnFnScalarEvaluator::new()
                .push_param(input)
                .evaluate(ScalarFuncSig::InetAton)
                .unwrap();
            assert_eq!(output, expect);
        }
    }
}
