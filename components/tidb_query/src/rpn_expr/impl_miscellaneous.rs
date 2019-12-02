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
const PREFIX_MAPPED: [u8; 12] = [
    0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0xff, 0xff,
];

#[rpn_fn]
#[inline]
pub fn inet_aton(addr: &Option<Bytes>) -> Result<Option<Int>> {
    Ok(addr
        .as_ref()
        .map(|addr| String::from_utf8_lossy(addr))
        .and_then(expr_util::miscellaneous::inet_aton))
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
pub fn is_ipv4_compat(addr: &Option<Bytes>) -> Result<Option<i64>> {
    Ok(addr.as_ref().map_or(Some(0), |addr| {
        if addr.len() != IPV6_LENGTH || !addr.starts_with(&PREFIX_COMPAT) {
            Some(0)
        } else {
            Some(1)
        }
    }))
}

#[rpn_fn]
#[inline]
pub fn is_ipv4_mapped(addr: &Option<Bytes>) -> Result<Option<i64>> {
    Ok(addr.as_ref().map_or(Some(0), |addr| {
        if addr.len() != IPV6_LENGTH || !addr.starts_with(&PREFIX_MAPPED) {
            Some(0)
        } else {
            Some(1)
        }
    }))
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

#[cfg(test)]
mod tests {
    use tipb::ScalarFuncSig;

    use super::*;
    use crate::rpn_expr::test_util::RpnFnScalarEvaluator;

    fn hex(data: impl AsRef<[u8]>) -> Vec<u8> {
        hex::decode(data).unwrap()
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
}
