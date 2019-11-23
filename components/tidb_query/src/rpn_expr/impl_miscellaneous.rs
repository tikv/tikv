// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

use tidb_query_codegen::rpn_fn;

use crate::codec::data_type::*;
use crate::Result;
use std::net::{Ipv4Addr, Ipv6Addr};
use std::str::FromStr;

const IPV6_LENGTH: usize = 16;
const PREFIX_COMPAT: [u8; 12] = [0x00; 12];

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

#[cfg(test)]
mod tests {
    use tipb::ScalarFuncSig;

    use crate::rpn_expr::types::test_util::RpnFnScalarEvaluator;

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
    fn test_inet6_aton() {
        let cases = vec![
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
}
