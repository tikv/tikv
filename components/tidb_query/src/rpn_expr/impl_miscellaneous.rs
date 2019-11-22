// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

use std::net::{Ipv4Addr, Ipv6Addr};
use std::str::FromStr;
use tidb_query_codegen::rpn_fn;

use crate::codec::data_type::*;
use crate::Result;

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

#[cfg(test)]
mod tests {
    use tipb::ScalarFuncSig;

    use crate::rpn_expr::types::test_util::RpnFnScalarEvaluator;

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
    }
}
