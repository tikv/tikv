// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

use tidb_query_codegen::rpn_fn;

use crate::codec::data_type::*;
use crate::Result;

use std::net::Ipv4Addr;

#[rpn_fn]
#[inline]
pub fn inet_ntoa(arg: &Option<Int>) -> Result<Option<Bytes>> {
    Ok(match arg {
        Some(arg) => {
            if *arg < 0 || *arg > i64::from(u32::max_value()) {
                None
            } else {
                let v = *arg as u32;
                let ipv4_addr =
                    Ipv4Addr::new((v >> 24) as u8, (v >> 16) as u8, (v >> 8) as u8, v as u8);
                Some(format!("{}", ipv4_addr).into_bytes())
            }
        }
        _ => None,
    })
}

#[cfg(test)]
mod test {
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
            (
                Some(i64::from(u32::max_value())),
                Some(Bytes::from("255.255.255.255")),
            ),
        ];
        for (arg, expected) in test_cases {
            let output = RpnFnScalarEvaluator::new()
                .push_params(arg)
                .evaluate(ScalarFuncSig::InetNtoa)
                .unwrap();
            assert_eq!(output, expected);
        }
    }
}
