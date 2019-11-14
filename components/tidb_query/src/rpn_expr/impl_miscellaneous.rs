// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

use tidb_query_codegen::rpn_fn;

use crate::codec::data_type::*;
use crate::Result;

use std::convert::TryFrom;
use std::net::Ipv4Addr;

#[rpn_fn]
#[inline]
pub fn inet_ntoa(arg: &Option<Int>) -> Result<Option<Bytes>> {
    Ok(arg
        .and_then(|arg| u32::try_from(arg).ok())
        .map(|arg| format!("{}", Ipv4Addr::from(arg)).into_bytes()))
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
}
