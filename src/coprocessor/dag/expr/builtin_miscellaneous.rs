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

use std::net::IpAddr;
use std::str::FromStr;

use super::{EvalContext, Result, ScalarFunc};
use coprocessor::codec::Datum;

impl ScalarFunc {
    pub fn is_ipv4(&self, ctx: &mut EvalContext, row: &[Datum]) -> Result<Option<i64>> {
        let input = try_opt!(self.children[0].eval_string_and_decode(ctx, row));
        if is_given_string_ipv4(&input) {
            Ok(Some(1))
        } else {
            Ok(Some(0))
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

}
