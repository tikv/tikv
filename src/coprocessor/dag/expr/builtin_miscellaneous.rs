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

use std::net::{IpAddr, Ipv4Addr};
use std::str::FromStr;
use std::borrow::Borrow;

use super::{EvalContext, Result, ScalarFunc};
use coprocessor::codec::{Datum};

impl ScalarFunc {

    pub fn is_ipv4(&self,
                    ctx: &mut EvalContext,
                    row: &[Datum],) -> Result<Option<i64>> {
        let input = try_opt!(self.children[0].eval_string(ctx, row));
        let s = String::from_utf8(input.into_owned())?;
        if ScalarFunc::is_given_string_ipv4(&s) {
            return Ok(Some(1));

        } else {
            return Ok(Some(0));
        }
    }

    fn is_given_string_ipv4(some_str: &str) -> bool {
        let address = IpAddr::from_str(some_str);
        if let Ok(add) = address {
            return add.is_ipv4();
        }
        false
    }

}


#[cfg(test)]
mod test {
    use coprocessor::codec::{Datum};
    use coprocessor::dag::expr::test::{datum_expr, scalar_func_expr};
    use tipb::expression::ScalarFuncSig;
    use coprocessor::dag::expr::{EvalContext, Expression, ScalarFunc};

    #[test]
    fn test_is_ipv4_success() {
        let mut ctx = EvalContext::default();
        let input_str = "127.0.0.1";
        let input = datum_expr(Datum::Bytes(input_str.as_bytes().to_vec()));

        let op = scalar_func_expr(ScalarFuncSig::IsIPv4, &[input]);
        let op = Expression::build(&mut ctx, op).unwrap();
        let got = op.eval(&mut ctx, &[]).unwrap();
        let exp = Datum::from(1i64);
        assert_eq!(got, exp);
    }

    #[test]
    fn test_is_given_string_ipv4_success() {
        let is_ipv4 = ScalarFunc::is_given_string_ipv4("127.0.0.1");
        assert_eq!(is_ipv4, true);
    }

    #[test]
    fn test_is_given_string_ipv4_fail() {
        let is_ipv4 = ScalarFunc::is_given_string_ipv4("A.123.a.X");
        assert_eq!(is_ipv4, false);
    }


}
