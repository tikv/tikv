// Copyright 2018 PingCAP, Inc.
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

use std::i64;

use super::{EvalContext, Result, ScalarFunc};
use coprocessor::codec::Datum;

impl ScalarFunc {
    pub fn length(&self, ctx: &mut EvalContext, row: &[Datum]) -> Result<Option<i64>> {
        let input = try_opt!(self.children[0].eval_string(ctx, row));
        return Ok(Some(input.len() as i64))
    }
}

#[cfg(test)]
mod test {
    use coprocessor::codec::Datum;
    use coprocessor::dag::expr::test::{datum_expr, scalar_func_expr};
    use coprocessor::dag::expr::{EvalContext, Expression};
    use tipb::expression::ScalarFuncSig;

    #[test]
    fn test_length() {
        let cases = vec![
            ("你好", 6i64),
            ("TiKV", 4i64),
            ("あなたのことが好きです", 33i64),
            ("분산 데이터베이스", 25i64),
            ("россия в мире  кубок", 38i64),
        ];

        let mut ctx = EvalContext::default();
        for (input_str, exp) in cases {
            let input = datum_expr(Datum::Bytes(input_str.as_bytes().to_vec()));
            let op = scalar_func_expr(ScalarFuncSig::Length, &[input]);
            let op = Expression::build(&mut ctx, op).unwrap();
            let got = op.eval(&mut ctx, &[]).unwrap();
            let exp = Datum::from(exp);
            assert_eq!(got, exp, "length('{:?}')", input_str);
        }

        // test NULL case
        let input = datum_expr(Datum::Null);
        let op = scalar_func_expr(ScalarFuncSig::Length, &[input]);
        let op = Expression::build(&mut ctx, op).unwrap();
        let got = op.eval(&mut ctx, &[]).unwrap();
        let exp = Datum::Null;
        assert_eq!(got, exp, "length(NULL)");
    }
}
