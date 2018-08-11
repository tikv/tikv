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
    #[inline]
    pub fn bit_count(&self, ctx: &mut EvalContext, row: &[Datum]) -> Result<Option<i64>> {
        let res = self.children[0].eval_int(ctx, row);
        match res {
            Ok(r) => if let Some(v) = r {
                Ok(Some(i64::from(v.count_ones())))
            } else {
                Ok(None)
            },
            Err(e) => if e.is_overflow() {
                Ok(Some(64i64))
            } else {
                Err(e)
            },
        }
    }
}

#[cfg(test)]
mod test {
    use coprocessor::codec::Datum;
    use coprocessor::dag::expr::test::{datum_expr, scalar_func_expr};
    use coprocessor::dag::expr::{EvalContext, Expression};
    use tipb::expression::ScalarFuncSig;
    #[test]
    fn test_bit_count() {
        let cases = vec![
            (Datum::I64(8), Datum::I64(1)),
            (Datum::I64(29), Datum::I64(4)),
            (Datum::I64(0), Datum::I64(0)),
            (Datum::I64(-1), Datum::I64(64)),
            (Datum::I64(-11), Datum::I64(62)),
            (Datum::I64(-1000), Datum::I64(56)),
            (Datum::Null, Datum::Null),
        ];
        let mut ctx = EvalContext::default();
        for (input, exp) in cases {
            let args = &[datum_expr(input)];
            let op = scalar_func_expr(ScalarFuncSig::BitCount, args);
            let op = Expression::build(&mut ctx, op).unwrap();
            let res = op.eval(&mut ctx, &[]).unwrap();
            assert_eq!(res, exp);
        }

        // Test overflow
        let args = datum_expr(Datum::Bytes(
            b"111111111111111111111111111111111111111111111111111111111111111".to_vec(),
        ));
        let child = scalar_func_expr(ScalarFuncSig::CastStringAsInt, &[args]);
        let op = scalar_func_expr(ScalarFuncSig::BitCount, &[child]);
        let op = Expression::build(&mut ctx, op).unwrap();
        let res = op.eval(&mut ctx, &[]).unwrap();
        assert_eq!(res, Datum::I64(64));
    }
}
