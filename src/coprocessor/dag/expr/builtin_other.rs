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
                Ok(Some(64))
            } else {
                Err(e)
            },
        }
    }
}

#[cfg(test)]
mod tests {
    use coprocessor::codec::mysql::Decimal;
    use coprocessor::codec::Datum;
    use coprocessor::dag::expr::ctx::FLAG_OVERFLOW_AS_WARNING;
    use coprocessor::dag::expr::tests::{datum_expr, scalar_func_expr};
    use coprocessor::dag::expr::{EvalConfig, EvalContext, Expression};
    use std::str::FromStr;
    use std::sync::Arc;
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
            (Datum::I64(9223372036854775807), Datum::I64(63)),
            (Datum::U64(9223372036854775808), Datum::I64(1)),
            (Datum::U64(9223372036854775809), Datum::I64(2)),
            (Datum::U64(11111111112222222222), Datum::I64(37)),
            (Datum::U64(18446744073709551615), Datum::I64(64)),
            (Datum::U64(18446744073709551614), Datum::I64(63)),
            (Datum::Null, Datum::Null),
        ];
        let mut ctx = EvalContext::default();
        for (input, exp) in cases {
            let args = &[datum_expr(input)];
            let op = scalar_func_expr(ScalarFuncSig::BitCount, args);
            let op = Expression::build(&ctx, op).unwrap();
            let res = op.eval(&mut ctx, &[]).unwrap();
            assert_eq!(res, exp);
        }

        let cases = vec![
            (
                Datum::Bytes(
                    b"111111111111111111111111111111111111111111111111111111111111111".to_vec(),
                ),
                Datum::I64(64),
            ),
            (
                Datum::Bytes(b"18446744073709551616".to_vec()),
                Datum::I64(64),
            ),
            (
                Datum::Bytes(b"18446744073709551615".to_vec()),
                Datum::I64(64),
            ),
            (
                Datum::Bytes(b"18446744073709551614".to_vec()),
                Datum::I64(63),
            ),
            (
                Datum::Bytes(b"11111111112222222222".to_vec()),
                Datum::I64(37),
            ),
        ];
        let mut ctx = EvalContext::new(Arc::new(EvalConfig::from_flags(FLAG_OVERFLOW_AS_WARNING)));
        for (input, exp) in cases {
            let args = &[datum_expr(input)];
            let child = scalar_func_expr(ScalarFuncSig::CastStringAsInt, args);
            let op = scalar_func_expr(ScalarFuncSig::BitCount, &[child]);
            let op = Expression::build(&ctx, op).unwrap();
            let res = op.eval(&mut ctx, &[]).unwrap();
            assert_eq!(res, exp);
        }

        let cases = vec![
            (
                Datum::Dec(
                    Decimal::from_str(
                        "111111111111111111111111111111111111111111111111111111111111111",
                    ).unwrap(),
                ),
                Datum::I64(63),
            ),
            (
                Datum::Dec(
                    Decimal::from_str(
                        "-111111111111111111111111111111111111111111111111111111111111111",
                    ).unwrap(),
                ),
                Datum::I64(1),
            ),
            (
                Datum::Dec(Decimal::from_str("18446744073709551616").unwrap()),
                Datum::I64(63),
            ),
        ];
        let mut ctx = EvalContext::new(Arc::new(EvalConfig::from_flags(FLAG_OVERFLOW_AS_WARNING)));
        for (input, exp) in cases {
            let args = &[datum_expr(input)];
            let child = scalar_func_expr(ScalarFuncSig::CastDecimalAsInt, args);
            let op = scalar_func_expr(ScalarFuncSig::BitCount, &[child]);
            let op = Expression::build(&ctx, op).unwrap();
            let res = op.eval(&mut ctx, &[]).unwrap();
            assert_eq!(res, exp);
        }
    }
}
