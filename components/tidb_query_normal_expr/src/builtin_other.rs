// Copyright 2018 TiKV Project Authors. Licensed under Apache-2.0.

use std::i64;

use crate::ScalarFunc;
use tidb_query_datatype::codec::Datum;
use tidb_query_datatype::expr::{EvalContext, Result};

impl ScalarFunc {
    #[inline]
    pub fn bit_count(&self, ctx: &mut EvalContext, row: &[Datum]) -> Result<Option<i64>> {
        let res = self.children[0].eval_int(ctx, row);
        match res {
            Ok(r) => {
                if let Some(v) = r {
                    Ok(Some(i64::from(v.count_ones())))
                } else {
                    Ok(None)
                }
            }
            Err(e) => {
                if e.is_overflow() {
                    Ok(Some(64))
                } else {
                    Err(e)
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::tests::{datum_expr, scalar_func_expr};
    use crate::Expression;
    use std::str::FromStr;
    use std::sync::Arc;
    use tidb_query_datatype::codec::mysql::Decimal;
    use tidb_query_datatype::codec::Datum;
    use tidb_query_datatype::expr::Flag;
    use tidb_query_datatype::expr::{EvalConfig, EvalContext};
    use tipb::ScalarFuncSig;

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
            let op = Expression::build(&mut ctx, op).unwrap();
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
        let mut ctx = EvalContext::new(Arc::new(EvalConfig::from_flag(Flag::OVERFLOW_AS_WARNING)));
        for (input, exp) in cases {
            let args = &[datum_expr(input)];
            let child = scalar_func_expr(ScalarFuncSig::CastStringAsInt, args);
            let op = scalar_func_expr(ScalarFuncSig::BitCount, &[child]);
            let op = Expression::build(&mut ctx, op).unwrap();
            let res = op.eval(&mut ctx, &[]).unwrap();
            assert_eq!(res, exp);
        }

        let cases = vec![
            (
                Datum::Dec(
                    Decimal::from_str(
                        "111111111111111111111111111111111111111111111111111111111111111",
                    )
                    .unwrap(),
                ),
                Datum::I64(63),
            ),
            (
                Datum::Dec(
                    Decimal::from_str(
                        "-111111111111111111111111111111111111111111111111111111111111111",
                    )
                    .unwrap(),
                ),
                Datum::I64(1),
            ),
            (
                Datum::Dec(Decimal::from_str("18446744073709551616").unwrap()),
                Datum::I64(63),
            ),
        ];
        let mut ctx = EvalContext::new(Arc::new(EvalConfig::from_flag(Flag::OVERFLOW_AS_WARNING)));
        for (input, exp) in cases {
            let args = &[datum_expr(input)];
            let child = scalar_func_expr(ScalarFuncSig::CastDecimalAsInt, args);
            let op = scalar_func_expr(ScalarFuncSig::BitCount, &[child]);
            let op = Expression::build(&mut ctx, op).unwrap();
            let res = op.eval(&mut ctx, &[]).unwrap();
            assert_eq!(res, exp);
        }
    }
}
