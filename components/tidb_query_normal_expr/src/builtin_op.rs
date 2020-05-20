// Copyright 2017 TiKV Project Authors. Licensed under Apache-2.0.

use std::borrow::Cow;
use std::i64;

use crate::ScalarFunc;
use tidb_query_datatype::codec::mysql::Decimal;
use tidb_query_datatype::codec::Datum;
use tidb_query_datatype::expr::{Error, EvalContext, Result};

impl ScalarFunc {
    pub fn logical_and(&self, ctx: &mut EvalContext, row: &[Datum]) -> Result<Option<i64>> {
        let arg0 = self.children[0].eval_int(ctx, row)?;
        if arg0.map_or(false, |v| v == 0) {
            return Ok(Some(0));
        }
        let arg1 = self.children[1].eval_int(ctx, row)?;
        if arg1.map_or(false, |v| v == 0) {
            return Ok(Some(0));
        }
        if arg0.is_none() || arg1.is_none() {
            return Ok(None);
        }
        Ok(Some(1))
    }

    pub fn logical_or(&self, ctx: &mut EvalContext, row: &[Datum]) -> Result<Option<i64>> {
        let arg0 = self.children[0].eval_int(ctx, row)?;
        if arg0.map_or(false, |v| v != 0) {
            return Ok(Some(1));
        }
        let arg1 = try_opt!(self.children[1].eval_int(ctx, row));
        if arg1 != 0 {
            Ok(Some(1))
        } else {
            Ok(arg0)
        }
    }

    pub fn logical_xor(&self, ctx: &mut EvalContext, row: &[Datum]) -> Result<Option<i64>> {
        let arg0 = try_opt!(self.children[0].eval_int(ctx, row));
        let arg1 = try_opt!(self.children[1].eval_int(ctx, row));
        Ok(Some(((arg0 == 0) ^ (arg1 == 0)) as i64))
    }

    pub fn int_is_true(
        &self,
        ctx: &mut EvalContext,
        row: &[Datum],
        keep_null: bool,
    ) -> Result<Option<i64>> {
        let input = self.children[0].eval_int(ctx, row)?;
        Ok(if keep_null {
            input.map(|v| (v != 0) as i64)
        } else {
            Some(input.map_or(0, |v| (v != 0) as i64))
        })
    }

    pub fn real_is_true(
        &self,
        ctx: &mut EvalContext,
        row: &[Datum],
        keep_null: bool,
    ) -> Result<Option<i64>> {
        let input = self.children[0].eval_real(ctx, row)?;
        Ok(if keep_null {
            input.map(|i| (i != 0f64) as i64)
        } else {
            Some(input.map_or(0, |i| (i != 0f64) as i64))
        })
    }

    pub fn decimal_is_true(
        &self,
        ctx: &mut EvalContext,
        row: &[Datum],
        keep_null: bool,
    ) -> Result<Option<i64>> {
        let input = self.children[0].eval_decimal(ctx, row)?;
        Ok(if keep_null {
            input.map(|dec| !dec.is_zero() as i64)
        } else {
            Some(input.map_or(0, |dec| !dec.is_zero() as i64))
        })
    }

    pub fn int_is_false(
        &self,
        ctx: &mut EvalContext,
        row: &[Datum],
        keep_null: bool,
    ) -> Result<Option<i64>> {
        let input = self.children[0].eval_int(ctx, row)?;
        Ok(if keep_null {
            input.map(|i| (i == 0) as i64)
        } else {
            Some(input.map_or(0, |i| (i == 0) as i64))
        })
    }

    pub fn real_is_false(
        &self,
        ctx: &mut EvalContext,
        row: &[Datum],
        keep_null: bool,
    ) -> Result<Option<i64>> {
        let input = self.children[0].eval_real(ctx, row)?;
        Ok(if keep_null {
            input.map(|v| (v == 0f64) as i64)
        } else {
            Some(input.map_or(0, |v| (v == 0f64) as i64))
        })
    }

    pub fn decimal_is_false(
        &self,
        ctx: &mut EvalContext,
        row: &[Datum],
        keep_null: bool,
    ) -> Result<Option<i64>> {
        let input = self.children[0].eval_decimal(ctx, row)?;
        Ok(if keep_null {
            input.map(|v| v.is_zero() as i64)
        } else {
            Some(input.map_or(0, |v| v.is_zero() as i64))
        })
    }

    pub fn unary_not_real(&self, ctx: &mut EvalContext, row: &[Datum]) -> Result<Option<i64>> {
        let arg = try_opt!(self.children[0].eval_real(ctx, row));
        Ok(Some((arg == 0f64) as i64))
    }

    pub fn unary_not_int(&self, ctx: &mut EvalContext, row: &[Datum]) -> Result<Option<i64>> {
        let arg = try_opt!(self.children[0].eval_int(ctx, row));
        Ok(Some((arg == 0) as i64))
    }

    pub fn unary_not_decimal(&self, ctx: &mut EvalContext, row: &[Datum]) -> Result<Option<i64>> {
        let arg = try_opt!(self.children[0].eval_decimal(ctx, row));
        Ok(Some(arg.is_zero() as i64))
    }

    pub fn unary_minus_int(&self, ctx: &mut EvalContext, row: &[Datum]) -> Result<Option<i64>> {
        use std::cmp::Ordering::*;

        let val = try_opt!(self.children[0].eval_int(ctx, row));
        if self.children[0].is_unsigned() {
            let uval = val as u64;
            match uval.cmp(&(i64::MAX as u64 + 1)) {
                Greater => return Err(Error::overflow("BIGINT", &format!("-{}", uval))),
                Equal => return Ok(Some(i64::MIN)),
                Less => {}
            }
        } else if val == i64::MIN {
            return Err(Error::overflow("BIGINT", &format!("-{}", val)));
        }
        Ok(Some(-val))
    }

    pub fn unary_minus_decimal<'a, 'b: 'a>(
        &'b self,
        ctx: &mut EvalContext,
        row: &'a [Datum],
    ) -> Result<Option<Cow<'a, Decimal>>> {
        let dec = try_opt!(self.children[0].eval_decimal(ctx, row)).into_owned();
        Ok(Some(Cow::Owned(-dec)))
    }

    pub fn unary_minus_real(&self, ctx: &mut EvalContext, row: &[Datum]) -> Result<Option<f64>> {
        let val = try_opt!(self.children[0].eval_real(ctx, row));
        Ok(Some(-val))
    }

    pub fn decimal_is_null(&self, ctx: &mut EvalContext, row: &[Datum]) -> Result<Option<i64>> {
        let arg = self.children[0].eval_decimal(ctx, row)?;
        Ok(Some(arg.is_none() as i64))
    }

    pub fn int_is_null(&self, ctx: &mut EvalContext, row: &[Datum]) -> Result<Option<i64>> {
        let arg = self.children[0].eval_int(ctx, row)?;
        Ok(Some(arg.is_none() as i64))
    }

    pub fn real_is_null(&self, ctx: &mut EvalContext, row: &[Datum]) -> Result<Option<i64>> {
        let arg = self.children[0].eval_real(ctx, row)?;
        Ok(Some(arg.is_none() as i64))
    }

    pub fn string_is_null(&self, ctx: &mut EvalContext, row: &[Datum]) -> Result<Option<i64>> {
        let arg = self.children[0].eval_string(ctx, row)?;
        Ok(Some(arg.is_none() as i64))
    }

    pub fn time_is_null(&self, ctx: &mut EvalContext, row: &[Datum]) -> Result<Option<i64>> {
        let arg = self.children[0].eval_time(ctx, row)?;
        Ok(Some(arg.is_none() as i64))
    }

    pub fn duration_is_null(&self, ctx: &mut EvalContext, row: &[Datum]) -> Result<Option<i64>> {
        let arg = self.children[0].eval_duration(ctx, row)?;
        Ok(Some(arg.is_none() as i64))
    }

    pub fn json_is_null(&self, ctx: &mut EvalContext, row: &[Datum]) -> Result<Option<i64>> {
        let arg = self.children[0].eval_json(ctx, row)?;
        Ok(Some(arg.is_none() as i64))
    }

    pub fn bit_and(&self, ctx: &mut EvalContext, row: &[Datum]) -> Result<Option<i64>> {
        let lhs = try_opt!(self.children[0].eval_int(ctx, row));
        let rhs = try_opt!(self.children[1].eval_int(ctx, row));
        Ok(Some(lhs & rhs))
    }

    pub fn bit_or(&self, ctx: &mut EvalContext, row: &[Datum]) -> Result<Option<i64>> {
        let lhs = try_opt!(self.children[0].eval_int(ctx, row));
        let rhs = try_opt!(self.children[1].eval_int(ctx, row));
        Ok(Some(lhs | rhs))
    }

    pub fn bit_xor(&self, ctx: &mut EvalContext, row: &[Datum]) -> Result<Option<i64>> {
        let lhs = try_opt!(self.children[0].eval_int(ctx, row));
        let rhs = try_opt!(self.children[1].eval_int(ctx, row));
        Ok(Some(lhs ^ rhs))
    }

    pub fn bit_neg(&self, ctx: &mut EvalContext, row: &[Datum]) -> Result<Option<i64>> {
        let lhs = try_opt!(self.children[0].eval_int(ctx, row));
        Ok(Some(!lhs))
    }

    pub fn left_shift(&self, ctx: &mut EvalContext, row: &[Datum]) -> Result<Option<i64>> {
        let lhs = try_opt!(self.children[0].eval_int(ctx, row));
        let rhs = try_opt!(self.children[1].eval_int(ctx, row));
        let ret = if rhs as u64 >= 64 {
            0
        } else {
            (lhs as u64).wrapping_shl(rhs as u32)
        };
        Ok(Some(ret as i64))
    }

    pub fn right_shift(&self, ctx: &mut EvalContext, row: &[Datum]) -> Result<Option<i64>> {
        let lhs = try_opt!(self.children[0].eval_int(ctx, row));
        let rhs = try_opt!(self.children[1].eval_int(ctx, row));
        let ret = if rhs as u64 >= 64 {
            0
        } else {
            (lhs as u64).wrapping_shr(rhs as u32)
        };
        Ok(Some(ret as i64))
    }
}

#[cfg(test)]
mod tests {
    use crate::tests::{check_overflow, datum_expr, scalar_func_expr, str2dec};
    use crate::Expression;
    use std::i64;
    use tidb_query_datatype::codec::mysql::Duration;
    use tidb_query_datatype::codec::Datum;
    use tidb_query_datatype::expr::EvalContext;
    use tipb::ScalarFuncSig;

    #[test]
    fn test_logic_op() {
        let tests = vec![
            (
                ScalarFuncSig::LogicalAnd,
                Datum::I64(1),
                Datum::I64(1),
                Some(1),
            ),
            (
                ScalarFuncSig::LogicalAnd,
                Datum::I64(1),
                Datum::I64(0),
                Some(0),
            ),
            (
                ScalarFuncSig::LogicalAnd,
                Datum::I64(0),
                Datum::I64(0),
                Some(0),
            ),
            (
                ScalarFuncSig::LogicalAnd,
                Datum::I64(2),
                Datum::I64(-1),
                Some(1),
            ),
            (
                ScalarFuncSig::LogicalAnd,
                Datum::I64(0),
                Datum::Null,
                Some(0),
            ),
            (ScalarFuncSig::LogicalAnd, Datum::Null, Datum::I64(1), None),
            (
                ScalarFuncSig::LogicalOr,
                Datum::I64(1),
                Datum::I64(1),
                Some(1),
            ),
            (
                ScalarFuncSig::LogicalOr,
                Datum::I64(1),
                Datum::I64(0),
                Some(1),
            ),
            (
                ScalarFuncSig::LogicalOr,
                Datum::I64(0),
                Datum::I64(0),
                Some(0),
            ),
            (
                ScalarFuncSig::LogicalOr,
                Datum::I64(2),
                Datum::I64(-1),
                Some(1),
            ),
            (
                ScalarFuncSig::LogicalOr,
                Datum::I64(1),
                Datum::Null,
                Some(1),
            ),
            (ScalarFuncSig::LogicalOr, Datum::Null, Datum::I64(0), None),
            (
                ScalarFuncSig::LogicalXor,
                Datum::I64(1),
                Datum::I64(1),
                Some(0),
            ),
            (
                ScalarFuncSig::LogicalXor,
                Datum::I64(1),
                Datum::I64(0),
                Some(1),
            ),
            (
                ScalarFuncSig::LogicalXor,
                Datum::I64(0),
                Datum::I64(0),
                Some(0),
            ),
            (
                ScalarFuncSig::LogicalXor,
                Datum::I64(2),
                Datum::I64(-1),
                Some(0),
            ),
            (ScalarFuncSig::LogicalXor, Datum::I64(0), Datum::Null, None),
            (ScalarFuncSig::LogicalXor, Datum::Null, Datum::I64(1), None),
        ];
        let mut ctx = EvalContext::default();
        for (op, lhs, rhs, exp) in tests {
            let arg1 = datum_expr(lhs);
            let arg2 = datum_expr(rhs);
            {
                let op = Expression::build(
                    &mut ctx,
                    scalar_func_expr(op, &[arg1.clone(), arg2.clone()]),
                )
                .unwrap();
                let res = op.eval_int(&mut ctx, &[]).unwrap();
                assert_eq!(res, exp);
            }
            {
                let op = Expression::build(&mut ctx, scalar_func_expr(op, &[arg2, arg1])).unwrap();
                let res = op.eval_int(&mut ctx, &[]).unwrap();
                assert_eq!(res, exp);
            }
        }
    }

    #[test]
    fn test_unary_minus_op() {
        let tests = vec![
            (
                ScalarFuncSig::UnaryMinusInt,
                Datum::I64(i64::MAX),
                Datum::I64(-i64::MAX),
            ),
            (
                ScalarFuncSig::UnaryMinusInt,
                Datum::I64(-i64::MAX),
                Datum::I64(i64::MAX),
            ),
            (ScalarFuncSig::UnaryMinusInt, Datum::I64(0), Datum::I64(0)),
            (
                ScalarFuncSig::UnaryMinusInt,
                Datum::U64(i64::MAX as u64 + 1),
                Datum::I64(i64::MIN),
            ),
            (ScalarFuncSig::UnaryMinusInt, Datum::Null, Datum::Null),
            (
                ScalarFuncSig::UnaryMinusReal,
                Datum::F64(0.123),
                Datum::F64(-0.123),
            ),
            (
                ScalarFuncSig::UnaryMinusReal,
                Datum::F64(-0.123),
                Datum::F64(0.123),
            ),
            (
                ScalarFuncSig::UnaryMinusReal,
                Datum::F64(0.0),
                Datum::F64(0.0),
            ),
            (ScalarFuncSig::UnaryMinusReal, Datum::Null, Datum::Null),
            (ScalarFuncSig::UnaryMinusDecimal, str2dec("0"), str2dec("0")),
            (
                ScalarFuncSig::UnaryMinusDecimal,
                str2dec("0.123"),
                str2dec("-0.123"),
            ),
            (ScalarFuncSig::UnaryMinusDecimal, Datum::Null, Datum::Null),
        ];
        let mut ctx = EvalContext::default();
        for (operator, arg, exp) in tests {
            let arg1 = datum_expr(arg);
            let op = Expression::build(&mut ctx, scalar_func_expr(operator, &[arg1])).unwrap();
            let res = op.eval(&mut ctx, &[]).unwrap();
            assert_eq!(res, exp);
        }
    }

    #[test]
    fn test_unary_op() {
        let tests = vec![
            (ScalarFuncSig::UnaryNotInt, Datum::I64(1), Some(0)),
            (ScalarFuncSig::UnaryNotInt, Datum::I64(0), Some(1)),
            (ScalarFuncSig::UnaryNotInt, Datum::I64(123), Some(0)),
            (ScalarFuncSig::UnaryNotInt, Datum::I64(-123), Some(0)),
            (ScalarFuncSig::UnaryNotInt, Datum::Null, None),
            (ScalarFuncSig::UnaryNotReal, Datum::F64(0.3), Some(0)),
            (ScalarFuncSig::UnaryNotReal, Datum::F64(0.0), Some(1)),
            (ScalarFuncSig::UnaryNotReal, Datum::Null, None),
            (ScalarFuncSig::UnaryNotDecimal, str2dec("0.3"), Some(0)),
            (ScalarFuncSig::UnaryNotDecimal, str2dec("0"), Some(1)),
            (ScalarFuncSig::UnaryNotDecimal, Datum::Null, None),
            (ScalarFuncSig::RealIsTrue, Datum::F64(0.25), Some(1)),
            (ScalarFuncSig::RealIsTrueWithNull, Datum::F64(0.25), Some(1)),
            (ScalarFuncSig::RealIsTrue, Datum::F64(0.0), Some(0)),
            (ScalarFuncSig::RealIsTrueWithNull, Datum::F64(0.0), Some(0)),
            (ScalarFuncSig::RealIsTrue, Datum::Null, Some(0)),
            (ScalarFuncSig::RealIsTrueWithNull, Datum::Null, None),
            (ScalarFuncSig::RealIsFalse, Datum::F64(0.25), Some(0)),
            (
                ScalarFuncSig::RealIsFalseWithNull,
                Datum::F64(0.25),
                Some(0),
            ),
            (ScalarFuncSig::RealIsFalse, Datum::F64(0.000), Some(1)),
            (
                ScalarFuncSig::RealIsFalseWithNull,
                Datum::F64(0.000),
                Some(1),
            ),
            (ScalarFuncSig::RealIsFalse, Datum::F64(-0.00), Some(1)),
            (
                ScalarFuncSig::RealIsFalseWithNull,
                Datum::F64(-0.00),
                Some(1),
            ),
            (ScalarFuncSig::RealIsFalse, Datum::F64(-0.011), Some(0)),
            (
                ScalarFuncSig::RealIsFalseWithNull,
                Datum::F64(-0.011),
                Some(0),
            ),
            (ScalarFuncSig::RealIsFalse, Datum::Null, Some(0)),
            (ScalarFuncSig::RealIsFalseWithNull, Datum::Null, None),
            (ScalarFuncSig::RealIsNull, Datum::F64(1.25), Some(0)),
            (ScalarFuncSig::RealIsNull, Datum::Null, Some(1)),
            (ScalarFuncSig::DecimalIsTrue, str2dec("1.1"), Some(1)),
            (
                ScalarFuncSig::DecimalIsTrueWithNull,
                str2dec("1.1"),
                Some(1),
            ),
            (ScalarFuncSig::DecimalIsTrue, str2dec("0"), Some(0)),
            (ScalarFuncSig::DecimalIsTrueWithNull, str2dec("0"), Some(0)),
            (ScalarFuncSig::DecimalIsTrue, Datum::Null, Some(0)),
            (ScalarFuncSig::DecimalIsTrueWithNull, Datum::Null, None),
            (ScalarFuncSig::DecimalIsFalse, str2dec("1.1"), Some(0)),
            (
                ScalarFuncSig::DecimalIsFalseWithNull,
                str2dec("1.1"),
                Some(0),
            ),
            (ScalarFuncSig::DecimalIsFalse, str2dec("0"), Some(1)),
            (ScalarFuncSig::DecimalIsFalseWithNull, str2dec("0"), Some(1)),
            (ScalarFuncSig::DecimalIsFalse, Datum::Null, Some(0)),
            (ScalarFuncSig::DecimalIsFalseWithNull, Datum::Null, None),
            (ScalarFuncSig::DecimalIsNull, str2dec("1.1"), Some(0)),
            (ScalarFuncSig::DecimalIsNull, Datum::Null, Some(1)),
            (ScalarFuncSig::IntIsTrue, Datum::I64(0), Some(0)),
            (ScalarFuncSig::IntIsTrueWithNull, Datum::I64(0), Some(0)),
            (ScalarFuncSig::IntIsTrue, Datum::I64(12), Some(1)),
            (ScalarFuncSig::IntIsTrueWithNull, Datum::I64(12), Some(1)),
            (ScalarFuncSig::IntIsTrue, Datum::Null, Some(0)),
            (ScalarFuncSig::IntIsTrueWithNull, Datum::Null, None),
            (ScalarFuncSig::IntIsFalse, Datum::I64(0), Some(1)),
            (ScalarFuncSig::IntIsFalseWithNull, Datum::I64(0), Some(1)),
            (ScalarFuncSig::IntIsFalse, Datum::I64(1), Some(0)),
            (ScalarFuncSig::IntIsFalseWithNull, Datum::I64(1), Some(0)),
            (ScalarFuncSig::IntIsFalse, Datum::Null, Some(0)),
            (ScalarFuncSig::IntIsFalseWithNull, Datum::Null, None),
            (ScalarFuncSig::IntIsNull, Datum::I64(1), Some(0)),
            (ScalarFuncSig::IntIsNull, Datum::Null, Some(1)),
            (ScalarFuncSig::StringIsNull, Datum::Null, Some(1)),
            (
                ScalarFuncSig::StringIsNull,
                Datum::Bytes(b"abc".to_vec()),
                Some(0),
            ),
            (ScalarFuncSig::TimeIsNull, Datum::Null, Some(1)),
            // TODO: add Time related tests after Time is implemented in Expression::build
            (
                ScalarFuncSig::DurationIsNull,
                Datum::Dur(Duration::zero()),
                Some(0),
            ),
            (ScalarFuncSig::DurationIsNull, Datum::Null, Some(1)),
        ];
        let mut ctx = EvalContext::default();
        for (op, arg, exp) in tests {
            let arg1 = datum_expr(arg);
            let op = Expression::build(&mut ctx, scalar_func_expr(op, &[arg1])).unwrap();
            let res = op.eval_int(&mut ctx, &[]).unwrap();
            assert_eq!(res, exp);
        }
    }

    #[test]
    fn test_unary_op_overflow() {
        let tests = vec![
            (ScalarFuncSig::UnaryMinusInt, Datum::I64(i64::MIN)),
            (
                ScalarFuncSig::UnaryMinusInt,
                Datum::U64(i64::MAX as u64 + 2),
            ),
        ];
        let mut ctx = EvalContext::default();
        for (op, argument) in tests {
            let arg = datum_expr(argument);
            let op = Expression::build(&mut ctx, scalar_func_expr(op, &[arg])).unwrap();
            let got = op.eval(&mut ctx, &[]).unwrap_err();
            assert!(check_overflow(got).is_ok());
        }
    }

    #[test]
    fn test_bit_and() {
        let cases = vec![
            (Datum::I64(123), Datum::I64(321), Datum::I64(65)),
            (Datum::I64(-123), Datum::I64(321), Datum::I64(257)),
            (Datum::Null, Datum::I64(1), Datum::Null),
        ];
        let mut ctx = EvalContext::default();
        for (lhs, rhs, exp) in cases {
            let args = &[datum_expr(lhs), datum_expr(rhs)];
            let op = Expression::build(&mut ctx, scalar_func_expr(ScalarFuncSig::BitAndSig, args))
                .unwrap();
            let res = op.eval(&mut ctx, &[]).unwrap();
            assert_eq!(res, exp);
        }
    }

    #[test]
    fn test_bit_or() {
        let cases = vec![
            (Datum::I64(123), Datum::I64(321), Datum::I64(379)),
            (Datum::I64(-123), Datum::I64(321), Datum::I64(-59)),
            (Datum::Null, Datum::I64(1), Datum::Null),
        ];
        let mut ctx = EvalContext::default();
        for (lhs, rhs, exp) in cases {
            let args = &[datum_expr(lhs), datum_expr(rhs)];
            let op = Expression::build(&mut ctx, scalar_func_expr(ScalarFuncSig::BitOrSig, args))
                .unwrap();
            let res = op.eval(&mut ctx, &[]).unwrap();
            assert_eq!(res, exp);
        }
    }

    #[test]
    fn test_bit_xor() {
        let cases = vec![
            (Datum::I64(123), Datum::I64(321), Datum::I64(314)),
            (Datum::I64(-123), Datum::I64(321), Datum::I64(-316)),
            (Datum::Null, Datum::I64(1), Datum::Null),
        ];
        let mut ctx = EvalContext::default();
        for (lhs, rhs, exp) in cases {
            let args = &[datum_expr(lhs), datum_expr(rhs)];
            let op = Expression::build(&mut ctx, scalar_func_expr(ScalarFuncSig::BitXorSig, args))
                .unwrap();
            let res = op.eval(&mut ctx, &[]).unwrap();
            assert_eq!(res, exp);
        }
    }

    #[test]
    fn test_bit_neg() {
        let cases = vec![
            (Datum::I64(123), Datum::I64(-124)),
            (Datum::I64(-123), Datum::I64(122)),
            (Datum::Null, Datum::Null),
        ];
        let mut ctx = EvalContext::default();
        for (arg, exp) in cases {
            let args = &[datum_expr(arg)];
            let op = Expression::build(&mut ctx, scalar_func_expr(ScalarFuncSig::BitNegSig, args))
                .unwrap();
            let res = op.eval(&mut ctx, &[]).unwrap();
            assert_eq!(res, exp);
        }
    }

    #[test]
    fn test_left_shift() {
        let cases = vec![
            (Datum::I64(123), Datum::I64(2), Datum::I64(492)),
            (Datum::I64(-123), Datum::I64(-1), Datum::I64(0)),
            (Datum::I64(123), Datum::I64(0), Datum::I64(123)),
            (Datum::Null, Datum::I64(1), Datum::Null),
            (Datum::I64(123), Datum::Null, Datum::Null),
            (
                Datum::I64(-123),
                Datum::I64(60),
                Datum::I64(5764607523034234880),
            ),
        ];
        let mut ctx = EvalContext::default();
        for (lhs, rhs, exp) in cases {
            let lhs = datum_expr(lhs);
            let rhs = datum_expr(rhs);
            let op = Expression::build(
                &mut ctx,
                scalar_func_expr(ScalarFuncSig::LeftShift, &[lhs, rhs]),
            )
            .unwrap();
            let res = op.eval(&mut ctx, &[]).unwrap();
            assert_eq!(res, exp);
        }
    }

    #[test]
    fn test_right_shift() {
        let cases = vec![
            (Datum::I64(123), Datum::I64(2), Datum::I64(30)),
            (Datum::I64(-123), Datum::I64(-1), Datum::I64(0)),
            (Datum::I64(123), Datum::I64(0), Datum::I64(123)),
            (Datum::Null, Datum::I64(1), Datum::Null),
            (Datum::I64(123), Datum::Null, Datum::Null),
            (
                Datum::I64(-123),
                Datum::I64(2),
                Datum::I64(4611686018427387873),
            ),
        ];
        let mut ctx = EvalContext::default();
        for (lhs, rhs, exp) in cases {
            let lhs = datum_expr(lhs);
            let rhs = datum_expr(rhs);
            let op = Expression::build(
                &mut ctx,
                scalar_func_expr(ScalarFuncSig::RightShift, &[lhs, rhs]),
            )
            .unwrap();
            let res = op.eval(&mut ctx, &[]).unwrap();
            assert_eq!(res, exp);
        }
    }
}
