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

use super::{Error, EvalContext, FnCall, Result};
use coprocessor::codec::mysql::{Decimal, Res};
use coprocessor::codec::{mysql, Datum, div_i64, div_i64_with_u64, div_u64_with_i64};
use std::borrow::Cow;
use std::ops::{Add, Mul, Sub};
use std::{f64, i64, u64};

impl FnCall {
    pub fn plus_real(&self, ctx: &mut EvalContext, row: &[Datum]) -> Result<Option<f64>> {
        let lhs = try_opt!(self.children[0].eval_real(ctx, row));
        let rhs = try_opt!(self.children[1].eval_real(ctx, row));
        let res = lhs + rhs;
        if !res.is_finite() {
            return Err(Error::overflow("DOUBLE", &format!("({} + {})", lhs, rhs)));
        }
        Ok(Some(res))
    }

    pub fn plus_decimal<'a, 'b: 'a>(
        &'b self,
        ctx: &mut EvalContext,
        row: &'a [Datum],
    ) -> Result<Option<Cow<'a, Decimal>>> {
        let lhs = try_opt!(self.children[0].eval_decimal(ctx, row));
        let rhs = try_opt!(self.children[1].eval_decimal(ctx, row));
        let result: Result<Decimal> = lhs.add(&rhs).into();
        result.map(|t| Some(Cow::Owned(t)))
    }

    pub fn plus_int(&self, ctx: &mut EvalContext, row: &[Datum]) -> Result<Option<i64>> {
        let lhs = try_opt!(self.children[0].eval_int(ctx, row));
        let rhs = try_opt!(self.children[1].eval_int(ctx, row));
        let lus = mysql::has_unsigned_flag(self.children[0].get_tp().get_flag());
        let rus = mysql::has_unsigned_flag(self.children[1].get_tp().get_flag());
        let res = match (lus, rus) {
            (true, true) => (lhs as u64).checked_add(rhs as u64).map(|t| t as i64),
            (true, false) => if rhs >= 0 {
                (lhs as u64).checked_add(rhs as u64).map(|t| t as i64)
            } else {
                (lhs as u64)
                    .checked_sub(rhs.overflowing_neg().0 as u64)
                    .map(|t| t as i64)
            },
            (false, true) => if lhs >= 0 {
                (lhs as u64).checked_add(rhs as u64).map(|t| t as i64)
            } else {
                (rhs as u64)
                    .checked_sub(lhs.overflowing_neg().0 as u64)
                    .map(|t| t as i64)
            },
            (false, false) => lhs.checked_add(rhs),
        };
        let data_type = if lus | rus {
            "BIGINT UNSIGNED"
        } else {
            "BIGINT"
        };
        res.ok_or_else(|| Error::overflow(data_type, &format!("({} + {})", lhs, rhs)))
            .map(Some)
    }

    pub fn minus_real(&self, ctx: &mut EvalContext, row: &[Datum]) -> Result<Option<f64>> {
        let lhs = try_opt!(self.children[0].eval_real(ctx, row));
        let rhs = try_opt!(self.children[1].eval_real(ctx, row));
        let res = lhs - rhs;
        if !res.is_finite() {
            return Err(Error::overflow("DOUBLE", &format!("({} - {})", lhs, rhs)));
        }
        Ok(Some(res))
    }

    pub fn minus_decimal<'a, 'b: 'a>(
        &'b self,
        ctx: &mut EvalContext,
        row: &'a [Datum],
    ) -> Result<Option<Cow<'a, Decimal>>> {
        let lhs = try_opt!(self.children[0].eval_decimal(ctx, row));
        let rhs = try_opt!(self.children[1].eval_decimal(ctx, row));
        let result: Result<Decimal> = lhs.sub(&rhs).into();
        result.map(Cow::Owned).map(Some)
    }

    pub fn minus_int(&self, ctx: &mut EvalContext, row: &[Datum]) -> Result<Option<i64>> {
        let lhs = try_opt!(self.children[0].eval_int(ctx, row));
        let rhs = try_opt!(self.children[1].eval_int(ctx, row));
        let lus = mysql::has_unsigned_flag(self.children[0].get_tp().get_flag());
        let rus = mysql::has_unsigned_flag(self.children[1].get_tp().get_flag());
        let data_type = if lus | rus {
            "BIGINT UNSIGNED"
        } else {
            "BIGINT"
        };
        let res = match (lus, rus) {
            (true, true) => (lhs as u64).checked_sub(rhs as u64).map(|t| t as i64),
            (true, false) => if rhs >= 0 {
                (lhs as u64).checked_sub(rhs as u64).map(|t| t as i64)
            } else {
                (lhs as u64)
                    .checked_add(rhs.overflowing_neg().0 as u64)
                    .map(|t| t as i64)
            },
            (false, true) => if lhs >= 0 {
                (lhs as u64).checked_sub(rhs as u64).map(|t| t as i64)
            } else {
                return Err(Error::overflow(data_type, &format!("({} - {})", lhs, rhs)));
            },
            (false, false) => lhs.checked_sub(rhs),
        };
        res.ok_or_else(|| Error::overflow(data_type, &format!("({} - {})", lhs, rhs)))
            .map(Some)
    }

    pub fn multiply_real(&self, ctx: &mut EvalContext, row: &[Datum]) -> Result<Option<f64>> {
        let lhs = try_opt!(self.children[0].eval_real(ctx, row));
        let rhs = try_opt!(self.children[1].eval_real(ctx, row));
        let res = lhs * rhs;
        if !res.is_finite() {
            return Err(Error::overflow("DOUBLE", &format!("({} * {})", lhs, rhs)));
        }
        Ok(Some(res))
    }

    pub fn multiply_decimal<'a, 'b: 'a>(
        &'b self,
        ctx: &mut EvalContext,
        row: &'a [Datum],
    ) -> Result<Option<Cow<'a, Decimal>>> {
        let lhs = try_opt!(self.children[0].eval_decimal(ctx, row));
        let rhs = try_opt!(self.children[1].eval_decimal(ctx, row));
        let result: Result<Decimal> = lhs.mul(&rhs).into();
        result.map(Cow::Owned).map(Some)
    }

    pub fn multiply_int(&self, ctx: &mut EvalContext, row: &[Datum]) -> Result<Option<i64>> {
        let lhs = try_opt!(self.children[0].eval_int(ctx, row));
        let rhs = try_opt!(self.children[1].eval_int(ctx, row));
        let lus = mysql::has_unsigned_flag(self.children[0].get_tp().get_flag());
        let rus = mysql::has_unsigned_flag(self.children[1].get_tp().get_flag());
        let u64_mul_i64 = |u, s| {
            if s >= 0 {
                (u as u64).checked_mul(s as u64).map(|t| t as i64)
            } else {
                None
            }
        };
        let res = match (lus, rus) {
            (true, true) => (lhs as u64).checked_mul(rhs as u64).map(|t| t as i64),
            (false, false) => lhs.checked_mul(rhs),
            (true, false) => u64_mul_i64(lhs, rhs),
            (false, true) => u64_mul_i64(rhs, lhs),
        };
        res.ok_or_else(|| Error::overflow("BIGINT UNSIGNED", &format!("({} * {})", lhs, rhs)))
            .map(Some)
    }

    pub fn divide_real(&self, ctx: &mut EvalContext, row: &[Datum]) -> Result<Option<f64>> {
        let lhs = try_opt!(self.children[0].eval_real(ctx, row));
        let rhs = try_opt!(self.children[1].eval_real(ctx, row));
        if rhs == 0f64 {
            return ctx.handle_division_by_zero().map(|()| None);
        }
        let res = lhs / rhs;
        if res.is_infinite() {
            Err(Error::overflow("DOUBLE", &format!("({} / {})", lhs, rhs)))
        } else {
            Ok(Some(res))
        }
    }

    pub fn divide_decimal<'a, 'b: 'a>(
        &'b self,
        ctx: &mut EvalContext,
        row: &'a [Datum],
    ) -> Result<Option<Cow<'a, Decimal>>> {
        let lhs = try_opt!(self.children[0].eval_decimal(ctx, row));
        let rhs = try_opt!(self.children[1].eval_decimal(ctx, row));
        let overflow = Error::overflow("DECIMAL", &format!("({} / {})", lhs, rhs));
        match lhs.into_owned() / rhs.into_owned() {
            Some(v) => match v {
                Res::Ok(v) => Ok(Some(Cow::Owned(v))),
                Res::Truncated(_) => Err(Error::truncated()),
                Res::Overflow(_) => Err(overflow),
            },
            None => ctx.handle_division_by_zero().map(|()| None),
        }
    }

    pub fn int_divide_int(&self, ctx: &mut EvalContext, row: &[Datum]) -> Result<Option<i64>> {
        let rhs = try_opt!(self.children[1].eval_int(ctx, row));
        if rhs == 0 {
            return Ok(None);
        }
        let rus = mysql::has_unsigned_flag(self.children[1].get_tp().get_flag());

        let lhs = try_opt!(self.children[0].eval_int(ctx, row));
        let lus = mysql::has_unsigned_flag(self.children[0].get_tp().get_flag());

        let res = match (lus, rus) {
            (true, true) => Ok(((lhs as u64) / (rhs as u64)) as i64),
            (false, false) => div_i64(lhs, rhs),
            (false, true) => div_i64_with_u64(lhs, rhs as u64).map(|r| r as i64),
            (true, false) => div_u64_with_i64(lhs as u64, rhs).map(|r| r as i64),
        };
        res.map(Some)
    }

    pub fn int_divide_decimal(&self, ctx: &mut EvalContext, row: &[Datum]) -> Result<Option<i64>> {
        match self.divide_decimal(ctx, row) {
            Ok(Some(v)) => match v.as_i64() {
                Res::Ok(v_i64) => Ok(Some(v_i64)),
                Res::Truncated(v_i64) => Ok(Some(v_i64)),
                Res::Overflow(_) => Err(Error::overflow("BIGINT", &v.to_string())),
            },
            Ok(None) => Ok(None),
            Err(e) => Err(e),
        }
    }

    pub fn mod_real(&self, ctx: &mut EvalContext, row: &[Datum]) -> Result<Option<f64>> {
        let rhs = try_opt!(self.children[1].eval_real(ctx, row));
        if rhs == 0f64 {
            return Ok(None);
        }
        let lhs = try_opt!(self.children[0].eval_real(ctx, row));

        let res = lhs % rhs;
        Ok(Some(res))
    }

    pub fn mod_decimal<'a, 'b: 'a>(
        &'b self,
        ctx: &mut EvalContext,
        row: &'a [Datum],
    ) -> Result<Option<Cow<'a, Decimal>>> {
        let rhs = try_opt!(self.children[1].eval_decimal(ctx, row));
        let lhs = try_opt!(self.children[0].eval_decimal(ctx, row));
        let overflow = Error::overflow("DECIMAL", &format!("({} % {})", lhs, rhs));
        match lhs.into_owned() % rhs.into_owned() {
            Some(v) => match v {
                Res::Ok(v) => Ok(Some(Cow::Owned(v))),
                Res::Truncated(_) => Err(Error::truncated()),
                Res::Overflow(_) => Err(overflow),
            },
            None => Ok(None),
        }
    }

    pub fn mod_int(&self, ctx: &mut EvalContext, row: &[Datum]) -> Result<Option<i64>> {
        let rhs = try_opt!(self.children[1].eval_int(ctx, row));
        if rhs == 0 {
            return Ok(None);
        }
        let rus = mysql::has_unsigned_flag(self.children[1].get_tp().get_flag());

        let lhs = try_opt!(self.children[0].eval_int(ctx, row));
        let lus = mysql::has_unsigned_flag(self.children[0].get_tp().get_flag());

        let res = match (lus, rus) {
            (true, true) => ((lhs as u64) % (rhs as u64)) as i64,
            (false, false) => lhs % rhs,
            (true, false) => ((lhs as u64) % (rhs.overflowing_abs().0 as u64)) as i64,
            (false, true) => ((lhs.overflowing_abs().0 as u64) % (rhs as u64)) as i64,
        };
        Ok(Some(res))
    }
}

#[cfg(test)]
mod test {
    use coprocessor::codec::error::ERR_DIVISION_BY_ZERO;
    use coprocessor::codec::mysql::{types, Decimal};
    use coprocessor::codec::{mysql, Datum};
    use coprocessor::dag::expr::test::{check_divide_by_zero, check_overflow, datum_expr,
                                       fncall_expr, str2dec};
    use coprocessor::dag::expr::*;
    use std::sync::Arc;
    use std::{f64, i64, u64};
    use tipb::expression::ScalarFuncSig;

    #[test]
    fn test_arithmetic_int() {
        let tests = vec![
            (
                ScalarFuncSig::PlusInt,
                Datum::Null,
                Datum::I64(1),
                Datum::Null,
            ),
            (
                ScalarFuncSig::PlusInt,
                Datum::I64(1),
                Datum::Null,
                Datum::Null,
            ),
            (
                ScalarFuncSig::PlusInt,
                Datum::I64(12),
                Datum::I64(1),
                Datum::I64(13),
            ),
            (
                ScalarFuncSig::PlusInt,
                Datum::I64(i64::MIN),
                Datum::U64(i64::MAX as u64 + 1),
                Datum::U64(0),
            ),
            (
                ScalarFuncSig::MinusInt,
                Datum::I64(12),
                Datum::I64(1),
                Datum::I64(11),
            ),
            (
                ScalarFuncSig::MinusInt,
                Datum::U64(0),
                Datum::I64(i64::MIN),
                Datum::U64(i64::MAX as u64 + 1),
            ),
            (
                ScalarFuncSig::MultiplyInt,
                Datum::I64(12),
                Datum::I64(1),
                Datum::I64(12),
            ),
            (
                ScalarFuncSig::MultiplyInt,
                Datum::I64(i64::MIN),
                Datum::I64(1),
                Datum::I64(i64::MIN),
            ),
            (
                ScalarFuncSig::IntDivideInt,
                Datum::I64(13),
                Datum::I64(11),
                Datum::I64(1),
            ),
            (
                ScalarFuncSig::IntDivideInt,
                Datum::I64(-13),
                Datum::I64(11),
                Datum::I64(-1),
            ),
            (
                ScalarFuncSig::IntDivideInt,
                Datum::I64(13),
                Datum::I64(-11),
                Datum::I64(-1),
            ),
            (
                ScalarFuncSig::IntDivideInt,
                Datum::I64(-13),
                Datum::I64(-11),
                Datum::I64(1),
            ),
            (
                ScalarFuncSig::IntDivideInt,
                Datum::I64(33),
                Datum::I64(11),
                Datum::I64(3),
            ),
            (
                ScalarFuncSig::IntDivideInt,
                Datum::I64(-33),
                Datum::I64(11),
                Datum::I64(-3),
            ),
            (
                ScalarFuncSig::IntDivideInt,
                Datum::I64(33),
                Datum::I64(-11),
                Datum::I64(-3),
            ),
            (
                ScalarFuncSig::IntDivideInt,
                Datum::I64(-33),
                Datum::I64(-11),
                Datum::I64(3),
            ),
            (
                ScalarFuncSig::IntDivideInt,
                Datum::I64(11),
                Datum::I64(0),
                Datum::Null,
            ),
            (
                ScalarFuncSig::IntDivideInt,
                Datum::I64(-11),
                Datum::I64(0),
                Datum::Null,
            ),
            (
                ScalarFuncSig::IntDivideInt,
                Datum::U64(3),
                Datum::I64(-5),
                Datum::U64(0),
            ),
            (
                ScalarFuncSig::IntDivideInt,
                Datum::I64(-3),
                Datum::U64(5),
                Datum::U64(0),
            ),
            (
                ScalarFuncSig::IntDivideInt,
                Datum::I64(i64::MIN + 1),
                Datum::I64(-1),
                Datum::I64(i64::MAX),
            ),
            (
                ScalarFuncSig::IntDivideInt,
                Datum::I64(i64::MIN),
                Datum::I64(1),
                Datum::I64(i64::MIN),
            ),
            (
                ScalarFuncSig::IntDivideInt,
                Datum::I64(i64::MAX),
                Datum::I64(1),
                Datum::I64(i64::MAX),
            ),
            (
                ScalarFuncSig::IntDivideInt,
                Datum::U64(u64::MAX),
                Datum::I64(1),
                Datum::U64(u64::MAX),
            ),
            (
                ScalarFuncSig::ModInt,
                Datum::I64(13),
                Datum::I64(11),
                Datum::I64(2),
            ),
            (
                ScalarFuncSig::ModInt,
                Datum::I64(-13),
                Datum::I64(11),
                Datum::I64(-2),
            ),
            (
                ScalarFuncSig::ModInt,
                Datum::I64(13),
                Datum::I64(-11),
                Datum::I64(2),
            ),
            (
                ScalarFuncSig::ModInt,
                Datum::I64(-13),
                Datum::I64(-11),
                Datum::I64(-2),
            ),
            (
                ScalarFuncSig::ModInt,
                Datum::I64(33),
                Datum::I64(11),
                Datum::I64(0),
            ),
            (
                ScalarFuncSig::ModInt,
                Datum::I64(33),
                Datum::I64(-11),
                Datum::I64(0),
            ),
            (
                ScalarFuncSig::ModInt,
                Datum::I64(-33),
                Datum::I64(-11),
                Datum::I64(0),
            ),
            (
                ScalarFuncSig::ModInt,
                Datum::I64(11),
                Datum::I64(0),
                Datum::Null,
            ),
            (
                ScalarFuncSig::ModInt,
                Datum::I64(-11),
                Datum::I64(0),
                Datum::Null,
            ),
            (
                ScalarFuncSig::ModInt,
                Datum::I64(i64::MAX),
                Datum::I64(i64::MIN),
                Datum::I64(i64::MAX),
            ),
            (
                ScalarFuncSig::ModInt,
                Datum::I64(i64::MIN),
                Datum::I64(i64::MAX),
                Datum::I64(-1),
            ),
            (
                ScalarFuncSig::ModInt,
                Datum::U64(u64::MAX),
                Datum::I64(i64::MIN),
                Datum::U64(i64::MAX as u64),
            ),
            (
                ScalarFuncSig::ModInt,
                Datum::I64(i64::MIN),
                Datum::U64(u64::MAX),
                Datum::U64(i64::MIN as u64),
            ),
        ];
        let mut ctx = EvalContext::default();
        for tt in tests {
            let lhs = datum_expr(tt.1);
            let rhs = datum_expr(tt.2);

            let lus = mysql::has_unsigned_flag(lhs.get_field_type().get_flag());
            let rus = mysql::has_unsigned_flag(rhs.get_field_type().get_flag());
            let unsigned = lus | rus;

            let mut op = Expression::build(&mut ctx, fncall_expr(tt.0, &[lhs, rhs])).unwrap();
            if unsigned {
                // According to TiDB, the result is unsigned if any of arguments is unsigned.
                op.mut_tp().set_flag(types::UNSIGNED_FLAG as u32);
            }

            let got = op.eval(&mut ctx, &[]).unwrap();
            assert_eq!(got, tt.3);
        }
    }

    #[test]
    fn test_arithmetic_real() {
        let tests = vec![
            (
                ScalarFuncSig::PlusReal,
                Datum::F64(1.01001),
                Datum::F64(-0.01),
                Datum::F64(1.00001),
            ),
            (
                ScalarFuncSig::MinusReal,
                Datum::F64(1.01001),
                Datum::F64(-0.01),
                Datum::F64(1.02001),
            ),
            (
                ScalarFuncSig::MultiplyReal,
                Datum::F64(1.01001),
                Datum::F64(-0.01),
                Datum::F64(-0.0101001),
            ),
            (
                ScalarFuncSig::DivideReal,
                Datum::F64(2.0),
                Datum::F64(0.3),
                Datum::F64(6.666666666666667),
            ),
            (
                ScalarFuncSig::DivideReal,
                Datum::F64(44.3),
                Datum::F64(0.000),
                Datum::Null,
            ),
            (
                ScalarFuncSig::DivideReal,
                Datum::Null,
                Datum::F64(1.0),
                Datum::Null,
            ),
            (
                ScalarFuncSig::DivideReal,
                Datum::F64(1.0),
                Datum::Null,
                Datum::Null,
            ), // TODO: support precision in divide.
            // (
            //     ScalarFuncSig::DivideReal,
            //     Datum::F64(-12.3),
            //     Datum::F64(41f64),
            //     Datum::F64(-0.3),
            // ),
            // (
            //     ScalarFuncSig::DivideReal,
            //     Datum::F64(12.3),
            //     Datum::F64(0.3),
            //     Datum::F64(41f64)
            // )
            (
                ScalarFuncSig::ModReal,
                Datum::F64(1.0),
                Datum::F64(1.1),
                Datum::F64(1.0),
            ),
            (
                ScalarFuncSig::ModReal,
                Datum::F64(-1.0),
                Datum::F64(1.1),
                Datum::F64(-1.0),
            ),
            (
                ScalarFuncSig::ModReal,
                Datum::F64(1.0),
                Datum::F64(-1.1),
                Datum::F64(1.0),
            ),
            (
                ScalarFuncSig::ModReal,
                Datum::F64(-1.0),
                Datum::F64(-1.1),
                Datum::F64(-1.0),
            ),
            (
                ScalarFuncSig::ModReal,
                Datum::F64(1.0),
                Datum::F64(0.0),
                Datum::Null,
            ),
        ];
        let mut ctx = EvalContext::default();
        for tt in tests {
            let lhs = datum_expr(tt.1);
            let rhs = datum_expr(tt.2);

            let op = Expression::build(&mut ctx, fncall_expr(tt.0, &[lhs, rhs])).unwrap();
            let got = op.eval(&mut ctx, &[]).unwrap();
            assert_eq!(got, tt.3);
        }
    }

    #[test]
    fn test_arithmetic_decimal() {
        let tests = vec![
            (
                ScalarFuncSig::PlusDecimal,
                str2dec("1.1"),
                str2dec("2.2"),
                str2dec("3.3"),
            ),
            (
                ScalarFuncSig::MinusDecimal,
                str2dec("1.1"),
                str2dec("2.2"),
                str2dec("-1.1"),
            ),
            (
                ScalarFuncSig::MultiplyDecimal,
                str2dec("1.1"),
                str2dec("2.2"),
                str2dec("2.42"),
            ),
            (
                ScalarFuncSig::DivideDecimal,
                str2dec("12.3"),
                str2dec("-0.3"),
                str2dec("-41"),
            ),
            (
                ScalarFuncSig::DivideDecimal,
                str2dec("12.3"),
                str2dec("0.3"),
                str2dec("41"),
            ),
            (
                ScalarFuncSig::DivideDecimal,
                str2dec("12.3"),
                str2dec("0"),
                Datum::Null,
            ),
            (
                ScalarFuncSig::DivideDecimal,
                Datum::Null,
                str2dec("123"),
                Datum::Null,
            ),
            (
                ScalarFuncSig::DivideDecimal,
                str2dec("123"),
                Datum::Null,
                Datum::Null,
            ),
            (
                ScalarFuncSig::IntDivideDecimal,
                str2dec("11.01"),
                str2dec("1.1"),
                Datum::I64(10),
            ),
            (
                ScalarFuncSig::IntDivideDecimal,
                str2dec("-11.01"),
                str2dec("1.1"),
                Datum::I64(-10),
            ),
            (
                ScalarFuncSig::IntDivideDecimal,
                str2dec("11.01"),
                str2dec("-1.1"),
                Datum::I64(-10),
            ),
            (
                ScalarFuncSig::IntDivideDecimal,
                str2dec("-11.01"),
                str2dec("-1.1"),
                Datum::I64(10),
            ),
            (
                ScalarFuncSig::IntDivideDecimal,
                str2dec("123"),
                Datum::Null,
                Datum::Null,
            ),
            (
                ScalarFuncSig::IntDivideDecimal,
                Datum::Null,
                str2dec("123"),
                Datum::Null,
            ),
            (
                ScalarFuncSig::IntDivideDecimal,
                str2dec("0.0"),
                str2dec("0"),
                Datum::Null,
            ),
            (
                ScalarFuncSig::IntDivideDecimal,
                Datum::Null,
                Datum::Null,
                Datum::Null,
            ),
            (
                ScalarFuncSig::ModDecimal,
                str2dec("13"),
                str2dec("11"),
                str2dec("2"),
            ),
            (
                ScalarFuncSig::ModDecimal,
                str2dec("-13"),
                str2dec("11"),
                str2dec("-2"),
            ),
            (
                ScalarFuncSig::ModDecimal,
                str2dec("13"),
                str2dec("-11"),
                str2dec("2"),
            ),
            (
                ScalarFuncSig::ModDecimal,
                str2dec("-13"),
                str2dec("-11"),
                str2dec("-2"),
            ),
            (
                ScalarFuncSig::ModDecimal,
                str2dec("33"),
                str2dec("11"),
                str2dec("0"),
            ),
            (
                ScalarFuncSig::ModDecimal,
                str2dec("-33"),
                str2dec("11"),
                str2dec("0"),
            ),
            (
                ScalarFuncSig::ModDecimal,
                str2dec("33"),
                str2dec("-11"),
                str2dec("0"),
            ),
            (
                ScalarFuncSig::ModDecimal,
                str2dec("-33"),
                str2dec("-11"),
                str2dec("0"),
            ),
            (
                ScalarFuncSig::ModDecimal,
                str2dec("1"),
                str2dec("1.1"),
                str2dec("1"),
            ),
            (
                ScalarFuncSig::ModDecimal,
                str2dec("-1"),
                str2dec("1.1"),
                str2dec("-1"),
            ),
            (
                ScalarFuncSig::ModDecimal,
                str2dec("1"),
                str2dec("-1.1"),
                str2dec("1"),
            ),
            (
                ScalarFuncSig::ModDecimal,
                str2dec("-1"),
                str2dec("-1.1"),
                str2dec("-1"),
            ),
            (
                ScalarFuncSig::ModDecimal,
                str2dec("3"),
                str2dec("0"),
                Datum::Null,
            ),
            (
                ScalarFuncSig::ModDecimal,
                str2dec("-3"),
                str2dec("0"),
                Datum::Null,
            ),
            (
                ScalarFuncSig::ModDecimal,
                str2dec("0"),
                str2dec("0"),
                Datum::Null,
            ),
            (
                ScalarFuncSig::ModDecimal,
                str2dec("-3"),
                Datum::Null,
                Datum::Null,
            ),
            (
                ScalarFuncSig::ModDecimal,
                Datum::Null,
                str2dec("-3"),
                Datum::Null,
            ),
            (
                ScalarFuncSig::ModDecimal,
                Datum::Null,
                Datum::Null,
                Datum::Null,
            ),
        ];
        let mut ctx = EvalContext::default();
        for tt in tests {
            let lhs = datum_expr(tt.1);
            let rhs = datum_expr(tt.2);

            let op = Expression::build(&mut ctx, fncall_expr(tt.0, &[lhs, rhs])).unwrap();
            let got = op.eval(&mut ctx, &[]).unwrap();
            assert_eq!(got, tt.3);
        }
    }

    #[test]
    fn test_arithmetic_overflow_int() {
        let tests = vec![
            (
                ScalarFuncSig::PlusInt,
                Datum::I64(i64::MAX),
                Datum::I64(i64::MAX),
            ),
            (
                ScalarFuncSig::PlusInt,
                Datum::I64(i64::MIN),
                Datum::I64(i64::MIN),
            ),
            (ScalarFuncSig::PlusInt, Datum::I64(-2), Datum::U64(1)),
            (ScalarFuncSig::PlusInt, Datum::U64(1), Datum::I64(-2)),
            (
                ScalarFuncSig::MinusInt,
                Datum::I64(i64::MIN),
                Datum::I64(i64::MAX),
            ),
            (
                ScalarFuncSig::MinusInt,
                Datum::I64(i64::MAX),
                Datum::I64(i64::MIN),
            ),
            (ScalarFuncSig::MinusInt, Datum::I64(-1), Datum::U64(2)),
            (ScalarFuncSig::MinusInt, Datum::U64(1), Datum::I64(2)),
            (
                ScalarFuncSig::MultiplyInt,
                Datum::I64(i64::MIN),
                Datum::I64(i64::MAX),
            ),
            (
                ScalarFuncSig::MultiplyInt,
                Datum::U64(u64::MAX),
                Datum::I64(i64::MAX),
            ),
            (
                ScalarFuncSig::MultiplyInt,
                Datum::I64(i64::MIN),
                Datum::U64(1),
            ),
            (
                ScalarFuncSig::IntDivideInt,
                Datum::I64(i64::MIN),
                Datum::I64(-1),
            ),
            (ScalarFuncSig::IntDivideInt, Datum::I64(-1), Datum::U64(1)),
            (ScalarFuncSig::IntDivideInt, Datum::I64(-2), Datum::U64(1)),
            (ScalarFuncSig::IntDivideInt, Datum::U64(1), Datum::I64(-1)),
            (ScalarFuncSig::IntDivideInt, Datum::U64(2), Datum::I64(-1)),
            (
                ScalarFuncSig::IntDivideDecimal,
                Datum::Dec(Decimal::from(i64::MIN)),
                Datum::Dec(Decimal::from(-1)),
            ),
            (
                ScalarFuncSig::IntDivideDecimal,
                Datum::Dec(Decimal::from(i64::MAX)),
                str2dec("0.1"),
            ),
        ];
        let mut ctx = EvalContext::default();
        for tt in tests {
            let lhs = datum_expr(tt.1);
            let rhs = datum_expr(tt.2);

            let lus = mysql::has_unsigned_flag(lhs.get_field_type().get_flag());
            let rus = mysql::has_unsigned_flag(rhs.get_field_type().get_flag());
            let unsigned = lus | rus;

            let mut op = Expression::build(&mut ctx, fncall_expr(tt.0, &[lhs, rhs])).unwrap();
            if unsigned {
                // According to TiDB, the result is unsigned if any of arguments is unsigned.
                op.mut_tp().set_flag(types::UNSIGNED_FLAG as u32);
            }

            let got = op.eval(&mut ctx, &[]).unwrap_err();
            assert!(check_overflow(got).is_ok());
        }
    }

    #[test]
    fn test_arithmetic_overflow_real() {
        let tests = vec![
            (
                ScalarFuncSig::PlusReal,
                Datum::F64(f64::MAX),
                Datum::F64(f64::MAX),
            ),
            (
                ScalarFuncSig::MinusReal,
                Datum::F64(f64::MIN),
                Datum::F64(f64::MAX),
            ),
            (
                ScalarFuncSig::MultiplyReal,
                Datum::F64(f64::MIN),
                Datum::F64(f64::MAX),
            ),
            (
                ScalarFuncSig::DivideReal,
                Datum::F64(f64::MAX),
                Datum::F64(0.00001),
            ),
        ];
        let mut ctx = EvalContext::default();
        for tt in tests {
            let lhs = datum_expr(tt.1);
            let rhs = datum_expr(tt.2);

            let op = Expression::build(&mut ctx, fncall_expr(tt.0, &[lhs, rhs])).unwrap();
            let got = op.eval(&mut ctx, &[]).unwrap_err();
            assert!(check_overflow(got).is_ok());
        }
    }

    #[test]
    fn test_divide_by_zero() {
        let data = vec![
            (
                ScalarFuncSig::DivideReal,
                Datum::F64(f64::MAX),
                Datum::F64(f64::from(0)),
            ),
            (
                ScalarFuncSig::DivideReal,
                Datum::F64(f64::MAX),
                Datum::F64(0.00000),
            ),
            (
                ScalarFuncSig::DivideDecimal,
                str2dec("12.3"),
                str2dec("0.0"),
            ),
            (
                ScalarFuncSig::DivideDecimal,
                str2dec("12.3"),
                str2dec("-0.0"),
            ),
        ];

        let cases = vec![
            //(flag,sql_mode,strict_sql_mode=>is_ok,has_warning)
            (0, 0, false, true, true), //warning
            (
                FLAG_IN_UPDATE_OR_DELETE_STMT,
                MODE_ERROR_FOR_DIVISION_BY_ZERO,
                true,
                false,
                false,
            ), //error
            (FLAG_IN_UPDATE_OR_DELETE_STMT, 0, true, true, false), //ok
            (
                FLAG_IN_UPDATE_OR_DELETE_STMT | FLAG_DIVIDED_BY_ZERO_AS_WARNING,
                MODE_ERROR_FOR_DIVISION_BY_ZERO,
                true,
                true,
                true,
            ), //warning
        ];
        for (sig, left, right) in data {
            let lhs = datum_expr(left);
            let rhs = datum_expr(right);
            let fncall = fncall_expr(sig, &[lhs, rhs]);
            for (flag, sql_mode, strict_sql_mode, is_ok, has_warning) in &cases {
                let mut cfg = EvalConfig::new(0, *flag).unwrap();
                cfg.set_sql_mode(*sql_mode);
                cfg.set_strict_sql_mode(*strict_sql_mode);
                let mut ctx = EvalContext::new(Arc::new(cfg));
                let op = Expression::build(&mut ctx, fncall.clone()).unwrap();
                let got = op.eval(&mut ctx, &[]);
                if *is_ok {
                    assert_eq!(got.unwrap(), Datum::Null);
                } else {
                    assert!(check_divide_by_zero(got.unwrap_err()).is_ok());
                }
                if *has_warning {
                    assert_eq!(
                        ctx.take_warnings().warnings[0].get_code(),
                        ERR_DIVISION_BY_ZERO
                    );
                } else {
                    assert!(ctx.take_warnings().warnings.is_empty());
                }
            }
        }
    }
}
