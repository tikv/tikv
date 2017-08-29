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

use std::{f64, i64, u64};
use std::borrow::Cow;
use std::ops::{Add, Mul, Sub};
use coprocessor::codec::{mysql, Datum};
use coprocessor::codec::mysql::Decimal;
use super::{Error, FnCall, Result, StatementContext};

impl FnCall {
    pub fn plus_real(&self, ctx: &StatementContext, row: &[Datum]) -> Result<Option<f64>> {
        let lhs = try_opt!(self.children[0].eval_real(ctx, row));
        let rhs = try_opt!(self.children[1].eval_real(ctx, row));
        let res = lhs + rhs;
        if !res.is_finite() {
            return Err(Error::Overflow);
        }
        Ok(Some(res))
    }

    pub fn plus_decimal<'a, 'b: 'a>(
        &'b self,
        ctx: &StatementContext,
        row: &'a [Datum],
    ) -> Result<Option<Cow<'a, Decimal>>> {
        let lhs = try_opt!(self.children[0].eval_decimal(ctx, row));
        let rhs = try_opt!(self.children[1].eval_decimal(ctx, row));
        let result: Result<Decimal> = lhs.add(&rhs).into();
        result.map(|t| Some(Cow::Owned(t)))
    }

    pub fn plus_int(&self, ctx: &StatementContext, row: &[Datum]) -> Result<Option<i64>> {
        let lhs = try_opt!(self.children[0].eval_int(ctx, row));
        let rhs = try_opt!(self.children[1].eval_int(ctx, row));
        let lus = mysql::has_unsigned_flag(self.children[0].get_tp().get_flag());
        let rus = mysql::has_unsigned_flag(self.children[1].get_tp().get_flag());
        let res = match (lus, rus) {
            (true, true) => (lhs as u64).checked_add(rhs as u64).map(|t| t as i64),
            (true, false) => if rhs >= 0 {
                (lhs as u64).checked_add(rhs as u64).map(|t| t as i64)
            } else {
                (lhs as u64).checked_sub(opp_neg!(rhs)).map(|t| t as i64)
            },
            (false, true) => if lhs >= 0 {
                (lhs as u64).checked_add(rhs as u64).map(|t| t as i64)
            } else {
                (rhs as u64).checked_sub(opp_neg!(lhs)).map(|t| t as i64)
            },
            (false, false) => lhs.checked_add(rhs),
        };
        res.ok_or(Error::Overflow).map(Some)
    }

    pub fn minus_real(&self, ctx: &StatementContext, row: &[Datum]) -> Result<Option<f64>> {
        let lhs = try_opt!(self.children[0].eval_real(ctx, row));
        let rhs = try_opt!(self.children[1].eval_real(ctx, row));
        let res = lhs - rhs;
        if !res.is_finite() {
            return Err(Error::Overflow);
        }
        Ok(Some(res))
    }

    pub fn minus_decimal<'a, 'b: 'a>(
        &'b self,
        ctx: &StatementContext,
        row: &'a [Datum],
    ) -> Result<Option<Cow<'a, Decimal>>> {
        let lhs = try_opt!(self.children[0].eval_decimal(ctx, row));
        let rhs = try_opt!(self.children[1].eval_decimal(ctx, row));
        let result: Result<Decimal> = lhs.sub(&rhs).into();
        result.map(Cow::Owned).map(Some)
    }

    pub fn minus_int(&self, ctx: &StatementContext, row: &[Datum]) -> Result<Option<i64>> {
        let lhs = try_opt!(self.children[0].eval_int(ctx, row));
        let rhs = try_opt!(self.children[1].eval_int(ctx, row));
        let lus = mysql::has_unsigned_flag(self.children[0].get_tp().get_flag());
        let rus = mysql::has_unsigned_flag(self.children[1].get_tp().get_flag());
        let res = match (lus, rus) {
            (true, true) => (lhs as u64).checked_sub(rhs as u64).map(|t| t as i64),
            (true, false) => if rhs >= 0 {
                (lhs as u64).checked_sub(rhs as u64).map(|t| t as i64)
            } else {
                (lhs as u64).checked_add(opp_neg!(rhs)).map(|t| t as i64)
            },
            (false, true) => if lhs >= 0 {
                (lhs as u64).checked_sub(rhs as u64).map(|t| t as i64)
            } else {
                return Err(Error::Overflow);
            },
            (false, false) => lhs.checked_sub(rhs),
        };
        res.ok_or(Error::Overflow).map(Some)
    }

    pub fn multiply_real(&self, ctx: &StatementContext, row: &[Datum]) -> Result<Option<f64>> {
        let lhs = try_opt!(self.children[0].eval_real(ctx, row));
        let rhs = try_opt!(self.children[1].eval_real(ctx, row));
        let res = lhs * rhs;
        if !res.is_finite() {
            return Err(Error::Overflow);
        }
        Ok(Some(res))
    }

    pub fn multiply_decimal<'a, 'b: 'a>(
        &'b self,
        ctx: &StatementContext,
        row: &'a [Datum],
    ) -> Result<Option<Cow<'a, Decimal>>> {
        let lhs = try_opt!(self.children[0].eval_decimal(ctx, row));
        let rhs = try_opt!(self.children[1].eval_decimal(ctx, row));
        let result: Result<Decimal> = lhs.mul(&rhs).into();
        result.map(Cow::Owned).map(Some)
    }

    pub fn multiply_int(&self, ctx: &StatementContext, row: &[Datum]) -> Result<Option<i64>> {
        let lhs = try_opt!(self.children[0].eval_int(ctx, row));
        let rhs = try_opt!(self.children[1].eval_int(ctx, row));
        let lus = mysql::has_unsigned_flag(self.children[0].get_tp().get_flag());
        let rus = mysql::has_unsigned_flag(self.children[1].get_tp().get_flag());
        let u64_mul_i64 = |u, s| if s >= 0 {
            (u as u64).checked_mul(s as u64).map(|t| t as i64)
        } else {
            None
        };
        let res = match (lus, rus) {
            (true, true) => (lhs as u64).checked_mul(rhs as u64).map(|t| t as i64),
            (false, false) => lhs.checked_mul(rhs),
            (true, false) => u64_mul_i64(lhs, rhs),
            (false, true) => u64_mul_i64(rhs, lhs),
        };
        res.ok_or(Error::Overflow).map(Some)
    }
}

#[cfg(test)]
mod test {
    use std::{f64, i64, u64};
    use tipb::expression::ScalarFuncSig;
    use coprocessor::codec::{mysql, Datum};
    use coprocessor::codec::mysql::types;
    use coprocessor::dag::expr::{Expression, StatementContext};
    use coprocessor::dag::expr::test::{check_overflow, fncall_expr, str2dec};
    use coprocessor::select::xeval::evaluator::test::datum_expr;

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
        ];
        let ctx = StatementContext::default();
        for tt in tests {
            let lhs = datum_expr(tt.1);
            let rhs = datum_expr(tt.2);

            let lus = mysql::has_unsigned_flag(lhs.get_field_type().get_flag());
            let rus = mysql::has_unsigned_flag(rhs.get_field_type().get_flag());
            let unsigned = lus | rus;

            let mut op = Expression::build(fncall_expr(tt.0, &[lhs, rhs]), 0, &ctx).unwrap();
            if unsigned {
                // According to TiDB, the result is unsigned if any of arguments is unsigned.
                op.mut_tp().set_flag(types::UNSIGNED_FLAG as u32);
            }

            let expected = Expression::build(datum_expr(tt.3), 0, &ctx).unwrap();

            let got = op.eval_int(&ctx, &[]).unwrap();
            let exp = expected.eval_int(&ctx, &[]).unwrap();
            assert_eq!(got, exp);
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
        ];
        let ctx = StatementContext::default();
        for tt in tests {
            let lhs = datum_expr(tt.1);
            let rhs = datum_expr(tt.2);

            let op = Expression::build(fncall_expr(tt.0, &[lhs, rhs]), 0, &ctx).unwrap();
            let expected = Expression::build(datum_expr(tt.3), 0, &ctx).unwrap();

            let got = op.eval_real(&ctx, &[]).unwrap();
            let exp = expected.eval_real(&ctx, &[]).unwrap();
            assert_eq!(got, exp);
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
        ];
        let ctx = StatementContext::default();
        for tt in tests {
            let lhs = datum_expr(tt.1);
            let rhs = datum_expr(tt.2);

            let op = Expression::build(fncall_expr(tt.0, &[lhs, rhs]), 0, &ctx).unwrap();
            let expected = Expression::build(datum_expr(tt.3), 0, &ctx).unwrap();

            let got = op.eval_decimal(&ctx, &[]).unwrap();
            let exp = expected.eval_decimal(&ctx, &[]).unwrap();
            assert_eq!(got, exp);
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
        ];
        let ctx = StatementContext::default();
        for tt in tests {
            let lhs = datum_expr(tt.1);
            let rhs = datum_expr(tt.2);

            let lus = mysql::has_unsigned_flag(lhs.get_field_type().get_flag());
            let rus = mysql::has_unsigned_flag(rhs.get_field_type().get_flag());
            let unsigned = lus | rus;

            let mut op = Expression::build(fncall_expr(tt.0, &[lhs, rhs]), 0, &ctx).unwrap();
            if unsigned {
                // According to TiDB, the result is unsigned if any of arguments is unsigned.
                op.mut_tp().set_flag(types::UNSIGNED_FLAG as u32);
            }

            let got = op.eval_int(&ctx, &[]).unwrap_err();
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
        ];
        let ctx = StatementContext::default();
        for tt in tests {
            let lhs = datum_expr(tt.1);
            let rhs = datum_expr(tt.2);

            let op = Expression::build(fncall_expr(tt.0, &[lhs, rhs]), 0, &ctx).unwrap();
            let got = op.eval_real(&ctx, &[]).unwrap_err();
            assert!(check_overflow(got).is_ok());
        }
    }
}
