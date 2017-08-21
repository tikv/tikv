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

use std::{f64, i64};
use std::borrow::Cow;
use coprocessor::codec::Datum;
use coprocessor::codec::mysql::Decimal;
use super::{Error, FnCall, Result, StatementContext};

impl FnCall {
    pub fn plus_real(&self, ctx: &StatementContext, row: &[Datum]) -> Result<Option<f64>> {
        let lhs = try!(self.children[0].eval_real(ctx, row));
        let rhs = try!(self.children[1].eval_real(ctx, row));
        do_arithmetic(lhs, rhs, |l, r| {
            let res = l + r;
            if !res.is_finite() {
                return Err(Error::Overflow);
            }
            Ok(res)
        })
    }

    pub fn plus_decimal<'a, 'b: 'a>(
        &'b self,
        ctx: &StatementContext,
        row: &'a [Datum],
    ) -> Result<Option<Cow<'a, Decimal>>> {
        let lhs = try!(self.children[0].eval_decimal(ctx, row));
        let rhs = try!(self.children[1].eval_decimal(ctx, row));
        do_arithmetic(lhs, rhs, |l, r| {
            let result: Result<Decimal> = (l.as_ref() + r.as_ref()).into();
            result.map(Cow::Owned)
        })
    }

    pub fn plus_int(&self, ctx: &StatementContext, row: &[Datum]) -> Result<Option<i64>> {
        let lhs = try!(self.children[0].eval_int(ctx, row));
        let rhs = try!(self.children[1].eval_int(ctx, row));
        do_arithmetic(lhs, rhs, |l, r| l.checked_add(r).ok_or(Error::Overflow))
    }

    pub fn plus_uint(&self, ctx: &StatementContext, row: &[Datum]) -> Result<Option<i64>> {
        let lhs = try!(self.children[0].eval_int(ctx, row));
        let rhs = try!(self.children[1].eval_int(ctx, row));
        do_arithmetic(lhs, rhs, |l, r| {
            let res = (l as u64).checked_add(r as u64).ok_or(Error::Overflow);
            res.map(|u| u as i64)
        })
    }

    pub fn minus_real(&self, ctx: &StatementContext, row: &[Datum]) -> Result<Option<f64>> {
        let lhs = try!(self.children[0].eval_real(ctx, row));
        let rhs = try!(self.children[1].eval_real(ctx, row));
        do_arithmetic(lhs, rhs, |l, r| {
            let res = l - r;
            if !res.is_finite() {
                return Err(Error::Overflow);
            }
            Ok(res)
        })
    }

    pub fn minus_decimal<'a, 'b: 'a>(
        &'b self,
        ctx: &StatementContext,
        row: &'a [Datum],
    ) -> Result<Option<Cow<'a, Decimal>>> {
        let lhs = try!(self.children[0].eval_decimal(ctx, row));
        let rhs = try!(self.children[1].eval_decimal(ctx, row));
        do_arithmetic(lhs, rhs, |l, r| {
            let result: Result<Decimal> = (l.as_ref() - r.as_ref()).into();
            result.map(Cow::Owned)
        })
    }

    pub fn minus_int(&self, ctx: &StatementContext, row: &[Datum]) -> Result<Option<i64>> {
        let lhs = try!(self.children[0].eval_int(ctx, row));
        let rhs = try!(self.children[1].eval_int(ctx, row));
        do_arithmetic(lhs, rhs, |l, r| l.checked_sub(r).ok_or(Error::Overflow))
    }

    pub fn minus_uint(&self, ctx: &StatementContext, row: &[Datum]) -> Result<Option<i64>> {
        let lhs = try!(self.children[0].eval_int(ctx, row));
        let rhs = try!(self.children[1].eval_int(ctx, row));
        do_arithmetic(lhs, rhs, |l, r| {
            let res = (l as u64).checked_sub(r as u64).ok_or(Error::Overflow);
            res.map(|u| u as i64)
        })
    }

    pub fn multiply_real(&self, ctx: &StatementContext, row: &[Datum]) -> Result<Option<f64>> {
        let lhs = try!(self.children[0].eval_real(ctx, row));
        let rhs = try!(self.children[1].eval_real(ctx, row));
        do_arithmetic(lhs, rhs, |l, r| {
            let res = l * r;
            if !res.is_finite() {
                return Err(Error::Overflow);
            }
            Ok(res)
        })
    }

    pub fn multiply_decimal<'a, 'b: 'a>(
        &'b self,
        ctx: &StatementContext,
        row: &'a [Datum],
    ) -> Result<Option<Cow<'a, Decimal>>> {
        let lhs = try!(self.children[0].eval_decimal(ctx, row));
        let rhs = try!(self.children[1].eval_decimal(ctx, row));
        do_arithmetic(lhs, rhs, |l, r| {
            let result: Result<Decimal> = (l.as_ref() * r.as_ref()).into();
            result.map(Cow::Owned)
        })
    }

    pub fn multiply_int(&self, ctx: &StatementContext, row: &[Datum]) -> Result<Option<i64>> {
        let lhs = try!(self.children[0].eval_int(ctx, row));
        let rhs = try!(self.children[1].eval_int(ctx, row));
        do_arithmetic(lhs, rhs, |l, r| l.checked_mul(r).ok_or(Error::Overflow))
    }

    pub fn multiply_uint(&self, ctx: &StatementContext, row: &[Datum]) -> Result<Option<i64>> {
        let lhs = try!(self.children[0].eval_int(ctx, row));
        let rhs = try!(self.children[1].eval_int(ctx, row));
        do_arithmetic(lhs, rhs, |l, r| {
            let res = (l as u64).checked_mul(r as u64).ok_or(Error::Overflow);
            res.map(|u| u as i64)
        })
    }
}

#[inline]
fn do_arithmetic<T, F>(lhs: Option<T>, rhs: Option<T>, op: F) -> Result<Option<T>>
where
    F: Fn(T, T) -> Result<T>,
{
    match (lhs, rhs) {
        (Some(lhs), Some(rhs)) => op(lhs, rhs).map(Some),
        _ => Ok(None),
    }
}

#[cfg(test)]
mod test {
    use std::{f64, i64, u64};
    use tipb::expression::ScalarFuncSig;
    use coprocessor::codec::Datum;
    use coprocessor::dag::expr::{Error, Expression, StatementContext};
    use coprocessor::dag::expr::test::{fncall_expr, str2dec};
    use coprocessor::select::xeval::evaluator::test::datum_expr;

    fn check_overflow(e: Error) -> Result<(), ()> {
        match e {
            Error::Overflow => Ok(()),
            _ => Err(()),
        }
    }

    #[test]
    fn test_arithmetic() {
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
                ScalarFuncSig::PlusIntUnsigned,
                Datum::U64(12),
                Datum::U64(1),
                Datum::U64(13),
            ),
            (
                ScalarFuncSig::PlusReal,
                Datum::F64(1.01001),
                Datum::F64(-0.01),
                Datum::F64(1.00001),
            ),
            (
                ScalarFuncSig::PlusDecimal,
                str2dec("1.1"),
                str2dec("2.2"),
                str2dec("3.3"),
            ),
            (
                ScalarFuncSig::MinusInt,
                Datum::I64(12),
                Datum::I64(1),
                Datum::I64(11),
            ),
            (
                ScalarFuncSig::MinusIntUnsigned,
                Datum::U64(12),
                Datum::U64(1),
                Datum::U64(11),
            ),
            (
                ScalarFuncSig::MinusReal,
                Datum::F64(1.01001),
                Datum::F64(-0.01),
                Datum::F64(1.02001),
            ),
            (
                ScalarFuncSig::MinusDecimal,
                str2dec("1.1"),
                str2dec("2.2"),
                str2dec("-1.1"),
            ),
            (
                ScalarFuncSig::MultiplyInt,
                Datum::I64(12),
                Datum::I64(1),
                Datum::I64(12),
            ),
            (
                ScalarFuncSig::MultiplyIntUnsigned,
                Datum::U64(12),
                Datum::U64(1),
                Datum::U64(12),
            ),
            (
                ScalarFuncSig::MultiplyReal,
                Datum::F64(1.01001),
                Datum::F64(-0.01),
                Datum::F64(-0.0101001),
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
            let expected = Expression::build(datum_expr(tt.3), 0).unwrap();
            let op = Expression::build(fncall_expr(tt.0, &[lhs, rhs]), 0).unwrap();
            match tt.0 {
                ScalarFuncSig::PlusInt |
                ScalarFuncSig::MinusInt |
                ScalarFuncSig::MultiplyInt |
                ScalarFuncSig::PlusIntUnsigned |
                ScalarFuncSig::MinusIntUnsigned |
                ScalarFuncSig::MultiplyIntUnsigned => {
                    let lhs = op.eval_int(&ctx, &[]).unwrap();
                    let rhs = expected.eval_int(&ctx, &[]).unwrap();
                    assert_eq!(lhs, rhs);
                }
                ScalarFuncSig::PlusReal | ScalarFuncSig::MinusReal => {
                    let lhs = op.eval_real(&ctx, &[]).unwrap();
                    let rhs = expected.eval_real(&ctx, &[]).unwrap();
                    assert_eq!(lhs, rhs);
                }
                ScalarFuncSig::MultiplyReal => {
                    let lhs = op.eval_real(&ctx, &[]).unwrap();
                    let rhs = expected.eval_real(&ctx, &[]).unwrap();
                    assert_eq!(lhs, rhs);
                }
                ScalarFuncSig::PlusDecimal |
                ScalarFuncSig::MinusDecimal |
                ScalarFuncSig::MultiplyDecimal => {
                    let lhs = op.eval_decimal(&ctx, &[]).unwrap();
                    let rhs = expected.eval_decimal(&ctx, &[]).unwrap();
                    assert_eq!(lhs, rhs);
                }
                _ => unreachable!(),
            }
        }
    }

    #[test]
    fn test_arithmetic_overflow() {
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
            (
                ScalarFuncSig::PlusIntUnsigned,
                Datum::U64(u64::MAX),
                Datum::U64(u64::MAX),
            ),
            (
                ScalarFuncSig::PlusReal,
                Datum::F64(f64::MAX),
                Datum::F64(f64::MAX),
            ),
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
            (
                ScalarFuncSig::MinusIntUnsigned,
                Datum::U64(1u64),
                Datum::U64(2u64),
            ),
            (
                ScalarFuncSig::MinusReal,
                Datum::F64(f64::MIN),
                Datum::F64(f64::MAX),
            ),
            (
                ScalarFuncSig::MultiplyInt,
                Datum::I64(i64::MIN),
                Datum::I64(i64::MAX),
            ),
            (
                ScalarFuncSig::MultiplyIntUnsigned,
                Datum::U64(u64::MAX),
                Datum::U64(u64::MAX),
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
            let op = Expression::build(fncall_expr(tt.0, &[lhs, rhs]), 0).unwrap();
            match tt.0 {
                ScalarFuncSig::PlusInt |
                ScalarFuncSig::MinusInt |
                ScalarFuncSig::MultiplyInt |
                ScalarFuncSig::PlusIntUnsigned |
                ScalarFuncSig::MinusIntUnsigned |
                ScalarFuncSig::MultiplyIntUnsigned => {
                    let lhs = op.eval_int(&ctx, &[]).unwrap_err();
                    assert!(check_overflow(lhs).is_ok());
                }
                ScalarFuncSig::PlusReal | ScalarFuncSig::MinusReal => {
                    let lhs = op.eval_real(&ctx, &[]).unwrap_err();
                    assert!(check_overflow(lhs).is_ok());
                }
                ScalarFuncSig::MultiplyReal => {
                    let lhs = op.eval_real(&ctx, &[]).unwrap_err();
                    assert!(check_overflow(lhs).is_ok());
                }
                ScalarFuncSig::PlusDecimal |
                ScalarFuncSig::MinusDecimal |
                ScalarFuncSig::MultiplyDecimal => {
                    let lhs = op.eval_decimal(&ctx, &[]).unwrap_err();
                    assert!(check_overflow(lhs).is_ok());
                }
                _ => unreachable!(),
            }
        }
    }
}
