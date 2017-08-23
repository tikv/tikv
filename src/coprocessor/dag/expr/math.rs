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

use std::i64;
use std::borrow::Cow;
use coprocessor::codec::Datum;
use coprocessor::codec::mysql::Decimal;
use super::{Error, FnCall, Result, StatementContext};

impl FnCall {
    #[inline]
    pub fn abs_real(&self, ctx: &StatementContext, row: &[Datum]) -> Result<Option<f64>> {
        let n = try_opt!(self.children[0].eval_real(ctx, row));
        Ok(Some(n.abs()))
    }

    #[inline]
    pub fn abs_decimal<'a, 'b: 'a>(
        &'b self,
        ctx: &StatementContext,
        row: &'a [Datum],
    ) -> Result<Option<Cow<'a, Decimal>>> {
        let d = try_opt!(self.children[0].eval_decimal(ctx, row));
        let result: Result<Decimal> = d.abs().into();
        result.map(|t| Some(Cow::Owned(t)))
    }

    #[inline]
    pub fn abs_int(&self, ctx: &StatementContext, row: &[Datum]) -> Result<Option<i64>> {
        let n = try_opt!(self.children[0].eval_int(ctx, row));
        if n == i64::MIN {
            return Err(Error::Overflow);
        }
        Ok(Some(n.abs()))
    }

    #[inline]
    pub fn abs_uint(&self, ctx: &StatementContext, row: &[Datum]) -> Result<Option<i64>> {
        self.children[0].eval_int(ctx, row)
    }

    #[inline]
    pub fn ceil_real(&self, ctx: &StatementContext, row: &[Datum]) -> Result<Option<f64>> {
        let n = try_opt!(self.children[0].eval_real(ctx, row));
        Ok(Some(n.ceil()))
    }

    #[inline]
    pub fn ceil_int_int(&self, ctx: &StatementContext, row: &[Datum]) -> Result<Option<i64>> {
        self.children[0].eval_int(ctx, row)
    }

    #[inline]
    pub fn ceil_int_dec<'a, 'b: 'a>(
        &'b self,
        ctx: &StatementContext,
        row: &'a [Datum],
    ) -> Result<Option<Cow<'a, Decimal>>> {
        let n = try_opt!(self.children[0].eval_int(ctx, row));
        Ok(Some(Cow::Owned(Decimal::from(n))))
    }

    #[inline]
    pub fn ceil_dec_int(&self, ctx: &StatementContext, row: &[Datum]) -> Result<Option<i64>> {
        // FIXME: here we can't do same logic with TiDB.
        self.cast_decimal_as_int(ctx, row)
    }

    #[inline]
    pub fn ceil_dec_dec<'a, 'b: 'a>(
        &'b self,
        ctx: &StatementContext,
        row: &'a [Datum],
    ) -> Result<Option<Cow<'a, Decimal>>> {
        let d = try_opt!(self.children[0].eval_decimal(ctx, row));
        let result: Result<Decimal> = d.ceil().into();
        result.map(|t| Some(Cow::Owned(t)))
    }

    #[inline]
    pub fn floor_real(&self, ctx: &StatementContext, row: &[Datum]) -> Result<Option<f64>> {
        let n = try_opt!(self.children[0].eval_real(ctx, row));
        Ok(Some(n.floor()))
    }

    #[inline]
    pub fn floor_int_int(&self, ctx: &StatementContext, row: &[Datum]) -> Result<Option<i64>> {
        self.children[0].eval_int(ctx, row)
    }

    #[inline]
    pub fn floor_int_dec<'a, 'b: 'a>(
        &'b self,
        ctx: &StatementContext,
        row: &'a [Datum],
    ) -> Result<Option<Cow<'a, Decimal>>> {
        let n = try_opt!(self.children[0].eval_int(ctx, row));
        Ok(Some(Cow::Owned(Decimal::from(n))))
    }

    #[inline]
    pub fn floor_dec_int(&self, ctx: &StatementContext, row: &[Datum]) -> Result<Option<i64>> {
        // FIXME: here we can't do same logic with TiDB.
        self.cast_decimal_as_int(ctx, row)
    }

    #[inline]
    pub fn floor_dec_dec<'a, 'b: 'a>(
        &'b self,
        ctx: &StatementContext,
        row: &'a [Datum],
    ) -> Result<Option<Cow<'a, Decimal>>> {
        let d = try_opt!(self.children[0].eval_decimal(ctx, row));
        let result: Result<Decimal> = d.floor().into();
        result.map(|t| Some(Cow::Owned(t)))
    }
}

#[cfg(test)]
mod test {
    use std::{i64, u64};
    use tipb::expression::ScalarFuncSig;
    use coprocessor::codec::Datum;
    use coprocessor::dag::expr::test::{fncall_expr, str2dec};
    use coprocessor::dag::expr::{Expression, StatementContext};
    use coprocessor::select::xeval::evaluator::test::datum_expr;

    #[test]
    fn test_abs() {
        let tests = vec![
            (ScalarFuncSig::AbsInt, Datum::I64(-3), Datum::I64(3)),
            (
                ScalarFuncSig::AbsInt,
                Datum::I64(i64::MAX),
                Datum::I64(i64::MAX),
            ),
            (
                ScalarFuncSig::AbsUInt,
                Datum::U64(u64::MAX),
                Datum::U64(u64::MAX),
            ),
            (ScalarFuncSig::AbsReal, Datum::F64(3.5), Datum::F64(3.5)),
            (ScalarFuncSig::AbsReal, Datum::F64(-3.5), Datum::F64(3.5)),
            (ScalarFuncSig::AbsDecimal, str2dec("1.1"), str2dec("1.1")),
            (ScalarFuncSig::AbsDecimal, str2dec("-1.1"), str2dec("1.1")),
        ];
        let ctx = StatementContext::default();
        for tt in tests {
            let arg = datum_expr(tt.1);
            let op = Expression::build(fncall_expr(tt.0, &[arg]), 0).unwrap();
            let expected = Expression::build(datum_expr(tt.2), 0).unwrap();
            match tt.0 {
                ScalarFuncSig::AbsInt | ScalarFuncSig::AbsUInt => {
                    let got = op.eval_int(&ctx, &[]).unwrap();
                    let exp = expected.eval_int(&ctx, &[]).unwrap();
                    assert_eq!(got, exp);
                }
                ScalarFuncSig::AbsReal => {
                    let got = op.eval_real(&ctx, &[]).unwrap();
                    let exp = expected.eval_real(&ctx, &[]).unwrap();
                    assert_eq!(got, exp);
                }
                ScalarFuncSig::AbsDecimal => {
                    let got = op.eval_decimal(&ctx, &[]).unwrap();
                    let exp = expected.eval_decimal(&ctx, &[]).unwrap();
                    assert_eq!(got, exp);
                }
                _ => unreachable!(),
            }
        }
    }
}
