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
        let result: Result<Decimal> = d.into_owned().abs().into();
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
    pub fn ceil_real(&self, ctx: &StatementContext, row: &[Datum]) -> Result<Option<f64>> {
        let n = try_opt!(self.children[0].eval_real(ctx, row));
        Ok(Some(n.ceil()))
    }

    #[inline]
    pub fn ceil_dec_to_int(&self, ctx: &StatementContext, row: &[Datum]) -> Result<Option<i64>> {
        let d = try_opt!(self.children[0].eval_decimal(ctx, row));
        let d: Result<Decimal> = d.ceil().into();
        d.and_then(|dec| dec.as_i64_with_ctx(ctx)).map(Some)
    }

    #[inline]
    pub fn ceil_dec_to_dec<'a, 'b: 'a>(
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
    pub fn floor_dec_to_int(&self, ctx: &StatementContext, row: &[Datum]) -> Result<Option<i64>> {
        let d = try_opt!(self.children[0].eval_decimal(ctx, row));
        let d: Result<Decimal> = d.floor().into();
        d.and_then(|dec| dec.as_i64_with_ctx(ctx)).map(Some)
    }

    #[inline]
    pub fn floor_dec_to_dec<'a, 'b: 'a>(
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
    use std::{f64, i64, u64};
    use tipb::expression::ScalarFuncSig;
    use coprocessor::codec::{convert, mysql, Datum};
    use coprocessor::codec::mysql::types;
    use coprocessor::dag::expr::test::{check_overflow, fncall_expr, str2dec};
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
        for (sig, arg, exp) in tests {
            let arg = datum_expr(arg);
            let op = Expression::build(fncall_expr(sig, &[arg]), 0, &ctx).unwrap();
            let got: Datum = match sig {
                ScalarFuncSig::AbsInt => op.eval_int(&ctx, &[]).unwrap().into(),
                ScalarFuncSig::AbsUInt => op.eval_int(&ctx, &[]).unwrap().map(|i| i as u64).into(),
                ScalarFuncSig::AbsReal => op.eval_real(&ctx, &[]).unwrap().into(),
                ScalarFuncSig::AbsDecimal => op.eval_decimal(&ctx, &[]).unwrap().into(),
                _ => unreachable!(),
            };
            assert_eq!(got, exp);
        }
    }

    #[test]
    fn test_overflow_abs() {
        let tests = vec![(ScalarFuncSig::AbsInt, Datum::I64(i64::MIN))];
        let ctx = StatementContext::default();
        for tt in tests {
            let arg = datum_expr(tt.1);
            let op = Expression::build(fncall_expr(tt.0, &[arg]), 0, &ctx).unwrap();
            match tt.0 {
                ScalarFuncSig::AbsInt | ScalarFuncSig::AbsUInt => {
                    let got = op.eval_int(&ctx, &[]).unwrap_err();
                    assert!(check_overflow(got).is_ok());
                }
                _ => unreachable!(),
            }
        }
    }

    #[test]
    fn test_ceil() {
        let tests = vec![
            (ScalarFuncSig::CeilReal, Datum::F64(3.45), Datum::F64(4f64)),
            (
                ScalarFuncSig::CeilReal,
                Datum::F64(-3.45),
                Datum::F64(-3f64),
            ),
            (
                ScalarFuncSig::CeilReal,
                Datum::F64(f64::MAX),
                Datum::F64(f64::MAX),
            ),
            (
                ScalarFuncSig::CeilReal,
                Datum::F64(f64::MIN),
                Datum::F64(f64::MIN),
            ),
            (
                ScalarFuncSig::CeilIntToInt,
                Datum::I64(i64::MIN),
                Datum::I64(i64::MIN),
            ),
            (
                ScalarFuncSig::CeilIntToInt,
                Datum::U64(u64::MAX),
                Datum::U64(u64::MAX),
            ),
            (
                ScalarFuncSig::CeilIntToDec,
                Datum::I64(i64::MIN),
                str2dec("-9223372036854775808"),
            ),
            (
                ScalarFuncSig::CeilDecToInt,
                str2dec("123.456"),
                Datum::I64(124),
            ),
            (
                ScalarFuncSig::CeilDecToInt,
                str2dec("-123.456"),
                Datum::I64(-123),
            ),
            (
                ScalarFuncSig::CeilDecToDec,
                str2dec("9223372036854775808"),
                str2dec("9223372036854775808"),
            ),
        ];
        let mut ctx = StatementContext::default();
        ctx.ignore_truncate = true; // for ceil decimal to int.
        for (sig, arg, exp) in tests {
            let arg = datum_expr(arg);
            let mut op = Expression::build(fncall_expr(sig, &[arg.clone()]), 0, &ctx).unwrap();
            let exp = Expression::build(datum_expr(exp), 0, &ctx).unwrap();
            if mysql::has_unsigned_flag(arg.get_field_type().get_flag()) {
                op.mut_tp().set_flag(types::UNSIGNED_FLAG as u32);
            }
            if sig == ScalarFuncSig::CeilIntToDec || sig == ScalarFuncSig::CeilDecToDec {
                op.mut_tp().set_flen(convert::UNSPECIFIED_LENGTH);
                op.mut_tp().set_decimal(convert::UNSPECIFIED_LENGTH);
            }
            match sig {
                ScalarFuncSig::CeilReal => {
                    let got = op.eval_real(&ctx, &[]).unwrap();
                    let exp = exp.eval_real(&ctx, &[]).unwrap();
                    assert_eq!(got, exp);
                }
                ScalarFuncSig::CeilIntToInt | ScalarFuncSig::CeilDecToInt => {
                    let got = op.eval_int(&ctx, &[]).unwrap();
                    let exp = exp.eval_int(&ctx, &[]).unwrap();
                    assert_eq!(got, exp);
                }
                ScalarFuncSig::CeilIntToDec | ScalarFuncSig::CeilDecToDec => {
                    let got = op.eval_decimal(&ctx, &[]).unwrap();
                    let exp = exp.eval_decimal(&ctx, &[]).unwrap();
                    assert_eq!(got, exp);
                }
                _ => unreachable!(),
            }
        }
    }

    #[test]
    fn test_floor() {
        let tests = vec![
            (ScalarFuncSig::FloorReal, Datum::F64(3.45), Datum::F64(3f64)),
            (
                ScalarFuncSig::FloorReal,
                Datum::F64(-3.45),
                Datum::F64(-4f64),
            ),
            (
                ScalarFuncSig::FloorReal,
                Datum::F64(f64::MAX),
                Datum::F64(f64::MAX),
            ),
            (
                ScalarFuncSig::FloorReal,
                Datum::F64(f64::MIN),
                Datum::F64(f64::MIN),
            ),
            (
                ScalarFuncSig::FloorIntToInt,
                Datum::I64(i64::MIN),
                Datum::I64(i64::MIN),
            ),
            (
                ScalarFuncSig::FloorIntToInt,
                Datum::U64(u64::MAX),
                Datum::U64(u64::MAX),
            ),
            (
                ScalarFuncSig::FloorIntToDec,
                Datum::I64(i64::MIN),
                str2dec("-9223372036854775808"),
            ),
            (
                ScalarFuncSig::FloorDecToInt,
                str2dec("123.456"),
                Datum::I64(123),
            ),
            (
                ScalarFuncSig::FloorDecToInt,
                str2dec("-123.456"),
                Datum::I64(-124),
            ),
            (
                ScalarFuncSig::FloorDecToDec,
                str2dec("9223372036854775808"),
                str2dec("9223372036854775808"),
            ),
        ];
        let mut ctx = StatementContext::default();
        ctx.ignore_truncate = true; // for ceil decimal to int.
        for (sig, arg, exp) in tests {
            let arg = datum_expr(arg);
            let mut op = Expression::build(fncall_expr(sig, &[arg.clone()]), 0, &ctx).unwrap();
            let exp = Expression::build(datum_expr(exp), 0, &ctx).unwrap();
            if mysql::has_unsigned_flag(arg.get_field_type().get_flag()) {
                op.mut_tp().set_flag(types::UNSIGNED_FLAG as u32);
            }
            if sig == ScalarFuncSig::FloorIntToDec || sig == ScalarFuncSig::FloorDecToDec {
                op.mut_tp().set_flen(convert::UNSPECIFIED_LENGTH);
                op.mut_tp().set_decimal(convert::UNSPECIFIED_LENGTH);
            }
            match sig {
                ScalarFuncSig::FloorReal => {
                    let got = op.eval_real(&ctx, &[]).unwrap();
                    let exp = exp.eval_real(&ctx, &[]).unwrap();
                    assert_eq!(got, exp);
                }
                ScalarFuncSig::FloorIntToInt | ScalarFuncSig::FloorDecToInt => {
                    let got = op.eval_int(&ctx, &[]).unwrap();
                    let exp = exp.eval_int(&ctx, &[]).unwrap();
                    assert_eq!(got, exp);
                }
                ScalarFuncSig::FloorIntToDec | ScalarFuncSig::FloorDecToDec => {
                    let got = op.eval_decimal(&ctx, &[]).unwrap();
                    let exp = exp.eval_decimal(&ctx, &[]).unwrap();
                    assert_eq!(got, exp);
                }
                _ => unreachable!(),
            }
        }
    }
}
