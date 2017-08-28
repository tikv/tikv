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

use std::borrow::Cow;
use super::{FnCall, Result, StatementContext};
use coprocessor::codec::Datum;
use coprocessor::codec::mysql::{Decimal, Duration, Time};

fn if_null<F, T>(f: F) -> Result<Option<T>>
where
    F: Fn(usize) -> Result<Option<T>>,
{
    let arg0 = try!(f(0));
    if !arg0.is_none() {
        return Ok(arg0);
    }
    f(1)
}

fn if_condition<F, T>(
    expr: &FnCall,
    ctx: &StatementContext,
    row: &[Datum],
    f: F,
) -> Result<Option<T>>
where
    F: Fn(usize) -> Result<Option<T>>,
{
    let arg0 = try!(expr.children[0].eval_int(ctx, row));
    if arg0.map_or(false, |arg| arg != 0) {
        f(1)
    } else {
        f(2)
    }
}

impl FnCall {
    pub fn if_null_int(&self, ctx: &StatementContext, row: &[Datum]) -> Result<Option<i64>> {
        if_null(|i| self.children[i].eval_int(ctx, row))
    }

    pub fn if_null_real(&self, ctx: &StatementContext, row: &[Datum]) -> Result<Option<f64>> {
        if_null(|i| self.children[i].eval_real(ctx, row))
    }

    pub fn if_null_decimal<'a, 'b: 'a>(
        &'b self,
        ctx: &StatementContext,
        row: &'a [Datum],
    ) -> Result<Option<Cow<'a, Decimal>>> {
        if_null(|i| self.children[i].eval_decimal(ctx, row))
    }

    pub fn if_null_string<'a, 'b: 'a>(
        &'b self,
        ctx: &StatementContext,
        row: &'a [Datum],
    ) -> Result<Option<Cow<'a, Vec<u8>>>> {
        if_null(|i| self.children[i].eval_string(ctx, row))
    }

    pub fn if_null_time<'a, 'b: 'a>(
        &'b self,
        ctx: &StatementContext,
        row: &'a [Datum],
    ) -> Result<Option<Cow<'a, Time>>> {
        if_null(|i| self.children[i].eval_time(ctx, row))
    }

    pub fn if_null_duration<'a, 'b: 'a>(
        &'b self,
        ctx: &StatementContext,
        row: &'a [Datum],
    ) -> Result<Option<Cow<'a, Duration>>> {
        if_null(|i| self.children[i].eval_duration(ctx, row))
    }

    pub fn if_int(&self, ctx: &StatementContext, row: &[Datum]) -> Result<Option<i64>> {
        if_condition(self, ctx, row, |i| self.children[i].eval_int(ctx, row))
    }

    pub fn if_real(&self, ctx: &StatementContext, row: &[Datum]) -> Result<Option<f64>> {
        if_condition(self, ctx, row, |i| self.children[i].eval_real(ctx, row))
    }

    pub fn if_decimal<'a, 'b: 'a>(
        &'b self,
        ctx: &StatementContext,
        row: &'a [Datum],
    ) -> Result<Option<Cow<'a, Decimal>>> {
        if_condition(self, ctx, row, |i| self.children[i].eval_decimal(ctx, row))
    }

    pub fn if_string<'a, 'b: 'a>(
        &'b self,
        ctx: &StatementContext,
        row: &'a [Datum],
    ) -> Result<Option<Cow<'a, Vec<u8>>>> {
        if_condition(self, ctx, row, |i| self.children[i].eval_string(ctx, row))
    }

    pub fn if_time<'a, 'b: 'a>(
        &'b self,
        ctx: &StatementContext,
        row: &'a [Datum],
    ) -> Result<Option<Cow<'a, Time>>> {
        if_condition(self, ctx, row, |i| self.children[i].eval_time(ctx, row))
    }

    pub fn if_duration<'a, 'b: 'a>(
        &'b self,
        ctx: &StatementContext,
        row: &'a [Datum],
    ) -> Result<Option<Cow<'a, Duration>>> {
        if_condition(self, ctx, row, |i| self.children[i].eval_duration(ctx, row))
    }
}

#[cfg(test)]
mod test {
    use tipb::expression::ScalarFuncSig;
    use coprocessor::codec::Datum;
    use coprocessor::codec::mysql::Duration;
    use coprocessor::dag::expr::{Expression, StatementContext};
    use coprocessor::dag::expr::test::{fncall_expr, str2dec};
    use coprocessor::select::xeval::evaluator::test::datum_expr;

    #[test]
    fn test_if_null() {
        let tests = vec![
            (
                ScalarFuncSig::IfNullInt,
                Datum::I64(0),
                Datum::I64(2),
                Datum::I64(0),
            ),
            (
                ScalarFuncSig::IfNullInt,
                Datum::I64(1),
                Datum::I64(2),
                Datum::I64(1),
            ),
            (
                ScalarFuncSig::IfNullInt,
                Datum::Null,
                Datum::I64(2),
                Datum::I64(2),
            ),
            (
                ScalarFuncSig::IfNullReal,
                Datum::F64(0.0),
                Datum::F64(2.2),
                Datum::F64(0.0),
            ),
            (
                ScalarFuncSig::IfNullReal,
                Datum::F64(1.1),
                Datum::F64(2.2),
                Datum::F64(1.1),
            ),
            (
                ScalarFuncSig::IfNullReal,
                Datum::Null,
                Datum::F64(2.2),
                Datum::F64(2.2),
            ),
            (
                ScalarFuncSig::IfNullString,
                Datum::Bytes(b"abc".to_vec()),
                Datum::Bytes(b"abd".to_vec()),
                Datum::Bytes(b"abc".to_vec()),
            ),
            (
                ScalarFuncSig::IfNullString,
                Datum::Null,
                Datum::Bytes(b"abd".to_vec()),
                Datum::Bytes(b"abd".to_vec()),
            ),
            (
                ScalarFuncSig::IfNullDecimal,
                str2dec("1.123"),
                str2dec("2.345"),
                str2dec("1.123"),
            ),
            (
                ScalarFuncSig::IfNullDecimal,
                Datum::Null,
                str2dec("2.345"),
                str2dec("2.345"),
            ),
            (
                ScalarFuncSig::IfNullDuration,
                Datum::Dur(Duration::from_nanos(123, 1).unwrap()),
                Datum::Dur(Duration::from_nanos(345, 2).unwrap()),
                Datum::Dur(Duration::from_nanos(123, 1).unwrap()),
            ),
            (
                ScalarFuncSig::IfNullDuration,
                Datum::Null,
                Datum::Dur(Duration::from_nanos(345, 2).unwrap()),
                Datum::Dur(Duration::from_nanos(345, 2).unwrap()),
            ),
            // TODO: add Time related tests after Time is implementted in Expression::build
        ];
        let ctx = StatementContext::default();
        for (operator, branch1, branch2, exp) in tests {
            let arg1 = datum_expr(branch1);
            let arg2 = datum_expr(branch2);
            let op = Expression::build(fncall_expr(operator, &[arg1, arg2]), 0, &ctx).unwrap();
            let res: Datum = match operator {
                ScalarFuncSig::IfNullInt => op.eval_int(&ctx, &[]).unwrap().into(),
                ScalarFuncSig::IfNullReal => op.eval_real(&ctx, &[]).unwrap().into(),
                ScalarFuncSig::IfNullString => op.eval_string(&ctx, &[]).unwrap().into(),
                ScalarFuncSig::IfNullDecimal => op.eval_decimal(&ctx, &[]).unwrap().into(),
                ScalarFuncSig::IfNullTime => op.eval_time(&ctx, &[]).unwrap().into(),
                ScalarFuncSig::IfNullDuration => op.eval_duration(&ctx, &[]).unwrap().into(),
                _ => unreachable!(),
            };
            assert_eq!(res, exp);
        }
    }

    #[test]
    fn test_if() {
        let tests = vec![
            (
                ScalarFuncSig::IfInt,
                Datum::I64(1),
                Datum::I64(1),
                Datum::I64(2),
                Datum::I64(1),
            ),
            (
                ScalarFuncSig::IfInt,
                Datum::Null,
                Datum::I64(1),
                Datum::I64(2),
                Datum::I64(2),
            ),
            (
                ScalarFuncSig::IfInt,
                Datum::I64(0),
                Datum::I64(1),
                Datum::I64(2),
                Datum::I64(2),
            ),
            (
                ScalarFuncSig::IfReal,
                Datum::I64(1),
                Datum::F64(1.1),
                Datum::F64(2.2),
                Datum::F64(1.1),
            ),
            (
                ScalarFuncSig::IfReal,
                Datum::Null,
                Datum::F64(1.1),
                Datum::F64(2.2),
                Datum::F64(2.2),
            ),
            (
                ScalarFuncSig::IfReal,
                Datum::I64(0),
                Datum::F64(1.1),
                Datum::F64(2.2),
                Datum::F64(2.2),
            ),
            (
                ScalarFuncSig::IfString,
                Datum::I64(1),
                Datum::Bytes(b"abc".to_vec()),
                Datum::Bytes(b"abd".to_vec()),
                Datum::Bytes(b"abc".to_vec()),
            ),
            (
                ScalarFuncSig::IfString,
                Datum::Null,
                Datum::Bytes(b"abc".to_vec()),
                Datum::Bytes(b"abd".to_vec()),
                Datum::Bytes(b"abd".to_vec()),
            ),
            (
                ScalarFuncSig::IfString,
                Datum::I64(0),
                Datum::Bytes(b"abc".to_vec()),
                Datum::Bytes(b"abd".to_vec()),
                Datum::Bytes(b"abd".to_vec()),
            ),
            (
                ScalarFuncSig::IfDecimal,
                Datum::I64(1),
                str2dec("1.123"),
                str2dec("2.345"),
                str2dec("1.123"),
            ),
            (
                ScalarFuncSig::IfDecimal,
                Datum::Null,
                str2dec("1.123"),
                str2dec("2.345"),
                str2dec("2.345"),
            ),
            (
                ScalarFuncSig::IfDecimal,
                Datum::I64(0),
                str2dec("1.123"),
                str2dec("2.345"),
                str2dec("2.345"),
            ),
            (
                ScalarFuncSig::IfDuration,
                Datum::I64(1),
                Datum::Dur(Duration::from_nanos(123, 1).unwrap()),
                Datum::Dur(Duration::from_nanos(345, 2).unwrap()),
                Datum::Dur(Duration::from_nanos(123, 1).unwrap()),
            ),
            (
                ScalarFuncSig::IfDuration,
                Datum::Null,
                Datum::Dur(Duration::from_nanos(123, 1).unwrap()),
                Datum::Dur(Duration::from_nanos(345, 2).unwrap()),
                Datum::Dur(Duration::from_nanos(345, 2).unwrap()),
            ),
            (
                ScalarFuncSig::IfDuration,
                Datum::I64(0),
                Datum::Dur(Duration::from_nanos(123, 1).unwrap()),
                Datum::Dur(Duration::from_nanos(345, 2).unwrap()),
                Datum::Dur(Duration::from_nanos(345, 2).unwrap()),
            ),
            // TODO: add Time related tests after Time is implementted in Expression::build
        ];
        let ctx = StatementContext::default();
        for (operator, cond, branch1, branch2, exp) in tests {
            let arg1 = datum_expr(cond);
            let arg2 = datum_expr(branch1);
            let arg3 = datum_expr(branch2);
            let expected = Expression::build(datum_expr(exp), 0, &ctx).unwrap();
            let op =
                Expression::build(fncall_expr(operator, &[arg1, arg2, arg3]), 0, &ctx).unwrap();
            match operator {
                ScalarFuncSig::IfInt => {
                    let lhs = op.eval_int(&ctx, &[]).unwrap();
                    let rhs = expected.eval_int(&ctx, &[]).unwrap();
                    assert_eq!(lhs, rhs);
                }
                ScalarFuncSig::IfReal => {
                    let lhs = op.eval_real(&ctx, &[]).unwrap();
                    let rhs = expected.eval_real(&ctx, &[]).unwrap();
                    assert_eq!(lhs, rhs);
                }
                ScalarFuncSig::IfString => {
                    let lhs = op.eval_string(&ctx, &[]).unwrap();
                    let rhs = expected.eval_string(&ctx, &[]).unwrap();
                    assert_eq!(lhs, rhs);
                }
                ScalarFuncSig::IfDecimal => {
                    let lhs = op.eval_decimal(&ctx, &[]).unwrap();
                    let rhs = expected.eval_decimal(&ctx, &[]).unwrap();
                    assert_eq!(lhs, rhs);
                }
                ScalarFuncSig::IfTime => {
                    let lhs = op.eval_time(&ctx, &[]).unwrap();
                    let rhs = expected.eval_time(&ctx, &[]).unwrap();
                    assert_eq!(lhs, rhs);
                }
                ScalarFuncSig::IfDuration => {
                    let lhs = op.eval_duration(&ctx, &[]).unwrap();
                    let rhs = expected.eval_duration(&ctx, &[]).unwrap();
                    assert_eq!(lhs, rhs);
                }
                _ => unreachable!(),
            }
        }
    }
}
