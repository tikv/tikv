// Copyright 2017 TiKV Project Authors. Licensed under Apache-2.0.

use super::{EvalContext, Result, ScalarFunc};
use crate::codec::mysql::{Decimal, Duration, Json, Time};
use crate::codec::Datum;
use crate::expr::Expression;
use std::borrow::Cow;

fn if_null<F, T>(mut f: F) -> Result<Option<T>>
where
    F: FnMut(usize) -> Result<Option<T>>,
{
    let arg0 = f(0)?;
    if arg0.is_some() {
        return Ok(arg0);
    }
    f(1)
}

fn if_condition<F, T>(
    expr: &ScalarFunc,
    ctx: &mut EvalContext,
    row: &[Datum],
    mut f: F,
) -> Result<Option<T>>
where
    F: FnMut(usize, &mut EvalContext) -> Result<Option<T>>,
{
    let arg0 = expr.children[0].eval_int(ctx, row)?;
    if arg0.map_or(false, |arg| arg != 0) {
        f(1, ctx)
    } else {
        f(2, ctx)
    }
}

/// See <https://dev.mysql.com/doc/refman/5.7/en/case.html>
fn case_when<'a, F, T>(
    expr: &'a ScalarFunc,
    ctx: &mut EvalContext,
    row: &'a [Datum],
    f: F,
) -> Result<Option<T>>
where
    F: Fn(&'a Expression, &mut EvalContext) -> Result<Option<T>>,
{
    for chunk in expr.children.chunks(2) {
        if chunk.len() == 1 {
            // else statement
            return f(&chunk[0], ctx);
        }
        let cond = chunk[0].eval_int(ctx, row)?;
        if cond.unwrap_or(0) == 0 {
            continue;
        }
        return f(&chunk[1], ctx);
    }
    Ok(None)
}

impl ScalarFunc {
    pub fn if_null_int(&self, ctx: &mut EvalContext, row: &[Datum]) -> Result<Option<i64>> {
        if_null(|i| self.children[i].eval_int(ctx, row))
    }

    pub fn if_null_real(&self, ctx: &mut EvalContext, row: &[Datum]) -> Result<Option<f64>> {
        if_null(|i| self.children[i].eval_real(ctx, row))
    }

    pub fn if_null_decimal<'a, 'b: 'a>(
        &'b self,
        ctx: &mut EvalContext,
        row: &'a [Datum],
    ) -> Result<Option<Cow<'a, Decimal>>> {
        if_null(|i| self.children[i].eval_decimal(ctx, row))
    }

    pub fn if_null_string<'a, 'b: 'a>(
        &'b self,
        ctx: &mut EvalContext,
        row: &'a [Datum],
    ) -> Result<Option<Cow<'a, [u8]>>> {
        if_null(|i| self.children[i].eval_string(ctx, row))
    }

    pub fn if_null_time<'a, 'b: 'a>(
        &'b self,
        ctx: &mut EvalContext,
        row: &'a [Datum],
    ) -> Result<Option<Cow<'a, Time>>> {
        if_null(|i| self.children[i].eval_time(ctx, row))
    }

    pub fn if_null_duration<'a, 'b: 'a>(
        &'b self,
        ctx: &mut EvalContext,
        row: &'a [Datum],
    ) -> Result<Option<Duration>> {
        if_null(|i| self.children[i].eval_duration(ctx, row))
    }

    pub fn if_null_json<'a, 'b: 'a>(
        &'b self,
        ctx: &mut EvalContext,
        row: &'a [Datum],
    ) -> Result<Option<Cow<'a, Json>>> {
        if_null(|i| self.children[i].eval_json(ctx, row))
    }

    pub fn if_int(&self, ctx: &mut EvalContext, row: &[Datum]) -> Result<Option<i64>> {
        if_condition(self, ctx, row, |i, ctx| self.children[i].eval_int(ctx, row))
    }

    pub fn if_real(&self, ctx: &mut EvalContext, row: &[Datum]) -> Result<Option<f64>> {
        if_condition(self, ctx, row, |i, ctx| {
            self.children[i].eval_real(ctx, row)
        })
    }

    pub fn if_decimal<'a, 'b: 'a>(
        &'b self,
        ctx: &mut EvalContext,
        row: &'a [Datum],
    ) -> Result<Option<Cow<'a, Decimal>>> {
        if_condition(self, ctx, row, |i, ctx| {
            self.children[i].eval_decimal(ctx, row)
        })
    }

    pub fn if_string<'a, 'b: 'a>(
        &'b self,
        ctx: &mut EvalContext,
        row: &'a [Datum],
    ) -> Result<Option<Cow<'a, [u8]>>> {
        if_condition(self, ctx, row, |i, ctx| {
            self.children[i].eval_string(ctx, row)
        })
    }

    pub fn if_time<'a, 'b: 'a>(
        &'b self,
        ctx: &mut EvalContext,
        row: &'a [Datum],
    ) -> Result<Option<Cow<'a, Time>>> {
        if_condition(self, ctx, row, |i, ctx| {
            self.children[i].eval_time(ctx, row)
        })
    }

    pub fn if_duration<'a, 'b: 'a>(
        &'b self,
        ctx: &mut EvalContext,
        row: &'a [Datum],
    ) -> Result<Option<Duration>> {
        if_condition(self, ctx, row, |i, ctx| {
            self.children[i].eval_duration(ctx, row)
        })
    }

    pub fn if_json<'a, 'b: 'a>(
        &'b self,
        ctx: &mut EvalContext,
        row: &'a [Datum],
    ) -> Result<Option<Cow<'a, Json>>> {
        if_condition(self, ctx, row, |i, ctx| {
            self.children[i].eval_json(ctx, row)
        })
    }

    pub fn case_when_int(&self, ctx: &mut EvalContext, row: &[Datum]) -> Result<Option<i64>> {
        case_when(self, ctx, row, |v, ctx| v.eval_int(ctx, row))
    }

    pub fn case_when_real(&self, ctx: &mut EvalContext, row: &[Datum]) -> Result<Option<f64>> {
        case_when(self, ctx, row, |v, ctx| v.eval_real(ctx, row))
    }

    pub fn case_when_decimal<'a, 'b: 'a>(
        &'b self,
        ctx: &mut EvalContext,
        row: &'a [Datum],
    ) -> Result<Option<Cow<'a, Decimal>>> {
        case_when(self, ctx, row, |v, ctx| v.eval_decimal(ctx, row))
    }

    pub fn case_when_string<'a, 'b: 'a>(
        &'b self,
        ctx: &mut EvalContext,
        row: &'a [Datum],
    ) -> Result<Option<Cow<'a, [u8]>>> {
        case_when(self, ctx, row, |v, ctx| v.eval_string(ctx, row))
    }

    pub fn case_when_time<'a, 'b: 'a>(
        &'b self,
        ctx: &mut EvalContext,
        row: &'a [Datum],
    ) -> Result<Option<Cow<'a, Time>>> {
        case_when(self, ctx, row, |v, ctx| v.eval_time(ctx, row))
    }

    pub fn case_when_duration<'a, 'b: 'a>(
        &'b self,
        ctx: &mut EvalContext,
        row: &'a [Datum],
    ) -> Result<Option<Duration>> {
        case_when(self, ctx, row, |v, ctx| v.eval_duration(ctx, row))
    }

    pub fn case_when_json<'a, 'b: 'a>(
        &'b self,
        ctx: &mut EvalContext,
        row: &'a [Datum],
    ) -> Result<Option<Cow<'a, Json>>> {
        case_when(self, ctx, row, |v, ctx| v.eval_json(ctx, row))
    }
}

#[cfg(test)]
mod tests {
    use tipb::{Expr, ExprType, ScalarFuncSig};

    use crate::codec::mysql::{Duration, Json, Time};
    use crate::codec::Datum;
    use crate::expr::tests::{col_expr, datum_expr, scalar_func_expr, str2dec};
    use crate::expr::{EvalContext, Expression};

    #[test]
    fn test_if_null() {
        let mut ctx = EvalContext::default();
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
                Datum::Dur(Duration::from_millis(123, 1).unwrap()),
                Datum::Dur(Duration::from_millis(345, 2).unwrap()),
                Datum::Dur(Duration::from_millis(123, 1).unwrap()),
            ),
            (
                ScalarFuncSig::IfNullDuration,
                Datum::Null,
                Datum::Dur(Duration::from_millis(345, 2).unwrap()),
                Datum::Dur(Duration::from_millis(345, 2).unwrap()),
            ),
            (
                ScalarFuncSig::IfNullTime,
                Datum::Null,
                Datum::Time(
                    Time::parse_datetime(&mut ctx, "1970-01-01 12:00:00", 6, true).unwrap(),
                ),
                Datum::Time(
                    Time::parse_datetime(&mut ctx, "1970-01-01 12:00:00", 6, true).unwrap(),
                ),
            ),
            (
                ScalarFuncSig::IfNullJson,
                Datum::Null,
                Datum::Json(Json::String("hello".to_owned())),
                Datum::Json(Json::String("hello".to_owned())),
            ),
        ];
        for (operator, branch1, branch2, exp) in tests {
            let arg1 = datum_expr(branch1);
            let arg2 = datum_expr(branch2);
            let op =
                Expression::build(&mut ctx, scalar_func_expr(operator, &[arg1, arg2])).unwrap();
            let res = op.eval(&mut ctx, &[]).unwrap();
            assert_eq!(res, exp);
        }
    }

    #[test]
    fn test_if() {
        let mut ctx = EvalContext::default();
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
                Datum::Dur(Duration::from_millis(123, 1).unwrap()),
                Datum::Dur(Duration::from_millis(345, 2).unwrap()),
                Datum::Dur(Duration::from_millis(123, 1).unwrap()),
            ),
            (
                ScalarFuncSig::IfDuration,
                Datum::Null,
                Datum::Dur(Duration::from_millis(123, 1).unwrap()),
                Datum::Dur(Duration::from_millis(345, 2).unwrap()),
                Datum::Dur(Duration::from_millis(345, 2).unwrap()),
            ),
            (
                ScalarFuncSig::IfDuration,
                Datum::I64(0),
                Datum::Dur(Duration::from_millis(123, 1).unwrap()),
                Datum::Dur(Duration::from_millis(345, 2).unwrap()),
                Datum::Dur(Duration::from_millis(345, 2).unwrap()),
            ),
            (
                ScalarFuncSig::IfTime,
                Datum::I64(0),
                Datum::Time(
                    Time::parse_datetime(&mut ctx, "1970-01-01 12:00:00", 6, true).unwrap(),
                ),
                Datum::Time(
                    Time::parse_datetime(&mut ctx, "1971-01-01 12:00:00", 6, true).unwrap(),
                ),
                Datum::Time(
                    Time::parse_datetime(&mut ctx, "1971-01-01 12:00:00", 6, true).unwrap(),
                ),
            ),
            (
                ScalarFuncSig::IfJson,
                Datum::I64(0),
                Datum::Json(Json::I64(300)),
                Datum::Json(Json::String("hello".to_owned())),
                Datum::Json(Json::String("hello".to_owned())),
            ),
        ];
        for (operator, cond, branch1, branch2, exp) in tests {
            let arg1 = datum_expr(cond);
            let arg2 = datum_expr(branch1);
            let arg3 = datum_expr(branch2);
            let expected = Expression::build(&mut ctx, datum_expr(exp)).unwrap();
            let op = Expression::build(&mut ctx, scalar_func_expr(operator, &[arg1, arg2, arg3]))
                .unwrap();
            let lhs = op.eval(&mut ctx, &[]).unwrap();
            let rhs = expected.eval(&mut ctx, &[]).unwrap();
            assert_eq!(lhs, rhs);
        }
    }

    fn cond(ok: bool) -> Datum {
        if ok {
            Datum::I64(1)
        } else {
            Datum::I64(0)
        }
    }

    #[test]
    fn test_case_when() {
        let mut ctx = EvalContext::default();
        let dec1 = Datum::Dec("1.1".parse().unwrap());
        let dec2 = Datum::Dec("2.2".parse().unwrap());
        let dur1 = Datum::Dur(Duration::parse(b"01:00:00", 0).unwrap());
        let dur2 = Datum::Dur(Duration::parse(b"12:00:12", 0).unwrap());
        let time1 =
            Datum::Time(Time::parse_datetime(&mut ctx, "2012-12-12 12:00:23", 0, false).unwrap());
        let s = "你好".as_bytes().to_owned();

        let cases = vec![
            (
                ScalarFuncSig::CaseWhenInt,
                vec![cond(true), Datum::I64(3), cond(true), Datum::I64(5)],
                Datum::I64(3),
            ),
            (
                ScalarFuncSig::CaseWhenDecimal,
                vec![cond(false), dec1, cond(true), dec2.clone()],
                dec2,
            ),
            (
                ScalarFuncSig::CaseWhenDuration,
                vec![Datum::Null, dur1, cond(true), dur2.clone()],
                dur2,
            ),
            (ScalarFuncSig::CaseWhenTime, vec![time1.clone()], time1),
            (
                ScalarFuncSig::CaseWhenReal,
                vec![cond(false), Datum::Null],
                Datum::Null,
            ),
            (
                ScalarFuncSig::CaseWhenString,
                vec![cond(true), Datum::Bytes(s.clone())],
                Datum::Bytes(s),
            ),
            (
                ScalarFuncSig::CaseWhenJson,
                vec![
                    cond(false),
                    Datum::Null,
                    Datum::Null,
                    Datum::Null,
                    Datum::Json(Json::I64(23)),
                ],
                Datum::Json(Json::I64(23)),
            ),
        ];

        for (sig, row, exp) in cases {
            let children: Vec<Expr> = (0..row.len()).map(|id| col_expr(id as i64)).collect();
            let mut expr = Expr::default();
            expr.set_tp(ExprType::ScalarFunc);
            expr.set_sig(sig);

            expr.set_children(children.into());
            let e = Expression::build(&mut ctx, expr).unwrap();
            let res = e.eval(&mut ctx, &row).unwrap();
            assert_eq!(res, exp);
        }
    }
}
