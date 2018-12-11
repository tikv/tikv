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
use std::cmp::{max, min, Ordering};
use std::{f64, i64};

use chrono::TimeZone;

use cop_datatype::prelude::*;
use cop_datatype::FieldTypeFlag;

use super::{Error, EvalContext, Result, ScalarFunc};
use coprocessor::codec::mysql::{Decimal, Duration, Json, Time, TimeType};
use coprocessor::codec::{datum, mysql, Datum};
use coprocessor::dag::expr::Expression;

#[derive(Clone, Copy, PartialEq)]
pub enum CmpOp {
    LT,
    LE,
    GT,
    GE,
    NE,
    EQ,
    NullEQ,
}

impl ScalarFunc {
    pub fn compare_int(
        &self,
        ctx: &mut EvalContext,
        row: &[Datum],
        op: CmpOp,
    ) -> Result<Option<i64>> {
        let e = |i: usize| self.children[i].eval_int(ctx, row);
        do_compare(e, op, |l, r| {
            let lhs_unsigned = self.children[0]
                .field_type()
                .flag()
                .contains(FieldTypeFlag::UNSIGNED);
            let rhs_unsigned = self.children[1]
                .field_type()
                .flag()
                .contains(FieldTypeFlag::UNSIGNED);
            Ok(cmp_i64_with_unsigned_flag(l, lhs_unsigned, r, rhs_unsigned))
        })
    }

    pub fn compare_real(
        &self,
        ctx: &mut EvalContext,
        row: &[Datum],
        op: CmpOp,
    ) -> Result<Option<i64>> {
        do_compare(
            |i| self.children[i].eval_real(ctx, row),
            op,
            |l, r| datum::cmp_f64(l, r).map_err(Error::from),
        )
    }

    pub fn compare_decimal(
        &self,
        ctx: &mut EvalContext,
        row: &[Datum],
        op: CmpOp,
    ) -> Result<Option<i64>> {
        let e = |i: usize| self.children[i].eval_decimal(ctx, row);
        do_compare(e, op, |l, r| Ok(l.cmp(&r)))
    }

    pub fn compare_string(
        &self,
        ctx: &mut EvalContext,
        row: &[Datum],
        op: CmpOp,
    ) -> Result<Option<i64>> {
        let e = |i: usize| self.children[i].eval_string(ctx, row);
        do_compare(e, op, |l, r| Ok(l.cmp(&r)))
    }

    pub fn compare_time(
        &self,
        ctx: &mut EvalContext,
        row: &[Datum],
        op: CmpOp,
    ) -> Result<Option<i64>> {
        let e = |i: usize| self.children[i].eval_time(ctx, row);
        do_compare(e, op, |l, r| Ok(l.cmp(&r)))
    }

    pub fn compare_duration(
        &self,
        ctx: &mut EvalContext,
        row: &[Datum],
        op: CmpOp,
    ) -> Result<Option<i64>> {
        let e = |i: usize| self.children[i].eval_duration(ctx, row);
        do_compare(e, op, |l, r| Ok(l.cmp(&r)))
    }

    pub fn compare_json(
        &self,
        ctx: &mut EvalContext,
        row: &[Datum],
        op: CmpOp,
    ) -> Result<Option<i64>> {
        let e = |i: usize| self.children[i].eval_json(ctx, row);
        do_compare(e, op, |l, r| Ok(l.cmp(&r)))
    }

    /// See http://dev.mysql.com/doc/refman/5.7/en/comparison-operators.html#function_coalesce
    pub fn coalesce_int(&self, ctx: &mut EvalContext, row: &[Datum]) -> Result<Option<i64>> {
        do_coalesce(self, |v| v.eval_int(ctx, row))
    }

    pub fn coalesce_real(&self, ctx: &mut EvalContext, row: &[Datum]) -> Result<Option<f64>> {
        do_coalesce(self, |v| v.eval_real(ctx, row))
    }

    pub fn coalesce_decimal<'a, 'b: 'a>(
        &'b self,
        ctx: &mut EvalContext,
        row: &'a [Datum],
    ) -> Result<Option<Cow<'a, Decimal>>> {
        do_coalesce(self, |v| v.eval_decimal(ctx, row))
    }

    pub fn coalesce_time<'a, 'b: 'a>(
        &'b self,
        ctx: &mut EvalContext,
        row: &'a [Datum],
    ) -> Result<Option<Cow<'a, Time>>> {
        do_coalesce(self, |v| v.eval_time(ctx, row))
    }

    pub fn coalesce_duration<'a, 'b: 'a>(
        &'b self,
        ctx: &mut EvalContext,
        row: &'a [Datum],
    ) -> Result<Option<Cow<'a, Duration>>> {
        do_coalesce(self, |v| v.eval_duration(ctx, row))
    }

    pub fn coalesce_string<'a, 'b: 'a>(
        &'b self,
        ctx: &mut EvalContext,
        row: &'a [Datum],
    ) -> Result<Option<Cow<'a, [u8]>>> {
        do_coalesce(self, |v| v.eval_string(ctx, row))
    }

    pub fn coalesce_json<'a, 'b: 'a>(
        &'b self,
        ctx: &mut EvalContext,
        row: &'a [Datum],
    ) -> Result<Option<Cow<'a, Json>>> {
        do_coalesce(self, |v| v.eval_json(ctx, row))
    }

    pub fn greatest_int(&self, ctx: &mut EvalContext, row: &[Datum]) -> Result<Option<i64>> {
        do_get_extremum(self, |e| e.eval_int(ctx, row), max)
    }

    pub fn greatest_real(&self, ctx: &mut EvalContext, row: &[Datum]) -> Result<Option<f64>> {
        do_get_extremum(self, |e| e.eval_real(ctx, row), |x, y| x.max(y))
    }

    pub fn greatest_decimal<'a, 'b: 'a>(
        &'b self,
        ctx: &mut EvalContext,
        row: &'a [Datum],
    ) -> Result<Option<Cow<'a, Decimal>>> {
        do_get_extremum(self, |e| e.eval_decimal(ctx, row), max)
    }

    pub fn greatest_time<'a, 'b: 'a>(
        &'b self,
        ctx: &mut EvalContext,
        row: &'a [Datum],
    ) -> Result<Option<Cow<'a, [u8]>>> {
        let mut greatest = mysql::time::zero_datetime(ctx.cfg.tz);

        for exp in &self.children {
            let s = try_opt!(exp.eval_string_and_decode(ctx, row));
            match Time::parse_datetime(&s, Time::parse_fsp(&s), ctx.cfg.tz) {
                Ok(t) => greatest = max(greatest, t),
                Err(_) => {
                    if let Err(e) = ctx.handle_invalid_time_error(Error::invalid_time_format(&s)) {
                        return Err(e);
                    } else {
                        return Ok(None);
                    }
                }
            }
        }

        Ok(Some(Cow::Owned(greatest.to_string().into_bytes())))
    }

    pub fn greatest_string<'a, 'b: 'a>(
        &'b self,
        ctx: &mut EvalContext,
        row: &'a [Datum],
    ) -> Result<Option<Cow<'a, [u8]>>> {
        do_get_extremum(self, |e| e.eval_string(ctx, row), max)
    }

    pub fn least_int(&self, ctx: &mut EvalContext, row: &[Datum]) -> Result<Option<i64>> {
        do_get_extremum(self, |e| e.eval_int(ctx, row), min)
    }

    pub fn least_real(&self, ctx: &mut EvalContext, row: &[Datum]) -> Result<Option<f64>> {
        do_get_extremum(self, |e| e.eval_real(ctx, row), |x, y| x.min(y))
    }

    pub fn least_decimal<'a, 'b: 'a>(
        &'b self,
        ctx: &mut EvalContext,
        row: &'a [Datum],
    ) -> Result<Option<Cow<'a, Decimal>>> {
        do_get_extremum(self, |e| e.eval_decimal(ctx, row), min)
    }

    pub fn least_time<'a, 'b: 'a>(
        &'b self,
        ctx: &mut EvalContext,
        row: &'a [Datum],
    ) -> Result<Option<Cow<'a, [u8]>>> {
        let mut res = None;
        let mut least = Time::new(
            ctx.cfg.tz.timestamp(
                mysql::time::MAX_TIMESTAMP,
                mysql::time::MAX_TIME_NANOSECONDS,
            ),
            TimeType::DateTime,
            mysql::MAX_FSP,
        )?;

        for exp in &self.children {
            let s = try_opt!(exp.eval_string_and_decode(ctx, row));
            match Time::parse_datetime(&s, Time::parse_fsp(&s), ctx.cfg.tz) {
                Ok(t) => least = min(least, t),
                Err(_) => match ctx.handle_invalid_time_error(Error::invalid_time_format(&s)) {
                    Err(e) => return Err(e),
                    _ => return Ok(None),
                },
            }
        }

        if res == None {
            res = Some(Cow::Owned(least.to_string().into_bytes()));
        }
        Ok(res)
    }

    pub fn least_string<'a, 'b: 'a>(
        &'b self,
        ctx: &mut EvalContext,
        row: &'a [Datum],
    ) -> Result<Option<Cow<'a, [u8]>>> {
        do_get_extremum(self, |e| e.eval_string(ctx, row), min)
    }

    pub fn in_int(&self, ctx: &mut EvalContext, row: &[Datum]) -> Result<Option<i64>> {
        do_in(
            self,
            |v| v.eval_int(ctx, row),
            |l, r| {
                let lhs_unsigned = self.children[0]
                    .field_type()
                    .flag()
                    .contains(FieldTypeFlag::UNSIGNED);
                let rhs_unsigned = self.children[1]
                    .field_type()
                    .flag()
                    .contains(FieldTypeFlag::UNSIGNED);
                Ok(cmp_i64_with_unsigned_flag(
                    *l,
                    lhs_unsigned,
                    *r,
                    rhs_unsigned,
                ))
            },
        )
    }

    pub fn in_real(&self, ctx: &mut EvalContext, row: &[Datum]) -> Result<Option<i64>> {
        do_in(
            self,
            |v| v.eval_real(ctx, row),
            |l, r| datum::cmp_f64(*l, *r).map_err(Error::from),
        )
    }

    pub fn in_decimal(&self, ctx: &mut EvalContext, row: &[Datum]) -> Result<Option<i64>> {
        do_in(self, |v| v.eval_decimal(ctx, row), |l, r| Ok(l.cmp(r)))
    }

    pub fn in_time(&self, ctx: &mut EvalContext, row: &[Datum]) -> Result<Option<i64>> {
        do_in(self, |v| v.eval_time(ctx, row), |l, r| Ok(l.cmp(r)))
    }

    pub fn in_duration(&self, ctx: &mut EvalContext, row: &[Datum]) -> Result<Option<i64>> {
        do_in(self, |v| v.eval_duration(ctx, row), |l, r| Ok(l.cmp(r)))
    }

    pub fn in_string(&self, ctx: &mut EvalContext, row: &[Datum]) -> Result<Option<i64>> {
        do_in(self, |v| v.eval_string(ctx, row), |l, r| Ok(l.cmp(r)))
    }

    pub fn in_json(&self, ctx: &mut EvalContext, row: &[Datum]) -> Result<Option<i64>> {
        do_in(self, |v| v.eval_json(ctx, row), |l, r| Ok(l.cmp(r)))
    }

    pub fn interval_int(&self, ctx: &mut EvalContext, row: &[Datum]) -> Result<Option<i64>> {
        let target = match self.children[0].eval_int(ctx, row)? {
            None => return Ok(Some(-1)),
            Some(v) => v,
        };
        let tus = self.children[0]
            .field_type()
            .flag()
            .contains(FieldTypeFlag::UNSIGNED);

        let mut left = 1;
        let mut right = self.children.len();
        while left < right {
            let mid = left + (right - left) / 2;
            let m = self.children[mid].eval_int(ctx, row)?.unwrap_or(target);

            let mus = self.children[mid]
                .field_type()
                .flag()
                .contains(FieldTypeFlag::UNSIGNED);

            let cmp = match (tus, mus) {
                (false, false) => target < m,
                (true, true) => (target as u64) < (m as u64),
                (false, true) => target < 0 || (target as u64) < (m as u64),
                (true, false) => m > 0 && (target as u64) < (m as u64),
            };

            if !cmp {
                left = mid + 1
            } else {
                right = mid
            }
        }
        Ok(Some((left - 1) as i64))
    }

    pub fn interval_real(&self, ctx: &mut EvalContext, row: &[Datum]) -> Result<Option<i64>> {
        let target = match self.children[0].eval_real(ctx, row)? {
            None => return Ok(Some(-1)),
            Some(v) => v,
        };

        let mut left = 1;
        let mut right = self.children.len();
        while left < right {
            let mid = left + (right - left) / 2;
            let m = self.children[mid].eval_real(ctx, row)?.unwrap_or(target);

            if target >= m {
                left = mid + 1
            } else {
                right = mid
            }
        }
        Ok(Some((left - 1) as i64))
    }
}

fn do_compare<T, E, F>(mut e: E, op: CmpOp, get_order: F) -> Result<Option<i64>>
where
    E: FnMut(usize) -> Result<Option<T>>,
    F: Fn(T, T) -> Result<Ordering>,
{
    let lhs = e(0)?;
    if lhs.is_none() && op != CmpOp::NullEQ {
        return Ok(None);
    }
    let rhs = e(1)?;
    match (lhs, rhs) {
        (None, None) => Ok(Some(1)),
        (Some(lhs), Some(rhs)) => {
            let ordering = get_order(lhs, rhs)?;
            let r = match op {
                CmpOp::LT => ordering == Ordering::Less,
                CmpOp::LE => ordering != Ordering::Greater,
                CmpOp::GT => ordering == Ordering::Greater,
                CmpOp::GE => ordering != Ordering::Less,
                CmpOp::NE => ordering != Ordering::Equal,
                CmpOp::EQ | CmpOp::NullEQ => ordering == Ordering::Equal,
            };
            Ok(Some(r as i64))
        }
        _ => match op {
            CmpOp::NullEQ => Ok(Some(0)),
            _ => Ok(None),
        },
    }
}

#[inline]
fn cmp_i64_with_unsigned_flag(
    lhs: i64,
    lhs_unsigned: bool,
    rhs: i64,
    rhs_unsigned: bool,
) -> Ordering {
    match (lhs_unsigned, rhs_unsigned) {
        (false, false) => lhs.cmp(&rhs),
        (true, true) => {
            let lhs = lhs as u64;
            let rhs = rhs as u64;
            lhs.cmp(&rhs)
        }
        (true, false) => if rhs < 0 || lhs as u64 > i64::MAX as u64 {
            Ordering::Greater
        } else {
            lhs.cmp(&rhs)
        },
        (false, true) => if lhs < 0 || rhs as u64 > i64::MAX as u64 {
            Ordering::Less
        } else {
            lhs.cmp(&rhs)
        },
    }
}

fn do_coalesce<'a, F, T>(expr: &'a ScalarFunc, mut f: F) -> Result<Option<T>>
where
    F: FnMut(&'a Expression) -> Result<Option<T>>,
{
    for exp in &expr.children {
        let v = f(exp)?;
        if v.is_some() {
            return Ok(v);
        }
    }
    Ok(None)
}

fn do_in<'a, T, E, F>(expr: &'a ScalarFunc, mut f: F, get_order: E) -> Result<Option<i64>>
where
    F: FnMut(&'a Expression) -> Result<Option<T>>,
    E: Fn(&T, &T) -> Result<Ordering>,
{
    let (first, others) = expr.children.split_first().unwrap();
    let arg = try_opt!(f(first));
    let mut ret_when_not_matched = Ok(Some(0));
    for exp in others {
        let arg1 = f(exp)?;
        if arg1.is_none() {
            ret_when_not_matched = Ok(None);
            continue;
        }
        let cmp_result = get_order(&arg, &arg1.unwrap())?;
        if cmp_result == Ordering::Equal {
            return Ok(Some(1));
        }
    }
    ret_when_not_matched
}

#[inline]
fn do_get_extremum<'a, T, E, F>(
    expr: &'a ScalarFunc,
    mut evaluator: F,
    chooser: E,
) -> Result<Option<T>>
where
    F: FnMut(&'a Expression) -> Result<Option<T>>,
    E: Fn(T, T) -> T,
{
    let (first, others) = expr.children.split_first().unwrap();
    let mut res = try_opt!(evaluator(first));
    for exp in others {
        let val = try_opt!(evaluator(exp));
        res = chooser(res, val);
    }
    Ok(Some(res))
}

#[cfg(test)]
mod tests {
    use super::super::EvalConfig;
    use super::*;
    use coprocessor::codec::error::ERR_TRUNCATE_WRONG_VALUE;
    use coprocessor::codec::mysql::{Decimal, Duration, Json, Time};
    use coprocessor::codec::Datum;
    use coprocessor::dag::expr::tests::{col_expr, datum_expr, str2dec};
    use coprocessor::dag::expr::{EvalContext, Expression};
    use protobuf::RepeatedField;
    use std::sync::Arc;
    use std::{i64, u64};
    use tipb::expression::{Expr, ExprType, ScalarFuncSig};

    #[test]
    fn test_cmp_i64_with_unsigned_flag() {
        let cases = vec![
            (5, false, 3, false, Ordering::Greater),
            (u64::MAX as i64, false, 5 as i64, false, Ordering::Less),
            (
                u64::MAX as i64,
                true,
                (u64::MAX - 1) as i64,
                true,
                Ordering::Greater,
            ),
            (u64::MAX as i64, true, 5 as i64, true, Ordering::Greater),
            (5, true, i64::MIN, false, Ordering::Greater),
            (u64::MAX as i64, true, i64::MIN, false, Ordering::Greater),
            (5, true, 3, false, Ordering::Greater),
            (i64::MIN, false, 3, true, Ordering::Less),
            (5, false, u64::MAX as i64, true, Ordering::Less),
            (5, false, 3, true, Ordering::Greater),
        ];
        for (a, b, c, d, e) in cases {
            let o = cmp_i64_with_unsigned_flag(a, b, c, d);
            assert_eq!(o, e);
        }
    }

    #[test]
    fn test_coalesce() {
        let dec = "1.1".parse::<Decimal>().unwrap();
        let s = "你好".as_bytes().to_owned();
        let dur = Duration::parse(b"01:00:00", 0).unwrap();
        let json = Json::I64(12);
        let t = Time::parse_utc_datetime("2012-12-12 12:00:39", 0).unwrap();
        let cases = vec![
            (ScalarFuncSig::CoalesceInt, vec![Datum::Null], Datum::Null),
            (
                ScalarFuncSig::CoalesceInt,
                vec![Datum::Null, Datum::Null],
                Datum::Null,
            ),
            (
                ScalarFuncSig::CoalesceInt,
                vec![Datum::Null, Datum::Null, Datum::Null],
                Datum::Null,
            ),
            (
                ScalarFuncSig::CoalesceInt,
                vec![Datum::Null, Datum::I64(0), Datum::Null],
                Datum::I64(0),
            ),
            (
                ScalarFuncSig::CoalesceReal,
                vec![Datum::Null, Datum::F64(3.2), Datum::Null],
                Datum::F64(3.2),
            ),
            (
                ScalarFuncSig::CoalesceInt,
                vec![Datum::I64(32), Datum::F64(1.0)],
                Datum::I64(32),
            ),
            (
                ScalarFuncSig::CoalesceDecimal,
                vec![Datum::Null, Datum::Dec(dec.clone())],
                Datum::Dec(dec),
            ),
            (
                ScalarFuncSig::CoalesceDuration,
                vec![Datum::Null, Datum::Dur(dur.clone())],
                Datum::Dur(dur),
            ),
            (
                ScalarFuncSig::CoalesceJson,
                vec![Datum::Json(json.clone())],
                Datum::Json(json),
            ),
            (
                ScalarFuncSig::CoalesceString,
                vec![Datum::Bytes(s.clone())],
                Datum::Bytes(s),
            ),
            (
                ScalarFuncSig::CoalesceTime,
                vec![Datum::Time(t.clone())],
                Datum::Time(t),
            ),
        ];

        let mut ctx = EvalContext::default();

        for (sig, row, exp) in cases {
            let children: Vec<Expr> = (0..row.len()).map(|id| col_expr(id as i64)).collect();
            let mut expr = Expr::new();
            expr.set_tp(ExprType::ScalarFunc);
            expr.set_sig(sig);

            expr.set_children(RepeatedField::from_vec(children));
            let e = Expression::build(&ctx, expr).unwrap();
            let res = e.eval(&mut ctx, &row).unwrap();
            assert_eq!(res, exp);
        }
    }

    #[test]
    fn test_in() {
        let dec1 = "1.1".parse::<Decimal>().unwrap();
        let dec2 = "1.11".parse::<Decimal>().unwrap();
        let dur1 = Duration::parse(b"01:00:00", 0).unwrap();
        let dur2 = Duration::parse(b"02:00:00", 0).unwrap();
        let json1 = Json::I64(11);
        let json2 = Json::I64(12);
        let s1 = "你好".as_bytes().to_owned();
        let s2 = "你好啊".as_bytes().to_owned();
        let t1 = Time::parse_utc_datetime("2012-12-12 12:00:39", 0).unwrap();
        let t2 = Time::parse_utc_datetime("2012-12-12 13:00:39", 0).unwrap();
        let cases = vec![
            (
                ScalarFuncSig::InInt,
                vec![Datum::I64(1), Datum::I64(2)],
                Datum::I64(0),
            ),
            (
                ScalarFuncSig::InInt,
                vec![Datum::I64(1), Datum::I64(2), Datum::I64(1)],
                Datum::I64(1),
            ),
            (
                ScalarFuncSig::InInt,
                vec![Datum::I64(1), Datum::I64(2), Datum::Null],
                Datum::Null,
            ),
            (
                ScalarFuncSig::InInt,
                vec![Datum::I64(1), Datum::I64(2), Datum::Null, Datum::I64(1)],
                Datum::I64(1),
            ),
            (
                ScalarFuncSig::InInt,
                vec![Datum::Null, Datum::I64(2), Datum::I64(1)],
                Datum::Null,
            ),
            (
                ScalarFuncSig::InReal,
                vec![Datum::F64(3.1), Datum::F64(3.2), Datum::F64(3.3)],
                Datum::I64(0),
            ),
            (
                ScalarFuncSig::InReal,
                vec![Datum::F64(3.1), Datum::F64(3.2), Datum::F64(3.1)],
                Datum::I64(1),
            ),
            (
                ScalarFuncSig::InDecimal,
                vec![Datum::Dec(dec1.clone()), Datum::Dec(dec2.clone())],
                Datum::I64(0),
            ),
            (
                ScalarFuncSig::InDecimal,
                vec![
                    Datum::Dec(dec1.clone()),
                    Datum::Dec(dec2.clone()),
                    Datum::Dec(dec1.clone()),
                ],
                Datum::I64(1),
            ),
            (
                ScalarFuncSig::InDuration,
                vec![Datum::Dur(dur1.clone()), Datum::Dur(dur2.clone())],
                Datum::I64(0),
            ),
            (
                ScalarFuncSig::InDuration,
                vec![
                    Datum::Dur(dur1.clone()),
                    Datum::Dur(dur2.clone()),
                    Datum::Dur(dur1.clone()),
                ],
                Datum::I64(1),
            ),
            (
                ScalarFuncSig::InJson,
                vec![Datum::Json(json1.clone()), Datum::Json(json2.clone())],
                Datum::I64(0),
            ),
            (
                ScalarFuncSig::InJson,
                vec![
                    Datum::Json(json1.clone()),
                    Datum::Json(json2.clone()),
                    Datum::Json(json1.clone()),
                ],
                Datum::I64(1),
            ),
            (
                ScalarFuncSig::InString,
                vec![Datum::Bytes(s1.clone()), Datum::Bytes(s2.clone())],
                Datum::I64(0),
            ),
            (
                ScalarFuncSig::InString,
                vec![
                    Datum::Bytes(s1.clone()),
                    Datum::Bytes(s2.clone()),
                    Datum::Bytes(s1.clone()),
                ],
                Datum::I64(1),
            ),
            (
                ScalarFuncSig::InTime,
                vec![Datum::Time(t1.clone()), Datum::Time(t2.clone())],
                Datum::I64(0),
            ),
            (
                ScalarFuncSig::InTime,
                vec![
                    Datum::Time(t1.clone()),
                    Datum::Time(t2.clone()),
                    Datum::Time(t1.clone()),
                ],
                Datum::I64(1),
            ),
        ];

        let mut ctx = EvalContext::default();

        for (sig, row, exp) in cases {
            let children: Vec<Expr> = (0..row.len()).map(|id| col_expr(id as i64)).collect();
            let mut expr = Expr::new();
            expr.set_tp(ExprType::ScalarFunc);
            expr.set_sig(sig);

            expr.set_children(RepeatedField::from_vec(children));
            let e = Expression::build(&ctx, expr).unwrap();
            let res = e.eval(&mut ctx, &row).unwrap();
            assert_eq!(res, exp);
        }
    }

    #[test]
    fn test_greatest_and_least() {
        let s1 = "你好".as_bytes().to_owned();
        let s2 = "你好啊".as_bytes().to_owned();
        let s3 = b"nihaoa".to_owned().to_vec();
        let t1 = b"2012-12-12 12:00:39".to_owned().to_vec();
        let t2 = b"2012-12-24 12:00:39".to_owned().to_vec();
        let t3 = b"2012-12-31 12:00:39".to_owned().to_vec();
        let t4 = b"invalid_time".to_owned().to_vec();
        let t5 = b"2012-12-12 12:00:38.12003800000".to_owned().to_vec();
        let t6 = b"2012-12-31 12:00:39.120050".to_owned().to_vec();
        let t7 = b"2018-04-03.invalid".to_owned().to_vec();
        let _t8 = b"2012-12-31 12:00:40.invalid".to_owned().to_vec();

        let int_cases = vec![
            (vec![Datum::Null, Datum::Null], Datum::Null, Datum::Null),
            (
                vec![Datum::I64(1), Datum::I64(-1), Datum::Null],
                Datum::Null,
                Datum::Null,
            ),
            (
                vec![Datum::I64(-2), Datum::I64(-1), Datum::I64(1), Datum::I64(2)],
                Datum::I64(2),
                Datum::I64(-2),
            ),
            (
                vec![
                    Datum::I64(i64::MIN),
                    Datum::I64(0),
                    Datum::I64(-1),
                    Datum::I64(i64::MAX),
                ],
                Datum::I64(i64::MAX),
                Datum::I64(i64::MIN),
            ),
            (
                vec![Datum::U64(0), Datum::U64(4), Datum::U64(8)],
                Datum::I64(8),
                Datum::I64(0),
            ),
        ];

        let real_cases = vec![
            (vec![Datum::Null, Datum::Null], Datum::Null, Datum::Null),
            (
                vec![Datum::F64(1.0), Datum::F64(-1.0), Datum::Null],
                Datum::Null,
                Datum::Null,
            ),
            (
                vec![
                    Datum::F64(1.0),
                    Datum::F64(-1.0),
                    Datum::F64(-2.0),
                    Datum::F64(0f64),
                ],
                Datum::F64(1.0),
                Datum::F64(-2.0),
            ),
            (
                vec![Datum::F64(f64::MAX), Datum::F64(f64::MIN), Datum::F64(0f64)],
                Datum::F64(f64::MAX),
                Datum::F64(f64::MIN),
            ),
            (
                vec![Datum::F64(f64::NAN), Datum::F64(0f64)],
                Datum::F64(0f64),
                Datum::F64(0f64),
            ),
            (
                vec![
                    Datum::F64(f64::INFINITY),
                    Datum::F64(f64::NEG_INFINITY),
                    Datum::F64(f64::MAX),
                    Datum::F64(f64::MIN),
                ],
                Datum::F64(f64::INFINITY),
                Datum::F64(f64::NEG_INFINITY),
            ),
        ];

        let decimal_cases = vec![
            (vec![Datum::Null, Datum::Null], Datum::Null, Datum::Null),
            (
                vec![str2dec("1.0"), str2dec("-1.0"), Datum::Null],
                Datum::Null,
                Datum::Null,
            ),
            (
                vec![
                    str2dec("1.1"),
                    str2dec("-1.1"),
                    str2dec("0.0"),
                    str2dec("0.000"),
                ],
                str2dec("1.1"),
                str2dec("-1.1"),
            ),
        ];

        let string_cases = vec![
            (vec![Datum::Null, Datum::Null], Datum::Null, Datum::Null),
            (
                vec![
                    Datum::Bytes(s1.clone()),
                    Datum::Bytes(s2.clone()),
                    Datum::Null,
                ],
                Datum::Null,
                Datum::Null,
            ),
            (
                vec![
                    Datum::Bytes(s1.clone()),
                    Datum::Bytes(s2.clone()),
                    Datum::Bytes(s3.clone()),
                ],
                Datum::Bytes(s2),
                Datum::Bytes(s3),
            ),
        ];

        let time_cases = vec![
            (vec![Datum::Null, Datum::Null], Datum::Null, Datum::Null),
            (
                vec![
                    Datum::Bytes(t1.clone()),
                    Datum::Bytes(t2.clone()),
                    Datum::Null,
                ],
                Datum::Null,
                Datum::Null,
            ),
            (
                vec![
                    Datum::Bytes(t1.clone()),
                    Datum::Bytes(t2.clone()),
                    Datum::Bytes(t3.clone()),
                ],
                Datum::Bytes(t3.clone()),
                Datum::Bytes(t1.clone()),
            ),
            (
                vec![
                    Datum::Bytes(t1.clone()),
                    Datum::Bytes(t2.clone()),
                    Datum::Bytes(t3.clone()),
                    Datum::Bytes(t4.clone()),
                ],
                Datum::Null,
                Datum::Null,
            ),
            (
                vec![
                    Datum::Bytes(t1.clone()),
                    Datum::Bytes(t2.clone()),
                    Datum::Bytes(t3.clone()),
                    Datum::Bytes(t5.clone()),
                    Datum::Bytes(t6.clone()),
                    Datum::Bytes(t7.clone()),
                ],
                Datum::Bytes(b"2018-04-03 00:00:00.000000".to_vec()),
                Datum::Bytes(b"2012-12-12 12:00:38.120038".to_vec()),
            ),
        ];

        fn do_test(
            greatest_sig: ScalarFuncSig,
            least_sig: ScalarFuncSig,
            cases: Vec<(Vec<Datum>, Datum, Datum)>,
        ) {
            let mut ctx = EvalContext::default();
            for (row, greatest_exp, least_exp) in cases {
                // Evaluate and test greatest
                {
                    let children: Vec<Expr> = row.iter().map(|d| datum_expr(d.clone())).collect();
                    let mut expr = Expr::new();
                    expr.set_tp(ExprType::ScalarFunc);
                    expr.set_sig(greatest_sig);
                    expr.set_children(RepeatedField::from_vec(children));
                    let e = Expression::build(&ctx, expr).unwrap();
                    let res = e.eval(&mut ctx, &[]).unwrap();
                    assert_eq!(res, greatest_exp);
                }
                // Evaluate and test least
                {
                    let children: Vec<Expr> = row.iter().map(|d| datum_expr(d.clone())).collect();
                    let mut expr = Expr::new();
                    expr.set_tp(ExprType::ScalarFunc);
                    expr.set_sig(least_sig);
                    expr.set_children(RepeatedField::from_vec(children));
                    let e = Expression::build(&ctx, expr).unwrap();
                    let res = e.eval(&mut ctx, &[]).unwrap();
                    assert_eq!(res, least_exp);
                }
            }
        }

        do_test(
            ScalarFuncSig::GreatestInt,
            ScalarFuncSig::LeastInt,
            int_cases,
        );
        do_test(
            ScalarFuncSig::GreatestReal,
            ScalarFuncSig::LeastReal,
            real_cases,
        );
        do_test(
            ScalarFuncSig::GreatestDecimal,
            ScalarFuncSig::LeastDecimal,
            decimal_cases,
        );
        do_test(
            ScalarFuncSig::GreatestString,
            ScalarFuncSig::LeastString,
            string_cases,
        );
        do_test(
            ScalarFuncSig::GreatestTime,
            ScalarFuncSig::LeastTime,
            time_cases,
        );

        {
            let mut eval_config = EvalConfig::new();
            eval_config
                .set_in_insert_stmt(true)
                .set_strict_sql_mode(true);
            let mut ctx = EvalContext::new(Arc::new(eval_config));
            let row = vec![
                Datum::Bytes(t1.clone()),
                Datum::Bytes(t2.clone()),
                Datum::Bytes(t3.clone()),
                Datum::Bytes(t4.clone()),
            ];
            let children: Vec<Expr> = row.iter().map(|d| datum_expr(d.clone())).collect();
            let mut expr = Expr::new();
            expr.set_tp(ExprType::ScalarFunc);
            expr.set_sig(ScalarFuncSig::GreatestTime);
            expr.set_children(RepeatedField::from_vec(children));
            let e = Expression::build(&ctx, expr).unwrap();
            let err = e.eval(&mut ctx, &[]).unwrap_err();
            assert_eq!(err.code(), ERR_TRUNCATE_WRONG_VALUE);
        }
    }
}
