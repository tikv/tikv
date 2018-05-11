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
use std::cmp::Ordering;
use std::i64;
use std::slice::Iter;

use super::{Error, EvalContext, FnCall, Result};
use coprocessor::codec::mysql::{Decimal, Duration, Json, Time};
use coprocessor::codec::{datum, mysql, Datum};
use coprocessor::dag::expr::Expression;

const MAX_RECURSE_LEVEL: usize = 1024;

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

impl FnCall {
    pub fn compare_int(
        &self,
        ctx: &mut EvalContext,
        row: &[Datum],
        op: CmpOp,
    ) -> Result<Option<i64>> {
        let e = |i: usize| self.children[i].eval_int(ctx, row);
        do_compare(e, op, |l, r| {
            let lhs_unsigned = mysql::has_unsigned_flag(self.children[0].get_tp().get_flag());
            let rhs_unsigned = mysql::has_unsigned_flag(self.children[1].get_tp().get_flag());
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

    pub fn in_int(&self, ctx: &mut EvalContext, row: &[Datum]) -> Result<Option<i64>> {
        do_in(
            self,
            |v| v.eval_int(ctx, row),
            |l, r| {
                let lhs_unsigned = mysql::has_unsigned_flag(self.children[0].get_tp().get_flag());
                let rhs_unsigned = mysql::has_unsigned_flag(self.children[1].get_tp().get_flag());
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

    /// NOTE: LIKE compare target with pattern as bytes, even if they have different
    /// charsets. This behaviour is for keeping compatible with TiDB. But MySQL
    /// compare them as bytes only if any charset of pattern or target is binary,
    /// otherwise MySQL will compare decoded string.
    pub fn like(&self, ctx: &mut EvalContext, row: &[Datum]) -> Result<Option<i64>> {
        let target = try_opt!(self.children[0].eval_string(ctx, row));
        let pattern = try_opt!(self.children[1].eval_string(ctx, row));
        let escape = try_opt!(self.children[2].eval_int(ctx, row)) as u32;
        Ok(Some(like(&target, &pattern, escape, 0)? as i64))
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

fn do_coalesce<'a, F, T>(expr: &'a FnCall, mut f: F) -> Result<Option<T>>
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

fn do_in<'a, T, E, F>(expr: &'a FnCall, mut f: F, get_order: E) -> Result<Option<i64>>
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

// Do match until '%' is found.
#[inline]
fn partial_like(tcs: &mut Iter<u8>, pcs: &mut Iter<u8>, escape: u32) -> Option<bool> {
    loop {
        match pcs.next().cloned() {
            None => return Some(tcs.next().is_none()),
            Some(b'%') => return None,
            Some(c) => {
                let (npc, escape) = if u32::from(c) == escape {
                    pcs.next().map_or((c, false), |&c| (c, true))
                } else {
                    (c, false)
                };
                let nsc = match tcs.next() {
                    None => return Some(false),
                    Some(&c) => c,
                };
                if nsc != npc && (npc != b'_' || escape) {
                    return Some(false);
                }
            }
        }
    }
}

fn like(target: &[u8], pattern: &[u8], escape: u32, recurse_level: usize) -> Result<bool> {
    let mut tcs = target.iter();
    let mut pcs = pattern.iter();
    loop {
        if let Some(res) = partial_like(&mut tcs, &mut pcs, escape) {
            return Ok(res);
        }
        let next_char = loop {
            match pcs.next().cloned() {
                Some(b'%') => {}
                Some(b'_') => if tcs.next().is_none() {
                    return Ok(false);
                },
                // So the pattern should be some thing like 'xxx%'
                None => return Ok(true),
                Some(c) => {
                    break if u32::from(c) == escape {
                        pcs.next().map_or(escape, |&c| u32::from(c))
                    } else {
                        u32::from(c)
                    };
                }
            }
        };
        if recurse_level >= MAX_RECURSE_LEVEL {
            // TODO: maybe we should test if stack is actually about to overflow.
            return Err(box_err!(
                "recurse level should not be larger than {}",
                MAX_RECURSE_LEVEL
            ));
        }
        // Pattern must be something like "%xxx".
        loop {
            let s = match tcs.next() {
                None => return Ok(false),
                Some(&s) => u32::from(s),
            };
            if s == next_char && like(tcs.as_slice(), pcs.as_slice(), escape, recurse_level + 1)? {
                return Ok(true);
            }
        }
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use coprocessor::codec::Datum;
    use coprocessor::codec::mysql::{Decimal, Duration, Json, Time};
    use coprocessor::dag::expr::test::{col_expr, datum_expr, fncall_expr};
    use coprocessor::dag::expr::{EvalContext, Expression};
    use protobuf::RepeatedField;
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
            let e = Expression::build(&mut ctx, expr).unwrap();
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
            let e = Expression::build(&mut ctx, expr).unwrap();
            let res = e.eval(&mut ctx, &row).unwrap();
            assert_eq!(res, exp);
        }
    }

    #[test]
    fn test_like() {
        let cases = vec![
            (r#"hello"#, r#"%HELLO%"#, '\\', false),
            (r#"Hello, World"#, r#"Hello, World"#, '\\', true),
            (r#"Hello, World"#, r#"Hello, %"#, '\\', true),
            (r#"Hello, World"#, r#"%, World"#, '\\', true),
            (r#"test"#, r#"te%st"#, '\\', true),
            (r#"test"#, r#"te%%st"#, '\\', true),
            (r#"test"#, r#"test%"#, '\\', true),
            (r#"test"#, r#"%test%"#, '\\', true),
            (r#"test"#, r#"t%e%s%t"#, '\\', true),
            (r#"test"#, r#"_%_%_%_"#, '\\', true),
            (r#"test"#, r#"_%_%st"#, '\\', true),
            (r#"C:"#, r#"%\"#, '\\', false),
            (r#"C:\"#, r#"%\"#, '\\', true),
            (r#"C:\Programs"#, r#"%\"#, '\\', false),
            (r#"C:\Programs\"#, r#"%\"#, '\\', true),
            (r#"C:"#, r#"%\\"#, '\\', false),
            (r#"C:\"#, r#"%\\"#, '\\', true),
            (r#"C:\Programs"#, r#"%\\"#, '\\', false),
            (r#"C:\Programs\"#, r#"%\\"#, '\\', true),
            (r#"C:\Programs\"#, r#"%Prog%"#, '\\', true),
            (r#"C:\Programs\"#, r#"%Pr_g%"#, '\\', true),
            (r#"C:\Programs\"#, r#"%%\"#, '%', true),
            (r#"C:\Programs%"#, r#"%%%"#, '%', true),
            (r#"C:\Programs%"#, r#"%%%%"#, '%', true),
            (r#"hello"#, r#"\%"#, '\\', false),
            (r#"%"#, r#"\%"#, '\\', true),
            (r#"3hello"#, r#"%%hello"#, '%', true),
            (r#"3hello"#, r#"3%hello"#, '3', false),
            (r#"3hello"#, r#"__hello"#, '_', false),
            (r#"3hello"#, r#"%_hello"#, '%', true),
        ];
        let mut ctx = EvalContext::default();
        for (target_str, pattern_str, escape, exp) in cases {
            let target = datum_expr(Datum::Bytes(target_str.as_bytes().to_vec()));
            let pattern = datum_expr(Datum::Bytes(pattern_str.as_bytes().to_vec()));
            let escape = datum_expr(Datum::I64(escape as i64));
            let op = fncall_expr(ScalarFuncSig::LikeSig, &[target, pattern, escape]);
            let op = Expression::build(&mut ctx, op).unwrap();
            let got = op.eval(&mut ctx, &[]).unwrap();
            let exp = Datum::from(exp);
            assert_eq!(got, exp, "{:?} like {:?}", target_str, pattern_str);
        }
    }
}
