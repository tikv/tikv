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

use std::{char, str, i64};
use std::str::Chars;
use std::cmp::Ordering;
use std::borrow::Cow;

use coprocessor::codec::{datum, mysql, Datum};
use coprocessor::codec::mysql::{Decimal, Duration, Json, Time};
use coprocessor::dag::expr::Expression;
use super::{Error, FnCall, Result, StatementContext};

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
        ctx: &StatementContext,
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
        ctx: &StatementContext,
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
        ctx: &StatementContext,
        row: &[Datum],
        op: CmpOp,
    ) -> Result<Option<i64>> {
        let e = |i: usize| self.children[i].eval_decimal(ctx, row);
        do_compare(e, op, |l, r| Ok(l.cmp(&r)))
    }

    pub fn compare_string(
        &self,
        ctx: &StatementContext,
        row: &[Datum],
        op: CmpOp,
    ) -> Result<Option<i64>> {
        let e = |i: usize| self.children[i].eval_string(ctx, row);
        do_compare(e, op, |l, r| Ok(l.cmp(&r)))
    }

    pub fn compare_time(
        &self,
        ctx: &StatementContext,
        row: &[Datum],
        op: CmpOp,
    ) -> Result<Option<i64>> {
        let e = |i: usize| self.children[i].eval_time(ctx, row);
        do_compare(e, op, |l, r| Ok(l.cmp(&r)))
    }

    pub fn compare_duration(
        &self,
        ctx: &StatementContext,
        row: &[Datum],
        op: CmpOp,
    ) -> Result<Option<i64>> {
        let e = |i: usize| self.children[i].eval_duration(ctx, row);
        do_compare(e, op, |l, r| Ok(l.cmp(&r)))
    }

    pub fn compare_json(
        &self,
        ctx: &StatementContext,
        row: &[Datum],
        op: CmpOp,
    ) -> Result<Option<i64>> {
        let e = |i: usize| self.children[i].eval_json(ctx, row);
        do_compare(e, op, |l, r| Ok(l.cmp(&r)))
    }

    /// See http://dev.mysql.com/doc/refman/5.7/en/comparison-operators.html#function_coalesce
    pub fn coalesce_int(&self, ctx: &StatementContext, row: &[Datum]) -> Result<Option<i64>> {
        do_coalesce(self, |v| v.eval_int(ctx, row))
    }

    pub fn coalesce_real(&self, ctx: &StatementContext, row: &[Datum]) -> Result<Option<f64>> {
        do_coalesce(self, |v| v.eval_real(ctx, row))
    }

    pub fn coalesce_decimal<'a, 'b: 'a>(
        &'b self,
        ctx: &StatementContext,
        row: &'a [Datum],
    ) -> Result<Option<Cow<'a, Decimal>>> {
        do_coalesce(self, |v| v.eval_decimal(ctx, row))
    }

    pub fn coalesce_time<'a, 'b: 'a>(
        &'b self,
        ctx: &StatementContext,
        row: &'a [Datum],
    ) -> Result<Option<Cow<'a, Time>>> {
        do_coalesce(self, |v| v.eval_time(ctx, row))
    }

    pub fn coalesce_duration<'a, 'b: 'a>(
        &'b self,
        ctx: &StatementContext,
        row: &'a [Datum],
    ) -> Result<Option<Cow<'a, Duration>>> {
        do_coalesce(self, |v| v.eval_duration(ctx, row))
    }

    pub fn coalesce_string<'a, 'b: 'a>(
        &'b self,
        ctx: &StatementContext,
        row: &'a [Datum],
    ) -> Result<Option<Cow<'a, [u8]>>> {
        do_coalesce(self, |v| v.eval_string(ctx, row))
    }

    pub fn coalesce_json<'a, 'b: 'a>(
        &'b self,
        ctx: &StatementContext,
        row: &'a [Datum],
    ) -> Result<Option<Cow<'a, Json>>> {
        do_coalesce(self, |v| v.eval_json(ctx, row))
    }

    #[inline]
    pub fn like(&self, ctx: &StatementContext, row: &[Datum]) -> Result<Option<i64>> {
        let target = try_opt!(self.children[0].eval_string_and_decode(ctx, row));
        let pattern = try_opt!(self.children[1].eval_string_and_decode(ctx, row));
        let escape = {
            let c = try_opt!(self.children[2].eval_int(ctx, row)) as u32;
            char::from_u32(c)
                .ok_or::<Error>(box_err!("invalid escape char: {}", c))?
        };
        Ok(Some(like(&target, &pattern, escape, 0)? as i64))
    }
}

fn do_compare<T, E, F>(e: E, op: CmpOp, get_order: F) -> Result<Option<i64>>
where
    E: Fn(usize) -> Result<Option<T>>,
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

fn do_coalesce<'a, F, T>(expr: &'a FnCall, f: F) -> Result<Option<T>>
where
    F: Fn(&'a Expression) -> Result<Option<T>>,
{
    for exp in &expr.children {
        let v = f(exp)?;
        if v.is_some() {
            return Ok(v);
        }
    }
    Ok(None)
}

// Do match until '%' is found.
#[inline]
fn partial_like<'a>(tcs: &mut Chars<'a>, pcs: &mut Chars<'a>, escape: char) -> Option<bool> {
    loop {
        match pcs.next() {
            None => return Some(tcs.next().is_none()),
            Some('%') => return None,
            Some(c) => {
                let (npc, escape) = if c == escape {
                    pcs.next().map_or((c, false), |c| (c, true))
                } else {
                    (c, false)
                };
                let nsc = match tcs.next() {
                    None => return Some(false),
                    Some(c) => c,
                };
                if nsc != npc && (npc != '_' || escape) {
                    return Some(false);
                }
            }
        }
    }
}

fn like(target: &str, pattern: &str, escape: char, recurse_level: usize) -> Result<bool> {
    let mut tcs = target.chars();
    let mut pcs = pattern.chars();
    loop {
        if let Some(res) = partial_like(&mut tcs, &mut pcs, escape) {
            return Ok(res);
        }
        let next_char = loop {
            match pcs.next() {
                Some('%') => {}
                Some('_') => if tcs.next().is_none() {
                    return Ok(false);
                },
                // So the pattern should be some thing like 'xxx%'
                None => return Ok(true),
                Some(c) => {
                    break if c == escape {
                        pcs.next().unwrap_or(escape)
                    } else {
                        c
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
                Some(s) => s,
            };
            if s == next_char && like(tcs.as_str(), pcs.as_str(), escape, recurse_level + 1)? {
                return Ok(true);
            }
        }
    }
}

#[cfg(test)]
mod test {
    use std::{i64, u64};
    use tipb::expression::{Expr, ExprType, ScalarFuncSig};
    use protobuf::RepeatedField;
    use coprocessor::select::xeval::evaluator::test::{col_expr, datum_expr};
    use coprocessor::codec::mysql::{Decimal, Duration, Json, Time};
    use coprocessor::codec::Datum;
    use coprocessor::dag::expr::{Expression, StatementContext};
    use coprocessor::dag::expr::test::fncall_expr;
    use super::*;

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

        let ctx = StatementContext::default();

        for (sig, row, exp) in cases {
            let children: Vec<Expr> = (0..row.len()).map(|id| col_expr(id as i64)).collect();
            let mut expr = Expr::new();
            expr.set_tp(ExprType::ScalarFunc);
            expr.set_sig(sig);

            expr.set_children(RepeatedField::from_vec(children));
            let e = Expression::build(&ctx, expr).unwrap();
            let res = e.eval(&ctx, &row).unwrap();
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
        let ctx = StatementContext::default();
        for (target_str, pattern_str, escape, exp) in cases {
            let target = datum_expr(Datum::Bytes(target_str.as_bytes().to_vec()));
            let pattern = datum_expr(Datum::Bytes(pattern_str.as_bytes().to_vec()));
            let escape = datum_expr(Datum::I64(escape as i64));
            let op = fncall_expr(ScalarFuncSig::LikeSig, &[target, pattern, escape]);
            let op = Expression::build(&ctx, op).unwrap();
            let got = op.eval(&ctx, &[]).unwrap();
            let exp = Datum::from(exp);
            assert_eq!(got, exp, "{:?} like {:?}", target_str, pattern_str);
        }
    }
}
