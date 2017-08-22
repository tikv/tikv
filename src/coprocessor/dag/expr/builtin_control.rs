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

impl FnCall {
    pub fn if_null_int(&self, ctx: &StatementContext, row: &[Datum]) -> Result<Option<i64>> {
        let arg0 = try!(self.children[0].eval_int(ctx, row));
        if !arg0.is_none() {
            return Ok(arg0);
        }
        let arg1 = try!(self.children[1].eval_int(ctx, row));
        Ok(arg1)
    }

    pub fn if_null_real(&self, ctx: &StatementContext, row: &[Datum]) -> Result<Option<f64>> {
        let arg0 = try!(self.children[0].eval_real(ctx, row));
        if !arg0.is_none() {
            return Ok(arg0);
        }
        let arg1 = try!(self.children[1].eval_real(ctx, row));
        Ok(arg1)
    }

    pub fn if_null_decimal<'a, 'b: 'a>(
        &'b self,
        ctx: &StatementContext,
        row: &'a [Datum],
    ) -> Result<Option<Cow<'a, Decimal>>> {
        let arg0 = try!(self.children[0].eval_decimal(ctx, row));
        if !arg0.is_none() {
            return Ok(arg0);
        }
        let arg1 = try!(self.children[1].eval_decimal(ctx, row));
        Ok(arg1)
    }

    pub fn if_null_string<'a, 'b: 'a>(
        &'b self,
        ctx: &StatementContext,
        row: &'a [Datum],
    ) -> Result<Option<Cow<'a, Vec<u8>>>> {
        let arg0 = try!(self.children[0].eval_string(ctx, row));
        if !arg0.is_none() {
            return Ok(arg0);
        }
        let arg1 = try!(self.children[1].eval_string(ctx, row));
        Ok(arg1)
    }

    pub fn if_null_time<'a, 'b: 'a>(
        &'b self,
        ctx: &StatementContext,
        row: &'a [Datum],
    ) -> Result<Option<Cow<'a, Time>>> {
        let arg0 = try!(self.children[0].eval_time(ctx, row));
        if !arg0.is_none() {
            return Ok(arg0);
        }
        let arg1 = try!(self.children[1].eval_time(ctx, row));
        Ok(arg1)
    }

    pub fn if_null_duration<'a, 'b: 'a>(
        &'b self,
        ctx: &StatementContext,
        row: &'a [Datum],
    ) -> Result<Option<Cow<'a, Duration>>> {
        let arg0 = try!(self.children[0].eval_duration(ctx, row));
        if !arg0.is_none() {
            return Ok(arg0);
        }
        let arg1 = try!(self.children[1].eval_duration(ctx, row));
        Ok(arg1)
    }

    pub fn if_int(&self, ctx: &StatementContext, row: &[Datum]) -> Result<Option<i64>> {
        let arg0 = try!(self.children[0].eval_int(ctx, row));
        let arg1 = try!(self.children[1].eval_int(ctx, row));
        let arg2 = try!(self.children[2].eval_int(ctx, row));
        match arg0 {
            None | Some(0) => Ok(arg2),
            _ => Ok(arg1),
        }
    }

    pub fn if_real(&self, ctx: &StatementContext, row: &[Datum]) -> Result<Option<f64>> {
        let arg0 = try!(self.children[0].eval_int(ctx, row));
        let arg1 = try!(self.children[1].eval_real(ctx, row));
        let arg2 = try!(self.children[2].eval_real(ctx, row));
        match arg0 {
            None | Some(0) => Ok(arg2),
            _ => Ok(arg1),
        }
    }

    pub fn if_decimal<'a, 'b: 'a>(
        &'b self,
        ctx: &StatementContext,
        row: &'a [Datum],
    ) -> Result<Option<Cow<'a, Decimal>>> {
        let arg0 = try!(self.children[0].eval_int(ctx, row));
        let arg1 = try!(self.children[1].eval_decimal(ctx, row));
        let arg2 = try!(self.children[2].eval_decimal(ctx, row));
        match arg0 {
            None | Some(0) => Ok(arg2),
            _ => Ok(arg1),
        }
    }

    pub fn if_string<'a, 'b: 'a>(
        &'b self,
        ctx: &StatementContext,
        row: &'a [Datum],
    ) -> Result<Option<Cow<'a, Vec<u8>>>> {
        let arg0 = try!(self.children[0].eval_int(ctx, row));
        let arg1 = try!(self.children[1].eval_string(ctx, row));
        let arg2 = try!(self.children[2].eval_string(ctx, row));
        match arg0 {
            None | Some(0) => Ok(arg2),
            _ => Ok(arg1),
        }
    }

    pub fn if_time<'a, 'b: 'a>(
        &'b self,
        ctx: &StatementContext,
        row: &'a [Datum],
    ) -> Result<Option<Cow<'a, Time>>> {
        let arg0 = try!(self.children[0].eval_int(ctx, row));
        let arg1 = try!(self.children[1].eval_time(ctx, row));
        let arg2 = try!(self.children[2].eval_time(ctx, row));
        match arg0 {
            None | Some(0) => Ok(arg2),
            _ => Ok(arg1),
        }
    }

    pub fn if_duration<'a, 'b: 'a>(
        &'b self,
        ctx: &StatementContext,
        row: &'a [Datum],
    ) -> Result<Option<Cow<'a, Duration>>> {
        let arg0 = try!(self.children[0].eval_int(ctx, row));
        let arg1 = try!(self.children[1].eval_duration(ctx, row));
        let arg2 = try!(self.children[2].eval_duration(ctx, row));
        match arg0 {
            None | Some(0) => Ok(arg2),
            _ => Ok(arg1),
        }
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
            let expected = Expression::build(datum_expr(exp), 0).unwrap();
            let op = Expression::build(fncall_expr(operator, &[arg1, arg2]), 0).unwrap();
            match operator {
                ScalarFuncSig::IfNullInt => {
                    let lhs = op.eval_int(&ctx, &[]).unwrap();
                    let rhs = expected.eval_int(&ctx, &[]).unwrap();
                    assert_eq!(lhs, rhs);
                }
                ScalarFuncSig::IfNullReal => {
                    let lhs = op.eval_real(&ctx, &[]).unwrap();
                    let rhs = expected.eval_real(&ctx, &[]).unwrap();
                    assert_eq!(lhs, rhs);
                }
                ScalarFuncSig::IfNullString => {
                    let lhs = op.eval_string(&ctx, &[]).unwrap();
                    let rhs = expected.eval_string(&ctx, &[]).unwrap();
                    assert_eq!(lhs, rhs);
                }
                ScalarFuncSig::IfNullDecimal => {
                    let lhs = op.eval_decimal(&ctx, &[]).unwrap();
                    let rhs = expected.eval_decimal(&ctx, &[]).unwrap();
                    assert_eq!(lhs, rhs);
                }
                ScalarFuncSig::IfNullTime => {
                    let lhs = op.eval_time(&ctx, &[]).unwrap();
                    let rhs = expected.eval_time(&ctx, &[]).unwrap();
                    assert_eq!(lhs, rhs);
                }
                ScalarFuncSig::IfNullDuration => {
                    let lhs = op.eval_duration(&ctx, &[]).unwrap();
                    let rhs = expected.eval_duration(&ctx, &[]).unwrap();
                    assert_eq!(lhs, rhs);
                }
                _ => unreachable!(),
            }
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
            let expected = Expression::build(datum_expr(exp), 0).unwrap();
            let op = Expression::build(fncall_expr(operator, &[arg1, arg2, arg3]), 0).unwrap();
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
