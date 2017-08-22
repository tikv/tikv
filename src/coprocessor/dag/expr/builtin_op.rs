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

use super::{FnCall, Result, StatementContext};
use coprocessor::codec::Datum;

impl FnCall {
    pub fn logical_and(&self, ctx: &StatementContext, row: &[Datum]) -> Result<Option<i64>> {
        let arg0 = try!(self.children[0].eval_int(ctx, row));
        if arg0.map_or(false, |v| v == 0) {
            return Ok(Some(0));
        }
        let arg1 = try!(self.children[1].eval_int(ctx, row));
        if arg1.map_or(false, |v| v == 0) {
            return Ok(Some(0));
        }
        if arg0.is_none() || arg1.is_none() {
            return Ok(None);
        }
        Ok(Some(1))
    }

    pub fn logical_or(&self, ctx: &StatementContext, row: &[Datum]) -> Result<Option<i64>> {
        let arg0 = try!(self.children[0].eval_int(ctx, row));
        if arg0.map_or(false, |v| v != 0) {
            return Ok(Some(1));
        }
        let arg1 = try!(self.children[1].eval_int(ctx, row));
        match (arg0, arg1) {
            (None, None) => Ok(None),
            (None, Some(0)) | (Some(0), None) | (Some(0), Some(0)) => Ok(Some(0)),
            _ => Ok(Some(1)),
        }
    }

    pub fn logical_xor(&self, ctx: &StatementContext, row: &[Datum]) -> Result<Option<i64>> {
        let arg0 = try_opt!(self.children[0].eval_int(ctx, row));
        let arg1 = try_opt!(self.children[1].eval_int(ctx, row));
        Ok(Some(((arg0 == 0) ^ (arg1 == 0)) as i64))
    }

    pub fn real_is_true(&self, ctx: &StatementContext, row: &[Datum]) -> Result<Option<i64>> {
        let input = try!(self.children[0].eval_real(ctx, row));
        Ok(Some(input.map_or(0, |i| (i != 0f64) as i64)))
    }

    pub fn decimal_is_true(&self, ctx: &StatementContext, row: &[Datum]) -> Result<Option<i64>> {
        let input = try!(self.children[0].eval_decimal(ctx, row));
        Ok(Some(
            input.map_or(0, |dec| !dec.into_owned().is_zero() as i64),
        ))
    }

    pub fn int_is_false(&self, ctx: &StatementContext, row: &[Datum]) -> Result<Option<i64>> {
        let input = try!(self.children[0].eval_int(ctx, row));
        Ok(Some(input.map_or(0, |i| (i == 0) as i64)))
    }

    pub fn unary_not(&self, ctx: &StatementContext, row: &[Datum]) -> Result<Option<i64>> {
        let arg = try_opt!(self.children[0].eval_int(ctx, row));
        Ok(Some((arg == 0) as i64))
    }

    pub fn unary_minus_int(&self, ctx: &StatementContext, row: &[Datum]) -> Result<Option<i64>> {
        unimplemented!()
    }

    pub fn unary_minus_decimal(
        &self,
        ctx: &StatementContext,
        row: &[Datum],
    ) -> Result<Option<i64>> {
        unimplemented!()
    }

    pub fn unary_minus_real(&self, ctx: &StatementContext, row: &[Datum]) -> Result<Option<i64>> {
        unimplemented!()
    }

    pub fn decimal_is_null(&self, ctx: &StatementContext, row: &[Datum]) -> Result<Option<i64>> {
        let arg = try!(self.children[0].eval_decimal(ctx, row));
        Ok(Some(arg.is_none() as i64))
    }

    pub fn int_is_null(&self, ctx: &StatementContext, row: &[Datum]) -> Result<Option<i64>> {
        let arg = try!(self.children[0].eval_int(ctx, row));
        Ok(Some(arg.is_none() as i64))
    }

    pub fn real_is_null(&self, ctx: &StatementContext, row: &[Datum]) -> Result<Option<i64>> {
        let arg = try!(self.children[0].eval_real(ctx, row));
        Ok(Some(arg.is_none() as i64))
    }

    pub fn string_is_null(&self, ctx: &StatementContext, row: &[Datum]) -> Result<Option<i64>> {
        let arg = try!(self.children[0].eval_string(ctx, row));
        Ok(Some(arg.is_none() as i64))
    }

    pub fn time_is_null(&self, ctx: &StatementContext, row: &[Datum]) -> Result<Option<i64>> {
        let arg = try!(self.children[0].eval_time(ctx, row));
        Ok(Some(arg.is_none() as i64))
    }

    pub fn duration_is_null(&self, ctx: &StatementContext, row: &[Datum]) -> Result<Option<i64>> {
        let arg = try!(self.children[0].eval_duration(ctx, row));
        Ok(Some(arg.is_none() as i64))
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
    fn test_logic_op() {
        let tests = vec![
            (
                ScalarFuncSig::LogicalAnd,
                Datum::I64(1),
                Datum::I64(1),
                Datum::I64(1),
            ),
            (
                ScalarFuncSig::LogicalAnd,
                Datum::I64(1),
                Datum::I64(0),
                Datum::I64(0),
            ),
            (
                ScalarFuncSig::LogicalAnd,
                Datum::I64(0),
                Datum::I64(1),
                Datum::I64(0),
            ),
            (
                ScalarFuncSig::LogicalAnd,
                Datum::I64(0),
                Datum::I64(0),
                Datum::I64(0),
            ),
            (
                ScalarFuncSig::LogicalAnd,
                Datum::I64(2),
                Datum::I64(-1),
                Datum::I64(1),
            ),
            (
                ScalarFuncSig::LogicalAnd,
                Datum::I64(0),
                Datum::Null,
                Datum::I64(0),
            ),
            (
                ScalarFuncSig::LogicalAnd,
                Datum::Null,
                Datum::I64(0),
                Datum::I64(0),
            ),
            (
                ScalarFuncSig::LogicalAnd,
                Datum::Null,
                Datum::I64(1),
                Datum::Null,
            ),
            (
                ScalarFuncSig::LogicalOr,
                Datum::I64(1),
                Datum::I64(1),
                Datum::I64(1),
            ),
            (
                ScalarFuncSig::LogicalOr,
                Datum::I64(1),
                Datum::I64(0),
                Datum::I64(1),
            ),
            (
                ScalarFuncSig::LogicalOr,
                Datum::I64(0),
                Datum::I64(1),
                Datum::I64(1),
            ),
            (
                ScalarFuncSig::LogicalOr,
                Datum::I64(0),
                Datum::I64(0),
                Datum::I64(0),
            ),
            (
                ScalarFuncSig::LogicalOr,
                Datum::I64(2),
                Datum::I64(-1),
                Datum::I64(1),
            ),
            (
                ScalarFuncSig::LogicalOr,
                Datum::I64(1),
                Datum::Null,
                Datum::I64(1),
            ),
            (
                ScalarFuncSig::LogicalOr,
                Datum::Null,
                Datum::I64(1),
                Datum::I64(1),
            ),
            (
                ScalarFuncSig::LogicalOr,
                Datum::Null,
                Datum::I64(0),
                Datum::I64(0),
            ),
            (
                ScalarFuncSig::LogicalXor,
                Datum::I64(1),
                Datum::I64(1),
                Datum::I64(0),
            ),
            (
                ScalarFuncSig::LogicalXor,
                Datum::I64(1),
                Datum::I64(0),
                Datum::I64(1),
            ),
            (
                ScalarFuncSig::LogicalXor,
                Datum::I64(0),
                Datum::I64(1),
                Datum::I64(1),
            ),
            (
                ScalarFuncSig::LogicalXor,
                Datum::I64(0),
                Datum::I64(0),
                Datum::I64(0),
            ),
            (
                ScalarFuncSig::LogicalXor,
                Datum::I64(2),
                Datum::I64(-1),
                Datum::I64(0),
            ),
            (
                ScalarFuncSig::LogicalXor,
                Datum::I64(0),
                Datum::Null,
                Datum::Null,
            ),
            (
                ScalarFuncSig::LogicalXor,
                Datum::Null,
                Datum::I64(0),
                Datum::Null,
            ),
            (
                ScalarFuncSig::LogicalXor,
                Datum::Null,
                Datum::I64(1),
                Datum::Null,
            ),
        ];
        let ctx = StatementContext::default();
        for tt in tests {
            let arg1 = datum_expr(tt.1);
            let arg2 = datum_expr(tt.2);
            let expected = Expression::build(datum_expr(tt.3), 0).unwrap();
            let op = Expression::build(fncall_expr(tt.0, &[arg1, arg2]), 0).unwrap();
            let lhs = op.eval_int(&ctx, &[]).unwrap();
            let rhs = expected.eval_int(&ctx, &[]).unwrap();
            assert_eq!(lhs, rhs);
        }
    }

    #[test]
    fn test_unary_op() {
        let tests = vec![
            (ScalarFuncSig::UnaryNot, Datum::I64(1), Datum::I64(0)),
            (ScalarFuncSig::UnaryNot, Datum::I64(0), Datum::I64(1)),
            (ScalarFuncSig::UnaryNot, Datum::I64(123), Datum::I64(0)),
            (ScalarFuncSig::UnaryNot, Datum::I64(-123), Datum::I64(0)),
            (ScalarFuncSig::UnaryNot, Datum::Null, Datum::Null),
            (ScalarFuncSig::RealIsTrue, Datum::F64(0.25), Datum::I64(1)),
            (ScalarFuncSig::RealIsTrue, Datum::F64(0.0), Datum::I64(0)),
            (ScalarFuncSig::RealIsNull, Datum::F64(1.25), Datum::I64(0)),
            (ScalarFuncSig::RealIsNull, Datum::Null, Datum::I64(1)),
            (ScalarFuncSig::DecimalIsTrue, str2dec("1.1"), Datum::I64(1)),
            (ScalarFuncSig::DecimalIsTrue, str2dec("0"), Datum::I64(0)),
            (ScalarFuncSig::DecimalIsNull, str2dec("1.1"), Datum::I64(0)),
            (ScalarFuncSig::DecimalIsNull, Datum::Null, Datum::I64(1)),
            (ScalarFuncSig::IntIsFalse, Datum::I64(0), Datum::I64(1)),
            (ScalarFuncSig::IntIsFalse, Datum::I64(1), Datum::I64(0)),
            (ScalarFuncSig::IntIsNull, Datum::I64(1), Datum::I64(0)),
            (ScalarFuncSig::IntIsNull, Datum::Null, Datum::I64(1)),
            (ScalarFuncSig::StringIsNull, Datum::Null, Datum::I64(1)),
            (
                ScalarFuncSig::StringIsNull,
                Datum::Bytes(b"abc".to_vec()),
                Datum::I64(0),
            ),
            (ScalarFuncSig::TimeIsNull, Datum::Null, Datum::I64(1)),
            // TODO: add Time related tests after Time is implemented in Expression::build
            (
                ScalarFuncSig::DurationIsNull,
                Datum::Dur(Duration::zero()),
                Datum::I64(0),
            ),
            (ScalarFuncSig::DurationIsNull, Datum::Null, Datum::I64(1)),
        ];
        let ctx = StatementContext::default();
        for tt in tests {
            let arg1 = datum_expr(tt.1);
            let expected = Expression::build(datum_expr(tt.2), 0).unwrap();
            let op = Expression::build(fncall_expr(tt.0, &[arg1]), 0).unwrap();
            match tt.0 {
                ScalarFuncSig::UnaryNot |
                ScalarFuncSig::IntIsNull |
                ScalarFuncSig::IntIsFalse |
                ScalarFuncSig::RealIsTrue |
                ScalarFuncSig::RealIsNull |
                ScalarFuncSig::DecimalIsNull |
                ScalarFuncSig::DecimalIsTrue |
                ScalarFuncSig::StringIsNull |
                ScalarFuncSig::TimeIsNull |
                ScalarFuncSig::DurationIsNull => {
                    let lhs = op.eval_int(&ctx, &[]).unwrap();
                    let rhs = expected.eval_int(&ctx, &[]).unwrap();
                    assert_eq!(lhs, rhs);
                }
                _ => unreachable!(),
            }
        }
    }
}
