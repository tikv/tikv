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

mod arithmetic;
mod builtin_cast;
mod builtin_control;
mod builtin_op;
mod column;
mod compare;
mod constant;
mod ctx;
mod fncall;
mod json;
mod math;
mod time;

pub use self::ctx::*;
pub use coprocessor::codec::{Error, Result};

use coprocessor::codec::mysql::{Decimal, Duration, Json, Time, MAX_FSP};
use coprocessor::codec::mysql::{charset, types};
use coprocessor::codec::{self, Datum};
use std::borrow::Cow;
use std::str;
use tipb::expression::{Expr, ExprType, FieldType, ScalarFuncSig};
use util::codec::number;

#[derive(Debug, Clone, PartialEq)]
pub enum Expression {
    Constant(Constant),
    ColumnRef(Column),
    ScalarFn(FnCall),
}

#[derive(Debug, Clone, PartialEq)]
pub struct Column {
    offset: usize,
    tp: FieldType,
}

#[derive(Debug, Clone, PartialEq)]
pub struct Constant {
    val: Datum,
    tp: FieldType,
}

/// A single scalar function call
#[derive(Debug, Clone, PartialEq)]
pub struct FnCall {
    sig: ScalarFuncSig,
    children: Vec<Expression>,
    tp: FieldType,
}

impl Expression {
    fn new_const(v: Datum, field_type: FieldType) -> Expression {
        Expression::Constant(Constant {
            val: v,
            tp: field_type,
        })
    }

    #[inline]
    fn get_tp(&self) -> &FieldType {
        match *self {
            Expression::Constant(ref c) => &c.tp,
            Expression::ColumnRef(ref c) => &c.tp,
            Expression::ScalarFn(ref c) => &c.tp,
        }
    }

    #[cfg(test)]
    #[inline]
    fn mut_tp(&mut self) -> &mut FieldType {
        match *self {
            Expression::Constant(ref mut c) => &mut c.tp,
            Expression::ColumnRef(ref mut c) => &mut c.tp,
            Expression::ScalarFn(ref mut c) => &mut c.tp,
        }
    }

    #[allow(match_same_arms)]
    fn eval_int(&self, ctx: &mut EvalContext, row: &[Datum]) -> Result<Option<i64>> {
        match *self {
            Expression::Constant(ref constant) => constant.eval_int(),
            Expression::ColumnRef(ref column) => column.eval_int(row),
            Expression::ScalarFn(ref f) => f.eval_int(ctx, row),
        }
    }

    fn eval_real(&self, ctx: &mut EvalContext, row: &[Datum]) -> Result<Option<f64>> {
        match *self {
            Expression::Constant(ref constant) => constant.eval_real(),
            Expression::ColumnRef(ref column) => column.eval_real(row),
            Expression::ScalarFn(ref f) => f.eval_real(ctx, row),
        }
    }

    #[allow(match_same_arms)]
    fn eval_decimal<'a, 'b: 'a>(
        &'b self,
        ctx: &mut EvalContext,
        row: &'a [Datum],
    ) -> Result<Option<Cow<'a, Decimal>>> {
        match *self {
            Expression::Constant(ref constant) => constant.eval_decimal(),
            Expression::ColumnRef(ref column) => column.eval_decimal(row),
            Expression::ScalarFn(ref f) => f.eval_decimal(ctx, row),
        }
    }

    fn eval_string<'a, 'b: 'a>(
        &'b self,
        ctx: &mut EvalContext,
        row: &'a [Datum],
    ) -> Result<Option<Cow<'a, [u8]>>> {
        match *self {
            Expression::Constant(ref constant) => constant.eval_string(),
            Expression::ColumnRef(ref column) => column.eval_string(ctx, row),
            Expression::ScalarFn(ref f) => f.eval_bytes(ctx, row),
        }
    }

    fn eval_string_and_decode<'a, 'b: 'a>(
        &'b self,
        ctx: &mut EvalContext,
        row: &'a [Datum],
    ) -> Result<Option<Cow<'a, str>>> {
        let bytes = try_opt!(self.eval_string(ctx, row));
        let chrst = self.get_tp().get_charset();
        if charset::UTF8_CHARSETS.contains(&chrst) {
            let s = match bytes {
                Cow::Borrowed(bs) => str::from_utf8(bs).map_err(Error::from).map(Cow::Borrowed),
                Cow::Owned(bs) => String::from_utf8(bs).map_err(Error::from).map(Cow::Owned),
            };
            return s.map(Some);
        }
        Err(box_err!("unsupported charset: {}", chrst))
    }

    fn eval_time<'a, 'b: 'a>(
        &'b self,
        ctx: &mut EvalContext,
        row: &'a [Datum],
    ) -> Result<Option<Cow<'a, Time>>> {
        match *self {
            Expression::Constant(ref constant) => constant.eval_time(),
            Expression::ColumnRef(ref column) => column.eval_time(row),
            Expression::ScalarFn(ref f) => f.eval_time(ctx, row),
        }
    }

    fn eval_duration<'a, 'b: 'a>(
        &'b self,
        ctx: &mut EvalContext,
        row: &'a [Datum],
    ) -> Result<Option<Cow<'a, Duration>>> {
        match *self {
            Expression::Constant(ref constant) => constant.eval_duration(),
            Expression::ColumnRef(ref column) => column.eval_duration(row),
            Expression::ScalarFn(ref f) => f.eval_duration(ctx, row),
        }
    }

    fn eval_json<'a, 'b: 'a>(
        &'b self,
        ctx: &mut EvalContext,
        row: &'a [Datum],
    ) -> Result<Option<Cow<'a, Json>>> {
        match *self {
            Expression::Constant(ref constant) => constant.eval_json(),
            Expression::ColumnRef(ref column) => column.eval_json(row),
            Expression::ScalarFn(ref f) => f.eval_json(ctx, row),
        }
    }

    /// IsHybridType checks whether a ClassString expression is a hybrid type value which will
    /// return different types of value in different context.
    /// For ENUM/SET which is consist of a string attribute `Name` and an int attribute `Value`,
    /// it will cause an error if we convert ENUM/SET to int as a string value.
    /// For Bit/Hex, we will get a wrong result if we convert it to int as a string value.
    /// For example, when convert `0b101` to int, the result should be 5, but we will get
    /// 101 if we regard it as a string.
    pub fn is_hybrid_type(&self) -> bool {
        types::is_hybrid_type(self.get_tp().get_tp() as u8)
    }
}

impl Expression {
    pub fn eval(&self, ctx: &mut EvalContext, row: &[Datum]) -> Result<Datum> {
        match *self {
            Expression::Constant(ref constant) => Ok(constant.eval()),
            Expression::ColumnRef(ref column) => Ok(column.eval(row)),
            Expression::ScalarFn(ref f) => f.eval(ctx, row),
        }
    }

    pub fn batch_build(ctx: &mut EvalContext, exprs: Vec<Expr>) -> Result<Vec<Self>> {
        let mut data = Vec::with_capacity(exprs.len());
        for expr in exprs {
            let ex = Expression::build(ctx, expr)?;
            data.push(ex);
        }
        Ok(data)
    }

    pub fn build(ctx: &mut EvalContext, mut expr: Expr) -> Result<Self> {
        debug!("build expr:{:?}", expr);
        let tp = expr.take_field_type();
        match expr.get_tp() {
            ExprType::Null => Ok(Expression::new_const(Datum::Null, tp)),
            ExprType::Int64 => number::decode_i64(&mut expr.get_val())
                .map(Datum::I64)
                .map(|e| Expression::new_const(e, tp))
                .map_err(Error::from),
            ExprType::Uint64 => number::decode_u64(&mut expr.get_val())
                .map(Datum::U64)
                .map(|e| Expression::new_const(e, tp))
                .map_err(Error::from),
            ExprType::String | ExprType::Bytes => {
                Ok(Expression::new_const(Datum::Bytes(expr.take_val()), tp))
            }
            ExprType::Float32 | ExprType::Float64 => number::decode_f64(&mut expr.get_val())
                .map(Datum::F64)
                .map(|e| Expression::new_const(e, tp))
                .map_err(Error::from),
            ExprType::MysqlTime => number::decode_u64(&mut expr.get_val())
                .map_err(Error::from)
                .and_then(|i| {
                    let fsp = tp.get_decimal() as i8;
                    let t = tp.get_tp() as u8;
                    Time::from_packed_u64(i, t, fsp, &ctx.cfg.tz)
                })
                .map(|t| Expression::new_const(Datum::Time(t), tp)),
            ExprType::MysqlDuration => number::decode_i64(&mut expr.get_val())
                .map_err(Error::from)
                .and_then(|n| Duration::from_nanos(n, MAX_FSP))
                .map(Datum::Dur)
                .map(|e| Expression::new_const(e, tp)),
            ExprType::MysqlDecimal => Decimal::decode(&mut expr.get_val())
                .map(Datum::Dec)
                .map(|e| Expression::new_const(e, tp))
                .map_err(Error::from),
            ExprType::MysqlJson => Json::decode(&mut expr.get_val())
                .map(Datum::Json)
                .map(|e| Expression::new_const(e, tp))
                .map_err(Error::from),
            ExprType::ScalarFunc => {
                FnCall::check_args(expr.get_sig(), expr.get_children().len())?;
                expr.take_children()
                    .into_iter()
                    .map(|child| Expression::build(ctx, child))
                    .collect::<Result<Vec<_>>>()
                    .map(|children| {
                        Expression::ScalarFn(FnCall {
                            sig: expr.get_sig(),
                            children,
                            tp,
                        })
                    })
            }
            ExprType::ColumnRef => {
                let offset = number::decode_i64(&mut expr.get_val()).map_err(Error::from)? as usize;
                let column = Column { offset, tp };
                Ok(Expression::ColumnRef(column))
            }
            unhandled => Err(box_err!("can't handle {:?} expr in DAG mode", unhandled)),
        }
    }
}

#[inline]
pub fn eval_arith<F>(ctx: &mut EvalContext, left: Datum, right: Datum, f: F) -> Result<Datum>
where
    F: FnOnce(Datum, &mut EvalContext, Datum) -> codec::Result<Datum>,
{
    let left = left.into_arith(ctx)?;
    let right = right.into_arith(ctx)?;

    let (left, right) = Datum::coerce(left, right)?;
    if left == Datum::Null || right == Datum::Null {
        return Ok(Datum::Null);
    }

    f(left, ctx, right).map_err(From::from)
}

#[cfg(test)]
mod test {
    use super::{Error, EvalConfig, EvalContext, Expression, FLAG_IGNORE_TRUNCATE};
    use coprocessor::codec::error::{ERR_DATA_OUT_OF_RANGE, ERR_DIVISION_BY_ZERO};
    use coprocessor::codec::mysql::json::JsonEncoder;
    use coprocessor::codec::mysql::{charset, types, Decimal, DecimalEncoder, Duration, Json, Time};
    use coprocessor::codec::{convert, mysql, Datum};
    use std::sync::Arc;
    use std::{i64, u64};
    use tipb::expression::{Expr, ExprType, FieldType, ScalarFuncSig};
    use util::codec::number::{self, NumberEncoder};

    #[inline]
    pub fn str2dec(s: &str) -> Datum {
        Datum::Dec(s.parse().unwrap())
    }

    #[inline]
    pub fn make_null_datums(size: usize) -> Vec<Datum> {
        (0..size).map(|_| Datum::Null).collect()
    }

    #[inline]
    pub fn check_overflow(e: Error) -> Result<(), ()> {
        if e.code() == ERR_DATA_OUT_OF_RANGE {
            Ok(())
        } else {
            Err(())
        }
    }

    #[inline]
    pub fn check_divide_by_zero(e: Error) -> Result<(), ()> {
        if e.code() == ERR_DIVISION_BY_ZERO {
            Ok(())
        } else {
            Err(())
        }
    }

    pub fn fncall_expr(sig: ScalarFuncSig, children: &[Expr]) -> Expr {
        let mut expr = Expr::new();
        expr.set_tp(ExprType::ScalarFunc);
        expr.set_sig(sig);
        expr.set_field_type(FieldType::new());
        for child in children {
            expr.mut_children().push(child.clone());
        }
        expr
    }

    pub fn col_expr(col_id: i64) -> Expr {
        let mut expr = Expr::new();
        expr.set_tp(ExprType::ColumnRef);
        let mut buf = Vec::with_capacity(8);
        buf.encode_i64(col_id).unwrap();
        expr.set_val(buf);
        expr
    }

    pub fn datum_expr(datum: Datum) -> Expr {
        let mut expr = Expr::new();
        match datum {
            Datum::I64(i) => {
                expr.set_tp(ExprType::Int64);
                let mut buf = Vec::with_capacity(number::I64_SIZE);
                buf.encode_i64(i).unwrap();
                expr.set_val(buf);
            }
            Datum::U64(u) => {
                expr.set_tp(ExprType::Uint64);
                let mut buf = Vec::with_capacity(number::U64_SIZE);
                buf.encode_u64(u).unwrap();
                expr.set_val(buf);
                expr.mut_field_type().set_flag(types::UNSIGNED_FLAG as u32);
            }
            Datum::Bytes(bs) => {
                expr.set_tp(ExprType::Bytes);
                expr.set_val(bs);
                expr.mut_field_type()
                    .set_charset(charset::CHARSET_UTF8.to_owned());
            }
            Datum::F64(f) => {
                expr.set_tp(ExprType::Float64);
                let mut buf = Vec::with_capacity(number::F64_SIZE);
                buf.encode_f64(f).unwrap();
                expr.set_val(buf);
            }
            Datum::Dur(d) => {
                expr.set_tp(ExprType::MysqlDuration);
                let mut buf = Vec::with_capacity(number::I64_SIZE);
                buf.encode_i64(d.to_nanos()).unwrap();
                expr.set_val(buf);
            }
            Datum::Dec(d) => {
                expr.set_tp(ExprType::MysqlDecimal);
                let (prec, frac) = d.prec_and_frac();
                let mut buf = Vec::with_capacity(mysql::dec_encoded_len(&[prec, frac]).unwrap());
                buf.encode_decimal(&d, prec, frac).unwrap();
                expr.set_val(buf);
            }
            Datum::Time(t) => {
                expr.set_tp(ExprType::MysqlTime);
                let mut ft = FieldType::new();
                ft.set_tp(i32::from(t.get_tp()));
                ft.set_decimal(i32::from(t.get_fsp()));
                expr.set_field_type(ft);
                let u = t.to_packed_u64();
                let mut buf = Vec::with_capacity(number::U64_SIZE);
                buf.encode_u64(u).unwrap();
                expr.set_val(buf);
            }
            Datum::Json(j) => {
                expr.set_tp(ExprType::MysqlJson);
                let mut buf = Vec::new();
                buf.encode_json(&j).unwrap();
                expr.set_val(buf);
            }
            Datum::Null => expr.set_tp(ExprType::Null),
            d => panic!("unsupport datum: {:?}", d),
        };
        expr
    }

    #[test]
    fn test_expression_eval() {
        let mut ctx = EvalContext::new(Arc::new(EvalConfig::new(0, FLAG_IGNORE_TRUNCATE).unwrap()));
        let cases = vec![
            (
                ScalarFuncSig::CastStringAsReal,
                vec![Datum::Bytes(b"123".to_vec())],
                Datum::F64(123f64),
            ),
            (
                ScalarFuncSig::CastStringAsDecimal,
                vec![Datum::Bytes(b"123".to_vec())],
                Datum::Dec(Decimal::from(123)),
            ),
            (
                ScalarFuncSig::CastStringAsDuration,
                vec![Datum::Bytes(b"12:02:03".to_vec())],
                Datum::Dur(Duration::parse(b"12:02:03", 0).unwrap()),
            ),
            (
                ScalarFuncSig::CastStringAsTime,
                vec![Datum::Bytes(b"2012-12-12 14:00:05".to_vec())],
                Datum::Time(Time::parse_utc_datetime("2012-12-12 14:00:05", 0).unwrap()),
            ),
            (
                ScalarFuncSig::CastStringAsString,
                vec![Datum::Bytes(b"134".to_vec())],
                Datum::Bytes(b"134".to_vec()),
            ),
            (
                ScalarFuncSig::CastIntAsJson,
                vec![Datum::I64(12)],
                Datum::Json(Json::I64(12)),
            ),
        ];
        for (sig, cols, exp) in cases {
            let col_expr = col_expr(0);
            let mut ex = fncall_expr(sig, &[col_expr]);
            ex.mut_field_type()
                .set_decimal(convert::UNSPECIFIED_LENGTH as i32);
            ex.mut_field_type()
                .set_flen(convert::UNSPECIFIED_LENGTH as i32);
            let e = Expression::build(&mut ctx, ex).unwrap();
            let res = e.eval(&mut ctx, &cols).unwrap();
            if let Datum::F64(_) = exp {
                assert_eq!(format!("{}", res), format!("{}", exp));
            } else {
                assert_eq!(res, exp);
            }
        }
        // cases for integer
        let cases = vec![
            (
                Some(types::UNSIGNED_FLAG),
                vec![Datum::U64(u64::MAX)],
                Datum::U64(u64::MAX),
            ),
            (None, vec![Datum::I64(i64::MIN)], Datum::I64(i64::MIN)),
            (None, vec![Datum::Null], Datum::Null),
        ];
        for (flag, cols, exp) in cases {
            let col_expr = col_expr(0);
            let mut ex = fncall_expr(ScalarFuncSig::CastIntAsInt, &[col_expr]);
            if flag.is_some() {
                ex.mut_field_type().set_flag(flag.unwrap() as u32);
            }
            let e = Expression::build(&mut ctx, ex).unwrap();
            let res = e.eval(&mut ctx, &cols).unwrap();
            assert_eq!(res, exp);
        }
    }
}
