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

mod column;
mod constant;
mod fncall;
mod builtin_cast;
mod builtin_control;
mod builtin_op;
mod compare;
mod arithmetic;
mod math;
mod json;

use std::{error, io, str};
use std::borrow::Cow;
use std::string::FromUtf8Error;
use std::str::Utf8Error;

use tipb::expression::{Expr, ExprType, FieldType, ScalarFuncSig};

use coprocessor::codec::mysql::{Decimal, Duration, Json, Res, Time, MAX_FSP};
use coprocessor::codec::mysql::decimal::DecimalDecoder;
use coprocessor::codec::mysql::json::JsonDecoder;
use coprocessor::codec::mysql::{charset, types};
use coprocessor::codec::Datum;
use util;
use util::codec::number::NumberDecoder;
use util::codec::Error as CError;

pub use coprocessor::select::xeval::EvalContext as StatementContext;

quick_error! {
    #[derive(Debug)]
    pub enum Error {
        Io(err: io::Error) {
            from()
            description("io error")
            display("I/O error: {}", err)
            cause(err)
        }
        Type { has: &'static str, expected: &'static str } {
            description("type error")
            display("type error: cannot get {:?} result from {:?} expression", expected, has)
        }
        Codec(err: util::codec::Error) {
            from()
            description("codec error")
            display("codec error: {}", err)
            cause(err)
        }
        ColumnOffset(offset: usize) {
            description("column offset not found")
            display("illegal column offset: {}", offset)
        }
        UnknownSignature(sig: ScalarFuncSig) {
            description("Unknown signature")
            display("Unknown signature: {:?}", sig)
        }
        Truncated {
            description("Truncated")
            display("error Truncated")
        }
        Overflow {
            description("Overflow")
            display("error Overflow")
        }
        Other(err: Box<error::Error + Send + Sync>) {
            from()
            cause(err.as_ref())
            description(err.description())
            display("unknown error {:?}", err)
        }
    }
}

impl From<FromUtf8Error> for Error {
    fn from(err: FromUtf8Error) -> Error {
        Error::Codec(CError::Encoding(err.utf8_error().into()))
    }
}
impl From<Utf8Error> for Error {
    fn from(err: Utf8Error) -> Error {
        Error::Codec(CError::Encoding(err.into()))
    }
}

pub type Result<T> = ::std::result::Result<T, Error>;

impl<T> Into<Result<T>> for Res<T> {
    fn into(self) -> Result<T> {
        match self {
            Res::Ok(t) => Ok(t),
            Res::Truncated(_) => Err(Error::Truncated),
            Res::Overflow(_) => Err(Error::Overflow),
        }
    }
}

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
    fn eval_int(&self, ctx: &StatementContext, row: &[Datum]) -> Result<Option<i64>> {
        match *self {
            Expression::Constant(ref constant) => constant.eval_int(),
            Expression::ColumnRef(ref column) => column.eval_int(row),
            Expression::ScalarFn(ref f) => f.eval_int(ctx, row),
        }
    }

    fn eval_real(&self, ctx: &StatementContext, row: &[Datum]) -> Result<Option<f64>> {
        match *self {
            Expression::Constant(ref constant) => constant.eval_real(),
            Expression::ColumnRef(ref column) => column.eval_real(row),
            Expression::ScalarFn(ref f) => f.eval_real(ctx, row),
        }
    }

    #[allow(match_same_arms)]
    fn eval_decimal<'a, 'b: 'a>(
        &'b self,
        ctx: &StatementContext,
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
        ctx: &StatementContext,
        row: &'a [Datum],
    ) -> Result<Option<Cow<'a, [u8]>>> {
        match *self {
            Expression::Constant(ref constant) => constant.eval_string(),
            Expression::ColumnRef(ref column) => column.eval_string(row),
            Expression::ScalarFn(ref f) => f.eval_bytes(ctx, row),
        }
    }

    fn eval_string_and_decode<'a, 'b: 'a>(
        &'b self,
        ctx: &StatementContext,
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
        Err(box_err!("unsupported codec"))
    }

    fn eval_time<'a, 'b: 'a>(
        &'b self,
        ctx: &StatementContext,
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
        ctx: &StatementContext,
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
        ctx: &StatementContext,
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
    fn is_hybrid_type(&self) -> bool {
        match self.get_tp().get_tp() as u8 {
            types::ENUM | types::BIT | types::SET => {
                return true;
            }
            _ => {}
        }
        // TODO:For a constant, the field type will be inferred as `VARCHAR`
        // when the kind of it is `HEX` or `BIT`.
        false
    }
}

impl Expression {
    pub fn eval(&self, ctx: &StatementContext, row: &[Datum]) -> Result<Datum> {
        match *self {
            Expression::Constant(ref constant) => Ok(constant.eval()),
            Expression::ColumnRef(ref column) => Ok(column.eval(row)),
            Expression::ScalarFn(ref f) => f.eval(ctx, row),
        }
    }

    pub fn batch_build(ctx: &StatementContext, exprs: Vec<Expr>) -> Result<Vec<Self>> {
        let mut data = Vec::with_capacity(exprs.len());
        for expr in exprs {
            let ex = try!(Expression::build(ctx, expr));
            data.push(ex);
        }
        Ok(data)
    }

    pub fn build(ctx: &StatementContext, mut expr: Expr) -> Result<Self> {
        let tp = expr.take_field_type();
        match expr.get_tp() {
            ExprType::Null => Ok(Expression::new_const(Datum::Null, tp)),
            ExprType::Int64 => expr.get_val()
                .decode_i64()
                .map(Datum::I64)
                .map(|e| Expression::new_const(e, tp))
                .map_err(Error::from),
            ExprType::Uint64 => expr.get_val()
                .decode_u64()
                .map(Datum::U64)
                .map(|e| Expression::new_const(e, tp))
                .map_err(Error::from),
            ExprType::String | ExprType::Bytes => {
                Ok(Expression::new_const(Datum::Bytes(expr.take_val()), tp))
            }
            ExprType::Float32 | ExprType::Float64 => expr.get_val()
                .decode_f64()
                .map(Datum::F64)
                .map(|e| Expression::new_const(e, tp))
                .map_err(Error::from),
            ExprType::MysqlTime => expr.get_val()
                .decode_u64()
                .and_then(|i| {
                    let fsp = tp.get_decimal() as i8;
                    let t = tp.get_tp() as u8;
                    Time::from_packed_u64(i, t, fsp, &ctx.tz)
                })
                .map(|t| Expression::new_const(Datum::Time(t), tp))
                .map_err(Error::from),
            ExprType::MysqlDuration => expr.get_val()
                .decode_i64()
                .and_then(|n| Duration::from_nanos(n, MAX_FSP))
                .map(Datum::Dur)
                .map(|e| Expression::new_const(e, tp))
                .map_err(Error::from),
            ExprType::MysqlDecimal => expr.get_val()
                .decode_decimal()
                .map(Datum::Dec)
                .map(|e| Expression::new_const(e, tp))
                .map_err(Error::from),
            ExprType::MysqlJson => expr.get_val()
                .decode_json()
                .map(Datum::Json)
                .map(|e| Expression::new_const(e, tp))
                .map_err(Error::from),
            ExprType::ScalarFunc => {
                try!(FnCall::check_args(
                    expr.get_sig(),
                    expr.get_children().len()
                ));
                expr.take_children()
                    .into_iter()
                    .map(|child| Expression::build(ctx, child))
                    .collect::<Result<Vec<_>>>()
                    .map(|children| {
                        Expression::ScalarFn(FnCall {
                            sig: expr.get_sig(),
                            children: children,
                            tp: tp,
                        })
                    })
            }
            ExprType::ColumnRef => {
                let offset = try!(expr.get_val().decode_i64().map_err(Error::from)) as usize;
                let column = Column {
                    offset: offset,
                    tp: tp,
                };
                Ok(Expression::ColumnRef(column))
            }
            unhandled => unreachable!("can't handle {:?} expr in DAG mode", unhandled),
        }
    }
}

#[cfg(test)]
mod test {
    use std::{i64, u64};
    use coprocessor::codec::{convert, Datum};
    use coprocessor::codec::mysql::{types, Decimal, Duration, Json, Time};
    use coprocessor::select::xeval::evaluator::test::col_expr;
    use tipb::expression::{Expr, ExprType, FieldType, ScalarFuncSig};
    use super::{Error, Expression, StatementContext};

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
        match e {
            Error::Overflow => Ok(()),
            _ => Err(()),
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

    #[test]
    fn test_expression_eval() {
        let mut ctx = StatementContext::default();
        ctx.ignore_truncate = true;
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
            let e = Expression::build(&ctx, ex).unwrap();
            let res = e.eval(&ctx, &cols).unwrap();
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
            let e = Expression::build(&ctx, ex).unwrap();
            let res = e.eval(&ctx, &cols).unwrap();
            assert_eq!(res, exp);
        }
    }
}
