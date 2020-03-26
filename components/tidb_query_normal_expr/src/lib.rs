// Copyright 2020 TiKV Project Authors. Licensed under Apache-2.0.

//! This crate implements a simple SQL query engine to work with TiDB pushed down executors.
//!
//! The query engine is able to scan and understand rows stored by TiDB, run against a
//! series of executors and then return the execution result. The query engine is provided via
//! TiKV Coprocessor interface. However standalone UDF functions are also exported and can be used
//! standalone.

#![feature(proc_macro_hygiene)]
#![feature(test)]

#[macro_use]
extern crate failure;
#[macro_use(debug)]
extern crate slog_global;
#[macro_use(box_err, box_try, try_opt)]
extern crate tikv_util;

#[cfg(test)]
extern crate test;

use std::borrow::Cow;
use std::convert::TryInto;
use std::str;

use codec::prelude::NumberDecoder;
use tidb_query_datatype::prelude::*;
use tidb_query_datatype::FieldTypeFlag;
use tipb::{Expr, ExprType, FieldType, ScalarFuncSig};

use tidb_query_datatype::codec::mysql::charset;
use tidb_query_datatype::codec::mysql::{
    Decimal, DecimalDecoder, Duration, Json, JsonDecoder, Time, MAX_FSP,
};
use tidb_query_datatype::codec::Datum;
use tidb_query_datatype::expr::EvalContext;

mod builtin_arithmetic;
mod builtin_cast;
mod builtin_compare;
mod builtin_control;
mod builtin_encryption;
mod builtin_json;
mod builtin_like;
mod builtin_math;
mod builtin_miscellaneous;
mod builtin_op;
mod builtin_other;
mod builtin_string;
mod builtin_time;
mod column;
mod constant;
mod scalar_function;

pub use tidb_query_datatype::codec::{Error, Result};

#[derive(Debug)]
pub enum Expression {
    Constant(Constant),
    ColumnRef(Column),
    ScalarFn(ScalarFunc),
}

#[derive(Debug, Clone, PartialEq)]
pub struct Column {
    offset: usize,
    field_type: FieldType,
}

#[derive(Debug, Clone, PartialEq)]
pub struct Constant {
    val: Datum,
    field_type: FieldType,
}

/// A single scalar function call
#[derive(Debug)]
pub struct ScalarFunc {
    sig: ScalarFuncSig,
    children: Vec<Expression>,
    metadata: Option<Box<dyn protobuf::Message>>,
    field_type: FieldType,
}

impl Expression {
    fn new_const(val: Datum, field_type: FieldType) -> Expression {
        Expression::Constant(Constant { val, field_type })
    }

    #[inline]
    fn field_type(&self) -> &FieldType {
        match *self {
            Expression::Constant(ref c) => &c.field_type,
            Expression::ColumnRef(ref c) => &c.field_type,
            Expression::ScalarFn(ref c) => &c.field_type,
        }
    }

    #[cfg(test)]
    #[inline]
    fn mut_field_type(&mut self) -> &mut FieldType {
        match *self {
            Expression::Constant(ref mut c) => &mut c.field_type,
            Expression::ColumnRef(ref mut c) => &mut c.field_type,
            Expression::ScalarFn(ref mut c) => &mut c.field_type,
        }
    }

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
        let charset = self.field_type().get_charset();
        if charset::UTF8_CHARSETS.contains(&charset) {
            let s = match bytes {
                Cow::Borrowed(bs) => str::from_utf8(bs).map_err(Error::from).map(Cow::Borrowed),
                Cow::Owned(bs) => String::from_utf8(bs).map_err(Error::from).map(Cow::Owned),
            };
            return s.map(Some);
        }
        Err(box_err!("unsupported charset: {}", charset))
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
    ) -> Result<Option<Duration>> {
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

    #[inline]
    pub fn is_unsigned(&self) -> bool {
        self.field_type()
            .as_accessor()
            .flag()
            .contains(FieldTypeFlag::UNSIGNED)
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
        debug!(
            "build-expr";
            "expr" => ?expr
        );
        let field_type = expr.take_field_type();
        match expr.get_tp() {
            ExprType::Null => Ok(Expression::new_const(Datum::Null, field_type)),
            ExprType::Int64 => expr
                .get_val()
                .read_i64()
                .map(Datum::I64)
                .map(|e| Expression::new_const(e, field_type))
                .map_err(Error::from),
            ExprType::Uint64 => expr
                .get_val()
                .read_u64()
                .map(Datum::U64)
                .map(|e| Expression::new_const(e, field_type))
                .map_err(Error::from),
            ExprType::String | ExprType::Bytes => Ok(Expression::new_const(
                Datum::Bytes(expr.take_val()),
                field_type,
            )),
            ExprType::Float32 | ExprType::Float64 => expr
                .get_val()
                .read_f64()
                .map(Datum::F64)
                .map(|e| Expression::new_const(e, field_type))
                .map_err(Error::from),
            ExprType::MysqlTime => expr
                .get_val()
                .read_u64()
                .map_err(Error::from)
                .and_then(|i| {
                    let fsp = field_type.as_accessor().decimal() as i8;
                    Time::from_packed_u64(ctx, i, field_type.as_accessor().tp().try_into()?, fsp)
                })
                .map(|t| Expression::new_const(Datum::Time(t), field_type)),
            ExprType::MysqlDuration => expr
                .get_val()
                .read_i64()
                .map_err(Error::from)
                .and_then(|n| Duration::from_nanos(n, MAX_FSP))
                .map(Datum::Dur)
                .map(|e| Expression::new_const(e, field_type)),
            ExprType::MysqlDecimal => expr
                .get_val()
                .read_decimal()
                .map(Datum::Dec)
                .map(|e| Expression::new_const(e, field_type))
                .map_err(Error::from),
            ExprType::MysqlJson => expr
                .get_val()
                .read_json()
                .map(Datum::Json)
                .map(|e| Expression::new_const(e, field_type))
                .map_err(Error::from),
            ExprType::ScalarFunc => {
                ScalarFunc::check_args(expr.get_sig(), expr.get_children().len())?;
                expr.take_children()
                    .into_iter()
                    .map(|child| Expression::build(ctx, child))
                    .collect::<Result<Vec<_>>>()
                    .map(|children| {
                        Expression::ScalarFn(ScalarFunc {
                            sig: expr.get_sig(),
                            children,
                            field_type,
                            metadata: None,
                        })
                    })
            }
            ExprType::ColumnRef => {
                let offset = expr.get_val().read_i64().map_err(Error::from)? as usize;
                let column = Column { offset, field_type };
                Ok(Expression::ColumnRef(column))
            }
            unhandled => Err(box_err!("can't handle {:?} expr in DAG mode", unhandled)),
        }
    }
}

#[inline]
pub fn eval_arith<F>(ctx: &mut EvalContext, left: Datum, right: Datum, f: F) -> Result<Datum>
where
    F: FnOnce(Datum, &mut EvalContext, Datum) -> Result<Datum>,
{
    let left = left.into_arith(ctx)?;
    let right = right.into_arith(ctx)?;

    let (left, right) = Datum::coerce(ctx, left, right)?;
    if left == Datum::Null || right == Datum::Null {
        return Ok(Datum::Null);
    }

    f(left, ctx, right).map_err(From::from)
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::{i64, u64};

    use tidb_query_datatype::{self, Collation, FieldTypeAccessor, FieldTypeFlag, FieldTypeTp};
    use tipb::{Expr, ExprType, FieldType, ScalarFuncSig};

    use crate::Expression;
    use codec::{number, prelude::NumberEncoder};
    use tidb_query_datatype::codec::error::{ERR_DATA_OUT_OF_RANGE, ERR_DIVISION_BY_ZERO};
    use tidb_query_datatype::codec::mysql::json::JsonEncoder;
    use tidb_query_datatype::codec::mysql::{
        charset, Decimal, DecimalEncoder, Duration, Json, Time,
    };
    use tidb_query_datatype::codec::{mysql, Datum};
    use tidb_query_datatype::expr::{Error, EvalConfig, EvalContext};

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

    pub fn scalar_func_expr(sig: ScalarFuncSig, children: &[Expr]) -> Expr {
        let mut expr = Expr::default();
        expr.set_tp(ExprType::ScalarFunc);
        expr.set_sig(sig);
        expr.set_field_type(FieldType::default());
        for child in children {
            expr.mut_children().push(child.clone());
        }
        expr
    }

    pub fn col_expr(col_id: i64) -> Expr {
        let mut expr = Expr::default();
        expr.set_tp(ExprType::ColumnRef);
        let mut buf = Vec::with_capacity(8);
        buf.write_i64(col_id).unwrap();
        expr.set_val(buf);
        expr
    }

    pub fn string_datum_expr_with_tp(
        datum: Datum,
        tp: FieldTypeTp,
        flag: FieldTypeFlag,
        flen: isize,
        charset: String,
        collate: Collation,
    ) -> Expr {
        let mut expr = Expr::default();
        match datum {
            Datum::Bytes(bs) => {
                expr.set_tp(ExprType::Bytes);
                expr.set_val(bs);
                expr.mut_field_type()
                    .as_mut_accessor()
                    .set_tp(tp)
                    .set_flag(flag)
                    .set_flen(flen)
                    .set_collation(collate);
                expr.mut_field_type().set_charset(charset);
            }
            Datum::Null => expr.set_tp(ExprType::Null),
            d => panic!("unsupport datum: {}", d),
        }
        expr
    }

    pub fn datum_expr(datum: Datum) -> Expr {
        let mut expr = Expr::default();
        match datum {
            Datum::I64(i) => {
                expr.set_tp(ExprType::Int64);
                let mut buf = Vec::with_capacity(number::I64_SIZE);
                buf.write_i64(i).unwrap();
                expr.set_val(buf);
            }
            Datum::U64(u) => {
                expr.set_tp(ExprType::Uint64);
                let mut buf = Vec::with_capacity(number::U64_SIZE);
                buf.write_u64(u).unwrap();
                expr.set_val(buf);
                expr.mut_field_type()
                    .as_mut_accessor()
                    .set_flag(FieldTypeFlag::UNSIGNED);
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
                buf.write_f64(f).unwrap();
                expr.set_val(buf);
            }
            Datum::Dur(d) => {
                expr.set_tp(ExprType::MysqlDuration);
                let mut buf = Vec::with_capacity(number::I64_SIZE);
                buf.write_i64(d.to_nanos()).unwrap();
                expr.set_val(buf);
            }
            Datum::Dec(d) => {
                expr.set_tp(ExprType::MysqlDecimal);
                let (prec, frac) = d.prec_and_frac();
                let mut buf = Vec::with_capacity(mysql::dec_encoded_len(&[prec, frac]).unwrap());
                buf.write_decimal(&d, prec, frac).unwrap();
                expr.set_val(buf);
            }
            Datum::Time(t) => {
                let mut ctx = EvalContext::default();
                expr.set_tp(ExprType::MysqlTime);
                let mut ft = FieldType::default();
                ft.as_mut_accessor()
                    .set_tp(t.get_time_type().into())
                    .set_decimal(isize::from(t.fsp()));
                expr.set_field_type(ft);
                let u = t.to_packed_u64(&mut ctx).unwrap();
                let mut buf = Vec::with_capacity(number::U64_SIZE);
                buf.write_u64(u).unwrap();
                expr.set_val(buf);
            }
            Datum::Json(j) => {
                expr.set_tp(ExprType::MysqlJson);
                let mut buf = Vec::new();
                buf.write_json(j.as_ref()).unwrap();
                expr.set_val(buf);
            }
            Datum::Null => expr.set_tp(ExprType::Null),
            d => panic!("unsupport datum: {}", d),
        };
        expr
    }

    /// dispatch ScalarFuncSig with the args, return the result by calling eval.
    ///
    /// # Examples
    ///
    /// ```ignore
    /// let got = eval_func(ScalarFuncSig::TruncateInt, &[Datum::I64(1028), Datum::I64(-2)]).unwrap();
    /// assert_eq!(got, Datum::I64(1000));
    /// ```
    pub fn eval_func(sig: ScalarFuncSig, args: &[Datum]) -> super::Result<Datum> {
        eval_func_with(sig, args, |_, _| {})
    }

    /// dispatch ScalarFuncSig with the args, return the result by calling eval.
    /// f is used to setup the Expression before calling eval, like the set flag of the FieldType.
    ///
    /// # Examples
    ///
    /// ```ignore
    /// let x = Datum::U64(18446744073709551615);
    /// let d = Datum::I64(-2);
    /// let exp = Datum::U64(18446744073709551600);
    /// let got = eval_func_with(ScalarFuncSig::TruncateInt, &[x, d], |op, args| {
    ///     if mysql::has_unsigned_flag(args[0].get_field_type().get_flag()) {
    ///         op.mut_tp().set_flag(types::UNSIGNED_FLAG as u32);
    ///     }
    /// }).unwrap();
    /// assert_eq!(got, exp);
    /// ```
    pub fn eval_func_with<F: FnOnce(&mut Expression, &[Expr]) -> ()>(
        sig: ScalarFuncSig,
        args: &[Datum],
        f: F,
    ) -> super::Result<Datum> {
        let mut ctx = EvalContext::default();
        let args: Vec<Expr> = args.iter().map(|arg| datum_expr(arg.clone())).collect();
        let expr = scalar_func_expr(sig, &args);
        let mut op = Expression::build(&mut ctx, expr).unwrap();
        f(&mut op, &args);
        op.eval(&mut ctx, &[])
    }

    #[test]
    fn test_expression_eval() {
        let mut ctx = EvalContext::new(Arc::new(EvalConfig::default_for_test()));
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
                Datum::Dur(Duration::parse(&mut ctx, b"12:02:03", 0).unwrap()),
            ),
            (
                ScalarFuncSig::CastStringAsTime,
                vec![Datum::Bytes(b"2012-12-12 14:00:05".to_vec())],
                Datum::Time(
                    Time::parse_datetime(&mut ctx, "2012-12-12 14:00:05", 0, false).unwrap(),
                ),
            ),
            (
                ScalarFuncSig::CastStringAsString,
                vec![Datum::Bytes(b"134".to_vec())],
                Datum::Bytes(b"134".to_vec()),
            ),
            (
                ScalarFuncSig::CastIntAsJson,
                vec![Datum::I64(12)],
                Datum::Json(Json::from_i64(12).unwrap()),
            ),
        ];
        for (sig, cols, exp) in cases {
            let mut col_expr = col_expr(0);
            col_expr
                .mut_field_type()
                .set_charset(charset::CHARSET_UTF8.to_owned());
            let mut ex = scalar_func_expr(sig, &[col_expr]);
            ex.mut_field_type()
                .as_mut_accessor()
                .set_decimal(tidb_query_datatype::UNSPECIFIED_LENGTH)
                .set_flen(tidb_query_datatype::UNSPECIFIED_LENGTH);
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
                Some(FieldTypeFlag::UNSIGNED),
                vec![Datum::U64(u64::MAX)],
                Datum::U64(u64::MAX),
            ),
            (None, vec![Datum::I64(i64::MIN)], Datum::I64(i64::MIN)),
            (None, vec![Datum::Null], Datum::Null),
        ];
        for (flag, cols, exp) in cases {
            let col_expr = col_expr(0);
            let mut ex = scalar_func_expr(ScalarFuncSig::CastIntAsInt, &[col_expr]);
            if let Some(flag) = flag {
                ex.mut_field_type().as_mut_accessor().set_flag(flag);
            }
            let e = Expression::build(&mut ctx, ex).unwrap();
            let res = e.eval(&mut ctx, &cols).unwrap();
            assert_eq!(res, exp);
        }
    }
}
