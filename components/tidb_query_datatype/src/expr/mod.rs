// Copyright 2017 TiKV Project Authors. Licensed under Apache-2.0.

use crate::codec::Datum;

mod ctx;

pub use self::ctx::*;
pub use crate::codec::{Error, Result};

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

    use crate::{self, Collation, FieldTypeAccessor, FieldTypeFlag, FieldTypeTp};
    use tipb::{Expr, ExprType, FieldType, ScalarFuncSig};

    use super::{Error, EvalConfig, EvalContext, Expression};
    use crate::codec::error::{ERR_DATA_OUT_OF_RANGE, ERR_DIVISION_BY_ZERO};
    use crate::codec::mysql::json::JsonEncoder;
    use crate::codec::mysql::{charset, Decimal, DecimalEncoder, Duration, Json, Time};
    use crate::codec::{mysql, Datum};
    use codec::{number, prelude::NumberEncoder};

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
