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

use std::io;
use std::convert::TryFrom;

use super::super::codec::mysql::{Duration, Time, Decimal};
use super::super::codec::Datum;
use super::super::super::util;

use chrono::FixedOffset;

use tipb::expression::{ExprType, ScalarFuncSig};
use tipb::expression::Expr;
use tipb::select::DAGRequest;

use util::codec::number::{NumberDecoder, NumberEncoder};
use coprocessor::codec::mysql::decimal::DecimalDecoder;
use coprocessor::codec::mysql::MAX_FSP;

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
        Other(desc: &'static str) {
            description(desc)
            display("error {}", desc)
        }
    }
}

pub type Result<T> = ::std::result::Result<T, Error>;

/// Flags are used by `SelectRequest.flags` to handle execution mode, like how to handle
/// truncate error.
/// `FLAG_IGNORE_TRUNCATE` indicates if truncate error should be ignored.
/// Read-only statements should ignore truncate error, write statements should not ignore
/// truncate error.
pub const FLAG_IGNORE_TRUNCATE: u64 = 1;
/// `FLAG_TRUNCATE_AS_WARNING` indicates if truncate error should be returned as warning.
/// This flag only matters if `FLAG_IGNORE_TRUNCATE` is not set, in strict sql mode, truncate error
/// should be returned as error, in non-strict sql mode, truncate error should be saved as warning.
pub const FLAG_TRUNCATE_AS_WARNING: u64 = 1 << 1;

/// StatementContext contains variables for a statement
/// It should be reset before executing a statement
pub struct StatementContext {
    ignore_truncate: bool,
    truncate_as_warning: bool,
    timezone: FixedOffset,
}

/// Expression represents all scalar expression in SQL.
#[derive(Debug, Clone, PartialEq)]
pub enum Expression {
    Constant(Datum),
    ColumnRef(usize),
    ScalarFn(FnCall),
}

#[derive(Debug, Clone, PartialEq)]
pub struct FnCall {
    sig: ScalarFuncSig,
    children: Vec<Expression>,
}

impl Datum {
    fn to_i64(&self) -> Option<i64> {
        match *self {
            Datum::Null => None,
            _ => Some(self.i64()),
        }
    }

    fn to_f64(&self) -> Option<f64> {
        match *self {
            Datum::Null => None,
            _ => Some(self.f64()),
        }
    }
}

impl Expression {
    fn eval_int(&self, row: &[Datum], ctx: &StatementContext) -> Result<Option<i64>> {
        match *self {
            Expression::Constant(ref datum) => Ok(datum.to_i64()),
            Expression::ColumnRef(offset) => row.get(offset).map(Datum::to_i64).ok_or_else(|| Error::Other("offset not fount")),
            Expression::ScalarFn(ref fun) => {
                match fun.sig {
                    ScalarFuncSig::CastIntAsInt => fun.children[0].eval_int(row, ctx),
                    ScalarFuncSig::CastRealAsInt =>
                        fun.children[0].eval_real(row, ctx).map(|v| v.map(|v| v as i64)),
                    //
                    ScalarFuncSig::LTInt => {
                        let lhs = try!(fun.children[0].eval_int(row, ctx));
                        let rhs = try!(fun.children[1].eval_int(row, ctx));
                        match (lhs, rhs) {
                            (None, _) => Ok(None),
                            (_, None) => Ok(None),
                            (Some(l), Some(r)) => Ok(Some((l < r) as i64))
                        }
                    },
                    _ => unimplemented!()
                }
            }
        }
    }

    fn eval_real(&self, row: &[Datum], ctx: &StatementContext) -> Result<Option<f64>> {
        unimplemented!()
    }
    fn eval_decimal(&self, row: &[Datum], ctx: &StatementContext) -> Result<Option<Decimal>> {
        unimplemented!()
    }
    fn eval_string(&self, row: &[Datum], ctx: &StatementContext) -> Result<Option<String>> {
        unimplemented!()
    }
    fn eval_time(&self, row: &[Datum], ctx: &StatementContext) -> Result<Option<Time>> {
        unimplemented!()
    }
    fn eval_duration(&self, row: &[Datum], ctx: &StatementContext) -> Result<Option<Duration>> {
        unimplemented!()
    }
}

impl TryFrom<Expr> for Expression {
    type Error = Error;

    fn try_from(expr: Expr) -> ::std::result::Result<Expression, Self::Error> {
        match expr.get_tp() {
            ExprType::Null => {
                Ok(Expression::Constant(Datum::Null))
            }
            ExprType::Int64 => {
                expr.get_val().decode_i64()
                    .map(Datum::I64)
                    .map(Expression::Constant)
                    .map_err(Error::from)
            }
            ExprType::Uint64 => {
                expr.get_val().decode_u64()
                    .map(Datum::U64)
                    .map(Expression::Constant)
                    .map_err(Error::from)
            }
            ExprType::String | ExprType::Bytes => {
                let mut expr = expr;
                Ok(Expression::Constant(Datum::Bytes(expr.take_val())))
            }
            ExprType::Float32 | ExprType::Float64 => {
                expr.get_val().decode_f64()
                    .map(Datum::F64)
                    .map(Expression::Constant)
                    .map_err(Error::from)
            }
            ExprType::MysqlDuration => {
                expr.get_val().decode_i64()
                    .and_then(|n| Duration::from_nanos(n, MAX_FSP))
                    .map(Datum::Dur)
                    .map(Expression::Constant)
                    .map_err(Error::from)
            }
            ExprType::MysqlDecimal => {
                expr.get_val().decode_decimal()
                    .map(Datum::Dec)
                    .map(Expression::Constant)
                    .map_err(Error::from)
            }
            // TODO(andelf): fn sig verification
            ExprType::ScalarFunc => {
                let sig = expr.get_sig();
                let mut expr = expr;
                expr.take_children().into_iter()
                    .map(Expression::try_from)
                    .collect::<Result<Vec<_>>>()
                    .map(|children| {
                        Expression::ScalarFn(FnCall {
                            sig: sig,
                            children: children,
                        })
                    })
            }
            ExprType::ColumnRef => {
                expr.get_val().decode_i64()
                    .map(|i| Expression::ColumnRef(i as usize))
                    .map_err(Error::from)
            }
            _ => unreachable!("can't handle in DAG mode")
        }
    }
}


#[test]
fn test_smoke() {
    let mut pb = Expr::new();
    pb.set_tp(ExprType::ColumnRef);
    pb.mut_val().encode_i64(1).unwrap();

    let e: Result<Expression> = pb.try_into();
    let _ = e.unwrap();
}

