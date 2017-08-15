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
// FIXME(shirly): remove following later
#![allow(dead_code,unused_variables)]

mod builtin_cast;
mod compare;

use std::io;
use std::convert::TryFrom;

use tipb::expression::{DataType, Expr, ExprType, FieldType, ScalarFuncSig};

use coprocessor::codec::mysql::{Decimal, Duration, Json, Time, MAX_FSP};
use coprocessor::codec::mysql::decimal::DecimalDecoder;
use coprocessor::codec::Datum;
use util;
use util::codec::number::NumberDecoder;

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
        Other(desc: &'static str) {
            description(desc)
            display("error {}", desc)
        }
    }
}

pub type Result<T> = ::std::result::Result<T, Error>;

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

    fn get_tp(&self) -> &FieldType {
        match *self {
            Expression::Constant(ref c) => &c.tp,
            Expression::ColumnRef(ref c) => &c.tp,
            Expression::ScalarFn(ref c) => &c.tp,
        }
    }

    fn eval_int(&self, ctx: &StatementContext, row: &[Datum]) -> Result<Option<i64>> {
        match *self {
            Expression::ScalarFn(ref f) => match f.sig {
                ScalarFuncSig::LTInt |
                ScalarFuncSig::LEInt |
                ScalarFuncSig::GTInt |
                ScalarFuncSig::GEInt |
                ScalarFuncSig::EQInt |
                ScalarFuncSig::NEInt |
                ScalarFuncSig::NullEQInt => f.compare_int(ctx, row, f.sig),

                ScalarFuncSig::LTReal |
                ScalarFuncSig::LEReal |
                ScalarFuncSig::GTReal |
                ScalarFuncSig::GEReal |
                ScalarFuncSig::EQReal |
                ScalarFuncSig::NEReal |
                ScalarFuncSig::NullEQReal => f.compare_real(ctx, row, f.sig),

                ScalarFuncSig::LTDecimal |
                ScalarFuncSig::LEDecimal |
                ScalarFuncSig::GTDecimal |
                ScalarFuncSig::GEDecimal |
                ScalarFuncSig::EQDecimal |
                ScalarFuncSig::NEDecimal |
                ScalarFuncSig::NullEQDecimal => f.compare_decimal(ctx, row, f.sig),

                ScalarFuncSig::LTString |
                ScalarFuncSig::LEString |
                ScalarFuncSig::GTString |
                ScalarFuncSig::GEString |
                ScalarFuncSig::EQString |
                ScalarFuncSig::NEString |
                ScalarFuncSig::NullEQString => f.compare_string(ctx, row, f.sig),

                ScalarFuncSig::LTTime |
                ScalarFuncSig::LETime |
                ScalarFuncSig::GTTime |
                ScalarFuncSig::GETime |
                ScalarFuncSig::EQTime |
                ScalarFuncSig::NETime |
                ScalarFuncSig::NullEQTime => f.compare_time(ctx, row, f.sig),

                ScalarFuncSig::LTDuration |
                ScalarFuncSig::LEDuration |
                ScalarFuncSig::GTDuration |
                ScalarFuncSig::GEDuration |
                ScalarFuncSig::EQDuration |
                ScalarFuncSig::NEDuration |
                ScalarFuncSig::NullEQDuration => f.compare_duration(ctx, row, f.sig),

                ScalarFuncSig::LTJson |
                ScalarFuncSig::LEJson |
                ScalarFuncSig::GTJson |
                ScalarFuncSig::GEJson |
                ScalarFuncSig::EQJson |
                ScalarFuncSig::NEJson |
                ScalarFuncSig::NullEQJson => f.compare_json(ctx, row, f.sig),
                _ => Err(Error::Other("Unknown signature")),
            },
            _ => unimplemented!(),
        }
    }

    fn eval_real(&self, ctx: &StatementContext, row: &[Datum]) -> Result<Option<f64>> {
        unimplemented!()
    }

    fn eval_decimal(&self, ctx: &StatementContext, row: &[Datum]) -> Result<Option<Decimal>> {
        unimplemented!()
    }

    fn eval_string(&self, ctx: &StatementContext, row: &[Datum]) -> Result<Option<Vec<u8>>> {
        unimplemented!()
    }

    fn eval_time(&self, ctx: &StatementContext, row: &[Datum]) -> Result<Option<Time>> {
        unimplemented!()
    }

    fn eval_duration(&self, ctx: &StatementContext, row: &[Datum]) -> Result<Option<Duration>> {
        unimplemented!()
    }

    fn eval_json(&self, ctx: &StatementContext, row: &[Datum]) -> Result<Option<Json>> {
        unimplemented!()
    }

    /// IsHybridType checks whether a ClassString expression is a hybrid type value which will
    /// return different types of value in different context.
    /// For ENUM/SET which is consist of a string attribute `Name` and an int attribute `Value`,
    /// it will cause an error if we convert ENUM/SET to int as a string value.
    /// For Bit/Hex, we will get a wrong result if we convert it to int as a string value.
    /// For example, when convert `0b101` to int, the result should be 5, but we will get
    /// 101 if we regard it as a string.
    fn is_hybrid_type(&self) -> bool {
        match self.get_tp().get_tp() {
            DataType::TypeEnum | DataType::TypeBit | DataType::TypeSet => {
                return true;
            }
            _ => {}
        }
        // TODO:For a constant, the field type will be inferred as `VARCHAR`
        // when the kind of it is `HEX` or `BIT`.
        false
    }
}

impl TryFrom<Expr> for Expression {
    type Error = Error;

    fn try_from(expr: Expr) -> ::std::result::Result<Expression, Self::Error> {
        let mut expr = expr;
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
            // TODO(andelf): fn sig verification
            ExprType::ScalarFunc => {
                let sig = expr.get_sig();
                let mut expr = expr;
                expr.take_children()
                    .into_iter()
                    .map(Expression::try_from)
                    .collect::<Result<Vec<_>>>()
                    .map(|children| {
                        Expression::ScalarFn(FnCall {
                            sig: sig,
                            children: children,
                            tp: tp,
                        })
                    })
            }
            ExprType::ColumnRef => expr.get_val()
                .decode_i64()
                .map(|i| {
                    Expression::ColumnRef(Column {
                        offset: i as usize,
                        tp: tp,
                    })
                })
                .map_err(Error::from),
            unhandled => unreachable!("can't handle {:?} expr in DAG mode", unhandled),
        }
    }
}

#[test]
fn test_smoke() {
    use std::convert::TryInto;
    use util::codec::number::NumberEncoder;

    let mut pb = Expr::new();
    pb.set_tp(ExprType::ColumnRef);
    pb.mut_val().encode_i64(1).unwrap();

    let e: Result<Expression> = pb.try_into();
    let _ = e.unwrap();
}
