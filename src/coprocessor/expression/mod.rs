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
use std::convert::{TryFrom, TryInto};

use super::codec::mysql::{Duration, Time, Decimal};
use super::codec::Datum;
use super::super::util;

use chrono::FixedOffset;

use tipb::expression::{DataType, ExprType, ScalarFuncSig};
use tipb::expression::Expr;
use tipb::expression::FieldType;
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
        Type { has: TypeClass, expected: TypeClass } {
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


#[derive(Debug, Copy, Clone, PartialEq, Eq)]
pub enum TypeClass {
    String,
    Real,
    Int,
    Decimal,
    Time,
    Duration,
}


/// Expression represents all scalar expression in SQL.
#[derive(Debug, Clone, PartialEq)]
pub struct Expression {
    pub expr: ExprKind,
    pub ret_type: TypeClass,
}

#[derive(Debug, Clone, PartialEq)]
pub enum ExprKind {
    Constant(Datum),
    ScalarFn(BuiltinFn),
    ColumnRef(usize),
}


#[derive(Debug, Clone, PartialEq)]
pub enum UnOp {
    Not,
    Neg,
    BitNeg,
    // ISNULL(expr)
    IsNull,
    // IS boolean_value
    IsTrue,
    IsFalse,
}

#[derive(Debug, Clone, PartialEq)]
pub enum BinOp {
    // arithmetical
    Add,
    Minus,
    Mul,
    Div,
    Rem,
    IntDiv,
    // logical
    And,
    Or,
    Xor,
    // bitwise
    BitAnd,
    BitOr,
    BitXor,
    Shl,
    Shr,
    // compare
    Lt,
    Le,
    Eq,
    Ne,
    Ge,
    Gt,
    NullEq,
}

#[derive(Debug, Clone, PartialEq)]
pub struct CaseArm {
    pub condition: Option<Box<Expression>>,
    pub result: Box<Expression>,
}

#[derive(Debug, Clone, PartialEq)]
pub enum BuiltinFn {
    // ## operators
    Unary(UnOp, Box<Expression>),
    Binary(BinOp, Box<Expression>, Box<Expression>),
    // COALESCE(value,...)
    Coalesce(Vec<Expression>),
    // GREATEST(value1,value2,...)
    Greatest(Vec<Expression>),
    // expr IN (value,...)
    In(Box<Expression>, Vec<Expression>),
    // INTERVAL(N,N1,N2,N3,...)
    Interval(Box<Expression>, Vec<Expression>),
    // LEAST(value1,value2,...)
    Least(Vec<Expression>),

    // ## Control Flow Functions
    //
    // CASE [value] WHEN [compare_value] THEN result [WHEN [compare_value] THEN result ...]
    // [ELSE result] END
    Case(Option<Box<Expression>>, Vec<CaseArm>, Option<Box<Expression>>),
    // IF(expr1,expr2,expr3)
    If(Box<Expression>, Box<Expression>, Box<Expression>),
    // IFNULL(expr1,expr2)
    IfNull(Box<Expression>, Box<Expression>),
    // NULLIF(expr1,expr2)
    NullIf(Box<Expression>, Box<Expression>),

    // ## String Comparison Functions
    // expr LIKE pat
    Like(Box<Expression>, Option<String>),
    // CAST(expr AS type)
    Cast(Box<Expression>), // TODO: add more here
}


impl TryFrom<Expr> for Expression {
    type Error = Error;

    fn try_from(expr: Expr) -> ::std::result::Result<Expression, Self::Error> {
        match expr.get_tp() {
            ExprType::Null => {
                Ok(Expression {
                    expr: ExprKind::Constant(Datum::Null),
                    ret_type: TypeClass::Int, // FIXME: a NULL variant?
                })
            },
            ExprType::Int64 => {
                Ok(Expression {
                    expr: ExprKind::Constant(Datum::I64(expr.get_val().decode_i64()?)),
                    ret_type: TypeClass::Int,
                })
            },
            ExprType::Uint64 => {
                Ok(Expression {
                    expr: ExprKind::Constant(Datum::U64(expr.get_val().decode_u64()?)),
                    ret_type: TypeClass::Int,
                })
            },
            ExprType::String | ExprType::Bytes => {
                let mut expr = expr;
                Ok(Expression {
                    expr: ExprKind::Constant(Datum::Bytes(expr.take_val())),
                    ret_type: TypeClass::String,
                })
            },
            ExprType::Float32 | ExprType::Float64 => {
                Ok(Expression {
                    expr: ExprKind::Constant(Datum::F64(expr.get_val().decode_f64()?)),
                    ret_type: TypeClass::Real,
                })
            },
            ExprType::MysqlDuration => {
                let n = try!(expr.get_val().decode_i64());
                let dur = try!(Duration::from_nanos(n, MAX_FSP));
                Ok(Expression {
                    expr: ExprKind::Constant(Datum::Dur(dur)),
                    ret_type: TypeClass::Real,
                })
            },
            ExprType::MysqlDecimal => {
                Ok(Expression {
                    expr: ExprKind::Constant(Datum::Dec(expr.get_val().decode_decimal()?)),
                    ret_type: TypeClass::Decimal,
                })
            },
            ExprType::ScalarFunc => {
                expr_scalar_fn_into_expression(expr)
            },
            ExprType::ColumnRef => {
                Ok(Expression {
                    expr: ExprKind::ColumnRef(expr.get_val().decode_i64()? as usize),
                    ret_type: expr.get_field_type().get_tp().type_class(),
                })
            },

            _ => unimplemented!(),
        }
    }
}



fn expr_scalar_fn_into_expression(mut expr: Expr) -> Result<Expression> {
    match expr.get_sig() {
        ScalarFuncSig::CastIntAsInt => {
            let inner = expr.take_children().pop()
                .ok_or(Error::Other("empty expr children"))
                .and_then(Expression::try_from)?;
            Ok(Expression {
                expr: ExprKind::ScalarFn(BuiltinFn::Cast(box inner)),
                ret_type: TypeClass::Int,
            })
        }
        ScalarFuncSig::CastIntAsReal => {
            let inner = expr.take_children().pop()
                .ok_or(Error::Other("empty expr children"))
                .and_then(Expression::try_from)?;
            Ok(Expression {
                expr: ExprKind::ScalarFn(BuiltinFn::Cast(box inner)),
                ret_type: TypeClass::Real,
            })
        }
        ScalarFuncSig::CastIntAsString => {
            let inner = expr.take_children().pop()
                .ok_or(Error::Other("empty expr children"))
                .and_then(Expression::try_from)?;
            Ok(Expression {
                expr: ExprKind::ScalarFn(BuiltinFn::Cast(box inner)),
                ret_type: TypeClass::String,
            })
        }
        ScalarFuncSig::CastIntAsDecimal => {
            let inner = expr.take_children().pop()
                .ok_or(Error::Other("empty expr children"))
                .and_then(Expression::try_from)?;
            Ok(Expression {
                expr: ExprKind::ScalarFn(BuiltinFn::Cast(box inner)),
                ret_type: TypeClass::Decimal,
            })
        }
        ScalarFuncSig::CastIntAsTime => {
            let inner = expr.take_children().pop()
                .ok_or(Error::Other("empty expr children"))
                .and_then(Expression::try_from)?;
            Ok(Expression {
                expr: ExprKind::ScalarFn(BuiltinFn::Cast(box inner)),
                ret_type: TypeClass::Time,
            })
        }
        ScalarFuncSig::CastIntAsDuration => {
            let inner = expr.take_children().pop()
                .ok_or(Error::Other("empty expr children"))
                .and_then(Expression::try_from)?;
            Ok(Expression {
                expr: ExprKind::ScalarFn(BuiltinFn::Cast(box inner)),
                ret_type: TypeClass::Duration,
            })
        }

        // TODO: CAST family
        /*
        CastRealAsInt ,
	      CastRealAsReal ,
	      CastRealAsString ,
	      CastRealAsDecimal ,
	      CastRealAsTime ,
	      CastRealAsDuration ,

	      CastDecimalAsInt ,
	      CastDecimalAsReal ,
	      CastDecimalAsString ,
	      CastDecimalAsDecimal ,
	      CastDecimalAsTime ,
	      CastDecimalAsDuration ,

	      CastStringAsInt ,
	      CastStringAsReal ,
	      CastStringAsString ,
	      CastStringAsDecimal ,
	      CastStringAsTime ,
	      CastStringAsDuration ,

	      CastTimeAsInt ,
	      CastTimeAsReal ,
	      CastTimeAsString ,
	      CastTimeAsDecimal ,
	      CastTimeAsTime ,
	      CastTimeAsDuration ,

	      CastDurationAsInt ,
	      CastDurationAsReal ,
	      CastDurationAsString ,
	      CastDurationAsDecimal ,
	      CastDurationAsTime ,
	      CastDurationAsDuration ,
         */

        ScalarFuncSig::LTInt => {
            let rhs = expr.take_children().pop()
                .ok_or(Error::Other("empty expr children"))
                .and_then(Expression::try_from)?;
            let lhs = expr.take_children().pop()
                .ok_or(Error::Other("non enough expr children"))
                .and_then(Expression::try_from)?;
            Ok(Expression {
                expr: ExprKind::ScalarFn(
                    BuiltinFn::Binary(BinOp::Lt, box lhs, box rhs)),
                ret_type: TypeClass::Int,
            })
        }

        _ => unimplemented!()
    }
}



trait DataTypeExt {
    fn type_class(&self) -> TypeClass;
    fn is_blob_type(&self) -> bool;
    fn is_char_type(&self) -> bool;
    fn is_var_char_type(&self) -> bool;
    fn is_prefixable(&self) -> bool;
    fn has_time_fraction(&self) -> bool;
    fn is_time_type(&self) -> bool;
}

impl DataTypeExt for DataType {
    fn type_class(&self) -> TypeClass {
        match *self {
            DataType::TypeTiny |
            DataType::TypeShort |
            DataType::TypeInt24 |
            DataType::TypeLong |
            DataType::TypeLongLong |
            DataType::TypeBit |
            DataType::TypeYear => TypeClass::Int,
            DataType::TypeNewDecimal => TypeClass::Decimal,
            DataType::TypeFloat | DataType::TypeDouble => TypeClass::Real,
            _ => TypeClass::String,
        }
    }

    fn is_blob_type(&self) -> bool {
        match *self {
            DataType::TypeTinyBlob | DataType::TypeMediumBlob | DataType::TypeBlob |
            DataType::TypeLongBlob => true,
            _ => false,
        }
    }

    fn is_char_type(&self) -> bool {
        match *self {
            DataType::TypeString | DataType::TypeVarchar => true,
            _ => false,
        }
    }

    fn is_var_char_type(&self) -> bool {
        match *self {
            DataType::TypeVarString | DataType::TypeVarchar => true,
            _ => false,
        }
    }

    fn is_prefixable(&self) -> bool {
        self.is_blob_type() || self.is_char_type()
    }

    fn has_time_fraction(&self) -> bool {
        match *self {
            DataType::TypeDatetime | DataType::TypeDuration | DataType::TypeTimestamp => true,
            _ => false,
        }
    }

    fn is_time_type(&self) -> bool {
        match *self {
            DataType::TypeDatetime | DataType::TypeDate | DataType::TypeNewDate |
            DataType::TypeTimestamp => true,
            _ => false,
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

