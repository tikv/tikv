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

use super::codec::mysql::{Duration, Time, Decimal};
use super::codec::Datum;
use super::super::util;

use chrono::FixedOffset;

use tipb::expression::{DataType, ExprType, ScalarFuncSig};
use tipb::expression::Expr as ExprPb;
use tipb::expression::FieldType;
use tipb::select::DAGRequest;

use util::codec::number::NumberDecoder;
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
    String = 0,
    Real = 1,
    Int = 2,
    Decimal = 4,
    Time = 8,
}


/// Expression represents all scalar expression in SQL.
#[derive(Debug, Clone, PartialEq)]
pub struct Expr {
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
    pub condition: Option<Box<Expr>>,
    pub result: Box<Expr>,
}

#[derive(Debug, Clone, PartialEq)]
pub enum BuiltinFn {
    // ## operators
    Unary(UnOp, Box<Expr>),
    Binary(BinOp, Box<Expr>, Box<Expr>),
    // COALESCE(value,...)
    Coalesce(Vec<Expr>),
    // GREATEST(value1,value2,...)
    Greatest(Vec<Expr>),
    // expr IN (value,...)
    In(Box<Expr>, Vec<Expr>),
    // INTERVAL(N,N1,N2,N3,...)
    Interval(Box<Expr>, Vec<Expr>),
    // LEAST(value1,value2,...)
    Least(Vec<Expr>),

    // ## Control Flow Functions
    //
    // CASE value WHEN [compare_value] THEN result [WHEN [compare_value] THEN result ...]
    // [ELSE result] END
    Case(Box<Expr>, Vec<CaseArm>, Option<Box<Expr>>),
    // CASE WHEN [condition] THEN result [WHEN [condition] THEN result ...] [ELSE result] END
    CaseWhen(Vec<CaseArm>, Option<Box<Expr>>),
    // IF(expr1,expr2,expr3)
    If(Box<Expr>, Box<Expr>, Box<Expr>),
    // IFNULL(expr1,expr2)
    IfNull(Box<Expr>, Box<Expr>),
    // NULLIF(expr1,expr2)
    NullIf(Box<Expr>, Box<Expr>),

    // ## String Comparison Functions
    // expr LIKE pat [ESCAPE 'escape_char']
    Like(Box<Expr>, Box<Expr>, Option<String>),

    Cast(Box<Expr>),

    // TODO: add more here
}


impl TryFrom<ExprPb> for Expr {
    type Error = Error;

    fn try_from(expr: ExprPb) -> ::std::result::Result<Expr, Self::Error> {
        match expr.get_tp() {
            ExprType::ColumnRef => {
                Ok(Expr {
                    expr: ExprKind::ColumnRef(try!(expr.get_val().decode_i64()) as usize),
                    ret_type: expr.get_field_type().get_tp().type_class(),
                })
            }
            _ => unimplemented!(),
        }
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
