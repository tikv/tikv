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
    // from DAGRequest
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
    ColumnOffset(usize),
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
    IsFalse
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

    // CASE value WHEN [compare_value] THEN result [WHEN [compare_value] THEN result ...] [ELSE result] END
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

    // ## String Functions
    // StringFn(StringFnKind),

    // TODO: add more here
}


impl TryFrom<ExprPb> for Expr {
    type Error = Error;

    fn try_from(expr: ExprPb) -> ::std::result::Result<Expr, Self::Error> {
        match expr.get_tp() {
            ExprType::ColumnRef => {
                Ok(Expr {
                    expr: ExprKind::ColumnOffset(try!(expr.get_val().decode_i64()) as usize),
                    ret_type: TypeClass::Int,
                })
            },
            _ => unimplemented!()
        }
    }
}



/*
impl Expr {
    /// Eval evaluates an expression through a row.
    pub fn eval(&self, row: &[Datum]) -> Result<Datum> {

    }

    /// EvalInt returns the int64 representation of expression.
    pub fn eval_int(&self, row: &[Datum], ctx: &StatementContext) -> Result<Option<i64>> {

    }

    /// EvalReal returns the float64 representation of expression.
    pub fn eval_real(&self, row: &[Datum], ctx: &StatementContext) -> Result<Option<f64>> {

    }

    /// EvalString returns the string representation of expression.
    pub fn eval_string(&self, row: &[Datum], ctx: &StatementContext) -> Result<Option<String>> {

    }

    /// EvalDecimal returns the decimal representation of expression.
    pub fn eval_decimal(&self, row: &[Datum], ctx: &StatementContext) -> Result<Option<Decimal>> {

    }

    /// EvalTime returns the DATE/DATETIME/TIMESTAMP representation of expression.
    pub fn eval_time(&self, row: &[Datum], ctx: &StatementContext) -> Result<Option<Time>> {

    }

    /// gets the type that the expression returns.
    pub fn get_type(&self) -> FileType {

    }

    pub fn equal<T: Expr>(&self, rhs: &T, ctx: &Context) -> bool {

    }

    pub fn is_correlated(&self) -> bool {

    }

    pub fn decorrelate(&self, schema: &Schema) -> Expr {

    }

    pub fn resolve_indices(&mut self, schema: &Schema) {

    }

    pub fn explain_info(&self) -> String {

    }
}




*/
