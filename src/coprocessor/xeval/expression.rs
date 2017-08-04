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


use coprocessor::codes::mysql::types;
use tipb::expression::{Expr, ExprType, ScalarFuncSig};

use super::super::codec::datum::{Datum, DatumDecoder};

pub type Result<T> = ::std::result::Result<T, Error>;


use chrono::FixedOffset;


pub enum Nullable<T> {
    Value(T),
    Null,
}

impl<T> Nullable<T> {
    pub fn is_null(&self) -> bool {
        match *self {
            Nullable::Value(_) => false,
            Nullable::Null => true
        }
    }

     pub fn is_value(&self) -> bool {
        match *self {
            Nullable::Value(_) => true,
            Nullable::Null => false
        }
    }

    pub fn map<F>(self, f: F) -> Nullable<U>
        where F: FnOnce(T) -> U {
        match self {
            Nullable::Value(val) => Nullable::Value(f(val)),
            Nullable::Null => Nullable::Null
        }
    }
}


/// StatementContext contains variables for a statement
/// It should be reset before executing a statement
pub struct StatementContext {
    ignore_truncate: bool,
    truncate_as_warning: bool,
    // from DAGRequest
    timezone: FixedOffset,
}

/// Expression represents all scalar expression in SQL.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct Expr {
    pub expr: ExprKind,
    pub ret_type: u8,
}

pub enum ExprKind {
    Constant(Datum),
    ScalarFn(BuiltinFn),
    /// column offset
    Column(usize),
}


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

pub struct CaseArm {
    pub condition: Option<Box<Expr>>,
    pub result: Box<Expr>,
}

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
    // TODO: expr NOT IN (value,...)
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

    // ## String Functions
    StringFn(StringFnKind),

    // ## String Comparison Functions
    // expr LIKE pat [ESCAPE 'escape_char']
    Like(Box<Expr>, Box<Expr>, Option<String>),

    Cast(Box<Expr>),
}



/*
Item_decimal_typecast -> Item_func ;
Item_json_typecast -> Item_json_func ;
Item_char_typecast -> Item_str_func ;
Item_date_typecast -> Item_date_func ;
Item_time_typecast -> Item_time_func ;
Item_datetime_typecast -> Item_datetime_func ;
 */





/*

        value: Datum,
        ret_type: u8,
    },
    ScalarFunction {
        name: String,
        ret_type: u8,
        function: BuiltinFunc,
    },
    ColumnRef {
        offset: usize,
        ret_type: u8,
    },
    CorrelatedColumn {
        column: Column,
        data: Datum,
    }
}

*/

impl TryFrom<Expr> for Expr {
    type Error = ...;

    fn try_from(expr: Expr) -> Result<Expr, Self::Error> {
        match expr.get_tp() {
            ExprType::ColumnRef => Expr::ColumnRef,
        }
    }
}

impl Expr {
    /// Eval evaluates an expression through a row.
    pub fn eval(&self, row: &[Datum]) -> Result<Datum> {

    }

    /// EvalInt returns the int64 representation of expression.
    pub fn eval_int(&self, row: &[Datum], ctx: &StatementContext) -> Result<Nullable<i64>> {

    }

    /// EvalReal returns the float64 representation of expression.
    pub fn eval_real(&self, row: &[Datum], ctx: &StatementContext) -> Result<Nullable<f64>> {

    }

    /// EvalString returns the string representation of expression.
    pub fn eval_string(&self, row: &[Datum], ctx: &StatementContext) -> Result<Nullable<String>> {

    }

    /// EvalDecimal returns the decimal representation of expression.
    pub fn eval_decimal(&self, row: &[Datum], ctx: &StatementContext) -> Result<Nullable<Decimal>> {

    }

    /// EvalTime returns the DATE/DATETIME/TIMESTAMP representation of expression.
    pub fn eval_time(&self, row: &[Datum], ctx: &StatementContext) -> Result<Nullable<Time>> {

    }

    /// returns the duration representation of expression.
    pub fn eval_duration(&self, row: &[Datum], ctx: &StatementContext) -> Result<Nullable<Duration>> {

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

5 result type
enum Item_result {STRING_RESULT=0, REAL_RESULT, INT_RESULT, ROW_RESULT,
                  DECIMAL_RESULT};


IntFn:
collation.set_numeric()
    INT_RESULT



