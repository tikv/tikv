// Copyright 2016 PingCAP, Inc.
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


use util::codec::{number, Datum};
use super::{Result, Error};

use std::collections::HashMap;
use std::cmp::Ordering;
use tipb::expression::{Expr, ExprType};

/// `Evaluator` evaluates `tipb::Expr`.
pub struct Evaluator {
    // column_id -> column_value
    row: HashMap<i64, Datum>,
}

impl Evaluator {
    pub fn new(row: HashMap<i64, Datum>) -> Evaluator {
        Evaluator { row: row }
    }

    /// Eval evaluates expr to a Datum.
    pub fn eval(&self, expr: &Expr) -> Result<Datum> {
        match expr.get_tp() {
            ExprType::Int64 => self.eval_int(expr),
            ExprType::Uint64 => self.eval_uint(expr),
            // maybe we should use take here?
            ExprType::String | ExprType::Bytes => Ok(Datum::Bytes(expr.get_val().to_vec())),
            ExprType::ColumnRef => self.eval_column_ref(expr),
            ExprType::LT => self.eval_lt(expr),
            ExprType::LE => self.eval_le(expr),
            ExprType::EQ => self.eval_eq(expr),
            ExprType::NE => self.eval_ne(expr),
            ExprType::GE => self.eval_ge(expr),
            ExprType::GT => self.eval_gt(expr),
            ExprType::NullEQ => self.eval_null_eq(expr),
            ExprType::And => self.eval_and(expr),
            ExprType::Or => self.eval_or(expr),
            ExprType::Float32 | ExprType::Float64 => unimplemented!(),
            _ => Ok(Datum::Null),
        }
    }

    fn eval_int(&self, expr: &Expr) -> Result<Datum> {
        let i = try!(number::decode_i64(expr.get_val()));
        Ok(Datum::I64(i))
    }

    fn eval_uint(&self, expr: &Expr) -> Result<Datum> {
        let u = try!(number::decode_u64(expr.get_val()));
        Ok(Datum::U64(u))
    }

    fn eval_column_ref(&self, expr: &Expr) -> Result<Datum> {
        let i = try!(number::decode_i64(expr.get_val()));
        self.row.get(&i).cloned().ok_or_else(|| Error::Eval(format!("column {} not found", i)))
    }

    fn eval_lt(&self, expr: &Expr) -> Result<Datum> {
        let cmp = try!(self.cmp_children(expr));
        Ok(cmp.map(|c| c < Ordering::Equal).into())
    }

    fn eval_le(&self, expr: &Expr) -> Result<Datum> {
        let cmp = try!(self.cmp_children(expr));
        Ok(cmp.map(|c| c <= Ordering::Equal).into())
    }

    fn eval_eq(&self, expr: &Expr) -> Result<Datum> {
        let cmp = try!(self.cmp_children(expr));
        Ok(cmp.map(|c| c == Ordering::Equal).into())
    }

    fn eval_ne(&self, expr: &Expr) -> Result<Datum> {
        let cmp = try!(self.cmp_children(expr));
        Ok(cmp.map(|c| c != Ordering::Equal).into())
    }

    fn eval_ge(&self, expr: &Expr) -> Result<Datum> {
        let cmp = try!(self.cmp_children(expr));
        Ok(cmp.map(|c| c >= Ordering::Equal).into())
    }

    fn eval_gt(&self, expr: &Expr) -> Result<Datum> {
        let cmp = try!(self.cmp_children(expr));
        Ok(cmp.map(|c| c > Ordering::Equal).into())
    }

    fn eval_null_eq(&self, expr: &Expr) -> Result<Datum> {
        let (left, right) = try!(self.eval_children(expr));
        let cmp = try!(left.cmp(&right));
        Ok((cmp == Ordering::Equal).into())
    }

    fn cmp_children(&self, expr: &Expr) -> Result<Option<Ordering>> {
        let (left, right) = try!(self.eval_children(expr));
        if left == Datum::Null || right == Datum::Null {
            return Ok(None);
        }
        left.cmp(&right).map(Some).map_err(From::from)
    }

    fn eval_children(&self, expr: &Expr) -> Result<(Datum, Datum)> {
        let l = expr.get_children().len();
        if l != 2 {
            return Err(Error::Expr(format!("need 2 operands but got {}", l)));
        }
        let children = expr.get_children();
        let left = try!(self.eval(&children[0]));
        let right = try!(self.eval(&children[1]));
        Ok((left, right))
    }

    fn eval_and(&self, expr: &Expr) -> Result<Datum> {
        self.eval_children_as_bool(expr)
            .map(|(l, r)| (l.unwrap_or(false) && r.unwrap_or(false)).into())
    }

    fn eval_or(&self, expr: &Expr) -> Result<Datum> {
        self.eval_children_as_bool(expr)
            .map(|(l, r)| (l.unwrap_or(false) || r.unwrap_or(false)).into())
    }

    fn eval_children_as_bool(&self, expr: &Expr) -> Result<(Option<bool>, Option<bool>)> {
        let (left, right) = try!(self.eval_children(expr));
        let left_bool = try!(eval_as_bool(&left));
        let right_bool = try!(eval_as_bool(&right));
        Ok((left_bool, right_bool))
    }
}


/// eval datum as bool, if expr is Null, then None is return.
fn eval_as_bool(datum: &Datum) -> Result<Option<bool>> {
    if *datum == Datum::Null {
        Ok(None)
    } else {
        let b = try!(datum.as_bool());
        Ok(Some(b))
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use util::codec::{Datum, number};

    use tipb::expression::{Expr, ExprType};
    use protobuf::RepeatedField;

    fn datum_expr(datum: Datum) -> Expr {
        let mut expr = Expr::new();
        match datum {
            Datum::I64(i) => {
                expr.set_tp(ExprType::Int64);
                let mut buf = vec![0; 8];
                number::encode_i64(&mut buf, i).unwrap();
                expr.set_val(buf);
            }
            Datum::U64(u) => {
                expr.set_tp(ExprType::Uint64);
                let mut buf = vec![0; 8];
                number::encode_u64(&mut buf, u).unwrap();
                expr.set_val(buf);
            }
            Datum::Bytes(bs) => {
                expr.set_tp(ExprType::Bytes);
                expr.set_val(bs);
            }
            Datum::F32(_) => unimplemented!(),
            Datum::F64(_) => unimplemented!(),
            _ => expr.set_tp(ExprType::Null),
        };
        expr
    }

    fn col_expr(col_id: i64) -> Expr {
        let mut expr = Expr::new();
        expr.set_tp(ExprType::ColumnRef);
        let mut buf = vec![0; 8];
        number::encode_i64(&mut buf, col_id).unwrap();
        expr.set_val(buf);
        expr
    }

    fn bin_expr(left: Datum, right: Datum, tp: ExprType) -> Expr {
        bin_expr_r(datum_expr(left), datum_expr(right), tp)
    }

    fn bin_expr_r(left: Expr, right: Expr, tp: ExprType) -> Expr {
        let mut expr = Expr::new();
        expr.set_tp(tp);
        expr.set_children(RepeatedField::from_vec(vec![left, right]));
        expr
    }

    // TODO: add more tests.
    #[test]
    fn test_eval() {
        let tests = vec![
			(datum_expr(Datum::I64(1)), Datum::I64(1)),
			(datum_expr(Datum::U64(1)), Datum::U64(1)),
			(datum_expr(b"abc".as_ref().into()), b"abc".as_ref().into()),
			(datum_expr(Datum::Null), Datum::Null),
			(col_expr(1), Datum::I64(100)),
			(bin_expr(Datum::I64(100), Datum::I64(1), ExprType::LT), Datum::I64(0)),
			(bin_expr(Datum::I64(1), Datum::I64(100), ExprType::LT), Datum::I64(1)),
			(bin_expr(Datum::I64(100), Datum::Null, ExprType::LT), Datum::Null),
			(bin_expr(Datum::I64(100), Datum::I64(1), ExprType::LE), Datum::I64(0)),
			(bin_expr(Datum::I64(1), Datum::I64(1), ExprType::LE), Datum::I64(1)),
			(bin_expr(Datum::I64(100), Datum::Null, ExprType::LE), Datum::Null),
			(bin_expr(Datum::I64(100), Datum::I64(1), ExprType::EQ), Datum::I64(0)),
			(bin_expr(Datum::I64(100), Datum::I64(100), ExprType::EQ), Datum::I64(1)),
			(bin_expr(Datum::I64(100), Datum::Null, ExprType::EQ), Datum::Null),
			(bin_expr(Datum::I64(100), Datum::I64(100), ExprType::NE), Datum::I64(0)),
			(bin_expr(Datum::I64(100), Datum::I64(1), ExprType::NE), Datum::I64(1)),
			(bin_expr(Datum::I64(100), Datum::Null, ExprType::NE), Datum::Null),
			(bin_expr(Datum::I64(1), Datum::I64(100), ExprType::GE), Datum::I64(0)),
			(bin_expr(Datum::I64(100), Datum::I64(100), ExprType::GE), Datum::I64(1)),
			(bin_expr(Datum::I64(100), Datum::Null, ExprType::GE), Datum::Null),
			(bin_expr(Datum::I64(100), Datum::I64(100), ExprType::GT), Datum::I64(0)),
			(bin_expr(Datum::I64(100), Datum::I64(1), ExprType::GT), Datum::I64(1)),
			(bin_expr(Datum::I64(100), Datum::Null, ExprType::GT), Datum::Null),
			(bin_expr(Datum::I64(1), Datum::Null, ExprType::NullEQ), Datum::I64(0)),
			(bin_expr(Datum::Null, Datum::Null, ExprType::NullEQ), Datum::I64(1)),
			// logic operation
			(bin_expr(Datum::I64(0), Datum::I64(1), ExprType::And), Datum::I64(0)),
			(bin_expr(Datum::I64(1), Datum::I64(1), ExprType::And), Datum::I64(1)),
			(bin_expr(Datum::I64(1), Datum::Null, ExprType::And), Datum::I64(0)),
			(bin_expr(Datum::I64(0), Datum::I64(0), ExprType::Or), Datum::I64(0)),
			(bin_expr(Datum::I64(0), Datum::I64(1), ExprType::Or), Datum::I64(1)),
			(bin_expr(Datum::I64(1), Datum::Null, ExprType::Or), Datum::I64(1)),
			(bin_expr(Datum::Null, Datum::Null, ExprType::Or), Datum::I64(0)),
			(bin_expr_r(bin_expr(Datum::I64(1), Datum::I64(1), ExprType::EQ),
			 bin_expr(Datum::I64(1), Datum::I64(1), ExprType::EQ), ExprType::And), Datum::I64(1)),
        ];

        let row = map![1i64 => Datum::I64(100)];
        let xevaluator = Evaluator::new(row);
        for (expr, result) in tests {
            let res = xevaluator.eval(&expr);
            if res.is_err() {
                panic!("failed to eval {:?}: {:?}", expr, res);
            }
            let res = res.unwrap();
            if res != result {
                panic!("failed to eval {:?} expect {:?}, got {:?}",
                       expr,
                       result,
                       res);
            }
        }
    }
}
