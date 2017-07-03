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

use std::cmp::Ordering;
use std::ascii::AsciiExt;
use std::str;

use chrono::FixedOffset;
use tipb::expression::{Expr, ExprType};
use tipb::select::SelectRequest;

use util::codec::number::NumberDecoder;
use util::codec::datum::{Datum, DatumDecoder};
use util::codec::mysql::{DecimalDecoder, MAX_FSP, Duration};
use util::codec;
use util::collections::{HashMap, HashMapEntry};

use super::{Result, Error};

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

#[derive(Debug)]
/// Some global variables needed in an evaluation.
pub struct EvalContext {
    /// timezone to use when parse/calculate time.
    pub tz: FixedOffset,
    pub ignore_truncate: bool,
    pub truncate_as_warning: bool,
}

impl Default for EvalContext {
    fn default() -> EvalContext {
        EvalContext {
            tz: FixedOffset::east(0),
            ignore_truncate: false,
            truncate_as_warning: false,
        }
    }
}

const ONE_DAY: i64 = 3600 * 24;

impl EvalContext {
    pub fn new(sel: &SelectRequest) -> Result<EvalContext> {
        let offset = sel.get_time_zone_offset();
        if offset <= -ONE_DAY || offset >= ONE_DAY {
            return Err(Error::Eval(format!("invalid tz offset {}", offset)));
        }
        let tz = match FixedOffset::east_opt(offset as i32) {
            None => return Err(Error::Eval(format!("invalid tz offset {}", offset))),
            Some(tz) => tz,
        };

        let flags = sel.get_flags();

        let e = EvalContext {
            tz: tz,
            ignore_truncate: (flags & FLAG_IGNORE_TRUNCATE) > 0,
            truncate_as_warning: (flags & FLAG_TRUNCATE_AS_WARNING) > 0,
        };

        Ok(e)
    }
}

/// `Evaluator` evaluates `tipb::Expr`.
#[derive(Default)]
pub struct Evaluator {
    // column_id -> column_value
    pub row: HashMap<i64, Datum>,
    // expr pointer -> value list
    cached_value_list: HashMap<isize, Vec<Datum>>,
}

impl Evaluator {
    pub fn batch_eval(&mut self, ctx: &EvalContext, exprs: &[Expr]) -> Result<Vec<Datum>> {
        let mut res = Vec::with_capacity(exprs.len());
        for expr in exprs {
            let r = try!(self.eval(ctx, expr));
            res.push(r);
        }
        Ok(res)
    }

    /// Eval evaluates expr to a Datum.
    pub fn eval(&mut self, ctx: &EvalContext, expr: &Expr) -> Result<Datum> {
        match expr.get_tp() {
            ExprType::Int64 => self.eval_int(expr),
            ExprType::Uint64 => self.eval_uint(expr),
            // maybe we should use take here?
            ExprType::String | ExprType::Bytes => Ok(Datum::Bytes(expr.get_val().to_vec())),
            ExprType::ColumnRef => self.eval_column_ref(expr),
            ExprType::LT => self.eval_lt(ctx, expr),
            ExprType::LE => self.eval_le(ctx, expr),
            ExprType::EQ => self.eval_eq(ctx, expr),
            ExprType::NE => self.eval_ne(ctx, expr),
            ExprType::GE => self.eval_ge(ctx, expr),
            ExprType::GT => self.eval_gt(ctx, expr),
            ExprType::NullEQ => self.eval_null_eq(ctx, expr),
            ExprType::And => self.eval_logic(ctx, expr, Some(false), eval_and),
            ExprType::Or => self.eval_logic(ctx, expr, Some(true), eval_or),
            ExprType::Not => self.eval_not(ctx, expr),
            ExprType::Like => self.eval_like(ctx, expr),
            ExprType::Float32 |
            ExprType::Float64 => self.eval_float(expr),
            ExprType::MysqlDuration => self.eval_duration(expr),
            ExprType::MysqlDecimal => self.eval_decimal(expr),
            ExprType::In => self.eval_in(ctx, expr),
            ExprType::Plus => self.eval_arith(ctx, expr, Datum::checked_add),
            ExprType::Div => self.eval_arith(ctx, expr, Datum::checked_div),
            ExprType::Minus => self.eval_arith(ctx, expr, Datum::checked_minus),
            ExprType::Mul => self.eval_arith(ctx, expr, Datum::checked_mul),
            ExprType::IntDiv => self.eval_arith(ctx, expr, Datum::checked_int_div),
            ExprType::Mod => self.eval_arith(ctx, expr, Datum::checked_rem),
            ExprType::Case => self.eval_case_when(ctx, expr),
            ExprType::If => self.eval_if(ctx, expr),
            ExprType::Coalesce => self.eval_coalesce(ctx, expr),
            ExprType::IfNull => self.eval_if_null(ctx, expr),
            ExprType::IsNull => self.eval_is_null(ctx, expr),
            ExprType::NullIf => self.eval_null_if(ctx, expr),
            ExprType::JsonUnquote => self.eval_json_unquote(ctx, expr),
            _ => Ok(Datum::Null),
        }
    }

    fn eval_int(&self, expr: &Expr) -> Result<Datum> {
        let i = try!(expr.get_val().decode_i64());
        Ok(Datum::I64(i))
    }

    fn eval_uint(&self, expr: &Expr) -> Result<Datum> {
        let u = try!(expr.get_val().decode_u64());
        Ok(Datum::U64(u))
    }

    fn eval_float(&self, expr: &Expr) -> Result<Datum> {
        let f = try!(expr.get_val().decode_f64());
        Ok(Datum::F64(f))
    }

    fn eval_duration(&self, expr: &Expr) -> Result<Datum> {
        let n = try!(expr.get_val().decode_i64());
        let dur = try!(Duration::from_nanos(n, MAX_FSP));
        Ok(Datum::Dur(dur))
    }

    fn eval_decimal(&self, expr: &Expr) -> Result<Datum> {
        let d = try!(expr.get_val().decode_decimal());
        Ok(Datum::Dec(d))
    }

    fn eval_column_ref(&self, expr: &Expr) -> Result<Datum> {
        let i = try!(expr.get_val().decode_i64());
        self.row.get(&i).cloned().ok_or_else(|| Error::Eval(format!("column {} not found", i)))
    }

    fn eval_lt(&mut self, ctx: &EvalContext, expr: &Expr) -> Result<Datum> {
        let cmp = try!(self.cmp_children(ctx, expr));
        Ok(cmp.map(|c| c < Ordering::Equal).into())
    }

    fn eval_le(&mut self, ctx: &EvalContext, expr: &Expr) -> Result<Datum> {
        let cmp = try!(self.cmp_children(ctx, expr));
        Ok(cmp.map(|c| c <= Ordering::Equal).into())
    }

    fn eval_eq(&mut self, ctx: &EvalContext, expr: &Expr) -> Result<Datum> {
        let cmp = try!(self.cmp_children(ctx, expr));
        Ok(cmp.map(|c| c == Ordering::Equal).into())
    }

    fn eval_ne(&mut self, ctx: &EvalContext, expr: &Expr) -> Result<Datum> {
        let cmp = try!(self.cmp_children(ctx, expr));
        Ok(cmp.map(|c| c != Ordering::Equal).into())
    }

    fn eval_ge(&mut self, ctx: &EvalContext, expr: &Expr) -> Result<Datum> {
        let cmp = try!(self.cmp_children(ctx, expr));
        Ok(cmp.map(|c| c >= Ordering::Equal).into())
    }

    fn eval_gt(&mut self, ctx: &EvalContext, expr: &Expr) -> Result<Datum> {
        let cmp = try!(self.cmp_children(ctx, expr));
        Ok(cmp.map(|c| c > Ordering::Equal).into())
    }

    fn eval_null_eq(&mut self, ctx: &EvalContext, expr: &Expr) -> Result<Datum> {
        let (left, right) = try!(self.eval_two_children(ctx, expr));
        let cmp = try!(left.cmp(ctx, &right));
        Ok((cmp == Ordering::Equal).into())
    }

    fn cmp_children(&mut self, ctx: &EvalContext, expr: &Expr) -> Result<Option<Ordering>> {
        let (left, right) = try!(self.eval_two_children(ctx, expr));
        if left == Datum::Null || right == Datum::Null {
            return Ok(None);
        }
        left.cmp(ctx, &right).map(Some).map_err(From::from)
    }

    fn get_two_children<'a>(&mut self, expr: &'a Expr) -> Result<(&'a Expr, &'a Expr)> {
        let l = expr.get_children().len();
        if l != 2 {
            return Err(Error::Expr(format!("{:?} need 2 operands but got {}", expr.get_tp(), l)));
        }
        let children = expr.get_children();
        Ok((&children[0], &children[1]))
    }

    fn eval_one_child(&mut self, ctx: &EvalContext, expr: &Expr) -> Result<Datum> {
        let children = expr.get_children();
        if children.len() != 1 {
            return Err(Error::Expr(format!("{:?} need 1 operands but got {}",
                                           expr.get_tp(),
                                           children.len())));
        }
        let child = try!(self.eval(ctx, &children[0]));
        Ok(child)
    }

    fn eval_two_children(&mut self, ctx: &EvalContext, expr: &Expr) -> Result<(Datum, Datum)> {
        let (left_expr, right_expr) = try!(self.get_two_children(expr));
        let left = try!(self.eval(ctx, left_expr));
        let right = try!(self.eval(ctx, right_expr));
        Ok((left, right))
    }

    fn eval_not(&mut self, ctx: &EvalContext, expr: &Expr) -> Result<Datum> {
        let children_cnt = expr.get_children().len();
        if children_cnt != 1 {
            return Err(Error::Expr(format!("expect 1 operand, got {}", children_cnt)));
        }
        let d = try!(self.eval(ctx, &expr.get_children()[0]));
        if d == Datum::Null {
            return Ok(Datum::Null);
        }
        let b = try!(d.into_bool(ctx));
        Ok((b.map(|v| !v)).into())
    }

    fn eval_like(&mut self, ctx: &EvalContext, expr: &Expr) -> Result<Datum> {
        let (target, pattern) = try!(self.eval_two_children(ctx, expr));
        if Datum::Null == target || Datum::Null == pattern {
            return Ok(Datum::Null);
        }
        let mut target_str = try!(target.into_string());
        let mut pattern_str = try!(pattern.into_string());
        if pattern_str.chars().any(|x| x.is_ascii() && x.is_alphabetic()) {
            target_str = target_str.to_ascii_lowercase();
            pattern_str = pattern_str.to_ascii_lowercase();
        }
        // for now, tidb ensures that pattern being pushed down must match ^%?[^\\_%]*%?$.
        let len = pattern_str.len();
        if pattern_str.starts_with('%') {
            if pattern_str[1..].ends_with('%') {
                Ok(target_str.contains(&pattern_str[1..len - 1]).into())
            } else {
                Ok(target_str.ends_with(&pattern_str[1..]).into())
            }
        } else if pattern_str.ends_with('%') {
            Ok(target_str.starts_with(&pattern_str[..len - 1]).into())
        } else {
            Ok(target_str.eq(&pattern_str).into())
        }
    }

    fn eval_in(&mut self, ctx: &EvalContext, expr: &Expr) -> Result<Datum> {
        if expr.get_children().len() != 2 {
            return Err(Error::Expr(format!("IN need 2 operand, got {}",
                                           expr.get_children().len())));
        }
        let children = expr.get_children();
        let target = try!(self.eval(ctx, &children[0]));
        if let Datum::Null = target {
            return Ok(target);
        }
        let value_list_expr = &children[1];
        if value_list_expr.get_tp() != ExprType::ValueList {
            return Err(Error::Expr("the second children should be value list type".to_owned()));
        }
        let decoded = try!(self.decode_value_list(value_list_expr));
        if try!(check_in(ctx, target, decoded)) {
            return Ok(true.into());
        }
        if decoded.first().map_or(false, |d| *d == Datum::Null) {
            return Ok(Datum::Null);
        }
        Ok(false.into())
    }

    fn decode_value_list(&mut self, value_list_expr: &Expr) -> Result<&Vec<Datum>> {
        let p = value_list_expr as *const Expr as isize;
        let decoded = match self.cached_value_list.entry(p) {
            HashMapEntry::Occupied(entry) => entry.into_mut(),
            HashMapEntry::Vacant(entry) => {
                let default = try!(value_list_expr.get_val().decode());
                entry.insert(default)
            }
        };
        Ok(decoded)
    }

    fn eval_arith<F>(&mut self, ctx: &EvalContext, expr: &Expr, f: F) -> Result<Datum>
        where F: FnOnce(Datum, &EvalContext, Datum) -> codec::Result<Datum>
    {
        let (left, right) = try!(self.eval_two_children(ctx, expr));
        eval_arith(ctx, left, right, f)
    }

    fn eval_case_when(&mut self, ctx: &EvalContext, expr: &Expr) -> Result<Datum> {
        for chunk in expr.get_children().chunks(2) {
            let res = try!(self.eval(ctx, &chunk[0]));
            if chunk.len() == 1 {
                // else statement
                return Ok(res);
            }
            if !try!(res.into_bool(ctx)).unwrap_or(false) {
                continue;
            }
            return self.eval(ctx, &chunk[1]).map_err(From::from);
        }
        Ok(Datum::Null)
    }

    fn eval_if(&mut self, ctx: &EvalContext, expr: &Expr) -> Result<Datum> {
        let children = expr.get_children();
        if children.len() != 3 {
            return Err(Error::Expr(format!("expect 3 operands, got {}", children.len())));
        }
        let cond = try!(self.eval(ctx, &children[0]));
        let d = match try!(cond.into_bool(ctx)) {
            Some(true) => try!(self.eval(ctx, &children[1])),
            _ => try!(self.eval(ctx, &children[2])),
        };
        Ok(d)
    }

    fn eval_coalesce(&mut self, ctx: &EvalContext, expr: &Expr) -> Result<Datum> {
        for child in expr.get_children() {
            match try!(self.eval(ctx, child)) {
                Datum::Null => {}
                res => return Ok(res),
            }
        }
        Ok(Datum::Null)
    }

    fn eval_if_null(&mut self, ctx: &EvalContext, expr: &Expr) -> Result<Datum> {
        let children = expr.get_children();
        if children.len() != 2 {
            return Err(Error::Expr(format!("expect 2 operands, got {}", children.len())));
        }
        let left = try!(self.eval(ctx, &children[0]));
        if left == Datum::Null {
            Ok(try!(self.eval(ctx, &children[1])))
        } else {
            Ok(left)
        }
    }

    fn eval_is_null(&mut self, ctx: &EvalContext, expr: &Expr) -> Result<Datum> {
        let children = expr.get_children();
        if children.len() != 1 {
            return Err(Error::Expr(format!("expect 1 operand, got {}", children.len())));
        }
        let d = try!(self.eval(ctx, &children[0]));
        Ok((d == Datum::Null).into())
    }

    fn eval_null_if(&mut self, ctx: &EvalContext, expr: &Expr) -> Result<Datum> {
        let (left, right) = try!(self.eval_two_children(ctx, expr));
        if left == Datum::Null || right == Datum::Null {
            return Ok(left);
        }
        if let Ordering::Equal = try!(left.cmp(ctx, &right)) {
            Ok(Datum::Null)
        } else {
            Ok(left)
        }
    }

    fn eval_json_unquote(&mut self, ctx: &EvalContext, expr: &Expr) -> Result<Datum> {
        let child = try!(self.eval_one_child(ctx, expr));
        if child == Datum::Null {
            return Ok(Datum::Null);
        }
        // here Datum::Byte(bs) should be converted into Json::String(bs)
        // select JSON_UNQUOTE('{"a":   "b"}');
        // +------------------------------+
        // | JSON_UNQUOTE('{"a":   "b"}') |
        // +------------------------------+
        // | {"a":   "b"}                 |
        // +------------------------------+
        let json = try!(child.into_json());
        let unquote_data = try!(json.unquote());
        Ok(Datum::Bytes(unquote_data.into_bytes()))
    }

    fn eval_logic<F>(&mut self,
                     ctx: &EvalContext,
                     expr: &Expr,
                     break_res: Option<bool>,
                     logic_func: F)
                     -> Result<Datum>
        where F: FnOnce(Option<bool>, Option<bool>) -> Datum
    {
        let (left_expr, right_expr) = try!(self.get_two_children(expr));
        let left_datum = try!(self.eval(ctx, left_expr));
        let left = try!(left_datum.into_bool(ctx));
        if left == break_res {
            return Ok(left.into());
        }
        let right_datum = try!(self.eval(ctx, right_expr));
        let right = try!(right_datum.into_bool(ctx));
        if right == break_res {
            return Ok(right.into());
        }
        Ok(logic_func(left, right))
    }
}

// lhs and rhs can't be Some(false)
#[inline]
fn eval_and(lhs: Option<bool>, rhs: Option<bool>) -> Datum {
    match (lhs, rhs) {
        (Some(true), Some(true)) => true.into(),
        _ => Datum::Null,
    }
}

// lhs and rhs can't be Some(true)
#[inline]
fn eval_or(lhs: Option<bool>, rhs: Option<bool>) -> Datum {
    match (lhs, rhs) {
        (Some(false), Some(false)) => false.into(),
        _ => Datum::Null,
    }
}

#[inline]
pub fn eval_arith<F>(ctx: &EvalContext, left: Datum, right: Datum, f: F) -> Result<Datum>
    where F: FnOnce(Datum, &EvalContext, Datum) -> codec::Result<Datum>
{
    let left = try!(left.into_arith(ctx));
    let right = try!(right.into_arith(ctx));

    let (left, right) = try!(Datum::coerce(left, right));
    if left == Datum::Null || right == Datum::Null {
        return Ok(Datum::Null);
    }

    f(left, ctx, right).map_err(From::from)
}

/// Check if `target` is in `value_list`.
fn check_in(ctx: &EvalContext, target: Datum, value_list: &[Datum]) -> Result<bool> {
    let mut err = None;
    let pos = value_list.binary_search_by(|d| {
        match d.cmp(ctx, &target) {
            Ok(ord) => ord,
            Err(e) => {
                err = Some(e);
                Ordering::Less
            }
        }
    });
    if let Some(e) = err {
        return Err(e.into());
    }
    Ok(pos.is_ok())
}

#[cfg(test)]
mod test {
    use super::*;
    use util::codec::number::{self, NumberEncoder};
    use util::codec::{Datum, datum};
    use util::codec::mysql::{self, MAX_FSP, Decimal, Duration, DecimalEncoder};

    use std::i32;

    use tipb::expression::{Expr, ExprType};
    use tipb::select::SelectRequest;
    use protobuf::RepeatedField;

    fn datum_expr(datum: Datum) -> Expr {
        let mut expr = Expr::new();
        match datum {
            Datum::I64(i) => {
                expr.set_tp(ExprType::Int64);
                let mut buf = Vec::with_capacity(number::I64_SIZE);
                buf.encode_i64(i).unwrap();
                expr.set_val(buf);
            }
            Datum::U64(u) => {
                expr.set_tp(ExprType::Uint64);
                let mut buf = Vec::with_capacity(number::U64_SIZE);
                buf.encode_u64(u).unwrap();
                expr.set_val(buf);
            }
            Datum::Bytes(bs) => {
                expr.set_tp(ExprType::Bytes);
                expr.set_val(bs);
            }
            Datum::F64(f) => {
                expr.set_tp(ExprType::Float64);
                let mut buf = Vec::with_capacity(number::F64_SIZE);
                buf.encode_f64(f).unwrap();
                expr.set_val(buf);
            }
            Datum::Dur(d) => {
                expr.set_tp(ExprType::MysqlDuration);
                let mut buf = Vec::with_capacity(number::I64_SIZE);
                buf.encode_i64(d.to_nanos()).unwrap();
                expr.set_val(buf);
            }
            Datum::Dec(d) => {
                expr.set_tp(ExprType::MysqlDecimal);
                let (prec, frac) = d.prec_and_frac();
                let mut buf = Vec::with_capacity(mysql::dec_encoded_len(&[prec, frac]).unwrap());
                buf.encode_decimal(&d, prec, frac).unwrap();
                expr.set_val(buf);
            }
            Datum::Null => expr.set_tp(ExprType::Null),
            d => panic!("unsupport datum: {:?}", d),
        };
        expr
    }

    fn col_expr(col_id: i64) -> Expr {
        let mut expr = Expr::new();
        expr.set_tp(ExprType::ColumnRef);
        let mut buf = Vec::with_capacity(8);
        buf.encode_i64(col_id).unwrap();
        expr.set_val(buf);
        expr
    }

    fn bin_expr(left: Datum, right: Datum, tp: ExprType) -> Expr {
        build_expr(vec![left, right], tp)
    }

    fn build_expr(children: Vec<Datum>, tp: ExprType) -> Expr {
        let children_expr = children.into_iter().map(datum_expr).collect();
        build_expr_r(children_expr, tp)
    }

    fn build_expr_r(children_expr: Vec<Expr>, tp: ExprType) -> Expr {
        let mut expr = Expr::new();
        expr.set_tp(tp);
        expr.set_children(RepeatedField::from_vec(children_expr));
        expr
    }

    fn case_when(datums: Vec<Datum>) -> Expr {
        build_expr(datums, ExprType::Case)
    }

    fn coalesce(datums: Vec<Datum>) -> Expr {
        build_expr(datums, ExprType::Coalesce)
    }

    fn not_expr(value: Datum) -> Expr {
        let mut expr = Expr::new();
        expr.set_tp(ExprType::Not);
        expr.mut_children().push(datum_expr(value));
        expr
    }

    fn like_expr(target: &'static str, pattern: &'static str) -> Expr {
        let target_expr = datum_expr(Datum::Bytes(target.as_bytes().to_vec()));
        let pattern_expr = datum_expr(Datum::Bytes(pattern.as_bytes().to_vec()));
        let mut expr = Expr::new();
        expr.set_tp(ExprType::Like);
        expr.mut_children().push(target_expr);
        expr.mut_children().push(pattern_expr);
        expr
    }

    macro_rules! test_eval {
        ($tag:ident, $cases:expr) => {
            #[test]
            fn $tag() {
                let cases = $cases;

                let mut xevaluator = Evaluator::default();
                xevaluator.row.insert(1, Datum::I64(100));
                for (expr, exp) in cases {
                    let res = xevaluator.eval(&Default::default(), &expr);
                    if res.is_err() {
                        panic!("failed to eval {:?}: {:?}", expr, res);
                    }
                    let res = res.unwrap();
                    if res != exp {
                        panic!("failed to eval {:?} expect {:?}, got {:?}", expr, exp, res);
                    }
                }
            }
        };
    }

    macro_rules! test_eval_err {
        ($tag:ident, $cases:expr) => {
            #[test]
            fn $tag() {
                let cases = $cases;

                let mut xevaluator = Evaluator::default();
                xevaluator.row.insert(1, Datum::I64(100));
                for expr in cases {
                    let res = xevaluator.eval(&Default::default(), &expr);
                    assert!(res.is_err());
                }
            }
        };
    }


    test_eval!(test_eval_datum_col,
               vec![
        (datum_expr(Datum::F64(1.1)), Datum::F64(1.1)),
        (datum_expr(Datum::I64(1)), Datum::I64(1)),
        (datum_expr(Datum::U64(1)), Datum::U64(1)),
        (datum_expr(b"abc".as_ref().into()), b"abc".as_ref().into()),
        (datum_expr(Datum::Null), Datum::Null),
        (datum_expr(Duration::parse(b"01:00:00", 0).unwrap().into()),
            Duration::from_nanos(3600 * 1_000_000_000, MAX_FSP).unwrap().into()),
        (datum_expr(Datum::Dec("1.1".parse().unwrap())),
            Datum::Dec(Decimal::from_f64(1.1).unwrap())),
        (col_expr(1), Datum::I64(100)),
    ]);

    test_eval!(test_eval_cmp,
               vec![
        (bin_expr(Duration::parse(b"11:00:00", 0).unwrap().into(),
            Duration::parse(b"00:00:00", 0).unwrap().into(), ExprType::LT), Datum::I64(0)),
        (bin_expr(Duration::parse(b"11:00:00.233", 2).unwrap().into(),
            Duration::parse(b"11:00:00.233", 0).unwrap().into(), ExprType::EQ), Datum::I64(0)),
        (bin_expr(Duration::parse(b"11:00:00.233", 3).unwrap().into(),
            Duration::parse(b"11:00:00.233", 4).unwrap().into(), ExprType::EQ), Datum::I64(1)),
        (bin_expr(Datum::Dec(Decimal::from_f64(2.0).unwrap()), Datum::Dec(2u64.into()),
            ExprType::EQ), Datum::I64(1)),
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
    ]);

    test_eval!(test_eval_logic,
               vec![
        (bin_expr(Datum::I64(0), Datum::I64(1), ExprType::And), Datum::I64(0)),
        (bin_expr(Datum::I64(1), Datum::I64(1), ExprType::And), Datum::I64(1)),
        (bin_expr(Datum::I64(1), Datum::Null, ExprType::And), Datum::Null),
        (bin_expr(Datum::Null, Datum::I64(1), ExprType::And), Datum::Null),
        (bin_expr(Datum::I64(0), Datum::Null, ExprType::And), Datum::I64(0)),
        (bin_expr(Datum::Null, Datum::I64(0), ExprType::And), Datum::I64(0)),
        (bin_expr(Datum::Dec(Decimal::from_f64(2.0).unwrap()), Datum::Dec(0u64.into()),
            ExprType::And), Datum::I64(0)),
        (bin_expr(Datum::Null, Datum::Null, ExprType::And), Datum::Null),
        (bin_expr(Datum::I64(0), Datum::I64(0), ExprType::Or), Datum::I64(0)),
        (bin_expr(Datum::I64(0), Datum::I64(1), ExprType::Or), Datum::I64(1)),
        (bin_expr(Datum::Dec(Decimal::from_f64(2.0).unwrap()), Datum::Dec(0u64.into()),
            ExprType::Or), Datum::I64(1)),
        (bin_expr(Datum::I64(1), Datum::Null, ExprType::Or), Datum::I64(1)),
        (bin_expr(Datum::Null, Datum::I64(1), ExprType::Or), Datum::I64(1)),
        (bin_expr(Datum::Null, Datum::Null, ExprType::Or), Datum::Null),
        (bin_expr(Datum::Null, Datum::I64(0), ExprType::Or), Datum::Null),
        (bin_expr(Datum::I64(0), Datum::Null, ExprType::Or), Datum::Null),
        (build_expr_r(vec![bin_expr(Datum::I64(1), Datum::I64(1), ExprType::EQ),
            bin_expr(Datum::I64(1), Datum::I64(1), ExprType::EQ)], ExprType::And), Datum::I64(1)),
        (not_expr(Datum::I64(1)), Datum::I64(0)),
        (not_expr(Datum::I64(0)), Datum::I64(1)),
        (not_expr(Datum::Null), Datum::Null),
    ]);

    test_eval!(test_eval_like,
               vec![
        (like_expr("a", ""), Datum::I64(0)),
        (like_expr("a", "a"), Datum::I64(1)),
        (like_expr("a", "b"), Datum::I64(0)),
        (like_expr("aAb", "AaB"), Datum::I64(1)),
        (like_expr("a", "%"), Datum::I64(1)),
        (like_expr("aAD", "%d"), Datum::I64(1)),
        (like_expr("aAeD", "%e"), Datum::I64(0)),
        (like_expr("aAb", "Aa%"), Datum::I64(1)),
        (like_expr("abAb", "Aa%"), Datum::I64(0)),
        (like_expr("aAcb", "%C%"), Datum::I64(1)),
        (like_expr("aAb", "%C%"), Datum::I64(0)),
        (bin_expr(Datum::I64(1), Datum::I64(1), ExprType::Like), Datum::I64(1)),
        (bin_expr(Datum::U64(1), Datum::U64(1), ExprType::Like), Datum::I64(1)),
        (bin_expr(Datum::F64(1.0), Datum::F64(1.0), ExprType::Like), Datum::I64(1)),
        (bin_expr(Duration::parse(b"11:00:00", 0).unwrap().into(),
         Duration::parse(b"11:00:00", 0).unwrap().into(), ExprType::Like), Datum::I64(1)),
    ]);

    // TODO: test time
    test_eval!(test_eval_plus,
               vec![
		(bin_expr(Datum::I64(1), Datum::I64(1), ExprType::Plus), Datum::I64(2)),
        (bin_expr(Datum::I64(1), Datum::U64(1), ExprType::Plus), Datum::U64(2)),
        (bin_expr(Datum::I64(1), Datum::Bytes(b"1".to_vec()), ExprType::Plus), Datum::F64(2.0)),
        (bin_expr(Datum::I64(1), Datum::Bytes(b"-1".to_vec()), ExprType::Plus), Datum::F64(0.0)),
        (bin_expr(Datum::Null, Datum::Null, ExprType::Plus), Datum::Null),
        (bin_expr(Datum::I64(-1), Datum::Null, ExprType::Plus), Datum::Null),
        (bin_expr(Datum::Null, Datum::I64(-1), ExprType::Plus), Datum::Null),
        (bin_expr(Datum::I64(-1), Datum::U64(1), ExprType::Plus), Datum::U64(0)),
        (bin_expr(Datum::I64(i64::min_value()), Datum::U64(i64::max_value() as u64 + 1),
         ExprType::Plus), Datum::U64(0)),
        (bin_expr(Datum::F64(2.0), Datum::I64(-1), ExprType::Plus), Datum::F64(1.0)),
        (bin_expr(Datum::F64(2.0), Datum::U64(1), ExprType::Plus), Datum::F64(3.0)),
        (bin_expr(Datum::Dec("3.3".parse().unwrap()), Datum::I64(-1), ExprType::Plus),
         Datum::Dec("2.3".parse().unwrap())),
        (bin_expr(Datum::F64(2.0), Datum::Dec("3.3".parse().unwrap()), ExprType::Plus),
         Datum::F64(5.3)),
        (bin_expr(Datum::Dec("3.3".parse().unwrap()), b"2.0".as_ref().into(), ExprType::Plus),
         Datum::F64(5.3)),
        (bin_expr(Datum::I64(2), Datum::Dur(Duration::parse(b"21 00:02", 0).unwrap()),
         ExprType::Plus), Datum::Dec(Decimal::from_f64(5040202.000000).unwrap())),
        (bin_expr(Datum::I64(2), Datum::Dur(Duration::parse(b"21 00:02:00.321", 2).unwrap()),
         ExprType::Plus), Datum::Dec(Decimal::from_f64(5040202.32).unwrap())),
    ]);

    test_eval!(test_eval_div,
               vec![
		(bin_expr(Datum::I64(1), Datum::I64(1), ExprType::Div), Datum::Dec(1.into())),
        (bin_expr(Datum::I64(1), Datum::U64(1), ExprType::Div), Datum::Dec(1.into())),
        (bin_expr(Datum::I64(1), Datum::Bytes(b"1".to_vec()), ExprType::Div), Datum::F64(1f64)),
        (bin_expr(Datum::I64(1), Datum::Bytes(b"-1".to_vec()), ExprType::Div), Datum::F64(-1f64)),
        (bin_expr(Datum::Null, Datum::Null, ExprType::Div), Datum::Null),
        (bin_expr(Datum::I64(-1), Datum::Null, ExprType::Div), Datum::Null),
        (bin_expr(Datum::Null, Datum::I64(-1), ExprType::Div), Datum::Null),
        (bin_expr(Datum::I64(-1), Datum::U64(1), ExprType::Div), Datum::Dec((-1).into())),
        (bin_expr(Datum::I64(i64::min_value()), Datum::U64(i64::max_value() as u64 + 1),
         ExprType::Div), Datum::Dec((-1).into())),
        (bin_expr(Datum::F64(2.0), Datum::I64(-1), ExprType::Div), Datum::F64(-2.0)),
        (bin_expr(Datum::F64(2.0), Datum::U64(1), ExprType::Div), Datum::F64(2.0)),
        (bin_expr(Datum::F64(2.0), Datum::Dec("0.3".parse().unwrap()), ExprType::Div),
         Datum::F64(6.666666666666667)),
        (bin_expr(Datum::F64(2.0), Datum::Dur(Duration::parse(b"1 00:02", 0).unwrap()),
         ExprType::Div), Datum::F64(0.00000832639467110741)),
        (bin_expr(Datum::Dec("3.3".parse().unwrap()), Datum::I64(-1), ExprType::Div),
         Datum::Dec("-3.3".parse().unwrap())),
        (bin_expr(Datum::I64(2000), Datum::Dur(Duration::parse(b"1 00:02", 0).unwrap()),
         ExprType::Div), Datum::Dec("0.008326394671107410".parse().unwrap())),
        (bin_expr(Datum::I64(2000), Datum::Dur(Duration::parse(b"00:02:00.321", 2).unwrap()),
         ExprType::Div), Datum::Dec("9.984025559105431309".parse().unwrap())),
    ]);

    test_eval!(test_eval_minus,
               vec![
        (bin_expr(Datum::I64(1), Datum::I64(1), ExprType::Minus), Datum::I64(0)),
        (bin_expr(Datum::I64(1), Datum::U64(1), ExprType::Minus), Datum::U64(0)),
        (bin_expr(Datum::U64(1), Datum::I64(-1), ExprType::Minus), Datum::U64(2)),
        (bin_expr(Datum::U64(1), Datum::U64(1), ExprType::Minus), Datum::U64(0)),
        (bin_expr(Datum::I64(1), Datum::Bytes(b"1".to_vec()), ExprType::Minus), Datum::F64(0.0)),
        (bin_expr(Datum::I64(1), Datum::Bytes(b"-1".to_vec()), ExprType::Minus), Datum::F64(2.0)),
        (bin_expr(Datum::Null, Datum::Null, ExprType::Minus), Datum::Null),
        (bin_expr(Datum::I64(-1), Datum::Null, ExprType::Minus), Datum::Null),
        (bin_expr(Datum::U64(1), Datum::I64(i64::min_value()), ExprType::Minus),
         Datum::U64(i64::max_value() as u64 + 2)),
        (bin_expr(Datum::Null, Datum::I64(-1), ExprType::Minus), Datum::Null),
        (bin_expr(Datum::F64(2.0), Datum::I64(-1), ExprType::Minus), Datum::F64(3.0)),
        (bin_expr(Datum::F64(2.0), Datum::U64(1), ExprType::Minus), Datum::F64(1.0)),
        (bin_expr(Datum::F64(2.0), Datum::Dec("0.3".parse().unwrap()), ExprType::Minus),
         Datum::F64(1.7)),
        (bin_expr(Datum::F64(2.0), Datum::Dur(Duration::parse(b"1 00:02", 0).unwrap()),
         ExprType::Minus), Datum::F64(-240198.0)),
        (bin_expr(Datum::Dec("3.3".parse().unwrap()), Datum::I64(-1), ExprType::Minus),
         Datum::Dec("4.3".parse().unwrap())),
        (bin_expr(Datum::I64(2000), Datum::Dur(Duration::parse(b"1 00:02", 0).unwrap()),
         ExprType::Minus), Datum::Dec("-238200".parse().unwrap())),
        (bin_expr(Datum::I64(2000), Datum::Dur(Duration::parse(b"00:02:00.321", 2).unwrap()),
         ExprType::Minus), Datum::Dec("1799.680000".parse().unwrap())),
    ]);

    test_eval!(test_eval_mul,
               vec![
		(bin_expr(Datum::I64(1), Datum::I64(1), ExprType::Mul), Datum::I64(1)),
        (bin_expr(Datum::I64(1), Datum::U64(1), ExprType::Mul), Datum::U64(1)),
        (bin_expr(Datum::I64(1), Datum::Bytes(b"1".to_vec()), ExprType::Mul), Datum::F64(1f64)),
        (bin_expr(Datum::I64(1), Datum::Bytes(b"-1".to_vec()), ExprType::Mul), Datum::F64(-1f64)),
        (bin_expr(Datum::Null, Datum::Null, ExprType::Mul), Datum::Null),
        (bin_expr(Datum::I64(-1), Datum::Null, ExprType::Mul), Datum::Null),
        (bin_expr(Datum::Null, Datum::I64(-1), ExprType::Mul), Datum::Null),
        (bin_expr(Datum::I64(-1), Datum::I64(1), ExprType::Mul), Datum::I64((-1).into())),
        (bin_expr(Datum::F64(2.0), Datum::I64(-1), ExprType::Mul), Datum::F64(-2.0)),
        (bin_expr(Datum::F64(2.0), Datum::U64(1), ExprType::Mul), Datum::F64(2.0)),
        (bin_expr(Datum::F64(2.0), Datum::Dec("0.3".parse().unwrap()), ExprType::Mul),
         Datum::F64(0.6)),
        (bin_expr(Datum::F64(2.0), Datum::Dur(Duration::parse(b"1 00:02", 0).unwrap()),
         ExprType::Mul), Datum::F64(480400.0)),
        (bin_expr(Datum::Dec("3.3".parse().unwrap()), Datum::I64(-1), ExprType::Mul),
         Datum::Dec("-3.3".parse().unwrap())),
        (bin_expr(Datum::I64(2000), Datum::Dur(Duration::parse(b"1 00:02", 0).unwrap()),
         ExprType::Mul), Datum::Dec("480400000".parse().unwrap())),
        (bin_expr(Datum::I64(2000), Datum::Dur(Duration::parse(b"00:02:00.321", 2).unwrap()),
         ExprType::Mul), Datum::Dec("400640".parse().unwrap())),
    ]);

    test_eval!(test_eval_int_div,
               vec![
		(bin_expr(Datum::I64(1), Datum::I64(1), ExprType::IntDiv), Datum::I64(1)),
        (bin_expr(Datum::I64(1), Datum::I64(0), ExprType::IntDiv), Datum::Null),
        (bin_expr(Datum::I64(1), Datum::U64(1), ExprType::IntDiv), Datum::U64(1)),
        (bin_expr(Datum::I64(1), Datum::U64(0), ExprType::IntDiv), Datum::Null),
        (bin_expr(Datum::I64(1), Datum::Bytes(b"1".to_vec()), ExprType::IntDiv), Datum::I64(1)),
        (bin_expr(Datum::I64(1), Datum::Bytes(b"-1".to_vec()), ExprType::IntDiv), Datum::I64(-1)),
        (bin_expr(Datum::I64(1), Datum::Bytes(b"0".to_vec()), ExprType::IntDiv), Datum::Null),
        (bin_expr(Datum::Null, Datum::Null, ExprType::IntDiv), Datum::Null),
        (bin_expr(Datum::I64(-1), Datum::Null, ExprType::IntDiv), Datum::Null),
        (bin_expr(Datum::Null, Datum::I64(-1), ExprType::IntDiv), Datum::Null),
        (bin_expr(Datum::I64(i64::min_value()), Datum::U64(i64::max_value() as u64 + 2),
         ExprType::IntDiv), Datum::U64(0)),
        (bin_expr(Datum::F64(2.0), Datum::I64(-1), ExprType::IntDiv), Datum::I64(-2)),
        (bin_expr(Datum::F64(2.0), Datum::U64(1), ExprType::IntDiv), Datum::I64(2)),
        (bin_expr(Datum::I64(2), Datum::Dec("0.3".parse().unwrap()), ExprType::IntDiv),
         Datum::I64(6)),
        (bin_expr(Datum::F64(2.0), Datum::Dur(Duration::parse(b"1 00:02", 0).unwrap()),
         ExprType::IntDiv), Datum::I64(0)),
        (bin_expr(Datum::Dec("3.3".parse().unwrap()), Datum::I64(-1), ExprType::IntDiv),
         Datum::I64(-3)),
        (bin_expr(Datum::I64(2000), Datum::Dur(Duration::parse(b"1 00:02", 0).unwrap()),
         ExprType::IntDiv), Datum::I64(0)),
        (bin_expr(Datum::I64(2000), Datum::Dur(Duration::parse(b"00:02:00.321", 2).unwrap()),
         ExprType::IntDiv), Datum::I64(9)),
    ]);

    test_eval!(test_eval_rem,
               vec![
        (bin_expr(Datum::I64(3), Datum::I64(1), ExprType::Mod), Datum::I64(0)),
		(bin_expr(Datum::I64(3), Datum::I64(2), ExprType::Mod), Datum::I64(1)),
        (bin_expr(Datum::I64(1), Datum::I64(0), ExprType::Mod), Datum::Null),
        (bin_expr(Datum::I64(3), Datum::U64(2), ExprType::Mod), Datum::I64(1)),
        (bin_expr(Datum::I64(1), Datum::U64(0), ExprType::Mod), Datum::Null),
        (bin_expr(Datum::I64(3), Datum::Bytes(b"2".to_vec()), ExprType::Mod), Datum::F64(1.0)),
        (bin_expr(Datum::I64(3), Datum::Bytes(b"-2".to_vec()), ExprType::Mod), Datum::F64(1.0)),
        (bin_expr(Datum::Null, Datum::Null, ExprType::Mod), Datum::Null),
        (bin_expr(Datum::I64(-1), Datum::Null, ExprType::Mod), Datum::Null),
        (bin_expr(Datum::Null, Datum::I64(-1), ExprType::Mod), Datum::Null),
        (bin_expr(Datum::I64(-1), Datum::U64(2), ExprType::Mod), Datum::I64((-1).into())),
        (bin_expr(Datum::I64(i64::min_value()), Datum::U64(i64::max_value() as u64),
         ExprType::Mod), Datum::I64(-1)),
        (bin_expr(Datum::U64(i64::max_value() as u64), Datum::I64(i64::min_value()),
         ExprType::Mod), Datum::U64(i64::max_value() as u64)),
        (bin_expr(Datum::F64(3.2), Datum::I64(2), ExprType::Mod), Datum::F64(1.2000000000000002)),
        (bin_expr(Datum::F64(-3.2), Datum::I64(2), ExprType::Mod), Datum::F64(-1.2000000000000002)),
        (bin_expr(Datum::F64(2.0), Datum::Dec("0.3".parse().unwrap()), ExprType::Mod),
         Datum::F64(0.20000000000000007)),
        (bin_expr(Datum::F64(2.0), Datum::Dur(Duration::parse(b"1 00:02", 0).unwrap()),
         ExprType::Mod), Datum::F64(2.0)),
        (bin_expr(Datum::Dec("3.3".parse().unwrap()), Datum::I64(-1), ExprType::Mod),
         Datum::Dec("0.3".parse().unwrap())),
        (bin_expr(Datum::I64(2000), Datum::Dur(Duration::parse(b"1 00:02", 0).unwrap()),
         ExprType::Mod), Datum::Dec("2000".parse().unwrap())),
        (bin_expr(Datum::I64(2000), Datum::Dur(Duration::parse(b"00:02:00.321", 2).unwrap()),
         ExprType::Mod), Datum::Dec("197.12".parse().unwrap())),
    ]);

    test_eval!(test_eval_case_when,
               vec![
        (case_when(vec![
            Datum::I64(0), b"case1".as_ref().into(),
            Datum::I64(1), b"case2".as_ref().into(),
            Datum::I64(1), b"case3".as_ref().into()
        ]), b"case2".as_ref().into()),
        (case_when(vec![
            Datum::I64(0), b"case1".as_ref().into(),
            Datum::I64(0), b"case2".as_ref().into(),
            Datum::I64(0), b"case3".as_ref().into(),
            b"else".as_ref().into()
        ]), b"else".as_ref().into()),
        (case_when(vec![
            Datum::I64(0), b"case1".as_ref().into(),
            Datum::I64(0), b"case2".as_ref().into(),
            Datum::I64(0), b"case3".as_ref().into()
        ]), Datum::Null),
        (build_expr_r(vec![
            case_when(vec![
                Datum::I64(0), Datum::I64(0),
                Datum::I64(1), Datum::I64(1)
            ]), datum_expr(b"nested case when".as_ref().into()),
            datum_expr(Datum::I64(0)), datum_expr(b"case1".as_ref().into()),
            datum_expr(Datum::I64(1)), datum_expr(b"case2".as_ref().into()),
            datum_expr(Datum::I64(1)), datum_expr(b"case3".as_ref().into()),
        ], ExprType::Case), b"nested case when".as_ref().into()),
        (case_when(vec![
            Datum::Null, b"case1".as_ref().into(),
            Datum::I64(0), b"case2".as_ref().into(),
            Datum::I64(1), b"case3".as_ref().into()
        ]), b"case3".as_ref().into()),
    ]);

    test_eval!(test_eval_if,
               vec![
                (build_expr(vec![true.into(), b"expr1".as_ref().into(), b"expr2".as_ref().into()],
                    ExprType::If), b"expr1".as_ref().into()),
                (build_expr(vec![false.into(), b"expr1".as_ref().into(), b"expr2".as_ref().into()],
                    ExprType::If), b"expr2".as_ref().into()),
                (build_expr(vec![Datum::Null, b"expr1".as_ref().into(), b"expr2".as_ref().into()],
                    ExprType::If), b"expr2".as_ref().into()),
                (build_expr(vec![true.into(), Datum::Null, b"expr2".as_ref().into()],
                    ExprType::If), Datum::Null),
                (build_expr(vec![false.into(), b"expr1".as_ref().into(), Datum::Null],
                    ExprType::If), Datum::Null),
                (build_expr_r(vec![
                    build_expr(vec![true.into(), Datum::Null, true.into()], ExprType::If),
                    build_expr(vec![
                            true.into(),
                            b"expr1".as_ref().into(),
                            b"expr2".as_ref().into()
                        ],ExprType::If),
                    build_expr(vec![
                            false.into(),
                            b"expr1".as_ref().into(),
                            b"expr2".as_ref().into()
                        ],ExprType::If),
                ], ExprType::If), b"expr2".as_ref().into()),
    ]);

    test_eval!(test_eval_coalesce,
               vec![
        (coalesce(vec![Datum::Null, Datum::Null, Datum::Null]), Datum::Null),
        (coalesce(vec![Datum::Null, b"not-null".as_ref().into(), Datum::Null]),
         b"not-null".as_ref().into()),
        (coalesce(vec![Datum::Null, b"not-null".as_ref().into(),
         b"not-null-2".as_ref().into(), Datum::Null]), b"not-null".as_ref().into()),
    ]);

    test_eval!(test_eval_if_null,
               vec![
            (build_expr(vec![Datum::Null, b"right".as_ref().into()], ExprType::IfNull),
                b"right".as_ref().into()),
            (build_expr(vec![b"left".as_ref().into(), b"right".as_ref().into()], ExprType::IfNull),
                b"left".as_ref().into()),
            (build_expr(vec![b"left".as_ref().into(), Datum::Null], ExprType::IfNull),
                b"left".as_ref().into()),
            (build_expr(vec![Datum::Null, Datum::Null], ExprType::IfNull),
                Datum::Null),
    ]);

    test_eval!(test_eval_is_null,
               vec![
        (build_expr(vec![b"abc".as_ref().into()], ExprType::IsNull), false.into()),
        (build_expr(vec![Datum::Null], ExprType::IsNull), true.into()),
        (build_expr(vec![Datum::I64(0)], ExprType::IsNull), false.into()),
    ]);

    test_eval!(test_eval_null_if,
               vec![
         (build_expr(vec![b"abc".as_ref().into(), b"abc".as_ref().into()], ExprType::NullIf),
            Datum::Null),
         (build_expr(vec![Datum::Null, Datum::Null], ExprType::NullIf),
            Datum::Null),
         (build_expr(vec![123i64.into(), 111i64.into()], ExprType::NullIf),
            123i64.into()),
         (build_expr(vec![123i64.into(), Datum::Null], ExprType::NullIf),
            123i64.into()),
    ]);

    fn in_expr(target: Datum, mut list: Vec<Datum>) -> Expr {
        let target_expr = datum_expr(target);
        list.sort_by(|l, r| l.cmp(&Default::default(), r).unwrap());
        let val = datum::encode_value(&list).unwrap();
        let mut list_expr = Expr::new();
        list_expr.set_tp(ExprType::ValueList);
        list_expr.set_val(val);
        let mut expr = Expr::new();
        expr.set_tp(ExprType::In);
        expr.mut_children().push(target_expr);
        expr.mut_children().push(list_expr);
        expr
    }

    #[test]
    fn test_context() {
        let mut req = SelectRequest::new();
        req.set_time_zone_offset(i32::MAX as i64 + 1);
        let ctx = EvalContext::new(&req);
        assert!(ctx.is_err());
        req.set_time_zone_offset(3600);
        EvalContext::new(&req).unwrap();
    }

    #[test]
    fn test_where_in() {
        let cases = vec![
            (in_expr(Datum::I64(1), vec![Datum::I64(1), Datum::I64(2)]), Datum::I64(1)),
            (in_expr(Datum::I64(1), vec![Datum::I64(2), Datum::Null]), Datum::Null),
            (in_expr(Datum::Null, vec![Datum::I64(1), Datum::Null]), Datum::Null),
            (in_expr(Datum::I64(2), vec![Datum::I64(1), Datum::Null]), Datum::Null),
            (in_expr(Datum::I64(2), vec![]), Datum::I64(0)),
            (in_expr(b"abc".as_ref().into(), vec![b"abc".as_ref().into(),
             b"ab".as_ref().into()]), Datum::I64(1)),
            (in_expr(b"abc".as_ref().into(), vec![b"aba".as_ref().into(),
             b"bab".as_ref().into()]), Datum::I64(0)),
        ];

        let mut eval = Evaluator::default();
        for (expr, expect_res) in cases {
            let res = eval.eval(&Default::default(), &expr);
            if res.is_err() {
                panic!("failed to execute {:?}: {:?}", expr, res);
            }
            let res = res.unwrap();
            if res != expect_res {
                panic!("wrong result {:?}, expect {:?} while executing {:?}",
                       res,
                       expect_res,
                       expr);
            }
        }
    }

    fn build_byte_datums_expr(data: &[&[u8]], tp: ExprType) -> Expr {
        let datums = data.into_iter().map(|item| Datum::Bytes(item.to_vec())).collect();
        build_expr(datums, tp)
    }

    test_eval!(test_eval_json_unquote,
               vec![
            (build_expr(vec![Datum::Null], ExprType::JsonUnquote),
                        Datum::Null),
            (build_byte_datums_expr(&[b"a"], ExprType::JsonUnquote),
                        Datum::Bytes(b"a".to_vec())),
            (build_byte_datums_expr(&[br#"\"3\""#], ExprType::JsonUnquote),
                        Datum::Bytes(br#""3""#.to_vec())),
            (build_byte_datums_expr(&[br#"{"a":  "b"}"#], ExprType::JsonUnquote),
                        Datum::Bytes(br#"{"a":  "b"}"#.to_vec())),
            (build_byte_datums_expr(&[br#"hello,\"quoted string\",world"#],
                                    ExprType::JsonUnquote),
                        Datum::Bytes(br#"hello,"quoted string",world"#.to_vec())),
    ]);

    test_eval_err!(test_eval_json_err,
                   vec![
          build_expr(vec![], ExprType::JsonUnquote),
          build_byte_datums_expr(&[br#"true"#, br#"444"#], ExprType::JsonUnquote),
     ]);

}
