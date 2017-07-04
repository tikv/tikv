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

use tipb::expression::{Expr, ExprType};
use util::codec::datum::Datum;
use util::codec::mysql::{Decimal, Time, Duration};
use super::{Evaluator, EvalContext, Result, Error};

const INT_TYPES: &[ExprType] = &[ExprType::Int64, ExprType::Uint64];

fn assert_expr_types(expr: &Expr, tps: &[ExprType]) -> Result<()> {
    if tps.into_iter().any(|t| expr.get_tp() == *t) {
        return Ok(());
    }
    Err(Error::Eval(format!("invalid expr type: {:?}, expect to be one of: {:?}",
                            expr.get_tp(),
                            tps)))
}

impl Evaluator {
    pub fn cast_int_as_int(&mut self, ctx: &EvalContext, expr: &Expr) -> Result<Datum> {
        let child = try!(self.get_one_child(expr));
        try!(assert_expr_types(child, INT_TYPES));
        if child.get_tp() == ExprType::Uint64 {
            let d = try!(self.eval(ctx, child));
            Ok(Datum::I64(d.i64()))
        } else {
            self.eval(ctx, child)
        }
    }

    pub fn cast_int_as_real(&mut self, ctx: &EvalContext, expr: &Expr) -> Result<Datum> {
        let child = try!(self.get_one_child(expr));
        try!(assert_expr_types(child, INT_TYPES));
        let d = try!(self.eval(ctx, child));
        Ok(Datum::F64(d.f64()))
    }

    pub fn cast_int_as_string(&mut self, ctx: &EvalContext, expr: &Expr) -> Result<Datum> {
        let child = try!(self.get_one_child(expr));
        try!(assert_expr_types(child, INT_TYPES));
        let d = try!(self.eval(ctx, child));
        let s = try!(d.into_string());
        Ok(Datum::Bytes(s.into_bytes()))
    }

    pub fn cast_int_as_decimal(&mut self, ctx: &EvalContext, expr: &Expr) -> Result<Datum> {
        let child = try!(self.get_one_child(expr));
        try!(assert_expr_types(child, INT_TYPES));
        let datum = try!(self.eval(ctx, child));
        let decimal = if child.get_tp() == ExprType::Int64 {
            Decimal::from(datum.i64())
        } else {
            Decimal::from(datum.u64())
        };
        Ok(Datum::Dec(decimal))
    }

    pub fn cast_int_as_time(&mut self, ctx: &EvalContext, expr: &Expr) -> Result<Datum> {
        let child = try!(self.get_one_child(expr));
        try!(assert_expr_types(child, INT_TYPES));
        let d = try!(self.eval(ctx, child));
        let s = try!(d.into_string());
        let t = try!(Time::parse_utc_datetime(&s, 0));
        Ok(Datum::Time(t))
    }

    pub fn cast_int_as_duration(&mut self, ctx: &EvalContext, expr: &Expr) -> Result<Datum> {
        let child = try!(self.get_one_child(expr));
        try!(assert_expr_types(child, INT_TYPES));
        let d = try!(self.eval(ctx, child));
        let s = try!(d.into_string());
        let duration = try!(Duration::parse(s.as_bytes(), 0));
        Ok(Datum::Dur(duration))
    }
}
