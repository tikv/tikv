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

use tipb::expression::Expr;
use super::super::codec::datum::Datum;
use super::{Evaluator, EvalContext, Result, Error, ERROR_UNIMPLEMENTED, TYPE_INT,
            invalid_type_error};

impl Evaluator {
    pub fn abs_int(&mut self, ctx: &EvalContext, expr: &Expr) -> Result<Datum> {
        let child = try!(self.get_one_child(expr));
        let d = try!(self.eval(ctx, child));
        match d {
            Datum::I64(i) => {
                if i >= 0 {
                    Ok(Datum::I64(i))
                } else {
                    Ok(Datum::I64(-i))
                }
            }
            Datum::U64(_) => Ok(d),
            _ => invalid_type_error(&d, TYPE_INT),
        }
    }

    pub fn abs_real(&mut self, _ctx: &EvalContext, _expr: &Expr) -> Result<Datum> {
        // TODO add impl
        Err(Error::Eval(ERROR_UNIMPLEMENTED.to_owned()))
    }

    pub fn ceil_int(&mut self, _ctx: &EvalContext, _expr: &Expr) -> Result<Datum> {
        // TODO add impl
        Err(Error::Eval(ERROR_UNIMPLEMENTED.to_owned()))
    }

    pub fn ceil_real(&mut self, _ctx: &EvalContext, _expr: &Expr) -> Result<Datum> {
        // TODO add impl
        Err(Error::Eval(ERROR_UNIMPLEMENTED.to_owned()))
    }

    pub fn floor_int(&mut self, _ctx: &EvalContext, _expr: &Expr) -> Result<Datum> {
        // TODO add impl
        Err(Error::Eval(ERROR_UNIMPLEMENTED.to_owned()))
    }

    pub fn floor_real(&mut self, _ctx: &EvalContext, _expr: &Expr) -> Result<Datum> {
        // TODO add impl
        Err(Error::Eval(ERROR_UNIMPLEMENTED.to_owned()))
    }
}

#[cfg(test)]
mod test {
    use tipb::expression::{FieldType, ExprType, ScalarFuncSig};
    use coprocessor::codec::datum::Datum;
    use super::super::Evaluator;
    use super::super::evaluator::test::build_expr_with_sig;

    test_eval!(test_abs_int,
               vec![(build_expr_with_sig(vec![Datum::I64(-1)],
                                         ExprType::ScalarFunc,
                                         ScalarFuncSig::AbsInt,
                                         FieldType::new()),
                     Datum::I64(1)),
                    (build_expr_with_sig(vec![Datum::I64(1)],
                                         ExprType::ScalarFunc,
                                         ScalarFuncSig::AbsInt,
                                         FieldType::new()),
                     Datum::I64(1)),
                    (build_expr_with_sig(vec![Datum::U64(1)],
                                         ExprType::ScalarFunc,
                                         ScalarFuncSig::AbsInt,
                                         FieldType::new()),
                     Datum::U64(1))]);
}
