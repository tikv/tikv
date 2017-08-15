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
use coprocessor::codec::datum::Datum;
use super::{Error, EvalContext, Evaluator, Result};

const ERROR_UNIMPLEMENTED: &'static str = "unimplemented";
pub const TYPE_INT: &'static str = "int";
pub const TYPE_FLOAT: &'static str = "float";

pub fn invalid_type_error(datum: &Datum, expected_type: &str) -> Result<Datum> {
    Err(Error::Eval(format!(
        "invalid expr type: {:?}, expect: {}",
        datum,
        expected_type
    )))
}

impl Evaluator {
    pub fn abs_int(&mut self, ctx: &EvalContext, expr: &Expr) -> Result<Datum> {
        let child = try!(self.get_one_child(expr));
        let d = try!(self.eval(ctx, child));
        match d {
            Datum::I64(i) => if i >= 0 {
                Ok(Datum::I64(i))
            } else {
                Ok(Datum::I64(-i))
            },
            Datum::U64(_) | Datum::Null => Ok(d),
            _ => invalid_type_error(&d, TYPE_INT),
        }
    }

    pub fn abs_real(&mut self, ctx: &EvalContext, expr: &Expr) -> Result<Datum> {
        let child = try!(self.get_one_child(expr));
        let d = try!(self.eval(ctx, child));
        match d {
            Datum::F64(f) => Ok(Datum::F64(f.abs())),
            Datum::Null => Ok(Datum::Null),
            _ => invalid_type_error(&d, TYPE_FLOAT),
        }
    }

    pub fn ceil_int(&mut self, ctx: &EvalContext, expr: &Expr) -> Result<Datum> {
        let child = try!(self.get_one_child(expr));
        let d = try!(self.eval(ctx, child));
        match d {
            Datum::F64(i) => {
                let result = i.ceil() as i64;
                Ok(Datum::I64(result))
            }
            Datum::Null => Ok(Datum::Null),
            _ => invalid_type_error(&d, TYPE_FLOAT),
        }
    }

    pub fn ceil_real(&mut self, ctx: &EvalContext, expr: &Expr) -> Result<Datum> {
        let child = try!(self.get_one_child(expr));
        let d = try!(self.eval(ctx, child));
        match d {
            Datum::F64(f) => Ok(Datum::F64(f.ceil())),
            Datum::Null => Ok(Datum::Null),
            _ => invalid_type_error(&d, TYPE_FLOAT),
        }
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
    use tipb::expression::{ExprType, ScalarFuncSig};
    use coprocessor::codec::datum::Datum;
    use super::super::Evaluator;
    use super::super::evaluator::test::build_expr_with_sig;

    macro_rules! test_eval {
        ($tag:ident, $cases:expr) => {
            #[test]
            fn $tag() {
                let mut test_cases = $cases;
                let mut evaluator = Evaluator::default();
                for (i, (expr, expected)) in test_cases.drain(..).enumerate() {
                    let res = evaluator.eval(&Default::default(), &expr);
                    assert!(res.is_ok(),
                            "#{} expect eval expr {:?} ok but got {:?}",
                            i,
                            expr,
                            res);
                    let res = res.unwrap();
                    assert_eq!(res,
                               expected,
                               "#{} expect {:?} but got {:?}",
                               i,
                               expected,
                               res);
                }
            }
        };
    }

    test_eval!(
        test_abs_int,
        vec![
            (
                build_expr_with_sig(
                    vec![Datum::I64(-1)],
                    ExprType::ScalarFunc,
                    ScalarFuncSig::AbsInt,
                ),
                Datum::I64(1),
            ),
            (
                build_expr_with_sig(
                    vec![Datum::I64(1)],
                    ExprType::ScalarFunc,
                    ScalarFuncSig::AbsInt,
                ),
                Datum::I64(1),
            ),
            (
                build_expr_with_sig(
                    vec![Datum::U64(1)],
                    ExprType::ScalarFunc,
                    ScalarFuncSig::AbsInt,
                ),
                Datum::U64(1),
            ),
            (
                build_expr_with_sig(
                    vec![Datum::Null],
                    ExprType::ScalarFunc,
                    ScalarFuncSig::AbsInt,
                ),
                Datum::Null,
            ),
        ]
    );

    test_eval!(
        test_abs_real,
        vec![
            (
                build_expr_with_sig(
                    vec![Datum::F64(-1.0_f64)],
                    ExprType::ScalarFunc,
                    ScalarFuncSig::AbsReal,
                ),
                Datum::F64(1.0_f64),
            ),
            (
                build_expr_with_sig(
                    vec![Datum::F64(1.0_f64)],
                    ExprType::ScalarFunc,
                    ScalarFuncSig::AbsReal,
                ),
                Datum::F64(1.0_f64),
            ),
            (
                build_expr_with_sig(
                    vec![Datum::Null],
                    ExprType::ScalarFunc,
                    ScalarFuncSig::AbsReal,
                ),
                Datum::Null,
            ),
            (
                build_expr_with_sig(
                    vec![Datum::F64(0.0_f64)],
                    ExprType::ScalarFunc,
                    ScalarFuncSig::AbsReal,
                ),
                Datum::F64(0.0_f64),
            ),
        ]
    );

    test_eval!(
        test_ceil_int,
        vec![
            (
                build_expr_with_sig(
                    vec![Datum::F64(-1.23)],
                    ExprType::ScalarFunc,
                    ScalarFuncSig::CeilInt,
                ),
                Datum::I64(-1),
            ),
            (
                build_expr_with_sig(
                    vec![Datum::F64(1.23)],
                    ExprType::ScalarFunc,
                    ScalarFuncSig::CeilInt,
                ),
                Datum::I64(2),
            ),
            (
                build_expr_with_sig(
                    vec![Datum::F64(2.0)],
                    ExprType::ScalarFunc,
                    ScalarFuncSig::CeilInt,
                ),
                Datum::I64(2),
            ),
            (
                build_expr_with_sig(
                    vec![Datum::Null],
                    ExprType::ScalarFunc,
                    ScalarFuncSig::CeilInt,
                ),
                Datum::Null,
            ),
        ]
    );

    test_eval!(
        test_ceil_real,
        vec![
            (
                build_expr_with_sig(
                    vec![Datum::F64(-1.5)],
                    ExprType::ScalarFunc,
                    ScalarFuncSig::CeilReal,
                ),
                Datum::F64(-1.0),
            ),
            (
                build_expr_with_sig(
                    vec![Datum::F64(1.1)],
                    ExprType::ScalarFunc,
                    ScalarFuncSig::CeilReal,
                ),
                Datum::F64(2.0),
            ),
            (
                build_expr_with_sig(
                    vec![Datum::F64(2.0)],
                    ExprType::ScalarFunc,
                    ScalarFuncSig::CeilReal,
                ),
                Datum::F64(2.0),
            ),
            (
                build_expr_with_sig(
                    vec![Datum::Null],
                    ExprType::ScalarFunc,
                    ScalarFuncSig::CeilReal,
                ),
                Datum::Null,
            ),
        ]
    );
}
