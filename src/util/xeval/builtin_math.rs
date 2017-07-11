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
use util::codec::datum::Datum;
use super::{Evaluator, EvalContext, Result, Error};

pub const TYPE_INT: &'static str = "int";

pub fn invalid_type_error(datum: &Datum, expected_type: &str) -> Result<Datum> {
    Err(Error::Eval(format!("invalid expr type: {:?}, expect: {}", datum, expected_type)))
}

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
}

#[cfg(test)]
mod test {
    use tipb::expression::{ExprType, ScalarFuncSig};
    use util::codec::datum::Datum;
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

    test_eval!(test_abs_int,
               vec![(build_expr_with_sig(vec![Datum::I64(-1)],
                                         ExprType::ScalarFunc,
                                         ScalarFuncSig::AbsInt),
                     Datum::I64(1)),
                    (build_expr_with_sig(vec![Datum::I64(1)],
                                         ExprType::ScalarFunc,
                                         ScalarFuncSig::AbsInt),
                     Datum::I64(1)),
                    (build_expr_with_sig(vec![Datum::U64(1)],
                                         ExprType::ScalarFunc,
                                         ScalarFuncSig::AbsInt),
                     Datum::U64(1))]);
}
