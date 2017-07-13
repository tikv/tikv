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

const ERROR_UNIMPLEMENTED: &'static str = "unimplemented";

pub const TYPE_TIME: &'static str = "time";
fn invalid_type_error(datum: &Datum, expected_type: &str) -> Result<Datum> {
    Err(Error::Eval(format!("invalid expr type: {:?}, expect: {}", datum, expected_type)))
}

impl Evaluator {
    pub fn cast_int_as_int(&mut self, _ctx: &EvalContext, _expr: &Expr) -> Result<Datum> {
        // TODO: add impl
        Err(Error::Eval(ERROR_UNIMPLEMENTED.to_owned()))
    }

    pub fn cast_int_as_real(&mut self, _ctx: &EvalContext, _expr: &Expr) -> Result<Datum> {
        // TODO: add impl
        Err(Error::Eval(ERROR_UNIMPLEMENTED.to_owned()))
    }

    pub fn cast_int_as_string(&mut self, _ctx: &EvalContext, _expr: &Expr) -> Result<Datum> {
        // TODO: add impl
        Err(Error::Eval(ERROR_UNIMPLEMENTED.to_owned()))
    }

    pub fn cast_int_as_decimal(&mut self, _ctx: &EvalContext, _expr: &Expr) -> Result<Datum> {
        // TODO: add impl
        Err(Error::Eval(ERROR_UNIMPLEMENTED.to_owned()))
    }

    pub fn cast_int_as_time(&mut self, _ctx: &EvalContext, _expr: &Expr) -> Result<Datum> {
        // TODO: add impl
        Err(Error::Eval(ERROR_UNIMPLEMENTED.to_owned()))
    }

    pub fn cast_int_as_duration(&mut self, _ctx: &EvalContext, _expr: &Expr) -> Result<Datum> {
        // TODO: add impl
        Err(Error::Eval(ERROR_UNIMPLEMENTED.to_owned()))
    }

    pub fn cast_real_as_int(&mut self, _ctx: &EvalContext, _expr: &Expr) -> Result<Datum> {
        // TODO: add impl
        Err(Error::Eval(ERROR_UNIMPLEMENTED.to_owned()))
    }

    pub fn cast_real_as_real(&mut self, _ctx: &EvalContext, _expr: &Expr) -> Result<Datum> {
        // TODO: add impl
        Err(Error::Eval(ERROR_UNIMPLEMENTED.to_owned()))
    }

    pub fn cast_real_as_string(&mut self, _ctx: &EvalContext, _expr: &Expr) -> Result<Datum> {
        // TODO: add impl
        Err(Error::Eval(ERROR_UNIMPLEMENTED.to_owned()))
    }

    pub fn cast_real_as_decimal(&mut self, _ctx: &EvalContext, _expr: &Expr) -> Result<Datum> {
        // TODO: add impl
        Err(Error::Eval(ERROR_UNIMPLEMENTED.to_owned()))
    }

    pub fn cast_real_as_time(&mut self, _ctx: &EvalContext, _expr: &Expr) -> Result<Datum> {
        // TODO: add impl
        Err(Error::Eval(ERROR_UNIMPLEMENTED.to_owned()))
    }

    pub fn cast_real_as_duration(&mut self, _ctx: &EvalContext, _expr: &Expr) -> Result<Datum> {
        // TODO: add impl
        Err(Error::Eval(ERROR_UNIMPLEMENTED.to_owned()))
    }

    pub fn cast_decimal_as_int(&mut self, _ctx: &EvalContext, _expr: &Expr) -> Result<Datum> {
        // TODO: add impl
        Err(Error::Eval(ERROR_UNIMPLEMENTED.to_owned()))
    }

    pub fn cast_decimal_as_real(&mut self, _ctx: &EvalContext, _expr: &Expr) -> Result<Datum> {
        // TODO: add impl
        Err(Error::Eval(ERROR_UNIMPLEMENTED.to_owned()))
    }

    pub fn cast_decimal_as_string(&mut self, _ctx: &EvalContext, _expr: &Expr) -> Result<Datum> {
        // TODO: add impl
        Err(Error::Eval(ERROR_UNIMPLEMENTED.to_owned()))
    }

    pub fn cast_decimal_as_decimal(&mut self, _ctx: &EvalContext, _expr: &Expr) -> Result<Datum> {
        // TODO: add impl
        Err(Error::Eval(ERROR_UNIMPLEMENTED.to_owned()))
    }

    pub fn cast_decimal_as_time(&mut self, _ctx: &EvalContext, _expr: &Expr) -> Result<Datum> {
        // TODO: add impl
        Err(Error::Eval(ERROR_UNIMPLEMENTED.to_owned()))
    }

    pub fn cast_decimal_as_duration(&mut self, _ctx: &EvalContext, _expr: &Expr) -> Result<Datum> {
        // TODO: add impl
        Err(Error::Eval(ERROR_UNIMPLEMENTED.to_owned()))
    }

    pub fn cast_string_as_int(&mut self, _ctx: &EvalContext, _expr: &Expr) -> Result<Datum> {
        // TODO: add impl
        Err(Error::Eval(ERROR_UNIMPLEMENTED.to_owned()))
    }

    pub fn cast_string_as_real(&mut self, _ctx: &EvalContext, _expr: &Expr) -> Result<Datum> {
        // TODO: add impl
        Err(Error::Eval(ERROR_UNIMPLEMENTED.to_owned()))
    }

    pub fn cast_string_as_string(&mut self, _ctx: &EvalContext, _expr: &Expr) -> Result<Datum> {
        // TODO: add impl
        Err(Error::Eval(ERROR_UNIMPLEMENTED.to_owned()))
    }

    pub fn cast_string_as_decimal(&mut self, _ctx: &EvalContext, _expr: &Expr) -> Result<Datum> {
        // TODO: add impl
        Err(Error::Eval(ERROR_UNIMPLEMENTED.to_owned()))
    }

    pub fn cast_string_as_time(&mut self, _ctx: &EvalContext, _expr: &Expr) -> Result<Datum> {
        // TODO: add impl
        Err(Error::Eval(ERROR_UNIMPLEMENTED.to_owned()))
    }

    pub fn cast_string_as_duration(&mut self, _ctx: &EvalContext, _expr: &Expr) -> Result<Datum> {
        // TODO: add impl
        Err(Error::Eval(ERROR_UNIMPLEMENTED.to_owned()))
    }

    pub fn cast_time_as_int(&mut self, ctx: &EvalContext, expr: &Expr) -> Result<Datum> {
        let child = try!(self.get_one_child(expr));
        let d = try!(self.eval(ctx, child));
        if let Datum::Time(_) = d {
            let i = try!(try!(d.into_dec()).as_i64().into_result());
            return Ok(Datum::I64(i));
        }
        invalid_type_error(&d, TYPE_TIME)
    }

    pub fn cast_time_as_real(&mut self, ctx: &EvalContext, expr: &Expr) -> Result<Datum> {
        let child = try!(self.get_one_child(expr));
        let d = try!(self.eval(ctx, child));
        if let Datum::Time(_) = d {
            return Ok(Datum::F64(try!(try!(d.into_dec()).as_f64())));
        }
        invalid_type_error(&d, TYPE_TIME)
    }

    pub fn cast_time_as_string(&mut self, ctx: &EvalContext, expr: &Expr) -> Result<Datum> {
        let child = try!(self.get_one_child(expr));
        let d = try!(self.eval(ctx, child));
        if let Datum::Time(_) = d {
            let s = try!(d.into_string());
            return Ok(Datum::Bytes(s.into_bytes()));
        }
        invalid_type_error(&d, TYPE_TIME)
    }

    pub fn cast_time_as_decimal(&mut self, ctx: &EvalContext, expr: &Expr) -> Result<Datum> {
        let child = try!(self.get_one_child(expr));
        let d = try!(self.eval(ctx, child));
        if let Datum::Time(_) = d {
            let dec = try!(d.into_dec());
            return Ok(Datum::Dec(dec));
        }
        invalid_type_error(&d, TYPE_TIME)
    }

    pub fn cast_time_as_time(&mut self, ctx: &EvalContext, expr: &Expr) -> Result<Datum> {
        let child = try!(self.get_one_child(expr));
        let d = try!(self.eval(ctx, child));
        if let Datum::Time(_) = d {
            return Ok(d)
        }
        invalid_type_error(&d, TYPE_TIME)
    }

    pub fn cast_time_as_duration(&mut self, ctx: &EvalContext, expr: &Expr) -> Result<Datum> {
        let child = try!(self.get_one_child(expr));
        let d = try!(self.eval(ctx, child));
        if let Datum::Time(t) = d {
            return Ok(Datum::Dur(try!(t.to_dur())));
        }
        invalid_type_error(&d, TYPE_TIME)
    }

    pub fn cast_duration_as_int(&mut self, _ctx: &EvalContext, _expr: &Expr) -> Result<Datum> {
        // TODO: add impl
        Err(Error::Eval(ERROR_UNIMPLEMENTED.to_owned()))
    }

    pub fn cast_duration_as_real(&mut self, _ctx: &EvalContext, _expr: &Expr) -> Result<Datum> {
        // TODO: add impl
        Err(Error::Eval(ERROR_UNIMPLEMENTED.to_owned()))
    }

    pub fn cast_duration_as_string(&mut self, _ctx: &EvalContext, _expr: &Expr) -> Result<Datum> {
        // TODO: add impl
        Err(Error::Eval(ERROR_UNIMPLEMENTED.to_owned()))
    }

    pub fn cast_duration_as_decimal(&mut self, _ctx: &EvalContext, _expr: &Expr) -> Result<Datum> {
        // TODO: add impl
        Err(Error::Eval(ERROR_UNIMPLEMENTED.to_owned()))
    }

    pub fn cast_duration_as_time(&mut self, _ctx: &EvalContext, _expr: &Expr) -> Result<Datum> {
        // TODO: add impl
        Err(Error::Eval(ERROR_UNIMPLEMENTED.to_owned()))
    }

    pub fn cast_duration_as_duration(&mut self, _ctx: &EvalContext, _expr: &Expr) -> Result<Datum> {
        // TODO: add impl
        Err(Error::Eval(ERROR_UNIMPLEMENTED.to_owned()))
    }
}

#[cfg(test)]
mod test {
    use tipb::expression::{ExprType, ScalarFuncSig};
    use util::codec::datum::Datum;
    use util::codec::mysql::{Decimal, Time, Duration};
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
}
