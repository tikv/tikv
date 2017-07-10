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
use util::codec::mysql::{Decimal, Time, Duration, DEFAULT_FSP};
use super::{Evaluator, EvalContext, Result, Error};

const ERROR_UNIMPLEMENTED: &'static str = "unimplemented";

fn invalid_type_error(datum: &Datum) -> Result<Datum> {
    Err(Error::Eval(format!("invalid expr type: {:?}, expect to be int", datum)))
}

fn check_datum_int(datum: &Datum) -> Result<()> {
    match *datum {
        Datum::I64(_) | Datum::U64(_) => Ok(()),
        _ => Err(Error::Eval(format!("invalid expr type: {:?}, expect to be int", datum))),
    }
}

impl Evaluator {
    pub fn cast_int_as_int(&mut self, ctx: &EvalContext, expr: &Expr) -> Result<Datum> {
        let child = try!(self.get_one_child(expr));
        let d = try!(self.eval(ctx, child));
        match d {
            Datum::I64(_) => Ok(d),
            Datum::U64(_) => Ok(Datum::I64(d.i64())),
            _ => invalid_type_error(&d),
        }
    }

    pub fn cast_int_as_real(&mut self, ctx: &EvalContext, expr: &Expr) -> Result<Datum> {
        let child = try!(self.get_one_child(expr));
        let d = try!(self.eval(ctx, child));
        match d {
            Datum::I64(i) => Ok(Datum::F64(i as f64)),
            Datum::U64(u) => Ok(Datum::F64(u as f64)),
            _ => invalid_type_error(&d),
        }
    }

    pub fn cast_int_as_string(&mut self, ctx: &EvalContext, expr: &Expr) -> Result<Datum> {
        let child = try!(self.get_one_child(expr));
        let d = try!(self.eval(ctx, child));
        match d {
            Datum::I64(_) | Datum::U64(_) => {}
            _ => return invalid_type_error(&d),
        }
        let s = try!(d.into_string());
        Ok(Datum::Bytes(s.into_bytes()))
    }

    pub fn cast_int_as_decimal(&mut self, ctx: &EvalContext, expr: &Expr) -> Result<Datum> {
        let child = try!(self.get_one_child(expr));
        let datum = try!(self.eval(ctx, child));
        let decimal = match datum {
            Datum::I64(_) => Decimal::from(datum.i64()),
            Datum::U64(_) => Decimal::from(datum.u64()),
            _ => return invalid_type_error(&datum),
        };
        Ok(Datum::Dec(decimal))
    }

    pub fn cast_int_as_time(&mut self, ctx: &EvalContext, expr: &Expr) -> Result<Datum> {
        let child = try!(self.get_one_child(expr));
        let d = try!(self.eval(ctx, child));
        try!(check_datum_int(&d));
        let s = try!(d.into_string());
        let t = try!(Time::parse_datetime(&s, DEFAULT_FSP, &ctx.tz));
        Ok(Datum::Time(t))
    }

    pub fn cast_int_as_duration(&mut self, ctx: &EvalContext, expr: &Expr) -> Result<Datum> {
        let child = try!(self.get_one_child(expr));
        let d = try!(self.eval(ctx, child));
        try!(check_datum_int(&d));
        let s = try!(d.into_string());
        let duration = try!(Duration::parse(s.as_bytes(), DEFAULT_FSP));
        Ok(Datum::Dur(duration))
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

    pub fn cast_time_as_int(&mut self, _ctx: &EvalContext, _expr: &Expr) -> Result<Datum> {
        // TODO: add impl
        Err(Error::Eval(ERROR_UNIMPLEMENTED.to_owned()))
    }

    pub fn cast_time_as_real(&mut self, _ctx: &EvalContext, _expr: &Expr) -> Result<Datum> {
        // TODO: add impl
        Err(Error::Eval(ERROR_UNIMPLEMENTED.to_owned()))
    }

    pub fn cast_time_as_string(&mut self, _ctx: &EvalContext, _expr: &Expr) -> Result<Datum> {
        // TODO: add impl
        Err(Error::Eval(ERROR_UNIMPLEMENTED.to_owned()))
    }

    pub fn cast_time_as_decimal(&mut self, _ctx: &EvalContext, _expr: &Expr) -> Result<Datum> {
        // TODO: add impl
        Err(Error::Eval(ERROR_UNIMPLEMENTED.to_owned()))
    }

    pub fn cast_time_as_time(&mut self, _ctx: &EvalContext, _expr: &Expr) -> Result<Datum> {
        // TODO: add impl
        Err(Error::Eval(ERROR_UNIMPLEMENTED.to_owned()))
    }

    pub fn cast_time_as_duration(&mut self, _ctx: &EvalContext, _expr: &Expr) -> Result<Datum> {
        // TODO: add impl
        Err(Error::Eval(ERROR_UNIMPLEMENTED.to_owned()))
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
    use std::u64;
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

    test_eval!(test_cast_int_as_int,
               vec![(build_expr_with_sig(vec![Datum::I64(1)],
                                         ExprType::ScalarFunc,
                                         ScalarFuncSig::CastIntAsInt),
                     Datum::I64(1)),
                    (build_expr_with_sig(vec![Datum::I64(-1)],
                                         ExprType::ScalarFunc,
                                         ScalarFuncSig::CastIntAsInt),
                     Datum::I64(-1)),
                    (build_expr_with_sig(vec![Datum::U64(u64::MAX)],
                                         ExprType::ScalarFunc,
                                         ScalarFuncSig::CastIntAsInt),
                     Datum::I64(-1))]);

    test_eval!(test_cast_int_as_real,
               vec![(build_expr_with_sig(vec![Datum::I64(-1)],
                                         ExprType::ScalarFunc,
                                         ScalarFuncSig::CastIntAsReal),
                     Datum::F64(-1.0)),
                    (build_expr_with_sig(vec![Datum::U64(1)],
                                         ExprType::ScalarFunc,
                                         ScalarFuncSig::CastIntAsReal),
                     Datum::F64(1.0))]);
    test_eval!(test_cast_int_as_string,
               vec![(build_expr_with_sig(vec![Datum::I64(-1)],
                                         ExprType::ScalarFunc,
                                         ScalarFuncSig::CastIntAsString),
                     Datum::Bytes(b"-1".to_vec())),
                    (build_expr_with_sig(vec![Datum::U64(1)],
                                         ExprType::ScalarFunc,
                                         ScalarFuncSig::CastIntAsString),
                     Datum::Bytes(b"1".to_vec()))]);

    test_eval!(test_cast_int_as_decimal,
               vec![(build_expr_with_sig(vec![Datum::I64(-1)],
                                         ExprType::ScalarFunc,
                                         ScalarFuncSig::CastIntAsDecimal),
                     Datum::Dec(Decimal::from(-1))),
                    (build_expr_with_sig(vec![Datum::U64(1)],
                                         ExprType::ScalarFunc,
                                         ScalarFuncSig::CastIntAsDecimal),
                     Datum::Dec(Decimal::from(1)))]);

    test_eval!(test_cast_int_as_time,
               vec![(build_expr_with_sig(vec![Datum::I64(20121231113045)],
                                         ExprType::ScalarFunc,
                                         ScalarFuncSig::CastIntAsTime),
                     Datum::Time(Time::parse_utc_datetime("2012-12-31 11:30:45", 0).unwrap())),
                    (build_expr_with_sig(vec![Datum::U64(121231)],
                                         ExprType::ScalarFunc,
                                         ScalarFuncSig::CastIntAsTime),
                     Datum::Time(Time::parse_utc_datetime("2012-12-31 00:00:00", 0).unwrap()))]);

    test_eval!(test_cast_int_as_duration,
               vec![(build_expr_with_sig(vec![Datum::I64(101112)],
                                         ExprType::ScalarFunc,
                                         ScalarFuncSig::CastIntAsDuration),
                     Datum::Dur(Duration::parse(b"10:11:12", 0).unwrap()))]);
}
