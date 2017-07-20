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
use coprocessor::codec::datum::{Datum, produce_str_with_specified_tp,
                                produce_dec_with_specified_tp};
use super::{Evaluator, EvalContext, Result, Error, ERROR_UNIMPLEMENTED};

pub const TYPE_DURATION: &'static str = "duration";
pub const TYPE_TIME: &'static str = "time";
const ERROR_UNIMPLEMENTED: &'static str = "unimplemented";

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

    pub fn cast_duration_as_int(&mut self, ctx: &EvalContext, expr: &Expr) -> Result<Datum> {
        let child = try!(self.get_one_child(expr));
        let d = try!(self.eval(ctx, child));
        if let Datum::Dur(dur) = d {
            let d = Datum::Dur(try!(dur.round_frac(expr.get_field_type().get_decimal() as i8)));
            let i = try!(try!(d.into_dec()).as_i64().into_result());
            return Ok(Datum::I64(i));
        }
        invalid_type_error(&d, TYPE_DURATION)
    }

    pub fn cast_duration_as_real(&mut self, ctx: &EvalContext, expr: &Expr) -> Result<Datum> {
        let child = try!(self.get_one_child(expr));
        let d = try!(self.eval(ctx, child));
        if let Datum::Dur(_) = d {
            return Ok(Datum::F64(try!(try!(d.into_dec()).as_f64())));
        }
        invalid_type_error(&d, TYPE_DURATION)
    }

    pub fn cast_duration_as_string(&mut self, ctx: &EvalContext, expr: &Expr) -> Result<Datum> {
        let child = try!(self.get_one_child(expr));
        let d = try!(self.eval(ctx, child));
        if let Datum::Dur(_) = d {
            let s = try!(produce_str_with_specified_tp(try!(d.into_string()),
                                                       expr.get_field_type(),
                                                       ctx));
            return Ok(Datum::Bytes(s.into_bytes()));
        }
        invalid_type_error(&d, TYPE_DURATION)
    }

    pub fn cast_duration_as_decimal(&mut self, ctx: &EvalContext, expr: &Expr) -> Result<Datum> {
        let child = try!(self.get_one_child(expr));
        let d = try!(self.eval(ctx, child));
        if let Datum::Dur(_) = d {
            let dec =
                try!(produce_dec_with_specified_tp(try!(d.into_dec()), expr.get_field_type(), ctx));
            return Ok(Datum::Dec(dec));
        }
        invalid_type_error(&d, TYPE_DURATION)
    }

    pub fn cast_duration_as_time(&mut self, ctx: &EvalContext, expr: &Expr) -> Result<Datum> {
        let child = try!(self.get_one_child(expr));
        let d = try!(self.eval(ctx, child));
        if let Datum::Dur(d) = d {
            let tp = expr.get_field_type();
            return Ok(Datum::Time(
                try!(d.to_time(ctx.tz, tp.get_tp() as u8, tp.get_decimal() as i8))
            ));
        }
        invalid_type_error(&d, TYPE_DURATION)
    }

    pub fn cast_duration_as_duration(&mut self, ctx: &EvalContext, expr: &Expr) -> Result<Datum> {
        let child = try!(self.get_one_child(expr));
        let d = try!(self.eval(ctx, child));
        if let Datum::Dur(dur) = d {
            return Ok(Datum::Dur(try!(dur.round_frac(expr.get_field_type().get_decimal() as i8))));
        }
        invalid_type_error(&d, TYPE_DURATION)
    }
}

#[cfg(test)]
mod test {
    use protobuf::ProtobufEnum;
    use chrono::{Datelike, Local, FixedOffset};
    use tipb::expression::{ExprType, ScalarFuncSig, FieldType, DataType};
    use coprocessor::codec::datum::Datum;
    use coprocessor::codec::mysql::{Time, Duration, Decimal};
    use coprocessor::codec::mysql::time::ymd_hms_nanos;
    use coprocessor::codec::mysql::types;
    use super::super::Evaluator;
    use super::super::evaluator::test::build_expr_with_sig;
    use super::super::super::codec::mysql::duration::NANOS_PER_SEC;

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
    
    // first, 31d, 11h, 30m, 45s
    // second, 1d, 10h, 7m, 17s
    test_eval!(test_cast_duration_as_int,
               vec![(build_expr_with_sig(
                    vec![Datum::Dur(Duration::from_nanos(-2719845 * NANOS_PER_SEC, 6).unwrap())],
                    ExprType::ScalarFunc,
                    ScalarFuncSig::CastDurationAsInt, {
                        let mut tp = FieldType::new();
                        tp.set_decimal(5);
                        tp
                    }),
                    Datum::I64(-7553045)),
                (build_expr_with_sig(
                    vec![Datum::Dur(Duration::from_nanos(
                        122837 * NANOS_PER_SEC + 900_000_000, 6
                    ).unwrap())],
                    ExprType::ScalarFunc,
                    ScalarFuncSig::CastDurationAsInt, {
                        let mut tp = FieldType::new();
                        tp.set_decimal(0);
                        tp
                    }),
                    Datum::I64(340718)),
        ]);

    test_eval!(test_cast_duration_as_real,
               vec![(build_expr_with_sig(
                    vec![Datum::Dur(Duration::from_nanos(-2719845 * NANOS_PER_SEC, 6).unwrap())],
                    ExprType::ScalarFunc,
                    ScalarFuncSig::CastDurationAsReal,
                    FieldType::new()),
                    Datum::F64(-7553045 as f64)),
                (build_expr_with_sig(
                    vec![Datum::Dur(Duration::from_nanos(
                        122837 * NANOS_PER_SEC + 123_456_789, 6
                    ).unwrap())],
                    ExprType::ScalarFunc,
                    ScalarFuncSig::CastDurationAsReal,
                    FieldType::new()),
                    Datum::F64(340717.123457 as f64)),
        ]);

    test_eval!(test_cast_duration_as_string,
               vec![(build_expr_with_sig(
                    vec![Datum::Dur(Duration::from_nanos(-2719845 * NANOS_PER_SEC, 6).unwrap())],
                    ExprType::ScalarFunc,
                    ScalarFuncSig::CastDurationAsString, {
                        let mut tp = FieldType::new();
                        tp.set_flen(17);
                        tp
                    }),
                    Datum::Bytes(b"-755:30:45.000000".to_vec())),
                (build_expr_with_sig(
                    vec![Datum::Dur(Duration::from_nanos(
                        122837 * NANOS_PER_SEC + 123_456_789, 6
                    ).unwrap())],
                    ExprType::ScalarFunc,
                    ScalarFuncSig::CastDurationAsString, {
                        let mut tp = FieldType::new();
                        tp.set_flen(15);
                        tp
                    }),
                    Datum::Bytes(b"34:07:17.123457".to_vec())),
                (build_expr_with_sig(
                    vec![Datum::Dur(Duration::from_nanos(
                        122837 * NANOS_PER_SEC + 100_000_000, 3
                    ).unwrap())],
                    ExprType::ScalarFunc,
                    ScalarFuncSig::CastDurationAsString, {
                        let mut tp = FieldType::new();
                        tp.set_flen(15);
                        tp
                    }),
                    Datum::Bytes(b"34:07:17.100000".to_vec())),
        ]);

    test_eval!(test_cast_duration_as_decimal,
               vec![(build_expr_with_sig(
                    vec![Datum::Dur(Duration::from_nanos(-2719845 * NANOS_PER_SEC, 6).unwrap())],
                    ExprType::ScalarFunc,
                    ScalarFuncSig::CastDurationAsDecimal, {
                        let mut tp = FieldType::new();
                        tp.set_decimal(6);
                        tp.set_flen(13);
                        tp
                    }),
                    Datum::Dec("-7553045".parse().unwrap())),
                (build_expr_with_sig(
                    vec![Datum::Dur(Duration::from_nanos(
                        122837 * NANOS_PER_SEC + 123_456_789, 6
                    ).unwrap())],
                    ExprType::ScalarFunc,
                    ScalarFuncSig::CastDurationAsDecimal, {
                        let mut tp = FieldType::new();
                        tp.set_decimal(6);
                        tp.set_flen(12);
                        tp
                    }),
                    Datum::Dec("340717.123457".parse().unwrap())),
                (build_expr_with_sig(
                    vec![Datum::Dur(Duration::from_nanos(
                        122837 * NANOS_PER_SEC + 100_000_000, 6
                    ).unwrap())],
                    ExprType::ScalarFunc,
                    ScalarFuncSig::CastDurationAsDecimal, {
                        let mut tp = FieldType::new();
                        tp.set_decimal(6);
                        tp.set_flen(12);
                        tp
                    }),
                    Datum::Dec("340717.1".parse().unwrap())),
        ]);

    test_eval!(test_cast_duration_as_time,
               vec![(build_expr_with_sig(
                vec![Datum::Dur(Duration::from_nanos(
                        122837 * NANOS_PER_SEC + 123_456_789, 6
                    ).unwrap())],
                    ExprType::ScalarFunc,
                    ScalarFuncSig::CastDurationAsTime, {
                        let mut tp = FieldType::new();
                        tp.set_decimal(5);
                        tp.set_tp(DataType::from_i32(types::TIMESTAMP as i32).unwrap());
                        tp
                    }),
                    Datum::Time({
                        let t = Local::today();
                        let d = ymd_hms_nanos(&FixedOffset::east(0),
                              t.year(),
                              t.month(),
                              t.day(),
                              0,
                              0,
                              0,
                              122837 * NANOS_PER_SEC as i64 + 123_460_000).unwrap();
                        Time::new(d, types::TIMESTAMP, 5).unwrap()
                    })),
        ]);

    test_eval!(test_cast_duration_as_duration,
               vec![(build_expr_with_sig(
                    vec![Datum::Dur(Duration::from_nanos(
                        122837 * NANOS_PER_SEC + 100_000_000, 6
                    ).unwrap())],
                    ExprType::ScalarFunc,
                    ScalarFuncSig::CastDurationAsDuration, {
                        let mut tp = FieldType::new();
                        tp.set_decimal(5);
                        tp
                    }),
                    Datum::Dur(Duration::parse(b"1 10:07:17.1", 5).unwrap())),
                (build_expr_with_sig(
                    vec![Datum::Dur(Duration::from_nanos(
                        122837 * NANOS_PER_SEC + 155_555_555, 6
                    ).unwrap())],
                    ExprType::ScalarFunc,
                    ScalarFuncSig::CastDurationAsDuration, {
                        let mut tp = FieldType::new();
                        tp.set_decimal(5);
                        tp
                    }),
                    Datum::Dur(Duration::parse(b"1 10:07:17.155555555", 5).unwrap())),
        ]);
}
