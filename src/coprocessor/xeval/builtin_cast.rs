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

use std::{i64, u64};
use protobuf::ProtobufEnum;
use chrono::naive::time::NaiveTime;
use tipb::expression::{DataType, Expr};
use coprocessor::codec::datum::{Datum, produce_dec_with_specified_tp,
                                produce_str_with_specified_tp};
use coprocessor::codec::mysql::{Decimal, Time, Duration, has_unsigned_flag, Res};
use coprocessor::codec::convert;
use super::{Evaluator, EvalContext, Result, Error, ERROR_UNIMPLEMENTED};

const TYPE_REAL: &'static str = "real";
const TYPE_DECIMAL: &'static str = "decimal";

fn invalid_type_error(datum: &Datum, expected_type: &str) -> Result<Datum> {
    Err(Error::Eval(format!("invalid expr type: {:?}, expect: {}", datum, expected_type)))
}


// `decimal_try` wraps calls to decimal functions.
#[macro_export]
macro_rules! decimal_try {
    ($e:expr) => (
        match $e {
            Res::Ok(v) => v,
            Res::Overflow(_) => return Err(Error::Eval("round overflow".to_owned())),
            Res::Truncated(_) => return Err(Error::Eval("round truncated".to_owned())),
        });
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

    pub fn cast_real_as_int(&mut self, ctx: &EvalContext, expr: &Expr) -> Result<Datum> {
        let child = try!(self.get_one_child(expr));
        let d = try!(self.eval(ctx, child));
        if let Datum::F64(f) = d {
            if has_unsigned_flag(expr.get_field_type().get_flag() as u64) {
                let u = try!(convert::convert_float_to_uint(f, u64::MAX));
                return Ok(Datum::U64(u));
            } else {
                let i = try!(convert::convert_float_to_int(f, i64::MIN, i64::MAX));
                return Ok(Datum::I64(i));
            }
        }
        invalid_type_error(&d, TYPE_REAL)
    }

    pub fn cast_real_as_real(&mut self, ctx: &EvalContext, expr: &Expr) -> Result<Datum> {
        let child = try!(self.get_one_child(expr));
        let d = try!(self.eval(ctx, child));
        match d {
            Datum::F64(_) => Ok(d),
            _ => invalid_type_error(&d, TYPE_REAL),
        }
    }

    pub fn cast_real_as_string(&mut self, ctx: &EvalContext, expr: &Expr) -> Result<Datum> {
        let child = try!(self.get_one_child(expr));
        let d = try!(self.eval(ctx, child));
        if let Datum::F64(_) = d {
            let s = try!(d.into_string());
            let s = try!(produce_str_with_specified_tp(s, expr.get_field_type(), ctx));
            return Ok(Datum::Bytes(s.into_bytes()));
        }
        invalid_type_error(&d, TYPE_REAL)
    }

    pub fn cast_real_as_decimal(&mut self, ctx: &EvalContext, expr: &Expr) -> Result<Datum> {
        let child = try!(self.get_one_child(expr));
        let datum = try!(self.eval(ctx, child));
        if let Datum::F64(f) = datum {
            let decimal = try!(Decimal::from_f64(f));
            let decimal = try!(produce_dec_with_specified_tp(decimal, expr.get_field_type(), ctx));
            return Ok(Datum::Dec(decimal));
        }
        invalid_type_error(&datum, TYPE_REAL)
    }

    pub fn cast_real_as_time(&mut self, ctx: &EvalContext, expr: &Expr) -> Result<Datum> {
        let child = try!(self.get_one_child(expr));
        let d = try!(self.eval(ctx, child));
        match d {
            Datum::F64(_) => {}
            _ => return invalid_type_error(&d, TYPE_REAL),
        }
        let s = try!(d.into_string());
        let fsp = expr.get_field_type().get_decimal() as i8;
        let mut t = try!(Time::parse_datetime(&s, fsp, &ctx.tz));
        let data_type = expr.get_field_type().get_tp();
        if data_type == DataType::TypeDate {
            let dt = t.get_time().date().and_time(NaiveTime::from_hms(0, 0, 0)).unwrap();
            t = try!(Time::new(dt, data_type.value() as u8, fsp));
        }
        Ok(Datum::Time(t))
    }

    pub fn cast_real_as_duration(&mut self, ctx: &EvalContext, expr: &Expr) -> Result<Datum> {
        let child = try!(self.get_one_child(expr));
        let d = try!(self.eval(ctx, child));
        match d {
            Datum::F64(_) => {}
            _ => return invalid_type_error(&d, TYPE_REAL),
        }
        let s = try!(d.into_string());
        let duration = try!(Duration::parse(s.as_bytes(),
                                            expr.get_field_type().get_decimal() as i8));
        Ok(Datum::Dur(duration))
    }

    pub fn cast_decimal_as_int(&mut self, ctx: &EvalContext, expr: &Expr) -> Result<Datum> {
        let child = try!(self.get_one_child(expr));
        let datum = try!(self.eval(ctx, child));
        if let Datum::Dec(d) = datum {
            if has_unsigned_flag(expr.get_field_type().get_flag() as u64) {
                let f = try!(d.as_f64());
                let u = try!(convert::convert_float_to_uint(f, u64::MAX));
                return Ok(Datum::U64(u));
            } else {
                let d = decimal_try!(d.round(0));
                let i = decimal_try!(d.as_i64());
                return Ok(Datum::I64(i));
            }
        }
        invalid_type_error(&datum, TYPE_DECIMAL)
    }

    pub fn cast_decimal_as_real(&mut self, ctx: &EvalContext, expr: &Expr) -> Result<Datum> {
        let child = try!(self.get_one_child(expr));
        let datum = try!(self.eval(ctx, child));
        if let Datum::Dec(d) = datum {
            let f = try!(d.as_f64());
            return Ok(Datum::F64(f));
        }
        invalid_type_error(&datum, TYPE_DECIMAL)
    }

    pub fn cast_decimal_as_string(&mut self, ctx: &EvalContext, expr: &Expr) -> Result<Datum> {
        let child = try!(self.get_one_child(expr));
        let datum = try!(self.eval(ctx, child));
        if let Datum::Dec(_) = datum {
            let s = try!(datum.into_string());
            let s = try!(produce_str_with_specified_tp(s, expr.get_field_type(), ctx));
            return Ok(Datum::Bytes(s.into_bytes()));
        }
        invalid_type_error(&datum, TYPE_REAL)
    }

    pub fn cast_decimal_as_decimal(&mut self, ctx: &EvalContext, expr: &Expr) -> Result<Datum> {
        let child = try!(self.get_one_child(expr));
        let datum = try!(self.eval(ctx, child));
        if let Datum::Dec(d) = datum {
            let decimal = try!(produce_dec_with_specified_tp(d, expr.get_field_type(), ctx));
            return Ok(Datum::Dec(decimal));
        }
        invalid_type_error(&datum, TYPE_DECIMAL)
    }

    pub fn cast_decimal_as_time(&mut self, ctx: &EvalContext, expr: &Expr) -> Result<Datum> {
        let child = try!(self.get_one_child(expr));
        let datum = try!(self.eval(ctx, child));
        match datum {
            Datum::Dec(_) => {}
            _ => return invalid_type_error(&datum, TYPE_DECIMAL),
        }
        let s = try!(datum.into_string());
        let fsp = expr.get_field_type().get_decimal() as i8;
        let mut t = try!(Time::parse_datetime(&s, fsp, &ctx.tz));
        let data_type = expr.get_field_type().get_tp();
        if data_type == DataType::TypeDate {
            let dt = t.get_time().date().and_time(NaiveTime::from_hms(0, 0, 0)).unwrap();
            t = try!(Time::new(dt, data_type.value() as u8, fsp));
        }
        Ok(Datum::Time(t))
    }

    pub fn cast_decimal_as_duration(&mut self, ctx: &EvalContext, expr: &Expr) -> Result<Datum> {
        let child = try!(self.get_one_child(expr));
        let d = try!(self.eval(ctx, child));
        match d {
            Datum::Dec(_) => {}
            _ => return invalid_type_error(&d, TYPE_DECIMAL),
        }
        let s = try!(d.into_string());
        let duration = try!(Duration::parse(s.as_bytes(),
                                            expr.get_field_type().get_decimal() as i8));
        Ok(Datum::Dur(duration))
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
    use tipb::expression::{FieldType, ExprType, ScalarFuncSig};
    use coprocessor::codec::datum::Datum;
    use coprocessor::codec::mysql::{Decimal, Time, Duration};
    use coprocessor::codec::mysql::types;
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

    test_eval!(test_cast_real_as_int,
               vec![(build_expr_with_sig(vec![Datum::F64(1.5)],
                                         ExprType::ScalarFunc,
                                         ScalarFuncSig::CastRealAsInt,
                                         {
                                             let mut ft = FieldType::new();
                                             ft.set_flag(types::UNSIGNED_FLAG as u32);
                                             ft
                                         }),
                     Datum::U64(2)),
                    (build_expr_with_sig(vec![Datum::F64(-1.5)],
                                         ExprType::ScalarFunc,
                                         ScalarFuncSig::CastRealAsInt,
                                         FieldType::new()),
                     Datum::I64(-2))]);

    test_eval!(test_cast_real_as_real,
               vec![(build_expr_with_sig(vec![Datum::F64(1.0)],
                                         ExprType::ScalarFunc,
                                         ScalarFuncSig::CastRealAsReal,
                                         FieldType::new()),
                     Datum::F64(1.0))]);

    test_eval!(test_cast_real_as_string,
               vec![(build_expr_with_sig(vec![Datum::F64(5.1415926535)],
                                         ExprType::ScalarFunc,
                                         ScalarFuncSig::CastRealAsString,
                                         {
                                             let mut ft = FieldType::new();
                                             ft.set_flen(12);
                                             ft
                                         }),
                     Datum::Bytes(b"5.1415926535".to_vec()))]);

    test_eval!(test_cast_real_as_decimal,
               vec![(build_expr_with_sig(vec![Datum::F64(1000.0)],
                                         ExprType::ScalarFunc,
                                         ScalarFuncSig::CastRealAsDecimal,
                                         {
                                             let mut ft = FieldType::new();
                                             ft.set_flen(5);
                                             ft.set_decimal(1);
                                             ft
                                         }),
                     Datum::Dec(Decimal::from_f64(1000.0).unwrap()))]);

    test_eval!(test_cast_real_as_time,
               vec![(build_expr_with_sig(vec![Datum::F64(20121231113045.123345)],
                                         ExprType::ScalarFunc,
                                         ScalarFuncSig::CastRealAsTime,
                                         {
                                             let mut ft = FieldType::new();
                                             ft.set_flen(18);
                                             ft
                                         }),
                     Datum::Time(Time::parse_utc_datetime("2012-12-31 11:30:45.123345", 0)
                        .unwrap()))]);

    test_eval!(test_cast_real_as_duration,
               vec![(build_expr_with_sig(vec![Datum::F64(101112.123456)],
                                         ExprType::ScalarFunc,
                                         ScalarFuncSig::CastRealAsDuration,
                                         FieldType::new()),
                     Datum::Dur(Duration::parse(b"10:11:12", 0).unwrap()))]);

    test_eval!(test_cast_decimal_as_int,
               vec![(build_expr_with_sig(vec![Datum::Dec(Decimal::from(1))],
                                         ExprType::ScalarFunc,
                                         ScalarFuncSig::CastDecimalAsInt,
                                         {
                                             let mut ft = FieldType::new();
                                             ft.set_flag(types::UNSIGNED_FLAG as u32);
                                             ft
                                         }),
                     Datum::U64(1)),
                    (build_expr_with_sig(vec![Datum::Dec(Decimal::from(-1))],
                                         ExprType::ScalarFunc,
                                         ScalarFuncSig::CastDecimalAsInt,
                                         FieldType::new()),
                     Datum::I64(-1))]);

    test_eval!(test_cast_decimal_as_real,
               vec![(build_expr_with_sig(vec![Datum::Dec(Decimal::from_f64(5.14159).unwrap())],
                                         ExprType::ScalarFunc,
                                         ScalarFuncSig::CastDecimalAsReal,
                                         FieldType::new()),
                     Datum::F64(5.14159))]);

    test_eval!(test_cast_decimal_as_string,
               vec![(build_expr_with_sig(vec![Datum::Dec(Decimal::from(12345))],
                                         ExprType::ScalarFunc,
                                         ScalarFuncSig::CastDecimalAsString,
                                         {
                                             let mut ft = FieldType::new();
                                             ft.set_flen(5);
                                             ft
                                         }),
                     Datum::Bytes(b"12345".to_vec()))]);

    test_eval!(test_cast_decimal_as_decimal,
               vec![(build_expr_with_sig(vec![Datum::Dec(Decimal::from_f64(1000.0).unwrap())],
                                         ExprType::ScalarFunc,
                                         ScalarFuncSig::CastDecimalAsDecimal,
                                         {
                                             let mut ft = FieldType::new();
                                             ft.set_flen(5);
                                             ft.set_decimal(1);
                                             ft
                                         }),
                     Datum::Dec(Decimal::from_f64(1000.0).unwrap()))]);

    test_eval!(test_cast_decimal_as_time,
               vec![(build_expr_with_sig(vec![Datum::Dec(Decimal::from_f64(20121231113045.123345).unwrap())],
                                         ExprType::ScalarFunc,
                                         ScalarFuncSig::CastDecimalAsTime,
                                         {
                                             let mut ft = FieldType::new();
                                             ft.set_flen(18);
                                             ft
                                         }),
                     Datum::Time(Time::parse_utc_datetime("2012-12-31 11:30:45.123345", 0)
                                 .unwrap()))]);

    test_eval!(test_cast_decimal_as_duration,
               vec![(build_expr_with_sig(vec![Datum::Dec(Decimal::from_f64(101112.123456).unwrap())],
                                         ExprType::ScalarFunc,
                                         ScalarFuncSig::CastDecimalAsDuration,
                                         FieldType::new()),
                     Datum::Dur(Duration::parse(b"10:11:12", 0).unwrap()))]);
}
