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

use std::i64;
use protobuf::ProtobufEnum;
use chrono::naive::time::NaiveTime;
use chrono::offset::fixed::FixedOffset;
use tipb::expression::{DataType, Expr};
use coprocessor::codec::datum::{Datum, produce_dec_with_specified_tp,
                                produce_str_with_specified_tp};
use coprocessor::codec::mysql::{Decimal, Time, Duration, DEFAULT_FSP, has_unsigned_flag};
use coprocessor::codec::convert;
use super::{Evaluator, EvalContext, Result, Error, ERROR_UNIMPLEMENTED};

const TYPE_INT: &'static str = "int";

fn invalid_type_error(datum: &Datum, expected_type: &str) -> Result<Datum> {
    Err(Error::Eval(format!("invalid expr type: {:?}, expect: {}", datum, expected_type)))
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
            Datum::I64(_) => {
                if has_unsigned_flag(expr.get_field_type().get_flag() as u64) {
                    Ok(Datum::U64(d.u64()))
                } else {
                    Ok(d)
                }
            }
            Datum::U64(u) => {
                if has_unsigned_flag(expr.get_field_type().get_flag() as u64) {
                    Ok(d)
                } else {
                    let i = try!(convert::convert_uint_to_int(u, i64::MAX));
                    Ok(Datum::I64(i))
                }
            }
            _ => invalid_type_error(&d, TYPE_INT),
        }
    }

    pub fn cast_int_as_real(&mut self, ctx: &EvalContext, expr: &Expr) -> Result<Datum> {
        let child = try!(self.get_one_child(expr));
        let d = try!(self.eval(ctx, child));
        match d {
            Datum::I64(i) => Ok(Datum::F64(i as f64)),
            Datum::U64(u) => Ok(Datum::F64(u as f64)),
            _ => invalid_type_error(&d, TYPE_INT),
        }
    }

    pub fn cast_int_as_string(&mut self, ctx: &EvalContext, expr: &Expr) -> Result<Datum> {
        let child = try!(self.get_one_child(expr));
        let d = try!(self.eval(ctx, child));
        match d {
            Datum::I64(_) | Datum::U64(_) => {}
            _ => return invalid_type_error(&d, TYPE_INT),
        }
        let s = try!(d.into_string());
        let s = try!(produce_str_with_specified_tp(s, expr.get_field_type(), ctx));
        Ok(Datum::Bytes(s.into_bytes()))
    }

    pub fn cast_int_as_decimal(&mut self, ctx: &EvalContext, expr: &Expr) -> Result<Datum> {
        let child = try!(self.get_one_child(expr));
        let datum = try!(self.eval(ctx, child));
        let decimal = match datum {
            Datum::I64(_) => Decimal::from(datum.i64()),
            Datum::U64(_) => Decimal::from(datum.u64()),
            _ => return invalid_type_error(&datum, TYPE_INT),
        };
        let decimal = try!(produce_dec_with_specified_tp(decimal, expr.get_field_type(), ctx));
        Ok(Datum::Dec(decimal))
    }

    pub fn cast_int_as_time(&mut self, ctx: &EvalContext, expr: &Expr) -> Result<Datum> {
        let child = try!(self.get_one_child(expr));
        let d = try!(self.eval(ctx, child));
        try!(check_datum_int(&d));
        let s = try!(d.into_string());
        let decimal = expr.get_field_type().get_decimal();
        let offset = match FixedOffset::east_opt(decimal) {
            None => return Err(Error::Eval(format!("invalid tz offset {}", decimal))),
            Some(tz) => tz,
        };
        let mut t = try!(Time::parse_datetime(&s, DEFAULT_FSP, &offset));
        let data_type = expr.get_field_type().get_tp();
        if data_type == DataType::TypeDate {
            let dt = t.get_time().date().and_time(NaiveTime::from_hms(0, 0, 0)).unwrap();
            t = try!(Time::new(dt, data_type.value() as u8, decimal as i8));
        }
        Ok(Datum::Time(t))
    }

    pub fn cast_int_as_duration(&mut self, ctx: &EvalContext, expr: &Expr) -> Result<Datum> {
        let child = try!(self.get_one_child(expr));
        let d = try!(self.eval(ctx, child));
        try!(check_datum_int(&d));
        let s = try!(d.into_string());
        let duration = try!(Duration::parse(s.as_bytes(),
                                            expr.get_field_type().get_decimal() as i8));
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
    use std::{i64, u64};
    use chrono::offset::fixed::FixedOffset;
    use tipb::expression::{FieldType, ExprType, ScalarFuncSig};
    use coprocessor::codec::datum::Datum;
    use coprocessor::codec::mysql::{Decimal, Time, Duration};
    use coprocessor::codec::mysql::{types, charset};
    use coprocessor::codec::field_type;
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
                                         ScalarFuncSig::CastIntAsInt,
                                         FieldType::new()),
                     Datum::I64(1)),
                    (build_expr_with_sig(vec![Datum::I64(1)],
                                         ExprType::ScalarFunc,
                                         ScalarFuncSig::CastIntAsInt,
                                         {
                                             let mut ft = FieldType::new();
                                             ft.set_flag(types::UNSIGNED_FLAG as u32);
                                             ft
                                         }),
                     Datum::U64(1)),
                    (build_expr_with_sig(vec![Datum::I64(-1)],
                                         ExprType::ScalarFunc,
                                         ScalarFuncSig::CastIntAsInt,
                                         FieldType::new()),
                     Datum::I64(-1)),
                    (build_expr_with_sig(vec![Datum::U64(i64::MAX as u64)],
                                         ExprType::ScalarFunc,
                                         ScalarFuncSig::CastIntAsInt,
                                         FieldType::new()),
                     Datum::I64(i64::MAX)),
                    (build_expr_with_sig(vec![Datum::U64(u64::MAX)],
                                         ExprType::ScalarFunc,
                                         ScalarFuncSig::CastIntAsInt,
                                         {
                                             let mut ft = FieldType::new();
                                             ft.set_flag(types::UNSIGNED_FLAG as u32);
                                             ft
                                         }),
                     Datum::U64(u64::MAX))]);

    test_eval!(test_cast_int_as_real,
               vec![(build_expr_with_sig(vec![Datum::I64(-1)],
                                         ExprType::ScalarFunc,
                                         ScalarFuncSig::CastIntAsReal,
                                         FieldType::new()),
                     Datum::F64(-1.0)),
                    (build_expr_with_sig(vec![Datum::U64(1)],
                                         ExprType::ScalarFunc,
                                         ScalarFuncSig::CastIntAsReal,
                                         FieldType::new()),
                     Datum::F64(1.0))]);

    test_eval!(test_cast_int_as_string,
               vec![(build_expr_with_sig(vec![Datum::I64(-1)],
                                         ExprType::ScalarFunc,
                                         ScalarFuncSig::CastIntAsString,
                                         {
                                             let mut ft = FieldType::new();
                                             ft.set_flen(field_type::UNSPECIFIED_LENGTH);
                                             ft
                                         }),
                     Datum::Bytes(b"-1".to_vec())),
                    (build_expr_with_sig(vec![Datum::U64(1)],
                                         ExprType::ScalarFunc,
                                         ScalarFuncSig::CastIntAsString,
                                         {
                                             let mut ft = FieldType::new();
                                             ft.set_flen(field_type::UNSPECIFIED_LENGTH);
                                             ft
                                         }),
                     Datum::Bytes(b"1".to_vec())),
                    (build_expr_with_sig(vec![Datum::I64(10000)],
                                         ExprType::ScalarFunc,
                                         ScalarFuncSig::CastIntAsString,
                                         {
                                             let mut ft = FieldType::new();
                                             ft.set_flen(5);
                                             ft.set_charset(charset::CHARSET_UTF8.to_owned());
                                             ft
                                         }),
                     Datum::Bytes(b"10000".to_vec()))]);

    // test_eval!(test_cast_int_as_decimal,
    //            vec![(build_expr_with_sig(vec![Datum::I64(-1)],
    //                                      ExprType::ScalarFunc,
    //                                      ScalarFuncSig::CastIntAsDecimal),
    //                  Datum::Dec(Decimal::from(-1))),
    //                 (build_expr_with_sig(vec![Datum::U64(1)],
    //                                      ExprType::ScalarFunc,
    //                                      ScalarFuncSig::CastIntAsDecimal),
    //                  Datum::Dec(Decimal::from(1)))]);

    test_eval!(test_cast_int_as_time,
               vec![(build_expr_with_sig(vec![Datum::I64(20121231113045)],
                                         ExprType::ScalarFunc,
                                         ScalarFuncSig::CastIntAsTime,
                                         FieldType::new()),
                     Datum::Time(Time::parse_utc_datetime("2012-12-31 11:30:45", 0).unwrap())),
                    (build_expr_with_sig(vec![Datum::U64(121231)],
                                         ExprType::ScalarFunc,
                                         ScalarFuncSig::CastIntAsTime,
                                         {
                                             let mut ft = FieldType::new();
                                             ft.set_decimal(1);
                                             ft
                                         }),
                     Datum::Time(Time::parse_datetime("2012-12-31 00:00:00",
                                                      0,
                                                      &FixedOffset::east(1))
                        .unwrap()))]);

    test_eval!(test_cast_int_as_duration,
               vec![(build_expr_with_sig(vec![Datum::I64(101112)],
                                         ExprType::ScalarFunc,
                                         ScalarFuncSig::CastIntAsDuration,
                                         FieldType::new()),
                     Datum::Dur(Duration::parse(b"10:11:12", 0).unwrap()))]);
}
