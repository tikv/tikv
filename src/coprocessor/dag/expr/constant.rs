// Copyright 2017 TiKV Project Authors. Licensed under Apache-2.0.

use std::borrow::Cow;

use super::{Constant, Result};
use crate::coprocessor::codec::convert::{
    convert_bytes_to_int, convert_datetime_to_int, convert_decimal_to_int, convert_duration_to_int,
    convert_float_to_int, convert_json_to_int,
};
use crate::coprocessor::codec::mysql::{Decimal, Duration, Json, Time};
use crate::coprocessor::codec::{Datum, Error};
use crate::coprocessor::dag::expr::EvalContext;
use cop_datatype::FieldTypeTp;

impl Datum {
    #[inline]
    pub fn as_bool(&self, ctx: &mut EvalContext) -> Result<Option<bool>> {
        match *self {
            Datum::Null => Ok(None),
            Datum::I64(i) => Ok(Some(i != 0)),
            Datum::U64(u) => Ok(Some(u != 0)),
            Datum::F64(f) => Ok(Some(f.round() != 0f64)),
            Datum::Dur(d) => Ok(Some(!d.is_zero())),
            Datum::Bytes(ref bs) => Ok(Some(
                !bs.is_empty() && convert_bytes_to_int(ctx, bs, FieldTypeTp::LongLong)? != 0,
            )),
            Datum::Dec(ref d) => Ok(Some(d.as_f64()?.round() != 0f64)),
            Datum::Time(ref x) => Ok(Some(!x.is_zero())),
            Datum::Json(ref j) => Err(Error::InvalidDataType(format!(
                "cannot convert {:?}(type Json) to bool",
                j
            ))),
            Datum::Min | Datum::Max => {
                panic!("This type[{}] should not appear here", *self);
            }
        }
    }

    #[inline]
    pub fn as_int(&self, ctx: &mut EvalContext) -> Result<Option<i64>> {
        match *self {
            Datum::Null => Ok(None),
            Datum::I64(i) => Ok(Some(i)),
            Datum::U64(u) => Ok(Some(u as i64)),
            Datum::F64(f) => {
                let n = convert_float_to_int(ctx, f, FieldTypeTp::LongLong)?;
                Ok(Some(n))
            }
            Datum::Dur(d) => {
                let n = convert_duration_to_int(ctx, d, FieldTypeTp::LongLong)?;
                Ok(Some(n))
            }
            Datum::Bytes(ref bs) => {
                let n = convert_bytes_to_int(ctx, bs, FieldTypeTp::LongLong)?;
                Ok(Some(n))
            }
            Datum::Dec(ref d) => {
                let n = convert_decimal_to_int(ctx, d, FieldTypeTp::LongLong)?;
                Ok(Some(n))
            }
            Datum::Time(ref t) => {
                let n = convert_datetime_to_int(ctx, t, FieldTypeTp::LongLong)?;
                Ok(Some(n))
            }
            Datum::Json(ref j) => {
                let n = convert_json_to_int(ctx, j, FieldTypeTp::LongLong)?;
                Ok(Some(n))
            }
            Datum::Min | Datum::Max => {
                panic!("This type[{}] should not appear here", *self);
            }
        }
    }

    #[inline]
    pub fn as_real(&self) -> Result<Option<f64>> {
        match *self {
            Datum::Null => Ok(None),
            Datum::F64(f) => Ok(Some(f)),
            _ => Err(box_err!("Can't eval_real from Datum")),
        }
    }

    #[inline]
    pub fn as_decimal(&self) -> Result<Option<Cow<'_, Decimal>>> {
        match *self {
            Datum::Null => Ok(None),
            Datum::Dec(ref d) => Ok(Some(Cow::Borrowed(d))),
            _ => Err(box_err!("Can't eval_decimal from Datum")),
        }
    }

    #[inline]
    pub fn as_string(&self) -> Result<Option<Cow<'_, [u8]>>> {
        match *self {
            Datum::Null => Ok(None),
            Datum::Bytes(ref b) => Ok(Some(Cow::Borrowed(b))),
            _ => Err(box_err!("Can't eval_string from Datum")),
        }
    }

    #[inline]
    pub fn as_time(&self) -> Result<Option<Cow<'_, Time>>> {
        match *self {
            Datum::Null => Ok(None),
            Datum::Time(ref t) => Ok(Some(Cow::Borrowed(t))),
            _ => Err(box_err!("Can't eval_time from Datum")),
        }
    }

    #[inline]
    pub fn as_duration(&self) -> Result<Option<Duration>> {
        match *self {
            Datum::Null => Ok(None),
            Datum::Dur(d) => Ok(Some(d)),
            _ => Err(box_err!("Can't eval_duration from Datum")),
        }
    }

    #[inline]
    pub fn as_json(&self) -> Result<Option<Cow<'_, Json>>> {
        match *self {
            Datum::Null => Ok(None),
            Datum::Json(ref j) => Ok(Some(Cow::Borrowed(j))),
            _ => Err(box_err!("Can't eval_json from Datum")),
        }
    }
}

impl Constant {
    pub fn eval(&self) -> Datum {
        self.val.clone()
    }

    pub fn eval_bool(&self, ctx: &mut EvalContext) -> Result<Option<bool>> {
        self.val.as_bool(ctx)
    }

    #[inline]
    pub fn eval_int(&self, ctx: &mut EvalContext) -> Result<Option<i64>> {
        self.val.as_int(ctx)
    }

    #[inline]
    pub fn eval_real(&self) -> Result<Option<f64>> {
        self.val.as_real()
    }

    #[inline]
    pub fn eval_decimal(&self) -> Result<Option<Cow<'_, Decimal>>> {
        self.val.as_decimal()
    }

    #[inline]
    pub fn eval_string(&self) -> Result<Option<Cow<'_, [u8]>>> {
        self.val.as_string()
    }

    #[inline]
    pub fn eval_time(&self) -> Result<Option<Cow<'_, Time>>> {
        self.val.as_time()
    }

    #[inline]
    pub fn eval_duration(&self) -> Result<Option<Duration>> {
        self.val.as_duration()
    }

    #[inline]
    pub fn eval_json(&self) -> Result<Option<Cow<'_, Json>>> {
        self.val.as_json()
    }
}

#[cfg(test)]
mod tests {
    use crate::coprocessor::codec::mysql::{Decimal, Duration, Json, Time};
    use crate::coprocessor::codec::{Datum, Error};
    use crate::coprocessor::dag::expr::tests::datum_expr;
    use crate::coprocessor::dag::expr::{EvalContext, Expression};
    use std::u64;

    #[derive(PartialEq, Debug)]
    struct EvalResults(
        Option<i64>,
        Option<f64>,
        Option<Decimal>,
        Option<Vec<u8>>,
        Option<Time>,
        Option<Duration>,
        Option<Json>,
    );

    #[test]
    fn test_constant_eval() {
        let dec = "1.1".parse::<Decimal>().unwrap();
        let s = "你好".as_bytes().to_owned();
        let dur = Duration::parse(b"01:00:00", 0).unwrap();

        let tests = vec![
            (
                datum_expr(Datum::Null),
                EvalResults(None, None, None, None, None, None, None),
            ),
            (
                datum_expr(Datum::I64(-30)),
                EvalResults(Some(-30), None, None, None, None, None, None),
            ),
            (
                datum_expr(Datum::U64(u64::MAX)),
                EvalResults(Some(-1), None, None, None, None, None, None),
            ),
            (
                datum_expr(Datum::F64(124.32)),
                EvalResults(Some(124), Some(124.32), None, None, None, None, None),
            ),
            (
                datum_expr(Datum::Dec(dec.clone())),
                EvalResults(Some(1), None, Some(dec.clone()), None, None, None, None),
            ),
            (
                datum_expr(Datum::Bytes(s.clone())),
                EvalResults(None, None, None, Some(s.clone()), None, None, None),
            ),
            (
                datum_expr(Datum::Dur(dur)),
                EvalResults(Some(10000), None, None, None, None, Some(dur), None),
            ),
        ];

        let mut ctx = EvalContext::default();
        for (case, expected) in tests {
            let e = Expression::build(&ctx, case).unwrap();

            let i = e.eval_int(&mut ctx, &[]).unwrap_or(None);
            let r = e.eval_real(&mut ctx, &[]).unwrap_or(None);
            let dec = e
                .eval_decimal(&mut ctx, &[])
                .unwrap_or(None)
                .map(|t| t.into_owned());
            let s = e
                .eval_string(&mut ctx, &[])
                .unwrap_or(None)
                .map(|t| t.into_owned());
            let t = e
                .eval_time(&mut ctx, &[])
                .unwrap_or(None)
                .map(|t| t.into_owned());
            let dur = e.eval_duration(&mut ctx, &[]).unwrap_or(None);
            let j = e
                .eval_json(&mut ctx, &[])
                .unwrap_or(None)
                .map(|t| t.into_owned());

            let result = EvalResults(i, r, dec, s, t, dur, j);
            assert_eq!(result, expected);
        }
    }

    #[test]
    fn test_datum_as_bool() {
        let dec = "1.1".parse::<Decimal>().unwrap();
        let s = b"10".to_vec();
        let dur = Duration::parse(b"01:00:00", 0).unwrap();
        let json = Json::String(String::from("test_json"));

        let tests = vec![
            (Datum::Null, Ok(None)),
            (Datum::I64(0), Ok(Some(false))),
            (Datum::U64(u64::MAX), Ok(Some(true))),
            (Datum::F64(124.32), Ok(Some(true))),
            (Datum::Dec(dec), Ok(Some(true))),
            (Datum::Bytes(s), Ok(Some(true))),
            (Datum::Dur(dur), Ok(Some(true))),
            (
                Datum::Json(json),
                Err(Error::InvalidDataType(String::new())),
            ),
        ];

        let mut ctx = EvalContext::default();
        for (case, expected) in tests {
            let r = case.as_bool(&mut ctx);
            assert_eq!(
                r.is_err(),
                expected.is_err(),
                "{}, {}, {}",
                case,
                expected.is_err(),
                r.is_err()
            );
            if let Ok(x) = r {
                assert_eq!(x, expected.unwrap())
            }
        }
    }
}
