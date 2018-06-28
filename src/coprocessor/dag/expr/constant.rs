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

use std::borrow::Cow;

use super::{Constant, Result};
use coprocessor::codec::mysql::{Decimal, Duration, Json, Time};
use coprocessor::codec::Datum;

impl Datum {
    #[inline]
    pub fn as_int(&self) -> Result<Option<i64>> {
        match *self {
            Datum::Null => Ok(None),
            Datum::I64(i) => Ok(Some(i)),
            Datum::U64(u) => Ok(Some(u as i64)),
            _ => Err(box_err!("Can't eval_int from Datum")),
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
    pub fn as_decimal(&self) -> Result<Option<Cow<Decimal>>> {
        match *self {
            Datum::Null => Ok(None),
            Datum::Dec(ref d) => Ok(Some(Cow::Borrowed(d))),
            _ => Err(box_err!("Can't eval_decimal from Datum")),
        }
    }

    #[inline]
    pub fn as_string(&self) -> Result<Option<Cow<[u8]>>> {
        match *self {
            Datum::Null => Ok(None),
            Datum::Bytes(ref b) => Ok(Some(Cow::Borrowed(b))),
            _ => Err(box_err!("Can't eval_string from Datum")),
        }
    }

    #[inline]
    pub fn as_time(&self) -> Result<Option<Cow<Time>>> {
        match *self {
            Datum::Null => Ok(None),
            Datum::Time(ref t) => Ok(Some(Cow::Borrowed(t))),
            _ => Err(box_err!("Can't eval_time from Datum")),
        }
    }

    #[inline]
    pub fn as_duration(&self) -> Result<Option<Cow<Duration>>> {
        match *self {
            Datum::Null => Ok(None),
            Datum::Dur(ref d) => Ok(Some(Cow::Borrowed(d))),
            _ => Err(box_err!("Can't eval_duration from Datum")),
        }
    }

    #[inline]
    pub fn as_json(&self) -> Result<Option<Cow<Json>>> {
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

    #[inline]
    pub fn eval_int(&self) -> Result<Option<i64>> {
        self.val.as_int()
    }

    #[inline]
    pub fn eval_real(&self) -> Result<Option<f64>> {
        self.val.as_real()
    }

    #[inline]
    pub fn eval_decimal(&self) -> Result<Option<Cow<Decimal>>> {
        self.val.as_decimal()
    }

    #[inline]
    pub fn eval_string(&self) -> Result<Option<Cow<[u8]>>> {
        self.val.as_string()
    }

    #[inline]
    pub fn eval_time(&self) -> Result<Option<Cow<Time>>> {
        self.val.as_time()
    }

    #[inline]
    pub fn eval_duration(&self) -> Result<Option<Cow<Duration>>> {
        self.val.as_duration()
    }

    #[inline]
    pub fn eval_json(&self) -> Result<Option<Cow<Json>>> {
        self.val.as_json()
    }
}

#[cfg(test)]
mod test {
    use coprocessor::codec::mysql::{Decimal, Duration, Json, Time};
    use coprocessor::codec::Datum;
    use coprocessor::dag::expr::test::datum_expr;
    use coprocessor::dag::expr::{EvalContext, Expression};
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
            datum_expr(Datum::Null),
            datum_expr(Datum::I64(-30)),
            datum_expr(Datum::U64(u64::MAX)),
            datum_expr(Datum::F64(124.32)),
            datum_expr(Datum::Dec(dec.clone())),
            datum_expr(Datum::Bytes(s.clone())),
            datum_expr(Datum::Dur(dur.clone())),
        ];

        let expecteds = vec![
            EvalResults(None, None, None, None, None, None, None),
            EvalResults(Some(-30), None, None, None, None, None, None),
            EvalResults(Some(-1), None, None, None, None, None, None),
            EvalResults(None, Some(124.32), None, None, None, None, None),
            EvalResults(None, None, Some(dec.clone()), None, None, None, None),
            EvalResults(None, None, None, Some(s.clone()), None, None, None),
            EvalResults(None, None, None, None, None, Some(dur.clone()), None),
        ];

        let mut ctx = EvalContext::default();
        for (case, expected) in tests.into_iter().zip(expecteds.into_iter()) {
            let e = Expression::build(&mut ctx, case).unwrap();

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
            let dur = e
                .eval_duration(&mut ctx, &[])
                .unwrap_or(None)
                .map(|t| t.into_owned());
            let j = e
                .eval_json(&mut ctx, &[])
                .unwrap_or(None)
                .map(|t| t.into_owned());

            let result = EvalResults(i, r, dec, s, t, dur, j);
            assert_eq!(expected, result);
        }
    }
}
