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

use coprocessor::codec::Datum;
use coprocessor::codec::mysql::{Decimal, Duration, Json, Time};
use super::{Column, Result};

impl Column {
    pub fn eval(&self, row: &[Datum]) -> Datum {
        row[self.offset].clone()
    }

    #[inline]
    pub fn eval_int(&self, row: &[Datum]) -> Result<Option<i64>> {
        row[self.offset].as_int()
    }

    #[inline]
    pub fn eval_real(&self, row: &[Datum]) -> Result<Option<f64>> {
        row[self.offset].as_real()
    }

    #[inline]
    pub fn eval_decimal<'a>(&self, row: &'a [Datum]) -> Result<Option<Cow<'a, Decimal>>> {
        row[self.offset].as_decimal()
    }

    #[inline]
    pub fn eval_string<'a>(&self, row: &'a [Datum]) -> Result<Option<Cow<'a, [u8]>>> {
        row[self.offset].as_string()
    }

    #[inline]
    pub fn eval_time<'a>(&self, row: &'a [Datum]) -> Result<Option<Cow<'a, Time>>> {
        row[self.offset].as_time()
    }

    #[inline]
    pub fn eval_duration<'a>(&self, row: &'a [Datum]) -> Result<Option<Cow<'a, Duration>>> {
        row[self.offset].as_duration()
    }

    #[inline]
    pub fn eval_json<'a>(&self, row: &'a [Datum]) -> Result<Option<Cow<'a, Json>>> {
        row[self.offset].as_json()
    }
}

#[cfg(test)]
mod test {
    use std::u64;
    use coprocessor::codec::Datum;
    use coprocessor::codec::mysql::{Decimal, Duration, Json, Time};
    use coprocessor::dag::expr::{Expression, StatementContext};
    use coprocessor::select::xeval::evaluator::test::col_expr;

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
    fn test_column_eval() {
        let dec = "1.1".parse::<Decimal>().unwrap();
        let s = "你好".as_bytes().to_owned();
        let dur = Duration::parse(b"01:00:00", 0).unwrap();

        let row = vec![
            Datum::Null,
            Datum::I64(-30),
            Datum::U64(u64::MAX),
            Datum::F64(124.32),
            Datum::Dec(dec.clone()),
            Datum::Bytes(s.clone()),
            Datum::Dur(dur.clone()),
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

        let ctx = StatementContext::default();
        for (ii, exp) in expecteds.iter().enumerate().take(row.len()) {
            let c = col_expr(ii as i64);
            let e = Expression::build(&ctx, c).unwrap();

            let i = e.eval_int(&ctx, &row).unwrap_or(None);
            let r = e.eval_real(&ctx, &row).unwrap_or(None);
            let dec = e.eval_decimal(&ctx, &row)
                .unwrap_or(None)
                .map(|t| t.into_owned());
            let s = e.eval_string(&ctx, &row)
                .unwrap_or(None)
                .map(|t| t.into_owned());
            let t = e.eval_time(&ctx, &row)
                .unwrap_or(None)
                .map(|t| t.into_owned());
            let dur = e.eval_duration(&ctx, &row)
                .unwrap_or(None)
                .map(|t| t.into_owned());
            let j = e.eval_json(&ctx, &row)
                .unwrap_or(None)
                .map(|t| t.into_owned());

            let result = EvalResults(i, r, dec, s, t, dur, j);
            assert_eq!(*exp, result);
        }
    }
}
