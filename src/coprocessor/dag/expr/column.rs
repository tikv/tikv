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

use super::{Column, EvalContext, Result};
use coprocessor::codec::mysql::{types, Decimal, Duration, Json, Time};
use coprocessor::codec::Datum;
use std::borrow::Cow;
use std::str;

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
    pub fn eval_string<'a>(
        &self,
        ctx: &mut EvalContext,
        row: &'a [Datum],
    ) -> Result<Option<Cow<'a, [u8]>>> {
        if let Datum::Null = row[self.offset] {
            return Ok(None);
        }
        if types::is_hybrid_type(self.tp.get_tp() as u8) {
            let s = row[self.offset].to_string()?.into_bytes();
            return Ok(Some(Cow::Owned(s)));
        }

        if !ctx.cfg.pad_char_to_full_length || self.tp.get_tp() != i32::from(types::STRING) {
            return row[self.offset].as_string();
        }

        let res = row[self.offset].as_string()?.unwrap();
        let cur_len = str::from_utf8(res.as_ref())?.chars().count();
        let flen = self.tp.get_flen() as usize;
        if flen <= cur_len {
            return Ok(Some(res));
        }
        let new_len = flen - cur_len + res.len();
        let mut s = res.into_owned();
        s.resize(new_len, b' ');
        Ok(Some(Cow::Owned(s)))
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
    use std::sync::Arc;
    use std::{str, u64};

    use tipb::expression::FieldType;

    use coprocessor::codec::mysql::{types, Decimal, Duration, Json, Time};
    use coprocessor::codec::Datum;
    use coprocessor::dag::expr::test::col_expr;
    use coprocessor::dag::expr::{EvalConfig, EvalContext, Expression};

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

        let mut ctx = EvalContext::default();
        for (ii, exp) in expecteds.iter().enumerate().take(row.len()) {
            let c = col_expr(ii as i64);
            let e = Expression::build(&mut ctx, c).unwrap();

            let i = e.eval_int(&mut ctx, &row).unwrap_or(None);
            let r = e.eval_real(&mut ctx, &row).unwrap_or(None);
            let dec = e
                .eval_decimal(&mut ctx, &row)
                .unwrap_or(None)
                .map(|t| t.into_owned());
            let s = e
                .eval_string(&mut ctx, &row)
                .unwrap_or(None)
                .map(|t| t.into_owned());
            let t = e
                .eval_time(&mut ctx, &row)
                .unwrap_or(None)
                .map(|t| t.into_owned());
            let dur = e
                .eval_duration(&mut ctx, &row)
                .unwrap_or(None)
                .map(|t| t.into_owned());
            let j = e
                .eval_json(&mut ctx, &row)
                .unwrap_or(None)
                .map(|t| t.into_owned());

            let result = EvalResults(i, r, dec, s, t, dur, j);
            assert_eq!(*exp, result);
        }
    }

    #[test]
    fn test_with_pad_char_to_full_length() {
        let mut ctx = EvalContext::default();
        let mut pad_char_ctx_cfg = EvalConfig::default();
        pad_char_ctx_cfg.pad_char_to_full_length = true;
        let mut pad_char_ctx = EvalContext::new(Arc::new(pad_char_ctx_cfg));

        let mut c = col_expr(0);
        let mut field_tp = FieldType::new();
        let flen = 16;
        field_tp.set_tp(i32::from(types::STRING));
        field_tp.set_flen(flen);
        c.set_field_type(field_tp);
        let e = Expression::build(&mut ctx, c).unwrap();
        // test without pad_char_to_full_length
        let s = "你好".as_bytes().to_owned();
        let row = vec![Datum::Bytes(s.clone())];
        let res = e.eval_string(&mut ctx, &row).unwrap().unwrap();
        assert_eq!(res.to_owned(), s.clone());
        // test with pad_char_to_full_length
        let res = e.eval_string(&mut pad_char_ctx, &row).unwrap().unwrap();
        let s = str::from_utf8(res.as_ref()).unwrap();
        assert_eq!(s.chars().count(), flen as usize);
    }

    #[test]
    fn test_hybrid_type() {
        let mut ctx = EvalContext::default();
        let row = vec![Datum::I64(12)];
        let hybrid_cases = vec![types::ENUM, types::BIT, types::SET];
        let in_hybrid_cases = vec![types::JSON, types::NEW_DECIMAL, types::SHORT];
        for tp in hybrid_cases {
            let mut c = col_expr(0);
            let mut field_tp = FieldType::new();
            field_tp.set_tp(i32::from(tp));
            c.set_field_type(field_tp);
            let e = Expression::build(&mut ctx, c).unwrap();
            let res = e.eval_string(&mut ctx, &row).unwrap().unwrap();
            assert_eq!(res.as_ref(), b"12");
        }

        for tp in in_hybrid_cases {
            let mut c = col_expr(0);
            let mut field_tp = FieldType::new();
            field_tp.set_tp(i32::from(tp));
            c.set_field_type(field_tp);
            let e = Expression::build(&mut ctx, c).unwrap();
            let res = e.eval_string(&mut ctx, &row);
            assert!(res.is_err());
        }
    }
}
