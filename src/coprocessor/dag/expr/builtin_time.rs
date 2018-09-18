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

use super::{EvalContext, Result, ScalarFunc};
use chrono::offset::TimeZone;
use chrono::Datelike;
use coprocessor::codec::error::Error;
use coprocessor::codec::mysql::{self, Time};
use coprocessor::codec::Datum;
use std::borrow::Cow;

impl ScalarFunc {
    #[inline]
    pub fn date_format<'a, 'b: 'a>(
        &'b self,
        ctx: &mut EvalContext,
        row: &'a [Datum],
    ) -> Result<Option<Cow<'a, [u8]>>> {
        let e = match self.children[0].eval_time(ctx, row)? {
            Some(res) => if !res.invalid_zero() {
                let format_mask = try_opt!(self.children[1].eval_string_and_decode(ctx, row));
                let res = res.date_format(format_mask.into_owned())?;
                return Ok(Some(Cow::Owned(res.into_bytes())));
            } else {
                Error::incorrect_datetime_value(&format!("{}", res))
            },
            None => Error::incorrect_datetime_value("None"),
        };
        Error::handle_invalid_time_error(ctx, e)?;
        Ok(None)
    }

    #[inline]
    pub fn date<'a, 'b: 'a>(
        &'b self,
        ctx: &mut EvalContext,
        row: &'a [Datum],
    ) -> Result<Option<Cow<'a, Time>>> {
        let e = match self.children[0].eval_time(ctx, row)? {
            Some(mut t) => if !t.is_zero() {
                let mut res = t.to_mut().clone();
                res.set_tp(mysql::types::DATE).unwrap();
                return Ok(Some(Cow::Owned(res)));
            } else {
                Error::incorrect_datetime_value(&format!("{}", t))
            },
            None => Error::incorrect_datetime_value("None"),
        };
        Error::handle_invalid_time_error(ctx, e)?;
        Ok(None)
    }

    #[inline]
    pub fn month<'a, 'b: 'a>(
        &'b self,
        ctx: &mut EvalContext,
        row: &'a [Datum],
    ) -> Result<Option<i64>> {
        let t = match self.children[0].eval_time(ctx, row) {
            Err(err) => return Error::handle_invalid_time_error(ctx, err).map(|_| None),
            Ok(None) => {
                return Error::handle_invalid_time_error(
                    ctx,
                    Error::incorrect_datetime_value("None"),
                ).map(|_| None)
            }
            Ok(Some(res)) => res,
        };
        if t.is_zero() {
            return Error::handle_invalid_time_error(
                ctx,
                Error::incorrect_datetime_value(&format!("{}", t)),
            ).map(|_| None);
        }
        Ok(Some(i64::from(t.get_time().month())))
    }

    #[inline]
    pub fn last_day<'a, 'b: 'a>(
        &'b self,
        ctx: &mut EvalContext,
        row: &'a [Datum],
    ) -> Result<Option<Cow<'a, Time>>> {
        let e = match self.children[0].eval_time(ctx, row)? {
            Some(mut t) => if !t.is_zero() {
                let mut res = t.to_mut().clone();
                let time = t.get_time();
                res.set_time(
                    time.timezone()
                        .ymd_opt(time.year(), time.month(), t.last_day_of_month())
                        .and_hms_opt(0, 0, 0)
                        .unwrap(),
                );
                return Ok(Some(Cow::Owned(res)));
            } else {
                Error::incorrect_datetime_value(&format!("{}", t))
            },
            None => Error::incorrect_datetime_value("None"),
        };
        Error::handle_invalid_time_error(ctx, e)?;
        Ok(None)
    }
}

#[cfg(test)]
mod test {
    use coprocessor::codec::mysql::Time;
    use coprocessor::codec::Datum;
    use coprocessor::dag::expr::test::{datum_expr, scalar_func_expr};
    use coprocessor::dag::expr::*;
    use coprocessor::dag::expr::{EvalContext, Expression};
    use std::sync::Arc;
    use tipb::expression::ScalarFuncSig;

    #[test]
    fn test_date_format() {
        let tests = vec![
            (
                "2010-01-07 23:12:34.12345",
                "%b %M %m %c %D %d %e %j %k %h %i %p %r %T %s %f %U %u
                %V %v %a %W %w %X %x %Y %y %%",
                "Jan January 01 1 7th 07 7 007 23 11 12 PM 11:12:34 PM 23:12:34 34 123450 01 01
                01 01 Thu Thursday 4 2010 2010 2010 10 %",
            ),
            (
                "2012-12-21 23:12:34.123456",
                "%b %M %m %c %D %d %e %j %k %h %i %p %r %T %s %f %U
                %u %V %v %a %W %w %X %x %Y %y %%",
                "Dec December 12 12 21st 21 21 356 23 11 12 PM 11:12:34 PM 23:12:34 34 123456 51
                51 51 51 Fri Friday 5 2012 2012 2012 12 %",
            ),
            (
                "0000-01-01 00:00:00.123456",
                // Functions week() and yearweek() don't support multi mode,
                // so the result of "%U %u %V %Y" is different from MySQL.
                "%b %M %m %c %D %d %e %j %k %h %i %p %r %T %s %f %v
                %x %Y %y %%",
                "Jan January 01 1 1st 01 1 001 0 12 00 AM 12:00:00 AM 00:00:00 00 123456 52
                4294967295 0000 00 %",
            ),
            (
                "2016-09-3 00:59:59.123456",
                "abc%b %M %m %c %D %d %e %j %k %h %i %p %r %T %s %f %U
                %u %V %v %a %W %w %X %x %Y %y!123 %%xyz %z",
                "abcSep September 09 9 3rd 03 3 247 0 12 59 AM 12:59:59 AM 00:59:59 59 123456 35
                35 35 35 Sat Saturday 6 2016 2016 2016 16!123 %xyz z",
            ),
            (
                "2012-10-01 00:00:00",
                "%b %M %m %c %D %d %e %j %k %H %i %p %r %T %s %f %v
                %x %Y %y %%",
                "Oct October 10 10 1st 01 1 275 0 00 00 AM 12:00:00 AM 00:00:00 00 000000 40
                2012 2012 12 %",
            ),
        ];
        let mut ctx = EvalContext::default();
        for (arg1, arg2, exp) in tests {
            let arg1 = datum_expr(Datum::Time(Time::parse_utc_datetime(arg1, 6).unwrap()));
            let arg2 = datum_expr(Datum::Bytes(arg2.to_string().into_bytes()));
            let f = scalar_func_expr(ScalarFuncSig::DateFormatSig, &[arg1, arg2]);
            let op = Expression::build(&mut ctx, f).unwrap();
            let got = op.eval(&mut ctx, &[]).unwrap();
            assert_eq!(got, Datum::Bytes(exp.to_string().into_bytes()));
        }

        // test NULL case
        let arg1 = datum_expr(Datum::Null);
        let arg2 = datum_expr(Datum::Null);
        let f = scalar_func_expr(ScalarFuncSig::DateFormatSig, &[arg1, arg2]);
        let op = Expression::build(&mut ctx, f).unwrap();
        let got = op.eval(&mut ctx, &[]);
        match got {
            Ok(_) => assert!(false, "null should be wrong"),
            Err(_) => assert!(true),
        }

        // test zero case
        let cfg = EvalConfig::new()
            .set_by_flags(FLAG_IN_UPDATE_OR_DELETE_STMT)
            .set_sql_mode(MODE_ERROR_FOR_DIVISION_BY_ZERO)
            .set_strict_sql_mode(true);
        ctx = EvalContext::new(Arc::new(cfg));
        let arg1 = datum_expr(Datum::Null);
        let arg2 = datum_expr(Datum::Null);
        let f = scalar_func_expr(ScalarFuncSig::DateFormatSig, &[arg1, arg2]);
        let op = Expression::build(&mut ctx, f).unwrap();
        let got = op.eval(&mut ctx, &[]);
        match got {
            Ok(_) => assert!(false, "null should be wrong"),
            Err(_) => assert!(true),
        }
    }

    #[test]
    fn test_date() {
        let tests = vec![
            ("2011-11-11", "2011-11-11"),
            ("2011-11-11 10:10:10", "2011-11-11"),
        ];
        let mut ctx = EvalContext::default();
        for (arg, exp) in tests {
            let arg = datum_expr(Datum::Time(Time::parse_utc_datetime(arg, 6).unwrap()));
            let exp = Datum::Time(Time::parse_utc_datetime(exp, 6).unwrap());
            let f = scalar_func_expr(ScalarFuncSig::Date, &[arg]);
            let op = Expression::build(&mut ctx, f).unwrap();
            let got = op.eval(&mut ctx, &[]).unwrap();
            assert_eq!(got, exp);
        }

        // test NULL case
        let input = datum_expr(Datum::Null);
        let f = scalar_func_expr(ScalarFuncSig::Date, &[input]);
        let op = Expression::build(&mut ctx, f).unwrap();
        let got = op.eval(&mut ctx, &[]);
        match got {
            Ok(_) => assert!(false, "null should be wrong"),
            Err(_) => assert!(true),
        }

        // test zero case
        let cfg = EvalConfig::new()
            .set_by_flags(FLAG_IN_UPDATE_OR_DELETE_STMT)
            .set_sql_mode(MODE_ERROR_FOR_DIVISION_BY_ZERO)
            .set_strict_sql_mode(true);
        ctx = EvalContext::new(Arc::new(cfg));
        let arg = datum_expr(Datum::Time(
            Time::parse_utc_datetime("0000-00-00 00:00:00", 6).unwrap(),
        ));
        let f = scalar_func_expr(ScalarFuncSig::Date, &[arg]);
        let op = Expression::build(&mut ctx, f).unwrap();
        let got = op.eval(&mut ctx, &[]);
        match got {
            Ok(_) => assert!(false, "zero timestamp should be wrong"),
            Err(_) => assert!(true),
        }
    }

    #[test]
    fn test_month() {
        let tests = vec![
            ("2018-01-01 01:01:01", 1i64),
            ("2018-02-01 01:01:01", 2i64),
            ("2018-03-01 01:01:01", 3i64),
            ("2018-04-01 01:01:01", 4i64),
            ("2018-05-01 01:01:01", 5i64),
            ("2018-06-01 01:01:01", 6i64),
            ("2018-07-01 01:01:01", 7i64),
            ("2018-08-01 01:01:01", 8i64),
            ("2018-09-01 01:01:01", 9i64),
            ("2018-10-01 01:01:01", 10i64),
            ("2018-11-01 01:01:01", 11i64),
            ("2018-12-01 01:01:01", 12i64),
        ];
        let mut ctx = EvalContext::default();
        for (arg, exp) in tests {
            let arg = datum_expr(Datum::Time(Time::parse_utc_datetime(arg, 6).unwrap()));
            let exp = Datum::I64(exp);
            let f = scalar_func_expr(ScalarFuncSig::Month, &[arg]);
            let op = Expression::build(&mut ctx, f).unwrap();
            let got = op.eval(&mut ctx, &[]).unwrap();
            assert_eq!(got, exp);
        }

        // test NULL case
        let input = datum_expr(Datum::Null);
        let f = scalar_func_expr(ScalarFuncSig::Month, &[input]);
        let op = Expression::build(&mut ctx, f).unwrap();
        op.eval(&mut ctx, &[]).unwrap_err();

        // test zero case
        let cfg = EvalConfig::new()
            .set_by_flags(FLAG_IN_UPDATE_OR_DELETE_STMT)
            .set_sql_mode(MODE_ERROR_FOR_DIVISION_BY_ZERO)
            .set_strict_sql_mode(true);
        ctx = EvalContext::new(Arc::new(cfg));
        let arg = datum_expr(Datum::Time(
            Time::parse_utc_datetime("0000-00-00 00:00:00", 6).unwrap(),
        ));
        let f = scalar_func_expr(ScalarFuncSig::Month, &[arg]);
        let op = Expression::build(&mut ctx, f).unwrap();
        op.eval(&mut ctx, &[]).unwrap_err();
    }

    #[test]
    fn test_last_day() {
        let tests = vec![
            ("2011-11-11", "2011-11-30"),
            ("2008-02-10", "2008-02-29"),
            ("2000-02-11", "2000-02-29"),
            ("2100-02-11", "2100-02-28"),
            ("2011-11-11", "2011-11-30"),
            ("2011-11-11 10:10:10", "2011-11-30 00:00:00"),
        ];
        let mut ctx = EvalContext::default();
        for (arg, exp) in tests {
            let arg = datum_expr(Datum::Time(Time::parse_utc_datetime(arg, 6).unwrap()));
            let exp = Datum::Time(Time::parse_utc_datetime(exp, 6).unwrap());
            let f = scalar_func_expr(ScalarFuncSig::LastDay, &[arg]);
            let op = Expression::build(&mut ctx, f).unwrap();
            let got = op.eval(&mut ctx, &[]).unwrap();
            assert_eq!(got, exp);
        }
    }
}
