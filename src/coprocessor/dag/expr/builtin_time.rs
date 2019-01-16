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

fn handle_incorrect_datetime_error(ctx: &mut EvalContext, t: Cow<'_, Time>) -> Result<()> {
    Error::handle_invalid_time_error(ctx, Error::incorrect_datetime_value(&format!("{}", t)))
}

impl ScalarFunc {
    #[inline]
    pub fn date_format<'a, 'b: 'a>(
        &'b self,
        ctx: &mut EvalContext,
        row: &'a [Datum],
    ) -> Result<Option<Cow<'a, [u8]>>> {
        let t = try_opt!(self.children[0].eval_time(ctx, row));
        if t.invalid_zero() {
            return handle_incorrect_datetime_error(ctx, t).map(|_| None);
        }
        let format_mask = try_opt!(self.children[1].eval_string_and_decode(ctx, row));
        let t = t.date_format(format_mask.into_owned())?;
        Ok(Some(Cow::Owned(t.into_bytes())))
    }

    #[inline]
    pub fn date<'a, 'b: 'a>(
        &'b self,
        ctx: &mut EvalContext,
        row: &'a [Datum],
    ) -> Result<Option<Cow<'a, Time>>> {
        let mut t = try_opt!(self.children[0].eval_time(ctx, row));
        if t.is_zero() {
            return handle_incorrect_datetime_error(ctx, t).map(|_| None);
        }
        let mut res = t.to_mut().clone();
        res.set_tp(mysql::types::DATE).unwrap();
        Ok(Some(Cow::Owned(res)))
    }

    #[inline]
    pub fn month<'a, 'b: 'a>(
        &'b self,
        ctx: &mut EvalContext,
        row: &'a [Datum],
    ) -> Result<Option<i64>> {
        let t = try_opt!(self.children[0].eval_time(ctx, row));
        if t.is_zero() {
            if ctx.cfg.mode_no_zero_date_mode() {
                return handle_incorrect_datetime_error(ctx, t).map(|_| None);
            }
            return Ok(Some(0));
        }
        Ok(Some(i64::from(t.get_time().month())))
    }

    #[inline]
    pub fn month_name<'a, 'b: 'a>(
        &'b self,
        ctx: &mut EvalContext,
        row: &'a [Datum],
    ) -> Result<Option<Cow<'a, [u8]>>> {
        let t = try_opt!(self.children[0].eval_time(ctx, row));
        let month = t.get_time().month() as usize;
        if t.is_zero() && ctx.cfg.mode_no_zero_date_mode() {
            return handle_incorrect_datetime_error(ctx, t).map(|_| None);
        } else if month == 0 || t.is_zero() {
            return Ok(Some(Cow::Owned("".to_string().into_bytes())));
        }
        use coprocessor::codec::mysql::time::MONTH_NAMES;
        Ok(Some(Cow::Owned(
            MONTH_NAMES[month - 1].to_string().into_bytes(),
        )))
    }

    #[inline]
    pub fn day_name<'a, 'b: 'a>(
        &'b self,
        ctx: &mut EvalContext,
        row: &'a [Datum],
    ) -> Result<Option<Cow<'a, [u8]>>> {
        let t = try_opt!(self.children[0].eval_time(ctx, row));
        if t.is_zero() {
            return handle_incorrect_datetime_error(ctx, t).map(|_| None);
        }
        use coprocessor::codec::mysql::time::WeekdayExtension;
        let weekday = t.get_time().weekday();
        Ok(Some(Cow::Owned(weekday.name().to_string().into_bytes())))
    }

    #[inline]
    pub fn day_of_month(&self, ctx: &mut EvalContext, row: &[Datum]) -> Result<Option<i64>> {
        let t = try_opt!(self.children[0].eval_time(ctx, row));
        if t.is_zero() {
            if ctx.cfg.mode_no_zero_date_mode() {
                return handle_incorrect_datetime_error(ctx, t).map(|_| None);
            }
            return Ok(Some(0));
        }
        let day = t.get_time().day();
        Ok(Some(i64::from(day)))
    }

    #[inline]
    pub fn day_of_week(&self, ctx: &mut EvalContext, row: &[Datum]) -> Result<Option<i64>> {
        let t = try_opt!(self.children[0].eval_time(ctx, row));
        if t.is_zero() {
            return handle_incorrect_datetime_error(ctx, t).map(|_| None);
        }
        let day = t.get_time().weekday().number_from_sunday();
        Ok(Some(i64::from(day)))
    }

    #[inline]
    pub fn day_of_year(&self, ctx: &mut EvalContext, row: &[Datum]) -> Result<Option<i64>> {
        let t = try_opt!(self.children[0].eval_time(ctx, row));
        if t.is_zero() {
            return handle_incorrect_datetime_error(ctx, t).map(|_| None);
        }
        use coprocessor::codec::mysql::time::DateTimeExtension;
        let day = t.get_time().days();
        Ok(Some(i64::from(day)))
    }

    #[inline]
    pub fn year(&self, ctx: &mut EvalContext, row: &[Datum]) -> Result<Option<i64>> {
        let t = try_opt!(self.children[0].eval_time(ctx, row));
        if t.is_zero() {
            if ctx.cfg.mode_no_zero_date_mode() {
                return handle_incorrect_datetime_error(ctx, t).map(|_| None);
            }
            return Ok(Some(0));
        }
        Ok(Some(i64::from(t.get_time().year())))
    }

    #[inline]
    pub fn last_day<'a, 'b: 'a>(
        &'b self,
        ctx: &mut EvalContext,
        row: &'a [Datum],
    ) -> Result<Option<Cow<'a, Time>>> {
        let mut t = try_opt!(self.children[0].eval_time(ctx, row));
        if t.is_zero() {
            return handle_incorrect_datetime_error(ctx, t).map(|_| None);
        }
        let time = t.get_time();
        let mut res = t.to_mut().clone();
        res.set_time(
            time.timezone()
                .ymd_opt(time.year(), time.month(), t.last_day_of_month())
                .and_hms_opt(0, 0, 0)
                .unwrap(),
        );
        Ok(Some(Cow::Owned(res)))
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
    use tipb::expression::{Expr, ScalarFuncSig};

    fn expr_build(ctx: &mut EvalContext, sig: ScalarFuncSig, children: &[Expr]) -> Result<Datum> {
        let f = scalar_func_expr(sig, children);
        let op = Expression::build(ctx, f).unwrap();
        op.eval(ctx, &[])
    }

    fn test_ok_case_one_arg(ctx: &mut EvalContext, sig: ScalarFuncSig, arg: Datum, exp: Datum) {
        let children = &[datum_expr(arg)];
        match expr_build(ctx, sig, children) {
            Ok(got) => assert_eq!(got, exp),
            Err(_) => assert!(false, "eval failed"),
        }
    }

    fn test_err_case_one_arg(ctx: &mut EvalContext, sig: ScalarFuncSig, arg: Datum) {
        let children = &[datum_expr(arg)];
        match expr_build(ctx, sig, children) {
            Ok(got) => assert_eq!(got, Datum::Null),
            Err(_) => assert!(true),
        }
    }

    fn test_err_case_two_arg(ctx: &mut EvalContext, sig: ScalarFuncSig, arg1: Datum, arg2: Datum) {
        let children = &[datum_expr(arg1), datum_expr(arg2)];
        match expr_build(ctx, sig, children) {
            Ok(got) => assert_eq!(got, Datum::Null),
            Err(_) => assert!(true),
        }
    }

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
        test_err_case_two_arg(
            &mut ctx,
            ScalarFuncSig::DateFormatSig,
            Datum::Null,
            Datum::Null,
        );
        // test zero case
        let mut cfg = EvalConfig::new();
        cfg.set_by_flags(FLAG_IN_UPDATE_OR_DELETE_STMT)
            .set_sql_mode(MODE_ERROR_FOR_DIVISION_BY_ZERO)
            .set_strict_sql_mode(true);
        ctx = EvalContext::new(Arc::new(cfg));
        test_err_case_two_arg(
            &mut ctx,
            ScalarFuncSig::DateFormatSig,
            Datum::Null,
            Datum::Null,
        );
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
        test_err_case_one_arg(&mut ctx, ScalarFuncSig::Date, Datum::Null);
        // test zero case
        let mut cfg = EvalConfig::new();
        cfg.set_by_flags(FLAG_IN_UPDATE_OR_DELETE_STMT)
            .set_sql_mode(MODE_ERROR_FOR_DIVISION_BY_ZERO)
            .set_strict_sql_mode(true);
        ctx = EvalContext::new(Arc::new(cfg));
        test_err_case_one_arg(
            &mut ctx,
            ScalarFuncSig::Date,
            Datum::Time(Time::parse_utc_datetime("0000-00-00 00:00:00", 6).unwrap()),
        );
    }

    #[test]
    fn test_month() {
        let cases = vec![
            ("0000-00-00 00:00:00", 0i64),
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
        for (arg, exp) in cases {
            let arg = datum_expr(Datum::Time(Time::parse_utc_datetime(arg, 6).unwrap()));
            let exp = Datum::I64(exp);
            let f = scalar_func_expr(ScalarFuncSig::Month, &[arg]);
            let op = Expression::build(&mut ctx, f).unwrap();
            let got = op.eval(&mut ctx, &[]).unwrap();
            assert_eq!(got, exp);
        }

        // test NULL case
        test_err_case_one_arg(&mut ctx, ScalarFuncSig::Month, Datum::Null);
        // test zero case
        let mut cfg = EvalConfig::new();
        cfg.set_by_flags(FLAG_IN_UPDATE_OR_DELETE_STMT)
            .set_sql_mode(MODE_ERROR_FOR_DIVISION_BY_ZERO)
            .set_strict_sql_mode(true);
        ctx = EvalContext::new(Arc::new(cfg));
        test_err_case_one_arg(
            &mut ctx,
            ScalarFuncSig::Month,
            Datum::Time(Time::parse_utc_datetime("0000-00-00 00:00:00", 6).unwrap()),
        );
    }

    #[test]
    fn test_month_name() {
        let cases = vec![
            ("2018-01-01 00:00:00.000000", "January"),
            ("2018-02-01 00:00:00.000000", "February"),
            ("2018-03-01 00:00:00.000000", "March"),
            ("2018-04-01 00:00:00.000000", "April"),
            ("2018-05-01 00:00:00.000000", "May"),
            ("2018-06-01 00:00:00.000000", "June"),
            ("2018-07-01 00:00:00.000000", "July"),
            ("2018-08-01 00:00:00.000000", "August"),
            ("2018-09-01 00:00:00.000000", "September"),
            ("2018-10-01 00:00:00.000000", "October"),
            ("2018-11-01 00:00:00.000000", "November"),
            ("2018-12-01 00:00:00.000000", "December"),
        ];
        let mut ctx = EvalContext::default();
        for (arg, exp) in cases {
            test_ok_case_one_arg(
                &mut ctx,
                ScalarFuncSig::MonthName,
                Datum::Time(Time::parse_utc_datetime(arg, 6).unwrap()),
                Datum::Bytes(exp.as_bytes().to_vec()),
            );
        }
        // test NULL case
        test_err_case_one_arg(&mut ctx, ScalarFuncSig::MonthName, Datum::Null);
        //  test zero case
        let mut cfg = EvalConfig::new();
        cfg.set_by_flags(FLAG_IN_UPDATE_OR_DELETE_STMT)
            .set_sql_mode(MODE_NO_ZERO_DATE_MODE)
            .set_strict_sql_mode(true);
        ctx = EvalContext::new(Arc::new(cfg));
        test_err_case_one_arg(
            &mut ctx,
            ScalarFuncSig::MonthName,
            Datum::Time(Time::parse_utc_datetime("0000-00-00 00:00:00", 6).unwrap()),
        );
    }

    #[test]
    fn test_day_name() {
        let cases = vec![
            ("2018-11-11 00:00:00.000000", "Sunday"),
            ("2018-11-12 00:00:00.000000", "Monday"),
            ("2018-11-13 00:00:00.000000", "Tuesday"),
            ("2018-11-14 00:00:00.000000", "Wednesday"),
            ("2018-11-15 00:00:00.000000", "Thursday"),
            ("2018-11-16 00:00:00.000000", "Friday"),
            ("2018-11-17 00:00:00.000000", "Saturday"),
            ("2018-11-18 00:00:00.000000", "Sunday"),
        ];
        let mut ctx = EvalContext::default();
        for (arg, exp) in cases {
            test_ok_case_one_arg(
                &mut ctx,
                ScalarFuncSig::DayName,
                Datum::Time(Time::parse_utc_datetime(arg, 6).unwrap()),
                Datum::Bytes(exp.as_bytes().to_vec()),
            );
        }
        // test NULL case
        test_err_case_one_arg(&mut ctx, ScalarFuncSig::DayName, Datum::Null);
        //  test zero case
        test_err_case_one_arg(
            &mut ctx,
            ScalarFuncSig::DayName,
            Datum::Time(Time::parse_utc_datetime("0000-00-00 00:00:00", 6).unwrap()),
        );
    }

    #[test]
    fn test_day_of_month() {
        let cases = vec![
            ("0000-00-00 00:00:00.000000", 0),
            ("2018-02-01 00:00:00.000000", 1),
            ("2018-02-15 00:00:00.000000", 15),
            ("2018-02-28 00:00:00.000000", 28),
            ("2016-02-29 00:00:00.000000", 29),
        ];
        let mut ctx = EvalContext::default();
        for (arg, exp) in cases {
            test_ok_case_one_arg(
                &mut ctx,
                ScalarFuncSig::DayOfMonth,
                Datum::Time(Time::parse_utc_datetime(arg, 6).unwrap()),
                Datum::I64(exp),
            );
        }
        // test NULL case
        test_err_case_one_arg(&mut ctx, ScalarFuncSig::DayOfMonth, Datum::Null);
        //  test zero case
        let mut cfg = EvalConfig::new();
        cfg.set_by_flags(FLAG_IN_UPDATE_OR_DELETE_STMT)
            .set_sql_mode(MODE_NO_ZERO_DATE_MODE)
            .set_strict_sql_mode(true);
        ctx = EvalContext::new(Arc::new(cfg));
        test_err_case_one_arg(
            &mut ctx,
            ScalarFuncSig::DayOfMonth,
            Datum::Time(Time::parse_utc_datetime("0000-00-00 00:00:00", 6).unwrap()),
        );
    }

    #[test]
    fn test_day_of_week() {
        let cases = vec![
            ("2018-11-11 00:00:00.000000", 1),
            ("2018-11-12 00:00:00.000000", 2),
            ("2018-11-13 00:00:00.000000", 3),
            ("2018-11-14 00:00:00.000000", 4),
            ("2018-11-15 00:00:00.000000", 5),
            ("2018-11-16 00:00:00.000000", 6),
            ("2018-11-17 00:00:00.000000", 7),
            ("2018-11-18 00:00:00.000000", 1),
        ];
        let mut ctx = EvalContext::default();
        for (arg, exp) in cases {
            test_ok_case_one_arg(
                &mut ctx,
                ScalarFuncSig::DayOfWeek,
                Datum::Time(Time::parse_utc_datetime(arg, 6).unwrap()),
                Datum::I64(exp),
            );
        }
        // test NULL case
        test_err_case_one_arg(&mut ctx, ScalarFuncSig::DayOfWeek, Datum::Null);
        //  test zero case
        test_err_case_one_arg(
            &mut ctx,
            ScalarFuncSig::DayOfWeek,
            Datum::Time(Time::parse_utc_datetime("0000-00-00 00:00:00", 6).unwrap()),
        );
    }

    #[test]
    fn test_day_of_year() {
        let cases = vec![
            ("2018-11-11 00:00:00.000000", 315),
            ("2018-11-12 00:00:00.000000", 316),
            ("2018-11-30 00:00:00.000000", 334),
            ("2018-12-31 00:00:00.000000", 365),
            ("2016-12-31 00:00:00.000000", 366),
        ];
        let mut ctx = EvalContext::default();
        for (arg, exp) in cases {
            test_ok_case_one_arg(
                &mut ctx,
                ScalarFuncSig::DayOfYear,
                Datum::Time(Time::parse_utc_datetime(arg, 6).unwrap()),
                Datum::I64(exp),
            );
        }
        // test NULL case
        test_err_case_one_arg(&mut ctx, ScalarFuncSig::DayOfYear, Datum::Null);
        //  test zero case
        test_err_case_one_arg(
            &mut ctx,
            ScalarFuncSig::DayOfYear,
            Datum::Time(Time::parse_utc_datetime("0000-00-00 00:00:00", 6).unwrap()),
        );
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
        // test NULL case
        test_err_case_one_arg(&mut ctx, ScalarFuncSig::LastDay, Datum::Null);
        // test zero case
        test_err_case_one_arg(
            &mut ctx,
            ScalarFuncSig::LastDay,
            Datum::Time(Time::parse_utc_datetime("0000-00-00 00:00:00", 6).unwrap()),
        );
    }

    #[test]
    fn test_year() {
        let cases = vec![
            ("0000-00-00 00:00:00", 0i64),
            ("1-01-01 01:01:01", 1i64),
            ("2018-01-01 01:01:01", 2018i64),
            ("2019-01-01 01:01:01", 2019i64),
            ("2020-01-01 01:01:01", 2020i64),
            ("2021-01-01 01:01:01", 2021i64),
            ("2022-01-01 01:01:01", 2022i64),
            ("2023-01-01 01:01:01", 2023i64),
            ("2024-01-01 01:01:01", 2024i64),
            ("2025-01-01 01:01:01", 2025i64),
            ("2026-01-01 01:01:01", 2026i64),
            ("2027-01-01 01:01:01", 2027i64),
            ("2028-01-01 01:01:01", 2028i64),
            ("2029-01-01 01:01:01", 2029i64),
        ];
        let mut ctx = EvalContext::default();
        for (arg, exp) in cases {
            let arg = datum_expr(Datum::Time(Time::parse_utc_datetime(arg, 6).unwrap()));
            let exp = Datum::I64(exp);
            let f = scalar_func_expr(ScalarFuncSig::Year, &[arg]);
            let op = Expression::build(&mut ctx, f).unwrap();
            let got = op.eval(&mut ctx, &[]).unwrap();
            assert_eq!(got, exp);
        }
        // test NULL case
        test_err_case_one_arg(&mut ctx, ScalarFuncSig::Year, Datum::Null);
        // test zero case
        let mut cfg = EvalConfig::new();
        cfg.set_by_flags(FLAG_IN_UPDATE_OR_DELETE_STMT);
        cfg.set_sql_mode(MODE_NO_ZERO_DATE_MODE);
        cfg.set_strict_sql_mode(true);
        ctx = EvalContext::new(Arc::new(cfg));
        test_err_case_one_arg(
            &mut ctx,
            ScalarFuncSig::Year,
            Datum::Time(Time::parse_utc_datetime("0000-00-00 00:00:00", 6).unwrap()),
        );
    }
}
