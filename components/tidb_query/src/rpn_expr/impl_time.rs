// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

use tidb_query_codegen::rpn_fn;

use super::super::expr::EvalContext;

use crate::codec::data_type::*;
use crate::codec::mysql::time::extension::DateTimeExtension;
use crate::codec::mysql::time::weekmode::WeekMode;
use crate::codec::mysql::time::WeekdayExtension;
use crate::codec::mysql::Time;
use crate::codec::Error;
use crate::expr::SqlMode;
use crate::Result;

#[rpn_fn(capture = [ctx])]
#[inline]
pub fn date_format(
    ctx: &mut EvalContext,
    t: &Option<DateTime>,
    layout: &Option<Bytes>,
) -> Result<Option<Bytes>> {
    use std::str::from_utf8;

    if t.is_none() || layout.is_none() {
        return Ok(None);
    }
    let (t, layout) = (t.as_ref().unwrap(), layout.as_ref().unwrap());
    if t.invalid_zero() {
        return ctx
            .handle_invalid_time_error(Error::incorrect_datetime_value(&format!("{}", t)))
            .map(|_| Ok(None))?;
    }

    let t = t.date_format(from_utf8(layout.as_slice()).map_err(Error::Encoding)?);
    if let Err(err) = t {
        return ctx.handle_invalid_time_error(err).map(|_| Ok(None))?;
    }

    Ok(Some(t.unwrap().into_bytes()))
}

#[rpn_fn(capture = [ctx])]
#[inline]
pub fn week_day(ctx: &mut EvalContext, t: &Option<DateTime>) -> Result<Option<Int>> {
    if t.is_none() {
        return Ok(None);
    }
    let t = t.as_ref().unwrap();
    if t.invalid_zero() {
        return ctx
            .handle_invalid_time_error(Error::incorrect_datetime_value(&format!("{}", t)))
            .map(|_| Ok(None))?;
    }
    let day = t.weekday().num_days_from_monday();
    Ok(Some(i64::from(day)))
}

#[rpn_fn(capture = [ctx])]
#[inline]
pub fn day_of_year(ctx: &mut EvalContext, t: &Option<DateTime>) -> Result<Option<Int>> {
    if t.is_none() {
        return Ok(None);
    }
    let t = t.as_ref().unwrap();
    if t.invalid_zero() {
        return ctx
            .handle_invalid_time_error(Error::incorrect_datetime_value(t))
            .map(|_| Ok(None))?;
    }
    let day = t.days();
    Ok(Some(Int::from(day)))
}

#[rpn_fn(capture = [ctx])]
#[inline]
pub fn week_of_year(ctx: &mut EvalContext, t: &Option<DateTime>) -> Result<Option<Int>> {
    if t.is_none() {
        return Ok(None);
    }
    let t = t.as_ref().unwrap();
    if t.invalid_zero() {
        return ctx
            .handle_invalid_time_error(Error::incorrect_datetime_value(t))
            .map(|_| Ok(None))?;
    }
    let week = t.week(WeekMode::from_bits_truncate(3));
    Ok(Some(Int::from(week)))
}

#[rpn_fn(capture = [ctx])]
#[inline]
pub fn to_days(ctx: &mut EvalContext, t: &Option<DateTime>) -> Result<Option<Int>> {
    let t = match t {
        Some(v) => v,
        _ => return Ok(None),
    };
    if t.invalid_zero() {
        return ctx
            .handle_invalid_time_error(Error::incorrect_datetime_value(t))
            .map(|_| Ok(None))?;
    }
    Ok(Some(Int::from(t.day_number())))
}

#[rpn_fn(capture = [ctx])]
#[inline]
pub fn from_days(ctx: &mut EvalContext, arg: &Option<Int>) -> Result<Option<Time>> {
    arg.map_or(Ok(None), |daynr: Int| {
        let time = Time::from_days(ctx, daynr as u32)?;
        Ok(Some(time))
    })
}

#[rpn_fn]
#[inline]
pub fn month(t: &Option<DateTime>) -> Result<Option<Int>> {
    t.map_or(Ok(None), |time| Ok(Some(Int::from(time.month()))))
}

#[rpn_fn]
#[inline]
pub fn hour(t: &Option<Duration>) -> Result<Option<Int>> {
    Ok(t.as_ref().map(|t| i64::from(t.hours())))
}

#[rpn_fn]
#[inline]
pub fn minute(t: &Option<Duration>) -> Result<Option<Int>> {
    Ok(t.as_ref().map(|t| i64::from(t.minutes())))
}

#[rpn_fn]
#[inline]
pub fn second(t: &Option<Duration>) -> Result<Option<Int>> {
    Ok(t.as_ref().map(|t| i64::from(t.secs())))
}

#[rpn_fn]
#[inline]
pub fn micro_second(t: &Option<Duration>) -> Result<Option<Int>> {
    Ok(t.as_ref().map(|t| i64::from(t.subsec_micros())))
}

#[rpn_fn(capture = [ctx])]
#[inline]
pub fn year(ctx: &mut EvalContext, t: &Option<DateTime>) -> Result<Option<Int>> {
    let t = match t {
        Some(v) => v,
        _ => return Ok(None),
    };

    if t.is_zero() {
        if ctx.cfg.sql_mode.contains(SqlMode::NO_ZERO_DATE) {
            return ctx
                .handle_invalid_time_error(Error::incorrect_datetime_value(&format!("{}", t)))
                .map(|_| Ok(None))?;
        }
        return Ok(Some(0));
    }
    Ok(Some(Int::from(t.year())))
}

#[rpn_fn(capture = [ctx])]
#[inline]
pub fn day_of_month(ctx: &mut EvalContext, t: &Option<DateTime>) -> Result<Option<Int>> {
    let t = match t {
        Some(v) => v,
        _ => return Ok(None),
    };

    if t.is_zero() {
        if ctx.cfg.sql_mode.contains(SqlMode::NO_ZERO_DATE) {
            return ctx
                .handle_invalid_time_error(Error::incorrect_datetime_value(&format!("{}", t)))
                .map(|_| Ok(None))?;
        }
        return Ok(Some(0));
    }
    Ok(Some(Int::from(t.day())))
}

#[rpn_fn(capture = [ctx])]
#[inline]
pub fn day_name(ctx: &mut EvalContext, t: &Option<DateTime>) -> Result<Option<Bytes>> {
    match t {
        Some(t) => {
            if t.invalid_zero() {
                return ctx
                    .handle_invalid_time_error(Error::incorrect_datetime_value(t))
                    .map(|_| Ok(None))?;
            }
            Ok(Some(t.weekday().name().to_string().into_bytes()))
        }
        None => Ok(None),
    }
}

#[rpn_fn]
#[inline]
pub fn period_add(p: &Option<Int>, n: &Option<Int>) -> Result<Option<Int>> {
    Ok(match (p, n) {
        (Some(p), Some(n)) => {
            if *p == 0 {
                Some(0)
            } else {
                let (month, _) =
                    (i64::from(DateTime::period_to_month(*p as u64) as i32)).overflowing_add(*n);
                Some(DateTime::month_to_period(u64::from(month as u32)) as i64)
            }
        }
        _ => None,
    })
}

#[rpn_fn]
#[inline]
pub fn period_diff(p1: &Option<Int>, p2: &Option<Int>) -> Result<Option<Int>> {
    match (p1, p2) {
        (Some(p1), Some(p2)) => Ok(Some(
            DateTime::period_to_month(*p1 as u64) as i64
                - DateTime::period_to_month(*p2 as u64) as i64,
        )),
        _ => Ok(None),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use tipb::ScalarFuncSig;

    use crate::codec::error::ERR_TRUNCATE_WRONG_VALUE;
    use crate::codec::mysql::Time;
    use crate::rpn_expr::types::test_util::RpnFnScalarEvaluator;
    use tidb_query_datatype::FieldTypeTp;

    #[test]
    fn test_date_format() {
        use std::sync::Arc;

        use crate::expr::{EvalConfig, EvalContext, Flag, SqlMode};

        let cases = vec![
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
        for (date, format, expect) in cases {
            let date =
                Some(DateTime::parse_datetime(&mut EvalContext::default(), date, 6, true).unwrap());
            let format = Some(format.as_bytes().to_vec());
            let expect = Some(expect.as_bytes().to_vec());

            let output = RpnFnScalarEvaluator::new()
                .push_param(date.clone())
                .push_param(format.clone())
                .evaluate(ScalarFuncSig::DateFormatSig)
                .unwrap();
            assert_eq!(output, expect, "{:?} {:?}", date, format);
        }

        //                // TODO: pass this test after refactoring the issue #3953 is fixed.
        //                {
        //                    let format: Option<Bytes> =  Some("abc%b %M %m %c %D %d %e %j".as_bytes().to_vec());
        //                    let time: Option<DateTime> = Some( DateTime::parse_utc_datetime("0000-00-00 00:00:00", 6).unwrap());
        //
        //                    let mut cfg = EvalConfig::new();
        //                    cfg.set_flag(Flag::IN_UPDATE_OR_DELETE_STMT)
        //                        .set_sql_mode(SqlMode::NO_ZERO_DATE | SqlMode::STRICT_ALL_TABLES);
        //                    let ctx = EvalContext::new(Arc::new(cfg));
        //
        //                    let output = RpnFnScalarEvaluator::new()
        //                        .context(ctx)
        //                        .push_param(time.clone())
        //                        .push_param(format)
        //                        .evaluate::<Bytes>(ScalarFuncSig::DateFormatSig);
        //                    assert!(output.is_err());
        //                }

        {
            let mut cfg = EvalConfig::new();
            cfg.set_flag(Flag::IN_UPDATE_OR_DELETE_STMT)
                .set_sql_mode(SqlMode::NO_ZERO_DATE | SqlMode::STRICT_ALL_TABLES);
            let ctx = EvalContext::new(Arc::new(cfg));

            let output = RpnFnScalarEvaluator::new()
                .context(ctx)
                .push_param(None::<DateTime>)
                .push_param(None::<Bytes>)
                .evaluate::<Bytes>(ScalarFuncSig::DateFormatSig)
                .unwrap();
            assert_eq!(output, None);
        }

        // test date format when format is None
        let cases: Vec<(Option<&str>, Option<&str>)> = vec![
            (Some("2010-01-07 23:12:34.12345"), None),
            (None, None),
            // TODO: pass this test after refactoring the issue #3953 is fixed.
            //            (
            //                "0000-00-00 00:00:00",
            //                Some(
            //                    "%b %M %m %c %D %d %e %j %k %H %i %p %r %T %s %f %v
            //                            %x %Y %y %%",
            //                ),
            //            ),
        ];

        for (date, format) in cases {
            let date = date.map(|d| {
                DateTime::parse_datetime(&mut EvalContext::default(), d, 6, true).unwrap()
            });
            let format = format.map(|s| s.as_bytes().to_vec());

            let output = RpnFnScalarEvaluator::new()
                .push_param(date.clone())
                .push_param(format.clone())
                .evaluate::<Bytes>(ScalarFuncSig::DateFormatSig)
                .unwrap();
            assert_eq!(output, None, "{:?} {:?}", date, format);
        }
    }

    #[test]
    fn test_week_day() {
        let cases = vec![
            ("2018-12-03", Some(0i64)),
            ("2018-12-04", Some(1i64)),
            ("2018-12-05", Some(2i64)),
            ("2018-12-06", Some(3i64)),
            ("2018-12-07", Some(4i64)),
            ("2018-12-08", Some(5i64)),
            ("2018-12-09", Some(6i64)),
            ("0000-00-00", None),
            ("2018-12-00", None),
            ("2018-00-03", None),
        ];
        let mut ctx = EvalContext::default();
        for (arg, exp) in cases {
            let datetime = Some(DateTime::parse_datetime(&mut ctx, arg, 6, true).unwrap());
            let output = RpnFnScalarEvaluator::new()
                .push_param(datetime.clone())
                .evaluate(ScalarFuncSig::WeekDay)
                .unwrap();
            assert_eq!(output, exp);
        }
        let output = RpnFnScalarEvaluator::new()
            .push_param(None::<DateTime>)
            .evaluate::<Int>(ScalarFuncSig::WeekDay)
            .unwrap();
        assert_eq!(output, None);
    }

    #[test]
    fn test_week_of_year() {
        let cases = vec![
            ("2018-01-01", Some(1i64)),
            ("2018-02-28", Some(9i64)),
            ("2018-06-01", Some(22i64)),
            ("2018-07-31", Some(31i64)),
            ("2018-11-01", Some(44i64)),
            ("2018-12-30", Some(52i64)),
            ("2018-12-31", Some(1i64)),
            ("2017-01-01", Some(52i64)),
            ("2017-12-31", Some(52i64)),
            ("0000-00-00", None),
            ("2018-12-00", None),
            ("2018-00-03", None),
        ];
        let mut ctx = EvalContext::default();
        for (arg, exp) in cases {
            let datetime = Some(DateTime::parse_datetime(&mut ctx, arg, 6, true).unwrap());
            let output = RpnFnScalarEvaluator::new()
                .push_param(datetime.clone())
                .evaluate(ScalarFuncSig::WeekOfYear)
                .unwrap();
            assert_eq!(output, exp);
        }
        let output = RpnFnScalarEvaluator::new()
            .push_param(None::<DateTime>)
            .evaluate::<Int>(ScalarFuncSig::WeekOfYear)
            .unwrap();
        assert_eq!(output, None);
    }

    #[test]
    fn test_day_of_year() {
        let cases = vec![
            ("2018-11-11 00:00:00.000000", Some(315)),
            ("2018-11-12 00:00:00.000000", Some(316)),
            ("2018-11-30 00:00:00.000000", Some(334)),
            ("2018-12-31 00:00:00.000000", Some(365)),
            ("2016-12-31 00:00:00.000000", Some(366)),
            ("0000-00-00 00:00:00.000000", None),
            ("2018-11-00 00:00:00.000000", None),
            ("2018-00-11 00:00:00.000000", None),
        ];
        let mut ctx = EvalContext::default();
        for (arg, exp) in cases {
            let datetime = Some(DateTime::parse_datetime(&mut ctx, arg, 6, true).unwrap());
            let output = RpnFnScalarEvaluator::new()
                .push_param(datetime.clone())
                .evaluate(ScalarFuncSig::DayOfYear)
                .unwrap();
            assert_eq!(output, exp);
        }
        let output = RpnFnScalarEvaluator::new()
            .push_param(None::<DateTime>)
            .evaluate::<Int>(ScalarFuncSig::DayOfYear)
            .unwrap();
        assert_eq!(output, None);
    }

    #[test]
    fn test_to_days() {
        let cases = vec![
            (Some("950501"), Some(728779)),
            (Some("2007-10-07"), Some(733321)),
            (Some("2008-10-07"), Some(733687)),
            (Some("08-10-07"), Some(733687)),
            (Some("0000-01-01"), Some(1)),
            (Some("2007-10-07 00:00:59"), Some(733321)),
            (Some("0000-00-00 00:00:00"), None),
            (None, None),
        ];

        let mut ctx = EvalContext::default();
        for (arg, exp) in cases {
            let time = match arg {
                Some(arg) => Some(Time::parse_datetime(&mut ctx, arg, 6, true).unwrap()),
                None => None,
            };
            let output = RpnFnScalarEvaluator::new()
                .push_param(time.clone())
                .evaluate(ScalarFuncSig::ToDays)
                .unwrap();
            assert_eq!(output, exp);
        }
    }

    #[test]
    fn test_from_days() {
        let cases = vec![
            (ScalarValue::Int(Some(-140)), Some("0000-00-00")), // mysql FROM_DAYS returns 0000-00-00 for any day <= 365.
            (ScalarValue::Int(Some(140)), Some("0000-00-00")), // mysql FROM_DAYS returns 0000-00-00 for any day <= 365.
            (ScalarValue::Int(Some(735_000)), Some("2012-05-12")), // Leap year.
            (ScalarValue::Int(Some(735_030)), Some("2012-06-11")),
            (ScalarValue::Int(Some(735_130)), Some("2012-09-19")),
            (ScalarValue::Int(Some(734_909)), Some("2012-02-11")),
            (ScalarValue::Int(Some(734_878)), Some("2012-01-11")),
            (ScalarValue::Int(Some(734_927)), Some("2012-02-29")),
            (ScalarValue::Int(Some(734_634)), Some("2011-05-12")), // Non Leap year.
            (ScalarValue::Int(Some(734_664)), Some("2011-06-11")),
            (ScalarValue::Int(Some(734_764)), Some("2011-09-19")),
            (ScalarValue::Int(Some(734_544)), Some("2011-02-11")),
            (ScalarValue::Int(Some(734_513)), Some("2011-01-11")),
            (ScalarValue::Int(Some(3_652_424)), Some("9999-12-31")),
            (ScalarValue::Int(Some(3_652_425)), Some("0000-00-00")), // mysql FROM_DAYS returns 0000-00-00 for any day >= 3652425
            (ScalarValue::Int(None), None),
        ];
        let mut ctx = EvalContext::default();
        for (arg, exp) in cases {
            let datetime: Option<Time> =
                exp.map(|exp: &str| Time::parse_date(&mut ctx, exp).unwrap());
            let output: Option<Time> = RpnFnScalarEvaluator::new()
                .push_param(arg)
                .evaluate(ScalarFuncSig::FromDays)
                .unwrap();
            assert_eq!(output, datetime);
        }
    }

    #[test]
    fn test_month() {
        let cases = vec![
            (Some("0000-00-00 00:00:00"), Some(0i64)),
            (Some("2018-01-01 01:01:01"), Some(1i64)),
            (Some("2018-02-01 01:01:01"), Some(2i64)),
            (Some("2018-03-01 01:01:01"), Some(3i64)),
            (Some("2018-04-01 01:01:01"), Some(4i64)),
            (Some("2018-05-01 01:01:01"), Some(5i64)),
            (Some("2018-06-01 01:01:01"), Some(6i64)),
            (Some("2018-07-01 01:01:01"), Some(7i64)),
            (Some("2018-08-01 01:01:01"), Some(8i64)),
            (Some("2018-09-01 01:01:01"), Some(9i64)),
            (Some("2018-10-01 01:01:01"), Some(10i64)),
            (Some("2018-11-01 01:01:01"), Some(11i64)),
            (Some("2018-12-01 01:01:01"), Some(12i64)),
            (None, None),
        ];
        let mut ctx = EvalContext::default();
        for (time, expect) in cases {
            let time = time.map(|t| DateTime::parse_datetime(&mut ctx, t, 6, true).unwrap());
            let output = RpnFnScalarEvaluator::new()
                .push_param(time)
                .evaluate(ScalarFuncSig::Month)
                .unwrap();
            assert_eq!(output, expect);
        }
    }

    #[test]
    fn test_hour_min_sec_micro_sec() {
        // test hour, minute, second, micro_second
        let cases: Vec<(&str, i8, i64, i64, i64, i64)> = vec![
            ("0 00:00:00.0", 0, 0, 0, 0, 0),
            ("31 11:30:45", 0, 31 * 24 + 11, 30, 45, 0),
            ("11:30:45.123345", 3, 11, 30, 45, 123000),
            ("11:30:45.123345", 5, 11, 30, 45, 123350),
            ("11:30:45.123345", 6, 11, 30, 45, 123345),
            ("11:30:45.1233456", 6, 11, 30, 45, 123346),
            ("11:30:45.000010", 6, 11, 30, 45, 10),
            ("11:30:45.00010", 5, 11, 30, 45, 100),
            ("-11:30:45.9233456", 0, 11, 30, 46, 0),
            ("-11:30:45.9233456", 1, 11, 30, 45, 900000),
            ("272:59:59.94", 2, 272, 59, 59, 940000),
            ("272:59:59.99", 1, 273, 0, 0, 0),
            ("272:59:59.99", 0, 273, 0, 0, 0),
        ];

        for (arg, fsp, h, m, s, ms) in cases {
            let duration = Some(Duration::parse(arg.as_bytes(), fsp).unwrap());
            let test_case_func = |sig, res| {
                let output = RpnFnScalarEvaluator::new()
                    .push_param(duration.clone())
                    .evaluate::<Int>(sig)
                    .unwrap();
                assert_eq!(output, Some(res));
            };
            test_case_func(ScalarFuncSig::Hour, h);
            test_case_func(ScalarFuncSig::Minute, m);
            test_case_func(ScalarFuncSig::Second, s);
            test_case_func(ScalarFuncSig::MicroSecond, ms);
        }

        // test NULL case
        let test_null_case = |sig| {
            let output = RpnFnScalarEvaluator::new()
                .push_param(None::<Duration>)
                .evaluate::<Int>(sig)
                .unwrap();
            assert_eq!(output, None);
        };

        test_null_case(ScalarFuncSig::Hour);
        test_null_case(ScalarFuncSig::Minute);
        test_null_case(ScalarFuncSig::Second);
        test_null_case(ScalarFuncSig::MicroSecond);
    }

    #[test]
    fn test_year() {
        let cases = vec![
            (Some("0000-00-00 00:00:00"), Some(0i64)),
            (Some("1-01-01 01:01:01"), Some(1i64)),
            (Some("2018-01-01 01:01:01"), Some(2018i64)),
            (Some("2019-01-01 01:01:01"), Some(2019i64)),
            (Some("2020-01-01 01:01:01"), Some(2020i64)),
            (Some("2021-01-01 01:01:01"), Some(2021i64)),
            (Some("2022-01-01 01:01:01"), Some(2022i64)),
            (Some("2023-01-01 01:01:01"), Some(2023i64)),
            (Some("2024-01-01 01:01:01"), Some(2024i64)),
            (Some("2025-01-01 01:01:01"), Some(2025i64)),
            (Some("2026-01-01 01:01:01"), Some(2026i64)),
            (Some("2027-01-01 01:01:01"), Some(2027i64)),
            (Some("2028-01-01 01:01:01"), Some(2028i64)),
            (Some("2029-01-01 01:01:01"), Some(2029i64)),
            (None, None),
        ];

        let mut ctx = EvalContext::default();
        for (time, expect) in cases {
            let time = time.map(|t| DateTime::parse_datetime(&mut ctx, t, 6, true).unwrap());
            let output = RpnFnScalarEvaluator::new()
                .push_param(time)
                .evaluate(ScalarFuncSig::Year)
                .unwrap();
            assert_eq!(output, expect);
        }
    }

    #[test]
    fn test_day_of_month() {
        let cases = vec![
            (Some("0000-00-00 00:00:00.000000"), Some(0)),
            (Some("2018-02-01 00:00:00.000000"), Some(1)),
            (Some("2018-02-15 00:00:00.000000"), Some(15)),
            (Some("2018-02-28 00:00:00.000000"), Some(28)),
            (Some("2016-02-29 00:00:00.000000"), Some(29)),
            (None, None),
        ];
        let mut ctx = EvalContext::default();
        for (time, expect) in cases {
            let time = time.map(|t| DateTime::parse_datetime(&mut ctx, t, 6, true).unwrap());
            let output = RpnFnScalarEvaluator::new()
                .push_param(time)
                .evaluate(ScalarFuncSig::DayOfMonth)
                .unwrap();
            assert_eq!(output, expect);
        }
    }

    #[test]
    fn test_day_name() {
        let cases = vec![
            (None, None, None),
            (Some("0000-00-00"), Some(ERR_TRUNCATE_WRONG_VALUE), None),
            (Some("2019-11-17"), None, Some("Sunday")),
            (Some("2019-11-18"), None, Some("Monday")),
            (Some("2019-11-19"), None, Some("Tuesday")),
            (Some("2019-11-20"), None, Some("Wednesday")),
            (Some("2019-11-21"), None, Some("Thursday")),
            (Some("2019-11-22"), None, Some("Friday")),
            (Some("2019-11-23"), None, Some("Saturday")),
            (Some("2019-11-24"), None, Some("Sunday")),
            (Some("2019-11-00"), Some(ERR_TRUNCATE_WRONG_VALUE), None),
            (Some("2019-00-00"), Some(ERR_TRUNCATE_WRONG_VALUE), None),
            (Some("2019-00-01"), Some(ERR_TRUNCATE_WRONG_VALUE), None),
            (Some("2019-11-24 00:00:00.000000"), None, Some("Sunday")),
        ];

        for (arg, err_code, exp) in cases {
            let mut ctx = EvalContext::default();
            let arg = arg.map(|arg: &str| Time::parse_date(&mut ctx, arg).unwrap());
            let (output, ctx) = RpnFnScalarEvaluator::new()
                .push_param(arg)
                .context(ctx)
                .evaluate_raw(FieldTypeTp::String, ScalarFuncSig::DayName);
            let output = output.unwrap();
            assert_eq!(output.as_bytes(), &exp.map(|v| v.as_bytes().to_vec()));
            if let Some(err_code) = err_code {
                assert_eq!(ctx.warnings.warnings[0].get_code(), err_code);
            }
        }
    }

    #[test]
    fn test_period_add() {
        let cases = vec![
            (2, 222, 201808),
            (0, 222, 0),
            (196802, 14, 196904),
            (6901, 13, 207002),
            (7001, 13, 197102),
            (200212, 9223372036854775807, 200211),
            (9223372036854775807, 0, 27201459511),
            (9223372036854775807, 9223372036854775807, 27201459510),
            (201611, 2, 201701),
            (201611, 3, 201702),
            (201611, -13, 201510),
            (1611, 3, 201702),
            (7011, 3, 197102),
            (12323, 10, 12509),
            (0, 3, 0),
        ];
        for (arg1, arg2, exp) in cases {
            let output = RpnFnScalarEvaluator::new()
                .push_param(Some(arg1))
                .push_param(Some(arg2))
                .evaluate(ScalarFuncSig::PeriodAdd)
                .unwrap();
            assert_eq!(output, Some(exp));
        }
    }

    #[test]
    fn test_period_diff() {
        let cases = vec![
            (213002, 7010, 1912),
            (213002, 215810, -344),
            (2202, 9601, 313),
            (202202, 9601, 313),
            (200806, 6907, -733),
            (201611, 201611, 0),
            (200802, 200703, 11),
            (0, 999999999, -120000086),
            (9999999, 0, 1200086),
            (411, 200413, -2),
            (197000, 207700, -1284),
            (201701, 201611, 2),
            (201702, 201611, 3),
            (201510, 201611, -13),
            (201702, 1611, 3),
            (197102, 7011, 3),
            (12509, 12323, 10),
            (12509, 12323, 10),
        ];

        for (arg1, arg2, exp) in cases {
            let output = RpnFnScalarEvaluator::new()
                .push_param(Some(arg1))
                .push_param(Some(arg2))
                .evaluate(ScalarFuncSig::PeriodDiff)
                .unwrap();
            assert_eq!(output, Some(exp));
        }
    }
}
