// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

use tidb_query_codegen::rpn_fn;

use super::super::expr::EvalContext;

use crate::codec::data_type::*;
use crate::codec::Error;
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

#[rpn_fn]
#[inline]
pub fn period_add(p: &Option<Int>, n: &Option<Int>) -> Result<Option<Int>> {
    Ok(match (p, n) {
        (Some(p), Some(n)) => {
            if *p == 0 {
                Some(0)
            } else {
                let (month, _) = (i64::from(period_to_month(*p as u64) as i32)).overflowing_add(*n);
                Some(month_to_period(u64::from(month as u32)) as i64)
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
            period_to_month(*p1 as u64) as i64 - period_to_month(*p2 as u64) as i64,
        )),
        _ => Ok(None),
    }
}

#[inline]
fn period_to_month(period: u64) -> u64 {
    if period == 0 {
        return 0;
    }
    let (year, month) = (period / 100, period % 100);
    if year < 70 {
        (year + 2000) * 12 + month - 1
    } else if year < 100 {
        (year + 1900) * 12 + month - 1
    } else {
        year * 12 + month - 1
    }
}

#[inline]
fn month_to_period(month: u64) -> u64 {
    if month == 0 {
        return 0;
    }
    let year = month / 12;
    if year < 70 {
        (year + 2000) * 100 + month % 12 + 1
    } else if year < 100 {
        (year + 1900) * 100 + month % 12 + 1
    } else {
        year * 100 + month % 12 + 1
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use tipb::ScalarFuncSig;

    use crate::rpn_expr::types::test_util::RpnFnScalarEvaluator;

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
