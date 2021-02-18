// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

use tidb_query_codegen::rpn_fn;

use crate::RpnFnCallExtra;
use tidb_query_common::Result;

use tidb_query_datatype::codec::data_type::*;
use tidb_query_datatype::codec::mysql::duration::{
    MAX_HOUR_PART, MAX_MINUTE_PART, MAX_NANOS_PART, MAX_SECOND_PART, NANOS_PER_SEC,
};
use tidb_query_datatype::codec::mysql::time::extension::DateTimeExtension;
use tidb_query_datatype::codec::mysql::time::weekmode::WeekMode;
use tidb_query_datatype::codec::mysql::time::{WeekdayExtension, MONTH_NAMES};
use tidb_query_datatype::codec::mysql::{Duration, TimeType, MAX_FSP};
use tidb_query_datatype::codec::Error;
use tidb_query_datatype::expr::{EvalContext, SqlMode};
use tidb_query_datatype::FieldTypeAccessor;

#[rpn_fn(nullable, capture = [ctx])]
#[inline]
pub fn date_format(
    ctx: &mut EvalContext,
    t: Option<&DateTime>,
    layout: Option<BytesRef>,
) -> Result<Option<Bytes>> {
    use std::str::from_utf8;

    if t.is_none() || layout.is_none() {
        return Ok(None);
    }
    let (t, layout) = (t.as_ref().unwrap(), layout.as_ref().unwrap());
    if t.invalid_zero() {
        return ctx
            .handle_invalid_time_error(Error::incorrect_datetime_value(t))
            .map(|_| Ok(None))?;
    }

    let t = t.date_format(from_utf8(layout).map_err(Error::Encoding)?);
    if let Err(err) = t {
        return ctx.handle_invalid_time_error(err).map(|_| Ok(None))?;
    }

    Ok(Some(t.unwrap().into_bytes()))
}

#[rpn_fn(capture = [ctx])]
#[inline]
pub fn date(ctx: &mut EvalContext, t: &DateTime) -> Result<Option<DateTime>> {
    if t.invalid_zero() {
        return ctx
            .handle_invalid_time_error(Error::incorrect_datetime_value(t))
            .map(|_| Ok(None))?;
    }

    let mut res = *t;
    res.set_time_type(TimeType::Date)?;
    Ok(Some(res))
}

#[rpn_fn(nullable, capture = [ctx])]
#[inline]
pub fn week_with_mode(
    ctx: &mut EvalContext,
    t: Option<&DateTime>,
    m: Option<&Int>,
) -> Result<Option<Int>> {
    if t.is_none() || m.is_none() {
        return Ok(None);
    }
    let (t, m) = (t.unwrap(), m.unwrap());
    if t.invalid_zero() {
        return ctx
            .handle_invalid_time_error(Error::incorrect_datetime_value(t))
            .map(|_| Ok(None))?;
    }
    let week = t.week(WeekMode::from_bits_truncate(*m as u32));
    Ok(Some(i64::from(week)))
}

#[rpn_fn(capture = [ctx])]
#[inline]
pub fn week_without_mode(ctx: &mut EvalContext, t: &DateTime) -> Result<Option<Int>> {
    if t.invalid_zero() {
        return ctx
            .handle_invalid_time_error(Error::incorrect_datetime_value(t))
            .map(|_| Ok(None))?;
    }
    let week = t.week(WeekMode::from_bits_truncate(0u32));
    Ok(Some(i64::from(week)))
}

#[rpn_fn(nullable, capture = [ctx])]
#[inline]
pub fn week_day(ctx: &mut EvalContext, t: Option<&DateTime>) -> Result<Option<Int>> {
    if t.is_none() {
        return Ok(None);
    }
    let t = t.as_ref().unwrap();
    if t.invalid_zero() {
        return ctx
            .handle_invalid_time_error(Error::incorrect_datetime_value(t))
            .map(|_| Ok(None))?;
    }
    let day = t.weekday().num_days_from_monday();
    Ok(Some(i64::from(day)))
}

#[rpn_fn(nullable, capture = [ctx])]
#[inline]
pub fn day_of_week(ctx: &mut EvalContext, t: Option<&DateTime>) -> Result<Option<Int>> {
    if t.is_none() {
        return Ok(None);
    }
    let t = t.as_ref().unwrap();
    if t.invalid_zero() {
        return ctx
            .handle_invalid_time_error(Error::incorrect_datetime_value(t))
            .map(|_| Ok(None))?;
    }
    let day = t.weekday().number_from_sunday();
    Ok(Some(Int::from(day)))
}

#[rpn_fn(nullable, capture = [ctx])]
#[inline]
pub fn day_of_year(ctx: &mut EvalContext, t: Option<&DateTime>) -> Result<Option<Int>> {
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

#[rpn_fn(nullable, capture = [ctx])]
#[inline]
pub fn week_of_year(ctx: &mut EvalContext, t: Option<&DateTime>) -> Result<Option<Int>> {
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

// year_week_with_mode implements `YEARWEEK` in MySQL.
// See also: https://dev.mysql.com/doc/refman/5.7/en/date-and-time-functions.html#function_yearweek
//
// e.g.: SELECT YEARWEEK('1987-01-01');  -- -> 198652, here the first 4 digits represents year, and the last 2 digits represents week.
#[rpn_fn(capture = [ctx])]
#[inline]
pub fn year_week_with_mode(ctx: &mut EvalContext, t: &DateTime, mode: &Int) -> Result<Option<Int>> {
    if t.invalid_zero() {
        return ctx
            .handle_invalid_time_error(Error::incorrect_datetime_value(t))
            .map(|_| Ok(None))?;
    }

    let (year, week) = t.year_week(WeekMode::from_bits_truncate(*mode as u32));
    let result = i64::from(week + year * 100);
    if result < 0 {
        return Ok(Some(i64::from(u32::max_value())));
    }
    Ok(Some(result))
}

// year_week_without_mode implements `YEARWEEK` in MySQL.
// See also: https://dev.mysql.com/doc/refman/5.7/en/date-and-time-functions.html#function_yearweek
#[rpn_fn(capture = [ctx])]
#[inline]
pub fn year_week_without_mode(ctx: &mut EvalContext, t: &DateTime) -> Result<Option<Int>> {
    if t.invalid_zero() {
        return ctx
            .handle_invalid_time_error(Error::incorrect_datetime_value(t))
            .map(|_| Ok(None))?;
    }

    let (year, week) = t.year_week(WeekMode::from_bits_truncate(0u32));
    let result = i64::from(week + year * 100);
    if result < 0 {
        return Ok(Some(i64::from(u32::max_value())));
    }
    Ok(Some(result))
}

#[rpn_fn(nullable, capture = [ctx])]
#[inline]
pub fn to_days(ctx: &mut EvalContext, t: Option<&DateTime>) -> Result<Option<Int>> {
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
pub fn to_seconds(ctx: &mut EvalContext, t: &DateTime) -> Result<Option<Int>> {
    if t.invalid_zero() {
        return ctx
            .handle_invalid_time_error(Error::incorrect_datetime_value(t))
            .map(|_| Ok(None))?;
    }
    Ok(Some(t.second_number()))
}

#[rpn_fn(writer, capture = [ctx])]
#[inline]
pub fn add_string_and_duration(
    ctx: &mut EvalContext,
    arg0: BytesRef,
    arg1: &Duration,
    writer: BytesWriter,
) -> Result<BytesGuard> {
    let arg0 = std::str::from_utf8(arg0).map_err(Error::Encoding)?;
    if let Ok(arg0) = Duration::parse_exactly(ctx, arg0, MAX_FSP) {
        return match arg0.checked_add(*arg1) {
            Some(result) => Ok(writer.write(Some(duration_to_string(result).into_bytes()))),
            None => ctx
                .handle_overflow_err(Error::overflow("DURATION", &format!("{} + {}", arg0, arg1)))
                .map(|_| Ok(writer.write(None)))?,
        };
    };
    if let Ok(arg0) = DateTime::parse_datetime(ctx, arg0, MAX_FSP, true) {
        return match arg0.checked_add(ctx, *arg1) {
            Some(result) => Ok(writer.write(Some(datetime_to_string(result).into_bytes()))),
            None => ctx
                .handle_overflow_err(Error::overflow("DATETIME", &format!("{} + {}", arg0, arg1)))
                .map(|_| Ok(writer.write(None)))?,
        };
    };
    ctx.handle_invalid_time_error(Error::incorrect_datetime_value(arg0))?;

    Ok(writer.write(None))
}

#[rpn_fn(writer, capture = [ctx])]
#[inline]
pub fn sub_string_and_duration(
    ctx: &mut EvalContext,
    arg0: BytesRef,
    arg1: &Duration,
    writer: BytesWriter,
) -> Result<BytesGuard> {
    let arg0 = std::str::from_utf8(arg0).map_err(Error::Encoding)?;
    if let Ok(arg0) = Duration::parse_exactly(ctx, arg0, MAX_FSP) {
        return match arg0.checked_sub(*arg1) {
            Some(result) => Ok(writer.write(Some(duration_to_string(result).into_bytes()))),
            None => ctx
                .handle_overflow_err(Error::overflow("DURATION", &format!("{} - {}", arg0, arg1)))
                .map(|_| Ok(writer.write(None)))?,
        };
    };
    if let Ok(arg0) = DateTime::parse_datetime(ctx, arg0, MAX_FSP, true) {
        return match arg0.checked_sub(ctx, *arg1) {
            Some(result) => Ok(writer.write(Some(datetime_to_string(result).into_bytes()))),
            None => ctx
                .handle_overflow_err(Error::overflow("DATETIME", &format!("{} - {}", arg0, arg1)))
                .map(|_| Ok(writer.write(None)))?,
        };
    };
    ctx.handle_invalid_time_error(Error::incorrect_datetime_value(arg0))?;

    Ok(writer.write(None))
}

#[rpn_fn]
#[inline]
pub fn date_diff(from_time: &DateTime, to_time: &DateTime) -> Result<Option<Int>> {
    Ok(from_time.date_diff(*to_time))
}

#[rpn_fn(capture = [ctx])]
#[inline]
pub fn add_datetime_and_duration(
    ctx: &mut EvalContext,
    datetime: &DateTime,
    duration: &Duration,
) -> Result<Option<DateTime>> {
    let mut res = match datetime.checked_add(ctx, *duration) {
        Some(res) => res,
        None => {
            return ctx
                .handle_invalid_time_error(Error::overflow(
                    "DATETIME",
                    format!("({} + {})", datetime, duration),
                ))
                .map(|_| Ok(None))?
        }
    };
    if res.set_time_type(TimeType::DateTime).is_err() {
        return Ok(None);
    }
    Ok(Some(res))
}

#[rpn_fn(capture=[ctx])]
#[inline]
pub fn add_datetime_and_string(
    ctx: &mut EvalContext,
    arg0: &DateTime,
    arg1: BytesRef,
) -> Result<Option<DateTime>> {
    let arg1 = std::str::from_utf8(&arg1).map_err(Error::Encoding)?;
    let arg1 = match Duration::parse(ctx, arg1, MAX_FSP) {
        Ok(arg) => arg,
        Err(_) => return Ok(None),
    };

    let res = match arg0.checked_add(ctx, arg1) {
        Some(res) => res,
        None => {
            return ctx
                .handle_invalid_time_error(Error::overflow(
                    "DATETIME",
                    format!("({} + {})", arg0, arg1),
                ))
                .map(|_| Ok(None))?;
        }
    };
    Ok(Some(res))
}

#[rpn_fn(capture=[ctx])]
#[inline]
pub fn sub_duration_and_duration(
    ctx: &mut EvalContext,
    duration1: &Duration,
    duration2: &Duration,
) -> Result<Option<Duration>> {
    let result = match duration1.checked_sub(*duration2) {
        Some(result) => result,
        None => {
            return ctx
                .handle_invalid_time_error(Error::overflow(
                    "Duration",
                    format!("({} - {})", duration1, duration2),
                ))
                .map(|_| Ok(None))?
        }
    };
    Ok(Some(result))
}

#[rpn_fn(capture = [ctx])]
#[inline]
pub fn sub_datetime_and_duration(
    ctx: &mut EvalContext,
    datetime: &DateTime,
    duration: &Duration,
) -> Result<Option<DateTime>> {
    let mut res = match datetime.checked_sub(ctx, *duration) {
        Some(res) => res,
        None => {
            return ctx
                .handle_invalid_time_error(Error::overflow(
                    "DATETIME",
                    format!("({} - {})", datetime, duration),
                ))
                .map(|_| Ok(None))?
        }
    };
    if res.set_time_type(TimeType::DateTime).is_err() {
        return Ok(None);
    }
    Ok(Some(res))
}

#[rpn_fn(capture = [ctx])]
#[inline]
pub fn sub_datetime_and_string(
    ctx: &mut EvalContext,
    datetime: &DateTime,
    duration_str: BytesRef,
) -> Result<Option<DateTime>> {
    let duration_str = std::str::from_utf8(&duration_str).map_err(Error::Encoding)?;
    let duration = match Duration::parse(ctx, duration_str, MAX_FSP) {
        Ok(duration) => duration,
        Err(_) => return Ok(None),
    };

    let res = match datetime.checked_sub(ctx, duration) {
        Some(res) => res,
        None => {
            return ctx
                .handle_invalid_time_error(Error::overflow(
                    "DATETIME",
                    format!("({} - {})", datetime, duration),
                ))
                .map(|_| Ok(None))?;
        }
    };
    Ok(Some(res))
}

#[rpn_fn(capture = [ctx])]
#[inline]
pub fn from_days(ctx: &mut EvalContext, arg: &Int) -> Result<Option<DateTime>> {
    let time = DateTime::from_days(ctx, *arg as u32)?;
    Ok(Some(time))
}

#[rpn_fn(capture = [ctx])]
#[inline]
pub fn make_date(ctx: &mut EvalContext, year: &Int, day: &Int) -> Result<Option<DateTime>> {
    let mut year = *year;
    let mut day = *day;
    if day <= 0 || year < 0 || year > 9999 || day > 366 * 9999 {
        return Ok(None);
    }
    if year < 70 {
        year += 2000;
    } else if year < 100 {
        year += 1900;
    }
    year -= 1;
    let d4 = year / 4;
    let d100 = year / 100;
    let d400 = year / 400;
    let leap = d4 - d100 + d400;
    day = day + leap + year * 365 + 365;
    let days = day as u32;
    let ret = DateTime::from_days(ctx, days)?;
    if ret.year() > 9999 || ret.is_zero() {
        return Ok(None);
    }
    Ok(Some(ret))
}

#[rpn_fn(capture = [extra, args])]
#[inline]
pub fn make_time(
    extra: &RpnFnCallExtra,
    args: &[crate::RpnStackNode<'_>],
    hour: &Int,
    minute: &Int,
    second: &Real,
) -> Result<Option<Duration>> {
    let (is_negative, mut hour) = if args[0].field_type().is_unsigned() {
        (false, *hour as u64)
    } else {
        (hour.is_negative(), hour.wrapping_abs() as u64)
    };

    let nanosecond = second.fract().abs() * NANOS_PER_SEC as f64;
    let second = second.trunc();
    let minute = *minute;

    // Filter out the number that is negative or greater than `MAX_MINUTE_PART`.
    let mut minute = if 0 <= minute && minute <= MAX_MINUTE_PART.into() {
        minute as u32
    } else {
        return Ok(None);
    };

    // Filter out the number that is negative or greater than `MAX_SECOND_PART`.
    let mut second = if 0.0 <= second && second <= MAX_SECOND_PART.into() {
        second as u32
    } else {
        return Ok(None);
    };

    // Ensure that the nanosecond part is valid.
    debug_assert!(0.0 <= nanosecond && nanosecond.floor() <= MAX_NANOS_PART.into());
    let mut nanosecond = nanosecond as u32;

    let is_overflow = (hour, minute, second, nanosecond)
        > (MAX_HOUR_PART.into(), MAX_MINUTE_PART, MAX_SECOND_PART, 0);
    if is_overflow {
        hour = MAX_HOUR_PART.into();
        minute = MAX_MINUTE_PART;
        second = MAX_SECOND_PART;
        nanosecond = 0;
    }

    match Duration::new_from_parts(
        is_negative,
        hour as _,
        minute,
        second,
        nanosecond,
        extra.ret_field_type.get_decimal() as i8,
    ) {
        Ok(duration) => Ok(Some(duration)),

        // May encounter the fsp error
        Err(err) => Err(err.into()),
    }
}

#[rpn_fn(nullable)]
#[inline]
pub fn month(t: Option<&DateTime>) -> Result<Option<Int>> {
    t.map_or(Ok(None), |time| Ok(Some(Int::from(time.month()))))
}

#[rpn_fn(capture = [ctx])]
#[inline]
pub fn month_name(ctx: &mut EvalContext, t: &DateTime) -> Result<Option<Bytes>> {
    if t.invalid_zero() {
        return ctx
            .handle_invalid_time_error(Error::incorrect_datetime_value(t))
            .map(|_| Ok(None))?;
    }
    let month = t.month() as usize;
    Ok(Some(MONTH_NAMES[month - 1].to_string().into_bytes()))
}

#[rpn_fn(nullable)]
#[inline]
pub fn hour(t: Option<&Duration>) -> Result<Option<Int>> {
    Ok(t.as_ref().map(|t| i64::from(t.hours())))
}

#[rpn_fn(nullable)]
#[inline]
pub fn minute(t: Option<&Duration>) -> Result<Option<Int>> {
    Ok(t.as_ref().map(|t| i64::from(t.minutes())))
}

#[rpn_fn(nullable)]
#[inline]
pub fn second(t: Option<&Duration>) -> Result<Option<Int>> {
    Ok(t.as_ref().map(|t| i64::from(t.secs())))
}

#[rpn_fn(nullable)]
#[inline]
pub fn time_to_sec(t: Option<&Duration>) -> Result<Option<Int>> {
    Ok(t.as_ref().map(|t| t.to_secs()))
}

#[rpn_fn(nullable)]
#[inline]
pub fn micro_second(t: Option<&Duration>) -> Result<Option<Int>> {
    Ok(t.as_ref().map(|t| i64::from(t.subsec_micros())))
}

#[rpn_fn(nullable, capture = [ctx])]
#[inline]
pub fn year(ctx: &mut EvalContext, t: Option<&DateTime>) -> Result<Option<Int>> {
    let t = match t {
        Some(v) => v,
        _ => return Ok(None),
    };

    if t.is_zero() {
        if ctx.cfg.sql_mode.contains(SqlMode::NO_ZERO_DATE) {
            return ctx
                .handle_invalid_time_error(Error::incorrect_datetime_value(t))
                .map(|_| Ok(None))?;
        }
        return Ok(Some(0));
    }
    Ok(Some(Int::from(t.year())))
}

#[rpn_fn(nullable, capture = [ctx])]
#[inline]
pub fn day_of_month(ctx: &mut EvalContext, t: Option<&DateTime>) -> Result<Option<Int>> {
    let t = match t {
        Some(v) => v,
        _ => return Ok(None),
    };

    if t.is_zero() {
        if ctx.cfg.sql_mode.contains(SqlMode::NO_ZERO_DATE) {
            return ctx
                .handle_invalid_time_error(Error::incorrect_datetime_value(t))
                .map(|_| Ok(None))?;
        }
        return Ok(Some(0));
    }
    Ok(Some(Int::from(t.day())))
}

#[rpn_fn(nullable, capture = [ctx])]
#[inline]
pub fn day_name(ctx: &mut EvalContext, t: Option<&DateTime>) -> Result<Option<Bytes>> {
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

#[rpn_fn(nullable)]
#[inline]
pub fn period_add(p: Option<&Int>, n: Option<&Int>) -> Result<Option<Int>> {
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

#[rpn_fn(nullable)]
#[inline]
pub fn period_diff(p1: Option<&Int>, p2: Option<&Int>) -> Result<Option<Int>> {
    match (p1, p2) {
        (Some(p1), Some(p2)) => Ok(Some(
            DateTime::period_to_month(*p1 as u64) as i64
                - DateTime::period_to_month(*p2 as u64) as i64,
        )),
        _ => Ok(None),
    }
}

#[rpn_fn(capture = [ctx])]
#[inline]
pub fn last_day(ctx: &mut EvalContext, t: &DateTime) -> Result<Option<DateTime>> {
    if t.month() == 0 {
        return ctx
            .handle_invalid_time_error(Error::incorrect_datetime_value(t))
            .map(|_| Ok(None))?;
    }
    if t.day() == 0 {
        let one_day = Duration::parse(ctx, "1 00:00:00", 6).unwrap();
        let adjusted_t: DateTime = t.checked_add(ctx, one_day).unwrap();
        return Ok(adjusted_t.last_date_of_month());
    }
    Ok(t.last_date_of_month())
}

#[rpn_fn(capture = [ctx])]
#[inline]
pub fn add_duration_and_duration(
    ctx: &mut EvalContext,
    duration1: &Duration,
    duration2: &Duration,
) -> Result<Option<Duration>> {
    let res = duration1.checked_add(*duration2);
    let res = match res {
        None => {
            return ctx
                .handle_invalid_time_error(Error::overflow(
                    "DURATION",
                    format!("({} + {})", duration1, duration2),
                ))
                .map(|_| Ok(None))?
        }
        Some(res) => res,
    };
    Ok(Some(res))
}

#[rpn_fn(capture = [ctx])]
#[inline]
pub fn add_duration_and_string(
    ctx: &mut EvalContext,
    arg1: &Duration,
    arg2: BytesRef,
) -> Result<Option<Duration>> {
    let arg2 = std::str::from_utf8(&arg2).map_err(Error::Encoding)?;
    let arg2 = match Duration::parse(ctx, arg2, MAX_FSP) {
        Ok(arg) => arg,
        Err(_) => return Ok(None),
    };

    let res = match arg1.checked_add(arg2) {
        Some(res) => res,
        None => {
            return ctx
                .handle_invalid_time_error(Error::overflow(
                    "DURATION",
                    format!("({} + {})", arg1, arg2),
                ))
                .map(|_| Ok(None))?;
        }
    };
    Ok(Some(res))
}

/// Cast Duration into string representation and drop subsec if possible.
fn duration_to_string(duration: Duration) -> String {
    match duration.subsec_micros() {
        0 => duration.minimize_fsp().to_string(),
        _ => duration.maximize_fsp().to_string(),
    }
}

/// Cast DateTime into string representation and drop subsec if possible.
fn datetime_to_string(mut datetime: DateTime) -> String {
    match datetime.micro() {
        0 => datetime.minimize_fsp(),
        _ => datetime.maximize_fsp(),
    };
    datetime.to_string()
}

#[cfg(test)]
mod tests {
    use super::*;

    use tipb::ScalarFuncSig;

    use crate::types::test_util::RpnFnScalarEvaluator;
    use tidb_query_datatype::builder::FieldTypeBuilder;
    use tidb_query_datatype::codec::error::ERR_TRUNCATE_WRONG_VALUE;
    use tidb_query_datatype::codec::mysql::{Time, MAX_FSP};
    use tidb_query_datatype::FieldTypeTp;

    #[test]
    fn test_add_duration_and_duration() {
        let cases = vec![
            (Some("00:01:01"), Some("00:01:01"), Some("00:02:02")),
            (Some("11:59:59"), Some("00:00:01"), Some("12:00:00")),
            (Some("23:59:59"), Some("00:00:01"), Some("24:00:00")),
            (Some("23:59:59"), Some("00:00:02"), Some("24:00:01")),
            (None, None, None),
        ];
        let mut ctx = EvalContext::default();
        for (duration1, duration2, exp) in cases {
            let expected = exp.map(|exp| Duration::parse(&mut ctx, exp, MAX_FSP).unwrap());
            let duration1 = duration1.map(|arg1| Duration::parse(&mut ctx, arg1, MAX_FSP).unwrap());
            let duration2 = duration2.map(|arg2| Duration::parse(&mut ctx, arg2, MAX_FSP).unwrap());
            let output = RpnFnScalarEvaluator::new()
                .push_param(duration1)
                .push_param(duration2)
                .evaluate(ScalarFuncSig::AddDurationAndDuration)
                .unwrap();
            assert_eq!(output, expected);
        }
    }

    #[test]
    fn test_add_duration_and_string() {
        let cases = vec![
            (Some("00:01:01"), Some("00:01:01"), Some("00:02:02")),
            (Some("11:59:59"), Some("00:00:01"), Some("12:00:00")),
            (Some("23:59:59"), Some("00:00:01"), Some("24:00:00")),
            (Some("23:59:59"), Some("00:00:02"), Some("24:00:01")),
            (None, None, None),
        ];
        let mut ctx = EvalContext::default();
        for (duration, string, exp) in cases {
            let expected = exp.map(|exp| Duration::parse(&mut ctx, exp, MAX_FSP).unwrap());
            let duration = duration.map(|arg1| Duration::parse(&mut ctx, arg1, MAX_FSP).unwrap());
            let string = string.map(|arg2| Duration::parse(&mut ctx, arg2, MAX_FSP).unwrap());
            let output = RpnFnScalarEvaluator::new()
                .push_param(duration)
                .push_param(string)
                .evaluate(ScalarFuncSig::AddDurationAndDuration)
                .unwrap();
            assert_eq!(output, expected);
        }
    }

    #[test]
    fn test_sub_duration_and_duration() {
        let cases = vec![
            (Some("00:02:02"), Some("00:01:01"), Some("00:01:01")),
            (Some("12:00:00"), Some("00:00:01"), Some("11:59:59")),
            (Some("24:00:00"), Some("00:00:01"), Some("23:59:59")),
            (Some("24:00:01"), Some("00:00:02"), Some("23:59:59")),
            (None, None, None),
        ];
        let mut ctx = EvalContext::default();
        for (duration1, duration2, exp) in cases {
            let expected = exp.map(|exp| Duration::parse(&mut ctx, exp, MAX_FSP).unwrap());
            let duration1 = duration1.map(|arg1| Duration::parse(&mut ctx, arg1, MAX_FSP).unwrap());
            let duration2 = duration2.map(|arg2| Duration::parse(&mut ctx, arg2, MAX_FSP).unwrap());
            let output = RpnFnScalarEvaluator::new()
                .push_param(duration1)
                .push_param(duration2)
                .evaluate(ScalarFuncSig::SubDurationAndDuration)
                .unwrap();
            assert_eq!(output, expected);
        }
    }
    #[test]
    fn test_date_format() {
        use std::sync::Arc;

        use tidb_query_datatype::expr::{EvalConfig, EvalContext, Flag, SqlMode};

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
                .push_param(date)
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
                .push_param(date)
                .push_param(format.clone())
                .evaluate::<Bytes>(ScalarFuncSig::DateFormatSig)
                .unwrap();
            assert_eq!(output, None, "{:?} {:?}", date, format);
        }
    }

    #[test]
    fn test_date() {
        let cases = vec![
            ("2011-11-11", Some("2011-11-11")),
            ("2011-11-11 10:10:10", Some("2011-11-11")),
            ("0000-00-00 00:00:00", None),
        ];
        let mut ctx = EvalContext::default();
        for (date, expect) in cases {
            let date = Some(DateTime::parse_datetime(&mut ctx, date, MAX_FSP, true).unwrap());
            let expect =
                expect.map(|expect| Time::parse_datetime(&mut ctx, expect, MAX_FSP, true).unwrap());

            let output = RpnFnScalarEvaluator::new()
                .push_param(date)
                .evaluate(ScalarFuncSig::Date)
                .unwrap();
            assert_eq!(output, expect, "{:?}", date);
        }
    }

    #[test]
    fn test_week_with_mode() {
        let cases = vec![
            ("2008-02-20 00:00:00", Some(1), Some(8i64)),
            ("2008-12-31 00:00:00", Some(1), Some(53i64)),
            ("2000-01-01", Some(0), Some(0i64)),
            ("2008-02-20", Some(0), Some(7i64)),
            ("2017-01-01", Some(0), Some(1i64)),
            ("2017-01-01", Some(1), Some(0i64)),
            ("2017-01-01", Some(2), Some(1i64)),
            ("2017-01-01", Some(3), Some(52i64)),
            ("2017-01-01", Some(4), Some(1i64)),
            ("2017-01-01", Some(5), Some(0i64)),
            ("2017-01-01", Some(6), Some(1i64)),
            ("2017-01-01", Some(7), Some(52i64)),
            ("2017-12-31", Some(0), Some(53i64)),
            ("2017-12-31", Some(1), Some(52i64)),
            ("2017-12-31", Some(2), Some(53i64)),
            ("2017-12-31", Some(3), Some(52i64)),
            ("2017-12-31", Some(4), Some(53i64)),
            ("2017-12-31", Some(5), Some(52i64)),
            ("2017-12-31", Some(6), Some(1i64)),
            ("2017-12-31", Some(7), Some(52i64)),
            ("2017-12-31", Some(14), Some(1i64)),
            ("2017-12-31", None::<Int>, None),
            ("0000-00-00", Some(0), None),
            ("2018-12-00", Some(0), None),
            ("2018-00-03", Some(0), None),
        ];
        let mut ctx = EvalContext::default();
        for (arg1, arg2, exp) in cases {
            let datetime = Some(DateTime::parse_datetime(&mut ctx, arg1, 6, true).unwrap());
            let output = RpnFnScalarEvaluator::new()
                .push_param(datetime)
                .push_param(arg2)
                .evaluate(ScalarFuncSig::WeekWithMode)
                .unwrap();
            assert_eq!(output, exp);
        }
        let output = RpnFnScalarEvaluator::new()
            .push_param(None::<DateTime>)
            .push_param(Some(0))
            .evaluate::<Int>(ScalarFuncSig::WeekWithMode)
            .unwrap();
        assert_eq!(output, None);
    }

    #[test]
    fn test_week_without_mode() {
        let cases = vec![
            ("2000-01-01", Some(0i64)),
            ("2008-02-20", Some(7i64)),
            ("2017-01-01", Some(1i64)),
            ("0000-00-00", None),
            ("2018-12-00", None),
            ("2018-00-03", None),
        ];
        let mut ctx = EvalContext::default();
        for (datetime, exp) in cases {
            let datetime = DateTime::parse_datetime(&mut ctx, datetime, MAX_FSP, true).unwrap();
            let output = RpnFnScalarEvaluator::new()
                .push_param(datetime)
                .evaluate(ScalarFuncSig::WeekWithoutMode)
                .unwrap();
            assert_eq!(output, exp);
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
                .push_param(datetime)
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
                .push_param(datetime)
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
    fn test_year_week_with_mode() {
        let cases = vec![
            ("1987-01-01", 0, Some(198652i64)),
            ("2000-01-01", 0, Some(199952i64)),
            ("0000-01-01", 0, Some(1i64)),
            ("0000-01-01", 1, Some(4294967295i64)),
            ("0000-01-01", 2, Some(1i64)),
            ("0000-01-01", 3, Some(4294967295i64)),
            ("0000-01-01", 4, Some(1i64)),
            ("0000-01-01", 5, Some(4294967295i64)),
            ("0000-01-01", 6, Some(1i64)),
            ("0000-01-01", 7, Some(4294967295i64)),
            ("0000-01-01", 15, Some(4294967295i64)),
            ("0000-00-00", 0, None),
        ];
        let mut ctx = EvalContext::default();
        for (arg1, arg2, exp) in cases {
            let datetime = DateTime::parse_datetime(&mut ctx, arg1, MAX_FSP, true).unwrap();
            let output = RpnFnScalarEvaluator::new()
                .push_param(datetime)
                .push_param(arg2)
                .evaluate(ScalarFuncSig::YearWeekWithMode)
                .unwrap();
            assert_eq!(output, exp);
        }
    }

    #[test]
    fn test_year_week_without_mode() {
        let cases = vec![
            ("1987-01-01", Some(198652i64)),
            ("2000-01-01", Some(199952i64)),
            ("0000-01-01", Some(1i64)),
            ("0000-00-00", None),
        ];
        let mut ctx = EvalContext::default();
        for (datetime, exp) in cases {
            let datetime = DateTime::parse_datetime(&mut ctx, datetime, MAX_FSP, true).unwrap();
            let output = RpnFnScalarEvaluator::new()
                .push_param(datetime)
                .evaluate(ScalarFuncSig::YearWeekWithoutMode)
                .unwrap();
            assert_eq!(output, exp);
        }
    }

    #[test]
    fn test_day_of_week() {
        let cases = vec![
            ("2018-11-11 00:00:00.000000", Some(1)),
            ("2018-11-12 00:00:00.000000", Some(2)),
            ("2018-11-13 00:00:00.000000", Some(3)),
            ("2018-11-14 00:00:00.000000", Some(4)),
            ("2018-11-15 00:00:00.000000", Some(5)),
            ("2018-11-16 00:00:00.000000", Some(6)),
            ("2018-11-17 00:00:00.000000", Some(7)),
            ("2018-11-18 00:00:00.000000", Some(1)),
            ("0000-00-00 00:00:00.000000", None),
            ("2018-11-00 00:00:00.000000", None),
            ("2018-00-11 00:00:00.000000", None),
        ];
        let mut ctx = EvalContext::default();
        for (arg, exp) in cases {
            let datetime = Some(DateTime::parse_datetime(&mut ctx, arg, 6, true).unwrap());
            let output = RpnFnScalarEvaluator::new()
                .push_param(datetime)
                .evaluate(ScalarFuncSig::DayOfWeek)
                .unwrap();
            assert_eq!(output, exp);
        }
        let output = RpnFnScalarEvaluator::new()
            .push_param(None::<DateTime>)
            .evaluate::<Int>(ScalarFuncSig::DayOfWeek)
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
                .push_param(datetime)
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
                .push_param(time)
                .evaluate(ScalarFuncSig::ToDays)
                .unwrap();
            assert_eq!(output, exp);
        }
    }

    #[test]
    fn test_to_seconds() {
        let cases = vec![
            ("950501", Some(62966505600)),
            ("2009-11-29", Some(63426672000)),
            ("2009-11-29 13:43:32", Some(63426721412)),
            ("09-11-29 13:43:32", Some(63426721412)),
            ("99-11-29 13:43:32", Some(63111102212)),
            ("0000-00-00 00:00:00", None),
        ];

        let mut ctx = EvalContext::default();
        for (arg, exp) in cases {
            let time = Time::parse_datetime(&mut ctx, arg, 6, true).unwrap();
            let output = RpnFnScalarEvaluator::new()
                .push_param(time)
                .evaluate(ScalarFuncSig::ToSeconds)
                .unwrap();
            assert_eq!(output, exp);
        }
    }

    #[test]
    fn test_add_sub_string_and_duration() {
        let cases = vec![
            // normal cases
            (
                Some("01:00:00.999999"),
                Some("02:00:00.999998"),
                Some("03:00:01.999997"),
            ),
            (Some("23:59:59"), Some("00:00:01"), Some("24:00:00")),
            (Some("110:00:00"), Some("1 02:00:00"), Some("136:00:00")),
            (Some("-110:00:00"), Some("1 02:00:00"), Some("-84:00:00")),
            (Some("00:00:01"), Some("-00:00:01"), Some("00:00:00")),
            (Some("00:00:03"), Some("-00:00:01"), Some("00:00:02")),
            (
                Some("2018-02-28 23:00:00"),
                Some("01:30:30.123456"),
                Some("2018-03-01 00:30:30.123456"),
            ),
            (
                Some("2016-02-28 23:00:00"),
                Some("01:30:30"),
                Some("2016-02-29 00:30:30"),
            ),
            (
                Some("2018-12-31 23:00:00"),
                Some("01:30:30"),
                Some("2019-01-01 00:30:30"),
            ),
            (
                Some("2018-12-31 23:00:00"),
                Some("1 01:30:30"),
                Some("2019-01-02 00:30:30"),
            ),
            (
                Some("2019-01-01 01:00:00"),
                Some("-01:01:00"),
                Some("2018-12-31 23:59:00"),
            ),
            // null cases
            (None, None, None),
            (None, Some("11:30:45.123456"), None),
            (Some("00:00:00"), None, Some("00:00:00")),
            (Some("01:00:00"), None, Some("01:00:00")),
            (
                Some("2019-01-01 01:00:00"),
                None,
                Some("2019-01-01 01:00:00"),
            ),
        ];

        for (arg_str, arg_dur, sum) in cases {
            let arg_str = arg_str.map(|str| str.as_bytes().to_vec());
            let arg_dur = match arg_dur {
                Some(arg_dur) => {
                    Some(Duration::parse(&mut EvalContext::default(), arg_dur, MAX_FSP).unwrap())
                }
                None => Some(Duration::zero()),
            };
            let sum = sum.map(|str| str.as_bytes().to_vec());
            let add_output = RpnFnScalarEvaluator::new()
                .push_param(arg_str.clone())
                .push_param(arg_dur)
                .evaluate(ScalarFuncSig::AddStringAndDuration)
                .unwrap();
            assert_eq!(add_output, sum);

            let sub_output = RpnFnScalarEvaluator::new()
                .push_param(sum)
                .push_param(arg_dur)
                .evaluate(ScalarFuncSig::SubStringAndDuration)
                .unwrap();
            assert_eq!(sub_output, arg_str);
        }
    }

    #[test]
    fn test_date_diff() {
        let cases = vec![
            (
                "0000-01-01 00:00:00.000000",
                "0000-01-01 00:00:00.000000",
                Some(0),
            ),
            (
                "2018-02-01 00:00:00.000000",
                "2018-02-01 00:00:00.000000",
                Some(0),
            ),
            (
                "2018-02-02 00:00:00.000000",
                "2018-02-01 00:00:00.000000",
                Some(1),
            ),
            (
                "2018-02-01 00:00:00.000000",
                "2018-02-02 00:00:00.000000",
                Some(-1),
            ),
            (
                "2018-02-02 00:00:00.000000",
                "2018-02-01 23:59:59.999999",
                Some(1),
            ),
            (
                "2018-02-01 23:59:59.999999",
                "2018-02-02 00:00:00.000000",
                Some(-1),
            ),
        ];
        let mut ctx = EvalContext::default();
        for (arg1, arg2, exp) in cases {
            let arg1 = Time::parse_datetime(&mut ctx, arg1, 6, true).unwrap();
            let arg2 = Time::parse_datetime(&mut ctx, arg2, 6, true).unwrap();
            let output = RpnFnScalarEvaluator::new()
                .push_param(arg1)
                .push_param(arg2)
                .evaluate(ScalarFuncSig::DateDiff)
                .unwrap();
            assert_eq!(output, exp);
        }
    }

    #[test]
    fn test_add_datetime_and_duration() {
        let mut ctx = EvalContext::default();
        let cases = vec![
            // null cases
            (None, None, None),
            (None, Some("11:30:45.123456"), None),
            (Some("2019-01-01 01:00:00"), None, None),
            // normal cases
            (
                Some("2018-01-01"),
                Some("11:30:45.123456"),
                Some("2018-01-01 11:30:45.123456"),
            ),
            (
                Some("2018-02-28 23:00:00"),
                Some("01:30:30.123456"),
                Some("2018-03-01 00:30:30.123456"),
            ),
            (
                Some("2016-02-28 23:00:00"),
                Some("01:30:30"),
                Some("2016-02-29 00:30:30"),
            ),
            (
                Some("2018-12-31 23:00:00"),
                Some("01:30:30"),
                Some("2019-01-01 00:30:30"),
            ),
            (
                Some("2018-12-31 23:00:00"),
                Some("1 01:30:30"),
                Some("2019-01-02 00:30:30"),
            ),
            (
                Some("2019-01-01 01:00:00"),
                Some("-01:01:00"),
                Some("2018-12-31 23:59:00"),
            ),
        ];
        for (arg1, arg2, exp) in cases {
            let exp = exp.map(|exp| Time::parse_datetime(&mut ctx, exp, MAX_FSP, true).unwrap());
            let arg1 =
                arg1.map(|arg1| Time::parse_datetime(&mut ctx, arg1, MAX_FSP, true).unwrap());
            let arg2 = arg2.map(|arg2| Duration::parse(&mut ctx, arg2, MAX_FSP).unwrap());
            let output = RpnFnScalarEvaluator::new()
                .push_param(arg1)
                .push_param(arg2)
                .evaluate(ScalarFuncSig::AddDatetimeAndDuration)
                .unwrap();
            assert_eq!(output, exp);
        }
    }

    #[test]
    fn test_add_datetime_and_string() {
        let mut ctx = EvalContext::default();
        let cases = vec![
            // null cases
            (None, None, None),
            (None, Some("11:30:45.123456"), None),
            (Some("2019-01-01 01:00:00"), None, None),
            // normal cases
            (
                Some("2018-01-01"),
                Some("11:30:45.123456"),
                Some("2018-01-01 11:30:45.123456"),
            ),
            (
                Some("2018-02-28 23:00:00"),
                Some("01:30:30.123456"),
                Some("2018-03-01 00:30:30.123456"),
            ),
            (
                Some("2016-02-28 23:00:00"),
                Some("01:30:30"),
                Some("2016-02-29 00:30:30"),
            ),
            (
                Some("2018-12-31 23:00:00"),
                Some("01:30:30"),
                Some("2019-01-01 00:30:30"),
            ),
        ];
        for (arg0, arg1, exp) in cases {
            let exp = exp.map(|exp| Time::parse_datetime(&mut ctx, exp, MAX_FSP, true).unwrap());
            let arg0 =
                arg0.map(|arg0| Time::parse_datetime(&mut ctx, arg0, MAX_FSP, true).unwrap());
            let arg1 = arg1.map(|str| str.as_bytes().to_vec());

            let output = RpnFnScalarEvaluator::new()
                .push_param(arg0)
                .push_param(arg1)
                .evaluate(ScalarFuncSig::AddDatetimeAndString)
                .unwrap();
            assert_eq!(output, exp);
        }
    }

    #[test]
    fn test_sub_datetime_and_duration() {
        let mut ctx = EvalContext::default();
        let cases = vec![
            // null cases
            (None, None, None, true),
            (None, Some("11:30:45.123456"), None, true),
            (Some("2019-01-01 01:00:00"), None, None, true),
            // normal cases
            (
                Some("2018-01-01"),
                Some("11:30:45.123456"),
                Some("2018-01-01 11:30:45.123456"),
                false,
            ),
            (
                Some("2018-02-28 23:00:00"),
                Some("01:30:30.123456"),
                Some("2018-03-01 00:30:30.123456"),
                false,
            ),
            (
                Some("2016-02-28 23:00:00"),
                Some("01:30:30"),
                Some("2016-02-29 00:30:30"),
                false,
            ),
            (
                Some("2018-12-31 23:00:00"),
                Some("01:30:30"),
                Some("2019-01-01 00:30:30"),
                false,
            ),
            (
                Some("2018-12-31 23:00:00"),
                Some("1 01:30:30"),
                Some("2019-01-02 00:30:30"),
                false,
            ),
            (
                Some("2019-01-01 01:00:00"),
                Some("-01:01:00"),
                Some("2018-12-31 23:59:00"),
                false,
            ),
        ];
        for (arg1, arg2, exp, null_case) in cases {
            let exp = exp.map(|exp| Time::parse_datetime(&mut ctx, exp, MAX_FSP, true).unwrap());
            let arg1 =
                arg1.map(|arg1| Time::parse_datetime(&mut ctx, arg1, MAX_FSP, true).unwrap());
            let arg2 = arg2.map(|arg2| Duration::parse(&mut ctx, arg2, MAX_FSP).unwrap());
            if null_case {
                let output = RpnFnScalarEvaluator::new()
                    .push_param(arg1)
                    .push_param(arg2)
                    .evaluate(ScalarFuncSig::SubDatetimeAndDuration)
                    .unwrap();
                assert_eq!(output, exp);
            } else {
                let output = RpnFnScalarEvaluator::new()
                    .push_param(exp)
                    .push_param(arg2)
                    .evaluate(ScalarFuncSig::SubDatetimeAndDuration)
                    .unwrap();
                assert_eq!(output, arg1);
            }
        }
    }

    #[test]
    fn test_sub_datetime_and_string() {
        let mut ctx = EvalContext::default();
        let cases = vec![
            // null cases
            (None, None, None),
            (None, Some("11:30:45.123456"), None),
            (Some("2019-01-01 01:00:00"), None, None),
            // normal cases
            (
                Some("2018-01-01 11:30:45.123456"),
                Some("11:30:45.123456"),
                Some("2018-01-01"),
            ),
            (
                Some("2018-03-01 00:30:30.123456"),
                Some("01:30:30.123456"),
                Some("2018-02-28 23:00:00"),
            ),
            (
                Some("2016-02-29 00:30:30"),
                Some("01:30:30"),
                Some("2016-02-28 23:00:00"),
            ),
            (
                Some("2019-01-01 00:30:30"),
                Some("01:30:30"),
                Some("2018-12-31 23:00:00"),
            ),
        ];
        for (arg0, arg1, exp) in cases {
            let exp = exp.map(|exp| Time::parse_datetime(&mut ctx, exp, MAX_FSP, true).unwrap());
            let arg0 =
                arg0.map(|arg0| Time::parse_datetime(&mut ctx, arg0, MAX_FSP, true).unwrap());
            let arg1 = arg1.map(|str| str.as_bytes().to_vec());

            let output = RpnFnScalarEvaluator::new()
                .push_param(arg0)
                .push_param(arg1)
                .evaluate(ScalarFuncSig::SubDatetimeAndString)
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
    fn test_make_date() {
        let null_cases = vec![
            (None, None),
            (Some(2014i64), Some(0i64)),
            (Some(10000i64), Some(1i64)),
            (Some(9999i64), Some(366i64)),
            (Some(-1i64), Some(1i64)),
            (Some(-4294965282i64), Some(1i64)),
            (Some(0i64), Some(0i64)),
            (Some(0i64), Some(-1i64)),
            (Some(10i64), Some(-1i64)),
            (Some(0i64), Some(9223372036854775807i64)),
            (Some(100i64), Some(9999 * 366i64)),
            (Some(9999i64), Some(9999 * 366i64)),
            (Some(100i64), Some(3615901i64)),
        ];
        for (arg1, arg2) in null_cases {
            let exp: Option<Time> = None;
            let output = RpnFnScalarEvaluator::new()
                .push_param(arg1)
                .push_param(arg2)
                .evaluate(ScalarFuncSig::MakeDate)
                .unwrap();
            assert_eq!(output, exp);
        }
        let cases = vec![
            (0, 1, "2000-01-01"),
            (70, 1, "1970-01-01"),
            (71, 1, "1971-01-01"),
            (99, 1, "1999-01-01"),
            (100, 1, "0100-01-01"),
            (101, 1, "0101-01-01"),
            (2014, 224234, "2627-12-07"),
            (2014, 1, "2014-01-01"),
            (7900, 705000, "9830-03-23"),
            (7901, 705000, "9831-03-23"),
            (7904, 705000, "9834-03-22"),
            (8000, 705000, "9930-03-23"),
            (8001, 705000, "9931-03-24"),
            (100, 3615900, "9999-12-31"),
        ];
        let mut ctx = EvalContext::default();
        for (arg1, arg2, exp) in cases {
            let exp = Some(Time::parse_date(&mut ctx, exp).unwrap());
            let arg2 = Some(arg2);
            let arg1 = Some(arg1);
            let output = RpnFnScalarEvaluator::new()
                .push_param(arg1)
                .push_param(arg2)
                .evaluate(ScalarFuncSig::MakeDate)
                .unwrap();
            assert_eq!(output, exp);
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
    fn test_month_name() {
        let cases = vec![
            (None, None, None),
            (
                Some("0000-00-00 00:00:00"),
                Some(ERR_TRUNCATE_WRONG_VALUE),
                None,
            ),
            (
                Some("2018-01-00 01:01:01"),
                Some(ERR_TRUNCATE_WRONG_VALUE),
                None,
            ),
            (
                Some("2018-00-00 01:01:01"),
                Some(ERR_TRUNCATE_WRONG_VALUE),
                None,
            ),
            (
                Some("2018-00-01 01:01:01"),
                Some(ERR_TRUNCATE_WRONG_VALUE),
                None,
            ),
            (Some("2018-01-01 01:01:01"), None, Some("January")),
            (Some("2018-02-01 01:01:01"), None, Some("February")),
            (Some("2018-03-01 01:01:01"), None, Some("March")),
            (Some("2018-04-01 01:01:01"), None, Some("April")),
            (Some("2018-05-01 01:01:01"), None, Some("May")),
            (Some("2018-06-01 01:01:01"), None, Some("June")),
            (Some("2018-07-01 01:01:01"), None, Some("July")),
            (Some("2018-08-01 01:01:01"), None, Some("August")),
            (Some("2018-09-01 01:01:01"), None, Some("September")),
            (Some("2018-10-01 01:01:01"), None, Some("October")),
            (Some("2018-11-01 01:01:01"), None, Some("November")),
            (Some("2018-12-01 01:01:01"), None, Some("December")),
        ];
        for (time, err_code, expect) in cases {
            let mut ctx = EvalContext::default();
            let time = time.map(|time: &str| Time::parse_date(&mut ctx, time).unwrap());
            let (output, ctx) = RpnFnScalarEvaluator::new()
                .push_param(time)
                .context(ctx)
                .evaluate_raw(FieldTypeTp::String, ScalarFuncSig::MonthName);
            let output = output.unwrap();
            assert_eq!(output.as_bytes(), expect.map(|v| v.as_bytes()));
            if let Some(err_code) = err_code {
                assert_eq!(ctx.warnings.warnings[0].get_code(), err_code);
            }
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
            let duration = Some(Duration::parse(&mut EvalContext::default(), arg, fsp).unwrap());
            let test_case_func = |sig, res| {
                let output = RpnFnScalarEvaluator::new()
                    .push_param(duration)
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
    fn test_time_to_sec() {
        let cases: Vec<(&str, i8, i64)> = vec![
            ("31 11:30:45", 0, 2719845),
            ("11:30:45.123345", 3, 41445),
            ("-11:30:45.1233456", 0, -41445),
            ("272:59:59.14", 0, 982799),
        ];
        for (arg, fsp, s) in cases {
            let duration = Some(Duration::parse(&mut EvalContext::default(), arg, fsp).unwrap());
            let output = RpnFnScalarEvaluator::new()
                .push_param(duration)
                .evaluate::<Int>(ScalarFuncSig::TimeToSec)
                .unwrap();
            assert_eq!(output, Some(s));
        }
        // test NULL case
        let output = RpnFnScalarEvaluator::new()
            .push_param(None::<Duration>)
            .evaluate::<Int>(ScalarFuncSig::TimeToSec)
            .unwrap();
        assert_eq!(output, None);
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
            assert_eq!(output.as_bytes(), exp.map(|v| v.as_bytes()));
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

    #[test]
    fn test_last_day() {
        let cases = vec![
            ("2011-11-11", "2011-11-30"),
            ("2008-02-10", "2008-02-29"),
            ("2000-02-11", "2000-02-29"),
            ("2100-02-11", "2100-02-28"),
            ("2011-11-11", "2011-11-30"),
            ("2011-11-11 10:10:10", "2011-11-30 00:00:00"),
            ("2011-01-00 10:00:00", "2011-01-31"),
        ];
        let mut ctx = EvalContext::default();
        for (arg, exp) in cases {
            let time = Some(Time::parse_date(&mut ctx, arg).unwrap());
            let exp_val = Some(Time::parse_date(&mut ctx, exp).unwrap());
            let output = RpnFnScalarEvaluator::new()
                .push_param(time)
                .evaluate(ScalarFuncSig::LastDay)
                .unwrap();
            assert_eq!(output, exp_val);
        }
        let none_cases = vec!["2011-00-01 10:10:10"];
        for case in none_cases {
            let time = Some(Time::parse_date(&mut ctx, case).unwrap());
            let output = RpnFnScalarEvaluator::new()
                .push_param(time)
                .evaluate::<Time>(ScalarFuncSig::LastDay)
                .unwrap();
            assert_eq!(output, None);
        }
        let output = RpnFnScalarEvaluator::new()
            .push_param(None::<Time>)
            .evaluate::<Time>(ScalarFuncSig::LastDay)
            .unwrap();
        assert_eq!(output, None);
    }

    #[test]
    fn test_make_time() {
        let cases = vec![
            (12_i64, 15_i64, 30_f64, "12:15:30"),
            (25, 15, 30.0, "25:15:30"),
            (-25, 15, 30_f64, "-25:15:30"),
            (12, 15, 30.1, "12:15:30.1"),
            (12, 15, 30.2, "12:15:30.2"),
            (12, 15, 30.3000001, "12:15:30.3"),
            (12, 15, 30.0000005, "12:15:30.000001"),
            (0, 0, 0.0, "00:00:00"),
            (0, 1, 59.1, "00:01:59.1"),
            (837, 59, 59.1, "837:59:59.1"),
            (838, 59, 59.1, "838:59:59"),
            (838, 58, 59.1, "838:58:59.1"),
            (838, 0, 59.1, "838:00:59.1"),
            (-838, 59, 59.1, "-838:59:59"),
            (1000, 1, 1.0, "838:59:59"),
            (-1000, 1, 1.23, "-838:59:59"),
            (1000, 59, 1.0, "838:59:59"),
            (1000, 1, 59.1, "838:59:59"),
            (i64::MIN, 0, 1.0, "-838:59:59"),
            (i64::MAX, 0, 0.0, "838:59:59"),
        ];
        let mut ctx = EvalContext::default();
        for (hour, minute, second, ans) in cases {
            for fsp in 0..MAX_FSP {
                let ans_val = Some(Duration::parse(&mut ctx, ans, fsp).unwrap());
                let output = RpnFnScalarEvaluator::new()
                    .push_param(Some(hour))
                    .push_param(Some(minute))
                    .push_param(Some(Real::new(second).unwrap()))
                    .return_field_type(
                        FieldTypeBuilder::new()
                            .tp(FieldTypeTp::Duration)
                            .decimal(fsp as isize)
                            .build(),
                    )
                    .evaluate(ScalarFuncSig::MakeTime)
                    .unwrap();
                assert_eq!(output, ans_val);
            }
        }
        let none_case = vec![
            (12_i64, -15_i64, 30_f64),
            (12, 15, -30.0),
            (12, 15, 60.0),
            (12, 60, 0.0),
            (i64::MAX, i64::MAX, f64::MAX),
        ];
        for (hour, minute, second) in none_case {
            for fsp in 0..MAX_FSP {
                let output = RpnFnScalarEvaluator::new()
                    .push_param(Some(hour))
                    .push_param(Some(minute))
                    .push_param(Some(Real::new(second).unwrap()))
                    .return_field_type(
                        FieldTypeBuilder::new()
                            .tp(FieldTypeTp::Duration)
                            .decimal(fsp as isize)
                            .build(),
                    )
                    .evaluate::<Duration>(ScalarFuncSig::MakeTime)
                    .unwrap();
                assert_eq!(output, None);
            }
        }
        {
            let output = RpnFnScalarEvaluator::new()
                .push_param(None::<i64>)
                .push_param(None::<i64>)
                .push_param(None::<Real>)
                .return_field_type(
                    FieldTypeBuilder::new()
                        .tp(FieldTypeTp::Duration)
                        .decimal(MAX_FSP as isize)
                        .build(),
                )
                .evaluate::<Duration>(ScalarFuncSig::MakeTime)
                .unwrap();
            assert_eq!(output, None);
        }
        {
            let output = RpnFnScalarEvaluator::new()
                .push_param(Some::<i64>(1))
                .push_param(Some::<i64>(1))
                .push_param(Some::<f64>(1.0))
                .return_field_type(
                    FieldTypeBuilder::new()
                        .tp(FieldTypeTp::Duration)
                        .decimal((MAX_FSP + 1) as isize)
                        .build(),
                )
                .evaluate::<Duration>(ScalarFuncSig::MakeTime);
            assert!(output.is_err());
        }
    }
}
