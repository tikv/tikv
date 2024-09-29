// Copyright 2024 TiKV Project Authors. Licensed under Apache-2.0.

use std::{collections::HashMap, str::FromStr};

use lazy_static::lazy_static;
use regex::Regex;

use crate::{
    codec::{
        data_type::{BytesRef, Decimal, Real},
        mysql::{duration::*, RoundMode, DEFAULT_FSP, MAX_FSP, MIN_FSP},
        Error, Result,
    },
    expr::EvalContext,
};

/// See https://dev.mysql.com/doc/refman/8.0/en/expressions.html#temporal-intervals
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum IntervalUnit {
    Microsecond,
    Second,
    Minute,
    Hour,
    Day,
    Week,
    Month,
    Quarter,
    Year,
    SecondMicrosecond,
    MinuteMicrosecond,
    MinuteSecond,
    HourMicrosecond,
    HourSecond,
    HourMinute,
    DayMicrosecond,
    DaySecond,
    DayMinute,
    DayHour,
    YearMonth,
}

impl IntervalUnit {
    pub fn from_str(unit: &str) -> Result<Self> {
        use IntervalUnit::*;
        match unit.to_uppercase().as_str() {
            "MICROSECOND" => Ok(Microsecond),
            "SECOND" => Ok(Second),
            "MINUTE" => Ok(Minute),
            "HOUR" => Ok(Hour),
            "DAY" => Ok(Day),
            "WEEK" => Ok(Week),
            "MONTH" => Ok(Month),
            "QUARTER" => Ok(Quarter),
            "YEAR" => Ok(Year),
            "SECOND_MICROSECOND" => Ok(SecondMicrosecond),
            "MINUTE_MICROSECOND" => Ok(MinuteMicrosecond),
            "MINUTE_SECOND" => Ok(MinuteSecond),
            "HOUR_MICROSECOND" => Ok(HourMicrosecond),
            "HOUR_SECOND" => Ok(HourSecond),
            "HOUR_MINUTE" => Ok(HourMinute),
            "DAY_MICROSECOND" => Ok(DayMicrosecond),
            "DAY_SECOND" => Ok(DaySecond),
            "DAY_MINUTE" => Ok(DayMinute),
            "DAY_HOUR" => Ok(DayHour),
            "YEAR_MONTH" => Ok(YearMonth),
            _ => Err(box_err!("unknown unit str {}", unit)),
        }
    }

    pub fn is_clock_unit(&self) -> bool {
        use IntervalUnit::*;
        matches!(
            self,
            Microsecond
                | Second
                | Minute
                | Hour
                | SecondMicrosecond
                | MinuteMicrosecond
                | HourMicrosecond
                | DayMicrosecond
                | MinuteSecond
                | HourSecond
                | DaySecond
                | HourMinute
                | DayMinute
                | DayHour
        )
    }
}

#[derive(Clone, Copy, PartialEq)]
#[repr(usize)]
enum TimeIndex {
    Year = 0,
    Month = 1,
    Day = 2,
    Hour = 3,
    Minute = 4,
    Second = 5,
    Microsecond = 6,
    Max = 7,
}

lazy_static! {
    static ref ONE_TO_SIX_DIGIT_REGEX: Regex = Regex::new(r"^[0-9]{0,6}").unwrap();
    static ref NUMERIC_REGEX: Regex = Regex::new(r"[0-9]+").unwrap();
    static ref INTERVAL_REGEX: Regex = Regex::new(r"^[+-]?[\d]+").unwrap();
    /// Index of 'YEARS-MONTHS DAYS HOURS:MINUTES:SECONDS.MICROSECONDS' interval string Format.
    /// IntervalUnit -> (Time Index, Max Count)
    static ref INTERVAL_STR_INDEX_MAP: HashMap<IntervalUnit, (TimeIndex, usize)> = {
        [
            // 'SECONDS.MICROSECONDS'
            (IntervalUnit::SecondMicrosecond, (TimeIndex::Microsecond, 2)),
            // 'MINUTES:SECONDS.MICROSECONDS'
            (IntervalUnit::MinuteMicrosecond, (TimeIndex::Microsecond, 3)),
            // 'MINUTES:SECONDS'
            (IntervalUnit::MinuteSecond, (TimeIndex::Second, 2)),
            // 'HOURS:MINUTES:SECONDS.MICROSECONDS'
            (IntervalUnit::HourMicrosecond, (TimeIndex::Microsecond, 4)),
            // 'HOURS:MINUTES:SECONDS'
            (IntervalUnit::HourSecond, (TimeIndex::Second, 3)),
            // 'HOURS:MINUTES'
            (IntervalUnit::HourMinute, (TimeIndex::Minute, 2)),
            // 'DAYS HOURS:MINUTES:SECONDS.MICROSECONDS'
            (IntervalUnit::DayMicrosecond, (TimeIndex::Microsecond, 5)),
            // 'DAYS HOURS:MINUTES:SECONDS'
            (IntervalUnit::DaySecond, (TimeIndex::Second, 4)),
            // 'DAYS HOURS:MINUTES'
            (IntervalUnit::DayMinute, (TimeIndex::Minute, 3)),
            // 'DAYS HOURS'
            (IntervalUnit::DayHour, (TimeIndex::Hour, 2)),
            // 'YEARS-MONTHS'
            (IntervalUnit::YearMonth, (TimeIndex::Month, 2)),
        ].iter().cloned().collect()
    };
}

#[derive(Debug, PartialEq)]
pub struct Interval {
    month: i64,
    nano: i64,
    fsp: i8,
}

impl Interval {
    pub fn parse_from_str(ctx: &mut EvalContext, unit: &IntervalUnit, input: &str) -> Result<Self> {
        Self::parse_from_str_internal(ctx, unit, input, false)
    }

    #[inline]
    fn parse_from_str_internal(
        ctx: &mut EvalContext,
        unit: &IntervalUnit,
        input: &str,
        for_duration: bool,
    ) -> Result<Self> {
        if let Some(&(index, max_cnt)) = INTERVAL_STR_INDEX_MAP.get(unit) {
            Self::parse_time_value(ctx, input, index, max_cnt, for_duration)
        } else {
            Self::parse_single_time_value(ctx, unit, input, for_duration)
        }
    }

    fn parse_single_time_value(
        ctx: &mut EvalContext,
        unit: &IntervalUnit,
        input: &str,
        for_duration: bool,
    ) -> Result<Self> {
        use IntervalUnit::*;
        // Find decimal point position
        let decimal_point_pos = input.find('.').unwrap_or(input.len());

        // Handle negative sign
        let mut sign: i64 = 1;
        let integer_part = if input.starts_with('-') {
            sign = -1;
            &input[1..decimal_point_pos]
        } else {
            &input[..decimal_point_pos]
        };

        // Parse integer part before decimal point
        let iv = match i64::from_str(integer_part) {
            Ok(val) => val * sign,
            Err(_) => {
                if for_duration {
                    return Err(Error::incorrect_datetime_value(input));
                }
                ctx.handle_invalid_time_error(Error::incorrect_datetime_value(input))?;
                0
            }
        };
        // Rounded integer value
        let mut riv = iv;

        // Handle decimal part
        let mut decimal_len = 0;
        let mut dv = 0i64;
        if decimal_point_pos < input.len() - 1 {
            let dv_pre = &input[decimal_point_pos + 1..];
            let mut dv_pre = ONE_TO_SIX_DIGIT_REGEX
                .find(dv_pre)
                .map_or("", |m| m.as_str())
                .to_string();
            decimal_len = dv_pre.len();
            if decimal_len < MAX_FSP as usize {
                dv_pre.push_str(&"0".repeat(MAX_FSP as usize - decimal_len));
            }
            decimal_len = std::cmp::min(decimal_len, MAX_FSP as usize);

            dv = match i64::from_str(&dv_pre[..MAX_FSP as usize]) {
                Ok(val) => val,
                Err(_) => {
                    if for_duration {
                        return Err(Error::incorrect_datetime_value(input));
                    }
                    ctx.handle_invalid_time_error(Error::incorrect_datetime_value(input))?;
                    0
                }
            };

            // Round up, and we should keep 6 digits for microsecond, so dv should in
            // [000000, 999999].
            if dv >= 500_000 {
                riv += sign;
            }
            if *unit != Second {
                if for_duration {
                    return Err(Error::incorrect_datetime_value(input));
                }
                ctx.handle_invalid_time_error(Error::incorrect_datetime_value(input))?;
            }
            dv *= sign;
        }

        match unit {
            Microsecond => {
                if for_duration && riv.abs() > MAX_SECS * 1_000 {
                    return Err(Error::datetime_function_overflow());
                }
                Ok(Self {
                    month: 0,
                    nano: riv * NANOS_PER_MICRO,
                    fsp: MAX_FSP,
                })
            }
            Second => {
                if for_duration && iv.abs() > MAX_SECS {
                    return Err(Error::datetime_function_overflow());
                }
                Ok(Self {
                    month: 0,
                    nano: iv * NANOS_PER_SEC + dv * NANOS_PER_MICRO,
                    fsp: decimal_len as i8,
                })
            }
            Minute => {
                if for_duration && riv.abs() > (MAX_HOUR_PART * 60 + MAX_MINUTE_PART) as i64 {
                    return Err(Error::datetime_function_overflow());
                }
                Ok(Self {
                    month: 0,
                    nano: riv * NANOS_PER_MIN,
                    fsp: 0,
                })
            }
            Hour => {
                if for_duration && riv.abs() > MAX_HOUR_PART as i64 {
                    return Err(Error::datetime_function_overflow());
                }
                Ok(Self {
                    month: 0,
                    nano: riv * NANOS_PER_HOUR,
                    fsp: 0,
                })
            }
            Day => {
                if for_duration && riv.abs() > MAX_HOUR_PART as i64 / 24 {
                    return Err(Error::datetime_function_overflow());
                }
                Ok(Self {
                    month: 0,
                    nano: riv * NANOS_PER_DAY,
                    fsp: 0,
                })
            }
            Week => {
                if for_duration && riv.abs() * 7 > MAX_HOUR_PART as i64 / 24 {
                    return Err(Error::datetime_function_overflow());
                }
                Ok(Self {
                    month: 0,
                    nano: riv * NANOS_PER_DAY * 7,
                    fsp: 0,
                })
            }
            Month => {
                if for_duration && riv.abs() > 1 {
                    return Err(Error::datetime_function_overflow());
                }
                Ok(Self {
                    month: riv,
                    nano: 0,
                    fsp: 0,
                })
            }
            Quarter => {
                if for_duration {
                    return Err(Error::datetime_function_overflow());
                }
                Ok(Self {
                    month: riv * 3,
                    nano: 0,
                    fsp: 0,
                })
            }
            Year => {
                if for_duration {
                    return Err(Error::datetime_function_overflow());
                }
                Ok(Self {
                    month: riv * 12,
                    nano: 0,
                    fsp: 0,
                })
            }
            _ => Err(box_err!("invalid single time unit {:?}", unit)),
        }
    }

    fn parse_time_value(
        ctx: &mut EvalContext,
        input: &str,
        index: TimeIndex,
        max_cnt: usize,
        for_duration: bool,
    ) -> Result<Self> {
        let mut neg = false;
        let original_input = input;

        // Trim spaces and check if negative
        let mut input = input.trim();
        if input.starts_with('-') {
            neg = true;
            input = &input[1..];
        }

        // Initialize fields as "0"
        let mut fields = ["0"; TimeIndex::Max as usize];

        let matches: Vec<&str> = NUMERIC_REGEX.find_iter(input).map(|m| m.as_str()).collect();

        if matches.len() > max_cnt || matches.len() > index as usize + 1 {
            if for_duration {
                return Err(Error::incorrect_datetime_value(original_input));
            }
            ctx.handle_invalid_time_error(Error::incorrect_datetime_value(original_input))?;
            return Ok(Self {
                month: 0,
                nano: 0,
                fsp: DEFAULT_FSP,
            });
        }

        // Populate fields in reverse order
        for (i, &matched) in matches.iter().rev().enumerate() {
            fields[index as usize - i] = &matched;
        }

        // Helper to parse integer fields and handle errors
        let mut parse_field = |field: &str| -> Result<i64> {
            match i64::from_str(field) {
                Ok(val) => Ok(val),
                Err(_) => {
                    if for_duration {
                        return Err(Error::incorrect_datetime_value(original_input));
                    }
                    ctx.handle_invalid_time_error(Error::incorrect_datetime_value(original_input))?;
                    Ok(0)
                }
            }
        };

        // Parse the fields (year, month, day, hour, minute, second, microsecond)
        let years = parse_field(fields[TimeIndex::Year as usize])?;
        let months = parse_field(fields[TimeIndex::Month as usize])?;
        let days = parse_field(fields[TimeIndex::Day as usize])?;
        let hours = parse_field(fields[TimeIndex::Hour as usize])?;
        let minutes = parse_field(fields[TimeIndex::Minute as usize])?;
        let seconds = parse_field(fields[TimeIndex::Second as usize])?;

        let mut frac_part = fields[TimeIndex::Microsecond as usize].to_string();
        let frac_part_len = frac_part.len();
        if frac_part_len < MAX_FSP as usize {
            frac_part.push_str(&"0".repeat(MAX_FSP as usize - frac_part_len));
        }
        let microseconds = parse_field(&frac_part)?;

        // Convert everything into nanoseconds for the Interval struct
        let total_nanos = days * NANOS_PER_DAY
            + hours * NANOS_PER_HOUR
            + minutes * NANOS_PER_MIN
            + seconds * NANOS_PER_SEC
            + microseconds * NANOS_PER_MICRO;

        let month = if neg {
            -(years * 12 + months)
        } else {
            years * 12 + months
        };
        let nano = if neg { -total_nanos } else { total_nanos };

        // Return Interval with month, nano, and fsp values
        Ok(Self {
            month,
            nano,
            fsp: if index == TimeIndex::Microsecond {
                MAX_FSP
            } else {
                MIN_FSP
            },
        })
    }

    pub fn extract_duration(
        ctx: &mut EvalContext,
        unit: &IntervalUnit,
        input: &str,
    ) -> Result<Duration> {
        let val = Self::parse_from_str_internal(ctx, unit, input, true)?;
        use IntervalUnit::*;
        match unit {
            Microsecond | Second | Minute | Hour | Day | Week | Month | Quarter | Year => Ok(
                Duration::from_nanos(val.month * 30 * NANOS_PER_DAY + val.nano, val.fsp)?,
            ),
            _ => {
                if val.month != 0 || val.nano.abs() > MAX_NANOS {
                    return Err(Error::datetime_function_overflow());
                }
                Ok(Duration::from_nanos(val.nano, val.fsp)?)
            }
        }
    }

    pub fn negate(&self) -> Self {
        Self {
            month: -self.month,
            nano: -self.nano,
            fsp: self.fsp,
        }
    }

    pub fn month(&self) -> i64 {
        self.month
    }

    pub fn nano(&self) -> i64 {
        self.nano
    }

    pub fn fsp(&self) -> i8 {
        self.fsp
    }
}

/// Convert to a string which has a uniform interval format and then can be
/// parsed into Interval struct.
pub trait ConvertToIntervalStr {
    fn to_interval_string(
        &self,
        ctx: &mut EvalContext,
        unit: IntervalUnit,
        is_unsigned: bool,
        decimal: isize,
    ) -> Result<String>;
}

impl<'a> ConvertToIntervalStr for BytesRef<'a> {
    #[inline]
    fn to_interval_string(
        &self,
        ctx: &mut EvalContext,
        unit: IntervalUnit,
        _is_unsigned: bool,
        _decimal: isize,
    ) -> Result<String> {
        let mut interval = "0".to_string();
        let input = std::str::from_utf8(self).map_err(Error::Encoding)?;
        use IntervalUnit::*;
        match unit {
            Microsecond | Minute | Hour | Day | Week | Month | Quarter | Year => {
                let trimmed = input.trim();
                if let Some(m) = INTERVAL_REGEX.find(trimmed) {
                    interval = m.as_str().to_string();
                }

                if interval != trimmed {
                    ctx.handle_truncate(true)?;
                }
            }
            Second => {
                // The unit SECOND is specially handled, for example:
                // date + INTERVAL "1e2" SECOND = date + INTERVAL 100 second
                // date + INTERVAL "1.6" SECOND = date + INTERVAL 1.6 second
                // But:
                // date + INTERVAL "1e2" MINUTE = date + INTERVAL 1 MINUTE
                // date + INTERVAL "1.6" MINUTE = date + INTERVAL 1 MINUTE
                let dec = Decimal::from_bytes(self)?.into_result(ctx)?;
                interval = dec.to_string();
            }
            _ => {
                interval = input.to_string();
            }
        }
        Ok(interval)
    }
}

impl ConvertToIntervalStr for i64 {
    #[inline]
    fn to_interval_string(
        &self,
        _ctx: &mut EvalContext,
        _unit: IntervalUnit,
        is_unsigned: bool,
        _decimal: isize,
    ) -> Result<String> {
        if is_unsigned {
            Ok((*self as u64).to_string())
        } else {
            Ok(self.to_string())
        }
    }
}

impl ConvertToIntervalStr for Real {
    #[inline]
    fn to_interval_string(
        &self,
        _ctx: &mut EvalContext,
        _unit: IntervalUnit,
        _is_unsigned: bool,
        decimal: isize,
    ) -> Result<String> {
        if decimal < 0 {
            Ok(self.to_string())
        } else {
            Ok(format!("{:.*}", decimal as usize, self.into_inner()))
        }
    }
}

impl ConvertToIntervalStr for Decimal {
    #[inline]
    fn to_interval_string(
        &self,
        ctx: &mut EvalContext,
        unit: IntervalUnit,
        _is_unsigned: bool,
        _decimal: isize,
    ) -> Result<String> {
        let mut interval = self.to_string();
        use IntervalUnit::*;
        match unit {
            HourMinute | MinuteSecond | YearMonth | DayHour | DayMinute | DaySecond
            | DayMicrosecond | HourMicrosecond | HourSecond | MinuteMicrosecond
            | SecondMicrosecond => {
                let mut neg = false;
                if !interval.is_empty() && interval.starts_with('-') {
                    neg = true;
                    interval = interval[1..].to_string();
                }
                match unit {
                    HourMinute | MinuteSecond => interval = interval.replace('.', ":"),
                    YearMonth => interval = interval.replace('.', "-"),
                    DayHour => interval = interval.replace('.', " "),
                    DayMinute => interval = "0 ".to_string() + &interval.replace('.', ":"),
                    DaySecond => interval = "0 00:".to_string() + &interval.replace('.', ":"),
                    DayMicrosecond => interval = "0 00:00:".to_string() + &interval,
                    HourMicrosecond => interval = "00:00:".to_string() + &interval,
                    HourSecond => interval = "00:".to_string() + &interval.replace('.', ":"),
                    MinuteMicrosecond => interval = "00:".to_string() + &interval,
                    SecondMicrosecond => (),
                    _ => unreachable!(),
                }
                if neg {
                    interval = "-".to_string() + &interval;
                }
            }
            Second => (),
            _ => {
                let rounded = self.round(0, RoundMode::HalfEven).into_result(ctx)?;
                let int_val = rounded.as_i64().into_result(ctx)?;
                interval = int_val.to_string();
            }
        }
        Ok(interval)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::expr::{EvalConfig, Flag};

    #[test]
    fn test_is_clock_unit() -> Result<()> {
        let cases = vec![
            ("MICROSECOND", true),
            ("secOnd", true),
            ("MINUTE", true),
            ("HOUR", true),
            ("daY", false),
            ("WeeK", false),
            ("MONTH", false),
            ("QUARTER", false),
            ("year", false),
            ("SECOND_MIcROSECOnD", true),
            ("MINUTE_MICROSECOND", true),
            ("MINUTE_second", true),
            ("HOUR_MICROSECOND", true),
            ("HOUR_SECOND", true),
            ("HOUR_MINUTE", true),
            ("DAY_MICROSECOND", true),
            ("DAY_SECOND", true),
            ("DAY_MINUTE", true),
            ("DAY_HOUR", true),
            ("year_MONTH", false),
        ];
        for (str, result) in cases {
            let unit = IntervalUnit::from_str(str)?;
            assert_eq!(unit.is_clock_unit(), result);
        }
        Ok(())
    }

    #[test]
    fn test_bytes_ref_to_interval_string() {
        use IntervalUnit::*;
        let cases = vec![
            (b"365" as &[u8], Microsecond, "365"),
            (b"10", Minute, "10"),
            (b"-123", Minute, "-123"),
            (b"24", Hour, "24"),
            (b"  365", Day, "365"),
            (b"abc", Day, "0"),
            (b" -221", Week, "-221"),
            (b"a6", Month, "0"),
            (b"-24a", Quarter, "-24"),
            (b"1024", Year, "1024"),
            (b"1e2", Second, "100"),
            (b"-2e4", Second, "-20000"),
            (b"1.6", Second, "1.6"),
            (b"-1.6554", Second, "-1.6554"),
        ];

        let mut config = EvalConfig::new();
        config.set_flag(Flag::TRUNCATE_AS_WARNING);
        let mut ctx = EvalContext::new(std::sync::Arc::new(config));
        for (input, unit, expected) in cases {
            let result = input.to_interval_string(&mut ctx, unit, false, 0).unwrap();
            assert_eq!(result, expected);
        }

        let mut ctx = EvalContext::default();
        let err_cases = vec![(b"abc" as &[u8], Day), (b"a6", Month), (b"-24a", Quarter)];
        for (input, unit) in err_cases {
            input
                .to_interval_string(&mut ctx, unit, false, 0)
                .unwrap_err();
        }
    }

    #[test]
    fn test_i64_to_interval_string() {
        let cases = vec![
            (42i64, false, "42"),
            (-100i64, false, "-100"),
            (0i64, false, "0"),
            (9999999999i64, false, "9999999999"),
            (-9999999999i64, false, "-9999999999"),
            (9999999999i64, true, "9999999999"),
            (-9999999999i64, true, "18446744063709551617"),
        ];

        let mut ctx = EvalContext::default();
        for (input, is_unsigned, expected) in cases {
            let result = input
                .to_interval_string(&mut ctx, IntervalUnit::Second, is_unsigned, 0)
                .unwrap();
            assert_eq!(result, expected);
        }
    }

    #[test]
    fn test_real_to_interval_string() {
        let mut ctx = EvalContext::default();

        let cases = vec![
            (1.2345, 4, "1.2345"),
            (1.2345, 5, "1.23450"),
            (1.2345, 2, "1.23"),
            (-1.6789, 3, "-1.679"),
            (-1.6789, 6, "-1.678900"),
            (100.779, 0, "101"),
            (-100.779, 0, "-101"),
            (-123.123, -1, "-123.123"),
            (-123.1239123, -1, "-123.1239123"),
        ];

        for (input, decimal, expected) in cases {
            let real = Real::new(input).unwrap();
            let result = real
                .to_interval_string(&mut ctx, IntervalUnit::Second, false, decimal)
                .unwrap();
            assert_eq!(result, expected);
        }
    }

    #[test]
    fn test_decimal_to_interval() {
        use IntervalUnit::*;
        let cases = vec![
            // Basic unit cases
            ("12.34", Year, "12"),
            ("-12.34", Month, "-12"),
            ("12.5", Day, "13"),
            ("12.45", Hour, "12"),
            ("-12.6", Minute, "-13"),
            ("12.34", Second, "12.34"),
            ("-12.34", Second, "-12.34"),
            // Compound unit cases
            ("12.34", HourMinute, "12:34"),
            ("-12.34", MinuteSecond, "-12:34"),
            ("12.34", YearMonth, "12-34"),
            ("-12.34", YearMonth, "-12-34"),
            ("12.34", DayHour, "12 34"),
            ("-12.34", DayHour, "-12 34"),
            ("12.34", DayMinute, "0 12:34"),
            ("-12.3400", DayMinute, "-0 12:3400"),
            ("12.34", DaySecond, "0 00:12:34"),
            ("-12.34", DaySecond, "-0 00:12:34"),
            ("12.34", DayMicrosecond, "0 00:00:12.34"),
            ("-12.34", DayMicrosecond, "-0 00:00:12.34"),
            ("12.34", HourMicrosecond, "00:00:12.34"),
            ("-12.34", HourMicrosecond, "-00:00:12.34"),
            ("12.34", HourSecond, "00:12:34"),
            ("-12.34", HourSecond, "-00:12:34"),
            ("12.34", MinuteMicrosecond, "00:12.34"),
            ("-12.34", MinuteMicrosecond, "-00:12.34"),
            ("12.34", SecondMicrosecond, "12.34"),
            ("-12.34", SecondMicrosecond, "-12.34"),
            // Rounding case
            ("12.99", Year, "13"),
            ("12.49", Year, "12"),
            ("-12.99", Year, "-13"),
        ];

        let mut ctx = EvalContext::default();
        for (input, unit, expected) in cases {
            let decimal = Decimal::from_str(input).unwrap();
            let result = decimal
                .to_interval_string(&mut ctx, unit, false, 0)
                .unwrap();
            assert_eq!(
                result, expected,
                "Failed for input: {}, unit: {:?}",
                input, unit
            );
        }
    }

    #[test]
    fn test_interval_parse_from_str() {
        use IntervalUnit::*;
        let cases = vec![
            (
                "123456",
                Microsecond,
                Interval {
                    month: 0,
                    nano: 123456 * NANOS_PER_MICRO,
                    fsp: 6,
                },
            ),
            (
                "-123456",
                Microsecond,
                Interval {
                    month: 0,
                    nano: -123456 * NANOS_PER_MICRO,
                    fsp: 6,
                },
            ),
            (
                "2.123456",
                Second,
                Interval {
                    month: 0,
                    nano: 2 * NANOS_PER_SEC + 123456 * NANOS_PER_MICRO,
                    fsp: 6,
                },
            ),
            (
                "-2.123456",
                Second,
                Interval {
                    month: 0,
                    nano: -2 * NANOS_PER_SEC - 123456 * NANOS_PER_MICRO,
                    fsp: 6,
                },
            ),
            (
                "2.12345",
                Second,
                Interval {
                    month: 0,
                    nano: 2 * NANOS_PER_SEC + 123450 * NANOS_PER_MICRO,
                    fsp: 5,
                },
            ),
            (
                "-2.12345",
                Second,
                Interval {
                    month: 0,
                    nano: -2 * NANOS_PER_SEC - 123450 * NANOS_PER_MICRO,
                    fsp: 5,
                },
            ),
            (
                "2.1234567",
                Second,
                Interval {
                    month: 0,
                    nano: 2 * NANOS_PER_SEC + 123456 * NANOS_PER_MICRO,
                    fsp: 6,
                },
            ),
            (
                "-2.1234567",
                Second,
                Interval {
                    month: 0,
                    nano: -2 * NANOS_PER_SEC - 123456 * NANOS_PER_MICRO,
                    fsp: 6,
                },
            ),
            (
                "2.99",
                Second,
                Interval {
                    month: 0,
                    nano: 2 * NANOS_PER_SEC + 990000 * NANOS_PER_MICRO,
                    fsp: 2,
                },
            ),
            (
                "-2.50000",
                Second,
                Interval {
                    month: 0,
                    nano: -2 * NANOS_PER_SEC - 500000 * NANOS_PER_MICRO,
                    fsp: 5,
                },
            ),
            (
                "2.500000",
                Minute,
                Interval {
                    month: 0,
                    nano: 3 * NANOS_PER_MIN,
                    fsp: 0,
                },
            ),
            (
                "-2.50000",
                Minute,
                Interval {
                    month: 0,
                    nano: -3 * NANOS_PER_MIN,
                    fsp: 0,
                },
            ),
            (
                "99.9",
                Minute,
                Interval {
                    month: 0,
                    nano: 100 * NANOS_PER_MIN,
                    fsp: 0,
                },
            ),
            (
                "-99.4",
                Minute,
                Interval {
                    month: 0,
                    nano: -99 * NANOS_PER_MIN,
                    fsp: 0,
                },
            ),
            (
                "99.9",
                Hour,
                Interval {
                    month: 0,
                    nano: 100 * NANOS_PER_HOUR,
                    fsp: 0,
                },
            ),
            (
                "-99.4",
                Hour,
                Interval {
                    month: 0,
                    nano: -99 * NANOS_PER_HOUR,
                    fsp: 0,
                },
            ),
            (
                "99.9",
                Day,
                Interval {
                    month: 0,
                    nano: 100 * NANOS_PER_DAY,
                    fsp: 0,
                },
            ),
            (
                "-99.4",
                Day,
                Interval {
                    month: 0,
                    nano: -99 * NANOS_PER_DAY,
                    fsp: 0,
                },
            ),
            (
                "99.9",
                Week,
                Interval {
                    month: 0,
                    nano: 100 * NANOS_PER_DAY * 7,
                    fsp: 0,
                },
            ),
            (
                "-99.4",
                Week,
                Interval {
                    month: 0,
                    nano: -99 * NANOS_PER_DAY * 7,
                    fsp: 0,
                },
            ),
            (
                "99.9",
                Month,
                Interval {
                    month: 100,
                    nano: 0,
                    fsp: 0,
                },
            ),
            (
                "-99.4",
                Month,
                Interval {
                    month: -99,
                    nano: 0,
                    fsp: 0,
                },
            ),
            (
                "99.9",
                Quarter,
                Interval {
                    month: 100 * 3,
                    nano: 0,
                    fsp: 0,
                },
            ),
            (
                "-99.4",
                Quarter,
                Interval {
                    month: -99 * 3,
                    nano: 0,
                    fsp: 0,
                },
            ),
            (
                "99.9",
                Year,
                Interval {
                    month: 100 * 12,
                    nano: 0,
                    fsp: 0,
                },
            ),
            (
                "-99.4",
                Year,
                Interval {
                    month: -99 * 12,
                    nano: 0,
                    fsp: 0,
                },
            ),
            // Compound unit cases
            (
                "123",
                SecondMicrosecond,
                Interval {
                    month: 0,
                    nano: 123000 * NANOS_PER_MICRO,
                    fsp: 6,
                },
            ),
            (
                "-123",
                SecondMicrosecond,
                Interval {
                    month: 0,
                    nano: -123000 * NANOS_PER_MICRO,
                    fsp: 6,
                },
            ),
            (
                "123.123",
                SecondMicrosecond,
                Interval {
                    month: 0,
                    nano: 123 * NANOS_PER_SEC + 123000 * NANOS_PER_MICRO,
                    fsp: 6,
                },
            ),
            (
                "-123.123",
                SecondMicrosecond,
                Interval {
                    month: 0,
                    nano: -123 * NANOS_PER_SEC - 123000 * NANOS_PER_MICRO,
                    fsp: 6,
                },
            ),
            (
                "123",
                MinuteMicrosecond,
                Interval {
                    month: 0,
                    nano: 123000 * NANOS_PER_MICRO,
                    fsp: 6,
                },
            ),
            (
                "-123",
                MinuteMicrosecond,
                Interval {
                    month: 0,
                    nano: -123000 * NANOS_PER_MICRO,
                    fsp: 6,
                },
            ),
            (
                "123.123",
                MinuteMicrosecond,
                Interval {
                    month: 0,
                    nano: 123 * NANOS_PER_SEC + 123000 * NANOS_PER_MICRO,
                    fsp: 6,
                },
            ),
            (
                "-123.123",
                MinuteMicrosecond,
                Interval {
                    month: 0,
                    nano: -123 * NANOS_PER_SEC - 123000 * NANOS_PER_MICRO,
                    fsp: 6,
                },
            ),
            (
                "2:123.123",
                MinuteMicrosecond,
                Interval {
                    month: 0,
                    nano: 2 * NANOS_PER_MIN + 123 * NANOS_PER_SEC + 123000 * NANOS_PER_MICRO,
                    fsp: 6,
                },
            ),
            (
                "-62:123.123",
                MinuteMicrosecond,
                Interval {
                    month: 0,
                    nano: -62 * NANOS_PER_MIN - 123 * NANOS_PER_SEC - 123000 * NANOS_PER_MICRO,
                    fsp: 6,
                },
            ),
            (
                "123",
                MinuteSecond,
                Interval {
                    month: 0,
                    nano: 123 * NANOS_PER_SEC,
                    fsp: 0,
                },
            ),
            (
                "-123",
                MinuteSecond,
                Interval {
                    month: 0,
                    nano: -123 * NANOS_PER_SEC,
                    fsp: 0,
                },
            ),
            (
                "2:123",
                MinuteSecond,
                Interval {
                    month: 0,
                    nano: 2 * NANOS_PER_MIN + 123 * NANOS_PER_SEC,
                    fsp: 0,
                },
            ),
            (
                "-2:123",
                MinuteSecond,
                Interval {
                    month: 0,
                    nano: -2 * NANOS_PER_MIN - 123 * NANOS_PER_SEC,
                    fsp: 0,
                },
            ),
            (
                "123",
                HourMicrosecond,
                Interval {
                    month: 0,
                    nano: 123000 * NANOS_PER_MICRO,
                    fsp: 6,
                },
            ),
            (
                "-123",
                HourMicrosecond,
                Interval {
                    month: 0,
                    nano: -123000 * NANOS_PER_MICRO,
                    fsp: 6,
                },
            ),
            (
                "123.123",
                HourMicrosecond,
                Interval {
                    month: 0,
                    nano: 123 * NANOS_PER_SEC + 123000 * NANOS_PER_MICRO,
                    fsp: 6,
                },
            ),
            (
                "-123.123",
                HourMicrosecond,
                Interval {
                    month: 0,
                    nano: -123 * NANOS_PER_SEC - 123000 * NANOS_PER_MICRO,
                    fsp: 6,
                },
            ),
            (
                "2:123.123",
                HourMicrosecond,
                Interval {
                    month: 0,
                    nano: 2 * NANOS_PER_MIN + 123 * NANOS_PER_SEC + 123000 * NANOS_PER_MICRO,
                    fsp: 6,
                },
            ),
            (
                "-62:123.123",
                HourMicrosecond,
                Interval {
                    month: 0,
                    nano: -62 * NANOS_PER_MIN - 123 * NANOS_PER_SEC - 123000 * NANOS_PER_MICRO,
                    fsp: 6,
                },
            ),
            (
                "12:2:123.123",
                HourMicrosecond,
                Interval {
                    month: 0,
                    nano: 12 * NANOS_PER_HOUR
                        + 2 * NANOS_PER_MIN
                        + 123 * NANOS_PER_SEC
                        + 123000 * NANOS_PER_MICRO,
                    fsp: 6,
                },
            ),
            (
                "-2:62:123.123",
                HourMicrosecond,
                Interval {
                    month: 0,
                    nano: -2 * NANOS_PER_HOUR
                        - 62 * NANOS_PER_MIN
                        - 123 * NANOS_PER_SEC
                        - 123000 * NANOS_PER_MICRO,
                    fsp: 6,
                },
            ),
            (
                "123",
                HourSecond,
                Interval {
                    month: 0,
                    nano: 123 * NANOS_PER_SEC,
                    fsp: 0,
                },
            ),
            (
                "-123",
                HourSecond,
                Interval {
                    month: 0,
                    nano: -123 * NANOS_PER_SEC,
                    fsp: 0,
                },
            ),
            (
                "2:123",
                HourSecond,
                Interval {
                    month: 0,
                    nano: 2 * NANOS_PER_MIN + 123 * NANOS_PER_SEC,
                    fsp: 0,
                },
            ),
            (
                "-2:123",
                HourSecond,
                Interval {
                    month: 0,
                    nano: -2 * NANOS_PER_MIN - 123 * NANOS_PER_SEC,
                    fsp: 0,
                },
            ),
            (
                "9:62:123",
                HourSecond,
                Interval {
                    month: 0,
                    nano: 9 * NANOS_PER_HOUR + 62 * NANOS_PER_MIN + 123 * NANOS_PER_SEC,
                    fsp: 0,
                },
            ),
            (
                "-55:62:123",
                HourSecond,
                Interval {
                    month: 0,
                    nano: -55 * NANOS_PER_HOUR - 62 * NANOS_PER_MIN - 123 * NANOS_PER_SEC,
                    fsp: 0,
                },
            ),
            (
                "123",
                HourMinute,
                Interval {
                    month: 0,
                    nano: 123 * NANOS_PER_MIN,
                    fsp: 0,
                },
            ),
            (
                "-123",
                HourMinute,
                Interval {
                    month: 0,
                    nano: -123 * NANOS_PER_MIN,
                    fsp: 0,
                },
            ),
            (
                "2:123",
                HourMinute,
                Interval {
                    month: 0,
                    nano: 2 * NANOS_PER_HOUR + 123 * NANOS_PER_MIN,
                    fsp: 0,
                },
            ),
            (
                "-88:123",
                HourMinute,
                Interval {
                    month: 0,
                    nano: -88 * NANOS_PER_HOUR - 123 * NANOS_PER_MIN,
                    fsp: 0,
                },
            ),
            (
                "123",
                DayMicrosecond,
                Interval {
                    month: 0,
                    nano: 123000 * NANOS_PER_MICRO,
                    fsp: 6,
                },
            ),
            (
                "-123",
                DayMicrosecond,
                Interval {
                    month: 0,
                    nano: -123000 * NANOS_PER_MICRO,
                    fsp: 6,
                },
            ),
            (
                "123.123",
                DayMicrosecond,
                Interval {
                    month: 0,
                    nano: 123 * NANOS_PER_SEC + 123000 * NANOS_PER_MICRO,
                    fsp: 6,
                },
            ),
            (
                "-123.123",
                DayMicrosecond,
                Interval {
                    month: 0,
                    nano: -123 * NANOS_PER_SEC - 123000 * NANOS_PER_MICRO,
                    fsp: 6,
                },
            ),
            (
                "2:123.123",
                DayMicrosecond,
                Interval {
                    month: 0,
                    nano: 2 * NANOS_PER_MIN + 123 * NANOS_PER_SEC + 123000 * NANOS_PER_MICRO,
                    fsp: 6,
                },
            ),
            (
                "-62:123.123",
                DayMicrosecond,
                Interval {
                    month: 0,
                    nano: -62 * NANOS_PER_MIN - 123 * NANOS_PER_SEC - 123000 * NANOS_PER_MICRO,
                    fsp: 6,
                },
            ),
            (
                "12:2:123.123",
                DayMicrosecond,
                Interval {
                    month: 0,
                    nano: 12 * NANOS_PER_HOUR
                        + 2 * NANOS_PER_MIN
                        + 123 * NANOS_PER_SEC
                        + 123000 * NANOS_PER_MICRO,
                    fsp: 6,
                },
            ),
            (
                "-2:62:123.123",
                DayMicrosecond,
                Interval {
                    month: 0,
                    nano: -2 * NANOS_PER_HOUR
                        - 62 * NANOS_PER_MIN
                        - 123 * NANOS_PER_SEC
                        - 123000 * NANOS_PER_MICRO,
                    fsp: 6,
                },
            ),
            (
                "9 12:2:123.123",
                DayMicrosecond,
                Interval {
                    month: 0,
                    nano: 9 * NANOS_PER_DAY
                        + 12 * NANOS_PER_HOUR
                        + 2 * NANOS_PER_MIN
                        + 123 * NANOS_PER_SEC
                        + 123000 * NANOS_PER_MICRO,
                    fsp: 6,
                },
            ),
            (
                "-77 2:62:123.123",
                DayMicrosecond,
                Interval {
                    month: 0,
                    nano: -77 * NANOS_PER_DAY
                        - 2 * NANOS_PER_HOUR
                        - 62 * NANOS_PER_MIN
                        - 123 * NANOS_PER_SEC
                        - 123000 * NANOS_PER_MICRO,
                    fsp: 6,
                },
            ),
            (
                "123",
                DaySecond,
                Interval {
                    month: 0,
                    nano: 123 * NANOS_PER_SEC,
                    fsp: 0,
                },
            ),
            (
                "-123",
                DaySecond,
                Interval {
                    month: 0,
                    nano: -123 * NANOS_PER_SEC,
                    fsp: 0,
                },
            ),
            (
                "2:123",
                DaySecond,
                Interval {
                    month: 0,
                    nano: 2 * NANOS_PER_MIN + 123 * NANOS_PER_SEC,
                    fsp: 0,
                },
            ),
            (
                "-2:123",
                DaySecond,
                Interval {
                    month: 0,
                    nano: -2 * NANOS_PER_MIN - 123 * NANOS_PER_SEC,
                    fsp: 0,
                },
            ),
            (
                "9:62:123",
                DaySecond,
                Interval {
                    month: 0,
                    nano: 9 * NANOS_PER_HOUR + 62 * NANOS_PER_MIN + 123 * NANOS_PER_SEC,
                    fsp: 0,
                },
            ),
            (
                "-55:62:123",
                DaySecond,
                Interval {
                    month: 0,
                    nano: -55 * NANOS_PER_HOUR - 62 * NANOS_PER_MIN - 123 * NANOS_PER_SEC,
                    fsp: 0,
                },
            ),
            (
                "1 9:62:123",
                DaySecond,
                Interval {
                    month: 0,
                    nano: NANOS_PER_DAY
                        + 9 * NANOS_PER_HOUR
                        + 62 * NANOS_PER_MIN
                        + 123 * NANOS_PER_SEC,
                    fsp: 0,
                },
            ),
            (
                "-3 55:62:123",
                DaySecond,
                Interval {
                    month: 0,
                    nano: -3 * NANOS_PER_DAY
                        - 55 * NANOS_PER_HOUR
                        - 62 * NANOS_PER_MIN
                        - 123 * NANOS_PER_SEC,
                    fsp: 0,
                },
            ),
            (
                "123",
                DayMinute,
                Interval {
                    month: 0,
                    nano: 123 * NANOS_PER_MIN,
                    fsp: 0,
                },
            ),
            (
                "-123",
                DayMinute,
                Interval {
                    month: 0,
                    nano: -123 * NANOS_PER_MIN,
                    fsp: 0,
                },
            ),
            (
                "2:123",
                DayMinute,
                Interval {
                    month: 0,
                    nano: 2 * NANOS_PER_HOUR + 123 * NANOS_PER_MIN,
                    fsp: 0,
                },
            ),
            (
                "-88:123",
                DayMinute,
                Interval {
                    month: 0,
                    nano: -88 * NANOS_PER_HOUR - 123 * NANOS_PER_MIN,
                    fsp: 0,
                },
            ),
            (
                "08 2:123",
                DayMinute,
                Interval {
                    month: 0,
                    nano: 8 * NANOS_PER_DAY + 2 * NANOS_PER_HOUR + 123 * NANOS_PER_MIN,
                    fsp: 0,
                },
            ),
            (
                "-70 88:123",
                DayMinute,
                Interval {
                    month: 0,
                    nano: -70 * NANOS_PER_DAY - 88 * NANOS_PER_HOUR - 123 * NANOS_PER_MIN,
                    fsp: 0,
                },
            ),
            (
                "123",
                DayHour,
                Interval {
                    month: 0,
                    nano: 123 * NANOS_PER_HOUR,
                    fsp: 0,
                },
            ),
            (
                "-123",
                DayHour,
                Interval {
                    month: 0,
                    nano: -123 * NANOS_PER_HOUR,
                    fsp: 0,
                },
            ),
            (
                "66 123",
                DayHour,
                Interval {
                    month: 0,
                    nano: 66 * NANOS_PER_DAY + 123 * NANOS_PER_HOUR,
                    fsp: 0,
                },
            ),
            (
                "-77 123",
                DayHour,
                Interval {
                    month: 0,
                    nano: -77 * NANOS_PER_DAY - 123 * NANOS_PER_HOUR,
                    fsp: 0,
                },
            ),
            (
                "123",
                YearMonth,
                Interval {
                    month: 123,
                    nano: 0,
                    fsp: 0,
                },
            ),
            (
                "-123",
                YearMonth,
                Interval {
                    month: -123,
                    nano: 0,
                    fsp: 0,
                },
            ),
            (
                "99 123",
                YearMonth,
                Interval {
                    month: 99 * 12 + 123,
                    nano: 0,
                    fsp: 0,
                },
            ),
            (
                "-7 123",
                YearMonth,
                Interval {
                    month: -7 * 12 - 123,
                    nano: 0,
                    fsp: 0,
                },
            ),
        ];
        let mut ctx = EvalContext::default();
        for (input, unit, expected) in cases {
            let result = Interval::parse_from_str(&mut ctx, &unit, input).unwrap();
            assert_eq!(
                result, expected,
                "Failed for input: {}, unit: {:?}",
                input, unit
            );
        }

        let err_cases = vec![
            ("12:12.123", SecondMicrosecond),
            ("20:12:12.123", MinuteMicrosecond),
            ("12:12:12", MinuteSecond),
            ("1 12:12:12.11", HourMicrosecond),
            ("12:12:12.123", HourSecond),
            ("12:12:12", HourMinute),
            ("3 2 12:12:12.123", DayMicrosecond),
            ("3 12:12:12.123", DaySecond),
            ("3 12:12:12", DayMinute),
            ("3 12:12", DayHour),
            ("99 123:123", YearMonth),
        ];
        for (input, unit) in err_cases {
            let result = Interval::parse_from_str(&mut ctx, &unit, input).unwrap();
            assert_eq!(
                result,
                Interval {
                    month: 0,
                    nano: 0,
                    fsp: DEFAULT_FSP
                },
                "Failed for input: {}, unit: {:?}",
                input,
                unit
            );
        }
    }

    #[test]
    fn test_interval_extract_duration() {
        use IntervalUnit::*;
        let cases = vec![
            (
                "123456",
                Microsecond,
                Duration::from_nanos(123456 * NANOS_PER_MICRO, 6),
            ),
            (
                "-123456",
                Microsecond,
                Duration::from_nanos(-123456 * NANOS_PER_MICRO, 6),
            ),
            (
                "2.123456",
                Second,
                Duration::from_nanos(2 * NANOS_PER_SEC + 123456 * NANOS_PER_MICRO, 6),
            ),
            (
                "-2.123456",
                Second,
                Duration::from_nanos(-2 * NANOS_PER_SEC - 123456 * NANOS_PER_MICRO, 6),
            ),
            (
                "2.12345",
                Second,
                Duration::from_nanos(2 * NANOS_PER_SEC + 123450 * NANOS_PER_MICRO, 5),
            ),
            (
                "-2.12345",
                Second,
                Duration::from_nanos(-2 * NANOS_PER_SEC - 123450 * NANOS_PER_MICRO, 5),
            ),
            (
                "2.1234567",
                Second,
                Duration::from_nanos(2 * NANOS_PER_SEC + 123456 * NANOS_PER_MICRO, 6),
            ),
            (
                "-2.1234567",
                Second,
                Duration::from_nanos(-2 * NANOS_PER_SEC - 123456 * NANOS_PER_MICRO, 6),
            ),
            (
                "2.99",
                Second,
                Duration::from_nanos(2 * NANOS_PER_SEC + 990000 * NANOS_PER_MICRO, 2),
            ),
            (
                "-2.50000",
                Second,
                Duration::from_nanos(-2 * NANOS_PER_SEC - 500000 * NANOS_PER_MICRO, 5),
            ),
            ("99", Minute, Duration::from_nanos(99 * NANOS_PER_MIN, 0)),
            ("-99", Minute, Duration::from_nanos(-99 * NANOS_PER_MIN, 0)),
            ("30", Day, Duration::from_nanos(30 * NANOS_PER_DAY, 0)),
            ("-30", Day, Duration::from_nanos(-30 * NANOS_PER_DAY, 0)),
            ("2", Week, Duration::from_nanos(2 * NANOS_PER_DAY * 7, 0)),
            ("-2", Week, Duration::from_nanos(-2 * NANOS_PER_DAY * 7, 0)),
            ("1", Month, Duration::from_nanos(30 * NANOS_PER_DAY, 0)),
            ("-1", Month, Duration::from_nanos(-30 * NANOS_PER_DAY, 0)),
            (
                "29 12:23:36.1234",
                DayMicrosecond,
                Duration::from_nanos(
                    29 * NANOS_PER_DAY
                        + 12 * NANOS_PER_HOUR
                        + 23 * NANOS_PER_MIN
                        + 36 * NANOS_PER_SEC
                        + 123400 * NANOS_PER_MICRO,
                    6,
                ),
            ),
            (
                "-29 12:23:36.1234",
                DayMicrosecond,
                Duration::from_nanos(
                    -29 * NANOS_PER_DAY
                        - 12 * NANOS_PER_HOUR
                        - 23 * NANOS_PER_MIN
                        - 36 * NANOS_PER_SEC
                        - 123400 * NANOS_PER_MICRO,
                    6,
                ),
            ),
        ];
        let mut ctx = EvalContext::default();
        for (input, unit, expected) in cases {
            let result = Interval::extract_duration(&mut ctx, &unit, input).unwrap();
            assert_eq!(
                result,
                expected.unwrap(),
                "Failed for input: {}, unit: {:?}",
                input,
                unit
            );
        }
        let err_cases = vec![
            ("2.500000", Minute),
            ("-2.50000", Minute),
            ("99.9", Hour),
            ("-99.4", Hour),
            ("35", Day),
            ("-35", Day),
            ("2", Month),
            ("-2", Month),
            ("99", Quarter),
            ("-99", Quarter),
            ("99", Year),
            ("-99", Year),
            ("-34 23:59:59.1234", DayMicrosecond),
        ];
        for (input, unit) in err_cases {
            let result = Interval::extract_duration(&mut ctx, &unit, input);
            result.unwrap_err();
        }
    }
}
