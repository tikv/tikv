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

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
/// See https://dev.mysql.com/doc/refman/8.0/en/expressions.html#temporal-intervals
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
            _ => Err(box_err!("unknown unit str")),
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

#[derive(Debug)]
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

            dv = match i64::from_str(&dv_pre[..6]) {
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

        if matches.len() > max_cnt {
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
        Ok(format!("{:.*}", decimal as usize, self.into_inner()))
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
    fn test_bytes_to_interval_string() -> Result<()> {
        //let mut ctx = EvalContext::default();
        Ok(())
    }
}
