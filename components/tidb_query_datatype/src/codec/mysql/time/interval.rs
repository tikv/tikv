// Copyright 2024 TiKV Project Authors. Licensed under Apache-2.0.

use std::str::FromStr;

use lazy_static::lazy_static;
use regex::Regex;

use crate::{
    codec::{
        mysql::{duration::*, MAX_FSP, MIN_FSP},
        Error, Result,
    },
    expr::EvalContext,
};

/// Index of 'YEARS-MONTHS DAYS HOURS:MINUTES:SECONDS.MICROSECONDS' expr Format
const YEAR_INDEX: usize = 0;
const MONTH_INDEX: usize = YEAR_INDEX + 1;
const DAY_INDEX: usize = MONTH_INDEX + 1;
const HOUR_INDEX: usize = DAY_INDEX + 1;
const MINUTE_INDEX: usize = HOUR_INDEX + 1;
const SECOND_INDEX: usize = MINUTE_INDEX + 1;
const MICROSECOND_INDEX: usize = SECOND_INDEX + 1;

/// Max parameters count 'YEARS-MONTHS' expr Format allowed
const YEAR_MONTH_MAX_CNT: usize = 2;
/// Max parameters count 'DAYS HOURS' expr Format allowed
const DAY_HOUR_MAX_CNT: usize = 2;
/// Max parameters count 'DAYS HOURS:MINUTES' expr Format allowed
const DAY_MINUTE_MAX_CNT: usize = 3;
/// Max parameters count 'DAYS HOURS:MINUTES:SECONDS' expr Format allowed
const DAY_SECOND_MAX_CNT: usize = 4;
/// Max parameters count 'DAYS HOURS:MINUTES:SECONDS.MICROSECONDS' expr Format
/// allowed
const DAY_MICROSECOND_MAX_CNT: usize = 5;
/// Max parameters count 'HOURS:MINUTES' expr Format allowed
const HOUR_MINUTE_MAX_CNT: usize = 2;
/// Max parameters count 'HOURS:MINUTES:SECONDS' expr Format allowed
const HOUR_SECOND_MAX_CNT: usize = 3;
/// Max parameters count 'HOURS:MINUTES:SECONDS.MICROSECONDS' expr Format
/// allowed
const HOUR_MICROSECOND_MAX_CNT: usize = 4;
/// Max parameters count 'MINUTES:SECONDS' expr Format allowed
const MINUTE_SECOND_MAX_CNT: usize = 2;
/// Max parameters count 'MINUTES:SECONDS.MICROSECONDS' expr Format allowed
const MINUTE_MICROSECOND_MAX_CNT: usize = 3;
/// Max parameters count 'SECONDS.MICROSECONDS' expr Format allowed
const SECOND_MICROSECOND_MAX_CNT: usize = 2;
/// Parameters count 'YEARS-MONTHS DAYS HOURS:MINUTES:SECONDS.MICROSECONDS' expr
/// Format
const TIME_VALUE_CNT: usize = 7;

lazy_static! {
    static ref ONE_TO_SIX_DIGIT_REGEX: Regex = Regex::new(r"^[0-9]{0,6}").unwrap();
    static ref NUMERIC_REGEX: Regex = Regex::new(r"[0-9]+").unwrap();
}

#[derive(Debug, Clone, Copy, PartialEq)]
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
        match self {
            Microsecond | Second | Minute | Hour | SecondMicrosecond | MinuteMicrosecond
            | HourMicrosecond | DayMicrosecond | MinuteSecond | HourSecond | DaySecond
            | HourMinute | DayMinute | DayHour => true,
            _ => false,
        }
    }
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

    fn parse_from_str_internal(
        ctx: &mut EvalContext,
        unit: &IntervalUnit,
        input: &str,
        for_duration: bool,
    ) -> Result<Self> {
        use IntervalUnit::*;
        match unit {
            Microsecond | Second | Minute | Hour | Day | Week | Month | Quarter | Year => {
                Self::parse_single_time_value(ctx, unit, input, for_duration)
            }
            SecondMicrosecond => Self::parse_time_value(
                ctx,
                input,
                MICROSECOND_INDEX,
                SECOND_MICROSECOND_MAX_CNT,
                for_duration,
            ),
            MinuteMicrosecond => Self::parse_time_value(
                ctx,
                input,
                MICROSECOND_INDEX,
                MINUTE_MICROSECOND_MAX_CNT,
                for_duration,
            ),
            MinuteSecond => Self::parse_time_value(
                ctx,
                input,
                SECOND_INDEX,
                MINUTE_SECOND_MAX_CNT,
                for_duration,
            ),
            HourMicrosecond => Self::parse_time_value(
                ctx,
                input,
                MICROSECOND_INDEX,
                HOUR_MICROSECOND_MAX_CNT,
                for_duration,
            ),
            HourSecond => {
                Self::parse_time_value(ctx, input, SECOND_INDEX, HOUR_SECOND_MAX_CNT, for_duration)
            }
            HourMinute => {
                Self::parse_time_value(ctx, input, MINUTE_INDEX, HOUR_MINUTE_MAX_CNT, for_duration)
            }
            DayMicrosecond => Self::parse_time_value(
                ctx,
                input,
                MICROSECOND_INDEX,
                DAY_MICROSECOND_MAX_CNT,
                for_duration,
            ),
            DaySecond => {
                Self::parse_time_value(ctx, input, SECOND_INDEX, DAY_SECOND_MAX_CNT, for_duration)
            }
            DayMinute => {
                Self::parse_time_value(ctx, input, MINUTE_INDEX, DAY_MINUTE_MAX_CNT, for_duration)
            }
            DayHour => {
                Self::parse_time_value(ctx, input, HOUR_INDEX, DAY_HOUR_MAX_CNT, for_duration)
            }
            YearMonth => {
                Self::parse_time_value(ctx, input, MONTH_INDEX, YEAR_MONTH_MAX_CNT, for_duration)
            }
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
        let decimal_point_pos = input.find('.').unwrap_or_else(|| input.len());

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
        index: usize,
        count: usize,
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
        let mut fields = vec!["0"; TIME_VALUE_CNT];

        let matches: Vec<&str> = NUMERIC_REGEX.find_iter(input).map(|m| m.as_str()).collect();

        if matches.len() > count {
            if for_duration {
                return Err(Error::incorrect_datetime_value(original_input));
            }
            ctx.handle_invalid_time_error(Error::incorrect_datetime_value(original_input))?;
            return Ok(Self {
                month: 0,
                nano: 0,
                fsp: 0,
            });
        }

        // Populate fields in reverse order
        for (i, &matched) in matches.iter().enumerate() {
            fields[index - i] = &matched;
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
        let years = parse_field(fields[YEAR_INDEX])?;
        let months = parse_field(fields[MONTH_INDEX])?;
        let days = parse_field(fields[DAY_INDEX])?;
        let hours = parse_field(fields[HOUR_INDEX])?;
        let minutes = parse_field(fields[MINUTE_INDEX])?;
        let seconds = parse_field(fields[SECOND_INDEX])?;

        let mut frac_part = fields[MICROSECOND_INDEX].to_string();
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
            fsp: if index == MICROSECOND_INDEX {
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
