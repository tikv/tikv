use std::convert::TryFrom;

use crate::codec::{self, mysql};
use crate::codec::{Error, Result};
use crate::expr::{EvalContext, SqlMode};

use tidb_query_datatype::FieldTypeTp;

use bitfield::bitfield;
use chrono::prelude::*;

const MIN_TIMESTAMP: i64 = 0;
const MAX_TIMESTAMP: i64 = (1 << 31) - 1;

fn str_to_u32(input: &[u8]) -> Result<u32> {
    unsafe {
        std::str::from_utf8_unchecked(input)
            .parse::<u32>()
            .map_err(|_| Error::truncated())
    }
}

bitfield! {
    #[derive(Clone, Copy, Default)]
    pub struct Time(u64);

    u32;
    #[inline]
    get_year, set_year: 63, 50;
    #[inline]
    get_month, set_month: 49, 46;
    #[inline]
    get_day, set_day: 45, 41;
    #[inline]
    get_hour, set_hour: 40, 36;
    #[inline]
    get_minute, set_minute: 35, 30;
    #[inline]
    get_second,set_second: 29, 24;
    #[inline]
    get_micro, set_micro: 23, 4;

    // `fsp_tt` format:
    // | fsp: 3 bits | type: 1 bit |
    // When `fsp` is valid (in range [0, 6]):
    // 1. `type` bit 0 represent `DateTime`
    // 2. `type` bit 1 represent `TimeStamp`
    //
    // Since `Date` does not require `fsp`, we could use `fsp` == 0b111 to represent it.
    #[inline]
    u8, get_fsp_tt, set_fsp_tt: 3, 0;
}

#[derive(Debug)]
struct DateTimeMode {
    strict_mode: bool,
    no_zero_in_date: bool,
    no_zero_date: bool,
    allow_invalid_date: bool,
}

impl DateTimeMode {
    pub fn from_ctx(ctx: &EvalContext) -> DateTimeMode {
        let sql_mode = ctx.cfg.sql_mode;
        DateTimeMode {
            strict_mode: sql_mode.contains(SqlMode::STRICT_ALL_TABLES),
            no_zero_in_date: sql_mode.contains(SqlMode::NO_ZERO_IN_DATE),
            no_zero_date: sql_mode.contains(SqlMode::NO_ZERO_DATE),
            allow_invalid_date: sql_mode.contains(SqlMode::INVALID_DATES),
        }
    }
}

#[derive(PartialEq, Clone, Copy, Debug)]
pub enum TimeType {
    Date,
    DateTime,
    TimeStamp,
}

impl TryFrom<FieldTypeTp> for TimeType {
    type Error = codec::Error;
    fn try_from(time_type: FieldTypeTp) -> Result<TimeType> {
        Ok(match time_type {
            FieldTypeTp::Date => TimeType::Date,
            FieldTypeTp::DateTime => TimeType::DateTime,
            FieldTypeTp::Timestamp => TimeType::TimeStamp,
            _ => return Err(box_err!("Time does not support field type {}", time_type)),
        })
    }
}

fn is_leap_year(year: u32) -> bool {
    year & 3 == 0 && (year % 100 != 0 || year % 400 == 0)
}

fn last_day_of_month(year: u32, month: u32) -> u32 {
    match month {
        4 | 6 | 9 | 11 => 30,
        2 => is_leap_year(year) as u32 + 28,
        _ => 31,
    }
}

// The common set of methods for `date/time`
impl Time {
    /// Returns the hour number from 0 to 23.
    #[inline]
    pub fn hour(self) -> u32 {
        self.get_hour()
    }

    /// Returns the minute number from 0 to 59.
    #[inline]
    pub fn minute(self) -> u32 {
        self.get_minute()
    }

    /// Returns the second number from 0 to 59.
    #[inline]
    pub fn second(self) -> u32 {
        self.get_second()
    }

    /// Returns the number of microseconds since the whole second.
    pub fn microseconds(self) -> u32 {
        self.get_micro()
    }

    /// Returns the year number
    pub fn year(self) -> u32 {
        self.get_year()
    }

    /// Returns the month number
    pub fn month(self) -> u32 {
        self.get_month()
    }

    /// Returns the day number
    pub fn day(self) -> u32 {
        self.get_day()
    }
}

// Parser
impl Time {
    // Split `input` with `.`, returns the whole part and a optional fractional part.
    fn split_frac(input: &[u8]) -> Result<(&[u8], Option<&[u8]>)> {
        let parts = input.split(|&x| x == b'.').collect::<Vec<_>>();

        if parts.len() > 2 {
            Err(Error::truncated())
        } else {
            Ok((parts[0], parts.get(1).copied()))
        }
    }

    // Split the datetime with whitespace or 'T'.
    fn split_datetime(input: &[u8]) -> Vec<&[u8]> {
        input
            .split(|&x| x.is_ascii_whitespace() || x == b'T')
            .collect::<Vec<_>>()
    }

    fn round(parts: &mut [u32]) -> Result<()> {
        if parts.len() != 7 {
            return Err(Error::truncated());
        }
        let modulus = [
            0,
            12,
            last_day_of_month(parts[0], parts[1]),
            24,
            60,
            60,
            1_000_000,
        ];

        for i in (1..=6).rev() {
            if parts[i] >= modulus[i] {
                parts[i] -= modulus[i];
                parts[i - 1] += 1;
            }
        }
        Ok(())
    }

    fn parse_frac(input: &[u8], fsp: u8, round: bool) -> Result<(bool, u32)> {
        if input.is_empty() {
            return Ok((false, 0));
        }

        let len = input.len() as u32;
        let fsp = u32::from(fsp);

        let (input, len) = if fsp >= len {
            (input, len)
        } else {
            (&input[..=fsp as usize], fsp + 1)
        };

        let frac = str_to_u32(input)? * 10u32.pow(6u32.checked_sub(len).unwrap_or(0));
        Ok(if round {
            let frac = if frac < 1_000_000 { frac * 10 } else { frac };
            let mask = 10u32.pow((6 - fsp) as u32);
            let frac = (frac / mask + 5) / 10 * mask;
            (frac >= 1_000_000, frac)
        } else {
            // Truncate the result if `round` is not enabled
            let frac = if frac >= 1_000_000 { frac / 10 } else { frac };
            (false, frac)
        })
    }

    fn adjust_year(year: u32) -> u32 {
        if year <= 69 {
            2000 + year
        } else if year >= 70 && year <= 99 {
            1900 + year
        } else {
            year
        }
    }

    fn parse_float(
        ctx: &mut EvalContext,
        input: &[u8],
        fsp: u8,
        time_type: TimeType,
        round: bool,
    ) -> Result<Time> {
        let (whole, frac) = Self::split_frac(input)?;
        let mut parts = [0u32; 7];

        let year_digits = match whole.len() {
            14 | 8 => 4,
            9..=12 | 5..=7 => 2,
            _ => Err(Error::truncated())?,
        };

        parts[0] = str_to_u32(&whole[..year_digits])?;
        if year_digits == 2 {
            parts[0] = Time::adjust_year(parts[0]);
        }
        for (i, chunk) in whole[year_digits..].chunks(2).enumerate() {
            parts[i + 1] = str_to_u32(chunk)?;
        }

        // If we miss the `second`, the fractional part is meaningless.
        if frac.is_some() && whole.len() != 12 && whole.len() != 14 {
            return Err(Error::truncated());
        }

        let (carry, frac) = Self::parse_frac(frac.unwrap_or(&[]), fsp, round)?;
        parts[6] = frac;
        if carry {
            Time::round(&mut parts)?;
        }

        Self::from_slice(ctx, &parts, fsp, time_type)
    }

    fn parse_date(input: &[u8]) -> Result<[u32; 3]> {
        let date = input
            .split(|x| x.is_ascii_punctuation())
            .collect::<Vec<_>>();
        if date.len() != 3 {
            Err(Error::truncated())
        } else {
            let mut parts = [0u32; 3];
            for (i, value) in date.iter().enumerate() {
                parts[i] = str_to_u32(value)?;
            }
            if date[0].len() == 2 {
                parts[0] = Time::adjust_year(parts[0]);
            }
            Ok(parts)
        }
    }

    pub fn parse_time(input: &[u8], fsp: u8, round: bool) -> Result<(bool, [u32; 4])> {
        let (whole, frac) = Self::split_frac(input)?;

        let whole = whole
            .split(|x| x.is_ascii_punctuation())
            .collect::<Vec<_>>();

        let mut parts = [0u32; 4];

        for (i, value) in whole.into_iter().enumerate() {
            parts[i] = str_to_u32(value)?;
        }

        let (carry, frac) = Self::parse_frac(frac.unwrap_or(&[]), fsp, round)?;
        parts[3] = frac;
        Ok((carry, parts))
    }

    pub fn parse_datetime(
        ctx: &mut EvalContext,
        input: &str,
        fsp: i8,
        time_type: TimeType,
        round: bool,
    ) -> Result<Time> {
        let input = input.trim().as_bytes();
        let fsp = mysql::check_fsp(fsp)?;

        if input.is_empty() {
            return Err(Error::truncated());
        }

        let parts = Self::split_datetime(input);

        match parts.len() {
            // Float fromat
            1 => Self::parse_float(ctx, input, fsp, time_type, round),

            // Date format
            2 => {
                let ymd = Self::parse_date(parts[0])?;
                let (carry, hms) = Self::parse_time(parts[1], fsp, round)?;
                let mut parts = ymd
                    .into_iter()
                    .copied()
                    .chain(hms.into_iter().copied())
                    .collect::<Vec<_>>();

                if carry {
                    Time::round(&mut parts)?;
                }
                Self::from_slice(ctx, &parts, fsp, time_type)
            }
            _ => Err(Error::truncated()),
        }
    }
}

macro_rules! handle_zero_date {
    ($datetime:expr, $ctx:expr) => {
        let ctx: &mut EvalContext = $ctx;
        let DateTimeMode {
            strict_mode,
            no_zero_date,
            ..
        } = DateTimeMode::from_ctx(ctx);
        let datetime: TimeValidator = $datetime;
        assert!(datetime.is_zero());
        if no_zero_date {
            if strict_mode {
                return Err(Error::truncated());
            } else {
                ctx.warnings.append_warning(Error::truncated());
                return Ok(datetime);
            }
        }
    };
}

macro_rules! handle_zero_in_date {
    ($datetime:expr, $ctx:expr) => {
        let ctx: &mut EvalContext = $ctx;
        let DateTimeMode {
            strict_mode,
            no_zero_in_date,
            ..
        } = DateTimeMode::from_ctx(ctx);
        let datetime: TimeValidator = $datetime;
        assert!(datetime.month == 0 || datetime.year == 0);
        if no_zero_in_date {
            // If we are in NO_ZERO_IN_DATE + STRICT_ALL_TABLES, zero-in-date produces and error.
            // Otherwise, we reset the datetime value and check if we enabled NO_ZERO_DATE.
            if strict_mode {
                return Err(Error::truncated());
            } else {
                ctx.warnings.append_warning(Error::truncated());
                let cleared = datetime.clear();
                handle_zero_date!(cleared, ctx);
                return Ok(cleared);
            }
        }
    };
}

macro_rules! handle_invalid_date {
    ($datetime:expr, $ctx:expr) => {
        let ctx: &mut EvalContext = $ctx;
        let DateTimeMode {
            allow_invalid_date, ..
        } = DateTimeMode::from_ctx(ctx);
        let datetime: TimeValidator = $datetime;
        if allow_invalid_date {
            let cleared = datetime.clear();
            handle_zero_date!(cleared, ctx);
            return Ok(cleared);
        } else {
            return Err(Error::truncated());
        }
    };
}

#[derive(Copy, Clone, Debug)]
struct TimeValidator {
    year: u32,
    month: u32,
    day: u32,
    hour: u32,
    minute: u32,
    second: u32,
    micro: u32,
    fsp: u8,
    time_type: TimeType,
}

impl TimeValidator {
    pub fn new(
        year: u32,
        month: u32,
        day: u32,
        hour: u32,
        minute: u32,
        second: u32,
        micro: u32,
        fsp: u8,
        time_type: TimeType,
    ) -> Self {
        TimeValidator {
            year,
            month,
            day,
            hour,
            minute,
            second,
            micro,
            fsp,
            time_type,
        }
    }

    pub fn check(self, ctx: &mut EvalContext) -> Result<TimeValidator> {
        match self.time_type {
            TimeType::Date => self.check_date(ctx),
            TimeType::DateTime => self.check_datetime(ctx),
            TimeType::TimeStamp => self.check_timestamp(ctx),
        }
    }

    pub fn clear(self) -> TimeValidator {
        TimeValidator {
            year: 0,
            month: 0,
            day: 0,
            hour: 0,
            minute: 0,
            second: 0,
            micro: 0,
            ..self
        }
    }

    pub fn is_zero(self) -> bool {
        self.year == 0
            && self.month == 0
            && self.day == 0
            && self.hour == 0
            && self.minute == 0
            && self.second == 0
            && self.micro == 0
    }

    fn check_date(self, ctx: &mut EvalContext) -> Result<Self> {
        let Self {
            year, month, day, ..
        } = self;
        let DateTimeMode {
            allow_invalid_date: is_relaxed,
            ..
        } = DateTimeMode::from_ctx(ctx);

        if self.is_zero() {
            handle_zero_date!(self, ctx);
        }

        if month == 0 || day == 0 {
            handle_zero_in_date!(self, ctx);
        }

        if year > 9999 {
            handle_invalid_date!(self, ctx);
        }

        let result = Time::check_month_and_day(year, month, day, is_relaxed);

        if result.is_err() {
            handle_invalid_date!(self, ctx);
        }

        Ok(self)
    }

    fn check_datetime(self, ctx: &mut EvalContext) -> Result<Self> {
        let datetime = self.check_date(ctx)?;

        let Self {
            hour,
            minute,
            second,
            micro,
            ..
        } = self;

        if hour > 23 || minute > 59 || second > 59 || micro > 999999 {
            handle_invalid_date!(self, ctx);
        }

        Ok(datetime)
    }

    fn check_timestamp(self, ctx: &mut EvalContext) -> Result<Self> {
        if self.is_zero() {
            handle_zero_date!(self, ctx);
            return Ok(self);
        }

        let date = NaiveDate::from_ymd_opt(self.year as i32, self.month, self.day);

        if date.is_none() {
            handle_invalid_date!(self, ctx);
        }

        let datetime =
            date.unwrap()
                .and_hms_micro_opt(self.hour, self.minute, self.second, self.micro);

        if datetime.is_none() {
            handle_invalid_date!(self, ctx);
        }

        let datetime = ctx.cfg.tz.from_local_datetime(&datetime.unwrap()).single();

        if datetime.is_none() {
            handle_invalid_date!(self, ctx);
        }

        let datetime = datetime.unwrap().timestamp();

        // Out of range
        if datetime < MIN_TIMESTAMP || datetime > MAX_TIMESTAMP {
            handle_invalid_date!(self, ctx);
        }

        Ok(self)
    }
}

impl From<TimeValidator> for Time {
    fn from(validator: TimeValidator) -> Time {
        Time::new(
            validator.year,
            validator.month,
            validator.day,
            validator.hour,
            validator.minute,
            validator.second,
            validator.micro,
            validator.fsp,
            validator.time_type,
        )
    }
}

// Utility
impl Time {
    #[inline]
    pub fn get_time_type(self) -> TimeType {
        let ft = self.get_fsp_tt();

        if ft >> 1 == 0b111 {
            TimeType::Date
        } else if ft & 1 == 0 {
            TimeType::DateTime
        } else {
            TimeType::TimeStamp
        }
    }

    fn from_slice(
        ctx: &mut EvalContext,
        parts: &[u32],
        fsp: u8,
        time_type: TimeType,
    ) -> Result<Self> {
        if let &[year, month, day, hour, minute, second, micro] = parts {
            TimeValidator::new(
                year, month, day, hour, minute, second, micro, fsp, time_type,
            )
            .check(ctx)
            .map(Time::from)
        } else {
            Err(Error::truncated())
        }
    }

    fn check_month_and_day(
        year: u32,
        month: u32,
        day: u32,
        allow_invalid_date: bool,
    ) -> Result<()> {
        if month > 12 || day > 31 {
            return Err(Error::truncated());
        }

        if allow_invalid_date {
            return Ok(());
        }

        if day > last_day_of_month(year, month) {
            return Err(Error::truncated());
        }

        Ok(())
    }

    fn new(
        year: u32,
        month: u32,
        day: u32,
        hour: u32,
        minute: u32,
        second: u32,
        micro: u32,
        fsp: u8,
        time_type: TimeType,
    ) -> Self {
        let mut time = Time(0);
        time.set_year(year);
        time.set_month(month);
        time.set_day(day);
        time.set_hour(hour);
        time.set_minute(minute);
        time.set_second(second);
        time.set_micro(micro);
        time.set_fsp(fsp);
        time.set_time_type(time_type);
        time
    }

    pub fn zero() -> Self {
        Time(0)
    }

    pub fn is_zero(mut self) -> bool {
        self.set_fsp_tt(0);
        self.0 == 0
    }

    #[inline]
    pub fn is_leap_year(self) -> bool {
        is_leap_year(self.year())
    }

    #[inline]
    pub fn last_day_of_month(self) -> u32 {
        last_day_of_month(self.year(), self.month())
    }

    #[inline]
    pub fn fsp(self) -> u8 {
        let fsp = self.get_fsp_tt() >> 1;
        match self.get_time_type() {
            TimeType::Date => 0,
            _ => fsp,
        }
    }

    #[inline]
    fn set_fsp(&mut self, fsp: u8) {
        self.set_fsp_tt((fsp << 1) | (self.get_fsp_tt() & 1));
    }

    #[inline]
    fn set_time_type(mut self, time_type: TimeType) {
        let ft = self.get_fsp_tt();
        let mask = match time_type {
            // Set `fsp_tt` to 0b111x
            TimeType::Date => ft | !1,
            TimeType::DateTime => ft & !1,
            TimeType::TimeStamp => ft | 1,
        };
        self.set_fsp_tt(mask);
    }
}

impl std::fmt::Display for Time {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{:04}-{:02}-{:02} {:02}:{:02}:{:02}",
            self.year(),
            self.month(),
            self.day(),
            self.hour(),
            self.minute(),
            self.second(),
        )?;
        let fsp = usize::from(self.fsp());
        if fsp > 0 {
            write!(
                f,
                ".{:0width$}",
                self.microseconds() / 10u32.pow((6 - fsp) as u32),
                width = fsp
            )?;
        }
        Ok(())
    }
}

impl std::fmt::Debug for Time {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{:04}-{:02}-{:02} {:02}:{:02}:{:02}.{:06}({:?}: {})",
            self.year(),
            self.month(),
            self.day(),
            self.hour(),
            self.minute(),
            self.second(),
            self.microseconds(),
            self.get_time_type(),
            self.fsp()
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn test_parse_valid_datetime() -> Result<()> {
        let mut ctx = EvalContext::default();
        let cases = vec![
            ("2019-09-16 10:11:12", "20190916101112", 0, false),
            ("2019-09-16 10:11:12", "190916101112", 0, false),
            ("2019-09-16 10:11:01", "19091610111", 0, false),
            ("2019-09-16 10:11:00", "1909161011", 0, false),
            ("2019-09-16 10:01:00", "190916101", 0, false),
            ("1909-12-10 00:00:00", "19091210", 0, false),
            ("2019-09-16 01:00:00", "1909161", 0, false),
            ("2019-09-16 00:00:00", "190916", 0, false),
            ("2019-09-01 00:00:00", "19091", 0, false),
            ("2019-09-16 10:11:12.111", "190916101112.111", 3, false),
            ("2019-09-16 10:11:12.111", "20190916101112.111", 3, false),
            ("2019-09-16 10:11:12.67", "20190916101112.666", 2, true),
            ("2019-09-16 10:11:13.0", "20190916101112.999", 1, true),
            ("2019-09-16 10:11:12", "2019-09-16 10:11:12", 0, false),
            ("2019-09-16 10:11:12", "2019-09-16T10:11:12", 0, false),
            ("2019-09-16 10:11:12.7", "2019-09-16T10:11:12.66", 1, true),
            ("2019-09-16 10:11:13.0", "2019-09-16T10:11:12.99", 1, true),
            ("2020-01-01 00:00:00.0", "2019-12-31 23:59:59.99", 1, true),
            (
                "2020-01-01 00:00:00.000000",
                "2019-12-31 23:59:59.9999999",
                6,
                true,
            ),
            (
                "2019-12-31 23:59:59.999999",
                "2019-12-31 23:59:59.9999999",
                6,
                false,
            ),
            (
                "2019-12-31 23:59:59.999",
                "2019-12-31 23:59:59.999999",
                3,
                false,
            ),
            (
                "2019-12-31 23:59:59.999",
                "2019*12&31T23(59)59.999999",
                3,
                false,
            ),
        ];
        for (expected, actual, fsp, round) in cases {
            assert_eq!(
                expected,
                Time::parse_datetime(&mut ctx, actual, fsp, TimeType::DateTime, round)?.to_string()
            );
        }
        Ok(())
    }
}
