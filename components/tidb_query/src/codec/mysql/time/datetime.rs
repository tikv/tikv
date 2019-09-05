use std::convert::TryFrom;

use crate::codec::{self, error, mysql};
use crate::codec::{Error, Result};
use crate::expr::{EvalContext, SqlMode};

use tidb_query_datatype::FieldTypeTp;

use bitfield::bitfield;
use chrono::{self, Datelike, NaiveDate, NaiveDateTime, TimeZone};

const HOURS_PER_DAY: u32 = 24;
const SECS_PER_MINUTE: u32 = 60;
const MINUTES_PER_HOUR: u32 = 60;

const MICROS_PER_SEC: u32 = 1_000_000;

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
    get_month, set_month: 49,46;
    #[inline]
    get_day, set_day: 45,41;
    #[inline]
    get_hour, set_hour: 40,36;
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

#[derive(PartialEq)]
pub enum TimeType {
    Date,
    DateTime,
    TimeStamp,
}

impl TryFrom<FieldTypeTp> for TimeType {
    type Error = codec::Error;
    fn try_from(tp: FieldTypeTp) -> Result<TimeType> {
        Ok(match tp {
            FieldTypeTp::Date => TimeType::Date,
            FieldTypeTp::DateTime => TimeType::DateTime,
            FieldTypeTp::Timestamp => TimeType::TimeStamp,
            _ => return Err(box_err!("Time does not support field type {}", tp)),
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

// The common set of methods for time component.
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
}

// The common set of methods for date component.
impl Time {
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

impl From<Time> for NaiveDate {
    fn from(time: Time) -> NaiveDate {
        NaiveDate::from_ymd(time.year() as i32, time.month(), time.day())
    }
}

impl From<Time> for NaiveDateTime {
    fn from(time: Time) -> NaiveDateTime {
        unimplemented!()
        /*NaiveDateTime::from_ymd(time.year() as i32, time.month(), time.day()).and_hms(
            time.hour(),
            time.minute(),
            time.second(),
        )*/
    }
}

impl From<NaiveDate> for Time {
    fn from(date: NaiveDate) -> Time {
        let mut time = Time::default();

        time.set_year(date.year() as u32);
        time.set_month(date.month());
        time.set_day(date.day());

        time
    }
}

// Parser
impl Time {
    fn split_frac(input: &[u8]) -> Result<(&[u8], Option<&[u8]>)> {
        let parts = input.split(|&x| x == b'.').collect::<Vec<_>>();

        if parts.len() > 2 {
            Err(Error::truncated())
        } else {
            Ok((parts[0], parts.get(1).copied()))
        }
    }

    fn split_datetime(input: &[u8]) -> Vec<&[u8]> {
        input.split(|&x| x == b' ' || x == b'T').collect::<Vec<_>>()
    }

    fn parse_frac(input: &[u8], fsp: u8) -> Result<u32> {
        if input.is_empty() {
            return Ok(0);
        }

        let len = input.len() as u32;
        let fsp = u32::from(fsp);

        let (input, len) = if fsp >= len {
            (input, len)
        } else {
            (&input[..=fsp as usize], fsp + 1)
        };

        let frac = str_to_u32(input)?;
        Ok(frac * 10u32.pow(6u32.checked_sub(len).unwrap_or(0)))
    }

    fn parse_float_string(
        ctx: &mut EvalContext,
        input: &[u8],
        fsp: u8,
        tp: TimeType,
    ) -> Result<Time> {
        let (whole, frac) = Self::split_frac(input)?;
        let mut parts = [0u32; 7];

        let year_digits = match whole.len() {
            14 | 8 => 4,
            9..=12 | 5..=7 => 2,
            _ => Err(Error::truncated())?,
        };

        parts[0] = str_to_u32(&whole[..year_digits])?;
        for (i, chunk) in whole[year_digits..].chunks(2).enumerate() {
            parts[i + 1] = str_to_u32(chunk)?;
        }
        parts[6] = Self::parse_frac(frac.unwrap_or(&[]), fsp)?;

        Self::from_slice(ctx, &parts, fsp, tp)
    }

    fn parse_date(input: &[u8]) -> Result<[u32; 3]> {
        let date = input
            .split(|x| x.is_ascii_punctuation())
            .collect::<Vec<_>>();
        if date.len() != 3 {
            Err(Error::truncated())
        } else {
            let mut parts = [0u32; 3];
            for (i, value) in date.into_iter().enumerate() {
                parts[i] = str_to_u32(value)?;
            }
            Ok(parts)
        }
    }

    pub fn parse_time(input: &[u8], fsp: u8) -> Result<[u32; 4]> {
        let (whole, frac) = Self::split_frac(input)?;

        let whole = whole
            .split(|x| x.is_ascii_punctuation())
            .collect::<Vec<_>>();

        let mut parts = [0u32; 4];

        for (i, value) in whole.into_iter().enumerate() {
            parts[i] = str_to_u32(value)?;
        }

        parts[3] = Self::parse_frac(frac.unwrap_or(&[]), fsp)?;

        Ok(parts)
    }

    pub fn parse_datetime(
        ctx: &mut EvalContext,
        input: &str,
        fsp: i8,
        tp: TimeType,
    ) -> Result<Time> {
        let input = input.trim().as_bytes();
        let fsp = mysql::check_fsp(fsp)?;

        if input.is_empty() {
            return Err(Error::truncated());
        }

        let parts = Self::split_datetime(input);

        match parts.len() {
            // Float string fromat
            1 => Self::parse_float_string(ctx, input, fsp, tp),

            // Normal format
            2 => {
                let ymd = Self::parse_date(parts[0])?;
                let hms = Self::parse_time(parts[1], fsp)?;
                Self::from_slice(
                    ctx,
                    &ymd.into_iter()
                        .copied()
                        .chain(hms.into_iter().copied())
                        .collect::<Vec<_>>()[..],
                    fsp,
                    tp,
                )
            }
            _ => Err(Error::truncated()),
        }
    }
}

impl Time {
    #[inline]
    pub fn get_time_type(self) -> TimeType {
        let ft = self.get_fsp_tt();

        if ft == 0b1111 {
            TimeType::Date
        } else if ft & 1 == 0 {
            TimeType::DateTime
        } else {
            TimeType::TimeStamp
        }
    }

    /// Panic: if `parts`'s length < 7
    fn from_slice(ctx: &mut EvalContext, parts: &[u32], fsp: u8, tp: TimeType) -> Result<Self> {
        assert!(parts.len() >= 7);
        let mut time = Time(0);

        // Check the date
        let allow_invalid_date = ctx.cfg.sql_mode.contains(SqlMode::INVALID_DATES);

        time.set_year(parts[0]);
        time.set_month(parts[1]);
        time.set_day(parts[2]);

        if tp != TimeType::Date {
            time.set_hour(Self::check_hour(parts[3])?);
            time.set_minute(Self::check_minute(parts[4])?);
            time.set_second(Self::check_second(parts[5])?);
            time.set_micro(Self::check_micro(parts[6])?);
            time.set_fsp(fsp);
        }

        time.set_type(tp);

        Ok(time)
    }

    pub fn zero() -> Self {
        Time(0)
    }

    pub fn is_zero(mut self) -> bool {
        self.set_fsp_tt(0);
        self.0 == 0
    }

    /*pub fn into_chrono_datetime(self, ctx: &EvalContext) -> Option<DateTime<Tz>> {
        ctx.cfg
            .tz
            .from_local_datetime(&NaiveDateTime::from(self))
            .ok()
    }*/

    #[inline]
    pub fn is_leap_year(self) -> bool {
        is_leap_year(self.year())
    }

    #[inline]
    pub fn last_day_of_month(self) -> u32 {
        last_day_of_month(self.year(), self.month())
    }

    #[inline]
    fn check_year(year: u32, tp: TimeType) -> Result<u32> {
        if (tp == TimeType::TimeStamp && (year < 1970 || year > 2038))
            || (tp != TimeType::TimeStamp && year > 9999)
        {
            Err(Error::truncated())
        } else {
            Ok(year)
        }
    }

    #[inline]
    fn check_hour(hour: u32) -> Result<u32> {
        if hour < HOURS_PER_DAY {
            Ok(hour)
        } else {
            Err(Error::Eval(
                "DURATION OVERFLOW".to_string(),
                error::ERR_DATA_OUT_OF_RANGE,
            ))
        }
    }

    #[inline]
    fn check_minute(minute: u32) -> Result<u32> {
        if minute < MINUTES_PER_HOUR {
            Ok(minute)
        } else {
            Err(Error::truncated_wrong_val("MINUTES", minute))
        }
    }

    #[inline]
    fn check_second(second: u32) -> Result<u32> {
        if second < SECS_PER_MINUTE {
            Ok(second)
        } else {
            Err(Error::truncated_wrong_val("SECONDS", second))
        }
    }

    #[inline]
    fn check_micro(micros: u32) -> Result<u32> {
        if micros < MICROS_PER_SEC {
            Ok(micros)
        } else {
            Err(Error::truncated_wrong_val("MICROS", micros))
        }
    }

    #[inline]
    #[allow(dead_code)]
    fn set_fsp(mut self, fsp: u8) -> Self {
        self.set_fsp_tt((fsp << 1) | (self.get_fsp_tt() & 1));
        self
    }

    #[inline]
    #[allow(dead_code)]
    fn set_type(mut self, tp: TimeType) -> Self {
        let ft = self.get_fsp_tt();
        let mask = match tp {
            TimeType::Date => ft | !1,
            TimeType::DateTime => ft & !1,
            TimeType::TimeStamp => ft | 1,
        };
        self.set_fsp_tt(mask);
        self
    }
}
