// Copyright 2018 TiKV Project Authors. Licensed under Apache-2.0.

use std::cmp::Ordering;
use std::convert::{TryFrom, TryInto};

use crate::codec;
use crate::codec::mysql::{check_fsp, Tz};
use crate::codec::TEN_POW;
use crate::codec::{Error, Result};
use crate::expr::{EvalContext, Flag, SqlMode};

use tidb_query_datatype::FieldTypeTp;

use bitfield::bitfield;
use boolinator::Boolinator;
use chrono::prelude::*;

const MIN_TIMESTAMP: i64 = 0;
const MAX_TIMESTAMP: i64 = (1 << 31) - 1;
const MICRO_WIDTH: usize = 6;

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

#[inline]
fn chrono_datetime<T: TimeZone>(
    time_zone: &T,
    year: u32,
    month: u32,
    day: u32,
    hour: u32,
    minute: u32,
    second: u32,
    micro: u32,
) -> Result<DateTime<T>> {
    // NOTE: We are not using `tz::from_ymd_opt` as suggested in chrono's README due to
    // chronotope/chrono-tz #23.
    // As a workaround, we first build a NaiveDate, then attach time zone information to it.
    NaiveDate::from_ymd_opt(year as i32, month, day)
        .and_then(|date| date.and_hms_opt(hour, minute, second))
        .and_then(|t| t.checked_add_signed(chrono::Duration::microseconds(i64::from(micro))))
        .and_then(|datetime| time_zone.from_local_datetime(&datetime).earliest())
        .ok_or_else(|| Error::truncated())
}

fn round_frac(frac: u32, fsp: u8) -> (bool, u32) {
    debug_assert!(frac < 100_000_000 && fsp < 7);
    let fsp = usize::from(fsp);
    let width = if frac >= 1_000_000 { 7 } else { 6 };
    let mask = TEN_POW[width - fsp - 1];
    let result = (frac / mask + 5) / 10 * mask * if width == 6 { 10 } else { 1 };
    (result >= 1_000_000, result)
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
    pub fn micro(self) -> u32 {
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

mod parser {
    use super::*;
    fn bytes_to_u32(input: &[u8]) -> Option<u32> {
        input.iter().try_fold(0u32, |acc, d| {
            d.is_ascii_digit().as_option()?;
            acc.checked_mul(10)
                .and_then(|t| t.checked_add(u32::from(d - b'0')))
        })
    }

    fn digit1(input: &[u8]) -> Option<(&[u8], &[u8])> {
        let end = input
            .iter()
            .position(|&c| !c.is_ascii_digit())
            .unwrap_or_else(|| input.len());
        (end != 0).as_option()?;
        Some((&input[end..], &input[..end]))
    }

    fn space1(input: &[u8]) -> Option<&[u8]> {
        let end = input
            .iter()
            .position(|&c| !c.is_ascii_whitespace())
            .unwrap_or_else(|| input.len());

        (end < input.len()).as_option()?;
        Some(&input[end..])
    }

    /// We assume that the `input` is trimmed and is not empty.
    fn split_components(input: &str) -> Option<Vec<&[u8]>> {
        let mut buffer = input.as_bytes();

        debug_assert!(
            !buffer.is_empty()
                && !buffer.first().unwrap().is_ascii_whitespace()
                && !buffer.last().unwrap().is_ascii_whitespace()
        );

        let mut components = Vec::with_capacity(7);

        while !buffer.is_empty() {
            let (mut rest, digits): (&[u8], &[u8]) = digit1(buffer)?;

            components.push(digits);

            if !rest.is_empty() {
                // If a whitespace is acquired, we expect we have already collected ymd.
                if rest[0].is_ascii_whitespace() {
                    (components.len() == 3).as_option()?;
                    rest = space1(rest)?;
                }
                // If a 'T' is acquired, we expect we have already collected ymd.
                else if rest[0] == b'T' {
                    (components.len() == 3).as_option()?;
                    rest = &rest[1..];
                }
                // If a punctuation is acquired, move forward the pointer.
                else if rest[0].is_ascii_punctuation() {
                    rest = &rest[1..];
                } else {
                    return None;
                }
            }

            buffer = rest;
        }

        ((components.len() != 7 && components.len() != 2)
            || input.as_bytes()[input.len() - components.last().unwrap().len() - 1] == b'.')
            .as_option()?;

        Some(components)
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

    /// return an array that stores `[year, month, day, hour, minute, second]`
    fn parse_whole(input: &[u8]) -> Option<[u32; 6]> {
        let mut parts = [0u32; 6];

        let year_digits = match input.len() {
            14 | 8 => 4,
            9..=12 | 5..=7 => 2,
            _ => return None,
        };

        parts[0] = bytes_to_u32(&input[..year_digits])?;
        // If we encounter a two-digit year, translate it to a four-digit year.
        if year_digits == 2 {
            parts[0] = adjust_year(parts[0]);
        }

        for (i, chunk) in input[year_digits..].chunks(2).enumerate() {
            parts[i + 1] = bytes_to_u32(chunk)?;
        }

        Some(parts)
    }

    fn concat_components(components: Vec<&[u32]>) -> Vec<u32> {
        components
            .into_iter()
            .flatten()
            .copied()
            .collect::<Vec<_>>()
    }

    fn parse_frac(input: &[u8], fsp: u8, round: bool) -> Option<(bool, u32)> {
        let fsp = usize::from(fsp);
        let len = input.len();

        let (input, len) = if fsp >= input.len() {
            (input, len)
        } else {
            (&input[..fsp + round as usize], fsp + round as usize)
        };

        let frac = bytes_to_u32(input)? * TEN_POW[MICRO_WIDTH.checked_sub(len).unwrap_or(0)];

        Some(if round {
            round_frac(frac, fsp as u8)
        } else {
            (false, frac)
        })
    }

    fn round_components(parts: &mut [u32]) -> Option<()> {
        (parts.len() == 7).as_option()?;
        let modulus = [
            0,
            12,
            last_day_of_month(parts[0], parts[1]),
            24,
            60,
            60,
            1_000_000,
        ];

        for i in (1..7).rev() {
            if parts[i] >= modulus[i] {
                parts[i] -= modulus[i];
                parts[i - 1] += 1;
            }
        }
        Some(())
    }

    pub fn parse(
        ctx: &mut EvalContext,
        input: &str,
        time_type: TimeType,
        fsp: u8,
        round: bool,
    ) -> Option<Time> {
        let trimmed = input.trim();
        (!trimmed.is_empty()).as_option()?;

        let components = split_components(trimmed)?;
        match components.len() {
            1 | 2 => {
                let whole = parse_whole(components[0])?;

                let (carry, frac) = if let Some(frac) = components.get(1) {
                    // If we have a fractional part,
                    // we expect the `whole` is in format: `yymmddhhmmss/yyyymmddhhmmss`.
                    // Otherwise, the fractional part is meaningless.
                    (components[0].len() == 12 || components[0].len() == 14).as_option()?;
                    parse_frac(frac, fsp, round)?
                } else {
                    (false, 0)
                };

                let mut parts = concat_components(vec![&whole, &[frac]]);
                if carry {
                    round_components(&mut parts)?;
                }

                Time::from_slice(ctx, &parts, time_type, fsp)
            }
            3..=7 => {
                let whole = std::cmp::min(components.len(), 6);
                let mut parts: Vec<_> =
                    components[..whole]
                        .iter()
                        .try_fold(vec![], |mut acc, part| -> Option<_> {
                            acc.push(bytes_to_u32(part)?);
                            Some(acc)
                        })?;
                parts.resize(6, 0);

                let (carry, frac) = if let Some(frac) = components.get(6) {
                    parse_frac(frac, fsp, round)?
                } else {
                    (false, 0)
                };
                parts.push(frac);

                if carry {
                    round_components(&mut parts)?;
                }
                Time::from_slice(ctx, &parts, time_type, fsp)
            }
            _ => None,
        }
    }
}

impl Time {
    pub fn parse(
        ctx: &mut EvalContext,
        input: &str,
        time_type: TimeType,
        fsp: i8,
        round: bool,
    ) -> Result<Time> {
        parser::parse(ctx, input, time_type, check_fsp(fsp)?, round)
            .ok_or_else(|| Error::incorrect_datetime_value(input))
    }
}

fn handle_zero_date(ctx: &mut EvalContext, mut args: TimeArgs) -> Result<Option<TimeArgs>> {
    let sql_mode = ctx.cfg.sql_mode;
    let flags = ctx.cfg.flag;
    let strict_mode = sql_mode.contains(SqlMode::STRICT_ALL_TABLES)
        | sql_mode.contains(SqlMode::STRICT_TRANS_TABLES);
    let no_zero_date = sql_mode.contains(SqlMode::NO_ZERO_DATE);
    let ignore_truncate = flags.contains(Flag::IGNORE_TRUNCATE);

    debug_assert!(args.is_zero());

    if no_zero_date {
        (!strict_mode || ignore_truncate).ok_or(Error::truncated())?;
        ctx.warnings.append_warning(Error::truncated());
        args.clear();
        return Ok(None);
    }
    Ok(Some(args))
}

fn handle_zero_in_date(ctx: &mut EvalContext, mut args: TimeArgs) -> Result<Option<TimeArgs>> {
    let sql_mode = ctx.cfg.sql_mode;
    let flags = ctx.cfg.flag;

    let strict_mode = sql_mode.contains(SqlMode::STRICT_ALL_TABLES)
        | sql_mode.contains(SqlMode::STRICT_TRANS_TABLES);
    let no_zero_in_date = sql_mode.contains(SqlMode::NO_ZERO_IN_DATE);
    let ignore_truncate = flags.contains(Flag::IGNORE_TRUNCATE);

    debug_assert!(args.month == 0 || args.day == 0);

    if no_zero_in_date {
        // If we are in NO_ZERO_IN_DATE + STRICT_MODE, zero-in-date produces and error.
        // Otherwise, we reset the datetime value and check if we enabled NO_ZERO_DATE.
        (!strict_mode || ignore_truncate).ok_or(Error::truncated())?;
        ctx.warnings.append_warning(Error::truncated());
        args.clear();
        return handle_zero_date(ctx, args);
    }

    Ok(Some(args))
}

fn handle_invalid_date(ctx: &mut EvalContext, mut args: TimeArgs) -> Result<Option<TimeArgs>> {
    let sql_mode = ctx.cfg.sql_mode;
    let allow_invalid_date = sql_mode.contains(SqlMode::INVALID_DATES);
    allow_invalid_date.ok_or(Error::truncated())?;
    args.clear();
    handle_zero_date(ctx, args)
}

/// A validator that verify each field for the `Time`
/// NOTE: It's inappropriate to construct `Time` first and then verify it.
/// Because `Time` uses `bitfield`, the range of each field is quite narrow.
/// For example, the size of `month` field is 5 bits. If we get a value 16 for
/// `month` and set it, we will got 0 (16 % 16 == 0) instead 16 which is definitely
/// an invalid value. So we need a larger range for validation.
#[derive(Debug, Clone)]
pub struct TimeArgs {
    year: u32,
    month: u32,
    day: u32,
    hour: u32,
    minute: u32,
    second: u32,
    micro: u32,
    fsp: i8,
    time_type: TimeType,
}

impl TimeArgs {
    fn check(self, ctx: &mut EvalContext) -> Option<TimeArgs> {
        check_fsp(self.fsp).ok()?;
        let (fsp, time_type) = (self.fsp, self.time_type);
        match self.time_type {
            TimeType::Date => self.check_date(ctx),
            TimeType::DateTime => self.check_datetime(ctx),
            TimeType::TimeStamp => self.check_timestamp(ctx),
        }
        .map(|datetime| datetime.unwrap_or_else(|| TimeArgs::zero(fsp, time_type)))
        .ok()
    }

    pub fn zero(fsp: i8, time_type: TimeType) -> TimeArgs {
        TimeArgs {
            year: 0,
            month: 0,
            day: 0,
            hour: 0,
            minute: 0,
            second: 0,
            micro: 0,
            fsp,
            time_type,
        }
    }

    pub fn clear(&mut self) {
        self.year = 0;
        self.month = 0;
        self.day = 0;
        self.hour = 0;
        self.minute = 0;
        self.second = 0;
        self.micro = 0;
    }

    pub fn is_zero(&self) -> bool {
        self.year == 0
            && self.month == 0
            && self.day == 0
            && self.hour == 0
            && self.minute == 0
            && self.second == 0
            && self.micro == 0
    }

    fn check_date(mut self, ctx: &mut EvalContext) -> Result<Option<Self>> {
        let Self {
            year, month, day, ..
        } = self;

        let is_relaxed = ctx.cfg.sql_mode.contains(SqlMode::INVALID_DATES);

        if self.is_zero() {
            self = try_opt!(handle_zero_date(ctx, self));
        }

        if month == 0 || day == 0 {
            self = try_opt!(handle_zero_in_date(ctx, self));
        }

        if year > 9999 || Time::check_month_and_day(year, month, day, is_relaxed).is_err() {
            return handle_invalid_date(ctx, self);
        }

        Ok(Some(self))
    }

    fn check_datetime(self, ctx: &mut EvalContext) -> Result<Option<Self>> {
        let datetime = try_opt!(self.check_date(ctx));

        let Self {
            hour,
            minute,
            second,
            micro,
            ..
        } = datetime;

        if hour > 23 || minute > 59 || second > 59 || micro > 999999 {
            return handle_invalid_date(ctx, datetime);
        }

        Ok(Some(datetime))
    }

    fn check_timestamp(self, ctx: &mut EvalContext) -> Result<Option<Self>> {
        if self.is_zero() {
            return handle_zero_date(ctx, self);
        }

        let datetime = chrono_datetime(
            &ctx.cfg.tz,
            self.year,
            self.month,
            self.day,
            self.hour,
            self.minute,
            self.second,
            self.micro,
        );

        if datetime.is_err() {
            return handle_invalid_date(ctx, self);
        }

        let ts = datetime.unwrap().timestamp();

        // Out of range
        if ts < MIN_TIMESTAMP || ts > MAX_TIMESTAMP {
            return handle_invalid_date(ctx, self);
        }

        Ok(Some(self))
    }
}

// Utility
impl Time {
    fn from_slice(
        ctx: &mut EvalContext,
        parts: &[u32],
        time_type: TimeType,
        fsp: u8,
    ) -> Option<Self> {
        let [year, month, day, hour, minute, second, micro]: [u32; 7] = parts.try_into().ok()?;

        Time::new(
            ctx,
            TimeArgs {
                year,
                month,
                day,
                hour,
                minute,
                second,
                micro,
                fsp: fsp as i8,
                time_type,
            },
        )
        .ok()
    }

    pub fn try_from_chrono_datetime<T: Datelike + Timelike>(
        ctx: &mut EvalContext,
        datetime: T,
        fsp: i8,
        time_type: TimeType,
    ) -> Result<Self> {
        Time::new(
            ctx,
            TimeArgs {
                year: datetime.year() as u32,
                month: datetime.month(),
                day: datetime.day(),
                hour: datetime.hour(),
                minute: datetime.minute(),
                second: datetime.second(),
                micro: datetime.nanosecond() / 1000,
                fsp,
                time_type,
            },
        )
    }

    pub fn try_into_chrono_datetime(self, ctx: &mut EvalContext) -> Result<DateTime<Tz>> {
        chrono_datetime(
            &ctx.cfg.tz,
            self.year(),
            self.month(),
            self.day(),
            self.hour(),
            self.minute(),
            self.second(),
            self.micro(),
        )
    }

    fn unchecked_new(config: TimeArgs) -> Self {
        let mut time = Time(0);
        let TimeArgs {
            year,
            month,
            day,
            hour,
            minute,
            second,
            micro,
            fsp,
            time_type,
        } = config;
        time.set_year(year);
        time.set_month(month);
        time.set_day(day);
        time.set_hour(hour);
        time.set_minute(minute);
        time.set_second(second);
        time.set_micro(micro);
        time.set_fsp(fsp as u8);
        time.set_time_type(time_type);
        time
    }

    pub fn new(ctx: &mut EvalContext, mut config: TimeArgs) -> Result<Time> {
        if config.time_type == TimeType::Date {
            config.hour = 0;
            config.minute = 0;
            config.second = 0;
            config.micro = 0;
            config.fsp = 0;
        }

        let unchecked_time = Self::unchecked_new(config.clone());
        Ok(Self::unchecked_new(config.check(ctx).ok_or_else(|| {
            Error::incorrect_datetime_value(unchecked_time)
        })?))
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

    #[inline]
    fn set_time_type(&mut self, time_type: TimeType) {
        let ft = self.get_fsp_tt();
        let mask = match time_type {
            // Set `fsp_tt` to 0b111x
            TimeType::Date => ft | 0b1110,
            TimeType::DateTime => ft & 0b1110,
            TimeType::TimeStamp => ft | 1,
        };
        self.set_fsp_tt(mask);
    }

    pub fn from_packed_u64(
        ctx: &mut EvalContext,
        value: u64,
        time_type: TimeType,
        fsp: i8,
    ) -> Result<Time> {
        if value == 0 {
            return Time::new(ctx, TimeArgs::zero(fsp, time_type));
        }

        let fsp = check_fsp(fsp)?;
        let ymdhms = value >> 24;
        let ymd = ymdhms >> 17;
        let ym = ymd >> 5;
        let hms = ymdhms & ((1 << 17) - 1);

        let day = (ymd & ((1 << 5) - 1)) as u32;
        let month = (ym % 13) as u32;
        let year = (ym / 13) as u32;
        let second = (hms & ((1 << 6) - 1)) as u32;
        let minute = ((hms >> 6) & ((1 << 6) - 1)) as u32;
        let hour = (hms >> 12) as u32;
        let micro = (value & ((1 << 24) - 1)) as u32;

        if time_type == TimeType::TimeStamp {
            let utc = chrono_datetime(&Utc, year, month, day, hour, minute, second, micro)?;
            let timestamp = ctx.cfg.tz.from_utc_datetime(&utc.naive_utc());
            Time::try_from_chrono_datetime(ctx, timestamp.naive_local(), fsp as i8, time_type)
        } else {
            Time::new(
                ctx,
                TimeArgs {
                    year,
                    month,
                    day,
                    hour,
                    minute,
                    second,
                    micro,
                    fsp: fsp as i8,
                    time_type,
                },
            )
        }
    }

    pub fn to_packed_u64(mut self, ctx: &mut EvalContext) -> Result<u64> {
        if self.is_zero() {
            return Ok(0);
        }

        if self.get_time_type() == TimeType::TimeStamp {
            let ts = self.try_into_chrono_datetime(ctx)?;
            self = Time::try_from_chrono_datetime(
                ctx,
                ts.naive_utc(),
                self.fsp() as i8,
                self.get_time_type(),
            )?;
        }

        let ymd =
            ((u64::from(self.year()) * 13 + u64::from(self.month())) << 5) | u64::from(self.day());
        let hms = (u64::from(self.hour()) << 12)
            | (u64::from(self.minute()) << 6)
            | u64::from(self.second());

        Ok((((ymd << 17) | hms) << 24) | u64::from(self.micro()))
    }
}

impl PartialEq for Time {
    fn eq(&self, other: &Self) -> bool {
        let mut a = *self;
        let mut b = *other;
        a.set_fsp_tt(0);
        b.set_fsp_tt(0);
        a.0 == b.0
    }
}

impl Eq for Time {}

impl PartialOrd for Time {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for Time {
    fn cmp(&self, right: &Self) -> Ordering {
        let mut a = *self;
        let mut b = *right;
        a.set_fsp_tt(0);
        b.set_fsp_tt(0);
        a.0.cmp(&b.0)
    }
}

impl std::fmt::Display for Time {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{:04}-{:02}-{:02}",
            self.year(),
            self.month(),
            self.day()
        )?;

        if self.get_time_type() != TimeType::Date {
            write!(f, " ")?;
            write!(
                f,
                "{:02}:{:02}:{:02}",
                self.hour(),
                self.minute(),
                self.second()
            )?;
            let fsp = usize::from(self.fsp());
            if fsp > 0 {
                write!(
                    f,
                    ".{:0width$}",
                    self.micro() / TEN_POW[MICRO_WIDTH - fsp],
                    width = fsp
                )?;
            }
        }
        Ok(())
    }
}

impl std::fmt::Debug for Time {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{:04}-{:02}-{:02} {:02}:{:02}:{:02}.{:06}({:?}: {})({:b})",
            self.year(),
            self.month(),
            self.day(),
            self.hour(),
            self.minute(),
            self.second(),
            self.micro(),
            self.get_time_type(),
            self.fsp(),
            self.0
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::codec::mysql::{MAX_FSP, UNSPECIFIED_FSP};
    use crate::expr::EvalConfig;

    use std::sync::Arc;

    #[derive(Debug)]
    struct TimeEnv {
        strict_mode: bool,
        no_zero_in_date: bool,
        no_zero_date: bool,
        allow_invalid_date: bool,
        ignore_truncate: bool,
        time_zone: Option<Tz>,
    }

    impl Default for TimeEnv {
        fn default() -> TimeEnv {
            TimeEnv {
                strict_mode: false,
                no_zero_in_date: false,
                no_zero_date: false,
                allow_invalid_date: false,
                ignore_truncate: false,
                time_zone: None,
            }
        }
    }

    impl From<TimeEnv> for EvalContext {
        fn from(config: TimeEnv) -> EvalContext {
            let mut eval_config = EvalConfig::new();
            let mut sql_mode = SqlMode::empty();
            let mut flags = Flag::empty();

            if config.strict_mode {
                sql_mode |= SqlMode::STRICT_ALL_TABLES;
            }
            if config.allow_invalid_date {
                sql_mode |= SqlMode::INVALID_DATES;
            }
            if config.no_zero_date {
                sql_mode |= SqlMode::NO_ZERO_DATE;
            }
            if config.no_zero_in_date {
                sql_mode |= SqlMode::NO_ZERO_IN_DATE;
            }

            if config.ignore_truncate {
                flags |= Flag::IGNORE_TRUNCATE;
            }

            eval_config.set_sql_mode(sql_mode).set_flag(flags).tz =
                config.time_zone.unwrap_or_else(Tz::utc);

            EvalContext::new(Arc::new(eval_config))
        }
    }

    #[test]
    fn test_parse_valid_date() -> Result<()> {
        let mut ctx = EvalContext::default();
        let cases = vec![
            ("2019-09-16", "20190916101112"),
            ("2019-09-16", "190916101112"),
            ("2019-09-16", "19091610111"),
            ("2019-09-16", "1909161011"),
            ("2019-09-16", "190916101"),
            ("1909-12-10", "19091210"),
            ("2019-09-16", "1909161"),
            ("2019-09-16", "190916"),
            ("2019-09-01", "19091"),
            ("2019-09-16", "190916101112.111"),
            ("2019-09-16", "20190916101112.111"),
            ("2019-09-16", "20190916101112.666"),
            ("2019-09-16", "20190916101112.999"),
            ("2019-12-31", "2019-12-31"),
            ("2019-09-16", "2019-09-16 10:11:12"),
            ("2019-09-16", "2019-09-16T10:11:12"),
            ("2019-09-16", "2019-09-16T10:11:12.66"),
            ("2019-09-16", "2019-09-16T10:11:12.99"),
            ("2019-12-31", "2019-12-31 23:59:59.99"),
            ("2019-12-31", "2019-12-31 23:59:59.9999999"),
            ("2019-12-31", "2019-12-31 23:59:59.9999999"),
            ("2019-12-31", "2019-12-31 23:59:59.999999"),
            ("2019-12-31", "2019*12&31T23(59)59.999999"),
            ("2019-12-31", "2019.12.31.23.59.59.999999"),
            ("2019-12-31", "2019.12.31-23.59.59.999999"),
            ("2019-12-31", "2019.12.31(23.59.59.999999"),
            ("2019-12-31", "2019.12.31     23.59.59.999999"),
            ("2019-12-31", "2019.12.31 \t    23.59.59.999999"),
            ("2019-12-31", "2019.12.31 \t  23.59-59.999999"),
        ];

        for (expected, actual) in cases {
            let date = Time::parse(&mut ctx, actual, TimeType::Date, 0, false)?;
            assert_eq!(date.hour(), 0);
            assert_eq!(date.minute(), 0);
            assert_eq!(date.second(), 0);
            assert_eq!(date.micro(), 0);
            assert_eq!(date.fsp(), 0);
            assert_eq!(expected, date.to_string());
        }
        Ok(())
    }

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
            ("2019-09-16 00:00:00", "2019-09-16", 0, false),
            ("2019-09-16 10:11:12", "2019-09-16 10:11:12", 0, false),
            ("2019-09-16 10:11:12", "2019-09-16T10:11:12", 0, false),
            ("2019-09-16 10:11:12.7", "2019-09-16T10:11:12.66", 1, true),
            ("2019-09-16 10:11:13.0", "2019-09-16T10:11:12.99", 1, true),
            ("2020-01-01 00:00:00.0", "2019-12-31 23:59:59.99", 1, true),
            (
                "2020-01-01 00:00:00.0",
                "    2019-12-31 23:59:59.99   ",
                1,
                true,
            ),
            (
                "2020-01-01 00:00:00.000000",
                "2019-12-31 23:59:59.9999999",
                6,
                true,
            ),
            (
                "2020-01-01 00:00:00.000000",
                "2019-12(31-23.59.59.9999999",
                6,
                true,
            ),
            (
                "2020-01-01 00:00:00.000000",
                "2019-12(31-23.59.59.9999999",
                6,
                true,
            ),
            (
                "2020-01-01 00:00:00.000000",
                "2019-12(31    \t23.59.59.9999999",
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
                "2019-12-31  23:59:59.999999",
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
                Time::parse(&mut ctx, actual, TimeType::DateTime, fsp, round)?.to_string()
            );
        }

        let should_fail = vec![
            ("11-12-13 T 12:34:56", 0),
            ("11:12:13 T12:34:56", 0),
            ("11:12:13 T12:34:56.12", 7),
            ("11121311121.1", 2),
            ("1112131112.1", 2),
            ("111213111.1", 2),
            ("11121311.1", 2),
            ("1112131.1", 2),
            ("111213.1", 2),
            ("111213.1", 2),
            ("11121.1", 2),
            ("1112", 2),
        ];

        for (case, fsp) in should_fail {
            assert!(Time::parse(&mut ctx, case, TimeType::DateTime, fsp, false).is_err());
        }
        Ok(())
    }

    #[test]
    fn test_parse_valid_timestamp() -> Result<()> {
        let mut ctx = EvalContext::default();
        let cases = vec![
            ("2019-09-16 10:11:12", "20190916101112", 0, false),
            ("2019-09-16 10:11:12", "190916101112", 0, false),
            ("2019-09-16 10:11:01", "19091610111", 0, false),
            ("2019-09-16 10:11:00", "1909161011", 0, false),
            ("2019-09-16 10:01:00", "190916101", 0, false),
            ("2019-12-10 00:00:00", "20191210", 0, false),
            ("2019-09-16 01:00:00", "1909161", 0, false),
            ("2019-09-16 00:00:00", "190916", 0, false),
            ("2019-09-01 00:00:00", "19091", 0, false),
            ("2019-09-16 10:11:12.111", "190916101112.111", 3, false),
            ("2019-09-16 10:11:12.111", "20190916101112.111", 3, false),
            ("2019-09-16 10:11:12.67", "20190916101112.666", 2, true),
            ("2019-09-16 10:11:13.0", "20190916101112.999", 1, true),
            ("2019-09-16 00:00:00", "2019-09-16", 0, false),
            ("2019-09-16 10:11:12", "2019-09-16 10:11:12", 0, false),
            ("2019-09-16 10:11:12", "2019-09-16T10:11:12", 0, false),
            ("2019-09-16 10:11:12.7", "2019-09-16T10:11:12.66", 1, true),
            ("2019-09-16 10:11:13.0", "2019-09-16T10:11:12.99", 1, true),
            ("2020-01-01 00:00:00.0", "2019-12-31 23:59:59.99", 1, true),
            ("1970-01-01 00:00:00", "1970-01-01 00:00:00", 0, false),
            ("1970-01-01 00:00:00", "1970-1-1 00:00:00", 0, false),
            ("1970-01-01 12:13:09", "1970-1-1 12:13:9", 0, false),
            ("1970-01-01 09:08:09", "1970-1-1 9:8:9", 0, false),
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
                "2019-12-31     23:59:59.999999",
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
                Time::parse(&mut ctx, actual, TimeType::TimeStamp, fsp, round)?.to_string()
            );
        }
        Ok(())
    }

    #[test]
    fn test_allow_invalid_date() -> Result<()> {
        let cases = vec![
            ("2019-02-31", "2019-2-31"),
            ("2019-02-29", "2019-2-29"),
            ("2019-04-31", "2019-4-31"),
            ("0000-00-00", "2019-1-32"),
            ("0000-00-00", "2019-13-1"),
            ("2019-02-11", "2019-02-11"),
            ("2019-02-00", "2019-02-00"),
        ];

        for (expected, actual) in cases {
            let mut ctx = EvalContext::from(TimeEnv {
                allow_invalid_date: true,
                ..TimeEnv::default()
            });
            assert_eq!(
                expected,
                Time::parse(&mut ctx, actual, TimeType::Date, 0, false)?.to_string()
            );
        }
        Ok(())
    }

    #[test]
    fn test_invalid_datetime() -> Result<()> {
        let mut ctx = EvalContext::from(TimeEnv {
            allow_invalid_date: true,
            ..TimeEnv::default()
        });

        let cases = vec![
            "2019-12-31 24:23:22",
            "2019-12-31 23:60:22",
            "2019-12-31 23:24:60",
        ];

        for case in cases {
            assert_eq!(
                "0000-00-00 00:00:00",
                Time::parse(&mut ctx, case, TimeType::DateTime, 0, false)?.to_string()
            );
        }
        Ok(())
    }

    #[test]
    fn test_allow_invalid_timestamp() -> Result<()> {
        let mut ctx = EvalContext::from(TimeEnv {
            allow_invalid_date: true,
            ..TimeEnv::default()
        });

        let ok_cases = vec![
            "2019-9-31 11:11:11",
            "2019-0-1 11:11:11",
            "2013-2-29 11:11:11",
        ];
        for case in ok_cases {
            assert_eq!(
                "0000-00-00 00:00:00",
                Time::parse(&mut ctx, case, TimeType::TimeStamp, 0, false)?.to_string()
            );
        }

        let dsts = vec![
            ("2019-03-10 02:00:00", "America/New_York"),
            ("2018-04-01 02:00:00", "America/Monterrey"),
        ];
        for (timestamp, time_zone) in dsts {
            let mut ctx = EvalContext::from(TimeEnv {
                allow_invalid_date: true,
                time_zone: Tz::from_tz_name(time_zone),
                ..TimeEnv::default()
            });
            assert_eq!(
                "0000-00-00 00:00:00",
                Time::parse(&mut ctx, timestamp, TimeType::TimeStamp, 0, false)?.to_string()
            )
        }

        Ok(())
    }

    #[test]
    fn test_no_zero_date() -> Result<()> {
        // Enable NO_ZERO_DATE only. If zero-date is encountered, a warning is produced.
        let mut ctx = EvalContext::from(TimeEnv {
            no_zero_date: true,
            ..TimeEnv::default()
        });

        let _ = Time::parse(
            &mut ctx,
            "0000-00-00 00:00:00",
            TimeType::DateTime,
            0,
            false,
        )?;

        assert!(ctx.warnings.warning_cnt > 0);

        // Enable both NO_ZERO_DATE and STRICT_MODE.
        // If zero-date is encountered, an error is returned.
        let mut ctx = EvalContext::from(TimeEnv {
            no_zero_date: true,
            strict_mode: true,
            ..TimeEnv::default()
        });

        assert!(Time::parse(
            &mut ctx,
            "0000-00-00 00:00:00",
            TimeType::DateTime,
            0,
            false,
        )
        .is_err());

        // Enable NO_ZERO_DATE, STRICT_MODE and IGNORE_TRUNCATE.
        // If zero-date is encountered, an error is returned.
        let mut ctx = EvalContext::from(TimeEnv {
            no_zero_date: true,
            strict_mode: true,
            ignore_truncate: true,
            ..TimeEnv::default()
        });

        assert_eq!(
            "0000-00-00 00:00:00",
            Time::parse(
                &mut ctx,
                "0000-00-00 00:00:00",
                TimeType::DateTime,
                0,
                false,
            )?
            .to_string()
        );

        assert!(ctx.warnings.warning_cnt > 0);

        let cases = vec![
            "2019-12-31 24:23:22",
            "2019-12-31 23:60:22",
            "2019-12-31 23:24:60",
        ];

        for case in cases {
            // Enable NO_ZERO_DATE, STRICT_MODE and ALLOW_INVALID_DATE.
            // If an invalid date (converted to zero-date) is encountered, an error is returned.
            let mut ctx = EvalContext::from(TimeEnv {
                no_zero_date: true,
                strict_mode: true,
                ..TimeEnv::default()
            });
            assert!(Time::parse(&mut ctx, case, TimeType::DateTime, 0, false).is_err());
        }

        Ok(())
    }

    #[test]
    fn test_no_zero_in_date() -> Result<()> {
        let cases = vec!["2019-01-00", "2019-00-01"];

        for &case in cases.iter() {
            // Enable NO_ZERO_IN_DATE only. If zero-date is encountered, a warning is produced.
            let mut ctx = EvalContext::from(TimeEnv {
                no_zero_in_date: true,
                ..TimeEnv::default()
            });

            let _ = Time::parse(&mut ctx, case, TimeType::DateTime, 0, false)?;

            assert!(ctx.warnings.warning_cnt > 0);
        }

        // Enable NO_ZERO_IN_DATE, STRICT_MODE and IGNORE_TRUNCATE.
        // If zero-date is encountered, an error is returned.
        let mut ctx = EvalContext::from(TimeEnv {
            no_zero_in_date: true,
            strict_mode: true,
            ignore_truncate: true,
            ..TimeEnv::default()
        });

        assert_eq!(
            "0000-00-00 00:00:00",
            Time::parse(
                &mut ctx,
                "0000-00-00 00:00:00",
                TimeType::DateTime,
                0,
                false,
            )?
            .to_string()
        );

        assert!(ctx.warnings.warning_cnt > 0);

        for &case in cases.iter() {
            // Enable both NO_ZERO_IN_DATE and STRICT_MODE,.
            // If zero-date is encountered, an error is returned.
            let mut ctx = EvalContext::from(TimeEnv {
                no_zero_in_date: true,
                strict_mode: true,
                ..TimeEnv::default()
            });
            assert!(Time::parse(&mut ctx, case, TimeType::DateTime, 0, false).is_err());
        }

        Ok(())
    }

    #[test]
    fn test_codec_datetime() -> Result<()> {
        let cases = vec![
            ("2010-10-10 10:11:11", 0),
            ("2017-01-01 00:00:00", 0),
            ("2004-01-01 00:00:00", UNSPECIFIED_FSP),
            ("2013-01-01 00:00:00.000000", MAX_FSP),
            ("2019-01-01 00:00:00.123456", MAX_FSP),
            ("2001-01-01 00:00:00.123456", MAX_FSP),
            ("2007-06-01 00:00:00.999999", MAX_FSP),
            // Invalid cases
            ("0000-00-00 00:00:00", 0),
            ("2007-00-01 00:00:00.999999", MAX_FSP),
            ("2017-01-00 00:00:00.999999", MAX_FSP),
            ("2027-00-00 00:00:00.999999", MAX_FSP),
            ("2027-04-31 00:00:00.999999", MAX_FSP),
        ];

        for (case, fsp) in cases {
            let mut ctx = EvalContext::from(TimeEnv {
                allow_invalid_date: true,
                ..TimeEnv::default()
            });
            let time = Time::parse(&mut ctx, case, TimeType::DateTime, fsp, false)?;

            let packed = time.to_packed_u64(&mut ctx)?;
            let reverted_datetime =
                Time::from_packed_u64(&mut ctx, packed, TimeType::DateTime, fsp)?;

            assert_eq!(time, reverted_datetime);
        }

        Ok(())
    }

    #[test]
    fn test_codec_timestamp() -> Result<()> {
        let tz_table = vec!["Etc/GMT+11", "Etc/GMT0", "Etc/GMT-5", "UTC", "Universal"];

        let cases = vec![
            ("0000-00-00 00:00:00", 0),
            ("2010-10-10 10:11:11", 0),
            ("2017-01-01 00:00:00", 0),
            ("2004-01-01 00:00:00", UNSPECIFIED_FSP),
            ("2019-07-01 12:13:14.999", MAX_FSP),
            ("2013-01-01 00:00:00.000000", MAX_FSP),
            ("2019-04-01 00:00:00.123456", MAX_FSP),
            ("2001-01-01 00:00:00.123456", MAX_FSP),
            ("2007-08-01 00:00:00.999999", MAX_FSP),
        ];

        for tz in tz_table {
            for &(case, fsp) in cases.iter() {
                let mut ctx = EvalContext::from(TimeEnv {
                    time_zone: Tz::from_tz_name(tz),
                    ..TimeEnv::default()
                });

                let time = Time::parse(&mut ctx, case, TimeType::TimeStamp, fsp, false)?;

                let packed = time.to_packed_u64(&mut ctx)?;
                let reverted_datetime =
                    Time::from_packed_u64(&mut ctx, packed, TimeType::TimeStamp, fsp)?;

                assert_eq!(time, reverted_datetime);
            }
        }

        Ok(())
    }

    #[test]
    fn test_compare() -> Result<()> {
        let cases = vec![
            (
                "2019-03-17 12:13:14.11",
                "2019-03-17 12:13:14.11",
                Ordering::Equal,
            ),
            ("2019-4-1 1:2:3", "2019-3-31 23:59:59", Ordering::Greater),
            ("2019-09-16 1:2:3", "2019-10-01 1:2:1", Ordering::Less),
            ("0000-00-00", "0000-00-00", Ordering::Equal),
        ];

        for (left, right, expected) in cases {
            let mut ctx = EvalContext::default();
            let left = Time::parse(&mut ctx, left, TimeType::DateTime, MAX_FSP, false)?;
            let right = Time::parse(&mut ctx, right, TimeType::DateTime, MAX_FSP, false)?;
            assert_eq!(expected, left.cmp(&right));
        }
        Ok(())
    }
}
