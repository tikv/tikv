// Copyright 2018 TiKV Project Authors. Licensed under Apache-2.0.

pub mod extension;
mod tz;
pub mod weekmode;

pub use self::extension::*;
pub use self::tz::Tz;
pub use self::weekmode::WeekMode;

use std::cmp::Ordering;
use std::convert::{TryFrom, TryInto};
use std::fmt::Write;
use std::hash::{Hash, Hasher};

use bitfield::bitfield;
use boolinator::Boolinator;
use chrono::prelude::*;

use crate::{FieldTypeAccessor, FieldTypeTp};
use codec::prelude::*;
use tipb::FieldType;

use crate::codec::convert::ConvertTo;
use crate::codec::mysql::{check_fsp, Decimal, Duration};
use crate::codec::{Error, Result, TEN_POW};
use crate::expr::{EvalContext, Flag, SqlMode};

const MIN_TIMESTAMP: i64 = 0;
pub const MAX_TIMESTAMP: i64 = (1 << 31) - 1;
const MICRO_WIDTH: usize = 6;
const MAX_COMPONENTS_LEN: usize = 9;
pub const MIN_YEAR: u32 = 1901;
pub const MAX_YEAR: u32 = 2155;

pub const MONTH_NAMES: &[&str] = &[
    "January",
    "February",
    "March",
    "April",
    "May",
    "June",
    "July",
    "August",
    "September",
    "October",
    "November",
    "December",
];

const MONTH_NAMES_ABBR: &[&str] = &[
    "Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec",
];

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

/// Round each component.
/// ```ignore
/// let mut parts = [2019, 12, 1, 23, 59, 59, 1000000];
/// round_components(&mut parts);
/// assert_eq!([2019, 12, 2, 0, 0, 0, 0], parts);
/// ```
/// When year, month or day is zero, there can not have a carry.
/// e.g.: `"1998-11-00 23:59:59.999" (fsp = 2, round = true)`, in `hms` it contains a carry,
/// however, the `day` is 0, which is invalid in `MySQL`. When thoese cases encountered, return
/// None.
fn round_components(parts: &mut [u32]) -> Option<()> {
    debug_assert_eq!(parts.len(), 7);
    let modulus = [
        std::u32::MAX,
        12,
        last_day_of_month(parts[0], parts[1]),
        // hms[.fraction]
        24,
        60,
        60,
        1_000_000,
    ];
    for i in (1..=6).rev() {
        let is_ymd = u32::from(i < 3);
        if parts[i] >= modulus[i] + is_ymd {
            parts[i] -= modulus[i];
            if i < 4 && parts[i - 1] == 0 || parts[i - 1] > modulus[i - 1] {
                return None;
            }
            parts[i - 1] += 1;
        }
    }
    Some(())
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
        .ok_or_else(Error::truncated)
}

#[inline]
fn chrono_naive_datetime(
    year: u32,
    month: u32,
    day: u32,
    hour: u32,
    minute: u32,
    second: u32,
    micro: u32,
) -> Result<NaiveDateTime> {
    NaiveDate::from_ymd_opt(year as i32, month, day)
        .and_then(|date| date.and_hms_opt(hour, minute, second))
        .and_then(|t| t.checked_add_signed(chrono::Duration::microseconds(i64::from(micro))))
        .ok_or_else(Error::truncated)
}

/// Round `frac` with `fsp`, return if there is a carry and the result.
/// NOTE: we assume that `frac` is less than `100_000_000` and `fsp` is valid.
/// ```ignore
/// assert_eq!(123460, round_frac(123456, 5));
/// assert_eq!(1_000_000, round_frac(999999, 5));
/// assert_eq!(1230, round_frac(1234, 5)); // .001234, fsp = 5 => .001230
/// ```
fn round_frac(frac: u32, fsp: u8) -> (bool, u32) {
    debug_assert!(frac < 100_000_000);
    debug_assert!(fsp < 7);
    if frac < 1_000_000 && fsp == 6 {
        return (false, frac);
    }

    let fsp = usize::from(fsp);
    let width: usize = if frac >= 1_000_000 { 7 } else { 6 };
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
    // 2. `type` bit 1 represent `Timestamp`
    //
    // Since `Date` does not require `fsp`, we could use `fsp_tt` == 0b1110 to represent it.
    #[inline]
    u8, get_fsp_tt, set_fsp_tt: 3, 0;
}

#[derive(PartialEq, Clone, Copy, Debug)]
pub enum TimeType {
    Date,
    DateTime,
    Timestamp,
}

impl TryFrom<FieldTypeTp> for TimeType {
    type Error = crate::codec::Error;
    fn try_from(time_type: FieldTypeTp) -> Result<TimeType> {
        Ok(match time_type {
            FieldTypeTp::Date => TimeType::Date,
            FieldTypeTp::DateTime => TimeType::DateTime,
            FieldTypeTp::Timestamp => TimeType::Timestamp,
            // TODO: Remove the support of transfering `Unspecified` to `DateTime`
            FieldTypeTp::Unspecified => TimeType::DateTime,
            _ => return Err(box_err!("Time does not support field type {}", time_type)),
        })
    }
}

impl From<TimeType> for FieldTypeTp {
    fn from(time_type: TimeType) -> FieldTypeTp {
        match time_type {
            TimeType::Timestamp => FieldTypeTp::Timestamp,
            TimeType::DateTime => FieldTypeTp::DateTime,
            TimeType::Date => FieldTypeTp::Date,
        }
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

    /// used to convert period to month
    pub fn period_to_month(period: u64) -> u64 {
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

    /// used to convert month to period
    pub fn month_to_period(month: u64) -> u64 {
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

    /// Match at least one digit and return the rest of the slice.
    /// ```ignore
    ///  digit1(b"12:32") == Some((b":32", b"12"))
    ///  digit1(b":32") == None
    /// ```
    fn digit1(input: &[u8]) -> Option<(&[u8], &[u8])> {
        let end = input
            .iter()
            .position(|&c| !c.is_ascii_digit())
            .unwrap_or_else(|| input.len());
        (end != 0).as_option()?;
        Some((&input[end..], &input[..end]))
    }

    /// Match at least one space and return the rest of the slice.
    /// ```ignore
    ///  space1(b"    12:32") == Some((b"    ", b"12:32"))
    ///  space1(b":32") == None
    /// ```
    fn space1(input: &[u8]) -> Option<(&[u8], &[u8])> {
        let end = input.iter().position(|&c| !c.is_ascii_whitespace())?;

        Some((&input[end..], &input[..end]))
    }

    /// Match at least one ascii punctuation and return the rest of the slice.
    /// ```ignore
    ///  punct1(b"..10") == Some((b"..", b"10"))
    ///  punct1(b"10:32") == None
    /// ```
    fn punct1(input: &[u8]) -> Option<(&[u8], &[u8])> {
        let end = input.iter().position(|&c| !c.is_ascii_punctuation())?;

        Some((&input[end..], &input[..end]))
    }

    /// We assume that the `input` is trimmed and is not empty.
    /// ```ignore
    ///  split_components_with_tz(b"2020-12-24T15:37:50+0800")?.1 == Some(480*60)
    /// ```
    /// the second value if not None indicates the offset in seconds of the timezone parsed
    fn split_components_with_tz(input: &str) -> Option<(Vec<&[u8]>, Option<i32>)> {
        let mut buffer = input.as_bytes();

        debug_assert!(
            !buffer.is_empty()
                && !buffer.first().unwrap().is_ascii_whitespace()
                && !buffer.last().unwrap().is_ascii_whitespace()
        );

        let mut components = Vec::with_capacity(MAX_COMPONENTS_LEN);
        let mut separators = Vec::with_capacity(MAX_COMPONENTS_LEN - 1);

        while !buffer.is_empty() {
            let (mut rest, digits): (&[u8], &[u8]) = digit1(buffer)?;

            components.push(digits);

            if !rest.is_empty() {
                // If a whitespace is acquired, we expect we have already collected ymd.
                if rest[0].is_ascii_whitespace() {
                    (components.len() == 3).as_option()?;
                    let result = space1(rest)?;
                    rest = result.0;
                    separators.push(result.1);
                }
                // If a 'T' is acquired, we expect we have already collected ymd.
                else if rest[0] == b'T' {
                    (components.len() == 3).as_option()?;
                    separators.push(&rest[..1]);
                    rest = &rest[1..];
                }
                // If a 'Z' is acquired, we expect that we are parsing timezone now.
                // the time should be in ISO8601 format, e.g. 2020-10-10T19:27:10Z, so there should
                // be 6 part ahead or 7 if considering fsp.
                else if rest[0] == b'Z' {
                    (components.len() == 6 || components.len() == 7).as_option()?;
                    separators.push(&rest[..1]);
                    rest = &rest[1..];
                }
                // If a punctuation is acquired, move forward the pointer. Note that we should
                // consume multiple punctuations if existing because MySQL allows to parse time
                // like 2020--12..16T18::58^^45.
                else if rest[0].is_ascii_punctuation() {
                    let result = punct1(rest)?;
                    separators.push(result.1);
                    rest = result.0;
                } else {
                    return None;
                }
            }

            buffer = rest;
        }

        let mut tz_offset = 0i32;
        let mut tz_sign: &[u8] = b"";
        let mut tz_hour: &[u8] = b"";
        let mut tz_minute: &[u8] = b"";
        let mut has_tz = false;
        // the following statement handles timezone
        match components.len() {
            9 => {
                // 2020-12-23 15:59:23.233333+08:00
                (separators.len() == 8).as_option()?;
                match separators[6..] {
                    [b"+", b":"] | [b"-", b":"] => {
                        has_tz = true;
                        tz_sign = separators[6];
                        tz_minute = components.pop()?;
                        tz_hour = components.pop()?;
                    }
                    _ => return None,
                };
            }
            8 => {
                // 2020-12-23 15:59:23.2333-08
                // 2020-12-23 15:59:23.2333-0800
                // 2020-12-23 15:59:23+08:00
                (separators.len() == 7).as_option()?;
                match separators[5..] {
                    [b".", b"-"] | [b".", b"+"] => {
                        has_tz = true;
                        tz_sign = separators[6];
                        tz_hour = components.pop()?;
                    }
                    [b"+", b":"] | [b"-", b":"] => {
                        has_tz = true;
                        tz_sign = separators[5];
                        tz_minute = components.pop()?;
                        tz_hour = components.pop()?;
                    }
                    _ => return None,
                }
            }
            7 => {
                // 2020-12-23 15:59:23.23333Z
                // 2020-12-23 15:59:23+0800
                // 2020-12-23 15:59:23-08
                match separators.len() {
                    7 => {
                        (separators.last()? == b"Z").as_option()?;
                        has_tz = true;
                    }
                    6 => {
                        tz_sign = separators[5];
                        if tz_sign == b"+" || tz_sign == b"-" {
                            has_tz = true;
                            tz_hour = components.pop()?;
                        }
                    }
                    _ => return None, // this branch can never be reached
                }
            }
            6 => {
                // 2020-12-23 15:59:23Z
                if separators.len() == 6 && separators.last()? == b"Z" {
                    has_tz = true;
                }
            }
            _ => {}
        }
        if has_tz {
            if tz_hour.len() == 4 {
                let tmp = tz_hour.split_at(2);
                tz_hour = tmp.0;
                tz_minute = tmp.1;
            }
            ((tz_hour.len() == 2 || tz_hour.is_empty())
                && (tz_minute.len() == 2 || tz_minute.is_empty()))
            .as_option()?;
            let delta_hour = bytes_to_u32(tz_hour)? as i32;
            let delta_minute = bytes_to_u32(tz_minute)? as i32;
            (!(delta_hour > 14
                || delta_minute > 59
                || (delta_hour == 14 && delta_minute != 0)
                || (tz_sign == b"-" && delta_hour == 0 && delta_minute == 0)))
                .as_option()?;
            tz_offset = (delta_hour * 60 + delta_minute) * 60;
            if tz_sign == b"-" {
                tz_offset = -tz_offset;
            }
        }
        // the following statement checks fsp
        ((components.len() != 7 && components.len() != 2)
            || input.as_bytes()[input.len() - components.last().unwrap().len() - 1] == b'.')
            .as_option()?;

        Some((components, if has_tz { Some(tz_offset) } else { None }))
    }

    /// If a two-digit year encountered, add an offset to it.
    /// 99 -> 1999
    /// 20 -> 2020
    fn adjust_year(year: u32) -> u32 {
        if year <= 69 {
            2000 + year
        } else if (70..=99).contains(&year) {
            1900 + year
        } else {
            year
        }
    }

    /// Try to parse a datetime string `input` without fractional part and separators.
    /// return an array that stores `[year, month, day, hour, minute, second, 0]`
    fn parse_whole(input: &[u8]) -> Option<[u32; 7]> {
        let mut parts = [0u32; 7];

        // If `input`'s len is 8 or 14, then `input` should be in format like:
        // yyyymmdd/yyyymmddhhmmss which means we have a four-digit year.
        // Otherwise, we have a two-digit year.
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

    /// Try to parse a fractional part from `input` with `fsp`, round the result if `round` is
    /// true.
    /// NOTE: This function assumes that `fsp` is in range: [0, 6].
    fn parse_frac(input: &[u8], fsp: u8, round: bool) -> Option<(bool, u32)> {
        debug_assert!(fsp < 7);
        let fsp = usize::from(fsp);
        let len = input.len();

        let (input, len) = if fsp >= input.len() {
            (input, len)
        } else {
            (&input[..fsp + round as usize], fsp + round as usize)
        };

        let frac = bytes_to_u32(input)? * TEN_POW[MICRO_WIDTH.saturating_sub(len)];

        Some(if round {
            round_frac(frac, fsp as u8)
        } else {
            (false, frac)
        })
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

        // to support ISO8601 and MySQL's time zone support, we further parse the following formats
        // 2020-12-17T11:55:55Z
        // 2020-12-17T11:55:55+0800
        // 2020-12-17T11:55:55-08
        // 2020-12-17T11:55:55+02:00
        let (components, tz) = split_components_with_tz(trimmed)?;
        let time_without_tz = match components.len() {
            1 | 2 => {
                let mut whole = parse_whole(components[0])?;

                let (carry, frac) = if let Some(frac) = components.get(1) {
                    // If we have a fractional part,
                    // we expect the `whole` is in format: `yymmddhhmm/yymmddhhmmss/yyyymmddhhmmss`.
                    // Otherwise, the fractional part is meaningless.
                    (components[0].len() == 10
                        || components[0].len() == 12
                        || components[0].len() == 14)
                        .as_option()?;
                    parse_frac(frac, fsp, round)?
                } else {
                    (false, 0)
                };

                whole[6] = frac;
                let mut parts = whole;
                if carry {
                    round_components(&mut parts)?;
                }

                Time::from_slice(ctx, &parts, time_type, fsp)
            }
            3..=7 => {
                let whole = std::cmp::min(components.len(), 6);
                let mut parts: Vec<_> = components[..whole].iter().try_fold(
                    Vec::with_capacity(MAX_COMPONENTS_LEN),
                    |mut acc, part| -> Option<_> {
                        acc.push(bytes_to_u32(part)?);
                        Some(acc)
                    },
                )?;

                let (carry, frac) = if let Some(frac) = components.get(6) {
                    parse_frac(frac, fsp, round)?
                } else {
                    (false, 0)
                };

                parts.resize(6, 0);
                parts.push(frac);
                // Skip a special case "00-00-00".
                if components[0].len() == 2 && !parts.iter().all(|x| *x == 0u32) {
                    parts[0] = adjust_year(parts[0]);
                }

                if carry {
                    round_components(&mut parts)?;
                }
                Time::from_slice(ctx, &parts, time_type, fsp)
            }
            _ => None,
        };
        match (tz, time_without_tz) {
            (Some(tz_offset), Some(t)) => {
                let tz_parsed = Tz::from_offset(tz_offset as i64)?;
                let mut ts = chrono_datetime(
                    &tz_parsed,
                    t.year(),
                    t.month(),
                    t.day(),
                    t.hour(),
                    t.minute(),
                    t.second(),
                    t.micro(),
                )
                .ok()?;
                ts = ts.with_timezone(&ctx.cfg.tz);
                Some(
                    Time::try_from_chrono_datetime(ctx, ts.naive_local(), time_type, fsp as i8)
                        .ok()?,
                )
            }
            _ => time_without_tz,
        }
    }

    pub fn parse_from_decimal(
        ctx: &mut EvalContext,
        input: &Decimal,
        time_type: TimeType,
        fsp: u8,
        round: bool,
    ) -> Option<Time> {
        let decimal_as_string = input.to_string();
        let (components, _) = split_components_with_tz(decimal_as_string.as_str())?;
        match components.len() {
            1 | 2 => {
                let result: i64 = components[0].convert(ctx).ok()?;
                let whole_time = parse_from_i64(ctx, result, time_type, fsp)?;
                let mut whole = [
                    whole_time.get_year(),
                    whole_time.get_month(),
                    whole_time.get_day(),
                    whole_time.get_hour(),
                    whole_time.get_minute(),
                    whole_time.get_second(),
                    0,
                ];

                let (carry, frac) = if let Some(frac) = components.get(1) {
                    // If we have a fractional part,
                    // we expect the `whole` is in format: `yymmddhhmmss/yyyymmddhhmmss`,
                    // which match the `whole` part length from 9 to 14.
                    // Otherwise, the fractional part is meaningless.
                    if components[0].len() >= 9 && components[0].len() <= 14 {
                        parse_frac(frac, fsp, round)?
                    } else {
                        (false, 0)
                    }
                } else {
                    (false, 0)
                };

                whole[6] = frac;
                let mut parts = whole;
                if carry {
                    round_components(&mut parts)?;
                }

                Time::from_slice(ctx, &parts, time_type, fsp)
            }
            _ => None,
        }
    }

    /// Try to parse a `i64` into a `Time` with the given type and fsp
    pub fn parse_from_i64(
        ctx: &mut EvalContext,
        input: i64,
        time_type: TimeType,
        fsp: u8,
    ) -> Option<Time> {
        if input == 0 {
            return Time::zero(ctx, fsp as i8, time_type).ok();
        }
        // NOTE: These numbers can be consider as strings
        // The parser eats two digits each time from the end of string,
        // and fill it into `Time` with reversed order.
        // Port from: https://github.com/pingcap/tidb/blob/b1aad071489619998e4caefd235ed01f179c2db2/types/time.go#L1263
        let aligned = match input {
            101..=691_231 => (input + 20_000_000) * 1_000_000,
            700_101..=991_231 => (input + 19_000_000) * 1_000_000,
            991_232..=99_991_231 => input * 1_000_000,
            101_000_000..=691_231_235_959 => input + 20_000_000_000_000,
            700_101_000_000..=991_231_235_959 => input + 19_000_000_000_000,
            1_000_000_000_000..=std::i64::MAX => input,
            _ => return None,
        };

        Time::from_aligned_i64(ctx, aligned, time_type, fsp as i8).ok()
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
    pub fn parse_datetime(
        ctx: &mut EvalContext,
        input: &str,
        fsp: i8,
        round: bool,
    ) -> Result<Time> {
        Self::parse(ctx, input, TimeType::DateTime, fsp, round)
    }
    pub fn parse_date(ctx: &mut EvalContext, input: &str) -> Result<Time> {
        Self::parse(ctx, input, TimeType::Date, 0, false)
    }
    pub fn parse_timestamp(
        ctx: &mut EvalContext,
        input: &str,
        fsp: i8,
        round: bool,
    ) -> Result<Time> {
        Self::parse(ctx, input, TimeType::Timestamp, fsp, round)
    }
    pub fn parse_from_i64(
        ctx: &mut EvalContext,
        input: i64,
        time_type: TimeType,
        fsp: i8,
    ) -> Result<Time> {
        parser::parse_from_i64(ctx, input, time_type, check_fsp(fsp)?)
            .ok_or_else(|| Error::incorrect_datetime_value(input))
    }
    pub fn parse_from_decimal(
        ctx: &mut EvalContext,
        input: &Decimal,
        time_type: TimeType,
        fsp: i8,
        round: bool,
    ) -> Result<Time> {
        parser::parse_from_decimal(ctx, input, time_type, check_fsp(fsp)?, round)
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

impl Default for TimeArgs {
    fn default() -> Self {
        TimeArgs {
            year: 0,
            month: 0,
            day: 0,
            hour: 0,
            minute: 0,
            second: 0,
            micro: 0,
            fsp: 0,
            time_type: TimeType::Date,
        }
    }
}

impl TimeArgs {
    fn check(mut self, ctx: &mut EvalContext) -> Option<TimeArgs> {
        self.fsp = check_fsp(self.fsp).ok()? as i8;
        let (fsp, time_type) = (self.fsp, self.time_type);
        match self.time_type {
            TimeType::Date | TimeType::DateTime => self.check_datetime(ctx),
            TimeType::Timestamp => self.check_timestamp(ctx),
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
        if !(MIN_TIMESTAMP..=MAX_TIMESTAMP).contains(&ts) {
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

    /// Construct a `Time` via a number in format: yyyymmddhhmmss
    fn from_aligned_i64(
        ctx: &mut EvalContext,
        input: i64,
        time_type: TimeType,
        fsp: i8,
    ) -> Result<Time> {
        let ymd = (input / 1_000_000) as u32;
        let hms = (input % 1_000_000) as u32;

        let year = ymd / 10_000;
        let md = ymd % 10_000_u32;
        let month = md / 100;
        let day = md % 100;

        let hour = hms / 10_000;
        let ms = hms % 10_000;
        let minute = ms / 100;
        let second = ms % 100;

        Time::new(
            ctx,
            TimeArgs {
                year,
                month,
                day,
                hour,
                minute,
                second,
                micro: 0,
                time_type,
                fsp,
            },
        )
    }

    fn into_array(self) -> [u32; 7] {
        let mut slice = [0; 7];
        slice[0] = self.year();
        slice[1] = self.month();
        slice[2] = self.day();
        slice[3] = self.hour();
        slice[4] = self.minute();
        slice[5] = self.second();
        slice[6] = self.micro();
        slice
    }

    fn try_from_chrono_datetime<T: Datelike + Timelike>(
        ctx: &mut EvalContext,
        datetime: T,
        time_type: TimeType,
        fsp: i8,
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

    fn try_into_chrono_datetime(self, ctx: &mut EvalContext) -> Result<DateTime<Tz>> {
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

    fn try_into_chrono_naive_datetime(self) -> Result<NaiveDateTime> {
        chrono_naive_datetime(
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
        time.set_tt(time_type);
        time
    }

    fn new(ctx: &mut EvalContext, config: TimeArgs) -> Result<Time> {
        let unchecked_time = Self::unchecked_new(config.clone());
        let mut checked = config
            .check(ctx)
            .ok_or_else(|| Error::incorrect_datetime_value(unchecked_time))?;
        if checked.time_type == TimeType::Date {
            checked.hour = 0;
            checked.minute = 0;
            checked.second = 0;
            checked.micro = 0;
            checked.fsp = 0;
        }
        Ok(Self::unchecked_new(checked))
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

    pub fn zero(ctx: &mut EvalContext, fsp: i8, time_type: TimeType) -> Result<Self> {
        Time::new(ctx, TimeArgs::zero(fsp, time_type))
    }

    #[inline]
    pub fn is_leap_year(self) -> bool {
        is_leap_year(self.year())
    }

    #[inline]
    pub fn last_day_of_month(self) -> u32 {
        last_day_of_month(self.year(), self.month())
    }

    pub fn last_date_of_month(mut self) -> Option<Self> {
        if self.invalid_zero() {
            return None;
        }
        self.set_day(self.last_day_of_month());
        self.set_hour(0);
        self.set_minute(0);
        self.set_second(0);
        self.set_micro(0);
        Some(self)
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
    pub fn maximize_fsp(&mut self) {
        self.set_fsp(super::MAX_FSP as u8);
    }

    #[inline]
    pub fn minimize_fsp(&mut self) {
        self.set_fsp(super::MIN_FSP as u8);
    }

    #[inline]
    pub fn get_time_type(self) -> TimeType {
        let ft = self.get_fsp_tt();

        if ft >> 1 == 0b111 {
            TimeType::Date
        } else if ft & 1 == 0 {
            TimeType::DateTime
        } else {
            TimeType::Timestamp
        }
    }

    #[inline]
    pub fn set_time_type(&mut self, time_type: TimeType) -> Result<()> {
        if self.get_time_type() != time_type && time_type == TimeType::Date {
            self.set_hour(0);
            self.set_minute(0);
            self.set_second(0);
            self.set_micro(0);
            self.set_fsp(0);
        }
        if self.get_time_type() != time_type && time_type == TimeType::Timestamp {
            return Err(box_err!("can not convert datetime/date to timestamp"));
        }
        self.set_tt(time_type);
        Ok(())
    }

    #[inline]
    fn set_tt(&mut self, time_type: TimeType) {
        let ft = self.get_fsp_tt();
        let mask = match time_type {
            TimeType::Date => 0b1110,
            TimeType::DateTime => ft & 0b1110,
            TimeType::Timestamp => ft | 1,
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

        if time_type == TimeType::Timestamp {
            let utc = chrono_datetime(&Utc, year, month, day, hour, minute, second, micro)?;
            let timestamp = ctx.cfg.tz.from_utc_datetime(&utc.naive_utc());
            Time::try_from_chrono_datetime(ctx, timestamp.naive_local(), time_type, fsp as i8)
        } else {
            Ok(Time::unchecked_new(TimeArgs {
                year,
                month,
                day,
                hour,
                minute,
                second,
                micro,
                fsp: fsp as i8,
                time_type,
            }))
        }
    }

    pub fn to_packed_u64(mut self, ctx: &mut EvalContext) -> Result<u64> {
        if self.is_zero() {
            return Ok(0);
        }

        if self.get_time_type() == TimeType::Timestamp {
            let ts = self.try_into_chrono_datetime(ctx)?;
            self = Time::try_from_chrono_datetime(
                ctx,
                ts.naive_utc(),
                self.get_time_type(),
                self.fsp() as i8,
            )?;
        }

        let ymd =
            ((u64::from(self.year()) * 13 + u64::from(self.month())) << 5) | u64::from(self.day());
        let hms = (u64::from(self.hour()) << 12)
            | (u64::from(self.minute()) << 6)
            | u64::from(self.second());

        Ok((((ymd << 17) | hms) << 24) | u64::from(self.micro()))
    }

    pub fn from_duration(
        ctx: &mut EvalContext,
        duration: Duration,
        time_type: TimeType,
    ) -> Result<Self> {
        let dur = chrono::Duration::nanoseconds(duration.to_nanos());

        let time = Utc::today()
            .and_hms(0, 0, 0)
            .checked_add_signed(dur)
            .map(|utc| utc.with_timezone(&ctx.cfg.tz));

        let time = time.ok_or::<Error>(box_err!("parse from duration {} overflows", duration))?;

        Time::try_from_chrono_datetime(ctx, time, time_type, duration.fsp() as i8)
    }

    pub fn from_year(
        ctx: &mut EvalContext,
        year: u32,
        fsp: i8,
        time_type: TimeType,
    ) -> Result<Self> {
        Time::new(
            ctx,
            TimeArgs {
                year,
                month: 0,
                day: 0,
                hour: 0,
                minute: 0,
                second: 0,
                micro: 0,
                fsp,
                time_type,
            },
        )
    }

    pub fn round_frac(mut self, ctx: &mut EvalContext, fsp: i8) -> Result<Self> {
        let time_type = self.get_time_type();
        if time_type == TimeType::Date || self.is_zero() {
            return Ok(self);
        }

        let fsp = check_fsp(fsp)?;
        if fsp > self.fsp() {
            self.set_fsp(fsp);
            return Ok(self);
        }
        let (carry, frac) = round_frac(self.micro(), fsp);
        let mut slice = self.into_array();
        slice[6] = frac;

        // If we have cases like:
        //   1. 2012-0-1  23:59:59.999      (fsp: 2)
        //   2. 2012-4-31 23:59:59.999      (fsp: 2)
        // 0000-00-00.00 is expected.
        if carry && round_components(&mut slice).is_none() {
            return Time::new(ctx, TimeArgs::zero(fsp as i8, time_type));
        }

        Time::from_slice(ctx, &slice, time_type, fsp)
            .ok_or_else(|| Error::incorrect_datetime_value(self))
    }

    pub fn normalized(self, ctx: &mut EvalContext) -> Result<Self> {
        if self.get_time_type() == TimeType::Timestamp {
            return Ok(self);
        }

        if self.day() > self.last_day_of_month() || self.month() == 0 || self.day() == 0 {
            let date = if self.month() == 0 {
                (self.year() >= 1).ok_or(Error::incorrect_datetime_value(self))?;
                NaiveDate::from_ymd(self.year() as i32 - 1, 12, 1)
            } else {
                NaiveDate::from_ymd(self.year() as i32, self.month(), 1)
            } + chrono::Duration::days(i64::from(self.day()) - 1);
            let datetime = NaiveDateTime::new(
                date,
                NaiveTime::from_hms_micro(self.hour(), self.minute(), self.second(), self.micro()),
            );
            return Time::try_from_chrono_datetime(
                ctx,
                datetime,
                self.get_time_type(),
                self.fsp() as i8,
            );
        }

        Ok(self)
    }

    pub fn checked_add(self, ctx: &mut EvalContext, rhs: Duration) -> Option<Time> {
        let normalized = self.normalized(ctx).ok()?;
        let duration = chrono::Duration::nanoseconds(rhs.to_nanos());
        if self.get_time_type() == TimeType::Timestamp {
            let datetime = normalized
                .try_into_chrono_datetime(ctx)
                .ok()
                .and_then(|datetime| datetime.checked_add_signed(duration))?;
            Time::try_from_chrono_datetime(ctx, datetime, TimeType::Timestamp, self.fsp() as i8)
        } else {
            let naive = normalized
                .try_into_chrono_naive_datetime()
                .ok()
                .and_then(|datetime| datetime.checked_add_signed(duration))?;
            Time::try_from_chrono_datetime(ctx, naive, TimeType::Timestamp, self.fsp() as i8)
        }
        .ok()
    }

    pub fn checked_sub(self, ctx: &mut EvalContext, rhs: Duration) -> Option<Time> {
        let normalized = self.normalized(ctx).ok()?;
        let duration = chrono::Duration::nanoseconds(rhs.to_nanos());
        if self.get_time_type() == TimeType::Timestamp {
            let datetime = normalized
                .try_into_chrono_datetime(ctx)
                .ok()
                .and_then(|datetime| datetime.checked_sub_signed(duration))?;
            Time::try_from_chrono_datetime(ctx, datetime, TimeType::Timestamp, self.fsp() as i8)
        } else {
            let naive = normalized
                .try_into_chrono_naive_datetime()
                .ok()
                .and_then(|datetime| datetime.checked_sub_signed(duration))?;
            Time::try_from_chrono_datetime(ctx, naive, TimeType::Timestamp, self.fsp() as i8)
        }
        .ok()
    }

    pub fn date_diff(mut self, mut other: Self) -> Option<i64> {
        if self.invalid_zero() || other.invalid_zero() {
            return None;
        }
        self.set_time_type(TimeType::Date).ok()?;
        other.set_time_type(TimeType::Date).ok()?;

        let lhs = self.try_into_chrono_naive_datetime().ok()?;
        let rhs = other.try_into_chrono_naive_datetime().ok()?;
        Some(lhs.signed_duration_since(rhs).num_days())
    }

    pub fn ordinal(self) -> i32 {
        if self.month() == 0 {
            return self.day() as i32 - 32;
        }
        ((1..self.month()).fold(0, |acc, month| acc + last_day_of_month(self.year(), month))
            + self.day()) as i32
    }

    pub fn weekday(self) -> Weekday {
        let date = if self.month() == 0 {
            NaiveDate::from_ymd(self.year() as i32 - 1, 12, 1)
        } else {
            NaiveDate::from_ymd(self.year() as i32, self.month(), 1)
        } + chrono::Duration::days(i64::from(self.day()) - 1);
        date.weekday()
    }

    fn write_date_format_segment(self, b: char, output: &mut String) -> Result<()> {
        match b {
            'b' => {
                let month = self.month();
                if month == 0 {
                    return Err(box_err!("invalid time format"));
                } else {
                    output.push_str(MONTH_NAMES_ABBR[(month - 1) as usize]);
                }
            }
            'M' => {
                let month = self.month();
                if month == 0 {
                    return Err(box_err!("invalid time format"));
                } else {
                    output.push_str(MONTH_NAMES[(month - 1) as usize]);
                }
            }
            'm' => {
                write!(output, "{:02}", self.month()).unwrap();
            }
            'c' => {
                write!(output, "{}", self.month()).unwrap();
            }
            'D' => {
                write!(output, "{}{}", self.day(), self.abbr_day_of_month()).unwrap();
            }
            'd' => {
                write!(output, "{:02}", self.day()).unwrap();
            }
            'e' => {
                write!(output, "{}", self.day()).unwrap();
            }
            'j' => {
                write!(output, "{:03}", self.days()).unwrap();
            }
            'H' => {
                write!(output, "{:02}", self.hour()).unwrap();
            }
            'k' => {
                write!(output, "{}", self.hour()).unwrap();
            }
            'h' | 'I' => {
                let t = self.hour();
                if t == 0 || t == 12 {
                    output.push_str("12");
                } else {
                    write!(output, "{:02}", t % 12).unwrap();
                }
            }
            'l' => {
                let t = self.hour();
                if t == 0 || t == 12 {
                    output.push_str("12");
                } else {
                    write!(output, "{}", t % 12).unwrap();
                }
            }
            'i' => {
                write!(output, "{:02}", self.minute()).unwrap();
            }
            'p' => {
                let hour = self.hour();
                if (hour / 12) % 2 == 0 {
                    output.push_str("AM")
                } else {
                    output.push_str("PM")
                }
            }
            'r' => {
                let h = self.hour();
                if h == 0 {
                    write!(
                        output,
                        "{:02}:{:02}:{:02} AM",
                        12,
                        self.minute(),
                        self.second()
                    )
                    .unwrap();
                } else if h == 12 {
                    write!(
                        output,
                        "{:02}:{:02}:{:02} PM",
                        12,
                        self.minute(),
                        self.second()
                    )
                    .unwrap();
                } else if h < 12 {
                    write!(
                        output,
                        "{:02}:{:02}:{:02} AM",
                        h,
                        self.minute(),
                        self.second()
                    )
                    .unwrap();
                } else {
                    write!(
                        output,
                        "{:02}:{:02}:{:02} PM",
                        h - 12,
                        self.minute(),
                        self.second()
                    )
                    .unwrap();
                }
            }
            'T' => {
                write!(
                    output,
                    "{:02}:{:02}:{:02}",
                    self.hour(),
                    self.minute(),
                    self.second()
                )
                .unwrap();
            }
            'S' | 's' => {
                write!(output, "{:02}", self.second()).unwrap();
            }
            'f' => {
                write!(output, "{:06}", self.micro()).unwrap();
            }
            'U' => {
                let w = self.week(WeekMode::from_bits_truncate(0));
                write!(output, "{:02}", w).unwrap();
            }
            'u' => {
                let w = self.week(WeekMode::from_bits_truncate(1));
                write!(output, "{:02}", w).unwrap();
            }
            'V' => {
                let w = self.week(WeekMode::from_bits_truncate(2));
                write!(output, "{:02}", w).unwrap();
            }
            'v' => {
                let (_, w) = self.year_week(WeekMode::from_bits_truncate(3));
                write!(output, "{:02}", w).unwrap();
            }
            'a' => {
                output.push_str(self.weekday().name_abbr());
            }
            'W' => {
                output.push_str(self.weekday().name());
            }
            'w' => {
                write!(output, "{}", self.weekday().num_days_from_sunday()).unwrap();
            }
            'X' => {
                let (year, _) = self.year_week(WeekMode::from_bits_truncate(2));
                if year < 0 {
                    write!(output, "{}", u32::max_value()).unwrap();
                } else {
                    write!(output, "{:04}", year).unwrap();
                }
            }
            'x' => {
                let (year, _) = self.year_week(WeekMode::from_bits_truncate(3));
                if year < 0 {
                    write!(output, "{}", u32::max_value()).unwrap();
                } else {
                    write!(output, "{:04}", year).unwrap();
                }
            }
            'Y' => {
                write!(output, "{:04}", self.year()).unwrap();
            }
            'y' => {
                write!(output, "{:02}", self.year() % 100).unwrap();
            }
            _ => output.push(b),
        }
        Ok(())
    }

    pub fn date_format(self, layout: &str) -> Result<String> {
        let mut ret = String::new();
        let mut pattern_match = false;
        for b in layout.chars() {
            if pattern_match {
                self.write_date_format_segment(b, &mut ret)?;
                pattern_match = false;
                continue;
            }
            if b == '%' {
                pattern_match = true;
            } else {
                ret.push(b);
            }
        }
        Ok(ret)
    }

    /// Converts a `DateTime` to printable string representation
    #[inline]
    pub fn to_numeric_string(self) -> String {
        let mut buffer = String::with_capacity(15);
        write!(&mut buffer, "{}", self.date_format("%Y%m%d").unwrap()).unwrap();
        if self.get_time_type() != TimeType::Date {
            write!(&mut buffer, "{}", self.date_format("%H%i%S").unwrap()).unwrap();
        }
        let fsp = usize::from(self.fsp());
        if fsp > 0 {
            write!(
                &mut buffer,
                ".{:0width$}",
                self.micro() / TEN_POW[MICRO_WIDTH - fsp],
                width = fsp
            )
            .unwrap();
        }
        buffer
    }

    pub fn parse_fsp(s: &str) -> i8 {
        s.rfind('.').map_or(super::DEFAULT_FSP, |idx| {
            std::cmp::min((s.len() - idx - 1) as i8, super::MAX_FSP)
        })
    }

    pub fn invalid_zero(self) -> bool {
        self.month() == 0 || self.day() == 0
    }

    pub fn from_days(ctx: &mut EvalContext, daynr: u32) -> Result<Self> {
        let (year, month, day) = Time::get_date_from_daynr(daynr);
        let time_args = TimeArgs {
            year,
            month,
            day,
            ..Default::default()
        };
        Time::new(ctx, time_args)
    }

    // Changes a daynr to year, month and day, daynr 0 is returned as date 00.00.00
    #[inline]
    fn get_date_from_daynr(daynr: u32) -> (u32, u32, u32) {
        if daynr <= 365 || daynr >= 3_652_425 {
            return (0, 0, 0);
        }

        let mut year = daynr * 100 / 36525;
        let temp = (((year - 1) / 100 + 1) * 3) / 4;
        let mut day_of_year = daynr - year * 365 - (year - 1) / 4 + temp;

        let mut days_in_year = if is_leap_year(year) { 366 } else { 365 };
        while day_of_year > days_in_year {
            day_of_year -= days_in_year;
            year += 1;
            days_in_year = if is_leap_year(year) { 366 } else { 365 };
        }

        let mut month = 1;
        for each_month in 1..=12 {
            let last_day_of_month = last_day_of_month(year, each_month);
            if day_of_year <= last_day_of_month {
                break;
            }
            month += 1;
            day_of_year -= last_day_of_month;
        }

        let day = day_of_year;

        (year, month, day)
    }
}

impl ConvertTo<f64> for Time {
    /// This function should not return err,
    /// if it return err, then the err is because of bug.
    #[inline]
    fn convert(&self, _: &mut EvalContext) -> Result<f64> {
        if self.is_zero() {
            return Ok(0f64);
        }
        let r = self.to_numeric_string().parse::<f64>();
        debug_assert!(r.is_ok());
        Ok(r?)
    }
}

impl ConvertTo<Decimal> for Time {
    // Port from TiDB's Time::ToNumber
    #[inline]
    fn convert(&self, _: &mut EvalContext) -> Result<Decimal> {
        if self.is_zero() {
            return Ok(0.into());
        }

        self.to_numeric_string().parse()
    }
}

impl ConvertTo<Duration> for Time {
    /// Port from TiDB's Time::ConvertToDuration
    #[inline]
    fn convert(&self, _: &mut EvalContext) -> Result<Duration> {
        if self.is_zero() {
            return Ok(Duration::zero());
        }
        let seconds = i64::from(self.hour() * 3600 + self.minute() * 60 + self.second());
        // `microsecond` returns the number of microseconds since the whole non-leap second.
        // Such as for 2019-09-22 07:21:22.670936103 UTC,
        // it will return 670936103.
        let microsecond = i64::from(self.micro());
        Duration::from_micros(seconds * 1_000_000 + microsecond, self.fsp() as i8)
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

impl Hash for Time {
    fn hash<H: Hasher>(&self, state: &mut H) {
        let mut a = *self;
        a.set_fsp_tt(0);
        a.0.hash(state);
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

impl<T: BufferWriter> TimeEncoder for T {}

/// Time Encoder for Chunk format
pub trait TimeEncoder: NumberEncoder {
    #[inline]
    fn write_time(&mut self, v: Time) -> Result<()> {
        Ok(self.write_u64_le(v.0)?)
    }
}

pub trait TimeDatumPayloadChunkEncoder: TimeEncoder {
    #[inline]
    fn write_time_to_chunk_by_datum_payload_int(
        &mut self,
        mut src_payload: &[u8],
        ctx: &mut EvalContext,
        field_type: &FieldType,
    ) -> Result<()> {
        let time = src_payload.read_time_int(ctx, field_type)?;
        self.write_time(time)
    }

    #[inline]
    fn write_time_to_chunk_by_datum_payload_varint(
        &mut self,
        mut src_payload: &[u8],
        ctx: &mut EvalContext,
        field_type: &FieldType,
    ) -> Result<()> {
        let time = src_payload.read_time_varint(ctx, field_type)?;
        self.write_time(time)
    }
}

impl<T: BufferWriter> TimeDatumPayloadChunkEncoder for T {}

pub trait TimeDecoder: NumberDecoder {
    #[inline]
    fn read_time_int(&mut self, ctx: &mut EvalContext, field_type: &FieldType) -> Result<Time> {
        let v = self.read_u64()?;
        let fsp = field_type.as_accessor().decimal() as i8;
        let time_type = field_type.as_accessor().tp().try_into()?;
        Time::from_packed_u64(ctx, v, time_type, fsp)
    }

    #[inline]
    fn read_time_varint(&mut self, ctx: &mut EvalContext, field_type: &FieldType) -> Result<Time> {
        let v = self.read_var_u64()?;
        let fsp = field_type.as_accessor().decimal() as i8;
        let time_type = field_type.as_accessor().tp().try_into()?;
        Time::from_packed_u64(ctx, v, time_type, fsp)
    }

    #[inline]
    fn read_time_from_chunk(&mut self) -> Result<Time> {
        let t = self.read_u64_le()?;
        Ok(Time(t))
    }
}

impl<T: BufferReader> TimeDecoder for T {}

impl crate::codec::data_type::AsMySQLBool for Time {
    #[inline]
    fn as_mysql_bool(&self, _context: &mut crate::expr::EvalContext) -> crate::codec::Result<bool> {
        Ok(!self.is_zero())
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
    fn test_parse_from_i64() -> Result<()> {
        let cases = vec![
            ("0000-00-00 00:00:00", 0),
            ("2000-01-01 00:00:00", 101),
            ("2045-00-00 00:00:00", 450_000),
            ("2059-12-31 00:00:00", 591_231),
            ("1970-01-01 00:00:00", 700_101),
            ("1999-12-31 00:00:00", 991_231),
            ("1000-01-00 00:00:00", 10_000_100),
            ("2000-01-01 00:00:00", 101_000_000),
            ("2069-12-31 23:59:59", 691_231_235_959),
            ("1970-01-01 00:00:00", 700_101_000_000),
            ("1999-12-31 23:59:59", 991_231_235_959),
            ("0100-00-00 00:00:00", 1_000_000_000_000),
            ("1000-01-01 00:00:00", 10_000_101_000_000),
            ("1999-01-01 00:00:00", 19_990_101_000_000),
        ];
        let mut ctx = EvalContext::default();
        for (expected, input) in cases {
            let actual = Time::parse_from_i64(&mut ctx, input, TimeType::DateTime, 0)?;
            assert_eq!(actual.to_string(), expected);
        }

        let should_fail = vec![-1111, 1, 100, 700_100, 100_000_000, 100_000_101_000_000];
        for case in should_fail {
            assert!(Time::parse_from_i64(&mut ctx, case, TimeType::DateTime, 0).is_err());
        }
        Ok(())
    }

    #[test]
    fn test_parse_from_decimal() -> Result<()> {
        let cases = vec![
            ("2000-03-05 00:00:00", "305", 0),
            ("2000-12-03 00:00:00", "1203", 0),
            ("2003-12-05 00:00:00.0", "31205", 1),
            ("2007-01-18 00:00:00.00", "070118", 2),
            ("0101-12-09 00:00:00.000", "1011209.333", 3),
            ("2017-01-18 00:00:00.0000", "20170118.123", 4),
            ("2012-12-31 11:30:45.12335", "121231113045.123345", 5),
            ("2012-12-31 11:30:45.123345", "20121231113045.123345", 6),
            ("2012-12-31 11:30:46.00000", "121231113045.9999999", 5),
            ("2017-01-05 08:40:59.5756", "170105084059.575601", 4),
        ];
        let mut ctx = EvalContext::default();
        for (expected, input, fsp) in cases {
            let input: Decimal = input.parse().unwrap();
            let actual = Time::parse_from_decimal(&mut ctx, &input, TimeType::DateTime, fsp, true)?;
            assert_eq!(actual.to_string(), expected);
        }

        let should_fail = vec![
            "201705051315111.22",
            "2011110859.1111",
            "2011110859.1111",
            "191203081.1111",
            "43128.121105",
        ];
        for case in should_fail {
            let case: Decimal = case.parse().unwrap();
            assert!(
                Time::parse_from_decimal(&mut ctx, &case, TimeType::DateTime, 0, true).is_err()
            );
        }
        Ok(())
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
            ("2019-09-16", "19-09-16 10:11:12"),
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
            ("2013-05-28", "1305280512.000000000000"),
            ("0000-00-00", "00:00:00"),
        ];

        for (expected, actual) in cases {
            let date = Time::parse_date(&mut ctx, actual)?;
            assert_eq!(date.hour(), 0);
            assert_eq!(date.minute(), 0);
            assert_eq!(date.second(), 0);
            assert_eq!(date.micro(), 0);
            assert_eq!(date.fsp(), 0);
            assert_eq!(expected, date.to_string());
        }

        let should_fail = vec![
            ("11-12-13 T 12:34:56"),
            ("11:12:13 T12:34:56"),
            ("11:12:13 T12:34:56.12"),
            ("11:12:13T25:34:56.12"),
            ("2011-12-13t12:34:56.12"),
            ("11:12:13T23:61:56.12"),
            ("11:12:13T23:59:89.12"),
            ("11121311121.1"),
            ("1201012736"),
            ("1201012736.0"),
            ("111213111.1"),
            ("11121311.1"),
            ("1112131.1"),
            ("111213.1"),
            ("111213.1"),
            ("11121.1"),
            ("1112"),
        ];

        for case in should_fail {
            assert!(Time::parse_date(&mut ctx, case).is_err());
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
            ("2020-02-29 10:00:00", "20200229100000", 0, false),
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
            ("2013-05-28 05:12:00", "1305280512.000000000000", 0, true),
            ("2013-05-28 05:12:00", "1305280512", 0, true),
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
            ("0000-00-00 00:00:00", "00:00:00", 0, false),
            ("2020-12-23 15:59:10", "2020--12+-23 15:^59:-10", 0, false),
            ("2020-12-23 15:59:23", "2020-12-23 15:59:23Z", 0, false),
            ("2020-12-23 07:59:23", "2020-12-23 15:59:23+0800", 0, false),
            ("2020-12-23 23:59:23", "2020-12-23 15:59:23-08", 0, false),
            ("2020-12-23 07:59:23", "2020-12-23 15:59:23+08:00", 0, false),
        ];
        for (expected, actual, fsp, round) in cases {
            assert_eq!(
                expected,
                Time::parse_datetime(&mut ctx, actual, fsp, round)?.to_string()
            );
        }

        let should_fail = vec![
            ("11-12-13 T 12:34:56", 0),
            ("11:12:13 T12:34:56", 0),
            ("11:12:13 T12:34:56.12", 7),
            ("11:12:13T25:34:56.12", 7),
            ("11:12:13T23:61:56.12", 7),
            ("11:12:13T23:59:89.12", 7),
            ("11121311121.1", 2),
            ("1201012736", 2),
            ("1201012736.0", 2),
            ("111213111.1", 2),
            ("11121311.1", 2),
            ("1112131.1", 2),
            ("111213.1", 2),
            ("111213.1", 2),
            ("11121.1", 2),
            ("1112", 2),
        ];

        for (case, fsp) in should_fail {
            assert!(Time::parse_datetime(&mut ctx, case, fsp, false).is_err());
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
            ("2013-05-28 05:12:00", "1305280512.000000000000", 0, true),
            ("2013-05-28 05:12:00", "1305280512", 0, true),
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
            ("0000-00-00 00:00:00", "00:00:00", 0, false),
        ];
        for (expected, actual, fsp, round) in cases {
            assert_eq!(
                expected,
                Time::parse_timestamp(&mut ctx, actual, fsp, round)?.to_string()
            );
        }
        Ok(())
    }

    #[test]
    fn test_parse_time_with_tz() -> Result<()> {
        let ctx_with_tz = |tz: &str| {
            let mut cfg = EvalConfig::default();
            let raw = tz.as_bytes();
            // brutally turn timezone in format +08:00 into offset in minute
            let offset = if raw[0] == b'-' { -1 } else { 1 }
                * ((raw[1] - b'0') as i64 * 10 + (raw[2] - b'0') as i64)
                * 60
                + ((raw[4] - b'0') as i64 * 10 + (raw[5] - b'0') as i64);
            cfg.set_time_zone_by_offset(offset * 60).unwrap();
            let warnings = cfg.new_eval_warnings();
            EvalContext {
                cfg: Arc::new(cfg),
                warnings,
            }
        };
        struct Case {
            tz: &'static str,
            t: &'static str,
            r: Option<&'static str>,
            tp: TimeType,
        }
        let cases = vec![
            Case {
                tz: "+00:00",
                t: "2020-10-10T10:10:10Z",
                r: Some("2020-10-10 10:10:10.000000"),
                tp: TimeType::DateTime,
            },
            Case {
                tz: "+00:00",
                t: "2020-10-10T10:10:10+",
                r: None,
                tp: TimeType::DateTime,
            },
            Case {
                tz: "+00:00",
                t: "2020-10-10T10:10:10+14:01",
                r: None,
                tp: TimeType::DateTime,
            },
            Case {
                tz: "+00:00",
                t: "2020-10-10T10:10:10-00:00",
                r: None,
                tp: TimeType::DateTime,
            },
            Case {
                tz: "-08:00",
                t: "2020-10-10T10:10:10-08",
                r: Some("2020-10-10 10:10:10.000000"),
                tp: TimeType::DateTime,
            },
            Case {
                tz: "+08:00",
                t: "2020-10-10T10:10:10+08:00",
                r: Some("2020-10-10 10:10:10.000000"),
                tp: TimeType::DateTime,
            },
            Case {
                tz: "+08:00",
                t: "2020-10-10T10:10:10+08:00",
                r: Some("2020-10-10 10:10:10.000000"),
                tp: TimeType::Timestamp,
            },
        ];
        let mut result: Vec<Option<String>> = vec![];
        for Case { tz, t, r: _, tp } in &cases {
            let mut ctx = ctx_with_tz(tz);
            let parsed = Time::parse(&mut ctx, t, *tp, 6, true);
            match parsed {
                Ok(p) => result.push(Some(p.to_string())),
                Err(_) => result.push(None),
            }
        }
        for (a, b) in result.into_iter().zip(cases) {
            match (a, b.r) {
                (Some(a), Some(b)) => assert_eq!(a.as_str(), b),
                (None, None) => {}
                _ => {
                    return Err(Error::invalid_time_format(b.t));
                }
            }
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
            assert_eq!(expected, Time::parse_date(&mut ctx, actual)?.to_string());
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
                Time::parse_datetime(&mut ctx, case, 0, false)?.to_string()
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
                Time::parse_timestamp(&mut ctx, case, 0, false)?.to_string()
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
                Time::parse_timestamp(&mut ctx, timestamp, 0, false)?.to_string()
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

        let _ = Time::parse_datetime(&mut ctx, "0000-00-00 00:00:00", 0, false)?;

        assert!(ctx.warnings.warning_cnt > 0);

        // Enable both NO_ZERO_DATE and STRICT_MODE.
        // If zero-date is encountered, an error is returned.
        let mut ctx = EvalContext::from(TimeEnv {
            no_zero_date: true,
            strict_mode: true,
            ..TimeEnv::default()
        });

        assert!(Time::parse_datetime(&mut ctx, "0000-00-00 00:00:00", 0, false).is_err());

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
            Time::parse_datetime(&mut ctx, "0000-00-00 00:00:00", 0, false)?.to_string()
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
            assert!(Time::parse_datetime(&mut ctx, case, 0, false).is_err());
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

            let _ = Time::parse_datetime(&mut ctx, case, 0, false)?;

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
            Time::parse_datetime(&mut ctx, "0000-00-00 00:00:00", 0, false)?.to_string()
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
            assert!(Time::parse_datetime(&mut ctx, case, 0, false).is_err());
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
            let time = Time::parse_datetime(&mut ctx, case, fsp, false)?;

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

                let time = Time::parse_timestamp(&mut ctx, case, fsp, false)?;

                let packed = time.to_packed_u64(&mut ctx)?;
                let reverted_datetime =
                    Time::from_packed_u64(&mut ctx, packed, TimeType::Timestamp, fsp)?;

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
            let left = Time::parse_datetime(&mut ctx, left, MAX_FSP, false)?;
            let right = Time::parse_datetime(&mut ctx, right, MAX_FSP, false)?;
            assert_eq!(expected, left.cmp(&right));
        }
        Ok(())
    }

    #[test]
    fn test_from_duration() -> Result<()> {
        let cases = vec!["11:30:45.123456", "-35:30:46"];
        for case in cases {
            let mut ctx = EvalContext::default();
            let duration = Duration::parse(&mut ctx, case, MAX_FSP)?;

            let actual = Time::from_duration(&mut ctx, duration, TimeType::DateTime)?;
            let today = actual
                .try_into_chrono_datetime(&mut ctx)?
                .checked_sub_signed(chrono::Duration::nanoseconds(duration.to_nanos()))
                .unwrap();

            let now = Utc::now();
            assert_eq!(today.year(), now.year());
            assert_eq!(today.month(), now.month());
            assert_eq!(today.day(), now.day());
            assert_eq!(today.hour(), 0);
            assert_eq!(today.minute(), 0);
            assert_eq!(today.second(), 0);
        }
        Ok(())
    }

    #[test]
    fn test_round_frac() -> Result<()> {
        let cases = vec![
            ("121231113045.123345", 6, "2012-12-31 11:30:45.123345"),
            ("121231113045.999999", 6, "2012-12-31 11:30:45.999999"),
            ("121231113045.999999", 5, "2012-12-31 11:30:46.00000"),
            ("2012-12-31 11:30:45.123456", 4, "2012-12-31 11:30:45.1235"),
            (
                "2012-12-31 11:30:45.123456",
                6,
                "2012-12-31 11:30:45.123456",
            ),
            ("2012-12-31 11:30:45.123456", 0, "2012-12-31 11:30:45"),
            ("2012-12-31 11:30:45.9", 0, "2012-12-31 11:30:46"),
            ("2012-12-31 11:30:45.123456", 1, "2012-12-31 11:30:45.1"),
            ("2012-12-31 11:30:45.999999", 4, "2012-12-31 11:30:46.0000"),
            ("2012-12-31 11:30:45.999999", 0, "2012-12-31 11:30:46"),
            ("2012-12-31 23:59:59.999999", 0, "2013-01-01 00:00:00"),
            ("2012-12-31 23:59:59.999999", 3, "2013-01-01 00:00:00.000"),
            ("2012-00-00 11:30:45.999999", 3, "2012-00-00 11:30:46.000"),
            // Edge cases:
            ("2012-01-00 23:59:59.999999", 3, "0000-00-00 00:00:00.000"),
            ("2012-04-31 23:59:59.999999", 3, "0000-00-00 00:00:00.000"),
        ];

        for (input, fsp, expected) in cases {
            let mut ctx = EvalContext::from(TimeEnv {
                allow_invalid_date: true,
                ..TimeEnv::default()
            });
            assert_eq!(
                expected,
                Time::parse_datetime(&mut ctx, input, MAX_FSP, true)?
                    .round_frac(&mut ctx, fsp)?
                    .to_string()
            );
        }
        Ok(())
    }

    #[test]
    fn test_normalized() -> Result<()> {
        let should_pass = vec![
            ("2019-00-01 12:34:56.1", "2018-12-01 12:34:56.1"),
            ("2019-01-00 12:34:56.1", "2018-12-31 12:34:56.1"),
            ("2019-00-00 12:34:56.1", "2018-11-30 12:34:56.1"),
            ("2019-04-31 12:34:56.1", "2019-05-01 12:34:56.1"),
            ("2019-02-29 12:34:56.1", "2019-03-01 12:34:56.1"),
            ("2019-02-30 12:34:56.1", "2019-03-02 12:34:56.1"),
            ("2019-02-31 12:34:56.1", "2019-03-03 12:34:56.1"),
        ];
        for (input, expected) in should_pass {
            let mut ctx = EvalContext::from(TimeEnv {
                allow_invalid_date: true,
                ..TimeEnv::default()
            });
            assert_eq!(
                expected,
                Time::parse_datetime(&mut ctx, input, 1, false)?
                    .normalized(&mut ctx)?
                    .to_string()
            );
        }
        Ok(())
    }

    #[test]
    fn checked_add_sub_duration() -> Result<()> {
        let normal_cases = vec![
            (
                "2018-12-30 11:30:45.123456",
                "00:00:14.876545",
                "2018-12-30 11:31:00.000001",
            ),
            (
                "2018-12-30 11:30:45.123456",
                "00:30:00",
                "2018-12-30 12:00:45.123456",
            ),
            (
                "2018-12-30 11:30:45.123456",
                "12:30:00",
                "2018-12-31 00:00:45.123456",
            ),
            (
                "2018-12-30 11:30:45.123456",
                "1 12:30:00",
                "2019-01-01 00:00:45.123456",
            ),
        ];

        for (lhs, rhs, expected) in normal_cases.clone() {
            let mut ctx = EvalContext::default();
            let lhs = Time::parse_datetime(&mut ctx, lhs, 6, false)?;
            let rhs = Duration::parse(&mut ctx, rhs, 6)?;
            let actual = lhs.checked_add(&mut ctx, rhs).unwrap();
            assert_eq!(expected, actual.to_string());
        }

        for (expected, rhs, lhs) in normal_cases {
            let mut ctx = EvalContext::default();
            let lhs = Time::parse_datetime(&mut ctx, lhs, 6, false)?;
            let rhs = Duration::parse(&mut ctx, rhs, 6)?;
            let actual = lhs.checked_sub(&mut ctx, rhs).unwrap();
            assert_eq!(expected, actual.to_string());
        }

        // DSTs
        let mut ctx = EvalContext::from(TimeEnv {
            time_zone: Tz::from_tz_name("America/New_York"),
            ..TimeEnv::default()
        });
        let dsts = vec![
            ("2019-03-10 01:00:00", "1:00:00", "2019-03-10 03:00:00"),
            ("2018-03-11 01:00:00", "1:00:00", "2018-03-11 03:00:00"),
        ];

        for (lhs, rhs, expected) in dsts.clone() {
            let lhs = Time::parse_timestamp(&mut ctx, lhs, 0, false)?;
            let rhs = Duration::parse(&mut EvalContext::default(), rhs, 6)?;
            let actual = lhs.checked_add(&mut ctx, rhs).unwrap();
            assert_eq!(expected, actual.to_string());
        }

        for (expected, rhs, lhs) in dsts {
            let lhs = Time::parse_timestamp(&mut ctx, lhs, 0, false)?;
            let rhs = Duration::parse(&mut EvalContext::default(), rhs, 6)?;
            let actual = lhs.checked_sub(&mut ctx, rhs).unwrap();
            assert_eq!(expected, actual.to_string());
        }

        // Edge cases
        let mut ctx = EvalContext::from(TimeEnv {
            allow_invalid_date: true,
            ..TimeEnv::default()
        });
        let cases = vec![
            ("2019-04-31 00:00:00", "1:00:00", "2019-05-01 01:00:00"),
            ("2019-00-01 00:00:00", "1:00:00", "2018-12-01 01:00:00"),
            ("2019-2-0 00:00:00", "1:00:00", "2019-01-31 01:00:00"),
        ];
        for (lhs, rhs, expected) in cases {
            let lhs = Time::parse_datetime(&mut ctx, lhs, 0, false)?;
            let rhs = Duration::parse(&mut EvalContext::default(), rhs, 6)?;
            let actual = lhs.checked_add(&mut ctx, rhs).unwrap();
            assert_eq!(expected, actual.to_string());
        }

        // Failed cases
        let mut ctx = EvalContext::default();
        let lhs = Time::parse_datetime(&mut ctx, "9999-12-31 23:59:59", 6, false)?;
        let rhs = Duration::parse(&mut ctx, "01:00:00", 6)?;
        assert_eq!(lhs.checked_add(&mut ctx, rhs), None);

        let lhs = Time::parse_datetime(&mut ctx, "0000-01-01 00:00:01", 6, false)?;
        let rhs = Duration::parse(&mut ctx, "01:00:00", 6)?;
        assert_eq!(lhs.checked_sub(&mut ctx, rhs), None);

        Ok(())
    }

    #[test]
    fn test_weekday() -> Result<()> {
        let cases = vec![
            ("2019-10-12", "Sat"),
            ("2019-04-31", "Wed"),
            ("0000-01-01", "Sat"),
            ("0000-01-00", "Fri"),
        ];
        let mut ctx = EvalContext::from(TimeEnv {
            allow_invalid_date: true,
            ..TimeEnv::default()
        });
        for (s, expected) in cases {
            assert_eq!(
                expected,
                format!("{:?}", Time::parse_date(&mut ctx, s)?.weekday())
            );
        }
        Ok(())
    }

    #[test]
    fn test_date_format() -> Result<()> {
        let cases = vec![
            (
                "2010-01-07 23:12:34.12345",
                "%b %M %m %c %D %d %e %j %k %h %i %p %r %T %s %f %U %u %V
                %v %a %W %w %X %x %Y %y %%",
                "Jan January 01 1 7th 07 7 007 23 11 12 PM 11:12:34 PM 23:12:34 34 123450 01 01 01
                01 Thu Thursday 4 2010 2010 2010 10 %",
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
                "%b %M %m %c %D %d %e %j %k %h %i %p %r %T %s %f %v %Y
                %y %%",
                "Jan January 01 1 1st 01 1 001 0 12 00 AM 12:00:00 AM 00:00:00 00 123456 52 0000
                00 %",
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
        for (s, layout, expect) in cases {
            let mut ctx = EvalContext::default();
            let t = Time::parse_datetime(&mut ctx, s, 6, false)?;
            let get = t.date_format(layout)?;
            assert_eq!(get, expect);
        }
        Ok(())
    }

    #[test]
    fn test_to_numeric_string() {
        let cases = vec![
            ("2012-12-31 11:30:45.123456", 4, "20121231113045.1235"),
            ("2012-12-31 11:30:45.123456", 6, "20121231113045.123456"),
            ("2012-12-31 11:30:45.123456", 0, "20121231113045"),
            ("2012-12-31 11:30:45.999999", 0, "20121231113046"),
            ("2017-01-05 08:40:59.575601", 0, "20170105084100"),
            ("2017-01-05 23:59:59.575601", 0, "20170106000000"),
            ("0000-00-00 00:00:00", 6, "00000000000000.000000"),
        ];
        let mut ctx = EvalContext::default();
        for (s, fsp, expect) in cases {
            let t = Time::parse_datetime(&mut ctx, s, fsp, true).unwrap();
            let get = t.to_numeric_string();
            assert_eq!(get, expect);
        }
    }

    #[test]
    fn test_to_decimal() {
        let cases = vec![
            ("2012-12-31 11:30:45.123456", 4, "20121231113045.1235"),
            ("2012-12-31 11:30:45.123456", 6, "20121231113045.123456"),
            ("2012-12-31 11:30:45.123456", 0, "20121231113045"),
            ("2012-12-31 11:30:45.999999", 0, "20121231113046"),
            ("2017-01-05 08:40:59.575601", 0, "20170105084100"),
            ("2017-01-05 23:59:59.575601", 0, "20170106000000"),
            ("0000-00-00 00:00:00", 6, "0"),
        ];
        let mut ctx = EvalContext::default();
        for (s, fsp, expect) in cases {
            let t = Time::parse_datetime(&mut ctx, s, fsp, true).unwrap();
            let get: Decimal = t.convert(&mut ctx).unwrap();
            assert_eq!(
                get,
                expect.as_bytes().convert(&mut ctx).unwrap(),
                "convert datetime {} to decimal",
                s
            );
        }
    }

    #[test]
    #[allow(clippy::excessive_precision)]
    fn test_convert_to_f64() {
        let cases = vec![
            ("2012-12-31 11:30:45.123456", 4, 20121231113045.1235f64),
            ("2012-12-31 11:30:45.123456", 6, 20121231113045.123456f64),
            ("2012-12-31 11:30:45.123456", 0, 20121231113045f64),
            ("2012-12-31 11:30:45.999999", 0, 20121231113046f64),
            ("2017-01-05 08:40:59.575601", 0, 20170105084100f64),
            ("2017-01-05 23:59:59.575601", 0, 20170106000000f64),
            ("0000-00-00 00:00:00", 6, 0f64),
        ];
        let mut ctx = EvalContext::default();
        for (s, fsp, expect) in cases {
            let t = Time::parse_datetime(&mut ctx, s, fsp, true).unwrap();
            let get: f64 = t.convert(&mut ctx).unwrap();
            assert!(
                (expect - get).abs() < std::f64::EPSILON,
                "expect: {}, got: {}",
                expect,
                get
            );
        }
    }
}
