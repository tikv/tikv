// Copyright 2016 TiKV Project Authors. Licensed under Apache-2.0.

use std::{
    cmp::Ordering,
    fmt::{self, Display, Formatter},
};

use codec::prelude::*;
use tipb::FieldType;

use super::{check_fsp, Decimal, DEFAULT_FSP};
use crate::{
    codec::{
        convert::ConvertTo,
        error::{ERR_DATA_OUT_OF_RANGE, ERR_TRUNCATE_WRONG_VALUE},
        mysql::{Time as DateTime, TimeType, MAX_FSP, MIN_FSP},
        Error, Result, TEN_POW,
    },
    expr::EvalContext,
    FieldTypeAccessor,
};

pub const NANOS_PER_SEC: i64 = 1_000_000_000;
pub const NANOS_PER_MILLI: i64 = 1_000_000;
pub const NANOS_PER_MICRO: i64 = 1_000;
pub const MICROS_PER_SEC: i64 = 1_000_000;
pub const NANO_WIDTH: usize = 9;
pub const MICRO_WIDTH: usize = 6;

const SECS_PER_HOUR: i64 = 3600;
const SECS_PER_MINUTE: i64 = 60;

pub const MAX_HOUR_PART: u32 = 838;
pub const MAX_MINUTE_PART: u32 = 59;
pub const MAX_SECOND_PART: u32 = 59;
pub const MAX_NANOS_PART: u32 = 999_999_999;
pub const MAX_NANOS: i64 = ((MAX_HOUR_PART as i64 * SECS_PER_HOUR)
    + MAX_MINUTE_PART as i64 * SECS_PER_MINUTE
    + MAX_SECOND_PART as i64)
    * NANOS_PER_SEC;
const MAX_DURATION_INT_VALUE: u32 = MAX_HOUR_PART * 10000 + MAX_MINUTE_PART * 100 + MAX_SECOND_PART;

#[inline]
fn check_hour_part(hour: u32) -> Result<u32> {
    if hour > MAX_HOUR_PART {
        Err(Error::Eval(
            "DURATION OVERFLOW".to_string(),
            ERR_DATA_OUT_OF_RANGE,
        ))
    } else {
        Ok(hour)
    }
}

#[inline]
fn check_minute_part(minute: u32) -> Result<u32> {
    if minute > MAX_MINUTE_PART {
        Err(Error::truncated_wrong_val("MINUTES", minute))
    } else {
        Ok(minute)
    }
}

#[inline]
fn check_second_part(second: u32) -> Result<u32> {
    if second > MAX_SECOND_PART {
        Err(Error::truncated_wrong_val("SECONDS", second))
    } else {
        Ok(second)
    }
}

#[inline]
fn check_nanos_part(nanos: u32) -> Result<u32> {
    if nanos > MAX_NANOS_PART {
        Err(Error::truncated_wrong_val("NANOS", nanos))
    } else {
        Ok(nanos)
    }
}

#[inline]
fn check_nanos(nanos: i64) -> Result<i64> {
    if nanos < -MAX_NANOS || nanos > MAX_NANOS {
        Err(Error::truncated_wrong_val("NANOS", nanos))
    } else {
        Ok(nanos)
    }
}

mod parser {
    use nom::{
        character::complete::{anychar, char, digit0, digit1, space0, space1},
        combinator::opt,
        IResult,
    };

    use super::*;

    fn number(input: &str) -> IResult<&str, u32, ()> {
        let (rest, num) = digit1(input)?;
        Ok((rest, num.parse().map_err(|_| nom::Err::Error(()))?))
    }

    // If first character is '-', `negative` is true.
    fn negative(input: &str) -> IResult<&str, bool, ()> {
        let (rest, dash) = opt(char('-'))(input)?;
        Ok((rest, dash.is_some()))
    }

    fn colon(input: &str) -> IResult<&str, (), ()> {
        let (rest, _) = space0(input)?;
        let (rest, _) = char(':')(rest)?;
        let (rest, _) = space0(rest)?;
        Ok((rest, ()))
    }

    fn day_hhmmss(input: &str) -> IResult<&str, (u32, [u32; 3]), ()> {
        let (rest, day) = number(input)?;
        let (rest, _) = space1(rest)?;
        let (rest, hhmmss) = hhmmss_delimited(rest, false)?;
        Ok((rest, (day, hhmmss)))
    }

    fn hhmmss_delimited(input: &str, require_colon: bool) -> IResult<&str, [u32; 3], ()> {
        let mut hhmmss = [0; 3];

        let (mut rest, hour) = number(input)?;
        hhmmss[0] = hour;

        for i in 1..=2 {
            if let Ok((remain, _)) = colon(rest) {
                let (remain, num) = number(remain)?;
                hhmmss[i] = num;
                rest = remain;
            } else {
                if i == 1 && require_colon {
                    return Err(nom::Err::Error(()));
                }
                break;
            }
        }

        Ok((rest, hhmmss))
    }

    fn hhmmss_compact(input: &str) -> IResult<&str, [u32; 3], ()> {
        let (rest, num) = number(input)?;
        let hhmmss = [num / 10000, (num / 100) % 100, num % 100];
        Ok((rest, hhmmss))
    }

    fn hhmmss_datetime<'a>(
        ctx: &mut EvalContext,
        input: &'a str,
        fsp: u8,
    ) -> IResult<&'a str, Duration, ()> {
        let (rest, digits) = digit1(input)?;
        if digits.len() == 12 || digits.len() == 14 {
            let datetime = DateTime::parse_datetime(ctx, input, fsp as i8, true)
                .map_err(|_| nom::Err::Error(()))?;
            return Ok(("", datetime.convert(ctx).map_err(|_| nom::Err::Error(()))?));
        }
        let (rest, _) = anysep(rest)?;
        let (rest, _) = digit1(rest)?;
        let (rest, _) = anysep(rest)?;
        let (rest, _) = digit1(rest)?;

        let has_datetime_sep = matches!(rest.chars().next(), Some(c) if c == 'T' || c == ' ');

        if !has_datetime_sep {
            return Err(nom::Err::Error(()));
        }

        let datetime = DateTime::parse_datetime(ctx, input, fsp as i8, true)
            .map_err(|_| nom::Err::Error(()))?;
        Ok(("", datetime.convert(ctx).map_err(|_| nom::Err::Error(()))?))
    }

    fn anysep(input: &str) -> IResult<&str, char, ()> {
        let (rest, sep) = anychar(input)?;
        if !sep.is_ascii_punctuation() {
            Err(nom::Err::Error(()))
        } else {
            Ok((rest, sep))
        }
    }

    fn fraction(input: &str, fsp: u8) -> IResult<&str, u32, ()> {
        let fsp = usize::from(fsp);
        let (rest, dot) = opt(char('.'))(input)?;

        if dot.is_none() {
            return Ok((rest, 0));
        }

        let (rest, digits) = digit0(rest)?;
        let ((_, frac), len) = if fsp >= digits.len() {
            (number(digits)?, digits.len())
        } else {
            (number(&digits[..=fsp])?, fsp + 1)
        };

        Ok((rest, frac * TEN_POW[NANO_WIDTH.saturating_sub(len)]))
    }

    pub fn parse(
        ctx: &mut EvalContext,
        input: &str,
        fsp: u8,
        fallback_to_daytime: bool,
        overflow_as_null: bool,
    ) -> Option<Duration> {
        let input = input.trim();
        if input.is_empty() {
            return Some(Duration::zero());
        }

        let (rest, neg) = negative(input).ok()?;
        let (rest, _) = space0::<_, ()>(rest).ok()?;
        let duration = day_hhmmss(rest)
            .ok()
            .and_then(|(rest, (day, [hh, mm, ss]))| {
                Some((rest, [day.checked_mul(24)?.checked_add(hh)?, mm, ss]))
            })
            .or_else(|| hhmmss_delimited(rest, true).ok())
            .or_else(|| hhmmss_compact(rest).ok())
            .and_then(|(rest, hhmmss)| {
                let (rest, _) = space0::<_, ()>(rest).ok()?;
                let (rest, frac) = fraction(rest, fsp).ok()?;

                if !rest.is_empty() {
                    return None;
                }

                Some(Duration::new_from_parts(
                    neg, hhmmss[0], hhmmss[1], hhmmss[2], frac, fsp as i8,
                ))
            });

        match duration {
            Some(Ok(duration)) => Some(duration),
            Some(Err(err)) if err.is_overflow() => {
                if overflow_as_null {
                    return None;
                }
                ctx.handle_overflow_err(err).map_or(None, |_| {
                    let nanos = if neg { -MAX_NANOS } else { MAX_NANOS };
                    Some(Duration { nanos, fsp })
                })
            }
            None if fallback_to_daytime => {
                hhmmss_datetime(ctx, rest, fsp).map_or(None, |(_, duration)| Some(duration))
            }
            _ => None,
        }
    }
}

#[inline]
fn checked_round(nanos: i64, fsp: u8) -> Result<i64> {
    check_nanos(nanos)?;
    let min_step = TEN_POW[NANO_WIDTH - fsp as usize] as i64;
    let rem = nanos % min_step;
    let nanos = if rem.abs() < min_step / 2 {
        nanos - rem
    } else {
        nanos - rem + min_step * nanos.signum()
    };
    check_nanos(nanos)
}

#[derive(Debug, Clone, Copy)]
#[repr(C)]
pub struct Duration {
    nanos: i64,
    fsp: u8,
}

impl Duration {
    #[inline]
    pub fn is_neg(self) -> bool {
        self.nanos < 0
    }

    #[inline]
    pub fn hours(self) -> u32 {
        (self.to_secs().abs() / SECS_PER_HOUR) as u32
    }

    #[inline]
    pub fn minutes(self) -> u32 {
        (self.to_secs().abs() / SECS_PER_MINUTE % 60) as u32
    }

    #[inline]
    pub fn secs(self) -> u32 {
        (self.to_secs().abs() % SECS_PER_MINUTE) as u32
    }

    /// Returns the fractional part of `Duration` in microseconds.
    #[inline]
    pub fn subsec_micros(self) -> u32 {
        self.subsec_nanos() / 1_000
    }

    /// Returns the fractional part of `Duration` in nanoseconds.
    #[inline]
    pub fn subsec_nanos(self) -> u32 {
        (self.nanos.abs() % NANOS_PER_SEC) as u32
    }

    #[inline]
    pub fn fsp(self) -> u8 {
        self.fsp
    }

    #[inline]
    #[must_use]
    pub fn minimize_fsp(self) -> Self {
        Duration {
            fsp: MIN_FSP as u8,
            ..self
        }
    }

    #[inline]
    #[must_use]
    pub fn maximize_fsp(self) -> Self {
        Duration {
            fsp: MAX_FSP as u8,
            ..self
        }
    }

    #[inline]
    pub fn to_secs(self) -> i64 {
        self.nanos / NANOS_PER_SEC
    }

    /// Returns the number of seconds contained by this Duration as f64.
    /// The returned value does include the fractional (nanosecond) part of the duration.
    #[inline]
    pub fn to_secs_f64(self) -> f64 {
        self.nanos as f64 / NANOS_PER_SEC as f64
    }

    #[inline]
    pub fn to_millis(self) -> i64 {
        self.nanos / NANOS_PER_MILLI
    }

    #[inline]
    pub fn to_micros(self) -> i64 {
        self.nanos / NANOS_PER_MICRO
    }

    #[inline]
    pub fn to_nanos(self) -> i64 {
        self.nanos
    }

    /// Returns the identity element of `Duration`
    #[inline]
    pub fn zero() -> Duration {
        Duration {
            nanos: 0,
            fsp: DEFAULT_FSP as u8,
        }
    }

    /// Returns true if self is equal to the additive identity.
    #[inline]
    pub fn is_zero(self) -> bool {
        self.nanos == 0
    }

    /// Returns the absolute value of `Duration`
    #[inline]
    #[must_use]
    pub fn abs(self) -> Self {
        Duration {
            nanos: self.nanos.abs(),
            ..self
        }
    }

    pub fn from_secs(secs: i64, fsp: i8) -> Result<Duration> {
        let fsp = check_fsp(fsp)?;
        let nanos = secs
            .checked_mul(NANOS_PER_SEC)
            .ok_or_else(|| Error::Eval("DURATION OVERFLOW".to_string(), ERR_DATA_OUT_OF_RANGE))?;
        check_nanos(nanos)?;
        Ok(Duration { nanos, fsp })
    }

    pub fn from_millis(millis: i64, fsp: i8) -> Result<Duration> {
        let fsp = check_fsp(fsp)?;
        let nanos = millis
            .checked_mul(NANOS_PER_MILLI)
            .ok_or_else(|| Error::Eval("DURATION OVERFLOW".to_string(), ERR_DATA_OUT_OF_RANGE))?;
        let nanos = checked_round(nanos, fsp)?;
        Ok(Duration { nanos, fsp })
    }

    pub fn from_micros(micros: i64, fsp: i8) -> Result<Duration> {
        let fsp = check_fsp(fsp)?;
        let nanos = micros
            .checked_mul(NANOS_PER_MICRO)
            .ok_or_else(|| Error::Eval("DURATION OVERFLOW".to_string(), ERR_DATA_OUT_OF_RANGE))?;
        let nanos = checked_round(nanos, fsp)?;
        Ok(Duration { nanos, fsp })
    }

    pub fn from_nanos(nanos: i64, fsp: i8) -> Result<Duration> {
        let fsp = check_fsp(fsp)?;
        let nanos = checked_round(nanos, fsp)?;
        Ok(Duration { nanos, fsp })
    }

    pub fn new_from_parts(
        neg: bool,
        hour: u32,
        minute: u32,
        second: u32,
        nanos: u32,
        fsp: i8,
    ) -> Result<Duration> {
        check_minute_part(minute)?;
        check_second_part(second)?;
        check_nanos_part(nanos)?;
        check_hour_part(hour)?;
        let fsp = check_fsp(fsp)?;
        let signum = if neg { -1 } else { 1 };
        let minute = minute as i64 + hour as i64 * 60;
        let second = second as i64 + minute * SECS_PER_MINUTE;
        let nanos = nanos as i64 + second * NANOS_PER_SEC;
        let nanos = signum * nanos;
        let nanos = checked_round(nanos, fsp)?;
        Ok(Duration { nanos, fsp })
    }

    /// Parses the time from a formatted string with a fractional seconds part,
    /// returns the duration type `Time` value.
    /// See: <http://dev.mysql.com/doc/refman/5.7/en/fractional-seconds.html>
    pub fn parse(ctx: &mut EvalContext, input: &str, fsp: i8) -> Result<Duration> {
        let fsp = check_fsp(fsp)?;
        parser::parse(ctx, input, fsp, true, false)
            .ok_or_else(|| Error::truncated_wrong_val("TIME", input))
    }

    pub fn parse_consider_overflow(
        ctx: &mut EvalContext,
        input: &str,
        fsp: i8,
        overflow_as_null: bool,
    ) -> Result<Duration> {
        let fsp = check_fsp(fsp)?;
        parser::parse(ctx, input, fsp, true, overflow_as_null)
            .ok_or_else(|| Error::truncated_wrong_val("TIME", input))
    }

    /// Parses the input exactly as duration and will not fallback to datetime.
    pub fn parse_exactly(ctx: &mut EvalContext, input: &str, fsp: i8) -> Result<Duration> {
        let fsp = check_fsp(fsp)?;
        parser::parse(ctx, input, fsp, false, false)
            .ok_or_else(|| Error::truncated_wrong_val("TIME", input))
    }

    /// Rounds fractional seconds precision with new FSP and returns a new one.
    /// We will use the “round half up” rule, e.g, >= 0.5 -> 1, < 0.5 -> 0,
    /// so 10:10:10.999999 round with fsp: 1 -> 10:10:11.0
    /// and 10:10:10.000000 round with fsp: 0 -> 10:10:11
    pub fn round_frac(self, fsp: i8) -> Result<Self> {
        let fsp = check_fsp(fsp)?;

        if fsp >= self.fsp {
            return Ok(Duration { fsp, ..self });
        }

        let nanos = checked_round(self.nanos, fsp)?;

        Ok(Duration { nanos, fsp })
    }

    /// Checked duration addition. Computes self + rhs, returning None if overflow occurred.
    pub fn checked_add(self, rhs: Duration) -> Option<Duration> {
        let nanos = self.nanos.checked_add(rhs.nanos)?;
        check_nanos(nanos).ok()?;
        Some(Duration {
            nanos,
            fsp: self.fsp.max(rhs.fsp),
        })
    }

    /// Checked duration subtraction. Computes self - rhs, returning None if overflow occurred.
    pub fn checked_sub(self, rhs: Duration) -> Option<Duration> {
        let nanos = self.nanos.checked_sub(rhs.nanos)?;
        check_nanos(nanos).ok()?;
        Some(Duration {
            nanos,
            fsp: self.fsp.max(rhs.fsp),
        })
    }

    fn format(self, sep: &str) -> String {
        use std::fmt::Write;

        let res_max_len = 8 + 2 * sep.len() + MAX_FSP as usize;
        let mut string = String::with_capacity(res_max_len);
        if self.is_neg() {
            string.push('-');
        }

        write!(
            string,
            "{:02}{}{:02}{}{:02}",
            self.hours(),
            sep,
            self.minutes(),
            sep,
            self.secs()
        )
        .unwrap();

        if self.fsp > 0 {
            let frac = self.subsec_nanos() / TEN_POW[NANO_WIDTH - self.fsp as usize];
            write!(string, ".{:0width$}", frac, width = self.fsp as usize).unwrap();
        }

        string
    }

    /// Converts a `Duration` to printable numeric string representation
    #[inline]
    pub fn to_numeric_string(self) -> String {
        self.format("")
    }

    pub fn from_i64(ctx: &mut EvalContext, n: i64, fsp: i8) -> Result<Duration> {
        if n > i64::from(MAX_DURATION_INT_VALUE) || n < -i64::from(MAX_DURATION_INT_VALUE) {
            if n >= 10000000000 {
                if let Ok(t) = DateTime::parse_from_i64(ctx, n, TimeType::DateTime, fsp) {
                    return t.convert(ctx);
                }
            }
            return Err(Error::overflow("Duration", n));
        }

        let abs = n.abs();
        let hour = (abs / 10000) as u32;
        let minute = ((abs / 100) % 100) as u32;
        let second = (abs % 100) as u32;

        if hour > MAX_HOUR_PART || minute > MAX_MINUTE_PART || second > MAX_SECOND_PART {
            return Err(Error::Eval(
                format!("Truncated incorrect time value: '{}'", n),
                ERR_TRUNCATE_WRONG_VALUE,
            ));
        }

        Self::new_from_parts(n.is_negative(), hour, minute, second, 0, fsp)
    }
}

impl ConvertTo<f64> for Duration {
    #[inline]
    fn convert(&self, _: &mut EvalContext) -> Result<f64> {
        let val = self.to_numeric_string().parse()?;
        Ok(val)
    }
}

impl ConvertTo<Decimal> for Duration {
    /// This function should not return err,
    /// if it return err, then the err is because of bug.
    ///
    /// Port from TiDB' Duration::ToNumber
    #[inline]
    fn convert(&self, _: &mut EvalContext) -> Result<Decimal> {
        let r = self.to_numeric_string().parse::<Decimal>();
        debug_assert!(r.is_ok());
        r
    }
}

impl Display for Duration {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        write!(formatter, "{}", self.format(":"))
    }
}

impl PartialEq for Duration {
    fn eq(&self, rhs: &Duration) -> bool {
        self.nanos.eq(&rhs.nanos)
    }
}

impl Eq for Duration {}

impl PartialOrd for Duration {
    #[inline]
    fn partial_cmp(&self, rhs: &Duration) -> Option<Ordering> {
        self.nanos.partial_cmp(&rhs.nanos)
    }
}

impl Ord for Duration {
    #[inline]
    fn cmp(&self, rhs: &Duration) -> Ordering {
        self.partial_cmp(rhs).unwrap()
    }
}

impl std::hash::Hash for Duration {
    #[inline]
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.nanos.hash(state)
    }
}

impl<T: BufferWriter> DurationEncoder for T {}

pub trait DurationEncoder: NumberEncoder {
    #[inline]
    fn write_duration_to_chunk(&mut self, val: Duration) -> Result<()> {
        self.write_i64_le(val.to_nanos())?;
        Ok(())
    }
}

pub trait DurationDatumPayloadChunkEncoder: NumberEncoder {
    #[inline]
    fn write_duration_to_chunk_by_datum_payload_int(
        &mut self,
        mut src_payload: &[u8],
    ) -> Result<()> {
        let nanos = src_payload.read_i64()?;
        self.write_i64_le(nanos)?;
        Ok(())
    }

    #[inline]
    fn write_duration_to_chunk_by_datum_payload_varint(
        &mut self,
        mut src_payload: &[u8],
    ) -> Result<()> {
        let nanos = src_payload.read_var_i64()?;
        self.write_i64_le(nanos)?;
        Ok(())
    }
}

impl<T: BufferWriter> DurationDatumPayloadChunkEncoder for T {}

pub trait DurationDecoder: NumberDecoder {
    #[inline]
    fn read_duration_int(&mut self, field_type: &FieldType) -> Result<Duration> {
        let nanos = self.read_i64()?;
        Duration::from_nanos(nanos, field_type.as_accessor().decimal() as i8)
    }

    #[inline]
    fn read_duration_varint(&mut self, field_type: &FieldType) -> Result<Duration> {
        let nanos = self.read_var_i64()?;
        Duration::from_nanos(nanos, field_type.as_accessor().decimal() as i8)
    }

    #[inline]
    fn read_duration_from_chunk(&mut self, fsp: isize) -> Result<Duration> {
        let nanos = self.read_i64_le()?;
        Duration::from_nanos(nanos, fsp as i8)
    }
}

impl<T: BufferReader> DurationDecoder for T {}

impl crate::codec::data_type::AsMySQLBool for Duration {
    #[inline]
    fn as_mysql_bool(&self, _context: &mut crate::expr::EvalContext) -> crate::codec::Result<bool> {
        Ok(!self.is_zero())
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::*;
    use crate::{
        codec::{data_type::DateTime, mysql::UNSPECIFIED_FSP},
        expr::{EvalConfig, EvalContext, Flag},
    };

    #[test]
    fn test_hours() {
        let cases: Vec<(&str, i8, u32)> = vec![
            ("31 11:30:45", 0, 31 * 24 + 11),
            ("11:30:45", 0, 11),
            ("-11:30:45.9233456", 0, 11),
            ("272:59:59", 0, 272),
        ];

        for (input, fsp, exp) in cases {
            let dur = Duration::parse(&mut EvalContext::default(), input, fsp).unwrap();
            let res = dur.hours();
            assert_eq!(exp, res);
        }
    }

    #[test]
    fn test_minutes() {
        let cases: Vec<(&str, i8, u32)> = vec![
            ("31 11:30:45", 0, 30),
            ("11:30:45", 0, 30),
            ("-11:30:45.9233456", 0, 30),
        ];

        for (input, fsp, exp) in cases {
            let dur = Duration::parse(&mut EvalContext::default(), input, fsp).unwrap();
            let res = dur.minutes();
            assert_eq!(exp, res);
        }
    }

    #[test]
    fn test_secs() {
        let cases: Vec<(&str, i8, u32)> = vec![
            ("31 11:30:45", 0, 45),
            ("11:30:45", 0, 45),
            ("-11:30:45.9233456", 1, 45),
            ("-11:30:45.9233456", 0, 46),
        ];

        for (input, fsp, exp) in cases {
            let dur = Duration::parse(&mut EvalContext::default(), input, fsp).unwrap();
            let res = dur.secs();
            assert_eq!(exp, res);
        }
    }

    #[test]
    fn test_subsec_micros() {
        let cases: Vec<(&str, i8, u32)> = vec![
            ("31 11:30:45.123", 6, 123000),
            ("11:30:45.123345", 3, 123000),
            ("11:30:45.123345", 5, 123350),
            ("11:30:45.123345", 6, 123345),
            ("11:30:45.1233456", 6, 123346),
            ("11:30:45.9233456", 0, 0),
            ("11:30:45.000010", 6, 10),
            ("11:30:45.00010", 5, 100),
        ];

        for (input, fsp, exp) in cases {
            let dur = Duration::parse(&mut EvalContext::default(), input, fsp).unwrap();
            let res = dur.subsec_micros();
            assert_eq!(exp, res);
        }
    }

    #[test]
    fn test_parse_overflow_as_warning() {
        let cases: Vec<(&str, i8, &'static str)> = vec![
            ("-1062600704", 0, "-838:59:59"),
            ("1062600704", 0, "838:59:59"),
            // FIXME: some error information lost while converting `Result` to `Option`
            // (b"4294967295 0:59:59", 0, "838:59:59"),
        ];

        for (input, fsp, expect) in cases {
            let mut ctx =
                EvalContext::new(Arc::new(EvalConfig::from_flag(Flag::OVERFLOW_AS_WARNING)));
            let got = Duration::parse(&mut ctx, input, fsp);
            assert_eq!(expect, &format!("{}", got.unwrap()));
        }
    }

    #[test]
    fn test_parse_duration() {
        let cases: Vec<(&str, i8, Option<&'static str>)> = vec![
            ("10:11:12", 0, Some("10:11:12")),
            ("101112", 0, Some("10:11:12")),
            ("10:11", 0, Some("10:11:00")),
            ("101112.123456", 0, Some("10:11:12")),
            ("1112", 0, Some("00:11:12")),
            ("12", 0, Some("00:00:12")),
            ("1 12", 0, Some("36:00:00")),
            ("1 10:11:12", 0, Some("34:11:12")),
            ("1 10:11:12.123456", 0, Some("34:11:12")),
            ("1 10:11:12.123456", 4, Some("34:11:12.1235")),
            ("1 10:11:12.12", 4, Some("34:11:12.1200")),
            ("1 10:11:12.1234565", 6, Some("34:11:12.123457")),
            ("1 10:11:12.9999995", 6, Some("34:11:13.000000")),
            ("1 10:11:12.123456", 7, None),
            ("10:11:12.123456", 0, Some("10:11:12")),
            ("1 10:11", 0, Some("34:11:00")),
            ("1 10", 0, Some("34:00:00")),
            ("24 10", 0, Some("586:00:00")),
            ("-24 10", 0, Some("-586:00:00")),
            ("0 10", 0, Some("10:00:00")),
            ("-10:10:10", 0, Some("-10:10:10")),
            ("-838:59:59", 0, Some("-838:59:59")),
            ("838:59:59", 0, Some("838:59:59")),
            ("839:00:00", 0, None),
            ("-839:00:00", 0, None),
            ("23:60:59", 0, None),
            ("54:59:59", 0, Some("54:59:59")),
            ("2011-11-11 00:00:01", 0, Some("00:00:01")),
            ("20111111000001", 0, Some("00:00:01")),
            ("201112110102", 0, Some("11:01:02")),
            ("2011-11-11", 0, None),
            ("--23", 0, None),
            ("232 10", 0, None),
            ("-232 10", 0, None),
            ("00:00:00.1", 0, Some("00:00:00")),
            ("00:00:00.1", 1, Some("00:00:00.1")),
            ("00:00:00.777777", 2, Some("00:00:00.78")),
            ("00:00:00.777777", 6, Some("00:00:00.777777")),
            ("00:00:00.001", 3, Some("00:00:00.001")),
            // NOTE: The following case is easy to fail.
            ("- 1 ", 0, Some("-00:00:01")),
            ("1:2:3", 0, Some("01:02:03")),
            ("1 1:2:3", 0, Some("25:02:03")),
            ("-1 1:2:3.123", 3, Some("-25:02:03.123")),
            ("-.123", 3, None),
            ("12345", 0, Some("01:23:45")),
            ("-123", 0, Some("-00:01:23")),
            ("-23", 0, Some("-00:00:23")),
            ("- 1 1", 0, Some("-25:00:00")),
            ("-1 1", 0, Some("-25:00:00")),
            (" - 1:2:3 .123 ", 3, Some("-01:02:03.123")),
            (" - 1 :2 :3 .123 ", 3, Some("-01:02:03.123")),
            (" - 1 : 2 :3 .123 ", 3, Some("-01:02:03.123")),
            (" - 1 : 2 :  3 .123 ", 3, Some("-01:02:03.123")),
            (" - 1 .123 ", 3, Some("-00:00:01.123")),
            ("-", 0, None),
            ("- .1", 0, None),
            ("", 0, Some("00:00:00")),
            ("", 7, None),
            ("1.1", 1, Some("00:00:01.1")),
            ("-1.1", 1, Some("-00:00:01.1")),
            ("- 1.1", 1, Some("-00:00:01.1")),
            ("- 1 .1", 1, Some("-00:00:01.1")),
            ("18446744073709551615:59:59", 0, None),
            ("4294967295 0:59:59", 0, None),
            ("4294967295 232:59:59", 0, None),
            ("-4294967295 232:59:59", 0, None),
            ("1::2:3", 0, None),
            ("1.23 3", 0, None),
            ("1:62:3", 0, None),
            ("1:02:63", 0, None),
            ("-231342080", 0, None),
            // test fallback to datetime
            ("2010-02-12", 0, None),
            ("2010-02-12t12:23:34", 0, None),
            ("2010-02-12T12:23:34", 0, Some("12:23:34")),
            ("2010-02-12 12:23:34", 0, Some("12:23:34")),
            ("2010-02-12 12:23:34.12345", 6, Some("12:23:34.123450")),
            ("10-02-12 12:23:34.12345", 6, Some("12:23:34.123450")),
        ];

        for (input, fsp, expect) in cases {
            let got = Duration::parse(&mut EvalContext::default(), input, fsp);
            assert_eq!(got.ok().map(|d| d.to_string()), expect.map(str::to_string));
        }
    }

    #[test]
    fn test_parse_consider_overflow() {
        let cases: Vec<(&str, i8, Option<&'static str>, bool)> = vec![
            ("-790822912", 0, None, true),
            ("-790822912", 0, Some("-838:59:59"), false),
        ];

        for (input, fsp, expect, return_null) in cases {
            let mut ctx =
                EvalContext::new(Arc::new(EvalConfig::from_flag(Flag::OVERFLOW_AS_WARNING)));
            let got = Duration::parse_consider_overflow(&mut ctx, input, fsp, return_null);
            assert_eq!(got.ok().map(|d| d.to_string()), expect.map(str::to_string));
        }
    }

    #[test]
    fn test_to_numeric_string() {
        let cases: Vec<(&str, i8, &str)> = vec![
            ("11:30:45.123456", 4, "113045.1235"),
            ("11:30:45.123456", 6, "113045.123456"),
            ("11:30:45.123456", 0, "113045"),
            ("11:30:45.999999", 0, "113046"),
            ("08:40:59.575601", 0, "084100"),
            ("23:59:59.575601", 0, "240000"),
            ("00:00:00", 0, "000000"),
            ("00:00:00", 6, "000000.000000"),
        ];
        for (s, fsp, expect) in cases {
            let du = Duration::parse(&mut EvalContext::default(), s, fsp).unwrap();
            let get = du.to_numeric_string();
            assert_eq!(get, expect.to_string());
        }
    }

    #[test]
    fn test_to_decimal() {
        let cases = vec![
            ("31 11:30:45", 0, "7553045"),
            ("31 11:30:45", 6, "7553045.000000"),
            ("31 11:30:45", 0, "7553045"),
            ("31 11:30:45.123", 6, "7553045.123000"),
            ("11:30:45", 0, "113045"),
            ("11:30:45", 6, "113045.000000"),
            ("11:30:45.123", 6, "113045.123000"),
            ("11:30:45.123345", 0, "113045"),
            ("11:30:45.123345", 3, "113045.123"),
            ("11:30:45.123345", 5, "113045.12335"),
            ("11:30:45.123345", 6, "113045.123345"),
            ("11:30:45.1233456", 6, "113045.123346"),
            ("11:30:45.9233456", 0, "113046"),
            ("-11:30:45.9233456", 0, "-113046"),
        ];

        let mut ctx = EvalContext::default();
        for (input, fsp, exp) in cases {
            let t = Duration::parse(&mut EvalContext::default(), input, fsp).unwrap();
            let dec: Decimal = t.convert(&mut ctx).unwrap();
            let res = format!("{}", dec);
            assert_eq!(exp, res);
        }
        let cases = vec![
            ("2012-12-31 11:30:45.123456", 4, "113045.1235"),
            ("2012-12-31 11:30:45.123456", 6, "113045.123456"),
            ("2012-12-31 11:30:45.123456", 0, "113045"),
            ("2012-12-31 11:30:45.999999", 0, "113046"),
            ("2017-01-05 08:40:59.575601", 0, "084100"),
            ("2017-01-05 23:59:59.575601", 0, "000000"),
            ("0000-00-00 00:00:00", 6, "000000"),
        ];
        for (s, fsp, expect) in cases {
            let t = DateTime::parse_datetime(&mut ctx, s, fsp, true).unwrap();
            let du: Duration = t.convert(&mut ctx).unwrap();
            let get: Decimal = du.convert(&mut ctx).unwrap();
            assert_eq!(
                get,
                expect.as_bytes().convert(&mut ctx).unwrap(),
                "convert duration {} to decimal",
                s
            );
        }
    }

    #[test]
    fn test_to_f64() {
        let cases = vec![
            ("2012-12-31 11:30:45.123456", 4, 113045.1235f64),
            ("2012-12-31 11:30:45.123456", 6, 113045.123456f64),
            ("2012-12-31 11:30:45.123456", 0, 113045f64),
            ("2012-12-31 11:30:45.999999", 0, 113046f64),
            ("2017-01-05 08:40:59.575601", 0, 084100f64),
            ("2017-01-05 23:59:59.575601", 0, 0f64),
            ("0000-00-00 00:00:00", 6, 0f64),
        ];
        let mut ctx = EvalContext::default();
        for (s, fsp, expect) in cases {
            let t = DateTime::parse_datetime(&mut ctx, s, fsp, true).unwrap();
            let du: Duration = t.convert(&mut ctx).unwrap();
            let get: f64 = du.convert(&mut ctx).unwrap();
            assert!(
                (expect - get).abs() < f64::EPSILON,
                "expect: {}, got: {}",
                expect,
                get
            );
        }
    }

    #[test]
    fn test_round_frac() {
        let cases = vec![
            ("11:30:45.123456", 4, "11:30:45.1235"),
            ("11:30:45.123456", 6, "11:30:45.123456"),
            ("11:30:45.123456", 0, "11:30:45"),
            ("11:59:59.999999", 3, "12:00:00.000"),
            ("1 11:30:45.123456", 1, "35:30:45.1"),
            ("1 11:30:45.999999", 4, "35:30:46.0000"),
            ("-1 11:30:45.999999", 0, "-35:30:46"),
            ("-1 11:59:59.9999", 2, "-36:00:00.00"),
        ];
        for (input, fsp, exp) in cases {
            let t = Duration::parse(&mut EvalContext::default(), input, MAX_FSP)
                .unwrap()
                .round_frac(fsp)
                .unwrap();
            let res = format!("{}", t);
            assert_eq!(exp, res);
        }
    }

    #[test]
    fn test_codec() {
        let cases = vec![
            ("11:30:45.123456", 4),
            ("11:30:45.123456", 6),
            ("11:30:45.123456", 0),
            ("11:59:59.999999", 3),
            ("1 11:30:45.123456", 1),
            ("1 11:30:45.999999", 4),
            ("-1 11:30:45.999999", 0),
            ("-1 11:59:59.9999", 2),
        ];
        for (input, fsp) in cases {
            let t = Duration::parse(&mut EvalContext::default(), input, fsp).unwrap();
            let mut buf = vec![];
            buf.write_duration_to_chunk(t).unwrap();
            let got = buf
                .as_slice()
                .read_duration_from_chunk(fsp as isize)
                .unwrap();
            assert_eq!(t, got);
        }
    }

    #[test]
    fn test_checked_add_and_sub_duration() {
        /// `MAX_TIME_IN_SECS` is the maximum for mysql time type.
        const MAX_TIME_IN_SECS: i64 = MAX_HOUR_PART as i64 * SECS_PER_HOUR as i64
            + MAX_MINUTE_PART as i64 * SECS_PER_MINUTE
            + MAX_SECOND_PART as i64;

        let cases = vec![
            ("11:30:45.123456", "00:00:14.876545", "11:31:00.000001"),
            ("11:30:45.123456", "00:30:00", "12:00:45.123456"),
            ("11:30:45.123456", "12:30:00", "1 00:00:45.123456"),
            ("11:30:45.123456", "1 12:30:00", "2 00:00:45.123456"),
        ];
        for (lhs, rhs, exp) in cases.clone() {
            let lhs = Duration::parse(&mut EvalContext::default(), lhs, 6).unwrap();
            let rhs = Duration::parse(&mut EvalContext::default(), rhs, 6).unwrap();
            let res = lhs.checked_add(rhs).unwrap();
            let exp = Duration::parse(&mut EvalContext::default(), exp, 6).unwrap();
            assert_eq!(res, exp);
        }
        for (exp, rhs, lhs) in cases {
            let lhs = Duration::parse(&mut EvalContext::default(), lhs, 6).unwrap();
            let rhs = Duration::parse(&mut EvalContext::default(), rhs, 6).unwrap();
            let res = lhs.checked_sub(rhs).unwrap();
            let exp = Duration::parse(&mut EvalContext::default(), exp, 6).unwrap();
            assert_eq!(res, exp);
        }

        let lhs = Duration::parse(&mut EvalContext::default(), "00:00:01", 6).unwrap();
        let rhs = Duration::from_nanos(MAX_TIME_IN_SECS * NANOS_PER_SEC, 6).unwrap();
        assert_eq!(lhs.checked_add(rhs), None);
        let lhs = Duration::parse(&mut EvalContext::default(), "-00:00:01", 6).unwrap();
        let rhs = Duration::from_nanos(MAX_TIME_IN_SECS * NANOS_PER_SEC, 6).unwrap();
        assert_eq!(lhs.checked_sub(rhs), None);
    }

    #[test]
    fn test_from_i64() {
        let cs: Vec<(i64, i8, Result<Duration>, bool)> = vec![
            // (input, fsp, expect, overflow)
            // UNSPECIFIED_FSP
            (
                8385959,
                UNSPECIFIED_FSP as i8,
                Ok(Duration::parse(&mut EvalContext::default(), "838:59:59", 0).unwrap()),
                false,
            ),
            (
                101010,
                0,
                Ok(Duration::parse(&mut EvalContext::default(), "10:10:10", 0).unwrap()),
                false,
            ),
            (
                101010,
                5,
                Ok(Duration::parse(&mut EvalContext::default(), "10:10:10", 5).unwrap()),
                false,
            ),
            (
                8385959,
                0,
                Ok(Duration::parse(&mut EvalContext::default(), "838:59:59", 0).unwrap()),
                false,
            ),
            (
                8385959,
                6,
                Ok(Duration::parse(&mut EvalContext::default(), "838:59:59", 6).unwrap()),
                false,
            ),
            (
                -101010,
                0,
                Ok(Duration::parse(&mut EvalContext::default(), "-10:10:10", 0).unwrap()),
                false,
            ),
            (
                -101010,
                5,
                Ok(Duration::parse(&mut EvalContext::default(), "-10:10:10", 5).unwrap()),
                false,
            ),
            (
                -8385959,
                0,
                Ok(Duration::parse(&mut EvalContext::default(), "-838:59:59", 0).unwrap()),
                false,
            ),
            (
                -8385959,
                6,
                Ok(Duration::parse(&mut EvalContext::default(), "-838:59:59", 6).unwrap()),
                false,
            ),
            // will overflow
            (8385960, 0, Err(Error::overflow("Duration", 8385960)), false),
            (8385960, 1, Err(Error::overflow("Duration", 8385960)), false),
            (8385960, 5, Err(Error::overflow("Duration", 8385960)), false),
            (8385960, 6, Err(Error::overflow("Duration", 8385960)), false),
            (
                -8385960,
                0,
                Err(Error::overflow("Duration", 8385960)),
                false,
            ),
            (
                -8385960,
                1,
                Err(Error::overflow("Duration", 8385960)),
                false,
            ),
            (
                -8385960,
                5,
                Err(Error::overflow("Duration", 8385960)),
                false,
            ),
            (
                -8385960,
                6,
                Err(Error::overflow("Duration", 8385960)),
                false,
            ),
            // will truncated
            (8376049, 0, Err(Error::truncated_wrong_val("", "")), false),
            (8375960, 0, Err(Error::truncated_wrong_val("", "")), false),
            (8376049, 0, Err(Error::truncated_wrong_val("", "")), false),
            (
                10000000000,
                0,
                Ok(Duration::new_from_parts(false, 0, 0, 0, 0, 0).unwrap()),
                false,
            ),
            (
                10000235959,
                0,
                Ok(Duration::new_from_parts(false, 23, 59, 59, 0, 0).unwrap()),
                false,
            ),
            (
                -10000235959,
                0,
                Err(Error::overflow("Duration", "-10000235959")),
                false,
            ),
        ];
        for (input, fsp, expect, overflow) in cs {
            let cfg = Arc::new(EvalConfig::from_flag(Flag::OVERFLOW_AS_WARNING));
            let mut ctx = EvalContext::new(cfg);

            let r = Duration::from_i64(&mut ctx, input, fsp);

            let expect_str = if expect.is_ok() {
                format!("{}", expect.as_ref().unwrap())
            } else {
                format!("{:?}", &expect)
            };
            let result_str = if r.is_ok() {
                format!("{}", r.as_ref().unwrap())
            } else {
                format!("{:?}", &r)
            };
            let log = format!(
                "input: {}, fsp: {}, expect: {}, output: {}",
                input, fsp, expect_str, result_str
            );

            assert_eq!(r.is_ok(), expect.is_ok(), "{}", log.as_str());
            if let Ok(r) = r {
                assert_eq!(r, expect.unwrap(), "{}", log.as_str());
            } else {
                let e = r.err().unwrap();
                let e2 = expect.err().unwrap();
                assert_eq!(e.code(), e2.code(), "{}", log.as_str());
            }
            if overflow {
                assert_eq!(ctx.warnings.warning_cnt, 1, "{}", log.as_str());
                assert_eq!(
                    ctx.warnings.warnings[0].get_code(),
                    ERR_DATA_OUT_OF_RANGE,
                    "{}",
                    log.as_str()
                );
            }
        }
    }
}

#[cfg(test)]
mod benches {
    use super::*;
    use crate::codec::mysql::MAX_FSP;

    #[bench]
    fn bench_parse(b: &mut test::Bencher) {
        let cases = vec![
            ("12:34:56.1234", 0),
            ("12:34:56.789", 1),
            ("10:20:30.189", 2),
            ("2 27:54:32.828", 3),
            ("2 33:44:55.666777", 4),
            ("112233.445566", 5),
            ("1 23:12.1234567", 6),
        ];

        b.iter(|| {
            let cases = test::black_box(&cases);
            for &(s, fsp) in cases {
                let _ =
                    test::black_box(Duration::parse(&mut EvalContext::default(), s, fsp).unwrap());
            }
        })
    }

    #[bench]
    fn bench_hours(b: &mut test::Bencher) {
        let cases = &(3600..=7200)
            .map(|second| Duration::from_millis(second * 1000, MAX_FSP).unwrap())
            .collect::<Vec<Duration>>();

        b.iter(|| {
            for duration in cases {
                let duration = test::black_box(duration);
                let _ = test::black_box(duration.hours());
            }
        })
    }

    #[bench]
    fn bench_to_decimal(b: &mut test::Bencher) {
        let duration = Duration::parse(&mut EvalContext::default(), "-12:34:56.123456", 6).unwrap();
        b.iter(|| {
            let duration = test::black_box(duration);
            let dec: Result<Decimal> = duration.convert(&mut EvalContext::default());
            let _ = test::black_box(dec.unwrap());
        })
    }

    #[bench]
    fn bench_round_frac(b: &mut test::Bencher) {
        let (duration, fsp) = (
            Duration::parse(&mut EvalContext::default(), "12:34:56.789", 3).unwrap(),
            2,
        );
        b.iter(|| {
            let (duration, fsp) = (test::black_box(duration), test::black_box(fsp));
            let _ = test::black_box(duration.round_frac(fsp).unwrap());
        })
    }

    #[bench]
    fn bench_codec(b: &mut test::Bencher) {
        let cases: Vec<_> = vec![
            ("12:34:56.1234", 0),
            ("12:34:56.789", 1),
            ("10:20:30.189", 2),
            ("2 27:54:32.828", 3),
            ("2 33:44:55.666777", 4),
            ("112233.445566", 5),
            ("1 23", 5),
            ("1 23:12.1234567", 6),
        ]
        .into_iter()
        .map(|(s, fsp)| Duration::parse(&mut EvalContext::default(), s, fsp).unwrap())
        .collect();
        b.iter(|| {
            let cases = test::black_box(&cases);
            for &duration in cases {
                let t = test::black_box(duration);
                let mut buf = vec![];
                buf.write_duration_to_chunk(t).unwrap();
                let got = test::black_box(
                    buf.as_slice()
                        .read_duration_from_chunk(t.fsp() as isize)
                        .unwrap(),
                );
                assert_eq!(t, got);
            }
        })
    }

    #[bench]
    fn bench_checked_add_and_sub_duration(b: &mut test::Bencher) {
        let cases: Vec<_> = vec![
            ("11:30:45.123456", "00:00:14.876545"),
            ("11:30:45.123456", "00:30:00"),
            ("11:30:45.123456", "12:30:00"),
            ("11:30:45.123456", "1 12:30:00"),
        ]
        .into_iter()
        .map(|(lhs, rhs)| {
            (
                Duration::parse(&mut EvalContext::default(), lhs, MAX_FSP).unwrap(),
                Duration::parse(&mut EvalContext::default(), rhs, MAX_FSP).unwrap(),
            )
        })
        .collect();

        b.iter(|| {
            let cases = test::black_box(&cases);
            for &(lhs, rhs) in cases {
                let _ = test::black_box(lhs.checked_add(rhs).unwrap());
                let _ = test::black_box(lhs.checked_sub(rhs).unwrap());
            }
        })
    }
}
