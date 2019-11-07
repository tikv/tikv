// Copyright 2016 TiKV Project Authors. Licensed under Apache-2.0.

use std::cmp::Ordering;
use std::fmt::{self, Display, Formatter};
use std::{i64, u64};

use codec::prelude::*;

use super::{check_fsp, Decimal};
use crate::codec::convert::ConvertTo;
use crate::codec::error::{ERR_DATA_OUT_OF_RANGE, ERR_TRUNCATE_WRONG_VALUE};
use crate::codec::mysql::MAX_FSP;
use crate::codec::{Error, Result, TEN_POW};
use crate::expr::EvalContext;

use bitfield::bitfield;

pub const NANOS_PER_SEC: i64 = 1_000_000_000;
pub const MICROS_PER_SEC: i64 = 1_000_000;
pub const NANO_WIDTH: usize = 9;
pub const MICRO_WIDTH: usize = 6;

const SECS_PER_HOUR: u32 = 3600;
const SECS_PER_MINUTE: u32 = 60;

const MAX_HOURS: u32 = 838;
const MAX_MINUTES: u32 = 59;
const MAX_SECONDS: u32 = 59;
const MAX_MICROS: u32 = 999_999;
const MAX_DURATION_VALUE: u32 = MAX_HOURS * 10000 + MAX_MINUTES * 100 + MAX_SECONDS;

#[inline]
fn check_hour(hour: u32) -> Result<u32> {
    if hour > MAX_HOURS {
        Err(Error::Eval(
            "DURATION OVERFLOW".to_string(),
            ERR_DATA_OUT_OF_RANGE,
        ))
    } else {
        Ok(hour)
    }
}

#[inline]
fn check_minute(minute: u32) -> Result<u32> {
    if minute > MAX_MINUTES {
        Err(Error::truncated_wrong_val("MINUTES", minute))
    } else {
        Ok(minute)
    }
}

#[inline]
fn check_second(second: u32) -> Result<u32> {
    if second > MAX_SECONDS {
        Err(Error::truncated_wrong_val("SECONDS", second))
    } else {
        Ok(second)
    }
}

#[inline]
fn check_micros(micros: u32) -> Result<u32> {
    if micros > MAX_MICROS {
        Err(Error::truncated_wrong_val("MICROS", micros))
    } else {
        Ok(micros)
    }
}

mod parser {
    use super::{check_hour, check_minute, check_second, Error, MICRO_WIDTH, TEN_POW};
    use nom::character::complete::{digit1, multispace0, multispace1};
    use nom::{
        alt, call, char, complete, cond, do_parse, eof, map, map_res, opt, peek, preceded, tag,
        IResult,
    };

    #[inline]
    fn buf_to_int(buf: &[u8]) -> u32 {
        buf.iter().fold(0, |acc, c| acc * 10 + u32::from(c - b'0'))
    }

    /// Extracts a `u32` from a buffer which matches pattern: `\d+.*`
    ///
    /// ```ignore
    /// assert_eq!(read_int(b"123abc"), Ok((b"abc", 123)));
    /// assert_eq!(read_int(b"12:34"), Ok((b":34", 12)));
    /// assert!(read_int(b"12345678:1").is_err());
    /// ```
    ///
    /// NOTE:
    /// The range of MySQL's TIME is `-838:59:59 ~ 838:59:59`, so we can at most read 7 digits
    /// (pattern like `HHHMMSS`)
    fn read_int(input: &[u8]) -> IResult<&[u8], u32> {
        map_res!(input, digit1, |buf: &[u8]| {
            if buf.len() <= 7 {
                Ok(buf_to_int(buf))
            } else {
                Err(Error::truncated_wrong_val("TIME DIGITS", 7))
            }
        })
    }

    /// Extracts a `u32` with length `fsp` from a buffer which matches pattern: `\d+.*`
    /// This function assumes that `fsp` is valid
    ///
    /// ```ignore
    /// assert_eq!(read_int_with_fsp(b"1234567", 3), Ok(b"", 123400000));
    /// assert_eq!(read_int_with_fsp(b"1234", 6), Ok(b"", 123400000));
    /// ```
    ///
    /// NOTE:
    /// 1. The behavior of this function is similar to `read_int` except that it's designed to read the
    /// fractional part of a `TIME`
    /// 2. The fractional part will be align to a 9-digit number which it's easy to round with `fsp`
    ///
    /// FIXME: the fraction should not be round, it's incompatible with MySQL.
    fn read_int_with_fsp(input: &[u8], fsp: u8) -> IResult<&[u8], u32> {
        map!(input, digit1, |buf: &[u8]| -> u32 {
            let fsp = usize::from(fsp);
            let (fraction, len) = if fsp >= buf.len() {
                (buf_to_int(buf), buf.len())
            } else {
                (buf_to_int(&buf[..=fsp]), fsp + 1)
            };
            fraction * TEN_POW[MICRO_WIDTH.checked_sub(len).unwrap_or(0)]
        })
    }

    /// Parse the sign of `Duration`, return true if it's negative otherwise false
    ///
    /// ```ignore
    /// assert_eq!(neg(b"- .123"),  Ok(b".123", true));
    /// assert_eq!(neg(b"-.123"),   Ok(b".123", true));
    /// assert_eq!(neg(b"- 11:21"), Ok(b"11:21", true));
    /// assert_eq!(neg(b"-11:21"),  Ok(b"11:21", true));
    /// assert_eq!(neg(b"11:21"),   Ok(b"11:21", false));
    /// ```
    fn neg(input: &[u8]) -> IResult<&[u8], bool> {
        do_parse!(
            input,
            neg: map!(opt!(complete!(char!('-'))), |flag| flag.is_some())
                >> preceded!(
                    multispace0,
                    alt!(complete!(peek!(call!(digit1))) | complete!(peek!(tag!("."))))
                )
                >> (neg)
        )
    }

    /// Parse the day/block(format like `HHMMSS`) value of the `Duration`,
    /// further paring will determine the value we got is a `day` value or `block` value.
    ///
    /// ```ignore
    /// assert_eq!(day(b"1 1:1"), Ok(b"1:1", Some(1)));
    /// assert_eq!(day(b"1234"), Ok(b"", Some(1234)));
    /// assert_eq!(day(b"1234.123"), Ok(b".123", Some(1234)));
    /// assert_eq!(day(b"1:2:3"), Ok(b"1:2:3", None));
    /// assert_eq!(day(b".123"), Ok(b".123", None));
    /// ```
    fn day(input: &[u8]) -> IResult<&[u8], Option<u32>> {
        opt!(
            input,
            do_parse!(
                day: read_int
                    >> alt!(
                        complete!(preceded!(multispace1, peek!(call!(digit1))))
                            | complete!(preceded!(
                                multispace0,
                                alt!(complete!(peek!(tag!("."))) | complete!(eof!()))
                            ))
                    )
                    >> (day)
            )
        )
    }

    /// Parse a separator ':'
    ///
    /// ```ignore
    /// assert_eq!(separator(b" : "), Ok(b"", true));
    /// assert_eq!(separator(b":"), Ok(b"", true));
    /// assert_eq!(separator(b";"), Ok(b";", false));
    /// ```
    fn separator(input: &[u8]) -> IResult<&[u8], bool> {
        do_parse!(
            input,
            multispace0
                >> has_separator: map!(opt!(complete!(char!(':'))), |flag| flag.is_some())
                >> multispace0
                >> (has_separator)
        )
    }

    /// Parse format like: `hh:mm` `hh:mm:ss`
    ///
    /// ```ignore
    /// assert_eq!(hhmmss(b"12:34:56"), Ok(b"", [12, 34, 56]));
    /// assert_eq!(hhmmss(b"12:34"), Ok(b"", [12, 34, None]));
    /// assert_eq!(hhmmss(b"1234"), Ok(b"", [None, None, None]));
    /// ```
    fn hhmmss(input: &[u8]) -> IResult<&[u8], [Option<u32>; 3]> {
        do_parse!(
            input,
            hour: opt!(map_res!(read_int, check_hour))
                >> has_mintue: separator
                >> minute: cond!(has_mintue, map_res!(read_int, check_minute))
                >> has_second: separator
                >> second: cond!(has_second, map_res!(read_int, check_second))
                >> ([hour, minute, second])
        )
    }

    /// Parse fractional part.
    ///
    /// ```ignore
    /// assert_eq!(fraction(" .123", 3), Ok(b"", Some(123)));
    /// assert_eq!(fraction("123", 3), Ok(b"", None));
    /// ```
    fn fraction(input: &[u8], fsp: u8) -> IResult<&[u8], Option<u32>> {
        do_parse!(
            input,
            multispace0
                >> opt!(complete!(char!('.')))
                >> fraction: opt!(call!(read_int_with_fsp, fsp))
                >> multispace0
                >> (fraction)
        )
    }

    /// Parse `Duration`
    pub fn parse(input: &[u8], fsp: u8) -> IResult<&[u8], (bool, [Option<u32>; 5])> {
        do_parse!(
            input,
            multispace0
                >> neg: neg
                >> day: day
                >> hhmmss: hhmmss
                >> fraction: call!(fraction, fsp)
                >> eof!()
                >> (neg, [day, hhmmss[0], hhmmss[1], hhmmss[2], fraction])
        )
    }
} /* parser */

bitfield! {
    #[derive(Clone, Copy)]
    pub struct Duration(u64);
    impl Debug;
    #[inline]
    bool, get_neg, set_neg: 63;
    #[inline]
    bool, get_reserved, set_reserved: 62;
    #[inline]
    u32, get_hours, set_hours: 61, 48;
    #[inline]
    u32, get_minutes, set_minutes: 47, 40;
    #[inline]
    u32, get_secs, set_secs: 39, 32;
    #[inline]
    u32, get_micros, set_micros: 31, 8;
    #[inline]
    u8, get_fsp, set_fsp: 7, 0;
}

/// Rounds `micros` with `fsp` and handles the carry.
#[inline]
fn round(
    hours: &mut u32,
    minutes: &mut u32,
    secs: &mut u32,
    micros: &mut u32,
    fsp: u8,
) -> Result<()> {
    if *micros < 1_000_000 {
        *micros *= 10;
    }

    let fsp = usize::from(fsp);

    *micros = if fsp == MICRO_WIDTH {
        (*micros + 5) / 10
    } else {
        let mask = TEN_POW[MICRO_WIDTH - fsp];
        (*micros / mask + 5) / 10 * mask
    };

    if *micros >= 1_000_000 {
        *micros -= 1_000_000;
        *secs += 1;
        if *secs >= 60 {
            *secs -= 60;
            *minutes += 1;
        }
        if *minutes >= 60 {
            *minutes -= 60;
            *hours += 1;
        }
    }

    check_hour(*hours)?;
    Ok(())
}

impl Duration {
    /// Raw transmutation to u64.
    #[inline]
    pub fn to_bits(self) -> u64 {
        self.0
    }

    #[inline]
    pub fn neg(self) -> bool {
        self.get_neg()
    }

    #[inline]
    pub fn hours(self) -> u32 {
        self.get_hours()
    }

    #[inline]
    pub fn minutes(self) -> u32 {
        self.get_minutes()
    }

    #[inline]
    pub fn secs(self) -> u32 {
        self.get_secs()
    }

    #[inline]
    pub fn micros(self) -> u32 {
        self.get_micros()
    }

    #[inline]
    pub fn fsp(self) -> u8 {
        self.get_fsp()
    }

    #[inline]
    pub fn maximize_fsp(mut self) -> Self {
        self.set_fsp(MAX_FSP as u8);
        self
    }

    /// Raw transmutation from u64.
    pub fn from_bits(v: u64) -> Result<Duration> {
        let mut duration = Duration(v);

        check_micros(duration.micros())?;
        check_second(duration.secs())?;
        check_minute(duration.minutes())?;
        check_hour(duration.hours())?;

        duration.set_reserved(false);
        Ok(duration)
    }

    /// Returns the identity element of `Duration`
    pub fn zero() -> Duration {
        Duration(0)
    }

    /// Returns true if self is equal to the additive identity.
    pub fn is_zero(mut self) -> bool {
        self.set_neg(false);
        self.set_fsp(0);
        self.to_bits() == 0
    }

    /// Returns the absolute value of `Duration`
    pub fn abs(mut self) -> Self {
        self.set_neg(false);
        self
    }

    /// Returns the fractional part of `Duration`, in whole microseconds.
    pub fn subsec_micros(self) -> u32 {
        self.micros()
    }

    /// Returns the number of whole seconds contained by this Duration.
    pub fn to_secs(self) -> i32 {
        let secs =
            (self.hours() * SECS_PER_HOUR + self.minutes() * SECS_PER_MINUTE + self.secs()) as i32;

        if self.get_neg() {
            -secs
        } else {
            secs
        }
    }

    /// Returns the number of seconds contained by this Duration as f64.
    /// The returned value does include the fractional (nanosecond) part of the duration.
    pub fn to_secs_f64(self) -> f64 {
        let secs = f64::from(self.to_secs());
        let micros = f64::from(self.subsec_micros()) * 1e-6;

        secs + if self.get_neg() { -micros } else { micros }
    }

    /// Returns the `Duration` in whole nanoseconds
    pub fn to_nanos(self) -> i64 {
        let secs = i64::from(self.to_secs()) * NANOS_PER_SEC;
        let micros = i64::from(self.subsec_micros());

        secs + if self.get_neg() { -micros } else { micros } * 1000
    }

    /// Constructs a `Duration` from `nanos` with `fsp`
    pub fn from_nanos(nanos: i64, fsp: i8) -> Result<Duration> {
        Duration::from_micros(nanos / 1000, fsp)
    }

    pub fn from_micros(micros: i64, fsp: i8) -> Result<Duration> {
        let fsp = check_fsp(fsp)?;
        let neg = micros < 0;

        let secs = (micros / MICROS_PER_SEC).abs();
        let mut micros = (micros % MICROS_PER_SEC).abs() as u32;

        let mut hours = (secs / i64::from(SECS_PER_HOUR)) as u32;
        let mut minutes = (secs % i64::from(SECS_PER_HOUR) / i64::from(SECS_PER_MINUTE)) as u32;
        let mut secs = (secs % 60) as u32;

        round(&mut hours, &mut minutes, &mut secs, &mut micros, fsp)?;
        Ok(Duration::new(neg, hours, minutes, secs, micros, fsp))
    }

    pub fn from_millis(millis: i64, fsp: i8) -> Result<Duration> {
        Duration::from_micros(
            millis.checked_mul(1000).ok_or_else(|| {
                Error::Eval("DURATION OVERFLOW".to_string(), ERR_DATA_OUT_OF_RANGE)
            })?,
            fsp,
        )
    }

    /// Constructs a `Duration` from with details without validation
    fn new(neg: bool, hours: u32, minutes: u32, secs: u32, micros: u32, fsp: u8) -> Duration {
        let mut duration = Duration(0);

        duration.set_neg(neg);
        duration.set_hours(hours);
        duration.set_minutes(minutes);
        duration.set_secs(secs);
        duration.set_micros(micros);
        duration.set_fsp(fsp);

        duration
    }

    /// Parses the time form a formatted string with a fractional seconds part,
    /// returns the duration type `Time` value.
    /// See: http://dev.mysql.com/doc/refman/5.7/en/fractional-seconds.html
    pub fn parse(input: &[u8], fsp: i8) -> Result<Duration> {
        let fsp = check_fsp(fsp)?;

        if input.is_empty() {
            return Ok(Duration::zero());
        }

        let (mut neg, [mut day, mut hour, mut minute, mut second, micros]) =
            self::parser::parse(input, fsp)
                .map_err(|_| Error::truncated_wrong_val("time", format!("{:?}", input)))?
                .1;

        if day.is_some() && hour.is_none() {
            let block = day.take().unwrap();
            hour = Some(block / 10_000);
            minute = Some(block / 100 % 100);
            second = Some(block % 100);
        }

        let (mut hour, mut minute, mut second, mut micros) = (
            hour.unwrap_or(0) + day.unwrap_or(0) * 24,
            minute.unwrap_or(0),
            second.unwrap_or(0),
            micros.unwrap_or(0),
        );

        if hour == 0 && minute == 0 && second == 0 && micros == 0 {
            neg = false;
        }

        round(&mut hour, &mut minute, &mut second, &mut micros, fsp)?;
        Ok(Duration::new(neg, hour, minute, second, micros, fsp))
    }

    /// Rounds fractional seconds precision with new FSP and returns a new one.
    /// We will use the “round half up” rule, e.g, >= 0.5 -> 1, < 0.5 -> 0,
    /// so 10:10:10.999999 round with fsp: 1 -> 10:10:11.0
    /// and 10:10:10.000000 round with fsp: 0 -> 10:10:11
    pub fn round_frac(mut self, fsp: i8) -> Result<Self> {
        let fsp = check_fsp(fsp)?;

        if fsp >= self.fsp() {
            self.set_fsp(fsp);
            return Ok(self);
        }

        let mut hours = self.hours();
        let mut minutes = self.minutes();
        let mut secs = self.secs();
        let mut micros = self.micros();

        round(&mut hours, &mut minutes, &mut secs, &mut micros, fsp)?;

        Ok(Duration::new(
            self.get_neg(),
            hours,
            minutes,
            secs,
            micros,
            fsp,
        ))
    }

    /// Checked duration addition. Computes self + rhs, returning None if overflow occurred.
    pub fn checked_add(self, rhs: Duration) -> Option<Duration> {
        match (self.get_neg(), rhs.get_neg()) {
            (false, true) => self.checked_sub(rhs.abs()),
            (true, false) => rhs.checked_sub(self.abs()),
            (true, true) => self.abs().checked_add(rhs.abs()).map(|mut res| {
                res.set_neg(true);
                res
            }),
            (false, false) => {
                let mut micros = self.micros() + rhs.micros();
                let mut secs = self.secs() + rhs.secs();
                let mut minutes = self.minutes() + rhs.minutes();
                let mut hours = self.hours() + rhs.hours();

                if i64::from(micros) >= MICROS_PER_SEC {
                    micros -= MICROS_PER_SEC as u32;
                    secs += 1;
                }
                if secs >= 60 {
                    secs -= 60;
                    minutes += 1;
                }
                if minutes >= 60 {
                    minutes -= 60;
                    hours += 1;
                }

                check_hour(hours).ok()?;

                Some(Duration::new(
                    false,
                    hours,
                    minutes,
                    secs,
                    micros,
                    self.fsp().max(rhs.fsp()),
                ))
            }
        }
    }

    /// Checked duration subtraction. Computes self - rhs, returning None if overflow occurred.
    pub fn checked_sub(self, rhs: Duration) -> Option<Duration> {
        match (self.get_neg(), rhs.get_neg()) {
            (false, true) => self.checked_add(rhs.abs()),
            (true, false) => self.abs().checked_add(rhs.abs()).map(|mut res| {
                res.set_neg(true);
                res
            }),
            (true, true) => rhs.abs().checked_sub(self.abs()),
            (false, false) => {
                let neg = self < rhs;

                let (l, r) = if neg { (rhs, self) } else { (self, rhs) };

                let mut micros = l.micros() as i32 - r.micros() as i32;
                let mut secs = l.secs() as i32 - r.secs() as i32;
                let mut minutes = l.minutes() as i32 - r.minutes() as i32;
                let mut hours = l.hours() as i32 - r.hours() as i32;

                if micros < 0 {
                    micros += MICROS_PER_SEC as i32;
                    secs -= 1;
                }

                if secs < 0 {
                    secs += 60;
                    minutes -= 1;
                }

                if minutes < 0 {
                    minutes += 60;
                    hours -= 1;
                }

                Some(Duration::new(
                    neg,
                    hours as u32,
                    minutes as u32,
                    secs as u32,
                    micros as u32,
                    self.fsp().max(rhs.fsp()),
                ))
            }
        }
    }

    fn format(self, sep: &str) -> String {
        use std::fmt::Write;
        let res_max_len = 8 + 2 * sep.len() + MAX_FSP as usize;
        let mut string = String::with_capacity(res_max_len);
        if self.get_neg() {
            string.push('-');
        }

        write!(
            &mut string,
            "{:02}{}{:02}{}{:02}",
            self.hours(),
            sep,
            self.minutes(),
            sep,
            self.secs()
        )
        .unwrap();

        let fsp = usize::from(self.fsp());

        if self.fsp() > 0 {
            write!(
                &mut string,
                ".{:0width$}",
                self.micros() / TEN_POW[MICRO_WIDTH - fsp],
                width = fsp
            )
            .unwrap();
        }

        string
    }

    /// Converts a `Duration` to printable numeric string representation
    #[inline]
    pub fn to_numeric_string(self) -> String {
        self.format("")
    }

    /// If the error is overflow, the result will be returned, too.
    /// Otherwise, only one of result or err will be returned
    pub fn from_i64_without_ctx(mut n: i64, fsp: i8) -> Result<Duration> {
        let fsp = check_fsp(fsp)?;
        if n > i64::from(MAX_DURATION_VALUE) || n < -i64::from(MAX_DURATION_VALUE) {
            // FIXME: parse as `DateTime` if `n >= 10000000000`
            return Err(Error::overflow("Duration", n));
        }

        let negative = n < 0;
        if negative {
            n = -n;
        }
        if n / 10000 > i64::from(MAX_HOURS) || n % 100 >= 60 || (n / 100) % 100 >= 60 {
            return Err(Error::Eval(
                format!("invalid time format: '{}'", n),
                ERR_TRUNCATE_WRONG_VALUE,
            ));
        }
        let dur = Duration::new(
            negative,
            (n / 10000) as u32,
            ((n / 100) % 100) as u32,
            (n % 100) as u32,
            0,
            fsp,
        );
        Ok(dur)
    }

    pub fn from_i64(ctx: &mut EvalContext, n: i64, fsp: i8) -> Result<Duration> {
        Duration::from_i64_without_ctx(n, fsp).or_else(|e| {
            if e.is_overflow() {
                ctx.handle_overflow_err(e)?;
                // Returns max duration if overflow occurred
                Ok(Duration::new(
                    n < 0,
                    MAX_HOURS,
                    MAX_MINUTES,
                    MAX_SECONDS,
                    0,
                    fsp as u8,
                ))
            } else {
                Err(e)
            }
        })
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
        Ok(r?)
    }
}

impl Display for Duration {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        write!(formatter, "{}", self.format(":"))
    }
}

impl PartialEq for Duration {
    fn eq(&self, dur: &Duration) -> bool {
        let (mut a, mut b) = (*self, *dur);
        a.set_fsp(0);
        b.set_fsp(0);
        a.0 == b.0
    }
}

impl std::hash::Hash for Duration {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        let mut inner = *self;
        inner.set_fsp(0);
        inner.to_bits().hash(state)
    }
}

impl PartialOrd for Duration {
    fn partial_cmp(&self, dur: &Duration) -> Option<Ordering> {
        let mut a = *self;
        let mut b = *dur;

        a.set_fsp(0);
        b.set_fsp(0);

        let ordering = a.0.cmp(&b.0);
        Some(if a.get_neg() || b.get_neg() {
            ordering.reverse()
        } else {
            ordering
        })
    }
}

impl Eq for Duration {}

impl Ord for Duration {
    fn cmp(&self, dur: &Duration) -> Ordering {
        self.partial_cmp(dur).unwrap()
    }
}

impl<T: BufferWriter> DurationEncoder for T {}

pub trait DurationEncoder: NumberEncoder {
    fn write_duration(&mut self, v: Duration) -> Result<()> {
        self.write_i64(v.to_nanos())?;
        self.write_i64(i64::from(v.get_fsp())).map_err(From::from)
    }

    fn write_duration_to_chunk(&mut self, v: Duration) -> Result<()> {
        self.write_i64_le(v.to_nanos())?;
        Ok(())
    }
}

pub trait DurationDecoder: NumberDecoder {
    /// `read_duration` decodes duration encoded by `write_duration`.
    fn read_duration(&mut self) -> Result<Duration> {
        let nanos = self.read_i64()?;
        let fsp = self.read_i64()?;
        Duration::from_nanos(nanos, fsp as i8)
    }

    fn read_duration_from_chunk(&mut self, fsp: isize) -> Result<Duration> {
        let nanos = self.read_i64_le()?;
        Duration::from_nanos(nanos, fsp as i8)
    }
}

impl<T: BufferReader> DurationDecoder for T {}

impl crate::codec::data_type::AsMySQLBool for Duration {
    #[inline]
    fn as_mysql_bool(&self, _context: &mut crate::expr::EvalContext) -> crate::Result<bool> {
        Ok(!self.is_zero())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::codec::data_type::DateTime;
    use crate::codec::mysql::UNSPECIFIED_FSP;
    use crate::expr::{EvalConfig, EvalContext, Flag};
    use std::f64::EPSILON;
    use std::sync::Arc;

    #[test]
    fn test_hours() {
        let cases: Vec<(&str, i8, u32)> = vec![
            ("31 11:30:45", 0, 31 * 24 + 11),
            ("11:30:45", 0, 11),
            ("-11:30:45.9233456", 0, 11),
            ("272:59:59", 0, 272),
        ];

        for (input, fsp, exp) in cases {
            let dur = Duration::parse(input.as_bytes(), fsp).unwrap();
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
            let dur = Duration::parse(input.as_bytes(), fsp).unwrap();
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
            let dur = Duration::parse(input.as_bytes(), fsp).unwrap();
            let res = dur.secs();
            assert_eq!(exp, res);
        }
    }

    #[test]
    fn test_micros() {
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
            let dur = Duration::parse(input.as_bytes(), fsp).unwrap();
            let res = dur.micros();
            assert_eq!(exp, res);
        }
    }

    #[test]
    fn test_parse() {
        let cases: Vec<(&'static [u8], i8, Option<&'static str>)> = vec![
            (b"10:11:12", 0, Some("10:11:12")),
            (b"101112", 0, Some("10:11:12")),
            (b"10:11", 0, Some("10:11:00")),
            (b"101112.123456", 0, Some("10:11:12")),
            (b"1112", 0, Some("00:11:12")),
            (b"12", 0, Some("00:00:12")),
            (b"1 12", 0, Some("36:00:00")),
            (b"1 10:11:12", 0, Some("34:11:12")),
            (b"1 10:11:12.123456", 0, Some("34:11:12")),
            (b"1 10:11:12.123456", 4, Some("34:11:12.1235")),
            (b"1 10:11:12.12", 4, Some("34:11:12.1200")),
            (b"1 10:11:12.1234565", 6, Some("34:11:12.123457")),
            (b"1 10:11:12.9999995", 6, Some("34:11:13.000000")),
            (b"1 10:11:12.123456", 7, None),
            (b"10:11:12.123456", 0, Some("10:11:12")),
            (b"1 10:11", 0, Some("34:11:00")),
            (b"1 10", 0, Some("34:00:00")),
            (b"24 10", 0, Some("586:00:00")),
            (b"-24 10", 0, Some("-586:00:00")),
            (b"0 10", 0, Some("10:00:00")),
            (b"-10:10:10", 0, Some("-10:10:10")),
            (b"-838:59:59", 0, Some("-838:59:59")),
            (b"838:59:59", 0, Some("838:59:59")),
            (b"23:60:59", 0, None),
            (b"54:59:59", 0, Some("54:59:59")),
            (b"2011-11-11 00:00:01", 0, None),
            (b"2011-11-11", 0, None),
            (b"--23", 0, None),
            (b"232 10", 0, None),
            (b"-232 10", 0, None),
            (b"00:00:00.1", 0, Some("00:00:00")),
            (b"00:00:00.1", 1, Some("00:00:00.1")),
            (b"00:00:00.777777", 2, Some("00:00:00.78")),
            (b"00:00:00.777777", 6, Some("00:00:00.777777")),
            (b"00:00:00.001", 3, Some("00:00:00.001")),
            // NOTE: The following case is easy to fail.
            (b"- 1 ", 0, Some("-00:00:01")),
            (b"1:2:3", 0, Some("01:02:03")),
            (b"1 1:2:3", 0, Some("25:02:03")),
            (b"-1 1:2:3.123", 3, Some("-25:02:03.123")),
            (b"-.123", 3, Some("-00:00:00.123")),
            (b"12345", 0, Some("01:23:45")),
            (b"-123", 0, Some("-00:01:23")),
            (b"-23", 0, Some("-00:00:23")),
            (b"- 1 1", 0, Some("-25:00:00")),
            (b"-1 1", 0, Some("-25:00:00")),
            (b" - 1:2:3 .123 ", 3, Some("-01:02:03.123")),
            (b" - 1 :2 :3 .123 ", 3, Some("-01:02:03.123")),
            (b" - 1 : 2 :3 .123 ", 3, Some("-01:02:03.123")),
            (b" - 1 : 2 :  3 .123 ", 3, Some("-01:02:03.123")),
            (b" - 1 .123 ", 3, Some("-00:00:01.123")),
            (b"-", 0, None),
            (b"", 0, Some("00:00:00")),
            (b"", 7, None),
            (b"18446744073709551615:59:59", 0, None),
            (b"1::2:3", 0, None),
            (b"1.23 3", 0, None),
        ];

        for (input, fsp, expect) in cases {
            let got = Duration::parse(input, fsp);

            if let Some(expect) = expect {
                assert_eq!(
                    expect,
                    &format!(
                        "{}",
                        got.unwrap_or_else(|_| panic!(std::str::from_utf8(input)
                            .unwrap()
                            .to_string()))
                    )
                );
            } else {
                assert!(
                    got.is_err(),
                    format!(
                        "{} should not be passed, got {:?}",
                        std::str::from_utf8(input).unwrap(),
                        got
                    )
                );
            }
        }
    }

    #[test]
    fn test_to_numeric_string() {
        let cases: Vec<(&[u8], i8, &str)> = vec![
            (b"11:30:45.123456", 4, "113045.1235"),
            (b"11:30:45.123456", 6, "113045.123456"),
            (b"11:30:45.123456", 0, "113045"),
            (b"11:30:45.999999", 0, "113046"),
            (b"08:40:59.575601", 0, "084100"),
            (b"23:59:59.575601", 0, "240000"),
            (b"00:00:00", 0, "000000"),
            (b"00:00:00", 6, "000000.000000"),
        ];
        for (s, fsp, expect) in cases {
            let du = Duration::parse(s, fsp).unwrap();
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
            let t = Duration::parse(input.as_bytes(), fsp).unwrap();
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
                (expect - get).abs() < EPSILON,
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
            let t = Duration::parse(input.as_bytes(), MAX_FSP)
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
            let t = Duration::parse(input.as_bytes(), fsp).unwrap();
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
        const MAX_TIME_IN_SECS: i64 =
            (MAX_HOURS * SECS_PER_HOUR + MAX_MINUTES * SECS_PER_MINUTE + MAX_SECONDS) as i64;

        let cases = vec![
            ("11:30:45.123456", "00:00:14.876545", "11:31:00.000001"),
            ("11:30:45.123456", "00:30:00", "12:00:45.123456"),
            ("11:30:45.123456", "12:30:00", "1 00:00:45.123456"),
            ("11:30:45.123456", "1 12:30:00", "2 00:00:45.123456"),
        ];
        for (lhs, rhs, exp) in cases.clone() {
            let lhs = Duration::parse(lhs.as_bytes(), 6).unwrap();
            let rhs = Duration::parse(rhs.as_bytes(), 6).unwrap();
            let res = lhs.checked_add(rhs).unwrap();
            let exp = Duration::parse(exp.as_bytes(), 6).unwrap();
            assert_eq!(res, exp);
        }
        for (exp, rhs, lhs) in cases {
            let lhs = Duration::parse(lhs.as_bytes(), 6).unwrap();
            let rhs = Duration::parse(rhs.as_bytes(), 6).unwrap();
            let res = lhs.checked_sub(rhs).unwrap();
            let exp = Duration::parse(exp.as_bytes(), 6).unwrap();
            assert_eq!(res, exp);
        }

        let lhs = Duration::parse(b"00:00:01", 6).unwrap();
        let rhs = Duration::from_nanos(MAX_TIME_IN_SECS * NANOS_PER_SEC, 6).unwrap();
        assert_eq!(lhs.checked_add(rhs), None);
        let lhs = Duration::parse(b"-00:00:01", 6).unwrap();
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
                Ok(Duration::parse(b"838:59:59", 0).unwrap()),
                false,
            ),
            (
                101010,
                0,
                Ok(Duration::parse(b"10:10:10", 0).unwrap()),
                false,
            ),
            (
                101010,
                5,
                Ok(Duration::parse(b"10:10:10", 5).unwrap()),
                false,
            ),
            (
                8385959,
                0,
                Ok(Duration::parse(b"838:59:59", 0).unwrap()),
                false,
            ),
            (
                8385959,
                6,
                Ok(Duration::parse(b"838:59:59", 6).unwrap()),
                false,
            ),
            (
                -101010,
                0,
                Ok(Duration::parse(b"-10:10:10", 0).unwrap()),
                false,
            ),
            (
                -101010,
                5,
                Ok(Duration::parse(b"-10:10:10", 5).unwrap()),
                false,
            ),
            (
                -8385959,
                0,
                Ok(Duration::parse(b"-838:59:59", 0).unwrap()),
                false,
            ),
            (
                -8385959,
                6,
                Ok(Duration::parse(b"-838:59:59", 6).unwrap()),
                false,
            ),
            // will overflow
            (
                8385960,
                0,
                Ok(Duration::parse(b"838:59:59", 0).unwrap()),
                true,
            ),
            (
                8385960,
                1,
                Ok(Duration::parse(b"838:59:59", 1).unwrap()),
                true,
            ),
            (
                8385960,
                5,
                Ok(Duration::parse(b"838:59:59", 5).unwrap()),
                true,
            ),
            (
                8385960,
                6,
                Ok(Duration::parse(b"838:59:59", 6).unwrap()),
                true,
            ),
            (
                -8385960,
                0,
                Ok(Duration::parse(b"-838:59:59", 0).unwrap()),
                true,
            ),
            (
                -8385960,
                1,
                Ok(Duration::parse(b"-838:59:59", 1).unwrap()),
                true,
            ),
            (
                -8385960,
                5,
                Ok(Duration::parse(b"-838:59:59", 5).unwrap()),
                true,
            ),
            (
                -8385960,
                6,
                Ok(Duration::parse(b"-838:59:59", 6).unwrap()),
                true,
            ),
            // will truncated
            (8376049, 0, Err(Error::truncated_wrong_val("", "")), false),
            (8375960, 0, Err(Error::truncated_wrong_val("", "")), false),
            (8376049, 0, Err(Error::truncated_wrong_val("", "")), false),
            // TODO: fix these test case after Duration::from_f64
            //  had impl logic for num>=10000000000
            (
                10000000000,
                0,
                Ok(Duration::new(
                    false,
                    MAX_HOURS,
                    MAX_MINUTES,
                    MAX_SECONDS,
                    0,
                    0,
                )),
                true,
            ),
            (
                10000235959,
                0,
                Ok(Duration::new(
                    false,
                    MAX_HOURS,
                    MAX_MINUTES,
                    MAX_SECONDS,
                    0,
                    0,
                )),
                true,
            ),
            (
                10000000001,
                0,
                Ok(Duration::new(
                    false,
                    MAX_HOURS,
                    MAX_MINUTES,
                    MAX_SECONDS,
                    0,
                    0,
                )),
                true,
            ),
            (
                10000000000,
                5,
                Ok(Duration::new(
                    false,
                    MAX_HOURS,
                    MAX_MINUTES,
                    MAX_SECONDS,
                    0,
                    5,
                )),
                true,
            ),
            (
                10000235959,
                5,
                Ok(Duration::new(
                    false,
                    MAX_HOURS,
                    MAX_MINUTES,
                    MAX_SECONDS,
                    0,
                    5,
                )),
                true,
            ),
            (
                10000000001,
                5,
                Ok(Duration::new(
                    false,
                    MAX_HOURS,
                    MAX_MINUTES,
                    MAX_SECONDS,
                    0,
                    5,
                )),
                true,
            ),
            (
                10000000000,
                6,
                Ok(Duration::new(
                    false,
                    MAX_HOURS,
                    MAX_MINUTES,
                    MAX_SECONDS,
                    0,
                    6,
                )),
                true,
            ),
            (
                10000235959,
                6,
                Ok(Duration::new(
                    false,
                    MAX_HOURS,
                    MAX_MINUTES,
                    MAX_SECONDS,
                    0,
                    6,
                )),
                true,
            ),
            (
                10000000001,
                6,
                Ok(Duration::new(
                    false,
                    MAX_HOURS,
                    MAX_MINUTES,
                    MAX_SECONDS,
                    0,
                    6,
                )),
                true,
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
                let _ = test::black_box(Duration::parse(s.as_bytes(), fsp).unwrap());
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
        let duration = Duration::parse(b"-12:34:56.123456", 6).unwrap();
        b.iter(|| {
            let duration = test::black_box(duration);
            let dec: Result<Decimal> = duration.convert(&mut EvalContext::default());
            let _ = test::black_box(dec.unwrap());
        })
    }

    #[bench]
    fn bench_round_frac(b: &mut test::Bencher) {
        let (duration, fsp) = (Duration::parse(b"12:34:56.789", 3).unwrap(), 2);
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
        .map(|(s, fsp)| Duration::parse(s.as_bytes(), fsp).unwrap())
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
                Duration::parse(lhs.as_bytes(), MAX_FSP).unwrap(),
                Duration::parse(rhs.as_bytes(), MAX_FSP).unwrap(),
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
