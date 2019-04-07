// Copyright 2016 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

use crate::util::codec::number::{self, NumberEncoder};
use crate::util::codec::BytesSlice;
use bitfield::bitfield;
use std::cmp::Ordering;
use std::fmt::{self, Display, Formatter};
use std::io::Write;
use std::time::Duration as StdDuration;
use std::{i64, str, u64};

use super::super::{Result, TEN_POW};
use super::{check_fsp, Decimal};
use crate::util::escape;

pub const NANOS_PER_SEC: u64 = 1_000_000_000;
pub const NANO_WIDTH: u32 = 9;
const SECS_PER_HOUR: u64 = 3600;
const SECS_PER_MINUTE: u64 = 60;
const MINUTES_PER_HOUR: u64 = 60;
const NANOS_PER_MICRO: u64 = 1_000;
const MAX_HOURS: u64 = 838;
const MAX_MINUTES: u64 = 59;
const MAX_SECONDS: u64 = 59;
const MAX_BLOCK: u64 = 8385959;

#[inline]
fn check_hour(hour: u64) -> Result<u64> {
    if hour > MAX_HOURS {
        Err(invalid_type!(
            "invalid hour: {}, larger than {}",
            hour,
            MAX_HOURS
        ))
    } else {
        Ok(hour)
    }
}
#[inline]
fn check_minute(minute: u64) -> Result<u64> {
    if minute > MAX_MINUTES {
        Err(invalid_type!(
            "invalid minute: {}, larger than {}",
            minute,
            MAX_MINUTES
        ))
    } else {
        Ok(minute)
    }
}
#[inline]
fn check_second(second: u64) -> Result<u64> {
    if second > MAX_SECONDS {
        Err(invalid_type!(
            "invalid second: {}, larger than {}",
            second,
            MAX_SECONDS
        ))
    } else {
        Ok(second)
    }
}

#[inline]
fn check_block(block: u64) -> Result<u64> {
    if block > MAX_BLOCK {
        Err(invalid_type!(
            "invalid block: {}, larger than {}",
            block,
            MAX_BLOCK
        ))
    } else {
        Ok(block)
    }
}

/// `Duration` is the type for MySQL `time` type.
/// It only occupies 8 bytes in memory.
bitfield! {
    // For more information:
    // 1. https://dev.mysql.com/doc/internals/en/date-and-time-data-type-representation.html
    // 2. http://dev.mysql.com/doc/refman/5.7/en/fractional-seconds.html
    #[derive(Clone, Copy)]
    pub struct Duration(u64);
    impl Debug;

    #[inline]
    bool, neg, set_neg: 63;            // 1 bit

    #[inline]
    u64, hour, set_hour: 62, 53;         // 10 bits

    #[inline]
    u64, minute, set_minute: 52, 47;    // 6 bits

    #[inline]
    u64, second, set_second: 46, 41;    // 6 bits

    #[inline]
    u64, nano, set_nano: 40, 9;        // 32 bits

    // generate public method: fsp() (getter) / set_fsp() (setter)
    #[inline]
    pub u8, fsp, set_fsp: 8, 1;           // 8 bits

    #[inline]
    bool, unused, set_unused: 0;                 // 1 bit

}

/// NOTE:
/// 1. It will automatically round the `nano` field according to `fsp` if round_with_fsp is true.
/// 2. `nano` field can be greater than `NANOS_PER_SEC`.
/// 3. Return an `Err` if the result's fields is invalid.
#[derive(Clone, Copy)]
struct DurationBuilder {
    neg: bool,
    hour: u64,
    minute: u64,
    second: u64,
    nano: u64,
    fsp: u8,
    round_with_fsp: bool,
}

impl DurationBuilder {
    pub fn check(self) -> Result<Self> {
        check_hour(self.hour)?;
        check_minute(self.minute)?;
        check_second(self.second)?;
        check_fsp(self.fsp as i8)?;
        Ok(self)
    }
}

impl Duration {
    /// Return Duration: 00:00:00 (fsp == 0)
    pub fn zero() -> Duration {
        Duration(0)
    }

    /// Return the `hour` field of `self`
    #[inline]
    pub fn hours(self) -> u64 {
        self.hour()
    }

    /// Return the `minute` field of `self`
    #[inline]
    pub fn minutes(self) -> u64 {
        self.minute()
    }

    /// Return the `second` field of `self`
    #[inline]
    pub fn secs(self) -> u64 {
        self.second()
    }

    /// Return the `nano` field in microseconds
    #[inline]
    pub fn micro_secs(self) -> u64 {
        self.nano() / NANOS_PER_MICRO
    }

    /// Return the `nano` field
    #[inline]
    pub fn nano_secs(self) -> u64 {
        self.nano()
    }

    /// Returns the number of whole seconds contained by this Duration.
    #[inline]
    pub fn as_secs(self) -> u64 {
        self.hour() * SECS_PER_HOUR + self.minute() * SECS_PER_MINUTE + self.second()
    }

    /// Return self in seconds including fractional part
    #[inline]
    pub fn to_secs(self) -> f64 {
        let nonfrac = self.as_secs() as f64;
        let frac = self.nano() as f64 / NANOS_PER_SEC as f64;
        let res = nonfrac + frac;
        if self.neg() {
            -res
        } else {
            res
        }
    }

    /// Return `true` if self is zero
    #[inline]
    pub fn is_zero(self) -> bool {
        self.strip() == 0
    }

    /// Return `self` in nanoseconds
    #[inline]
    pub fn to_nanos(self) -> i64 {
        let nanos = (self.as_secs() * NANOS_PER_SEC + self.nano()) as i64;
        if self.neg() {
            -nanos
        } else {
            nanos
        }
    }

    /// Try to build a `Duration` via nanoseconds and fsp
    #[inline]
    pub fn from_nanos(nanos: i64, fsp: i8) -> Result<Duration> {
        let neg = nanos < 0;
        let nanos = nanos.abs();
        Duration::new(StdDuration::from_nanos(nanos as u64), neg, fsp)
    }

    /// Try to build a `Duration` via `DurationBuilder`
    #[inline]
    fn build(builder: DurationBuilder) -> Result<Duration> {
        let DurationBuilder {
            neg,
            mut hour,
            mut minute,
            mut second,
            mut nano,
            fsp,
            round_with_fsp,
        } = builder.check()?;

        if round_with_fsp {
            let round = u64::from(TEN_POW[NANO_WIDTH as usize - fsp as usize - 1]);
            nano /= round;
            nano = (nano + 5) / 10;
            nano *= round * 10;
        }

        if nano >= NANOS_PER_SEC {
            second += nano / NANOS_PER_SEC;
            minute += second / SECS_PER_MINUTE;
            hour += minute / MINUTES_PER_HOUR;
            hour = check_hour(hour)?;

            nano %= NANOS_PER_SEC;
            second %= SECS_PER_MINUTE;
            minute %= MINUTES_PER_HOUR;
        }

        let mut duration = Duration(0);
        duration.set_neg(neg);
        duration.set_hour(hour);
        duration.set_minute(minute);
        duration.set_second(second);
        duration.set_nano(nano);
        duration.set_fsp(fsp);
        Ok(duration)
    }

    /// Try to build a Duration from `u64`
    #[inline]
    fn from_u64(inner: u64) -> Result<Duration> {
        let duration = Duration(inner);
        check_fsp(duration.fsp() as i8)?;
        check_hour(duration.hour())?;
        check_minute(duration.minute())?;
        check_second(duration.second())?;
        Ok(duration)
    }

    /// Try to build a `Duration` from `StdDuration` with `fsp` and `neg`
    pub fn new(duration: StdDuration, neg: bool, fsp: i8) -> Result<Duration> {
        let fsp = check_fsp(fsp)?;
        let seconds = duration.as_secs();
        let hour = check_hour(seconds / SECS_PER_HOUR)?;
        let minute = seconds % SECS_PER_HOUR / SECS_PER_MINUTE;
        let second = seconds % SECS_PER_MINUTE;
        let nano = u64::from(duration.subsec_nanos());
        Duration::build(DurationBuilder {
            neg,
            hour,
            minute,
            second,
            nano,
            fsp,
            round_with_fsp: true,
        })
    }

    /// Parses the time form a formatted string with a fractional seconds part,
    /// returns the duration type `Time` value.
    /// See: http://dev.mysql.com/doc/refman/5.7/en/fractional-seconds.html
    pub fn parse(s: &[u8], fsp: i8) -> Result<Duration> {
        use State::*;
        #[derive(PartialEq, Debug)]
        enum State {
            Start,
            Block,
            PostBlock,
            Hour,
            MinuteColon,
            Minute,
            SecondColon,
            Second,
            Dot,
            Fraction,
            Consume,
            End,
        }

        let fsp = check_fsp(fsp)?;
        let to_dec = |d| u64::from(d - b'0');

        let mut neg = false;
        let (mut block, mut day, mut hour, mut minute, mut second, mut fract) = (0, 0, 0, 0, 0, 0);
        let mut eaten = 0;
        let info = || invalid_type!("Invalid time format: {}", escape(s));

        let mut state = Start;
        for &c in s {
            if c == b'.'
                && (state == Start
                    || state == Block
                    || state == PostBlock
                    || state == Hour
                    || state == Minute
                    || state == Second)
            {
                state = Dot;
                continue;
            }
            state = match state {
                Start => {
                    if c.is_ascii_digit() {
                        block = to_dec(c);
                        Block
                    } else if c.is_ascii_whitespace() {
                        Start
                    } else if c == b'-' {
                        if neg {
                            return Err(invalid_type!("Duplicated '-'"));
                        } else {
                            neg = true;
                            Start
                        }
                    } else {
                        return Err(info());
                    }
                }
                Block => {
                    if c.is_ascii_digit() {
                        block = check_block(block * 10 + to_dec(c))?;
                        Block
                    } else if c.is_ascii_whitespace() {
                        PostBlock
                    } else if c == b':' {
                        hour = block;
                        block = 0;
                        MinuteColon
                    } else {
                        return Err(info());
                    }
                }
                PostBlock => {
                    if c.is_ascii_digit() {
                        hour = to_dec(c);
                        day = block;
                        block = 0;
                        Hour
                    } else if c.is_ascii_whitespace() {
                        PostBlock
                    } else {
                        return Err(info());
                    }
                }
                Hour => {
                    if c.is_ascii_digit() {
                        hour = check_hour(hour * 10 + to_dec(c))?;
                        Hour
                    } else if c.is_ascii_whitespace() {
                        End
                    } else if c == b':' {
                        MinuteColon
                    } else {
                        return Err(info());
                    }
                }
                MinuteColon => {
                    if c.is_ascii_digit() {
                        minute = to_dec(c);
                        Minute
                    } else {
                        return Err(info());
                    }
                }
                Minute => {
                    if c.is_ascii_digit() {
                        minute = check_minute(minute * 10 + to_dec(c))?;
                        Minute
                    } else if c.is_ascii_whitespace() {
                        End
                    } else if c == b':' {
                        SecondColon
                    } else {
                        return Err(info());
                    }
                }
                SecondColon => {
                    if c.is_ascii_digit() {
                        second = to_dec(c);
                        Second
                    } else {
                        return Err(info());
                    }
                }
                Second => {
                    if c.is_ascii_digit() {
                        second = check_second(second * 10 + to_dec(c))?;
                        Second
                    } else if c.is_ascii_whitespace() {
                        End
                    } else {
                        return Err(info());
                    }
                }
                Dot => {
                    if c.is_ascii_digit() {
                        if fsp == 0 {
                            if to_dec(c) > 4 {
                                fract = 1;
                            }
                            Consume
                        } else {
                            fract = to_dec(c);
                            eaten = 1;
                            Fraction
                        }
                    } else {
                        return Err(info());
                    }
                }
                Fraction => {
                    if c.is_ascii_digit() {
                        if eaten < fsp {
                            fract = fract * 10 + to_dec(c);
                            eaten += 1;
                            Fraction
                        } else {
                            if to_dec(c) > 4 {
                                fract += 1;
                            }
                            Consume
                        }
                    } else if c.is_ascii_whitespace() {
                        End
                    } else {
                        return Err(info());
                    }
                }
                Consume => {
                    if c.is_ascii_digit() {
                        Consume
                    } else if c.is_ascii_whitespace() {
                        End
                    } else {
                        return Err(info());
                    }
                }
                End => {
                    if c.is_ascii_whitespace() {
                        End
                    } else {
                        return Err(info());
                    }
                }
            };
        }
        if state == MinuteColon || state == SecondColon {
            return Err(info());
        }

        if block != 0 {
            second = block % 100;
            minute = block / 100 % 100;
            hour = block / 10000;
        }

        hour += day * 24;
        fract *= u64::from(TEN_POW[NANO_WIDTH as usize - eaten as usize]);
        Duration::build(DurationBuilder {
            neg,
            hour,
            minute,
            second,
            nano: fract,
            fsp,
            round_with_fsp: false,
        })
    }

    /// Try to parse `self` to a `Decimal`
    pub fn to_decimal(self) -> Result<Decimal> {
        let mut buf = Vec::with_capacity(14);
        if self.neg() {
            write!(buf, "-")?;
        }
        write!(
            buf,
            "{:02}{:02}{:02}",
            self.hour(),
            self.minute(),
            self.second()
        )?;

        let fsp = self.fsp() as usize;
        if fsp > 0 {
            write!(buf, ".")?;
            let nanos = self.nano() / u64::from(TEN_POW[NANO_WIDTH as usize - fsp]);
            write!(buf, "{:01$}", nanos, fsp)?;
        }
        let d = unsafe { str::from_utf8_unchecked(&buf).parse()? };
        Ok(d)
    }

    /// Rounds fractional seconds precision with new FSP and returns a new one.
    /// We will use the “round half up” rule, e.g, >= 0.5 -> 1, < 0.5 -> 0,
    /// so 10:10:10.999999 round 0 -> 10:10:11
    /// and 10:10:10.000000 round 0 -> 10:10:10
    pub fn round_frac(mut self, fsp: i8) -> Result<Self> {
        let fsp = check_fsp(fsp)?;
        if fsp >= self.fsp() {
            self.set_fsp(fsp);
            return Ok(self);
        }

        Duration::build(DurationBuilder {
            neg: self.neg(),
            hour: self.hour(),
            minute: self.minute(),
            second: self.second(),
            nano: self.nano(),
            fsp,
            round_with_fsp: true,
        })
    }

    /// Checked duration addition. Computes self + rhs, returning None if overflow occurred.
    pub fn checked_add(self, rhs: Duration) -> Option<Duration> {
        let add = self.to_nanos().checked_add(rhs.to_nanos())?;
        Duration::from_nanos(add, self.fsp().max(rhs.fsp()) as i8).ok()
    }

    /// Checked duration subtraction. Computes self - rhs, returning None if overflow occurred.
    pub fn checked_sub(self, rhs: Duration) -> Option<Duration> {
        let sub = self.to_nanos().checked_sub(rhs.to_nanos())?;
        Duration::from_nanos(sub, self.fsp().max(rhs.fsp()) as i8).ok()
    }

    /// Set field `neg`, `unused` to false and fsp to zero,
    /// make `self` numerically comparable
    fn strip(mut self) -> u64 {
        self.set_neg(false);
        self.set_unused(false);
        self.set_fsp(0);
        self.0
    }

    /// Computes the absolute value of self.
    pub fn abs(mut self) -> Self {
        self.set_neg(false);
        self
    }
}

impl Display for Duration {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        if self.neg() {
            write!(formatter, "-")?;
        }
        write!(
            formatter,
            "{:02}:{:02}:{:02}",
            self.hour(),
            self.minute(),
            self.second()
        )?;

        let fsp = self.fsp() as usize;
        if fsp > 0 {
            write!(formatter, ".")?;
            let nanos = self.nano() / u64::from(TEN_POW[NANO_WIDTH as usize - fsp]);
            write!(formatter, "{:01$}", nanos, fsp)?;
        }
        Ok(())
    }
}

impl PartialEq for Duration {
    fn eq(&self, dur: &Duration) -> bool {
        let mask = !0x1ff;
        self.0 & mask == dur.0 & mask
    }
}

impl PartialOrd for Duration {
    fn partial_cmp(&self, dur: &Duration) -> Option<Ordering> {
        Some(match (self.neg(), dur.neg()) {
            (true, true) => dur.strip().cmp(&self.strip()),
            (true, false) => Ordering::Less,
            (false, true) => Ordering::Greater,
            (false, false) => self.strip().cmp(&dur.strip()),
        })
    }
}

impl Eq for Duration {}

impl Ord for Duration {
    fn cmp(&self, dur: &Duration) -> Ordering {
        self.partial_cmp(dur).unwrap()
    }
}

impl<T: Write> DurationEncoder for T {}
pub trait DurationEncoder: NumberEncoder {
    fn encode_duration(&mut self, v: Duration) -> Result<()> {
        self.encode_u64(v.0)?;
        Ok(())
    }
}

impl Duration {
    /// `decode` decodes duration encoded by `encode_duration`.
    pub fn decode(data: &mut BytesSlice<'_>) -> Result<Duration> {
        let inner = number::decode_u64(data)?;
        Duration::from_u64(inner)
    }
}

impl crate::coprocessor::codec::data_type::AsMySQLBool for Duration {
    #[inline]
    fn as_mysql_bool(
        &self,
        _context: &mut crate::coprocessor::dag::expr::EvalContext,
    ) -> crate::coprocessor::Result<bool> {
        Ok(!self.is_zero())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::coprocessor::codec::mysql::MAX_FSP;
    use crate::util::escape;

    /// `MAX_TIME_IN_SECS` is the maximum for mysql time type.
    const MAX_TIME_IN_SECS: u64 = 838 * SECS_PER_HOUR + 59 * SECS_PER_MINUTE + 59;

    #[test]
    fn test_hours() {
        let cases: Vec<(&str, i8, u64)> = vec![
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
        let cases: Vec<(&str, i8, u64)> = vec![
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
        let cases: Vec<(&str, i8, u64)> = vec![
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
    fn test_micro_secs() {
        let cases: Vec<(&str, i8, u64)> = vec![
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
            let res = dur.micro_secs();
            assert_eq!(exp, res);
        }
    }

    #[test]
    fn test_nano_secs() {
        let cases: Vec<(&str, i8, u64)> = vec![
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
            let res = dur.nano_secs();
            assert_eq!(exp * 1000, res);
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
            (b"1:2:3", 0, Some("01:02:03")),
            (b"1:2:3.123456", 5, Some("01:02:03.12346")),
            (b"-1:2:3", 0, Some("-01:02:03")),
            (b"-1 1:2:3", 0, Some("-25:02:03")),
            (b"1:2", 0, Some("01:02:00")),
            (b"12345", 0, Some("01:23:45")),
            (b"10305", 0, Some("01:03:05")),
            (b"\xe2\x82\xa1", 0, None),
            (b"10:10:0\xff", 0, None),
            (b"1\xff:10:01", 0, None),
            (b"  -1 1:2:3.12345  ", 4, Some("-25:02:03.1235")),
            (b"  -1 1:2:3 .12345  ", 4, None),
            (b"  1.12345  ", 5, Some("00:00:01.12345")),
            (b"  -.12345  ", 5, Some("-00:00:00.12345")),
            (b"  -1 .12345  ", 5, Some("-00:00:01.12345")),
            (b"  -   1 .12345  ", 5, Some("-00:00:01.12345")),
        ];

        for (input, fsp, expect) in cases {
            let d = Duration::parse(input, fsp);
            match expect {
                Some(exp) => {
                    let s = format!(
                        "{}",
                        d.unwrap_or_else(|e| panic!("{}: {:?}", escape(input), e))
                    );
                    if s != expect.unwrap() {
                        panic!("expect parse {} to {}, got {}", escape(input), exp, s);
                    }
                }
                None => {
                    if !d.is_err() {
                        panic!("{} should not be passed, got {:?}", escape(input), d);
                    }
                }
            }
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

        for (input, fsp, exp) in cases {
            let t = Duration::parse(input.as_bytes(), fsp).unwrap();
            let res = format!("{}", t.to_decimal().unwrap());
            assert_eq!(exp, res);
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
            buf.encode_duration(t).unwrap();
            let got = Duration::decode(&mut buf.as_slice()).unwrap();
            assert_eq!(t, got);
        }
    }

    #[test]
    fn test_checked_add_and_sub_duration() {
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
        let rhs = Duration::from_nanos((MAX_TIME_IN_SECS * NANOS_PER_SEC) as i64, 6).unwrap();
        assert_eq!(lhs.checked_add(rhs), None);
        let lhs = Duration::parse(b"-00:00:01", 6).unwrap();
        let rhs = Duration::from_nanos((MAX_TIME_IN_SECS * NANOS_PER_SEC) as i64, 6).unwrap();
        assert_eq!(lhs.checked_sub(rhs), None);
    }
}

#[cfg(test)]
mod benches {
    use super::*;
    use crate::coprocessor::codec::mysql::MAX_FSP;
    #[bench]
    fn bench_parse(b: &mut test::Bencher) {
        let cases = vec![
            ("12:34:56.1234", 0),
            ("12:34:56.789", 1),
            ("10:20:30.189", 2),
            ("2 27:54:32.828", 3),
            ("2 33:44:55.666777", 4),
            ("112233.445566", 5),
            ("1 23", 5),
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
        let duration = Duration::parse(b"-12:34:56.123456", 6).unwrap();
        b.iter(|| {
            let duration = test::black_box(duration);
            let _ = test::black_box(duration.hours());
        })
    }

    #[bench]
    fn bench_to_decimal(b: &mut test::Bencher) {
        let duration = Duration::parse(b"-12:34:56.123456", 6).unwrap();
        b.iter(|| {
            let duration = test::black_box(duration);
            let _ = test::black_box(duration.to_decimal().unwrap());
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
                buf.encode_duration(t).unwrap();
                let got = test::black_box(Duration::decode(&mut buf.as_slice()).unwrap());
                assert_eq!(t, got);
            }
        })
    }
    #[bench]
    fn bench_check_add_and_sub_duration(b: &mut test::Bencher) {
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
