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

use super::super::Result;
use super::{check_fsp, parse_frac, Decimal};

pub const NANOS_PER_SEC: u64 = 1_000_000_000;
pub const NANO_WIDTH: u32 = 9;
const SECS_PER_HOUR: u64 = 3600;
const SECS_PER_MINUTE: u64 = 60;
const MINUTES_PER_HOUR: u64 = 60;
const NANOS_PER_MICRO: u64 = 1_000;

/// `MAX_TIME_IN_SECS` is the maximum for mysql time type.
const MAX_TIME_IN_SECS: u64 = 838 * SECS_PER_HOUR + 59 * SECS_PER_MINUTE + 59;

fn check_dur(duration: &StdDuration) -> Result<()> {
    let secs = duration.as_secs();
    if secs > MAX_TIME_IN_SECS || secs == MAX_TIME_IN_SECS && duration.subsec_nanos() > 0 {
        return Err(invalid_type!(
            "{:?} is larger than {:?}",
            duration,
            MAX_TIME_IN_SECS
        ));
    }
    Ok(())
}

#[inline]
fn check_hour(hour: u64) -> Result<u64> {
    if hour > 838 {
        Err(invalid_type!("invalid hour: {}, larger than {}", hour, 838))
    } else {
        Ok(hour)
    }
}
#[inline]
fn check_minute(minute: u64) -> Result<u64> {
    if minute > 59 {
        Err(invalid_type!(
            "invalid minute: {}, larger than {}",
            minute,
            59
        ))
    } else {
        Ok(minute)
    }
}
#[inline]
fn check_second(second: u64) -> Result<u64> {
    if second > 59 {
        Err(invalid_type!(
            "invalid second: {}, larger than {}",
            second,
            59
        ))
    } else {
        Ok(second)
    }
}

#[inline]
fn round_hms(hour: &mut u64, minute: &mut u64, second: &mut u64, nano: &mut u64) {
    *second += *nano / NANOS_PER_SEC;
    *minute += *second / SECS_PER_MINUTE;
    *hour += *minute / MINUTES_PER_HOUR;
    *nano %= NANOS_PER_SEC;
    *second %= SECS_PER_MINUTE;
    *minute %= MINUTES_PER_HOUR;
}

/// `Duration` is the type for MySQL `time` type.
///
/// It only occupies 8 bytes in memory.
bitfield! {
    // For more information:
    // 1. https://dev.mysql.com/doc/internals/en/date-and-time-data-type-representation.html
    // 2. http://dev.mysql.com/doc/refman/5.7/en/fractional-seconds.html
    #[derive(Clone, Copy)]
    pub struct Duration(u64);
    impl Debug;

    #[inline]
    bool, sign, set_sign: 63;            // 1 bit

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
    bool, unused, _: 0;                 // 1 bit

}

impl Duration {
    pub fn zero() -> Duration {
        Duration(0)
    }

    #[inline]
    pub fn hours(self) -> u64 {
        self.hour()
    }

    #[inline]
    pub fn minutes(self) -> u64 {
        self.minute()
    }

    #[inline]
    pub fn secs(self) -> u64 {
        self.second()
    }

    #[inline]
    pub fn micro_secs(self) -> u64 {
        self.nano() / NANOS_PER_MICRO
    }

    #[inline]
    pub fn nano_secs(self) -> u64 {
        self.nano()
    }

    #[inline]
    pub fn to_secs(self) -> f64 {
        let nonfrac = self.second() as f64;
        let frac = self.nano() as f64 / NANOS_PER_SEC as f64;
        let res = nonfrac + frac;
        if self.sign() {
            -res
        } else {
            res
        }
    }

    pub fn is_zero(self) -> bool {
        self.strip() == 0
    }

    pub fn to_nanos(self) -> i64 {
        let hour_to_nanos = self.hour() * SECS_PER_HOUR * NANOS_PER_SEC;
        let minute_to_nanos = self.minute() * SECS_PER_MINUTE * NANOS_PER_SEC;
        let second_to_nanos = self.second() * NANOS_PER_SEC;
        let nanos = (hour_to_nanos + minute_to_nanos + second_to_nanos + self.nano()) as i64;
        if self.sign() {
            -nanos
        } else {
            nanos
        }
    }

    pub fn from_nanos(nanos: i64, fsp: i8) -> Result<Duration> {
        let sign = nanos < 0;
        let nanos = nanos.abs();
        Duration::new(StdDuration::from_nanos(nanos as u64), sign, fsp)
    }

    fn with_detail(
        sign: bool,
        hour: u64,
        minute: u64,
        second: u64,
        nano: u64,
        fsp: u8,
    ) -> Result<Self> {
        let mut duration = Duration(0);
        duration.set_sign(sign);
        duration.set_hour(check_hour(hour)?);
        duration.set_minute(check_minute(minute)?);
        duration.set_second(check_second(second)?);
        duration.set_nano(nano);
        duration.set_fsp(fsp);
        Ok(duration)
    }

    pub fn new(duration: StdDuration, sign: bool, fsp: i8) -> Result<Duration> {
        check_dur(&duration)?;
        let seconds = duration.as_secs();

        let mut hour = seconds / SECS_PER_HOUR;
        let mut minute = seconds % SECS_PER_HOUR / SECS_PER_MINUTE;
        let mut second = seconds % SECS_PER_MINUTE;
        let fsp = check_fsp(fsp)?;
        let round = 10u64.pow(NANO_WIDTH - u32::from(fsp));
        let nano = u64::from(duration.subsec_nanos()) / (round / 10);
        let mut nano = if nano % 10 > 4 {
            nano / 10 + 1
        } else {
            nano / 10
        } * round;
        round_hms(&mut hour, &mut minute, &mut second, &mut nano);

        Duration::with_detail(sign, hour, minute, second, nano, fsp)
    }

    /// Parses the time form a formatted string with a fractional seconds part,
    /// returns the duration type `Time` value.
    /// See: http://dev.mysql.com/doc/refman/5.7/en/fractional-seconds.html
    pub fn parse(s: &[u8], fsp: i8) -> Result<Duration> {
        let fsp = check_fsp(fsp)?;

        if s.is_empty() {
            return Ok(Duration::zero());
        }

        let mut s = str::from_utf8(s)?;
        let origin = s;

        // sign
        let sign = if s.as_bytes()[0] == b'-' {
            s = &s[1..];
            true
        } else {
            false
        };

        // day
        let mut day = None;
        let mut parts = s.splitn(2, ' ');
        s = parts.next().unwrap();
        if let Some(part) = parts.next() {
            day = Some(
                s.parse::<u64>()
                    .map_err(|_| invalid_type!("fail to parse day from: {}", s))?,
            );
            s = part;
        }

        // fractional part
        let mut nano = 0;
        let mut parts = s.splitn(2, '.');
        s = parts.next().unwrap();
        if let Some(frac) = parts.next() {
            nano = u64::from(parse_frac(frac, fsp)?) * 10u64.pow(NANO_WIDTH - u32::from(fsp));
        }
        let mut parts = s.splitn(3, ':');
        let first = parts
            .next()
            .ok_or(invalid_type!("invalid time format: {}", origin))?;

        let first_try = first.parse::<u64>();
        let mut hour;
        let (mut minute, mut second) = (0, 0);
        match parts.next() {
            Some(part) => {
                hour =
                    first_try.map_err(|_| invalid_type!("fail to parse hour from: {}", first))?;
                minute = part
                    .parse::<u64>()
                    .map_err(|_| invalid_type!("fail to parse minute from: {}", part))
                    .and_then(check_minute)?;

                if let Some(part) = parts.next() {
                    second = part
                        .parse::<u64>()
                        .map_err(|_| invalid_type!("fail to parse second from: {}", part))
                        .and_then(check_second)?;
                }
            }
            None if day.is_some() => {
                hour =
                    first_try.map_err(|_| invalid_type!("fail to parse hour from: {}", first))?;
            }
            None => {
                let time =
                    first_try.map_err(|_| invalid_type!("invalid time format: {}", first))?;
                second = check_second(time % 100)?;
                minute = check_minute(time / 100 % 100)?;
                hour = time / 1_00_00;
            }
        }

        round_hms(&mut hour, &mut minute, &mut second, &mut nano);
        hour += day.unwrap_or(0) * 24;
        Duration::with_detail(sign, hour, minute, second, nano, fsp)
    }

    pub fn to_decimal(self) -> Result<Decimal> {
        let mut buf = Vec::with_capacity(14);
        if self.sign() {
            write!(buf, "-")?;
        }
        write!(
            buf,
            "{:02}{:02}{:02}",
            self.hour(),
            self.minute(),
            self.second()
        )?;

        if self.fsp() > 0 {
            write!(buf, ".")?;
            let nanos = self.nano() as u32 / (10u32.pow(NANO_WIDTH - u32::from(self.fsp())));
            write!(buf, "{:01$}", nanos, self.fsp() as usize)?;
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
            self.set_fsp(fsp as u8);
            return Ok(self);
        }

        let mut hour = self.hour();
        let mut minute = self.minute();
        let mut second = self.second();
        let mut nano = self.nano();
        let round = 10u64.pow(NANO_WIDTH - u32::from(fsp));
        nano /= round / 10;
        nano = if nano % 10 > 4 {
            nano / 10 + 1
        } else {
            nano / 10
        } * round;
        round_hms(&mut hour, &mut minute, &mut second, &mut nano);
        Duration::with_detail(self.sign(), hour, minute, second, nano, fsp as u8)
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

    fn strip(self) -> u64 {
        let mask = 0x1 << 63 | 0x1ff;
        self.0 & !mask
    }

    pub fn abs(mut self) -> Self {
        self.set_sign(false);
        self
    }
}

impl Display for Duration {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        if self.sign() {
            write!(formatter, "-")?;
        }
        write!(
            formatter,
            "{:02}:{:02}:{:02}",
            self.hour(),
            self.minute(),
            self.second()
        )?;
        if self.fsp() > 0 {
            write!(formatter, ".")?;
            let nanos = self.nano() as u32 / (10u32.pow(NANO_WIDTH - u32::from(self.fsp())));
            write!(formatter, "{:01$}", nanos, self.fsp() as usize)?;
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
        Some(match (self.sign(), dur.sign()) {
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
        self.encode_i64(v.to_nanos())?;
        self.encode_i64(i64::from(v.fsp())).map_err(From::from)
    }
}

impl Duration {
    /// `decode` decodes duration encoded by `encode_duration`.
    pub fn decode(data: &mut BytesSlice<'_>) -> Result<Duration> {
        let nanos = number::decode_i64(data)?;
        let fsp = number::decode_i64(data)?;
        Duration::from_nanos(nanos, fsp as i8)
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
