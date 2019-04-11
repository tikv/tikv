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
use std::cmp::Ordering;
use std::fmt::{self, Display, Formatter};
use std::io::Write;
use std::time::Duration as StdDuration;
use std::{i64, str, u64};
use time::{self, Tm};

use super::super::Result;
use super::{check_fsp, parse_frac, Decimal};

pub const NANOS_PER_SEC: i64 = 1_000_000_000;
pub const NANO_WIDTH: u32 = 9;
const SECS_PER_HOUR: u64 = 3600;
const SECS_PER_MINUTE: u64 = 60;

/// `MAX_TIME_IN_SECS` is the maximum for mysql time type.
const MAX_TIME_IN_SECS: u64 = 838 * SECS_PER_HOUR + 59 * SECS_PER_MINUTE + 59;

fn check_dur(dur: &StdDuration) -> Result<()> {
    let secs = dur.as_secs();
    if secs > MAX_TIME_IN_SECS || secs == MAX_TIME_IN_SECS && dur.subsec_nanos() > 0 {
        return Err(invalid_type!(
            "{:?} is larger than {:?}",
            dur,
            MAX_TIME_IN_SECS
        ));
    }
    Ok(())
}

fn tm_to_secs(t: Tm) -> u64 {
    t.tm_hour as u64 * SECS_PER_HOUR + t.tm_min as u64 * SECS_PER_MINUTE + t.tm_sec as u64
}

/// `Duration` is the type for MySQL `time` type.
///
/// It only occupies 24 bytes in memory.
#[derive(Debug, Clone, Copy)]
pub struct Duration {
    dur: StdDuration,
    /// Fractional Seconds Precision
    /// See http://dev.mysql.com/doc/refman/5.7/en/fractional-seconds.html
    fsp: u8,
    neg: bool,
}

impl Duration {
    pub fn zero() -> Duration {
        Duration {
            dur: StdDuration::from_secs(0),
            neg: false,
            fsp: 0,
        }
    }

    pub fn fsp(&self) -> u8 {
        self.fsp
    }

    /// Changes the FSP part of the duration.
    pub fn set_fsp(&mut self, fsp: u8) {
        self.fsp = fsp;
    }

    pub fn hours(&self) -> u64 {
        self.dur.as_secs() / SECS_PER_HOUR
    }

    pub fn minutes(&self) -> u64 {
        self.dur.as_secs() % SECS_PER_HOUR / SECS_PER_MINUTE
    }

    pub fn secs(&self) -> u64 {
        self.dur.as_secs() % SECS_PER_MINUTE
    }

    pub fn micro_secs(&self) -> u32 {
        self.dur.subsec_micros()
    }

    pub fn nano_secs(&self) -> u32 {
        self.dur.subsec_nanos()
    }

    pub fn to_secs(&self) -> f64 {
        let res = self.dur.as_secs() as f64 + f64::from(self.dur.subsec_nanos()) * 10e-9;
        if self.neg {
            -res
        } else {
            res
        }
    }

    pub fn is_zero(&self) -> bool {
        self.to_nanos() == 0
    }

    pub fn to_nanos(&self) -> i64 {
        let nanos = self.dur.as_secs() as i64 * NANOS_PER_SEC + i64::from(self.dur.subsec_nanos());
        if self.neg {
            -nanos
        } else {
            nanos
        }
    }

    pub fn from_nanos(nanos: i64, fsp: i8) -> Result<Duration> {
        let neg = nanos < 0;
        let nanos = nanos.abs();

        let dur = StdDuration::new(
            (nanos / NANOS_PER_SEC) as u64,
            (nanos % NANOS_PER_SEC) as u32,
        );
        Duration::new(dur, neg, fsp)
    }

    pub fn new(dur: StdDuration, neg: bool, fsp: i8) -> Result<Duration> {
        check_dur(&dur)?;
        Ok(Duration {
            dur,
            neg,
            fsp: check_fsp(fsp)?,
        })
    }

    /// Parses the time form a formatted string with a fractional seconds part,
    /// returns the duration type `Time` value.
    /// See: http://dev.mysql.com/doc/refman/5.7/en/fractional-seconds.html
    pub fn parse(mut s: &[u8], fsp: i8) -> Result<Duration> {
        let fsp = check_fsp(fsp)?;

        let (mut neg, mut day, mut frac) = (false, None, 0);

        if s.is_empty() {
            return Ok(Duration::zero());
        } else if s[0] == b'-' {
            s = &s[1..];
            neg = true;
        }

        let mut parts = s.splitn(2, |c| *c == b' ');
        s = parts.next().unwrap();
        if let Some(remain) = parts.next() {
            let day_str = str::from_utf8(s)?;
            day = Some(box_try!(u64::from_str_radix(day_str, 10)));
            s = remain;
        }

        let mut parts = s.splitn(2, |c| *c == b'.');
        s = parts.next().unwrap();
        if let Some(frac_part) = parts.next() {
            frac = parse_frac(frac_part, fsp)?;
            frac *= 10u32.pow(NANO_WIDTH - u32::from(fsp));
        }

        let mut parts = s.splitn(2, |c| *c == b':');
        s = parts.next().unwrap();
        let s_str = str::from_utf8(s)?;
        let mut secs;
        match parts.next() {
            Some(remain) => {
                let remain_str = str::from_utf8(remain)?;
                let t = box_try!(match remain.len() {
                    5 => time::strptime(remain_str, "%M:%S"),
                    2 => time::strptime(remain_str, "%M"),
                    _ => return Err(invalid_type!("{} is invalid time.", remain_str)),
                });
                secs = tm_to_secs(t);
                secs += box_try!(u64::from_str_radix(s_str, 10)) * SECS_PER_HOUR;
            }
            None if day.is_some() => {
                secs = box_try!(u64::from_str_radix(s_str, 10)) * SECS_PER_HOUR;
            }
            None => {
                let t = box_try!(match s.len() {
                    6 => time::strptime(s_str, "%H%M%S"),
                    4 => time::strptime(s_str, "%M%S"),
                    2 => time::strptime(s_str, "%S"),
                    _ => return Err(invalid_type!("{} is invalid time", s_str)),
                });
                secs = tm_to_secs(t);
            }
        }

        if let Some(day) = day {
            secs += day * SECS_PER_HOUR * 24;
        }

        let dur = StdDuration::new(secs, frac);
        Duration::new(dur, neg, fsp as i8)
    }

    pub fn to_decimal(&self) -> Result<Decimal> {
        let mut buf = Vec::with_capacity(13);
        if self.neg {
            write!(buf, "-")?;
        }
        write!(
            buf,
            "{:02}{:02}{:02}",
            self.hours(),
            self.minutes(),
            self.secs()
        )?;
        if self.fsp > 0 {
            write!(buf, ".")?;
            let nanos = self.dur.subsec_nanos() / (10u32.pow(NANO_WIDTH - u32::from(self.fsp)));
            write!(buf, "{:01$}", nanos, self.fsp as usize)?;
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
        if fsp >= self.fsp {
            self.fsp = fsp;
            return Ok(self);
        }
        self.fsp = fsp;
        let nanos = f64::from(self.dur.subsec_nanos())
            / f64::from(10u32.pow(NANO_WIDTH - u32::from(self.fsp)));
        let nanos = (nanos.round() as u32) * (10u32.pow(NANO_WIDTH - u32::from(self.fsp)));
        self.dur = StdDuration::new(self.dur.as_secs(), nanos);
        Ok(self)
    }

    /// Checked duration addition. Computes self + rhs, returning None if overflow occurred.
    pub fn checked_add(self, rhs: &Duration) -> Option<Duration> {
        let add = match self.to_nanos().checked_add(rhs.to_nanos()) {
            Some(result) => result,
            None => return None,
        };
        Duration::from_nanos(add, self.fsp().max(rhs.fsp()) as i8).ok()
    }

    /// Checked duration subtraction. Computes self - rhs, returning None if overflow occurred.
    pub fn checked_sub(self, rhs: &Duration) -> Option<Duration> {
        let sub = match self.to_nanos().checked_sub(rhs.to_nanos()) {
            Some(result) => result,
            None => return None,
        };
        Duration::from_nanos(sub, self.fsp().max(rhs.fsp()) as i8).ok()
    }
}

impl Display for Duration {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        if self.neg {
            write!(formatter, "-")?;
        }
        write!(
            formatter,
            "{:02}:{:02}:{:02}",
            self.hours(),
            self.minutes(),
            self.secs()
        )?;
        if self.fsp > 0 {
            write!(formatter, ".")?;
            let nanos = self.dur.subsec_nanos() / (10u32.pow(NANO_WIDTH - u32::from(self.fsp)));
            write!(formatter, "{:01$}", nanos, self.fsp as usize)?;
        }
        Ok(())
    }
}

impl PartialEq for Duration {
    fn eq(&self, dur: &Duration) -> bool {
        self.neg == dur.neg && self.dur.eq(&dur.dur)
    }
}

impl PartialOrd for Duration {
    fn partial_cmp(&self, dur: &Duration) -> Option<Ordering> {
        Some(match (self.neg, dur.neg) {
            (true, true) => dur.dur.cmp(&self.dur),
            (true, false) => Ordering::Less,
            (false, true) => Ordering::Greater,
            (false, false) => self.dur.cmp(&dur.dur),
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
    fn encode_duration(&mut self, v: &Duration) -> Result<()> {
        self.encode_i64(v.to_nanos())?;
        self.encode_i64(i64::from(v.fsp)).map_err(From::from)
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
            let res = dur.micro_secs();
            assert_eq!(exp, res);
        }
    }

    #[test]
    fn test_nano_secs() {
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
                    if d.is_ok() {
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
            buf.encode_duration(&t).unwrap();
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
            let res = lhs.checked_add(&rhs).unwrap();
            let exp = Duration::parse(exp.as_bytes(), 6).unwrap();
            assert_eq!(res, exp);
        }
        for (exp, rhs, lhs) in cases {
            let lhs = Duration::parse(lhs.as_bytes(), 6).unwrap();
            let rhs = Duration::parse(rhs.as_bytes(), 6).unwrap();
            let res = lhs.checked_sub(&rhs).unwrap();
            let exp = Duration::parse(exp.as_bytes(), 6).unwrap();
            assert_eq!(res, exp);
        }

        let lhs = Duration::parse(b"00:00:01", 6).unwrap();
        let rhs = Duration::from_nanos(MAX_TIME_IN_SECS as i64 * NANOS_PER_SEC, 6).unwrap();
        assert_eq!(lhs.checked_add(&rhs), None);
        let lhs = Duration::parse(b"-00:00:01", 6).unwrap();
        let rhs = Duration::from_nanos(MAX_TIME_IN_SECS as i64 * NANOS_PER_SEC, 6).unwrap();
        assert_eq!(lhs.checked_sub(&rhs), None);
    }
}
