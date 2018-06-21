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

use std::cmp::Ordering;
use std::fmt::{self, Display, Formatter};
use std::io::Write;
use std::time::Duration as StdDuration;
use std::{str, i64, u64};
use time::{self, Tm};
use util::codec::BytesSlice;
use util::codec::number::{self, NumberEncoder};

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

/// `Duration` is the type for `MySQL` time type.
#[derive(Debug, Clone)]
pub struct Duration {
    pub dur: StdDuration,
    // Fsp is short for Fractional Seconds Precision.
    // See http://dev.mysql.com/doc/refman/5.7/en/fractional-seconds.html
    pub fsp: u8,
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

    pub fn get_fsp(&self) -> u8 {
        self.fsp
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

    pub fn is_empty(&self) -> bool {
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

    // `parse` parses the time form a formatted string with a fractional seconds part,
    // returns the duration type Time value.
    // See: http://dev.mysql.com/doc/refman/5.7/en/fractional-seconds.html
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
            let nanos = self.micro_secs() / (10u32.pow(NANO_WIDTH - u32::from(self.fsp)));
            write!(buf, "{:01$}", nanos, self.fsp as usize)?;
        }
        let d = unsafe { str::from_utf8_unchecked(&buf).parse()? };
        Ok(d)
    }

    pub fn round_frac(&mut self, fsp: i8) -> Result<()> {
        let fsp = check_fsp(fsp)?;
        if fsp >= self.fsp {
            self.fsp = fsp;
            return Ok(());
        }
        self.fsp = fsp;
        let nanos = f64::from(self.dur.subsec_nanos())
            / f64::from(10u32.pow(NANO_WIDTH - u32::from(self.fsp)));
        let nanos = (nanos.round() as u32) * (10u32.pow(NANO_WIDTH - u32::from(self.fsp)));
        self.dur = StdDuration::new(self.dur.as_secs(), nanos);
        Ok(())
    }
}

impl Display for Duration {
    fn fmt(&self, formatter: &mut Formatter) -> fmt::Result {
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
            let nanos = self.micro_secs() / (10u32.pow(NANO_WIDTH - u32::from(self.fsp)));
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
    pub fn decode(data: &mut BytesSlice) -> Result<Duration> {
        let nanos = number::decode_i64(data)?;
        let fsp = number::decode_i64(data)?;
        Duration::from_nanos(nanos, fsp as i8)
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use coprocessor::codec::mysql::MAX_FSP;
    use util::escape;

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
                    let s = format!("{}", d.expect(&escape(input)));
                    if s != expect.unwrap() {
                        panic!("expect parse {} to {}, got {}", escape(input), exp, s);
                    }
                }
                None => if !d.is_err() {
                    panic!("{} should not be passed, got {:?}", escape(input), d);
                },
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
            let mut t = Duration::parse(input.as_bytes(), MAX_FSP).unwrap();
            t.round_frac(fsp).unwrap();
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
            let mut t = Duration::parse(input.as_bytes(), fsp).unwrap();
            let mut buf = vec![];
            buf.encode_duration(&t).unwrap();
            let got = Duration::decode(&mut buf.as_slice()).unwrap();
            assert_eq!(t, got);
        }
    }
}
