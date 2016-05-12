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

use std::time::Duration as StdDuration;
use time::{self, Tm};
use std::cmp::Ordering;
use std::fmt::{self, Display, Formatter};
use std::{str, i64, u64};

use util::codec::{Error, Result};
use util::escape;

const NANOS_PER_SEC: i64 = 1_000_000_000;
const NANO_WIDTH: u32 = 9;
const SECS_PER_HOUR: u64 = 3600;
const SECS_PER_MINUTE: u64 = 60;

/// `MAX_FSP` is the maximum digit of fractional seconds part.
pub const MAX_FSP: usize = 6;
/// `DEFAULT_FSP` is the default digit of fractional seconds part.
/// `MySQL` use 0 as the default Fsp.
pub const DEFAULT_FSP: usize = 0;

/// `MAX_TIME_IN_SECS` is the maximum for mysql time type.
const MAX_TIME_IN_SECS: u64 = 838 * SECS_PER_HOUR + 59 * SECS_PER_MINUTE + 59;

fn check_fsp(fsp: usize) -> Result<()> {
    if fsp > MAX_FSP {
        return Err(Error::InvalidDataType(format!("Invalid fsp {}", fsp)));
    }
    Ok(())
}

fn check_dur(dur: &StdDuration) -> Result<()> {
    let secs = dur.as_secs();
    if secs > MAX_TIME_IN_SECS || secs == MAX_TIME_IN_SECS && dur.subsec_nanos() > 0 {
        return Err(Error::InvalidDataType(format!("{:?} is larger than {:?}",
                                                  dur,
                                                  MAX_TIME_IN_SECS)));
    }
    Ok(())
}

/// Parse string as if it's a fraction part of a number and keep
/// only `fsp` precision.
fn parse_frac(s: &[u8], fsp: usize) -> Result<u32> {
    if s.iter().any(|&c| c < b'0' || c > b'9') {
        return Err(Error::InvalidDataType(format!("{} contains invalid char", escape(s))));
    }
    let res = s.iter().take(fsp + 1).fold(0, |l, r| l * 10 + (r - b'0') as u32);
    if s.len() > fsp {
        if res % 10 >= 5 {
            Ok(res / 10 + 1)
        } else {
            Ok(res / 10)
        }
    } else {
        Ok(res * 10u32.pow((fsp - s.len()) as u32))
    }
}

fn tm_to_secs(t: Tm) -> u64 {
    t.tm_hour as u64 * SECS_PER_HOUR + t.tm_min as u64 * SECS_PER_MINUTE + t.tm_sec as u64
}

/// `Duration` is the type for `MySQL` time type.
#[derive(Debug, Clone)]
pub struct Duration {
    pub dur: StdDuration,
    neg: bool,
    // Fsp is short for Fractional Seconds Precision.
    // See http://dev.mysql.com/doc/refman/5.7/en/fractional-seconds.html
    fsp: usize,
}

impl Duration {
    pub fn zero() -> Duration {
        Duration {
            dur: StdDuration::from_secs(0),
            neg: false,
            fsp: 0,
        }
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
        let res = self.dur.as_secs() as f64 + self.dur.subsec_nanos() as f64 * 10e-9;
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
        let nanos = self.dur.as_secs() as i64 * NANOS_PER_SEC + self.dur.subsec_nanos() as i64;
        if self.neg {
            -nanos
        } else {
            nanos
        }
    }

    pub fn from_nanos(nanos: i64, fsp: usize) -> Result<Duration> {
        let neg = nanos < 0;
        let nanos = nanos.abs();

        let dur = StdDuration::new((nanos / NANOS_PER_SEC) as u64,
                                   (nanos % NANOS_PER_SEC) as u32);
        try!(check_dur(&dur));

        Ok(Duration {
            dur: dur,
            neg: neg,
            fsp: fsp,
        })
    }

    pub fn new(dur: StdDuration, neg: bool, fsp: usize) -> Result<Duration> {
        try!(check_dur(&dur));
        Ok(Duration {
            dur: dur,
            neg: neg,
            fsp: fsp,
        })
    }

    // `parse` parses the time form a formatted string with a fractional seconds part,
    // returns the duration type Time value.
    // See: http://dev.mysql.com/doc/refman/5.7/en/fractional-seconds.html
    pub fn parse(mut s: &[u8], fsp: usize) -> Result<Duration> {
        try!(check_fsp(fsp));

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
            let day_str = try!(str::from_utf8(s));
            day = Some(box_try!(u64::from_str_radix(day_str, 10)));
            s = remain;
        }

        let mut parts = s.splitn(2, |c| *c == b'.');
        s = parts.next().unwrap();
        if let Some(frac_part) = parts.next() {
            frac = try!(parse_frac(frac_part, fsp));
            frac *= 10u32.pow(NANO_WIDTH - fsp as u32);
        }

        let mut parts = s.splitn(2, |c| *c == b':');
        s = parts.next().unwrap();
        let s_str = try!(str::from_utf8(s));
        let mut secs;
        match parts.next() {
            Some(remain) => {
                let remain_str = try!(str::from_utf8(remain));
                let t = box_try!(match remain.len() {
                    5 => time::strptime(remain_str, "%M:%S"),
                    2 => time::strptime(remain_str, "%M"),
                    _ => {
                        return Err(Error::InvalidDataType(format!("{} is invalid time.",
                                                                  remain_str)))
                    }
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
                    _ => return Err(Error::InvalidDataType(format!("{} is invalid time", s_str))),
                });
                secs = tm_to_secs(t);
            }
        }

        if let Some(day) = day {
            secs += day * SECS_PER_HOUR * 24;
        }

        let dur = StdDuration::new(secs, frac);
        try!(check_dur(&dur));

        Ok(Duration {
            dur: dur,
            neg: neg,
            fsp: fsp,
        })
    }
}

impl Display for Duration {
    fn fmt(&self, formatter: &mut Formatter) -> fmt::Result {
        if self.neg {
            try!(write!(formatter, "-"));
        }
        try!(write!(formatter,
                    "{:02}:{:02}:{:02}",
                    self.hours(),
                    self.minutes(),
                    self.secs()));
        if self.fsp > 0 {
            try!(write!(formatter, "."));
            let nanos = self.micro_secs() / (10u32.pow(NANO_WIDTH - self.fsp as u32));
            try!(write!(formatter, "{:01$}", nanos, self.fsp));
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

#[cfg(test)]
mod test {
    use util::escape;
    use super::*;

    #[test]
    fn test_parse() {
        let cases: Vec<(&'static [u8], usize, Option<&'static str>)> = vec![
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
                None => {
                    if !d.is_err() {
                        panic!("{} should not be passed, got {:?}", escape(input), d);
                    }
                }
            }
        }
    }
}
