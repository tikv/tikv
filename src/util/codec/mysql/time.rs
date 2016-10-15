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
use std::str;
use std::fmt::{self, Formatter, Display};

use chrono::{DateTime, Timelike, UTC, Datelike, FixedOffset, Duration, TimeZone};

use util::codec::mysql::{self, types, parse_frac, check_fsp};
use util::codec::mysql::Decimal;
use util::codec::{Result, TEN_POW};


const ZERO_DATETIME_STR: &'static str = "0000-00-00 00:00:00";
const ZERO_DATE_STR: &'static str = "0000-00-00";
/// In go, `time.Date(0, 0, 0, 0, 0, 0, 0, time.UTC)` will be adjusted to
/// `-0001-11-30 00:00:00 +0000 UTC`, whose timestamp is -62169984000.
const ZERO_TIMESTAMP: i64 = -62169984000;

#[inline]
fn zero_time(tz: &FixedOffset) -> DateTime<FixedOffset> {
    tz.timestamp(ZERO_TIMESTAMP, 0)
}

#[inline]
fn zero_datetime(tz: &FixedOffset) -> Time {
    Time::new(zero_time(tz), types::DATETIME, mysql::DEFAULT_FSP).unwrap()
}

#[allow(too_many_arguments)]
#[inline]
fn ymd_hms_nanos<T: TimeZone>(tz: &T,
                              year: i32,
                              month: u32,
                              day: u32,
                              hour: u32,
                              min: u32,
                              secs: u32,
                              nanos: u32)
                              -> Result<DateTime<T>> {
    tz.ymd_opt(year, month, day)
        .and_hms_opt(hour, min, secs)
        .single()
        .and_then(|t| t.checked_add(Duration::nanoseconds(nanos as i64)))
        .ok_or_else(|| {
            box_err!("'{}-{}-{} {}:{}:{}.{:09}' is not a valid datetime",
                     year,
                     month,
                     day,
                     hour,
                     min,
                     secs,
                     nanos)
        })
}

#[inline]
fn from_bytes(bs: &[u8]) -> &str {
    unsafe { str::from_utf8_unchecked(bs) }
}

fn split_ymd_hms(mut s: &[u8]) -> Result<(i32, u32, u32, u32, u32, u32)> {
    let year: i32;
    if s.len() == 14 || s.len() == 8 {
        year = box_try!(from_bytes(&s[..4]).parse());
        s = &s[4..];
    } else {
        year = box_try!(from_bytes(&s[..2]).parse());
        s = &s[2..];
    };
    let month: u32 = box_try!(from_bytes(&s[..2]).parse());
    let day: u32 = box_try!(from_bytes(&s[2..4]).parse());
    let hour: u32 = if s.len() > 4 {
        box_try!(from_bytes(&s[4..6]).parse())
    } else {
        0
    };
    let minute: u32 = if s.len() > 6 {
        box_try!(from_bytes(&s[6..8]).parse())
    } else {
        0
    };
    let secs: u32 = if s.len() > 8 {
        box_try!(from_bytes(&s[8..10]).parse())
    } else {
        0
    };
    Ok((year, month, day, hour, minute, secs))
}

/// `Time` is the struct for handling datetime, timestamp and date.
#[derive(Clone, Debug)]
pub struct Time {
    // TimeZone should be loaded from request context.
    time: DateTime<FixedOffset>,
    tp: u8,
    fsp: u8,
}

impl Time {
    pub fn new(time: DateTime<FixedOffset>, tp: u8, fsp: i8) -> Result<Time> {
        Ok(Time {
            time: time,
            tp: tp,
            fsp: try!(check_fsp(fsp)),
        })
    }

    pub fn is_zero(&self) -> bool {
        self.time.timestamp() == ZERO_TIMESTAMP
    }

    pub fn get_fsp(&self) -> u8 {
        self.fsp
    }

    fn to_numeric_str(&self) -> String {
        if self.tp == types::DATE {
            // TODO: pure calculation should be enough.
            format!("{}", self.time.format("%Y%m%d"))
        } else {
            let s = self.time.format("%Y%m%d%H%M%S");
            if self.fsp > 0 {
                // Do we need to round the result?
                let nanos = self.time.nanosecond() / TEN_POW[9 - self.fsp as usize];
                format!("{}.{1:02$}", s, nanos, self.fsp as usize)
            } else {
                format!("{}", s)
            }
        }
    }

    pub fn to_decimal(&self) -> Result<Decimal> {
        if self.is_zero() {
            return Ok(0.into());
        }
        let dec: Decimal = box_try!(self.to_numeric_str().parse());
        Ok(dec)
    }

    pub fn to_f64(&self) -> Result<f64> {
        if self.is_zero() {
            return Ok(0f64);
        }
        let f: f64 = box_try!(self.to_numeric_str().parse());
        Ok(f)
    }

    fn parse_datetime_format(s: &str) -> Vec<&str> {
        let trimmed = s.trim();
        if trimmed.is_empty() {
            return vec![];
        }
        let spes: Vec<&str> = trimmed.split(|c| c < '0' || c > '9').collect();
        if spes.iter().any(|s| s.is_empty()) {
            vec![]
        } else {
            spes
        }
    }

    pub fn parse_utc_datetime(s: &str, fsp: i8) -> Result<Time> {
        Time::parse_datetime(s, fsp, &FixedOffset::east(0))
    }

    pub fn parse_datetime(s: &str, fsp: i8, tz: &FixedOffset) -> Result<Time> {
        let fsp = try!(check_fsp(fsp));
        let mut frac_str = "";
        let mut need_adjust = false;
        let parts = Time::parse_datetime_format(s);
        let (mut y, m, d, h, minute, sec): (i32, u32, u32, u32, u32, u32) =
            match *parts.as_slice() {
                [s1] => {
                    need_adjust = s1.len() == 12 || s1.len() == 6;
                    match s1.len() {
                        14 | 12 | 8 | 6 => try!(split_ymd_hms(s1.as_bytes())),
                        _ => return Err(box_err!("invalid datetime: {}", s)),
                    }
                }
                [s1, frac] => {
                    frac_str = frac;
                    need_adjust = s1.len() == 12;
                    match s1.len() {
                        14 | 12 => try!(split_ymd_hms(s1.as_bytes())),
                        _ => return Err(box_err!("invalid datetime: {}", s)),
                    }
                }
                [year, month, day] => {
                    (box_try!(year.parse()),
                     box_try!(month.parse()),
                     box_try!(day.parse()),
                     0,
                     0,
                     0)
                }
                [year, month, day, hour, min, sec] => {
                    (box_try!(year.parse()),
                     box_try!(month.parse()),
                     box_try!(day.parse()),
                     box_try!(hour.parse()),
                     box_try!(min.parse()),
                     box_try!(sec.parse()))
                }
                [year, month, day, hour, min, sec, frac] => {
                    frac_str = frac;
                    (box_try!(year.parse()),
                     box_try!(month.parse()),
                     box_try!(day.parse()),
                     box_try!(hour.parse()),
                     box_try!(min.parse()),
                     box_try!(sec.parse()))
                }
                _ => return Err(box_err!("invalid datetime: {}", s)),
            };

        if need_adjust || parts[0].len() == 2 {
            if y >= 0 && y <= 69 {
                y += 2000;
            } else if y >= 70 && y <= 99 {
                y += 1900;
            }
        }

        let frac = try!(parse_frac(frac_str.as_bytes(), fsp));
        if y == 0 && m == 0 && d == 0 && h == 0 && minute == 0 && sec == 0 {
            return Ok(zero_datetime(tz));
        }
        if y < 0 || y > 9999 {
            return Err(box_err!("unsupport year: {}", y));
        }
        let t = try!(ymd_hms_nanos(tz,
                                   y,
                                   m,
                                   d,
                                   h,
                                   minute,
                                   sec,
                                   frac * TEN_POW[9 - fsp as usize]));
        Time::new(t, types::DATETIME as u8, fsp as i8)
    }

    /// Get time from packed u64. When `tp` is `TIMESTAMP`, the packed time should
    /// be a UTC time; otherwise the packed time should be in the same timezone as `tz`
    /// specified.
    pub fn from_packed_u64(u: u64, tp: u8, fsp: i8, tz: &FixedOffset) -> Result<Time> {
        if u == 0 {
            return Time::new(zero_time(tz), tp, fsp);
        }
        let fsp = try!(mysql::check_fsp(fsp));
        let ymdhms = u >> 24;
        let ymd = ymdhms >> 17;
        let day = (ymd & ((1 << 5) - 1)) as u32;
        let ym = ymd >> 5;
        let month = (ym % 13) as u32;
        let year = (ym / 13) as i32;
        let hms = ymdhms & ((1 << 17) - 1);
        let second = (hms & ((1 << 6) - 1)) as u32;
        let minute = ((hms >> 6) & ((1 << 6) - 1)) as u32;
        let hour = (hms >> 12) as u32;
        let nanosec = ((u & ((1 << 24) - 1)) * 1000) as u32;
        let t = if tp == types::TIMESTAMP {
            let t = try!(ymd_hms_nanos(&UTC, year, month, day, hour, minute, second, nanosec));
            tz.from_utc_datetime(&t.naive_utc())
        } else {
            try!(ymd_hms_nanos(tz, year, month, day, hour, minute, second, nanosec))
        };
        Time::new(t, tp, fsp as i8)
    }

    /// Serialize time to a u64.
    ///
    /// If `tp` is TIMESTAMP, it will be converted to a UTC time first.
    pub fn to_packed_u64(&self) -> u64 {
        if self.is_zero() {
            return 0;
        }
        let t = if self.tp == types::TIMESTAMP {
            self.time.naive_utc()
        } else {
            self.time.naive_local()
        };
        let ymd = ((t.year() as u64 * 13 + t.month() as u64) << 5) | t.day() as u64;
        let hms = ((t.hour() as u64) << 12) | ((t.minute() as u64) << 6) | t.second() as u64;
        let micro = t.nanosecond() as u64 / 1000;
        (((ymd << 17) | hms) << 24) | micro
    }
}

impl PartialOrd for Time {
    fn partial_cmp(&self, right: &Time) -> Option<Ordering> {
        Some(self.cmp(right))
    }
}

impl PartialEq for Time {
    fn eq(&self, right: &Time) -> bool {
        self.time.eq(&right.time)
    }
}

impl Eq for Time {}

impl Ord for Time {
    fn cmp(&self, right: &Time) -> Ordering {
        self.time.cmp(&right.time)
    }
}

impl Display for Time {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        if self.is_zero() {
            if self.tp == types::DATE {
                return f.write_str(ZERO_DATE_STR);
            }

            return f.write_str(ZERO_DATETIME_STR);
        }

        if self.tp == types::DATE {
            if self.is_zero() {
                return f.write_str(ZERO_DATE_STR);
            } else {
                return write!(f, "{}", self.time.format("%Y-%m-%d"));
            }
        }

        if self.is_zero() {
            try!(f.write_str(ZERO_DATETIME_STR));
        } else {
            try!(write!(f, "{}", self.time.format("%Y-%m-%d %H:%M:%S")));
        }
        if self.fsp > 0 {
            // Do we need to round the result?
            let nanos = self.time.nanosecond() / TEN_POW[9 - self.fsp as usize];
            try!(write!(f, ".{0:01$}", nanos, self.fsp as usize));
        }
        Ok(())
    }
}

#[cfg(test)]
mod test {
    use super::*;

    use std::cmp::Ordering;

    use chrono::{FixedOffset, Duration};

    use util::codec::mysql::{MAX_FSP, UN_SPECIFIED_FSP, types};

    const MIN_OFFSET: i32 = -60 * 24 + 1;
    const MAX_OFFSET: i32 = 60 * 24;

    #[test]
    fn test_parse_datetime() {
        let ok_tables = vec![
            ("2012-12-31 11:30:45", UN_SPECIFIED_FSP, "2012-12-31 11:30:45"),
            ("0000-00-00 00:00:00", UN_SPECIFIED_FSP, "0000-00-00 00:00:00"),
            ("0001-01-01 00:00:00", UN_SPECIFIED_FSP, "0001-01-01 00:00:00"),
            ("00-12-31 11:30:45", UN_SPECIFIED_FSP, "2000-12-31 11:30:45"),
            ("12-12-31 11:30:45", UN_SPECIFIED_FSP, "2012-12-31 11:30:45"),
            ("2012-12-31", UN_SPECIFIED_FSP, "2012-12-31 00:00:00"),
            ("20121231", UN_SPECIFIED_FSP, "2012-12-31 00:00:00"),
            ("121231", UN_SPECIFIED_FSP, "2012-12-31 00:00:00"),
            ("2012^12^31 11+30+45", UN_SPECIFIED_FSP, "2012-12-31 11:30:45"),
            ("2012^12^31T11+30+45", UN_SPECIFIED_FSP, "2012-12-31 11:30:45"),
            ("2012-2-1 11:30:45", UN_SPECIFIED_FSP, "2012-02-01 11:30:45"),
            ("12-2-1 11:30:45", UN_SPECIFIED_FSP, "2012-02-01 11:30:45"),
            ("20121231113045", UN_SPECIFIED_FSP, "2012-12-31 11:30:45"),
            ("121231113045", UN_SPECIFIED_FSP, "2012-12-31 11:30:45"),
            ("2012-02-29", UN_SPECIFIED_FSP, "2012-02-29 00:00:00"),
            ("121231113045.123345", 6, "2012-12-31 11:30:45.123345"),
            ("20121231113045.123345", 6, "2012-12-31 11:30:45.123345"),
            ("121231113045.9999999", 6, "2012-12-31 11:30:46.000000"),
            ("121231113045.999999", 6, "2012-12-31 11:30:45.999999"),
            ("121231113045.999999", 5, "2012-12-31 11:30:46.00000"),
        ];

        for (input, fsp, exp) in ok_tables {
            let utc_t = Time::parse_utc_datetime(input, fsp).unwrap();
            assert_eq!(format!("{}", utc_t), exp);

            for mut offset in MIN_OFFSET..MAX_OFFSET {
                offset *= 60;
                let tz = FixedOffset::east(offset);
                let t = Time::parse_datetime(input, fsp, &tz).unwrap();
                if utc_t.is_zero() {
                    assert_eq!(t, utc_t);
                } else {
                    let exp_t = Time::new(utc_t.time - Duration::seconds(offset as i64),
                                          utc_t.tp,
                                          utc_t.fsp as i8)
                        .unwrap();
                    assert_eq!(exp_t, t);
                }
            }
        }

        let fail_tbl = vec![
            "1000-00-00 00:00:00",
            "1000-01-01 00:00:70",
            "1000-13-00 00:00:00",
            "10000-01-01 00:00:00",
            "1000-09-31 00:00:00",
            "1001-02-29 00:00:00",
        ];

        for t in fail_tbl {
            let tz = FixedOffset::east(0);
            assert!(Time::parse_datetime(t, 0, &tz).is_err(), t);
        }
    }

    #[test]
    fn test_codec() {
        let cases = vec![
            ("2010-10-10 10:11:11", 0),
            ("0001-01-01 00:00:00", 0),
            ("0001-01-01 00:00:00", UN_SPECIFIED_FSP),
            ("2000-01-01 00:00:00.000000", MAX_FSP),
            ("2000-01-01 00:00:00.123456", MAX_FSP),
            ("0001-01-01 00:00:00.123456", MAX_FSP),
            ("2000-06-01 00:00:00.999999", MAX_FSP),
        ];
        for (s, fsp) in cases {
            for mut offset in MIN_OFFSET..MAX_OFFSET {
                offset *= 60;
                let tz = FixedOffset::east(offset);
                let t = Time::parse_datetime(s, fsp, &tz).unwrap();
                let packed = t.to_packed_u64();
                let reverted_datetime = Time::from_packed_u64(packed, types::DATETIME, fsp, &tz)
                    .unwrap();
                assert_eq!(reverted_datetime, t);
                assert_eq!(reverted_datetime.to_packed_u64(), packed);

                let reverted_timestamp = Time::from_packed_u64(packed, types::TIMESTAMP, fsp, &tz)
                    .unwrap();
                assert_eq!(reverted_timestamp.time,
                           reverted_datetime.time + Duration::seconds(offset as i64));
                assert_eq!(reverted_timestamp.to_packed_u64(), packed);
            }
        }
    }

    #[test]
    fn test_to_dec() {
        let cases = vec![
            ("12-12-31 11:30:45", 0, "20121231113045", "20121231"),
            ("12-12-31 11:30:45", 6, "20121231113045.000000", "20121231"),
            ("12-12-31 11:30:45.123", 6, "20121231113045.123000", "20121231"),
            ("12-12-31 11:30:45.123345", 0, "20121231113045", "20121231"),
            ("12-12-31 11:30:45.123345", 3, "20121231113045.123", "20121231"),
            ("12-12-31 11:30:45.123345", 5, "20121231113045.12335", "20121231"),
            ("12-12-31 11:30:45.123345", 6, "20121231113045.123345", "20121231"),
            ("12-12-31 11:30:45.1233457", 6, "20121231113045.123346", "20121231"),
            ("12-12-31 11:30:45.823345", 0, "20121231113046", "20121231"),
        ];

        for (t_str, fsp, datetime_dec, date_dec) in cases {
            for mut offset in MIN_OFFSET..MAX_OFFSET {
                offset *= 60;
                let tz = FixedOffset::east(offset);
                let mut t = Time::parse_datetime(t_str, fsp, &tz).unwrap();
                let mut res = format!("{}", t.to_decimal().unwrap());
                assert_eq!(res, datetime_dec);

                t = Time::parse_datetime(t_str, 0, &tz).unwrap();
                t.tp = types::DATE;
                res = format!("{}", t.to_decimal().unwrap());
                assert_eq!(res, date_dec);
            }
        }
    }

    #[test]
    fn test_compare() {
        let cases = vec![
            ("2011-10-10 11:11:11", "2011-10-10 11:11:11", Ordering::Equal),
            ("2011-10-10 11:11:11.123456", "2011-10-10 11:11:11.1", Ordering::Greater),
            ("2011-10-10 11:11:11", "2011-10-10 11:11:11.123", Ordering::Less),
            ("0000-00-00 00:00:00", "2011-10-10 11:11:11", Ordering::Less),
            ("0000-00-00 00:00:00", "0000-00-00 00:00:00", Ordering::Equal),
        ];

        for (l, r, exp) in cases {
            for mut offset in MIN_OFFSET..MAX_OFFSET {
                offset *= 60;
                let tz = FixedOffset::east(offset);
                let l_t = Time::parse_datetime(l, MAX_FSP, &tz).unwrap();
                let r_t = Time::parse_datetime(r, MAX_FSP, &tz).unwrap();
                assert_eq!(exp, l_t.cmp(&r_t));
            }
        }
    }

    #[test]
    fn test_parse_datetime_format() {
        let cases = vec![
            ("2011-11-11 10:10:10.123456", vec!["2011", "11", "11", "10", "10", "10", "123456"]),
            ("  2011-11-11 10:10:10.123456  ",
             vec!["2011", "11", "11", "10", "10", "10", "123456"]),
            ("2011-11-11 10", vec!["2011", "11", "11", "10"]),
            ("2011-11-11T10:10:10.123456", vec!["2011", "11", "11", "10", "10", "10", "123456"]),
            ("2011:11:11T10:10:10.123456", vec!["2011", "11", "11", "10", "10", "10", "123456"]),
            ("xx2011-11-11 10:10:10", vec![]),
            ("T10:10:10", vec![]),
            ("2011-11-11x", vec![]),
            ("2011-11-11  10:10:10", vec![]),
            ("xxx 10:10:10", vec![]),
        ];

        for (s, exp) in cases {
            let res = Time::parse_datetime_format(s);
            assert_eq!(res, exp);
        }
    }
}
