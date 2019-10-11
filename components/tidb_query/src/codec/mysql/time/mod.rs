// Copyright 2018 TiKV Project Authors. Licensed under Apache-2.0.

pub mod extension;
mod tz;
pub mod weekmode;

use std::cmp::{min, Ordering};
use std::convert::{TryFrom, TryInto};
use std::fmt::Write;
use std::fmt::{self, Display, Formatter};
use std::{mem, str};

use chrono::{DateTime, Datelike, Duration, TimeZone, Timelike, Utc};

use codec::prelude::*;
use tidb_query_datatype::FieldTypeTp;

use crate::codec::convert::ConvertTo;
use crate::codec::mysql::duration::{Duration as MyDuration, NANOS_PER_SEC, NANO_WIDTH};
use crate::codec::mysql::{self, Decimal};
use crate::codec::{Error, Result, TEN_POW};
use crate::expr::EvalContext;

pub use self::extension::*;
pub use self::tz::Tz;
pub use self::weekmode::WeekMode;

const ZERO_DATETIME_NUMERIC_STR: &str = "00000000000000";
const ZERO_DATE_NUMERIC_STR: &str = "00000000";
const ZERO_DATETIME_STR: &str = "0000-00-00 00:00:00";
const ZERO_DATE_STR: &str = "0000-00-00";
/// In go, `time.Date(0, 0, 0, 0, 0, 0, 0, time.UTC)` will be adjusted to
/// `-0001-11-30 00:00:00 +0000 UTC`, whose timestamp is -62169984000.
const ZERO_TIMESTAMP: i64 = -62169984000;

/// In go, `time.Date(9999, 12, 31, 23, 59, 59, 0, time.UTC)` will be adjusted to
/// `9999-12-31 23:59:59 +0000 UTC`, whose timestamp is 253402300799.
pub const MAX_TIMESTAMP: i64 = 253402300799;
pub const MAX_TIME_NANOSECONDS: u32 = 999999000;

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

#[inline]
fn zero_time(tz: &Tz) -> DateTime<Tz> {
    tz.timestamp(ZERO_TIMESTAMP, 0)
}

#[inline]
pub fn zero_datetime(tz: &Tz) -> Time {
    Time::new(zero_time(tz), TimeType::DateTime, mysql::DEFAULT_FSP).unwrap()
}

#[allow(clippy::too_many_arguments)]
#[inline]
fn ymd_hms_nanos<T: TimeZone>(
    tz: &T,
    year: i32,
    month: u32,
    day: u32,
    hour: u32,
    min: u32,
    secs: u32,
    nanos: u32,
) -> Result<DateTime<T>> {
    use chrono::NaiveDate;

    // Note: We are not using `tz::from_ymd_opt` as suggested in chrono's README due to
    // chronotope/chrono-tz #23.
    // As a workaround, we first build a NaiveDate, then attach time zone information to it.
    NaiveDate::from_ymd_opt(year, month, day)
        .and_then(|date| date.and_hms_opt(hour, min, secs))
        .and_then(|t| t.checked_add_signed(Duration::nanoseconds(i64::from(nanos))))
        .and_then(|datetime| tz.from_local_datetime(&datetime).earliest())
        .ok_or_else(|| {
            Error::incorrect_datetime_value(&format!(
                "{}-{}-{} {}:{}:{}.{:09}",
                year, month, day, hour, min, secs, nanos
            ))
        })
}

// Safety: caller must ensure `bs` is valid utf8.
#[inline]
unsafe fn from_bytes(bs: &[u8]) -> &str {
    str::from_utf8_unchecked(bs)
}

// Safety: caller must ensure each byte of `s` and `frac` is a valid unicode
// character (i.e., `s` and `frac` may be sliced at any index and should give
// a valid unicode string).
unsafe fn split_ymd_hms_with_frac_as_s(
    mut s: &[u8],
    frac: &[u8],
) -> Result<(i32, u32, u32, u32, u32, u32)> {
    let year: i32;
    if s.len() == 14 {
        year = box_try!(from_bytes(&s[..4]).parse());
        s = &s[4..];
    } else {
        year = box_try!(from_bytes(&s[..2]).parse());
        s = &s[2..];
    };
    let month: u32 = box_try!(from_bytes(&s[..2]).parse());
    let day: u32 = box_try!(from_bytes(&s[2..4]).parse());
    let hour: u32 = box_try!(from_bytes(&s[4..6]).parse());
    let minute: u32 = if s.len() == 7 {
        box_try!(from_bytes(&s[6..7]).parse())
    } else {
        box_try!(from_bytes(&s[6..8]).parse())
    };
    let secs: u32 = if s.len() > 8 {
        let i = if s.len() > 9 { 10 } else { 9 };
        box_try!(from_bytes(&s[8..i]).parse())
    } else {
        match frac.len() {
            0 => 0,
            1 => box_try!(from_bytes(&frac[..1]).parse()),
            _ => box_try!(from_bytes(&frac[..2]).parse()),
        }
    };
    Ok((year, month, day, hour, minute, secs))
}

// Safety: caller must ensure `s` and `frac` are valid ascii.
unsafe fn split_ymd_with_frac_as_hms(
    mut s: &[u8],
    frac: &[u8],
    is_float: bool,
) -> Result<(i32, u32, u32, u32, u32, u32)> {
    let year: i32;
    if s.len() == 8 {
        year = box_try!(from_bytes(&s[..4]).parse());
        s = &s[4..];
    } else {
        year = box_try!(from_bytes(&s[..2]).parse());
        s = &s[2..];
    };
    let month: u32 = box_try!(from_bytes(&s[..2]).parse());
    let day: u32 = box_try!(from_bytes(&s[2..]).parse());
    let (hour, minute, sec): (u32, u32, u32) = if is_float {
        (0, 0, 0)
    } else {
        match frac.len() {
            0 => (0, 0, 0),
            1 | 2 => (box_try!(from_bytes(&frac[0..frac.len()]).parse()), 0, 0),
            3 | 4 => (
                box_try!(from_bytes(&frac[0..2]).parse()),
                box_try!(from_bytes(&frac[2..frac.len()]).parse()),
                0,
            ),
            5 => (
                box_try!(from_bytes(&frac[0..2]).parse()),
                box_try!(from_bytes(&frac[2..4]).parse()),
                box_try!(from_bytes(&frac[4..5]).parse()),
            ),
            _ => (
                box_try!(from_bytes(&frac[0..2]).parse()),
                box_try!(from_bytes(&frac[2..4]).parse()),
                box_try!(from_bytes(&frac[4..6]).parse()),
            ),
        }
    };
    Ok((year, month, day, hour, minute, sec))
}

#[derive(Clone, Debug, Copy, PartialEq)]
pub enum TimeType {
    Date,
    DateTime,
    Timestamp,
}

impl From<TimeType> for FieldTypeTp {
    fn from(time_type: TimeType) -> FieldTypeTp {
        match time_type {
            TimeType::Date => FieldTypeTp::Date,
            TimeType::DateTime => FieldTypeTp::DateTime,
            TimeType::Timestamp => FieldTypeTp::Timestamp,
        }
    }
}

impl TryFrom<FieldTypeTp> for TimeType {
    type Error = Error;

    fn try_from(value: FieldTypeTp) -> Result<Self> {
        match value {
            FieldTypeTp::Date => Ok(TimeType::Date),
            FieldTypeTp::DateTime => Ok(TimeType::DateTime),
            FieldTypeTp::Timestamp => Ok(TimeType::Timestamp),
            FieldTypeTp::Unspecified => Ok(TimeType::DateTime), // FIXME: We should forbid this
            _ => Err(box_err!("Time does not support field type {}", value)),
        }
    }
}

/// `Time` is the struct for handling datetime, timestamp and date.
#[derive(Clone, Debug)]
pub struct Time {
    // TimeZone should be loaded from request context.
    time: DateTime<Tz>,
    time_type: TimeType,
    fsp: u8,
}

impl Time {
    pub fn new(time: DateTime<Tz>, time_type: TimeType, fsp: i8) -> Result<Time> {
        Ok(Time {
            time,
            time_type,
            fsp: mysql::check_fsp(fsp)?,
        })
    }

    pub fn get_time_type(&self) -> TimeType {
        self.time_type
    }

    pub fn set_time_type(&mut self, time_type: TimeType) -> Result<()> {
        if self.time_type != time_type && time_type == TimeType::Date {
            // Truncate hh:mm::ss part if the type is Date
            self.time = self.time.date().and_hms(0, 0, 0); // TODO: might panic!
        }
        if self.time_type != time_type && time_type == TimeType::Timestamp {
            return Err(box_err!("can not convert datetime/date to timestamp"));
        }
        self.time_type = time_type;
        Ok(())
    }

    pub fn is_zero(&self) -> bool {
        self.time.timestamp() == ZERO_TIMESTAMP
    }

    pub fn invalid_zero(&self) -> bool {
        self.time.month() == 0 || self.time.day() == 0
    }

    pub fn get_fsp(&self) -> u8 {
        self.fsp
    }

    pub fn set_fsp(&mut self, fsp: u8) {
        self.fsp = fsp;
    }

    pub fn get_time(&self) -> DateTime<Tz> {
        self.time
    }

    pub fn set_time(&mut self, time: DateTime<Tz>) {
        self.time = time
    }

    /// Converts a `DateTime` to printable string representation
    #[inline]
    pub fn to_numeric_string(&self) -> String {
        if self.time_type == TimeType::Date {
            if self.is_zero() {
                String::from(ZERO_DATE_NUMERIC_STR)
            } else {
                format!("{}", self.time.format("%Y%m%d"))
            }
        } else {
            if self.is_zero() {
                if self.fsp > 0 {
                    // Do we need to round the result?
                    let nanos = self.time.nanosecond() / TEN_POW[9 - self.fsp as usize];
                    format!(
                        "{}.{1:02$}",
                        ZERO_DATETIME_NUMERIC_STR, nanos, self.fsp as usize
                    )
                } else {
                    String::from(ZERO_DATETIME_NUMERIC_STR)
                }
            } else {
                if self.fsp > 0 {
                    let nanos = self.time.nanosecond() / TEN_POW[9 - self.fsp as usize];
                    format!(
                        "{}.{1:02$}",
                        self.time.format("%Y%m%d%H%M%S"),
                        nanos,
                        self.fsp as usize
                    )
                } else {
                    format!("{}", self.time.format("%Y%m%d%H%M%S"))
                }
            }
        }
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

    fn split_datetime(s: &str) -> (Vec<&str>, &str) {
        let trimmed = s.trim();
        if trimmed.is_empty() {
            return (vec![], "");
        }
        let (parts, fracs) = if let Some(i) = trimmed.rfind('.') {
            (&trimmed[..i], &trimmed[i + 1..])
        } else {
            (trimmed, "")
        };
        (Time::parse_datetime_format(parts), fracs)
    }

    pub fn parse_utc_datetime(s: &str, fsp: i8) -> Result<Time> {
        Time::parse_datetime(s, fsp, &Tz::utc())
    }

    pub fn parse_utc_datetime_from_float_string(s: &str, fsp: i8) -> Result<Time> {
        Time::parse_datetime_from_float_string(s, fsp, &Tz::utc())
    }

    pub fn parse_datetime(s: &str, fsp: i8, tz: &Tz) -> Result<Time> {
        Time::parse_datetime_internal(s, fsp, tz, false)
    }

    pub fn parse_datetime_from_float_string(s: &str, fsp: i8, tz: &Tz) -> Result<Time> {
        Time::parse_datetime_internal(s, fsp, tz, true)
    }

    fn parse_datetime_internal(s: &str, fsp: i8, tz: &Tz, is_float: bool) -> Result<Time> {
        let fsp = mysql::check_fsp(fsp)?;
        let mut need_adjust = false;
        let mut has_hhmmss = false;
        let (parts, frac_str) = Time::split_datetime(s);
        let (mut year, month, day, hour, minute, sec): (i32, u32, u32, u32, u32, u32) = match *parts
            .as_slice()
        {
            [s1] => {
                need_adjust = s1.len() != 14 && s1.len() != 8;
                has_hhmmss = s1.len() == 14 || s1.len() == 12 || s1.len() == 11;
                match s1.len() {
                    // Safety: `s1` and `frac_str` must be ascii strings.
                    14 | 12 | 11 | 10 | 9 => unsafe {
                        split_ymd_hms_with_frac_as_s(s1.as_bytes(), frac_str.as_bytes())?
                    },
                    // Safety: `s1` and `frac_str` must be ascii strings.
                    8 | 6 | 5 => unsafe {
                        split_ymd_with_frac_as_hms(s1.as_bytes(), frac_str.as_bytes(), is_float)?
                    },
                    _ => {
                        return Err(box_err!(
                            "invalid datetime: {}, s1: {}, len: {}",
                            s,
                            s1,
                            s1.len()
                        ));
                    }
                }
            }
            [year, month, day] => (
                box_try!(year.parse()),
                box_try!(month.parse()),
                box_try!(day.parse()),
                0,
                0,
                0,
            ),
            [year, month, day, hour, min] => (
                box_try!(year.parse()),
                box_try!(month.parse()),
                box_try!(day.parse()),
                box_try!(hour.parse()),
                box_try!(min.parse()),
                0,
            ),
            [year, month, day, hour, min, sec] => {
                has_hhmmss = true;
                (
                    box_try!(year.parse()),
                    box_try!(month.parse()),
                    box_try!(day.parse()),
                    box_try!(hour.parse()),
                    box_try!(min.parse()),
                    box_try!(sec.parse()),
                )
            }
            _ => return Err(Error::incorrect_datetime_value(s)),
        };

        if need_adjust || parts[0].len() == 2 {
            if year >= 0 && year <= 69 {
                year += 2000;
            } else if year >= 70 && year <= 99 {
                year += 1900;
            }
        }

        let frac = if has_hhmmss {
            mysql::parse_frac(frac_str.as_bytes(), fsp)?
        } else {
            0
        };
        if year == 0 && month == 0 && day == 0 && hour == 0 && minute == 0 && sec == 0 {
            return Ok(zero_datetime(tz));
        }
        // it won't happen until 10000
        if year < 0 || year > 9999 {
            return Err(box_err!("unsupport year: {}", year));
        }
        let time = ymd_hms_nanos(
            tz,
            year,
            month,
            day,
            hour,
            minute,
            sec,
            frac * TEN_POW[9 - fsp as usize],
        )?;
        Time::new(time, TimeType::DateTime, fsp as i8)
    }

    pub fn parse_fsp(s: &str) -> i8 {
        s.rfind('.').map_or(super::DEFAULT_FSP, |idx| {
            min((s.len() - idx - 1) as i8, super::MAX_FSP)
        })
    }

    /// Get time from packed u64. When `tp` is `TIMESTAMP`, the packed time should
    /// be a UTC time; otherwise the packed time should be in the same timezone as `tz`
    /// specified.
    pub fn from_packed_u64(u: u64, time_type: TimeType, fsp: i8, tz: &Tz) -> Result<Time> {
        if u == 0 {
            return Time::new(zero_time(tz), time_type, fsp);
        }
        let fsp = mysql::check_fsp(fsp)?;
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
        let t = if time_type == TimeType::Timestamp {
            let t = ymd_hms_nanos(&Utc, year, month, day, hour, minute, second, nanosec)?;
            tz.from_utc_datetime(&t.naive_utc())
        } else {
            ymd_hms_nanos(tz, year, month, day, hour, minute, second, nanosec)?
        };
        Time::new(t, time_type, fsp as i8)
    }

    pub fn from_duration(tz: &Tz, time_type: TimeType, d: MyDuration) -> Result<Time> {
        let dur = Duration::nanoseconds(d.to_nanos());
        let t = Utc::now()
            .with_timezone(tz)
            .date()
            .and_hms(0, 0, 0) // TODO: might panic!
            .checked_add_signed(dur);
        if t.is_none() {
            return Err(box_err!("parse from duration {} overflows", d));
        }

        let t = t.unwrap();
        if t.year() < 1000 || t.year() > 9999 {
            return Err(box_err!(
                "datetime :{} out of range ('1000-01-01' to '9999-12-31')",
                t
            ));
        }
        if time_type == TimeType::Date {
            let t = t.date().and_hms(0, 0, 0); // TODO: might panic!
            Time::new(t, time_type, d.fsp() as i8)
        } else {
            Time::new(t, time_type, d.fsp() as i8)
        }
    }

    /// Serialize time to a u64.
    ///
    /// If `tp` is TIMESTAMP, it will be converted to a UTC time first.
    pub fn to_packed_u64(&self) -> u64 {
        if self.is_zero() {
            return 0;
        }
        let t = if self.time_type == TimeType::Timestamp {
            self.time.naive_utc()
        } else {
            self.time.naive_local()
        };
        let ymd = ((t.year() as u64 * 13 + u64::from(t.month())) << 5) | u64::from(t.day());
        let hms =
            (u64::from(t.hour()) << 12) | (u64::from(t.minute()) << 6) | u64::from(t.second());
        let micro = u64::from(t.nanosecond()) / 1000;
        (((ymd << 17) | hms) << 24) | micro
    }

    pub fn round_frac(&mut self, fsp: i8) -> Result<()> {
        if self.time_type == TimeType::Date || self.is_zero() {
            // date type has no fsp
            return Ok(());
        }
        let fsp = mysql::check_fsp(fsp)?;
        if fsp == self.fsp {
            return Ok(());
        }
        // TODO:support case month or day is 0(2012-00-00 12:12:12)
        let nanos = self.time.nanosecond();
        let base = TEN_POW[NANO_WIDTH - usize::from(fsp)];
        let expect_nanos = ((f64::from(nanos) / f64::from(base)).round() as u32) * base;
        let diff = i64::from(nanos) - i64::from(expect_nanos);
        let new_time = self.time.checked_add_signed(Duration::nanoseconds(diff));

        if let Some(new_time) = new_time {
            self.time = new_time;
            self.fsp = fsp;
            Ok(())
        } else {
            Err(box_err!("round_frac {} overflows", self.time))
        }
    }

    fn write_date_format_segment(&self, b: char, output: &mut String) -> Result<()> {
        match b {
            'b' => {
                let m = self.time.month();
                if m == 0 || m > 12 {
                    return Err(box_err!("invalid time format"));
                } else {
                    output.push_str(MONTH_NAMES_ABBR[(m - 1) as usize]);
                }
            }
            'M' => {
                let m = self.time.month();
                if m == 0 || m > 12 {
                    return Err(box_err!("invalid time format"));
                } else {
                    output.push_str(MONTH_NAMES[(m - 1) as usize]);
                }
            }
            'm' => {
                write!(output, "{:02}", self.time.month()).unwrap();
            }
            'c' => {
                write!(output, "{}", self.time.month()).unwrap();
            }
            'D' => {
                write!(
                    output,
                    "{}{}",
                    self.time.day(),
                    self.time.abbr_day_of_month()
                )
                .unwrap();
            }
            'd' => {
                write!(output, "{:02}", self.time.day()).unwrap();
            }
            'e' => {
                write!(output, "{}", self.time.day()).unwrap();
            }
            'j' => {
                write!(output, "{:03}", self.time.days()).unwrap();
            }
            'H' => {
                write!(output, "{:02}", self.time.hour()).unwrap();
            }
            'k' => {
                write!(output, "{}", self.time.hour()).unwrap();
            }
            'h' | 'I' => {
                let t = self.time.hour();
                if t == 0 || t == 12 {
                    output.push_str("12");
                } else {
                    write!(output, "{:02}", t % 12).unwrap();
                }
            }
            'l' => {
                let t = self.time.hour();
                if t == 0 || t == 12 {
                    output.push_str("12");
                } else {
                    write!(output, "{}", t % 12).unwrap();
                }
            }
            'i' => {
                write!(output, "{:02}", self.time.minute()).unwrap();
            }
            'p' => {
                let hour = self.time.hour();
                if (hour / 12) % 2 == 0 {
                    output.push_str("AM")
                } else {
                    output.push_str("PM")
                }
            }
            'r' => {
                let h = self.time.hour();
                if h == 0 {
                    write!(
                        output,
                        "{:02}:{:02}:{:02} AM",
                        12,
                        self.time.minute(),
                        self.time.second()
                    )
                    .unwrap();
                } else if h == 12 {
                    write!(
                        output,
                        "{:02}:{:02}:{:02} PM",
                        12,
                        self.time.minute(),
                        self.time.second()
                    )
                    .unwrap();
                } else if h < 12 {
                    write!(
                        output,
                        "{:02}:{:02}:{:02} AM",
                        h,
                        self.time.minute(),
                        self.time.second()
                    )
                    .unwrap();
                } else {
                    write!(
                        output,
                        "{:02}:{:02}:{:02} PM",
                        h - 12,
                        self.time.minute(),
                        self.time.second()
                    )
                    .unwrap();
                }
            }
            'T' => {
                write!(
                    output,
                    "{:02}:{:02}:{:02}",
                    self.time.hour(),
                    self.time.minute(),
                    self.time.second()
                )
                .unwrap();
            }
            'S' | 's' => {
                write!(output, "{:02}", self.time.second()).unwrap();
            }
            'f' => {
                write!(output, "{:06}", self.time.nanosecond() / 1000).unwrap();
            }
            'U' => {
                let w = self.time.week(WeekMode::from_bits_truncate(0));
                write!(output, "{:02}", w).unwrap();
            }
            'u' => {
                let w = self.time.week(WeekMode::from_bits_truncate(1));
                write!(output, "{:02}", w).unwrap();
            }
            'V' => {
                let w = self.time.week(WeekMode::from_bits_truncate(2));
                write!(output, "{:02}", w).unwrap();
            }
            'v' => {
                let (_, w) = self.time.year_week(WeekMode::from_bits_truncate(3));
                write!(output, "{:02}", w).unwrap();
            }
            'a' => {
                output.push_str(self.time.weekday().name_abbr());
            }
            'W' => {
                output.push_str(self.time.weekday().name());
            }
            'w' => {
                write!(output, "{}", self.time.weekday().num_days_from_sunday()).unwrap();
            }
            'X' => {
                let (year, _) = self.time.year_week(WeekMode::from_bits_truncate(2));
                if year < 0 {
                    write!(output, "{}", u32::max_value()).unwrap();
                } else {
                    write!(output, "{:04}", year).unwrap();
                }
            }
            'x' => {
                let (year, _) = self.time.year_week(WeekMode::from_bits_truncate(3));
                if year < 0 {
                    write!(output, "{}", u32::max_value()).unwrap();
                } else {
                    write!(output, "{:04}", year).unwrap();
                }
            }
            'Y' => {
                write!(output, "{:04}", self.time.year()).unwrap();
            }
            'y' => {
                write!(output, "{:02}", self.time.year() % 100).unwrap();
            }
            _ => output.push(b),
        }
        Ok(())
    }

    pub fn date_format(&self, layout: &str) -> Result<String> {
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

    pub fn is_leap_year(&self) -> bool {
        self.time.year() % 4 == 0 && (self.time.year() % 100 != 0 || self.time.year() % 400 == 0)
    }

    pub fn last_day_of_month(&self) -> u32 {
        match self.time.month() {
            4 | 6 | 9 | 11 => 30,
            2 => {
                if self.is_leap_year() {
                    29
                } else {
                    28
                }
            }
            _ => 31,
        }
    }

    /// Checked time addition. Computes self + rhs, returning None if overflow occurred.
    pub fn checked_add(self, rhs: MyDuration) -> Option<Time> {
        if let Some(add) = self
            .time
            .checked_add_signed(Duration::nanoseconds(rhs.to_nanos()))
        {
            if add.year() > 9999 {
                return None;
            }
            let mut res = self;
            res.set_time(add);
            Some(res)
        } else {
            None
        }
    }

    /// Checked time subtraction. Computes self - rhs, returning None if overflow occurred.
    pub fn checked_sub(self, rhs: MyDuration) -> Option<Time> {
        if let Some(sub) = self
            .time
            .checked_sub_signed(Duration::nanoseconds(rhs.to_nanos()))
        {
            if sub.year() < 0 {
                return None;
            }
            let mut res = self;
            res.set_time(sub);
            Some(res)
        } else {
            None
        }
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

impl ConvertTo<MyDuration> for Time {
    /// Port from TiDB's Time::ConvertToDuration
    #[inline]
    fn convert(&self, _: &mut EvalContext) -> Result<MyDuration> {
        if self.is_zero() {
            return Ok(MyDuration::zero());
        }
        let seconds = i64::from(self.time.num_seconds_from_midnight()) * NANOS_PER_SEC;
        // `nanosecond` returns the number of nanoseconds since the whole non-leap second.
        // Such as for 2019-09-22 07:21:22.670936103 UTC,
        // it will return 670936103.
        let nanosecond = i64::from(self.time.nanosecond());
        MyDuration::from_nanos(seconds + nanosecond, self.fsp as i8)
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
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        if self.is_zero() {
            if self.time_type == TimeType::Date {
                return f.write_str(ZERO_DATE_STR);
            }

            return f.write_str(ZERO_DATETIME_STR);
        }

        if self.time_type == TimeType::Date {
            if self.is_zero() {
                return f.write_str(ZERO_DATE_STR);
            } else {
                return write!(f, "{}", self.time.format("%Y-%m-%d"));
            }
        }

        if self.is_zero() {
            f.write_str(ZERO_DATETIME_STR)?;
        } else {
            write!(f, "{}", self.time.format("%Y-%m-%d %H:%M:%S"))?;
        }
        if self.fsp > 0 {
            // Do we need to round the result?
            let nanos = self.time.nanosecond() / TEN_POW[9 - self.fsp as usize];
            write!(f, ".{0:01$}", nanos, self.fsp as usize)?;
        }
        Ok(())
    }
}

impl<T: BufferWriter> TimeEncoder for T {}

/// Time Encoder for Chunk format
pub trait TimeEncoder: NumberEncoder {
    fn write_time(&mut self, v: &Time) -> Result<()> {
        if !v.is_zero() {
            self.write_u32_le(v.time.hour() as u32)?;
            self.write_u32_le(v.time.nanosecond() / 1000)?;
            self.write_u16_le(v.time.year() as u16)?;
            self.write_u8(v.time.month() as u8)?;
            self.write_u8(v.time.day() as u8)?;
            self.write_u8(v.time.minute() as u8)?;
            self.write_u8(v.time.second() as u8)?;
        } else {
            let len = mem::size_of::<u16>() + 2 * mem::size_of::<u32>() + 4;
            let buf = vec![0; len];
            self.write_bytes(&buf)?;
        }
        // Encode an useless u16 to make byte alignment 16 bytes.
        self.write_u16_le(0 as u16)?;

        let tp: FieldTypeTp = v.time_type.into();
        self.write_u8(tp.to_u8().unwrap())?;
        self.write_u8(v.fsp)?;
        // Encode an useless u16 to make byte alignment 20 bytes.
        self.write_u16_le(0 as u16).map_err(From::from)
    }
}

pub trait TimeDecoder: NumberDecoder {
    /// Decodes time encoded by `write_time` for Chunk format.
    fn read_time(&mut self) -> Result<Time> {
        let hour = self.read_u32_le()?;
        let nanoseconds = 1000 * self.read_u32_le()?;
        let year = i32::from(self.read_u16_le()?);
        let buf = self.read_bytes(4)?;
        let (month, day, minute, second) = (
            u32::from(buf[0]),
            u32::from(buf[1]),
            u32::from(buf[2]),
            u32::from(buf[3]),
        );
        let _ = self.read_u16();
        let buf = self.read_bytes(2)?;
        let (tp, fsp) = (
            FieldTypeTp::from_u8(buf[0]).unwrap_or(FieldTypeTp::Unspecified),
            buf[1],
        );
        let _ = self.read_u16();
        let tz = Tz::utc(); // TODO
        if year == 0
            && month == 0
            && day == 0
            && hour == 0
            && minute == 0
            && second == 0
            && nanoseconds == 0
        {
            return Ok(zero_datetime(&tz));
        }
        let t = if tp == FieldTypeTp::Timestamp {
            let t = ymd_hms_nanos(&Utc, year, month, day, hour, minute, second, nanoseconds)?;
            tz.from_utc_datetime(&t.naive_utc())
        } else {
            ymd_hms_nanos(
                &Tz::utc(),
                year,
                month,
                day,
                hour,
                minute,
                second,
                nanoseconds,
            )?
        };
        Time::new(t, tp.try_into()?, fsp as i8)
    }
}

impl<T: BufferReader> TimeDecoder for T {}

impl crate::codec::data_type::AsMySQLBool for Time {
    #[inline]
    fn as_mysql_bool(&self, _context: &mut crate::expr::EvalContext) -> crate::Result<bool> {
        Ok(!self.is_zero())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::cmp::Ordering;
    use std::f64::EPSILON;

    use chrono::{Duration, Local};

    use crate::codec::mysql::{Duration as MyDuration, MAX_FSP, UNSPECIFIED_FSP};
    use crate::expr::EvalContext;

    fn for_each_tz<F: FnMut(Tz, i64)>(mut f: F) {
        const MIN_OFFSET: i64 = -60 * 24 + 1;
        const MAX_OFFSET: i64 = 60 * 24;

        // test some offset
        for mut offset in MIN_OFFSET..MAX_OFFSET {
            offset *= 60;
            let tz = Tz::from_offset(offset).unwrap();
            f(tz, offset)
        }

        // test some time zone name without DST
        let tz_table = vec![
            ("Etc/GMT+11", -39600),
            ("Etc/GMT0", 0),
            ("Etc/GMT-5", 18000),
            ("UTC", 0),
            ("Universal", 0),
        ];
        for (name, offset) in tz_table {
            let tz = Tz::from_tz_name(name).unwrap();
            f(tz, offset)
        }
    }

    #[test]
    fn test_parse_datetime() {
        let ok_tables = vec![
            (
                "2012-12-31 11:30:45",
                UNSPECIFIED_FSP,
                "2012-12-31 11:30:45",
            ),
            (
                "0000-00-00 00:00:00",
                UNSPECIFIED_FSP,
                "0000-00-00 00:00:00",
            ),
            (
                "0001-01-01 00:00:00",
                UNSPECIFIED_FSP,
                "0001-01-01 00:00:00",
            ),
            ("00-12-31 11:30:45", UNSPECIFIED_FSP, "2000-12-31 11:30:45"),
            ("12-12-31 11:30:45", UNSPECIFIED_FSP, "2012-12-31 11:30:45"),
            ("2012-12-31", UNSPECIFIED_FSP, "2012-12-31 00:00:00"),
            ("20121231", UNSPECIFIED_FSP, "2012-12-31 00:00:00"),
            ("121231", UNSPECIFIED_FSP, "2012-12-31 00:00:00"),
            ("121231", UNSPECIFIED_FSP, "2012-12-31 00:00:00"),
            ("12121", UNSPECIFIED_FSP, "2012-12-01 00:00:00"),
            (
                "2012^12^31 11+30+45",
                UNSPECIFIED_FSP,
                "2012-12-31 11:30:45",
            ),
            (
                "2012^12^31T11+30+45",
                UNSPECIFIED_FSP,
                "2012-12-31 11:30:45",
            ),
            ("2012-2-1 11:30:45", UNSPECIFIED_FSP, "2012-02-01 11:30:45"),
            ("12-2-1 11:30:45", UNSPECIFIED_FSP, "2012-02-01 11:30:45"),
            ("20121231113045", UNSPECIFIED_FSP, "2012-12-31 11:30:45"),
            ("121231113045", UNSPECIFIED_FSP, "2012-12-31 11:30:45"),
            ("2012-02-29", UNSPECIFIED_FSP, "2012-02-29 00:00:00"),
            ("121231113045.123345", 6, "2012-12-31 11:30:45.123345"),
            ("20121231113045.123345", 6, "2012-12-31 11:30:45.123345"),
            ("121231113045.9999999", 6, "2012-12-31 11:30:46.000000"),
            ("121231113045.999999", 6, "2012-12-31 11:30:45.999999"),
            ("121231113045.999999", 5, "2012-12-31 11:30:46.00000"),
            ("17011801101", UNSPECIFIED_FSP, "2017-01-18 01:10:01"),
            ("20170118.1", UNSPECIFIED_FSP, "2017-01-18 01:00:00"),
            ("20170118.1", UNSPECIFIED_FSP, "2017-01-18 01:00:00"),
            ("20170118.11", UNSPECIFIED_FSP, "2017-01-18 11:00:00"),
            ("20170118.111", UNSPECIFIED_FSP, "2017-01-18 11:01:00"),
            ("20170118.1111", UNSPECIFIED_FSP, "2017-01-18 11:11:00"),
            ("20170118.11111", UNSPECIFIED_FSP, "2017-01-18 11:11:01"),
            ("20170118.111111", UNSPECIFIED_FSP, "2017-01-18 11:11:11"),
            ("20170118.1111111", UNSPECIFIED_FSP, "2017-01-18 11:11:11"),
            ("20170118.11111111", UNSPECIFIED_FSP, "2017-01-18 11:11:11"),
            ("1701020301.", UNSPECIFIED_FSP, "2017-01-02 03:01:00"),
            ("1701020304.1", UNSPECIFIED_FSP, "2017-01-02 03:04:01"),
            ("1701020302.11", UNSPECIFIED_FSP, "2017-01-02 03:02:11"),
            ("170102036", UNSPECIFIED_FSP, "2017-01-02 03:06:00"),
            ("170102039.", UNSPECIFIED_FSP, "2017-01-02 03:09:00"),
            ("170102037.11", UNSPECIFIED_FSP, "2017-01-02 03:07:11"),
            ("17011801101.111111", UNSPECIFIED_FSP, "2017-01-18 01:10:01"),
        ];

        for (input, fsp, exp) in ok_tables {
            let utc_t = Time::parse_utc_datetime(input, fsp).unwrap();
            assert_eq!(format!("{}", utc_t), exp);

            for_each_tz(move |tz, offset| {
                let t = Time::parse_datetime(input, fsp, &tz).unwrap();
                if utc_t.is_zero() {
                    assert_eq!(t, utc_t);
                } else {
                    let exp_t = Time::new(
                        utc_t.time - Duration::seconds(offset),
                        utc_t.time_type,
                        utc_t.fsp as i8,
                    )
                    .unwrap();
                    assert_eq!(exp_t, t);
                }
            });
        }

        // Test parse datetime from float string vs non-float string
        let ok_tables = vec![
            (
                "121231.0101",
                UNSPECIFIED_FSP,
                "2012-12-31 00:00:00",
                "2012-12-31 01:01:00",
            ),
            (
                "121231.1",
                UNSPECIFIED_FSP,
                "2012-12-31 00:00:00",
                "2012-12-31 01:00:00",
            ),
            (
                "19991231.111",
                UNSPECIFIED_FSP,
                "1999-12-31 00:00:00",
                "1999-12-31 11:01:00",
            ),
            (
                "20121231.1",
                UNSPECIFIED_FSP,
                "2012-12-31 00:00:00",
                "2012-12-31 01:00:00",
            ),
        ];

        for (input, fsp, exp_float, exp_non_float) in ok_tables {
            let utc_t = Time::parse_utc_datetime_from_float_string(input, fsp).unwrap();
            assert_eq!(format!("{}", utc_t), exp_float);
            let utc_t = Time::parse_utc_datetime(input, fsp).unwrap();
            assert_eq!(format!("{}", utc_t), exp_non_float);
        }

        let fail_tbl = vec![
            "1000-00-00 00:00:00",
            "1000-01-01 00:00:70",
            "1000-13-00 00:00:00",
            "10000-01-01 00:00:00",
            "1000-09-31 00:00:00",
            "1001-02-29 00:00:00",
            "20170118.999",
        ];

        for t in fail_tbl {
            let tz = Tz::utc();
            assert!(Time::parse_datetime(t, 0, &tz).is_err(), t);
        }
    }

    #[test]
    fn test_parse_datetime_dst() {
        let ok_tables = vec![
            ("Asia/Shanghai", "1988-04-09 23:59:59", 576604799),
            // No longer DST since tzdata 2018f
            ("Asia/Shanghai", "1988-04-10 00:00:00", 576604800),
            ("Asia/Shanghai", "1988-04-10 01:00:00", 576608400),
            // DST starts from 02:00
            ("Asia/Shanghai", "1988-04-17 01:00:00", 577213200),
            ("Asia/Shanghai", "1988-04-17 01:59:59", 577216799),
            ("Asia/Shanghai", "1988-04-17 03:00:00", 577216800),
            // DST ends at 02:00
            ("Asia/Shanghai", "1988-09-11 00:59:59", 589910399),
            ("Asia/Shanghai", "1988-09-11 01:00:00", 589910400), // ambiguous
            ("Asia/Shanghai", "1988-09-11 01:59:59", 589913999), // ambiguous
            ("Asia/Shanghai", "1988-09-11 02:00:00", 589917600),
            ("Asia/Shanghai", "1988-09-11 02:00:01", 589917601),
            ("Asia/Shanghai", "2015-01-02 23:59:59", 1420214399),
            ("America/Los_Angeles", "1919-03-30 01:59:59", -1601820001),
            ("America/Los_Angeles", "1919-03-30 03:00:00", -1601820000),
            ("America/Los_Angeles", "2011-03-13 01:59:59", 1300010399),
            ("America/Los_Angeles", "2011-03-13 03:00:00", 1300010400),
            ("America/Los_Angeles", "2011-11-06 01:59:59", 1320569999), // ambiguous
            ("America/Los_Angeles", "2011-11-06 02:00:00", 1320573600),
            ("America/Toronto", "2013-11-18 11:55:00", 1384793700),
        ];

        for (tz_name, time_str, utc_timestamp) in ok_tables {
            let tz = Tz::from_tz_name(tz_name).unwrap();
            let t = Time::parse_datetime(time_str, UNSPECIFIED_FSP, &tz).unwrap();
            assert_eq!(
                t.time.timestamp(),
                utc_timestamp,
                "{} {}",
                tz_name,
                time_str
            );
        }

        // TODO: When calling `UNIX_TIMESTAMP()` in MySQL, these date time will not fail.
        // However it will fail when inserting into a TIMESTAMP field.
        let fail_tables = vec![
            ("Asia/Shanghai", "1988-04-17 02:00:00"),
            ("Asia/Shanghai", "1988-04-17 02:59:59"),
            ("America/Los_Angeles", "1919-03-30 02:00:00"),
            ("America/Los_Angeles", "1919-03-30 02:59:59"),
            ("America/Los_Angeles", "2011-03-13 02:00:00"),
            ("America/Los_Angeles", "2011-03-13 02:59:59"),
        ];

        for (tz_name, time_str) in fail_tables {
            let tz = Tz::from_tz_name(tz_name).unwrap();
            assert!(
                Time::parse_datetime(time_str, UNSPECIFIED_FSP, &tz).is_err(),
                "{} {}",
                tz_name,
                time_str,
            );
        }
    }

    #[test]
    #[allow(clippy::zero_prefixed_literal)]
    fn test_parse_datetime_system_timezone() {
        // Basically, we check whether the parse result is the same when constructing using local.
        let tables = vec![
            (1988, 04, 09, 23, 59, 59),
            (1988, 04, 10, 01, 00, 00),
            (1988, 09, 11, 00, 00, 00),
            (1988, 09, 11, 00, 00, 01),
            (1988, 09, 10, 23, 59, 59),
            (1988, 09, 10, 23, 00, 00),
            (1988, 09, 10, 22, 59, 59),
            (2015, 01, 02, 23, 59, 59),
            (1919, 03, 30, 01, 59, 59),
            (1919, 03, 30, 03, 00, 00),
            (1988, 04, 10, 00, 00, 00),
            (1988, 04, 10, 00, 59, 59),
        ];
        // These are supposed to be local time zones
        let local_tzs = vec![
            Tz::from_tz_name("SYSTEM").unwrap(),
            Tz::from_tz_name("system").unwrap(),
            Tz::from_tz_name("System").unwrap(),
            Tz::local(),
        ];
        for (year, month, day, hour, minute, second) in tables {
            for tz in &local_tzs {
                // Some Date time listed in the test case may be invalid in the current time zone,
                // so we need to check it first.
                let local_time = Local
                    .ymd_opt(year, month, day)
                    .and_hms_opt(hour, minute, second)
                    .earliest();
                if let Some(local_time) = local_time {
                    let time_str =
                        format!("{}-{}-{} {}:{}:{}", year, month, day, hour, minute, second);
                    let t = Time::parse_datetime(&time_str, UNSPECIFIED_FSP, tz).unwrap();
                    assert_eq!(t.time, local_time);
                }
            }
        }
    }

    #[test]
    fn test_codec() {
        let cases = vec![
            ("2010-10-10 10:11:11", 0),
            ("0001-01-01 00:00:00", 0),
            ("0001-01-01 00:00:00", UNSPECIFIED_FSP),
            ("2000-01-01 00:00:00.000000", MAX_FSP),
            ("2000-01-01 00:00:00.123456", MAX_FSP),
            ("0001-01-01 00:00:00.123456", MAX_FSP),
            ("2000-06-01 00:00:00.999999", MAX_FSP),
        ];
        for (s, fsp) in cases {
            for_each_tz(move |tz, offset| {
                let t = Time::parse_datetime(s, fsp, &tz).unwrap();
                let packed = t.to_packed_u64();
                let reverted_datetime =
                    Time::from_packed_u64(packed, TimeType::DateTime, fsp, &tz).unwrap();
                assert_eq!(reverted_datetime, t);
                assert_eq!(reverted_datetime.to_packed_u64(), packed);

                let reverted_timestamp =
                    Time::from_packed_u64(packed, TimeType::Timestamp, fsp, &tz).unwrap();
                assert_eq!(
                    reverted_timestamp.time,
                    reverted_datetime.time + Duration::seconds(offset)
                );
                assert_eq!(reverted_timestamp.to_packed_u64(), packed);
            })
        }
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
            ("0000-00-00 00:00:00", 6, "00000000000000"),
        ];
        for (s, fsp, expect) in cases {
            let t = Time::parse_utc_datetime(s, fsp).unwrap();
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
            let t = Time::parse_utc_datetime(s, fsp).unwrap();
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
    fn test_to_dec() {
        let cases = vec![
            ("12-12-31 11:30:45", 0, "20121231113045", "20121231"),
            ("12-12-31 11:30:45", 6, "20121231113045.000000", "20121231"),
            (
                "12-12-31 11:30:45.123",
                6,
                "20121231113045.123000",
                "20121231",
            ),
            ("12-12-31 11:30:45.123345", 0, "20121231113045", "20121231"),
            (
                "12-12-31 11:30:45.123345",
                3,
                "20121231113045.123",
                "20121231",
            ),
            (
                "12-12-31 11:30:45.123345",
                5,
                "20121231113045.12335",
                "20121231",
            ),
            (
                "12-12-31 11:30:45.123345",
                6,
                "20121231113045.123345",
                "20121231",
            ),
            (
                "12-12-31 11:30:45.1233457",
                6,
                "20121231113045.123346",
                "20121231",
            ),
            ("12-12-31 11:30:45.823345", 0, "20121231113046", "20121231"),
        ];

        for (t_str, fsp, datetime_dec, date_dec) in cases {
            for_each_tz(move |tz, _offset| {
                let mut ctx = EvalContext::default();
                let mut t = Time::parse_datetime(t_str, fsp, &tz).unwrap();
                let dec: Result<Decimal> = t.convert(&mut ctx);
                let mut res = format!("{}", dec.unwrap());
                assert_eq!(res, datetime_dec);

                t = Time::parse_datetime(t_str, 0, &tz).unwrap();
                t.set_time_type(TimeType::Date).unwrap();
                let dec: Result<Decimal> = t.convert(&mut ctx);
                res = format!("{}", dec.unwrap());
                assert_eq!(res, date_dec);
            });
        }
    }

    #[test]
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
            let t = Time::parse_utc_datetime(s, fsp).unwrap();
            let get: f64 = t.convert(&mut ctx).unwrap();
            assert!(
                (expect - get).abs() < EPSILON,
                "expect: {}, got: {}",
                expect,
                get
            );
        }
    }

    #[test]
    fn test_compare() {
        let cases = vec![
            (
                "2011-10-10 11:11:11",
                "2011-10-10 11:11:11",
                Ordering::Equal,
            ),
            (
                "2011-10-10 11:11:11.123456",
                "2011-10-10 11:11:11.1",
                Ordering::Greater,
            ),
            (
                "2011-10-10 11:11:11",
                "2011-10-10 11:11:11.123",
                Ordering::Less,
            ),
            ("0000-00-00 00:00:00", "2011-10-10 11:11:11", Ordering::Less),
            (
                "0000-00-00 00:00:00",
                "0000-00-00 00:00:00",
                Ordering::Equal,
            ),
        ];

        for (l, r, exp) in cases {
            for_each_tz(move |tz, _offset| {
                let l_t = Time::parse_datetime(l, MAX_FSP, &tz).unwrap();
                let r_t = Time::parse_datetime(r, MAX_FSP, &tz).unwrap();
                assert_eq!(exp, l_t.cmp(&r_t));
            });
        }
    }

    #[test]
    fn test_parse_datetime_format() {
        let cases = vec![
            (
                "2011-11-11 10:10:10.123456",
                vec!["2011", "11", "11", "10", "10", "10", "123456"],
            ),
            (
                "  2011-11-11 10:10:10.123456  ",
                vec!["2011", "11", "11", "10", "10", "10", "123456"],
            ),
            ("2011-11-11 10", vec!["2011", "11", "11", "10"]),
            (
                "2011-11-11T10:10:10.123456",
                vec!["2011", "11", "11", "10", "10", "10", "123456"],
            ),
            (
                "2011:11:11T10:10:10.123456",
                vec!["2011", "11", "11", "10", "10", "10", "123456"],
            ),
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

    #[test]
    fn test_round_frac() {
        let ok_tables = vec![
            (
                "2012-12-31 11:30:45",
                UNSPECIFIED_FSP,
                "2012-12-31 11:30:45",
            ),
            (
                "0000-00-00 00:00:00",
                UNSPECIFIED_FSP,
                "0000-00-00 00:00:00",
            ),
            (
                "0001-01-01 00:00:00",
                UNSPECIFIED_FSP,
                "0001-01-01 00:00:00",
            ),
            ("00-12-31 11:30:45", UNSPECIFIED_FSP, "2000-12-31 11:30:45"),
            ("12-12-31 11:30:45", UNSPECIFIED_FSP, "2012-12-31 11:30:45"),
            ("2012-12-31", UNSPECIFIED_FSP, "2012-12-31 00:00:00"),
            ("20121231", UNSPECIFIED_FSP, "2012-12-31 00:00:00"),
            ("121231", UNSPECIFIED_FSP, "2012-12-31 00:00:00"),
            (
                "2012^12^31 11+30+45",
                UNSPECIFIED_FSP,
                "2012-12-31 11:30:45",
            ),
            (
                "2012^12^31T11+30+45",
                UNSPECIFIED_FSP,
                "2012-12-31 11:30:45",
            ),
            ("2012-2-1 11:30:45", UNSPECIFIED_FSP, "2012-02-01 11:30:45"),
            ("12-2-1 11:30:45", UNSPECIFIED_FSP, "2012-02-01 11:30:45"),
            ("20121231113045", UNSPECIFIED_FSP, "2012-12-31 11:30:45"),
            ("121231113045", UNSPECIFIED_FSP, "2012-12-31 11:30:45"),
            ("2012-02-29", UNSPECIFIED_FSP, "2012-02-29 00:00:00"),
            ("121231113045.123345", 6, "2012-12-31 11:30:45.123345"),
            ("20121231113045.123345", 6, "2012-12-31 11:30:45.123345"),
            ("121231113045.9999999", 6, "2012-12-31 11:30:46.000000"),
            ("121231113045.999999", 6, "2012-12-31 11:30:45.999999"),
            ("121231113045.999999", 5, "2012-12-31 11:30:46.00000"),
            ("2012-12-31 11:30:45.123456", 4, "2012-12-31 11:30:45.1235"),
            (
                "2012-12-31 11:30:45.123456",
                6,
                "2012-12-31 11:30:45.123456",
            ),
            ("2012-12-31 11:30:45.123456", 0, "2012-12-31 11:30:45"),
            ("2012-12-31 11:30:45.123456", 1, "2012-12-31 11:30:45.1"),
            ("2012-12-31 11:30:45.999999", 4, "2012-12-31 11:30:46.0000"),
            ("2012-12-31 11:30:45.999999", 0, "2012-12-31 11:30:46"),
            ("2012-12-31 23:59:59.999999", 0, "2013-01-01 00:00:00"),
            ("2012-12-31 23:59:59.999999", 3, "2013-01-01 00:00:00.000"),
            // TODO: TIDB can handle this case, but we can't.
            //("2012-00-00 11:30:45.999999", 3, "2012-00-00 11:30:46.000"),
            // TODO: MySQL can handle this case, but we can't.
            // ("2012-01-00 23:59:59.999999", 3, "2012-01-01 00:00:00.000"),
        ];

        for (input, fsp, exp) in ok_tables {
            let mut utc_t = Time::parse_utc_datetime(input, UNSPECIFIED_FSP).unwrap();
            utc_t.round_frac(fsp).unwrap();
            let expect = Time::parse_utc_datetime(exp, UNSPECIFIED_FSP).unwrap();
            assert_eq!(
                utc_t, expect,
                "input:{:?}, exp:{:?}, utc_t:{:?}, expect:{:?}",
                input, exp, utc_t, expect
            );

            for_each_tz(move |tz, offset| {
                let mut t = Time::parse_datetime(input, UNSPECIFIED_FSP, &tz).unwrap();
                t.round_frac(fsp).unwrap();
                let expect = Time::parse_datetime(exp, UNSPECIFIED_FSP, &tz).unwrap();
                assert_eq!(
                    t, expect,
                    "tz:{:?},input:{:?}, exp:{:?}, utc_t:{:?}, expect:{:?}",
                    offset, input, exp, t, expect
                );
            });
        }
    }

    #[test]
    fn test_set_tp() {
        let cases = vec![
            ("2011-11-11 10:10:10.123456", "2011-11-11"),
            ("  2011-11-11 23:59:59", "2011-11-11"),
        ];

        for (s, exp) in cases {
            let mut res = Time::parse_utc_datetime(s, UNSPECIFIED_FSP).unwrap();
            res.set_time_type(TimeType::Date).unwrap();
            res.set_time_type(TimeType::DateTime).unwrap();
            let ep = Time::parse_utc_datetime(exp, UNSPECIFIED_FSP).unwrap();
            assert_eq!(res, ep);
            let res = res.set_time_type(TimeType::Timestamp);
            assert!(res.is_err());
        }
    }

    #[test]
    fn test_from_duration() {
        let cases = vec![("11:30:45.123456"), ("-35:30:46")];
        let tz = Tz::utc();
        for s in cases {
            let d = MyDuration::parse(s.as_bytes(), MAX_FSP).unwrap();
            let get = Time::from_duration(&tz, TimeType::DateTime, d).unwrap();
            let get_today = get
                .time
                .checked_sub_signed(Duration::nanoseconds(d.to_nanos()))
                .unwrap();
            let now = Utc::now();
            assert_eq!(get_today.year(), now.year());
            assert_eq!(get_today.month(), now.month());
            assert_eq!(get_today.day(), now.day());
            assert_eq!(get_today.hour(), 0);
            assert_eq!(get_today.minute(), 0);
            assert_eq!(get_today.second(), 0);
        }
    }

    #[test]
    fn test_convert_to_duration() {
        let cases = vec![
            ("2012-12-31 11:30:45.123456", 4, "11:30:45.1235"),
            ("2012-12-31 11:30:45.123456", 6, "11:30:45.123456"),
            ("2012-12-31 11:30:45.123456", 0, "11:30:45"),
            ("2012-12-31 11:30:45.999999", 0, "11:30:46"),
            ("2017-01-05 08:40:59.575601", 0, "08:41:00"),
            ("2017-01-05 23:59:59.575601", 0, "00:00:00"),
            ("0000-00-00 00:00:00", 6, "00:00:00"),
        ];
        let mut ctx = EvalContext::default();
        for (s, fsp, expect) in cases {
            let t = Time::parse_utc_datetime(s, fsp).unwrap();
            let du: MyDuration = t.convert(&mut ctx).unwrap();
            let get = du.to_string();
            assert_eq!(get, expect);
        }
    }

    #[test]
    fn test_date_format() {
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
            let t = Time::parse_utc_datetime(s, 6).unwrap();
            let get = t.date_format(layout).unwrap();
            assert_eq!(get, expect);
        }
    }

    #[test]
    fn test_chunk_codec() {
        let cases = vec![
            ("2012-12-31 11:30:45.123456", 4),
            ("2012-12-31 11:30:45.123456", 6),
            ("2012-12-31 11:30:45.123456", 0),
            ("2012-12-31 11:30:45.999999", 0),
            ("2017-01-05 08:40:59.575601", 0),
            ("2017-01-05 23:59:59.575601", 0),
            ("0000-00-00 00:00:00", 6),
        ];
        for (s, fsp) in cases {
            let t = Time::parse_utc_datetime(s, fsp).unwrap();
            let mut buf = vec![];
            buf.write_time(&t).unwrap();
            let got = buf.as_slice().read_time().unwrap();
            assert_eq!(got, t);
        }
    }

    #[test]
    fn test_parse_fsp() {
        let cases = vec![
            ("2012-12-31 11:30:45.1234", 4),
            ("2012-12-31 11:30:45.123456", 6),
            ("2012-12-31 11:30:45", 0),
            ("2012-12-31 11:30:45.", 0),
            ("2017-01-05 08:40:59.5756014372987", 6),
            ("2017-01-05 23:59:59....432", 3),
            (".1.2.3.4.5.6", 1),
        ];
        for (s, fsp) in cases {
            let t = Time::parse_fsp(s);
            assert_eq!(fsp, t);
        }
    }

    #[test]
    fn test_checked_add_and_sub_duration() {
        let cases = vec![
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
        for (lhs, rhs, exp) in cases.clone() {
            let lhs = Time::parse_utc_datetime(lhs, 6).unwrap();
            let rhs = MyDuration::parse(rhs.as_bytes(), 6).unwrap();
            let res = lhs.checked_add(rhs).unwrap();
            let exp = Time::parse_utc_datetime(exp, 6).unwrap();
            assert_eq!(res, exp);
        }
        for (exp, rhs, lhs) in cases {
            let lhs = Time::parse_utc_datetime(lhs, 6).unwrap();
            let rhs = MyDuration::parse(rhs.as_bytes(), 6).unwrap();
            let res = lhs.checked_sub(rhs).unwrap();
            let exp = Time::parse_utc_datetime(exp, 6).unwrap();
            assert_eq!(res, exp);
        }

        let lhs = Time::parse_utc_datetime("9999-12-31 23:59:59", 6).unwrap();
        let rhs = MyDuration::parse(b"01:00:00", 6).unwrap();
        assert_eq!(lhs.checked_add(rhs), None);
        let lhs = Time::parse_utc_datetime("0000-01-01 00:00:01", 6).unwrap();
        let rhs = MyDuration::parse(b"01:00:00", 6).unwrap();
        assert_eq!(lhs.checked_sub(rhs), None);
    }
}
