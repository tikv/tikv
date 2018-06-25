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

use byteorder::WriteBytesExt;
use std::cmp::Ordering;
use std::fmt::{self, Display, Formatter};
use std::io::Write;
use std::{mem, str};

use chrono::{DateTime, Datelike, Duration, FixedOffset, TimeZone, Timelike, Utc, Weekday};

use super::super::{Error, Result, TEN_POW};
use coprocessor::codec::mysql::duration::{Duration as MyDuration, NANOS_PER_SEC, NANO_WIDTH};
use coprocessor::codec::mysql::Decimal;
use coprocessor::codec::mysql::{self, check_fsp, parse_frac, types};
use util::codec::number::{self, NumberEncoder};
use util::codec::BytesSlice;
const ZERO_DATETIME_STR: &str = "0000-00-00 00:00:00";
const ZERO_DATE_STR: &str = "0000-00-00";
/// In go, `time.Date(0, 0, 0, 0, 0, 0, 0, time.UTC)` will be adjusted to
/// `-0001-11-30 00:00:00 +0000 UTC`, whose timestamp is -62169984000.
const ZERO_TIMESTAMP: i64 = -62169984000;

const MONTH_NAMES: &[&str] = &[
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

bitflags! {
    pub struct WeekMode: u32 {
        const BEHAVIOR_MONDAY_FIRST  = 0b00000001;
        const BEHAVIOR_YEAR          = 0b00000010;
        const BEHAVIOR_FIRST_WEEKDAY = 0b00000100;
    }
}

impl WeekMode {
    pub fn to_normalized(self) -> WeekMode {
        let mut mode = self;
        if !mode.contains(WeekMode::BEHAVIOR_MONDAY_FIRST) {
            mode ^= WeekMode::BEHAVIOR_FIRST_WEEKDAY;
        }
        mode
    }
}

pub trait WeekdayExtension {
    fn name(&self) -> &'static str;
    fn name_abbr(&self) -> &'static str;
}

impl WeekdayExtension for Weekday {
    fn name(&self) -> &'static str {
        match *self {
            Weekday::Mon => "Monday",
            Weekday::Tue => "Tuesday",
            Weekday::Wed => "Wednesday",
            Weekday::Thu => "Thursday",
            Weekday::Fri => "Friday",
            Weekday::Sat => "Saturday",
            Weekday::Sun => "Sunday",
        }
    }

    fn name_abbr(&self) -> &'static str {
        match *self {
            Weekday::Mon => "Mon",
            Weekday::Tue => "Tue",
            Weekday::Wed => "Wed",
            Weekday::Thu => "Thu",
            Weekday::Fri => "Fri",
            Weekday::Sat => "Sat",
            Weekday::Sun => "Sun",
        }
    }
}

pub trait DateTimeExtension {
    fn days(&self) -> i32;
    fn calc_year_week(
        &self,
        monday_first: bool,
        week_year: bool,
        first_weekday: bool,
    ) -> (i32, i32);
    fn calc_year_week_by_week_mode(&self, week_mode: WeekMode) -> (i32, i32);
    fn week(&self, mode: WeekMode) -> i32;
    fn year_week(&self, mode: WeekMode) -> (i32, i32);
}

impl<Tz: TimeZone> DateTimeExtension for DateTime<Tz> {
    /// returns the day of year starting from 1.
    /// implements TiDB YearDay().
    fn days(&self) -> i32 {
        if self.month() == 0 || self.day() == 0 {
            (0)
        } else {
            self.ordinal() as i32
        }
    }

    /// returns the week of year and year. should not be called directly.
    /// when monday_first == true, Monday is considered as the first day in the week,
    ///         otherwise Sunday.
    /// when week_year == true, week is from 1 to 53, otherwise from 0 to 53.
    /// when first_weekday == true, the week that contains the first 'first-day-of-week' is week 1,
    ///         otherwise weeks are numbered according to ISO 8601:1988.
    fn calc_year_week(
        &self,
        monday_first: bool,
        mut week_year: bool,
        first_weekday: bool,
    ) -> (i32, i32) {
        let week: i32;
        let mut year = self.year();

        let daynr = calc_day_number(self.year(), self.month() as i32, self.day() as i32);
        let mut first_daynr = calc_day_number(self.year(), 1, 1);
        let mut weekday = calc_weekday(first_daynr, !monday_first);
        let mut days: i32;

        if self.month() == 1 && (self.day() as i32) <= 7 - weekday {
            if !week_year && ((first_weekday && weekday != 0) || (!first_weekday && weekday >= 4)) {
                return (year, 0);
            }
            week_year = true;
            year -= 1;
            days = calc_days_in_year(year);
            first_daynr -= days;
            weekday = (weekday + 53 * 7 - days) % 7;
        }

        if (first_weekday && weekday != 0) || (!first_weekday && weekday >= 4) {
            days = daynr - (first_daynr + 7 - weekday);
        } else {
            days = daynr - (first_daynr - weekday);
        }

        if week_year && days >= 52 * 7 {
            weekday = (weekday + calc_days_in_year(year)) % 7;
            if (!first_weekday && weekday < 4) || (first_weekday && weekday == 0) {
                year += 1;
                return (year, 1);
            }
        }
        week = days / 7 + 1;
        (year, week)
    }

    /// returns the week of year according to week mode. should not be called directly.
    /// implements TiDB calcWeek()
    fn calc_year_week_by_week_mode(&self, week_mode: WeekMode) -> (i32, i32) {
        let mode = week_mode.to_normalized();
        let monday_first = mode.contains(WeekMode::BEHAVIOR_MONDAY_FIRST);
        let week_year = mode.contains(WeekMode::BEHAVIOR_YEAR);
        let first_weekday = mode.contains(WeekMode::BEHAVIOR_FIRST_WEEKDAY);
        self.calc_year_week(monday_first, week_year, first_weekday)
    }

    /// returns the week of year.
    /// implementes TiDB Week().
    fn week(&self, mode: WeekMode) -> i32 {
        if self.month() == 0 || self.day() == 0 {
            return 0;
        }
        let (_, week) = self.calc_year_week_by_week_mode(mode);
        week
    }

    /// returns the week of year and year.
    /// implements TiDB YearWeek().
    fn year_week(&self, mode: WeekMode) -> (i32, i32) {
        self.calc_year_week_by_week_mode(mode | WeekMode::BEHAVIOR_YEAR)
    }
}

fn abbr_day_of_month(day: i32) -> &'static str {
    match day {
        1 | 21 | 31 => "st",
        2 | 22 => "nd",
        3 | 23 => "rd",
        _ => "th",
    }
}

// calculates days since 0000-00-00.
fn calc_day_number(mut year: i32, month: i32, day: i32) -> i32 {
    if year == 0 && month == 0 {
        return 0;
    }
    let mut delsum = 365 * year + 31 * (month - 1) + day;
    if month <= 2 {
        year -= 1;
    } else {
        delsum -= (month * 4 + 23) / 10;
    }
    let temp = ((year / 100 + 1) * 3) / 4;
    delsum + year / 4 - temp
}

/// calculates days in one year, it works with 0 <= year <= 99.
fn calc_days_in_year(year: i32) -> i32 {
    if (year & 3) == 0 && (year % 100 != 0 || (year % 400 == 0 && (year != 0))) {
        return 366;
    }
    365
}

/// calculates weekday from daynr, returns 0 for Monday, 1 for Tuesday ...
fn calc_weekday(mut daynr: i32, sunday_first_day: bool) -> i32 {
    daynr += 5;
    if sunday_first_day {
        daynr += 1;
    }
    daynr % 7
}

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
    tz.ymd_opt(year, month, day)
        .and_hms_opt(hour, min, secs)
        .single()
        .and_then(|t| t.checked_add_signed(Duration::nanoseconds(i64::from(nanos))))
        .ok_or_else(|| {
            box_err!(
                "'{}-{}-{} {}:{}:{}.{:09}' is not a valid datetime",
                year,
                month,
                day,
                hour,
                min,
                secs,
                nanos
            )
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
            time,
            tp,
            fsp: check_fsp(fsp)?,
        })
    }

    pub fn get_tp(&self) -> u8 {
        self.tp
    }

    pub fn set_tp(&mut self, tp: u8) -> Result<()> {
        if self.tp != tp && tp == types::DATE {
            // Truncate hh:mm::ss part if the type is Date
            self.time = self.time.date().and_hms(0, 0, 0);
        }
        if self.tp != tp && tp == types::TIMESTAMP {
            return Err(box_err!("can not convert datetime/date to timestamp"));
        }
        self.tp = tp;
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
        let fsp = check_fsp(fsp)?;
        let mut frac_str = "";
        let mut need_adjust = false;
        let parts = Time::parse_datetime_format(s);
        let (mut year, month, day, hour, minute, sec): (i32, u32, u32, u32, u32, u32) =
            match *parts.as_slice() {
                [s1] => {
                    need_adjust = s1.len() == 12 || s1.len() == 6;
                    match s1.len() {
                        14 | 12 | 8 | 6 => split_ymd_hms(s1.as_bytes())?,
                        _ => return Err(box_err!("invalid datetime: {}", s)),
                    }
                }
                [s1, frac] => {
                    frac_str = frac;
                    need_adjust = s1.len() == 12;
                    match s1.len() {
                        14 | 12 => split_ymd_hms(s1.as_bytes())?,
                        _ => return Err(box_err!("invalid datetime: {}", s)),
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
                [year, month, day, hour, min, sec] => (
                    box_try!(year.parse()),
                    box_try!(month.parse()),
                    box_try!(day.parse()),
                    box_try!(hour.parse()),
                    box_try!(min.parse()),
                    box_try!(sec.parse()),
                ),
                [year, month, day, hour, min, sec, frac] => {
                    frac_str = frac;
                    (
                        box_try!(year.parse()),
                        box_try!(month.parse()),
                        box_try!(day.parse()),
                        box_try!(hour.parse()),
                        box_try!(min.parse()),
                        box_try!(sec.parse()),
                    )
                }
                _ => return Err(box_err!("invalid datetime: {}", s)),
            };

        if need_adjust || parts[0].len() == 2 {
            if year >= 0 && year <= 69 {
                year += 2000;
            } else if year >= 70 && year <= 99 {
                year += 1900;
            }
        }

        let frac = parse_frac(frac_str.as_bytes(), fsp)?;
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
        Time::new(time, types::DATETIME as u8, fsp as i8)
    }

    /// Get time from packed u64. When `tp` is `TIMESTAMP`, the packed time should
    /// be a UTC time; otherwise the packed time should be in the same timezone as `tz`
    /// specified.
    pub fn from_packed_u64(u: u64, tp: u8, fsp: i8, tz: &FixedOffset) -> Result<Time> {
        if u == 0 {
            return Time::new(zero_time(tz), tp, fsp);
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
        let t = if tp == types::TIMESTAMP {
            let t = ymd_hms_nanos(&Utc, year, month, day, hour, minute, second, nanosec)?;
            tz.from_utc_datetime(&t.naive_utc())
        } else {
            ymd_hms_nanos(tz, year, month, day, hour, minute, second, nanosec)?
        };
        Time::new(t, tp, fsp as i8)
    }

    pub fn from_duration(tz: &FixedOffset, tp: u8, d: &MyDuration) -> Result<Time> {
        let dur = Duration::nanoseconds(d.to_nanos());
        let t = Utc::now()
            .with_timezone(tz)
            .date()
            .and_hms(0, 0, 0)
            .checked_add_signed(dur);
        if t.is_none() {
            return Err(box_err!("parse from duration {} overflows", d));
        }

        let t = t.unwrap();
        if t.year() < 1000 || t.year() > 9999 {
            return Err(box_err!(
                "datetime :{:?} out of range ('1000-01-01' to '9999-12-31')",
                t
            ));
        }
        if tp == types::DATE {
            let t = t.date().and_hms(0, 0, 0);
            Time::new(t, tp, d.fsp as i8)
        } else {
            Time::new(t, tp, d.fsp as i8)
        }
    }

    pub fn to_duration(&self) -> Result<MyDuration> {
        if self.is_zero() {
            return Ok(MyDuration::zero());
        }
        let nanos = i64::from(self.time.num_seconds_from_midnight()) * NANOS_PER_SEC
            + i64::from(self.time.nanosecond());
        MyDuration::from_nanos(nanos, self.fsp as i8)
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
        let ymd = ((t.year() as u64 * 13 + u64::from(t.month())) << 5) | u64::from(t.day());
        let hms =
            (u64::from(t.hour()) << 12) | (u64::from(t.minute()) << 6) | u64::from(t.second());
        let micro = u64::from(t.nanosecond()) / 1000;
        (((ymd << 17) | hms) << 24) | micro
    }

    pub fn round_frac(&mut self, fsp: i8) -> Result<()> {
        if self.tp == types::DATE || self.is_zero() {
            // date type has no fsp
            return Ok(());
        }
        let fsp = check_fsp(fsp)?;
        if fsp == self.fsp {
            return Ok(());
        }
        // TODO:support case month or day is 0(2012-00-00 12:12:12)
        let nanos = self.time.nanosecond();
        let base = 10u32.pow(NANO_WIDTH - u32::from(fsp));
        let expect_nanos = ((f64::from(nanos) / f64::from(base)).round() as u32) * base;
        let diff = i64::from(nanos) - i64::from(expect_nanos);
        let new_time = self.time.checked_add_signed(Duration::nanoseconds(diff));

        if new_time.is_none() {
            Err(box_err!("round_frac {} overflows", self.time))
        } else {
            self.time = new_time.unwrap();
            self.fsp = fsp;
            Ok(())
        }
    }

    fn convert_date_format(&self, b: char) -> Result<String> {
        match b {
            'b' => {
                let m = self.time.month();
                if m == 0 || m > 12 {
                    Err(box_err!("invalid time format"))
                } else {
                    Ok(MONTH_NAMES_ABBR[(m - 1) as usize].to_string())
                }
            }
            'M' => {
                let m = self.time.month();
                if m == 0 || m > 12 {
                    Err(box_err!("invalid time format"))
                } else {
                    Ok(MONTH_NAMES[(m - 1) as usize].to_string())
                }
            }
            'm' => Ok(format!("{:02}", self.time.month())),
            'c' => Ok(format!("{}", self.time.month())),
            'D' => Ok(format!(
                "{}{}",
                self.time.day(),
                abbr_day_of_month(self.time.day() as i32)
            )),
            'd' => Ok(format!("{:02}", self.time.day())),
            'e' => Ok(format!("{}", self.time.day())),
            'j' => Ok(format!("{:03}", self.time.days())),
            'H' => Ok(format!("{:02}", self.time.hour())),
            'k' => Ok(format!("{}", self.time.hour())),
            'h' | 'I' => {
                let t = self.time.hour();
                if t == 0 || t == 12 {
                    Ok("12".to_string())
                } else {
                    Ok(format!("{:02}", t % 12))
                }
            }
            'l' => {
                let t = self.time.hour();
                if t == 0 || t == 12 {
                    Ok("12".to_string())
                } else {
                    Ok(format!("{}", t % 12))
                }
            }
            'i' => Ok(format!("{:02}", self.time.minute())),
            'p' => {
                let hour = self.time.hour();
                if (hour / 12) % 2 == 0 {
                    Ok("AM".to_string())
                } else {
                    Ok("PM".to_string())
                }
            }
            'r' => {
                let h = self.time.hour();
                if h == 0 {
                    Ok(format!(
                        "{:02}:{:02}:{:02} AM",
                        12,
                        self.time.minute(),
                        self.time.second()
                    ))
                } else if h == 12 {
                    Ok(format!(
                        "{:02}:{:02}:{:02} PM",
                        12,
                        self.time.minute(),
                        self.time.second()
                    ))
                } else if h < 12 {
                    Ok(format!(
                        "{:02}:{:02}:{:02} AM",
                        h,
                        self.time.minute(),
                        self.time.second()
                    ))
                } else {
                    Ok(format!(
                        "{:02}:{:02}:{:02} PM",
                        h - 12,
                        self.time.minute(),
                        self.time.second()
                    ))
                }
            }
            'T' => Ok(format!(
                "{:02}:{:02}:{:02}",
                self.time.hour(),
                self.time.minute(),
                self.time.second()
            )),
            'S' | 's' => Ok(format!("{:02}", self.time.second())),
            'f' => Ok(format!("{:06}", self.time.nanosecond() / 1000)),
            'U' => {
                let w = self.time.week(WeekMode::from_bits_truncate(0));
                Ok(format!("{:02}", w))
            }
            'u' => {
                let w = self.time.week(WeekMode::from_bits_truncate(1));
                Ok(format!("{:02}", w))
            }
            'V' => {
                let w = self.time.week(WeekMode::from_bits_truncate(2));
                Ok(format!("{:02}", w))
            }
            'v' => {
                let (_, w) = self.time.year_week(WeekMode::from_bits_truncate(3));
                Ok(format!("{:02}", w))
            }
            'a' => Ok(self.time.weekday().name_abbr().to_string()),
            'W' => Ok(self.time.weekday().name().to_string()),
            'w' => Ok(format!("{}", self.time.weekday().num_days_from_sunday())),
            'X' => {
                let (year, _) = self.time.year_week(WeekMode::from_bits_truncate(2));
                if year < 0 {
                    Ok(u32::max_value().to_string())
                } else {
                    Ok(format!("{:04}", year))
                }
            }
            'x' => {
                let (year, _) = self.time.year_week(WeekMode::from_bits_truncate(3));
                if year < 0 {
                    Ok(u32::max_value().to_string())
                } else {
                    Ok(format!("{:04}", year))
                }
            }
            'Y' => Ok(format!("{:04}", self.time.year())),
            'y' => {
                let year_str = format!("{:04}", self.time.year());
                Ok(year_str[2..].to_string())
            }
            _ => Ok(b.to_string()),
        }
    }

    pub fn date_format(&self, layout: String) -> Result<String> {
        let mut ret = String::new();
        let mut pattern_match = false;
        for b in layout.chars() {
            if pattern_match {
                ret.push_str(&self.convert_date_format(b)?);
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

impl<T: Write> TimeEncoder for T {}
pub trait TimeEncoder: NumberEncoder {
    fn encode_time(&mut self, v: &Time) -> Result<()> {
        if !v.is_zero() {
            self.encode_u16(v.time.year() as u16)?;
            self.write_u8(v.time.month() as u8)?;
            self.write_u8(v.time.day() as u8)?;
            self.write_u8(v.time.hour() as u8)?;
            self.write_u8(v.time.minute() as u8)?;
            self.write_u8(v.time.second() as u8)?;
            self.encode_u32(v.time.nanosecond() / 1000)?;
        } else {
            let len = mem::size_of::<u16>() + mem::size_of::<u32>() + 5;
            let buf = vec![0; len];
            self.write_all(&buf)?;
        }

        self.write_u8(v.tp)?;
        self.write_u8(v.fsp).map_err(From::from)
    }
}

impl Time {
    /// `decode` decodes time encoded by `encode_time`.
    pub fn decode(data: &mut BytesSlice) -> Result<Time> {
        let year = i32::from(number::decode_u16(data)?);
        let (month, day, hour, minute, second) = if data.len() >= 5 {
            (
                u32::from(data[0]),
                u32::from(data[1]),
                u32::from(data[2]),
                u32::from(data[3]),
                u32::from(data[4]),
            )
        } else {
            return Err(Error::unexpected_eof());
        };
        *data = &data[5..];
        let nanoseconds = 1000 * number::decode_u32(data)?;
        let (tp, fsp) = if data.len() >= 2 {
            (data[0], data[1])
        } else {
            return Err(Error::unexpected_eof());
        };
        *data = &data[2..];
        let tz = FixedOffset::east(0); // TODO
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
        let t = if tp == types::TIMESTAMP {
            let t = ymd_hms_nanos(&Utc, year, month, day, hour, minute, second, nanoseconds)?;
            tz.from_utc_datetime(&t.naive_utc())
        } else {
            ymd_hms_nanos(
                &FixedOffset::east(0),
                year,
                month,
                day,
                hour,
                minute,
                second,
                nanoseconds,
            )?
        };
        Time::new(t, tp, fsp as i8)
    }
}

#[cfg(test)]
mod test {
    use super::*;

    use std::cmp::Ordering;

    use chrono::{Duration, FixedOffset};

    use coprocessor::codec::mysql::{types, Duration as MyDuration, MAX_FSP, UN_SPECIFIED_FSP};

    const MIN_OFFSET: i32 = -60 * 24 + 1;
    const MAX_OFFSET: i32 = 60 * 24;

    #[test]
    fn test_parse_datetime() {
        let ok_tables = vec![
            (
                "2012-12-31 11:30:45",
                UN_SPECIFIED_FSP,
                "2012-12-31 11:30:45",
            ),
            (
                "0000-00-00 00:00:00",
                UN_SPECIFIED_FSP,
                "0000-00-00 00:00:00",
            ),
            (
                "0001-01-01 00:00:00",
                UN_SPECIFIED_FSP,
                "0001-01-01 00:00:00",
            ),
            ("00-12-31 11:30:45", UN_SPECIFIED_FSP, "2000-12-31 11:30:45"),
            ("12-12-31 11:30:45", UN_SPECIFIED_FSP, "2012-12-31 11:30:45"),
            ("2012-12-31", UN_SPECIFIED_FSP, "2012-12-31 00:00:00"),
            ("20121231", UN_SPECIFIED_FSP, "2012-12-31 00:00:00"),
            ("121231", UN_SPECIFIED_FSP, "2012-12-31 00:00:00"),
            (
                "2012^12^31 11+30+45",
                UN_SPECIFIED_FSP,
                "2012-12-31 11:30:45",
            ),
            (
                "2012^12^31T11+30+45",
                UN_SPECIFIED_FSP,
                "2012-12-31 11:30:45",
            ),
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
                    let exp_t = Time::new(
                        utc_t.time - Duration::seconds(i64::from(offset)),
                        utc_t.tp,
                        utc_t.fsp as i8,
                    ).unwrap();
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
                let reverted_datetime =
                    Time::from_packed_u64(packed, types::DATETIME, fsp, &tz).unwrap();
                assert_eq!(reverted_datetime, t);
                assert_eq!(reverted_datetime.to_packed_u64(), packed);

                let reverted_timestamp =
                    Time::from_packed_u64(packed, types::TIMESTAMP, fsp, &tz).unwrap();
                assert_eq!(
                    reverted_timestamp.time,
                    reverted_datetime.time + Duration::seconds(i64::from(offset))
                );
                assert_eq!(reverted_timestamp.to_packed_u64(), packed);
            }
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
                UN_SPECIFIED_FSP,
                "2012-12-31 11:30:45",
            ),
            (
                "0000-00-00 00:00:00",
                UN_SPECIFIED_FSP,
                "0000-00-00 00:00:00",
            ),
            (
                "0001-01-01 00:00:00",
                UN_SPECIFIED_FSP,
                "0001-01-01 00:00:00",
            ),
            ("00-12-31 11:30:45", UN_SPECIFIED_FSP, "2000-12-31 11:30:45"),
            ("12-12-31 11:30:45", UN_SPECIFIED_FSP, "2012-12-31 11:30:45"),
            ("2012-12-31", UN_SPECIFIED_FSP, "2012-12-31 00:00:00"),
            ("20121231", UN_SPECIFIED_FSP, "2012-12-31 00:00:00"),
            ("121231", UN_SPECIFIED_FSP, "2012-12-31 00:00:00"),
            (
                "2012^12^31 11+30+45",
                UN_SPECIFIED_FSP,
                "2012-12-31 11:30:45",
            ),
            (
                "2012^12^31T11+30+45",
                UN_SPECIFIED_FSP,
                "2012-12-31 11:30:45",
            ),
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
            let mut utc_t = Time::parse_utc_datetime(input, UN_SPECIFIED_FSP).unwrap();
            utc_t.round_frac(fsp).unwrap();
            let expect = Time::parse_utc_datetime(exp, UN_SPECIFIED_FSP).unwrap();
            assert_eq!(
                utc_t, expect,
                "input:{:?}, exp:{:?}, utc_t:{:?}, expect:{:?}",
                input, exp, utc_t, expect
            );

            for mut offset in MIN_OFFSET..MAX_OFFSET {
                offset *= 60;
                let tz = FixedOffset::east(offset);
                let mut t = Time::parse_datetime(input, UN_SPECIFIED_FSP, &tz).unwrap();
                t.round_frac(fsp).unwrap();
                let expect = Time::parse_datetime(exp, UN_SPECIFIED_FSP, &tz).unwrap();
                assert_eq!(
                    t, expect,
                    "tz:{:?},input:{:?}, exp:{:?}, utc_t:{:?}, expect:{:?}",
                    offset, input, exp, t, expect
                );
            }
        }
    }

    #[test]
    fn test_set_tp() {
        let cases = vec![
            ("2011-11-11 10:10:10.123456", "2011-11-11"),
            ("  2011-11-11 23:59:59", "2011-11-11"),
        ];

        for (s, exp) in cases {
            let mut res = Time::parse_utc_datetime(s, UN_SPECIFIED_FSP).unwrap();
            res.set_tp(types::DATE).unwrap();
            res.set_tp(types::DATETIME).unwrap();
            let ep = Time::parse_utc_datetime(exp, UN_SPECIFIED_FSP).unwrap();
            assert_eq!(res, ep);
            let res = res.set_tp(types::TIMESTAMP);
            assert!(res.is_err());
        }
    }

    #[test]
    fn test_from_duration() {
        let cases = vec![("11:30:45.123456"), ("-35:30:46")];
        let tz = FixedOffset::east(0);
        for s in cases {
            let d = MyDuration::parse(s.as_bytes(), MAX_FSP).unwrap();
            let get = Time::from_duration(&tz, types::DATETIME, &d).unwrap();
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
        for (s, fsp, expect) in cases {
            let t = Time::parse_utc_datetime(s, fsp).unwrap();
            let du = t.to_duration().unwrap();
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
            let get = t.date_format(layout.to_string()).unwrap();
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
            buf.encode_time(&t).unwrap();
            let got = Time::decode(&mut buf.as_slice()).unwrap();
            assert_eq!(got, t);
        }
    }
}
