// Copyright 2018 PingCAP, Inc.
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

use std::fmt;
use std::str::FromStr;

use chrono::{FixedOffset, LocalResult, NaiveDate, NaiveDateTime, Offset, TimeZone};
use chrono_tz;

/// A time zone represented by either offset (i.e. +8) or name (i.e. Asia/Shanghai).
/// In this way, we can eliminate a `Box` while not introducing extra type parameter.
#[derive(Clone, Copy, PartialEq, Eq)]
pub enum Tz {
    Offset(FixedOffset),
    Name(chrono_tz::Tz),
}

impl Tz {
    /// Constructs a time zone from the offset in seconds.
    pub fn from_offset(secs: i64) -> Option<Self> {
        FixedOffset::east_opt(secs as i32).map(Tz::Offset)
    }

    /// Constructs a time zone from the name.
    pub fn from_tz_name(name: &str) -> Option<Self> {
        chrono_tz::Tz::from_str(name).ok().map(Tz::Name)
    }

    /// Constructs a UTC time zone.
    pub fn utc() -> Self {
        Tz::Name(chrono_tz::UTC)
    }
}

impl fmt::Debug for Tz {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match *self {
            Tz::Offset(ref offset) => fmt::Debug::fmt(offset, f),
            Tz::Name(ref tz) => fmt::Debug::fmt(tz, f),
        }
    }
}

impl fmt::Display for Tz {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        fmt::Debug::fmt(self, f)
    }
}

impl TimeZone for Tz {
    type Offset = TzOffset;

    fn from_offset(offset: &Self::Offset) -> Self {
        match *offset {
            TzOffset::Fixed(ref offset) => Tz::Offset(FixedOffset::from_offset(offset)),
            TzOffset::NonFixed(ref offset) => Tz::Name(chrono_tz::Tz::from_offset(offset)),
        }
    }

    fn offset_from_local_date(&self, local: &NaiveDate) -> LocalResult<Self::Offset> {
        match *self {
            Tz::Offset(ref offset) => offset.offset_from_local_date(local).map(TzOffset::Fixed),
            Tz::Name(ref tz) => tz.offset_from_local_date(local).map(TzOffset::NonFixed),
        }
    }

    fn offset_from_local_datetime(&self, local: &NaiveDateTime) -> LocalResult<Self::Offset> {
        match *self {
            Tz::Offset(ref offset) => offset
                .offset_from_local_datetime(local)
                .map(TzOffset::Fixed),
            Tz::Name(ref tz) => tz.offset_from_local_datetime(local).map(TzOffset::NonFixed),
        }
    }

    fn offset_from_utc_date(&self, utc: &NaiveDate) -> Self::Offset {
        match *self {
            Tz::Offset(ref offset) => TzOffset::Fixed(offset.offset_from_utc_date(utc)),
            Tz::Name(ref tz) => TzOffset::NonFixed(tz.offset_from_utc_date(utc)),
        }
    }

    fn offset_from_utc_datetime(&self, utc: &NaiveDateTime) -> Self::Offset {
        match *self {
            Tz::Offset(ref offset) => TzOffset::Fixed(offset.offset_from_utc_datetime(utc)),
            Tz::Name(ref tz) => TzOffset::NonFixed(tz.offset_from_utc_datetime(utc)),
        }
    }
}

/// A time zone offset. It is fixed if the time zone is specified by fixed offset,
/// or not if specified by name.
#[derive(Copy, Clone, PartialEq, Eq)]
pub enum TzOffset {
    Fixed(FixedOffset),
    NonFixed(<chrono_tz::Tz as TimeZone>::Offset),
}

impl Offset for TzOffset {
    fn fix(&self) -> FixedOffset {
        match *self {
            TzOffset::Fixed(ref offset) => offset.fix(),
            TzOffset::NonFixed(ref offset) => offset.fix(),
        }
    }
}

impl fmt::Debug for TzOffset {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match *self {
            TzOffset::Fixed(ref offset) => fmt::Debug::fmt(offset, f),
            TzOffset::NonFixed(ref offset) => fmt::Debug::fmt(offset, f),
        }
    }
}

impl fmt::Display for TzOffset {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        fmt::Debug::fmt(self, f)
    }
}
