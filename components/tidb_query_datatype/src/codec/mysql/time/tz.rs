// Copyright 2018 TiKV Project Authors. Licensed under Apache-2.0.

use std::{fmt, str::FromStr};

use chrono::*;

/// A time zone represented by either offset (i.e. +8) or name (i.e. Asia/Shanghai). In addition,
/// local time zone is also valid.
#[derive(Clone)]
pub enum Tz {
    /// A time zone specified by offset seconds.
    Offset(FixedOffset),

    /// A time zone specified by name (in IANA database).
    Name(chrono_tz::Tz),

    /// A local time zone.
    Local(Local),
    // chrono also has a offset kind `chrono::offset::Utc`, but we only use UTC in tests,
    // so we don't introduce an extra enum item for it.
}

impl Tz {
    /// Constructs a time zone from the offset in seconds.
    pub fn from_offset(secs: i64) -> Option<Self> {
        FixedOffset::east_opt(secs as i32).map(Tz::Offset)
    }

    /// Constructs a time zone from the name. If the specified time zone name is `system`,
    /// a local time zone will be constructed.
    pub fn from_tz_name(name: &str) -> Option<Self> {
        if name.to_lowercase() == "system" {
            Some(Tz::local())
        } else {
            chrono_tz::Tz::from_str(name).ok().map(Tz::Name)
        }
    }

    /// Constructs a UTC time zone.
    pub fn utc() -> Self {
        Tz::Name(chrono_tz::UTC)
    }

    /// Constructs a local time zone.
    pub fn local() -> Self {
        Tz::Local(Local)
    }
}

impl fmt::Debug for Tz {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match *self {
            Tz::Offset(ref offset) => write!(f, "Tz::Offset({})", offset),
            Tz::Name(ref offset) => write!(f, "Tz::Name({:?})", offset),
            Tz::Local(_) => write!(f, "Tz::Local"),
        }
    }
}

impl fmt::Display for Tz {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match *self {
            // `Display` is not implemented for them, so we use `Debug`.
            Tz::Offset(ref offset) => fmt::Debug::fmt(offset, f),
            Tz::Name(ref offset) => fmt::Debug::fmt(offset, f),
            Tz::Local(_) => write!(f, "Local"),
        }
    }
}

impl TimeZone for Tz {
    type Offset = TzOffset;

    fn from_offset(offset: &<Self as TimeZone>::Offset) -> Self {
        match *offset {
            TzOffset::Local(ref offset) => Tz::Local(Local::from_offset(offset)),
            TzOffset::Fixed(ref offset) => Tz::Offset(FixedOffset::from_offset(offset)),
            TzOffset::NonFixed(ref offset) => Tz::Name(chrono_tz::Tz::from_offset(offset)),
        }
    }

    fn offset_from_local_date(&self, local: &NaiveDate) -> LocalResult<<Self as TimeZone>::Offset> {
        match *self {
            Tz::Local(ref offset) => offset.offset_from_local_date(local).map(TzOffset::Local),
            Tz::Offset(ref offset) => offset.offset_from_local_date(local).map(TzOffset::Fixed),
            Tz::Name(ref offset) => offset.offset_from_local_date(local).map(TzOffset::NonFixed),
        }
    }

    fn offset_from_local_datetime(
        &self,
        local: &NaiveDateTime,
    ) -> LocalResult<<Self as TimeZone>::Offset> {
        match *self {
            Tz::Local(ref offset) => offset
                .offset_from_local_datetime(local)
                .map(TzOffset::Local),
            Tz::Offset(ref offset) => offset
                .offset_from_local_datetime(local)
                .map(TzOffset::Fixed),
            Tz::Name(ref offset) => offset
                .offset_from_local_datetime(local)
                .map(TzOffset::NonFixed),
        }
    }

    fn offset_from_utc_date(&self, utc: &NaiveDate) -> <Self as TimeZone>::Offset {
        match *self {
            Tz::Local(ref offset) => TzOffset::Local(offset.offset_from_utc_date(utc)),
            Tz::Offset(ref offset) => TzOffset::Fixed(offset.offset_from_utc_date(utc)),
            Tz::Name(ref offset) => TzOffset::NonFixed(offset.offset_from_utc_date(utc)),
        }
    }

    fn offset_from_utc_datetime(&self, utc: &NaiveDateTime) -> <Self as TimeZone>::Offset {
        match *self {
            Tz::Local(ref offset) => TzOffset::Local(offset.offset_from_utc_datetime(utc)),
            Tz::Offset(ref offset) => TzOffset::Fixed(offset.offset_from_utc_datetime(utc)),
            Tz::Name(ref offset) => TzOffset::NonFixed(offset.offset_from_utc_datetime(utc)),
        }
    }

    fn from_local_date(&self, local: &NaiveDate) -> LocalResult<Date<Self>> {
        match *self {
            Tz::Local(ref offset) => offset
                .from_local_date(local)
                .map(|t| Date::from_utc(t.naive_utc(), TzOffset::Local(*t.offset()))),
            Tz::Offset(ref offset) => offset
                .from_local_date(local)
                .map(|t| Date::from_utc(t.naive_utc(), TzOffset::Fixed(*t.offset()))),
            Tz::Name(ref offset) => offset
                .from_local_date(local)
                .map(|t| Date::from_utc(t.naive_utc(), TzOffset::NonFixed(*t.offset()))),
        }
    }

    fn from_local_datetime(&self, local: &NaiveDateTime) -> LocalResult<DateTime<Self>> {
        match *self {
            Tz::Local(ref offset) => offset
                .from_local_datetime(local)
                .map(|t| DateTime::from_utc(t.naive_utc(), TzOffset::Local(*t.offset()))),
            Tz::Offset(ref offset) => offset
                .from_local_datetime(local)
                .map(|t| DateTime::from_utc(t.naive_utc(), TzOffset::Fixed(*t.offset()))),
            Tz::Name(ref offset) => offset
                .from_local_datetime(local)
                .map(|t| DateTime::from_utc(t.naive_utc(), TzOffset::NonFixed(*t.offset()))),
        }
    }

    fn from_utc_date(&self, utc: &NaiveDate) -> Date<Self> {
        match *self {
            Tz::Local(ref offset) => {
                let t = offset.from_utc_date(utc);
                Date::from_utc(t.naive_utc(), TzOffset::Local(*t.offset()))
            }
            Tz::Offset(ref offset) => {
                let t = offset.from_utc_date(utc);
                Date::from_utc(t.naive_utc(), TzOffset::Fixed(*t.offset()))
            }
            Tz::Name(ref offset) => {
                let t = offset.from_utc_date(utc);
                Date::from_utc(t.naive_utc(), TzOffset::NonFixed(*t.offset()))
            }
        }
    }

    fn from_utc_datetime(&self, utc: &NaiveDateTime) -> DateTime<Self> {
        match *self {
            Tz::Local(ref offset) => {
                let t = offset.from_utc_datetime(utc);
                DateTime::from_utc(t.naive_utc(), TzOffset::Local(*t.offset()))
            }
            Tz::Offset(ref offset) => {
                let t = offset.from_utc_datetime(utc);
                DateTime::from_utc(t.naive_utc(), TzOffset::Fixed(*t.offset()))
            }
            Tz::Name(ref offset) => {
                let t = offset.from_utc_datetime(utc);
                DateTime::from_utc(t.naive_utc(), TzOffset::NonFixed(*t.offset()))
            }
        }
    }
}

/// A time zone offset, corresponding to the [`Tz`].
///
/// `Tz::Local` -> `TzOffset::Local`
/// `Tz::Offset` -> `TzOffset::Fixed`
/// `Tz::Name` -> `TzOffset::NonFixed`
#[derive(Copy, Clone, PartialEq, Eq, Debug)]
pub enum TzOffset {
    Local(FixedOffset),
    Fixed(FixedOffset),
    NonFixed(<chrono_tz::Tz as TimeZone>::Offset),
}

impl Offset for TzOffset {
    fn fix(&self) -> FixedOffset {
        match *self {
            TzOffset::Local(ref offset) => offset.fix(),
            TzOffset::Fixed(ref offset) => offset.fix(),
            TzOffset::NonFixed(ref offset) => offset.fix(),
        }
    }
}

impl fmt::Display for TzOffset {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Debug::fmt(self, f)
    }
}
