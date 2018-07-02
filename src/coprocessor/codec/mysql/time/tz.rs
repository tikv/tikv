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

use chrono::{FixedOffset, LocalResult, NaiveDate, NaiveDateTime, Offset, TimeZone};
use chrono_tz;

#[derive(Clone, Debug)]
pub enum Tz {
    Offset(FixedOffset),
    Name(chrono_tz::Tz),
}

impl TimeZone for Tz {
    type Offset = FixedOffset;

    fn from_offset(offset: &Self::Offset) -> Self {
        Tz::Offset(FixedOffset::from_offset(offset))
    }

    fn offset_from_local_date(&self, local: &NaiveDate) -> LocalResult<Self::Offset> {
        match *self {
            Tz::Offset(ref offset) => offset.offset_from_local_date(local),
            Tz::Name(ref tz) => tz
                .offset_from_local_date(local)
                .map(|tz_offset| tz_offset.fix()),
        }
    }

    fn offset_from_local_datetime(&self, local: &NaiveDateTime) -> LocalResult<Self::Offset> {
        match *self {
            Tz::Offset(ref offset) => offset.offset_from_local_datetime(local),
            Tz::Name(ref tz) => tz
                .offset_from_local_datetime(local)
                .map(|tz_offset| tz_offset.fix()),
        }
    }

    fn offset_from_utc_date(&self, utc: &NaiveDate) -> Self::Offset {
        match *self {
            Tz::Offset(ref offset) => offset.offset_from_utc_date(utc),
            Tz::Name(ref tz) => tz.offset_from_utc_date(utc).fix(),
        }
    }

    fn offset_from_utc_datetime(&self, utc: &NaiveDateTime) -> Self::Offset {
        match *self {
            Tz::Offset(ref offset) => offset.offset_from_utc_datetime(utc),
            Tz::Name(ref tz) => tz.offset_from_utc_datetime(utc).fix(),
        }
    }
}
