// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

use std::fmt;
use std::time::{SystemTime, UNIX_EPOCH};

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub struct TimeStamp(u64);

const TSO_PHYSICAL_SHIFT_BITS: u64 = 18;

impl TimeStamp {
    /// Create a time stamp from physical and logical components.
    pub fn compose(physical: u64, logical: u64) -> TimeStamp {
        TimeStamp((physical << TSO_PHYSICAL_SHIFT_BITS) + logical)
    }

    pub const fn zero() -> TimeStamp {
        TimeStamp(0)
    }

    pub const fn max() -> TimeStamp {
        TimeStamp(std::u64::MAX)
    }

    pub const fn new(ts: u64) -> TimeStamp {
        TimeStamp(ts)
    }

    /// Extracts physical part of a timestamp, in milliseconds.
    pub fn physical(self) -> u64 {
        self.0 >> TSO_PHYSICAL_SHIFT_BITS
    }

    pub fn next(self) -> TimeStamp {
        TimeStamp(self.0 + 1)
    }

    pub fn prev(self) -> TimeStamp {
        TimeStamp(self.0 - 1)
    }

    pub fn incr(&mut self) -> &mut TimeStamp {
        self.0 += 1;
        self
    }

    pub fn decr(&mut self) -> &mut TimeStamp {
        self.0 -= 1;
        self
    }

    pub fn is_zero(self) -> bool {
        self.0 == 0
    }

    pub fn into_inner(self) -> u64 {
        self.0
    }
}

impl From<u64> for TimeStamp {
    fn from(ts: u64) -> TimeStamp {
        TimeStamp(ts)
    }
}

impl From<&u64> for TimeStamp {
    fn from(ts: &u64) -> TimeStamp {
        TimeStamp(*ts)
    }
}

impl fmt::Display for TimeStamp {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        fmt::Display::fmt(&self.0, f)
    }
}

impl slog::Value for TimeStamp {
    fn serialize(
        &self,
        record: &slog::Record,
        key: slog::Key,
        serializer: &mut dyn slog::Serializer,
    ) -> slog::Result {
        slog::Value::serialize(&self.0, record, key, serializer)
    }
}

/// A time in seconds since the start of the Unix epoch.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub struct UnixSecs(u64);

impl UnixSecs {
    pub fn now() -> UnixSecs {
        UnixSecs(
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_secs(),
        )
    }

    pub fn zero() -> UnixSecs {
        UnixSecs(0)
    }

    pub fn into_inner(self) -> u64 {
        self.0
    }

    pub fn is_zero(self) -> bool {
        self.0 == 0
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::Key;

    #[test]
    fn test_ts() {
        let physical = 1568700549751;
        let logical = 108;
        let ts = TimeStamp::compose(physical, logical);
        assert_eq!(ts, 411225436913926252.into());

        let extracted_physical = ts.physical();
        assert_eq!(extracted_physical, physical);
    }

    #[test]
    fn test_split_ts() {
        let k = b"k";
        let ts = TimeStamp(123);
        assert!(Key::split_on_ts_for(k).is_err());
        let enc = Key::from_encoded_slice(k).append_ts(ts);
        let res = Key::split_on_ts_for(enc.as_encoded()).unwrap();
        assert_eq!(res, (k.as_ref(), ts));
    }
}
