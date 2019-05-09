// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

// TODO: Replace by failure crate.
/// A shortcut to box an error.
macro_rules! invalid_type {
    ($e:expr) => ({
        use crate::codec::Error;
        Error::InvalidDataType(($e).into())
    });
    ($f:tt, $($arg:expr),+) => ({
        use crate::codec::Error;
        Error::InvalidDataType(format!($f, $($arg),+))
    });
}

pub mod batch;
pub mod error;
pub mod mysql;
pub mod data_type;
pub mod datum;
pub mod convert;
pub mod table;

pub use self::error::{Error, Result};
pub use self::datum::Datum;

const TEN_POW: &[u32] = &[
    1,
    10,
    100,
    1_000,
    10_000,
    100_000,
    1_000_000,
    10_000_000,
    100_000_000,
    1_000_000_000,
];
