// Copyright 2022 TiKV Project Authors. Licensed under Apache-2.0.

pub fn from_rocksdb_error(msg: impl Into<String>) -> engine_traits::Error {
    // TODO: use correct code.
    engine_traits::Error::Engine(engine_traits::Status::with_error(
        engine_traits::Code::IoError,
        msg,
    ))
}

pub fn from_engine_traits_error(s: engine_traits::Error) -> String {
    format!("{:?}", s)
}

/// A macro that will transform a rocksdb error to engine trait error.
///
/// r stands for rocksdb, e stands for engine_trait.
macro_rules! r2e {
    ($res:expr) => {{ ($res).map_err($crate::status::from_rocksdb_error) }};
}

pub(crate) use r2e;

/// A trait that will transform a engine trait error to rocksdb error.
///
/// r stands for rocksdb, e stands for engine_trait.
macro_rules! e2r {
    ($res:expr) => {{ ($res).map_err($crate::status::from_engine_traits_error) }};
}

pub(crate) use e2r;
