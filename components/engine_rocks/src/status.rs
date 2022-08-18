// Copyright 2022 TiKV Project Authors. Licensed under Apache-2.0.

/// A function that will transform a rocksdb error to engine trait error.
///
/// r stands for rocksdb, e stands for engine_trait.
pub fn r2e(msg: impl Into<String>) -> engine_traits::Error {
    // TODO: use correct code.
    engine_traits::Error::Engine(engine_traits::Status::with_error(
        engine_traits::Code::IoError,
        msg,
    ))
}

/// A function that will transform a engine trait error to rocksdb error.
///
/// r stands for rocksdb, e stands for engine_trait.
pub fn e2r(s: engine_traits::Error) -> String {
    format!("{:?}", s)
}
