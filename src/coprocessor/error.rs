// Copyright 2018 TiKV Project Authors. Licensed under Apache-2.0.

use crate::storage;

#[derive(Fail, Debug)]
pub enum Error {
    #[fail(display = "Region error (will back off and retry) {:?}", _0)]
    Region(kvproto::errorpb::Error),

    #[fail(display = "Key is locked (will clean up) {:?}", _0)]
    Locked(kvproto::kvrpcpb::LockInfo),

    #[fail(display = "Coprocessor task terminated due to exceeding max time limit")]
    MaxExecuteTimeExceeded,

    #[fail(display = "Coprocessor task canceled due to exceeding max pending tasks")]
    MaxPendingTasksExceeded,

    #[fail(display = "{}", _0)]
    Other(String),
}

impl From<Box<dyn std::error::Error + Send + Sync>> for Error {
    #[inline]
    fn from(err: Box<dyn std::error::Error + Send + Sync>) -> Self {
        Error::Other(err.to_string())
    }
}

impl From<Error> for tidb_query::error::StorageError {
    fn from(err: Error) -> Self {
        failure::Error::from(err).into()
    }
}

impl From<tidb_query::error::StorageError> for Error {
    fn from(err: tidb_query::error::StorageError) -> Self {
        match err.0.downcast::<Error>() {
            Ok(e) => e,
            Err(e) => box_err!("Unknown storage error: {}", e),
        }
    }
}

impl From<tidb_query::error::EvaluateError> for Error {
    fn from(err: tidb_query::error::EvaluateError) -> Self {
        Error::Other(err.to_string())
    }
}

impl From<tidb_query::Error> for Error {
    fn from(err: tidb_query::Error) -> Self {
        use tidb_query::error::ErrorInner;

        match *err.0 {
            ErrorInner::Storage(err) => err.into(),
            ErrorInner::Evaluate(err) => err.into(),
        }
    }
}

impl From<storage::kv::Error> for Error {
    fn from(err: storage::kv::Error) -> Self {
        match err {
            storage::kv::Error::Request(e) => Error::Region(e),
            e => Error::Other(e.to_string()),
        }
    }
}

impl From<storage::mvcc::Error> for Error {
    fn from(err: storage::mvcc::Error) -> Self {
        match err {
            storage::mvcc::Error::KeyIsLocked(info) => Error::Locked(info),
            storage::mvcc::Error::Engine(engine_error) => Error::from(engine_error),
            e => Error::Other(e.to_string()),
        }
    }
}

impl From<storage::txn::Error> for Error {
    fn from(err: storage::txn::Error) -> Self {
        match err {
            storage::txn::Error::Mvcc(mvcc_error) => Error::from(mvcc_error),
            storage::txn::Error::Engine(engine_error) => Error::from(engine_error),
            e => Error::Other(e.to_string()),
        }
    }
}

impl From<tikv_util::deadline::DeadlineError> for Error {
    fn from(_: tikv_util::deadline::DeadlineError) -> Self {
        Error::MaxExecuteTimeExceeded
    }
}

pub type Result<T> = std::result::Result<T, Error>;
