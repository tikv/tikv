// Copyright 2018 TiKV Project Authors. Licensed under Apache-2.0.

use crate::storage;
use crate::storage::kv::{Error as KvError, ErrorInner as KvErrorInner};
use crate::storage::mvcc::{Error as MvccError, ErrorInner as MvccErrorInner};
use crate::storage::txn::{Error as TxnError, ErrorInner as TxnErrorInner};

#[derive(Fail, Debug)]
pub enum Error {
    #[fail(display = "Region error (will back off and retry) {:?}", _0)]
    Region(kvproto::errorpb::Error),

    #[fail(display = "Key is locked (will clean up) {:?}", _0)]
    Locked(kvproto::kvrpcpb::LockInfo),

    #[fail(display = "Coprocessor task terminated due to exceeding the deadline")]
    DeadlineExceeded,

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

impl From<Error> for tidb_query_common::error::StorageError {
    fn from(err: Error) -> Self {
        failure::Error::from(err).into()
    }
}

impl From<tidb_query_common::error::StorageError> for Error {
    fn from(err: tidb_query_common::error::StorageError) -> Self {
        match err.0.downcast::<Error>() {
            Ok(e) => e,
            Err(e) => box_err!("Unknown storage error: {}", e),
        }
    }
}

impl From<tidb_query_common::error::EvaluateError> for Error {
    fn from(err: tidb_query_common::error::EvaluateError) -> Self {
        Error::Other(err.to_string())
    }
}

impl From<tidb_query_common::Error> for Error {
    fn from(err: tidb_query_common::Error) -> Self {
        use tidb_query_common::error::ErrorInner;

        match *err.0 {
            ErrorInner::Storage(err) => err.into(),
            ErrorInner::Evaluate(err) => err.into(),
        }
    }
}

impl From<KvError> for Error {
    fn from(err: KvError) -> Self {
        match err {
            KvError(box KvErrorInner::Request(e)) => Error::Region(e),
            e => Error::Other(e.to_string()),
        }
    }
}

impl From<MvccError> for Error {
    fn from(err: MvccError) -> Self {
        match err {
            MvccError(box MvccErrorInner::KeyIsLocked(info)) => Error::Locked(info),
            MvccError(box MvccErrorInner::Engine(engine_error)) => Error::from(engine_error),
            e => Error::Other(e.to_string()),
        }
    }
}

impl From<TxnError> for Error {
    fn from(err: storage::txn::Error) -> Self {
        match err {
            TxnError(box TxnErrorInner::Mvcc(mvcc_error)) => Error::from(mvcc_error),
            TxnError(box TxnErrorInner::Engine(engine_error)) => Error::from(engine_error),
            e => Error::Other(e.to_string()),
        }
    }
}

impl From<tikv_util::deadline::DeadlineError> for Error {
    fn from(_: tikv_util::deadline::DeadlineError) -> Self {
        Error::DeadlineExceeded
    }
}

pub type Result<T> = std::result::Result<T, Error>;
