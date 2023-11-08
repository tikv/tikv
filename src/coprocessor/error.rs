// Copyright 2018 TiKV Project Authors. Licensed under Apache-2.0.

use error_code::{self, ErrorCode, ErrorCodeExt};
use thiserror::Error;

use crate::{
    storage,
    storage::{
        kv::{Error as KvError, ErrorInner as KvErrorInner},
        mvcc::{Error as MvccError, ErrorInner as MvccErrorInner},
        txn::{Error as TxnError, ErrorInner as TxnErrorInner},
    },
};

#[derive(Debug, Error)]
pub enum Error {
    #[error("Region error (will back off and retry) {0:?}")]
    Region(kvproto::errorpb::Error),

    #[error("Key is locked (will clean up) {0:?}")]
    Locked(kvproto::kvrpcpb::LockInfo),

    #[error("Coprocessor task terminated due to exceeding the deadline")]
    DeadlineExceeded,

    #[error("Coprocessor task canceled due to exceeding max pending tasks")]
    MaxPendingTasksExceeded,

    #[error("{0}")]
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
        anyhow::Error::from(err).into()
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
            KvError(box KvErrorInner::KeyIsLocked(lock_info)) => Error::Locked(lock_info),
            e => Error::Other(e.to_string()),
        }
    }
}

impl From<MvccError> for Error {
    fn from(err: MvccError) -> Self {
        match err {
            MvccError(box MvccErrorInner::KeyIsLocked(info)) => Error::Locked(info),
            MvccError(box MvccErrorInner::Kv(kv_error)) => Error::from(kv_error),
            e => Error::Other(e.to_string()),
        }
    }
}

impl From<TxnError> for Error {
    fn from(err: storage::txn::Error) -> Self {
        match err {
            TxnError(box TxnErrorInner::Mvcc(mvcc_error)) => Error::from(mvcc_error),
            TxnError(box TxnErrorInner::Engine(kv_error)) => Error::from(kv_error),
            e => Error::Other(e.to_string()),
        }
    }
}

impl From<tikv_util::deadline::DeadlineError> for Error {
    fn from(_: tikv_util::deadline::DeadlineError) -> Self {
        Error::DeadlineExceeded
    }
}

impl From<tidb_query_datatype::DataTypeError> for Error {
    fn from(err: tidb_query_datatype::DataTypeError) -> Self {
        Error::Other(err.to_string())
    }
}

impl From<tidb_query_datatype::codec::Error> for Error {
    fn from(err: tidb_query_datatype::codec::Error) -> Self {
        Error::Other(err.to_string())
    }
}

pub type Result<T> = std::result::Result<T, Error>;

impl ErrorCodeExt for Error {
    fn error_code(&self) -> ErrorCode {
        match self {
            Error::Region(e) => e.error_code(),
            Error::Locked(_) => error_code::coprocessor::LOCKED,
            Error::DeadlineExceeded => error_code::coprocessor::DEADLINE_EXCEEDED,
            Error::MaxPendingTasksExceeded => error_code::coprocessor::MAX_PENDING_TASKS_EXCEEDED,
            Error::Other(_) => error_code::UNKNOWN,
        }
    }
}
