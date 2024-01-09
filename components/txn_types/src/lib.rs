// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

#![feature(box_patterns)]
#![feature(min_specialization)]

#[allow(unused_extern_crates)]
extern crate tikv_alloc;

use std::io;

use error_code::{self, ErrorCode, ErrorCodeExt};
use kvproto::kvrpcpb;
pub use lock::{Lock, LockType, PessimisticLock, TxnLockRef};
use thiserror::Error;
pub use timestamp::{TimeStamp, TsSet, TSO_PHYSICAL_SHIFT_BITS};
pub use types::{
    insert_old_value_if_resolved, is_short_value, Key, KvPair, LastChange, Mutation, MutationType,
    OldValue, OldValues, TxnExtra, TxnExtraScheduler, Value, WriteBatchFlags, SHORT_VALUE_MAX_LEN,
};
pub use write::{Write, WriteRef, WriteType};

mod lock;
mod timestamp;
mod types;
mod write;

#[derive(Debug, Error)]
pub enum ErrorInner {
    #[error("{0}")]
    Io(#[from] io::Error),
    #[error("{0}")]
    Codec(#[from] tikv_util::codec::Error),
    #[error("bad format lock data")]
    BadFormatLock,
    #[error("bad format write data")]
    BadFormatWrite,
    #[error("key is locked (backoff or cleanup) {0:?}")]
    KeyIsLocked(kvproto::kvrpcpb::LockInfo),
    #[error(
        "write conflict, start_ts: {}, conflict_start_ts: {}, conflict_commit_ts: {}, key: {}, primary: {}, reason: {:?}",
        .start_ts, .conflict_start_ts, .conflict_commit_ts,
        log_wrappers::Value::key(.key), log_wrappers::Value::key(.primary), .reason
    )]
    WriteConflict {
        start_ts: TimeStamp,
        conflict_start_ts: TimeStamp,
        conflict_commit_ts: TimeStamp,
        key: Vec<u8>,
        primary: Vec<u8>,
        reason: kvrpcpb::WriteConflictReason,
    },
}

impl ErrorInner {
    pub fn maybe_clone(&self) -> Option<ErrorInner> {
        match self {
            ErrorInner::Codec(e) => e.maybe_clone().map(ErrorInner::Codec),
            ErrorInner::BadFormatLock => Some(ErrorInner::BadFormatLock),
            ErrorInner::BadFormatWrite => Some(ErrorInner::BadFormatWrite),
            ErrorInner::KeyIsLocked(info) => Some(ErrorInner::KeyIsLocked(info.clone())),
            ErrorInner::Io(_) => None,
            ErrorInner::WriteConflict {
                start_ts,
                conflict_start_ts,
                conflict_commit_ts,
                key,
                primary,
                reason,
            } => Some(ErrorInner::WriteConflict {
                start_ts: *start_ts,
                conflict_start_ts: *conflict_start_ts,
                conflict_commit_ts: *conflict_commit_ts,
                key: key.to_owned(),
                primary: primary.to_owned(),
                reason: reason.to_owned(),
            }),
        }
    }
}

#[derive(Debug, Error)]
#[error(transparent)]
pub struct Error(#[from] pub Box<ErrorInner>);

impl Error {
    pub fn maybe_clone(&self) -> Option<Error> {
        self.0.maybe_clone().map(Error::from)
    }
}

impl From<ErrorInner> for Error {
    #[inline]
    fn from(e: ErrorInner) -> Self {
        Error(Box::new(e))
    }
}

impl<T: Into<ErrorInner>> From<T> for Error {
    #[inline]
    default fn from(err: T) -> Self {
        let err = err.into();
        err.into()
    }
}

pub type Result<T> = std::result::Result<T, Error>;

impl ErrorCodeExt for Error {
    fn error_code(&self) -> ErrorCode {
        match self.0.as_ref() {
            ErrorInner::Io(_) => error_code::storage::IO,
            ErrorInner::Codec(e) => e.error_code(),
            ErrorInner::BadFormatLock => error_code::storage::BAD_FORMAT_LOCK,
            ErrorInner::BadFormatWrite => error_code::storage::BAD_FORMAT_WRITE,
            ErrorInner::KeyIsLocked(_) => error_code::storage::KEY_IS_LOCKED,
            ErrorInner::WriteConflict { .. } => error_code::storage::WRITE_CONFLICT,
        }
    }
}
