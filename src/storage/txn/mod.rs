// Copyright 2016 TiKV Project Authors. Licensed under Apache-2.0.

//! Storage Transactions

pub mod commands;
pub mod sched_pool;
pub mod scheduler;

mod latch;
mod process;
mod store;

use crate::storage::{
    types::{MvccInfo, TxnStatus},
    Error as StorageError, Result as StorageResult,
};
use kvproto::kvrpcpb::LockInfo;
use std::error;
use std::fmt;
use std::io::Error as IoError;
use txn_types::{Key, TimeStamp};

pub use self::commands::{Command, PointGetCommand};
pub use self::process::RESOLVE_LOCK_BATCH_SIZE;
pub use self::scheduler::{Msg, Scheduler};
pub use self::store::{EntryBatch, TxnEntry, TxnEntryScanner, TxnEntryStore};
pub use self::store::{FixtureStore, FixtureStoreScanner};
pub use self::store::{Scanner, SnapshotStore, Store};

/// Process result of a command.
pub enum ProcessResult {
    Res,
    MultiRes { results: Vec<StorageResult<()>> },
    MvccKey { mvcc: MvccInfo },
    MvccStartTs { mvcc: Option<(Key, MvccInfo)> },
    Locks { locks: Vec<LockInfo> },
    TxnStatus { txn_status: TxnStatus },
    NextCommand { cmd: Command },
    Failed { err: StorageError },
}

quick_error! {
    #[derive(Debug)]
    pub enum ErrorInner {
        Engine(err: crate::storage::kv::Error) {
            from()
            cause(err)
            description(err.description())
        }
        Codec(err: tikv_util::codec::Error) {
            from()
            cause(err)
            description(err.description())
        }
        ProtoBuf(err: protobuf::error::ProtobufError) {
            from()
            cause(err)
            description(err.description())
        }
        Mvcc(err: crate::storage::mvcc::Error) {
            from()
            cause(err)
            description(err.description())
        }
        Other(err: Box<dyn error::Error + Sync + Send>) {
            from()
            cause(err.as_ref())
            description(err.description())
            display("{:?}", err)
        }
        Io(err: IoError) {
            from()
            cause(err)
            description(err.description())
        }
        InvalidTxnTso {start_ts: TimeStamp, commit_ts: TimeStamp} {
            description("Invalid transaction tso")
            display("Invalid transaction tso with start_ts:{},commit_ts:{}",
                        start_ts,
                        commit_ts)
        }
        InvalidReqRange {start: Option<Vec<u8>>,
                        end: Option<Vec<u8>>,
                        lower_bound: Option<Vec<u8>>,
                        upper_bound: Option<Vec<u8>>} {
            description("Invalid request range")
            display("Request range exceeds bound, request range:[{}, end:{}), physical bound:[{}, {})",
                        start.as_ref().map(hex::encode_upper).unwrap_or_else(|| "(none)".to_owned()),
                        end.as_ref().map(hex::encode_upper).unwrap_or_else(|| "(none)".to_owned()),
                        lower_bound.as_ref().map(hex::encode_upper).unwrap_or_else(|| "(none)".to_owned()),
                        upper_bound.as_ref().map(hex::encode_upper).unwrap_or_else(|| "(none)".to_owned()))
        }
    }
}

impl ErrorInner {
    pub fn maybe_clone(&self) -> Option<ErrorInner> {
        match *self {
            ErrorInner::Engine(ref e) => e.maybe_clone().map(ErrorInner::Engine),
            ErrorInner::Codec(ref e) => e.maybe_clone().map(ErrorInner::Codec),
            ErrorInner::Mvcc(ref e) => e.maybe_clone().map(ErrorInner::Mvcc),
            ErrorInner::InvalidTxnTso {
                start_ts,
                commit_ts,
            } => Some(ErrorInner::InvalidTxnTso {
                start_ts,
                commit_ts,
            }),
            ErrorInner::InvalidReqRange {
                ref start,
                ref end,
                ref lower_bound,
                ref upper_bound,
            } => Some(ErrorInner::InvalidReqRange {
                start: start.clone(),
                end: end.clone(),
                lower_bound: lower_bound.clone(),
                upper_bound: upper_bound.clone(),
            }),
            ErrorInner::Other(_) | ErrorInner::ProtoBuf(_) | ErrorInner::Io(_) => None,
        }
    }
}

pub struct Error(pub Box<ErrorInner>);

impl Error {
    pub fn maybe_clone(&self) -> Option<Error> {
        self.0.maybe_clone().map(Error::from)
    }
}

impl fmt::Debug for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Debug::fmt(&self.0, f)
    }
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Display::fmt(&self.0, f)
    }
}

impl std::error::Error for Error {
    fn description(&self) -> &str {
        std::error::Error::description(&self.0)
    }

    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        std::error::Error::source(&self.0)
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
