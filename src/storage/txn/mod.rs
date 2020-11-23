// Copyright 2016 TiKV Project Authors. Licensed under Apache-2.0.

//! Storage Transactions

pub mod commands;
pub mod sched_pool;
pub mod scheduler;

mod actions;

pub use actions::commit::commit;
pub use actions::prewrite::prewrite;
pub use actions::shared::prewrite_key_value;

mod latch;
mod store;

use crate::storage::{
    types::{MvccInfo, PessimisticLockRes, PrewriteResult, SecondaryLocksStatus, TxnStatus},
    Error as StorageError, Result as StorageResult,
};
use error_code::{self, ErrorCode, ErrorCodeExt};
use kvproto::kvrpcpb::LockInfo;
use std::error;
use std::fmt;
use std::io::Error as IoError;
use txn_types::{Key, TimeStamp};

pub use self::commands::{Command, RESOLVE_LOCK_BATCH_SIZE};
pub use self::scheduler::Scheduler;
pub use self::store::{
    EntryBatch, FixtureStore, FixtureStoreScanner, Scanner, SnapshotStore, Store, TxnEntry,
    TxnEntryScanner, TxnEntryStore,
};

/// Process result of a command.
#[derive(Debug)]
pub enum ProcessResult {
    Res,
    MultiRes {
        results: Vec<StorageResult<()>>,
    },
    PrewriteResult {
        result: PrewriteResult,
    },
    MvccKey {
        mvcc: MvccInfo,
    },
    MvccStartTs {
        mvcc: Option<(Key, MvccInfo)>,
    },
    Locks {
        locks: Vec<LockInfo>,
    },
    TxnStatus {
        txn_status: TxnStatus,
    },
    NextCommand {
        cmd: Command,
    },
    Failed {
        err: StorageError,
    },
    PessimisticLockRes {
        res: StorageResult<PessimisticLockRes>,
    },
    SecondaryLocksStatus {
        status: SecondaryLocksStatus,
    },
}

impl ProcessResult {
    pub fn maybe_clone(&self) -> Option<ProcessResult> {
        match self {
            ProcessResult::PessimisticLockRes { res: Ok(r) } => {
                Some(ProcessResult::PessimisticLockRes { res: Ok(r.clone()) })
            }
            _ => None,
        }
    }
}

quick_error! {
    #[derive(Debug)]
    pub enum ErrorInner {
        Engine(err: crate::storage::kv::Error) {
            from()
            cause(err)
            display("{}", err)
        }
        Codec(err: tikv_util::codec::Error) {
            from()
            cause(err)
            display("{}", err)
        }
        ProtoBuf(err: protobuf::error::ProtobufError) {
            from()
            cause(err)
            display("{}", err)
        }
        Mvcc(err: crate::storage::mvcc::Error) {
            from()
            cause(err)
            display("{}", err)
        }
        Other(err: Box<dyn error::Error + Sync + Send>) {
            from()
            cause(err.as_ref())
            display("{:?}", err)
        }
        Io(err: IoError) {
            from()
            cause(err)
            display("{}", err)
        }
        InvalidTxnTso {start_ts: TimeStamp, commit_ts: TimeStamp} {
            display("Invalid transaction tso with start_ts:{},commit_ts:{}",
                        start_ts,
                        commit_ts)
        }
        InvalidReqRange {start: Option<Vec<u8>>,
                        end: Option<Vec<u8>>,
                        lower_bound: Option<Vec<u8>>,
                        upper_bound: Option<Vec<u8>>} {
            display("Request range exceeds bound, request range:[{}, end:{}), physical bound:[{}, {})",
                        start.as_ref().map(hex::encode_upper).unwrap_or_else(|| "(none)".to_owned()),
                        end.as_ref().map(hex::encode_upper).unwrap_or_else(|| "(none)".to_owned()),
                        lower_bound.as_ref().map(hex::encode_upper).unwrap_or_else(|| "(none)".to_owned()),
                        upper_bound.as_ref().map(hex::encode_upper).unwrap_or_else(|| "(none)".to_owned()))
        }
        MaxTimestampNotSynced { region_id: u64, start_ts: TimeStamp } {
            display("Prewrite for async commit fails due to potentially stale max timestamp, start_ts: {}, region_id: {}",
                        start_ts,
                        region_id)
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
            ErrorInner::MaxTimestampNotSynced {
                region_id,
                start_ts,
            } => Some(ErrorInner::MaxTimestampNotSynced {
                region_id,
                start_ts,
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

impl ErrorCodeExt for Error {
    fn error_code(&self) -> ErrorCode {
        match self.0.as_ref() {
            ErrorInner::Engine(e) => e.error_code(),
            ErrorInner::Codec(e) => e.error_code(),
            ErrorInner::ProtoBuf(_) => error_code::storage::PROTOBUF,
            ErrorInner::Mvcc(e) => e.error_code(),
            ErrorInner::Other(_) => error_code::storage::UNKNOWN,
            ErrorInner::Io(_) => error_code::storage::IO,
            ErrorInner::InvalidTxnTso { .. } => error_code::storage::INVALID_TXN_TSO,
            ErrorInner::InvalidReqRange { .. } => error_code::storage::INVALID_REQ_RANGE,
            ErrorInner::MaxTimestampNotSynced { .. } => {
                error_code::storage::MAX_TIMESTAMP_NOT_SYNCED
            }
        }
    }
}

pub mod tests {
    use super::*;
    pub use actions::commit::tests::{must_err as must_commit_err, must_succeed as must_commit};
    pub use actions::prewrite::tests::{try_prewrite_check_not_exists, try_prewrite_insert};
}
