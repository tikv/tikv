// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

use std::{error, result};

use error_code::{self, ErrorCode, ErrorCodeExt};
use raft::{Error as RaftError, StorageError};
use thiserror::Error;

#[repr(u8)]
#[derive(Debug, Copy, Clone, Hash, PartialEq)]
pub enum Code {
    Ok = 0,
    NotFound = 1,
    Corruption = 2,
    NotSupported = 3,
    InvalidArgument = 4,
    IoError = 5,
    MergeInProgress = 6,
    Incomplete = 7,
    ShutdownInProgress = 8,
    TimedOut = 9,
    Aborted = 10,
    Busy = 11,
    Expired = 12,
    TryAgain = 13,
    CompactionTooLarge = 14,
    ColumnFamilyDropped = 15,
}

#[repr(u8)]
#[derive(Debug, Copy, Clone, Hash, PartialEq)]
pub enum SubCode {
    None = 0,
    MutexTimeout = 1,
    LockTimeout = 2,
    LockLimit = 3,
    NoSpace = 4,
    Deadlock = 5,
    StaleFile = 6,
    MemoryLimit = 7,
    SpaceLimit = 8,
    PathNotFound = 9,
    MergeOperandsInsufficientCapacity = 10,
    ManualCompactionPaused = 11,
    Overwritten = 12,
    TxnNotPrepared = 13,
    IoFenced = 14,
}

#[repr(u8)]
#[derive(Debug, Copy, Clone, Hash, PartialEq)]
pub enum Severity {
    NoError = 0,
    SoftError = 1,
    HardError = 2,
    FatalError = 3,
    UnrecoverableError = 4,
}

#[repr(C)]
#[derive(Debug, Error)]
#[error("[{:?}] {:?}-{:?} {}", .code, .sub_code, .sev, .state)]
pub struct Status {
    code: Code,
    sub_code: SubCode,
    sev: Severity,
    state: String,
}

impl Status {
    pub fn with_code(code: Code) -> Status {
        Self {
            code,
            sub_code: SubCode::None,
            sev: Severity::NoError,
            state: String::new(),
        }
    }

    pub fn with_error(code: Code, error: impl Into<String>) -> Self {
        Self {
            code,
            sub_code: SubCode::None,
            sev: Severity::NoError,
            state: error.into(),
        }
    }

    #[inline]
    pub fn set_sub_code(&mut self, sub_code: SubCode) -> &mut Self {
        self.sub_code = sub_code;
        self
    }

    #[inline]
    pub fn set_severity(&mut self, sev: Severity) -> &mut Self {
        self.sev = sev;
        self
    }

    #[inline]
    pub fn code(&self) -> Code {
        self.code
    }

    #[inline]
    pub fn sub_code(&self) -> SubCode {
        self.sub_code
    }

    #[inline]
    pub fn severity(&self) -> Severity {
        self.sev
    }

    #[inline]
    pub fn state(&self) -> &str {
        &self.state
    }
}

#[derive(Debug, Error)]
pub enum Error {
    // Engine uses plain string as the error.
    #[error("Storage Engine {0:?}")]
    Engine(#[from] Status),
    // FIXME: It should not know Region.
    #[error(
        "Key {} is out of [region {}] [{}, {})",
        log_wrappers::Value::key(.key), .region_id, log_wrappers::Value::key(.start), log_wrappers::Value::key(.end)
    )]
    NotInRange {
        key: Vec<u8>,
        region_id: u64,
        start: Vec<u8>,
        end: Vec<u8>,
    },
    #[error("Protobuf {0}")]
    Protobuf(#[from] protobuf::ProtobufError),
    #[error("Io {0}")]
    Io(#[from] std::io::Error),
    #[error("{0:?}")]
    Other(#[from] Box<dyn error::Error + Sync + Send>),
    #[error("CF {0} not found")]
    CfName(String),
    #[error("Codec {0}")]
    Codec(#[from] tikv_util::codec::Error),
    #[error("The entries of region is unavailable")]
    EntriesUnavailable,
    #[error("The entries of region is compacted")]
    EntriesCompacted,
    #[error("Iterator of RangeCacheSnapshot is only supported with boundary set")]
    BoundaryNotSet,
}

pub type Result<T> = result::Result<T, Error>;

impl ErrorCodeExt for Error {
    fn error_code(&self) -> ErrorCode {
        match self {
            Error::Engine(_) => error_code::engine::ENGINE,
            Error::NotInRange { .. } => error_code::engine::NOT_IN_RANGE,
            Error::Protobuf(_) => error_code::engine::PROTOBUF,
            Error::Io(_) => error_code::engine::IO,
            Error::CfName(_) => error_code::engine::CF_NAME,
            Error::Codec(_) => error_code::engine::CODEC,
            Error::Other(_) => error_code::UNKNOWN,
            Error::EntriesUnavailable => error_code::engine::DATALOSS,
            Error::EntriesCompacted => error_code::engine::DATACOMPACTED,
            Error::BoundaryNotSet => error_code::engine::BOUNDARY_NOT_SET,
        }
    }
}

impl From<Error> for RaftError {
    fn from(e: Error) -> RaftError {
        match e {
            Error::EntriesUnavailable => RaftError::Store(StorageError::Unavailable),
            Error::EntriesCompacted => RaftError::Store(StorageError::Compacted),
            e => {
                let boxed = Box::new(e) as Box<dyn std::error::Error + Sync + Send>;
                raft::Error::Store(StorageError::Other(boxed))
            }
        }
    }
}

impl From<Error> for String {
    fn from(e: Error) -> String {
        format!("{:?}", e)
    }
}
