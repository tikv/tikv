// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

use std::{error, result};

use error_code::{self, ErrorCode, ErrorCodeExt};
use raft::{Error as RaftError, StorageError};
use thiserror::Error;

#[derive(Debug, Error)]
pub enum Error {
    // Engine uses plain string as the error.
    #[error("Storage Engine {0}")]
    Engine(String),
    // FIXME: It should not know Region.
    #[error(
        "Key {} is out of [region {}] [{}, {})",
        log_wrappers::Value::key(.0), .1, log_wrappers::Value::key(&.2), log_wrappers::Value::key(&.3)
    )]
    NotInRange(Vec<u8>, u64, Vec<u8>, Vec<u8>),
    #[error("Protobuf {0}")]
    Protobuf(#[from] protobuf::ProtobufError),
    #[error("Io {0}")]
    Io(#[from] std::io::Error),
    #[error("{0:?}")]
    Other(#[from] Box<dyn error::Error + Sync + Send>),
    #[error("CF {0} not found")]
    CFName(String),
    #[error("Codec {0}")]
    Codec(#[from] tikv_util::codec::Error),
    #[error("The entries of region is unavailable")]
    EntriesUnavailable,
    #[error("The entries of region is compacted")]
    EntriesCompacted,
}

impl From<String> for Error {
    fn from(err: String) -> Self {
        Error::Engine(err)
    }
}

pub type Result<T> = result::Result<T, Error>;

impl ErrorCodeExt for Error {
    fn error_code(&self) -> ErrorCode {
        match self {
            Error::Engine(_) => error_code::engine::ENGINE,
            Error::NotInRange(_, _, _, _) => error_code::engine::NOT_IN_RANGE,
            Error::Protobuf(_) => error_code::engine::PROTOBUF,
            Error::Io(_) => error_code::engine::IO,
            Error::CFName(_) => error_code::engine::CF_NAME,
            Error::Codec(_) => error_code::engine::CODEC,
            Error::Other(_) => error_code::UNKNOWN,
            Error::EntriesUnavailable => error_code::engine::DATALOSS,
            Error::EntriesCompacted => error_code::engine::DATACOMPACTED,
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
