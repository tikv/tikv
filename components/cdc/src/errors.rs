// Copyright 2020 TiKV Project Authors. Licensed under Apache-2.0.

use std::io::Error as IoError;
use std::{error, result};

use engine_traits::Error as EngineTraitsError;
use kvproto::errorpb::Error as ErrorHeader;
use std::time::Duration;
use thiserror::Error;
use tikv::storage::kv::{Error as EngineError, ErrorInner as EngineErrorInner};
use tikv::storage::mvcc::{Error as MvccError, ErrorInner as MvccErrorInner};
use tikv::storage::txn::{Error as TxnError, ErrorInner as TxnErrorInner};
use txn_types::Error as TxnTypesError;

/// The error type for cdc.
#[derive(Debug, Error)]
pub enum Error {
    #[error("Other error {0}")]
    Other(#[from] Box<dyn error::Error + Sync + Send>),
    #[error("RocksDB error {0}")]
    Rocks(String),
    #[error("IO error {0}")]
    Io(#[from] IoError),
    #[error("Engine error {0}")]
    Engine(#[from] EngineError),
    #[error("Transaction error {0}")]
    Txn(#[from] TxnError),
    #[error("Mvcc error {0}")]
    Mvcc(#[from] MvccError),
    #[error("Request error {0:?}")]
    Request(Box<ErrorHeader>),
    #[error("Engine traits error {0}")]
    EngineTraits(#[from] EngineTraitsError),
    #[error("Incremental scan timed out {0:?}")]
    ScanTimedOut(Duration),
    #[error("Fail to get real time stream start")]
    GetRealTimeStartFailed,
}

impl Error {
    pub fn request(err: ErrorHeader) -> Error {
        Error::Request(Box::new(err))
    }
}

macro_rules! impl_from {
    ($($inner:ty => $container:ident,)+) => {
        $(
            impl From<$inner> for Error {
                fn from(inr: $inner) -> Error {
                    Error::$container(inr.into())
                }
            }
        )+
    };
}

impl_from! {
    String => Rocks,
    TxnTypesError => Mvcc,
}

pub type Result<T> = result::Result<T, Error>;

impl Error {
    pub fn extract_error_header(self) -> ErrorHeader {
        match self {
            Error::Engine(EngineError(box EngineErrorInner::Request(e)))
            | Error::Txn(TxnError(box TxnErrorInner::Engine(EngineError(
                box EngineErrorInner::Request(e),
            ))))
            | Error::Txn(TxnError(box TxnErrorInner::Mvcc(MvccError(
                box MvccErrorInner::Engine(EngineError(box EngineErrorInner::Request(e))),
            )))) => e,
            Error::Request(e) => e.as_ref().clone(),
            other => {
                let mut e = ErrorHeader::default();
                e.set_message(format!("{:?}", other));
                e
            }
        }
    }
}
