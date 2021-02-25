// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

use std::io::Error as IoError;

use engine_traits::Error as EngineTraitsError;
use kvproto::errorpb::Error as ErrorHeader;
use raftstore::Error as RaftstoreError;
use tikv::storage::kv::{Error as EngineError, ErrorInner as EngineErrorInner};
use tikv::storage::mvcc::{Error as MvccError, ErrorInner as MvccErrorInner};
use tikv::storage::txn::{Error as TxnError, ErrorInner as TxnErrorInner};
use txn_types::Error as TxnTypesError;

#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("IO error {0}")]
    Io(#[from] IoError),
    #[error("Engine error {0}")]
    Engine(#[from] EngineError),
    #[error("Transaction error {0}")]
    Txn(#[from] TxnError),
    #[error("Mvcc error {0}")]
    Mvcc(#[from] MvccError),
    #[error("Request error {0:?}")]
    Request(ErrorHeader),
    #[error("Engine traits error {0}")]
    EngineTraits(#[from] EngineTraitsError),
    #[error("Txn types error {0}")]
    TxnTypes(#[from] TxnTypesError),
    #[error("Raftstore error {0}")]
    Raftstore(#[from] RaftstoreError),
    #[error("Other error {0}")]
    Other(#[from] Box<dyn std::error::Error + Sync + Send>),
}

impl Error {
    pub fn extract_error_header(self) -> ErrorHeader {
        match self {
            Error::Engine(EngineError(box EngineErrorInner::Request(e)))
            | Error::Txn(TxnError(box TxnErrorInner::Engine(EngineError(
                box EngineErrorInner::Request(e),
            ))))
            | Error::Txn(TxnError(box TxnErrorInner::Mvcc(MvccError(
                box MvccErrorInner::Engine(EngineError(box EngineErrorInner::Request(e))),
            ))))
            | Error::Request(e) => e,
            other => {
                let mut e = ErrorHeader::default();
                e.set_message(format!("{:?}", other));
                e
            }
        }
    }
}

pub type Result<T> = std::result::Result<T, Error>;
