// Copyright 2020 TiKV Project Authors. Licensed under Apache-2.0.

use std::io::Error as IoError;
use std::{error, result};

use engine_traits::Error as EngineTraitsError;
use kvproto::errorpb;
use tikv::storage::kv::{Error as EngineError, ErrorInner as EngineErrorInner};
use tikv::storage::mvcc::{Error as MvccError, ErrorInner as MvccErrorInner};
use tikv::storage::txn::{Error as TxnError, ErrorInner as TxnErrorInner};
use txn_types::Error as TxnTypesError;

use crate::channel::SendError;

/// The error type for cdc.
#[derive(Debug, Fail)]
pub enum Error {
    #[fail(display = "Other error {}", _0)]
    Other(Box<dyn error::Error + Sync + Send>),
    #[fail(display = "RocksDB error {}", _0)]
    Rocks(String),
    #[fail(display = "IO error {}", _0)]
    Io(IoError),
    #[fail(display = "Engine error {}", _0)]
    Engine(EngineError),
    #[fail(display = "Transaction error {}", _0)]
    Txn(TxnError),
    #[fail(display = "Mvcc error {}", _0)]
    Mvcc(MvccError),
    #[fail(display = "Request error {:?}", _0)]
    Request(errorpb::Error),
    #[fail(display = "Engine traits error {}", _0)]
    EngineTraits(EngineTraitsError),
    #[fail(display = "Resolver Builder has disconnected")]
    ResolverBuilderConnExited,
    #[fail(display = "Sink send error {:?}", _0)]
    Sink(SendError),
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
    Box<dyn error::Error + Sync + Send> => Other,
    String => Rocks,
    IoError => Io,
    EngineError => Engine,
    TxnError => Txn,
    MvccError => Mvcc,
    TxnTypesError => Mvcc,
    EngineTraitsError => EngineTraits,
    SendError => Sink,
}

pub type Result<T> = result::Result<T, Error>;

impl Error {
    pub fn has_region_error(&self) -> bool {
        matches!(
            self,
            Error::Engine(EngineError(box EngineErrorInner::Request(_)))
                | Error::Txn(TxnError(box TxnErrorInner::Engine(EngineError(
                    box EngineErrorInner::Request(_),
                ))))
                | Error::Txn(TxnError(box TxnErrorInner::Mvcc(MvccError(
                    box MvccErrorInner::Engine(EngineError(box EngineErrorInner::Request(_))),
                ))))
                | Error::Request(_)
        )
    }

    pub fn extract_region_error(self) -> errorpb::Error {
        match self {
            Error::Engine(EngineError(box EngineErrorInner::Request(e)))
            | Error::Txn(TxnError(box TxnErrorInner::Engine(EngineError(
                box EngineErrorInner::Request(e),
            ))))
            | Error::Txn(TxnError(box TxnErrorInner::Mvcc(MvccError(
                box MvccErrorInner::Engine(EngineError(box EngineErrorInner::Request(e))),
            ))))
            | Error::Request(e) => e,
            // TODO: it should be None, add more cdc errors.
            other => {
                let mut e = errorpb::Error::default();
                e.set_message(format!("{:?}", other));
                e
            }
        }
    }
}
