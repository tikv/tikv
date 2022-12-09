// Copyright 2020 TiKV Project Authors. Licensed under Apache-2.0.

use std::{error, io::Error as IoError, result};

use engine_traits::Error as EngineTraitsError;
use kvproto::{cdcpb::Error as ErrorEvent, errorpb};
use thiserror::Error;
use tikv::storage::{
    kv::{Error as KvError, ErrorInner as EngineErrorInner},
    mvcc::{Error as MvccError, ErrorInner as MvccErrorInner},
    txn::{Error as TxnError, ErrorInner as TxnErrorInner},
};
use txn_types::Error as TxnTypesError;

use crate::channel::SendError;

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
    Kv(#[from] KvError),
    #[error("Transaction error {0}")]
    Txn(#[from] TxnError),
    #[error("Mvcc error {0}")]
    Mvcc(#[from] MvccError),
    #[error("Request error {0:?}")]
    Request(Box<errorpb::Error>),
    #[error("Engine traits error {0}")]
    EngineTraits(#[from] EngineTraitsError),
    #[error("Sink send error {0:?}")]
    Sink(#[from] SendError),
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
    pub fn request(err: errorpb::Error) -> Error {
        Error::Request(Box::new(err))
    }

    pub fn has_region_error(&self) -> bool {
        matches!(
            self,
            Error::Kv(KvError(box EngineErrorInner::Request(_)))
                | Error::Txn(TxnError(box TxnErrorInner::Engine(KvError(
                    box EngineErrorInner::Request(_),
                ))))
                | Error::Txn(TxnError(box TxnErrorInner::Mvcc(MvccError(
                    box MvccErrorInner::Kv(KvError(box EngineErrorInner::Request(_))),
                ))))
                | Error::Request(_)
        )
    }

    pub fn extract_region_error(self) -> errorpb::Error {
        match self {
            Error::Kv(KvError(box EngineErrorInner::Request(e)))
            | Error::Txn(TxnError(box TxnErrorInner::Engine(KvError(
                box EngineErrorInner::Request(e),
            ))))
            | Error::Txn(TxnError(box TxnErrorInner::Mvcc(MvccError(box MvccErrorInner::Kv(
                KvError(box EngineErrorInner::Request(e)),
            )))))
            | Error::Request(box e) => e,
            // TODO: it should be None, add more cdc errors.
            other => {
                let mut e = errorpb::Error::default();
                e.set_message(format!("{:?}", other));
                e
            }
        }
    }

    pub fn into_error_event(self, region_id: u64) -> ErrorEvent {
        let mut err_event = ErrorEvent::default();
        let mut err = self.extract_region_error();
        if err.has_not_leader() {
            let not_leader = err.take_not_leader();
            err_event.set_not_leader(not_leader);
        } else if err.has_epoch_not_match() {
            let epoch_not_match = err.take_epoch_not_match();
            err_event.set_epoch_not_match(epoch_not_match);
        } else {
            // TODO: Add more errors to the cdc protocol
            let mut region_not_found = errorpb::RegionNotFound::default();
            region_not_found.set_region_id(region_id);
            err_event.set_region_not_found(region_not_found);
        }
        err_event
    }
}
