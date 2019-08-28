// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

use std::io::Error as IoError;
use std::{error, result};

use kvproto::backup::Error as ErrorPb;
use kvproto::errorpb::Error as RegionError;
use kvproto::kvrpcpb::KeyError;
use tikv::storage::kv::Error as EngineError;
use tikv::storage::mvcc::Error as MvccError;
use tikv::storage::txn::Error as TxnError;

impl Into<ErrorPb> for Error {
    // TODO: test error conversion.
    fn into(self) -> ErrorPb {
        let mut err = ErrorPb::new();
        match self {
            Error::ClusterID { current, request } => {
                err.mut_cluster_id_error().set_current(current);
                err.mut_cluster_id_error().set_request(request);
            }
            Error::Engine(EngineError::Request(e))
            | Error::Txn(TxnError::Engine(EngineError::Request(e)))
            | Error::Txn(TxnError::Mvcc(MvccError::Engine(EngineError::Request(e)))) => {
                err.set_region_error(e);
            }
            Error::Txn(TxnError::Mvcc(MvccError::KeyIsLocked(info))) => {
                let mut e = KeyError::new();
                e.set_locked(info);
                err.set_kv_error(e);
            }
            other => {
                err.set_msg(format!("{:?}", other));
            }
        }
        err
    }
}

/// The error type for backup.
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
    #[fail(display = "ClusterID error current {}, request {}", current, request)]
    ClusterID { current: u64, request: u64 },
}

macro_rules! impl_from {
    ($($inner:ty => $container:ident,)+) => {
        $(
            impl From<$inner> for Error {
                fn from(inr: $inner) -> Error {
                    Error::$container(inr)
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
}

pub type Result<T> = result::Result<T, Error>;
