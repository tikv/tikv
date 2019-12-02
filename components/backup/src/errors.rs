// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

use std::io::Error as IoError;
use std::{error, result};

use engine_traits::Error as EngineTraitError;
use kvproto::backup::Error as ErrorPb;
use kvproto::errorpb::{Error as RegionError, ServerIsBusy};
use kvproto::kvrpcpb::KeyError;
use tikv::storage::kv::{Error as EngineError, ErrorInner as EngineErrorInner};
use tikv::storage::mvcc::{Error as MvccError, ErrorInner as MvccErrorInner};
use tikv::storage::txn::{Error as TxnError, ErrorInner as TxnErrorInner};

use crate::metrics::*;

impl Into<ErrorPb> for Error {
    // TODO: test error conversion.
    fn into(self) -> ErrorPb {
        let mut err = ErrorPb::default();
        match self {
            Error::ClusterID { current, request } => {
                BACKUP_RANGE_ERROR_VEC
                    .with_label_values(&["cluster_mismatch"])
                    .inc();
                err.mut_cluster_id_error().set_current(current);
                err.mut_cluster_id_error().set_request(request);
            }
            Error::Engine(EngineError(box EngineErrorInner::Request(e)))
            | Error::Txn(TxnError(box TxnErrorInner::Engine(EngineError(
                box EngineErrorInner::Request(e),
            ))))
            | Error::Txn(TxnError(box TxnErrorInner::Mvcc(MvccError(
                box MvccErrorInner::Engine(EngineError(box EngineErrorInner::Request(e))),
            )))) => {
                if e.has_not_leader() {
                    BACKUP_RANGE_ERROR_VEC
                        .with_label_values(&["not_leader"])
                        .inc();
                } else if e.has_region_not_found() {
                    BACKUP_RANGE_ERROR_VEC
                        .with_label_values(&["region_not_found"])
                        .inc();
                } else if e.has_key_not_in_region() {
                    BACKUP_RANGE_ERROR_VEC
                        .with_label_values(&["key_not_in_region"])
                        .inc();
                } else if e.has_epoch_not_match() {
                    BACKUP_RANGE_ERROR_VEC
                        .with_label_values(&["epoch_not_match"])
                        .inc();
                } else if e.has_server_is_busy() {
                    BACKUP_RANGE_ERROR_VEC
                        .with_label_values(&["server_is_busy"])
                        .inc();
                } else if e.has_stale_command() {
                    BACKUP_RANGE_ERROR_VEC
                        .with_label_values(&["stale_command"])
                        .inc();
                } else if e.has_store_not_match() {
                    BACKUP_RANGE_ERROR_VEC
                        .with_label_values(&["store_not_match"])
                        .inc();
                }

                err.set_region_error(e);
            }
            Error::Txn(TxnError(box TxnErrorInner::Mvcc(MvccError(
                box MvccErrorInner::KeyIsLocked(info),
            )))) => {
                BACKUP_RANGE_ERROR_VEC
                    .with_label_values(&["key_is_locked"])
                    .inc();
                let mut e = KeyError::default();
                e.set_locked(info);
                err.set_kv_error(e);
            }
            timeout @ Error::Engine(EngineError(box EngineErrorInner::Timeout(_))) => {
                BACKUP_RANGE_ERROR_VEC.with_label_values(&["timeout"]).inc();
                let mut busy = ServerIsBusy::default();
                let reason = format!("{}", timeout);
                busy.set_reason(reason.clone());
                let mut e = RegionError::default();
                e.set_message(reason);
                e.set_server_is_busy(busy);
                err.set_region_error(e);
            }
            other => {
                BACKUP_RANGE_ERROR_VEC.with_label_values(&["other"]).inc();
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
    #[fail(display = "Engine error {}", _0)]
    EngineTrait(EngineTraitError),
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
    EngineTraitError => EngineTrait,
    TxnError => Txn,
}

pub type Result<T> = result::Result<T, Error>;
