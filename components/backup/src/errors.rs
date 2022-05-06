// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

use std::{error, io::Error as IoError, result};

use engine_traits::Error as EngineTraitError;
use kvproto::{
    brpb::Error as ErrorPb,
    errorpb::{Error as RegionError, ServerIsBusy},
    kvrpcpb::KeyError,
};
use thiserror::Error;
use tikv::storage::{
    kv::{Error as KvError, ErrorInner as EngineErrorInner},
    mvcc::{Error as MvccError, ErrorInner as MvccErrorInner},
    txn::{Error as TxnError, ErrorInner as TxnErrorInner},
};
use tikv_util::codec::Error as CodecError;
use tokio::sync::AcquireError;

use crate::metrics::*;

impl From<Error> for ErrorPb {
    // TODO: test error conversion.
    fn from(e: Error) -> ErrorPb {
        let mut err = ErrorPb::default();
        match e {
            Error::ClusterID { current, request } => {
                BACKUP_RANGE_ERROR_VEC
                    .with_label_values(&["cluster_mismatch"])
                    .inc();
                err.mut_cluster_id_error().set_current(current);
                err.mut_cluster_id_error().set_request(request);
            }
            Error::Kv(KvError(box EngineErrorInner::Request(e)))
            | Error::Txn(TxnError(box TxnErrorInner::Engine(KvError(
                box EngineErrorInner::Request(e),
            ))))
            | Error::Txn(TxnError(box TxnErrorInner::Mvcc(MvccError(box MvccErrorInner::Kv(
                KvError(box EngineErrorInner::Request(e)),
            ))))) => {
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
            timeout @ Error::Kv(KvError(box EngineErrorInner::Timeout(_))) => {
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
    #[error("Engine error {0}")]
    EngineTrait(#[from] EngineTraitError),
    #[error("Transaction error {0}")]
    Txn(#[from] TxnError),
    #[error("ClusterID error current {current}, request {request}")]
    ClusterID { current: u64, request: u64 },
    #[error("Invalid cf {cf}")]
    InvalidCf { cf: String },
    #[error("Failed to acquire the semaphore {0}")]
    Semaphore(#[from] AcquireError),
    #[error("Channel is closed")]
    ChannelClosed,
    #[error("Codec error {0}")]
    Codec(#[from] CodecError),
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
    String => Rocks,
}

impl<T> From<async_channel::SendError<T>> for Error {
    fn from(_: async_channel::SendError<T>) -> Self {
        Self::ChannelClosed
    }
}

pub type Result<T> = result::Result<T, Error>;
