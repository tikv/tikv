// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.
use etcd_client::Error as EtcdError;
use pd_client::Error as PdError;
use protobuf::ProtobufError;
use slog_global::error;
use std::error::Error as StdError;
use std::fmt::Display;
use std::io::Error as IoError;
use std::result::Result as StdResult;
use thiserror::Error as ThisError;
use tikv::storage::txn::Error as TxnError;
use tikv_util::worker::ScheduleError;

use crate::endpoint::Task;
#[cfg(not(test))]
use crate::metrics;

#[derive(ThisError, Debug)]
pub enum Error {
    #[error("Etcd meet error {0}")]
    Etcd(#[from] EtcdError),
    #[error("Protobuf meet error {0}")]
    Protobuf(#[from] ProtobufError),
    #[error("No such task {task_name:?}")]
    NoSuchTask { task_name: String },
    #[error("Malformed metadata {0}")]
    MalformedMetadata(String),
    #[error("I/O Error: {0}")]
    Io(#[from] IoError),
    #[error("Txn error: {0}")]
    Txn(#[from] TxnError),
    #[error("TiKV scheduler error: {0}")]
    Sched(#[from] ScheduleError<Task>),
    #[error("PD client meet error: {0}")]
    Pd(#[from] PdError),
    #[error("Other Error: {0}")]
    Other(#[from] Box<dyn StdError + Send + Sync + 'static>),
}

pub type Result<T> = StdResult<T, Error>;

/// Like `errors.Annotate` in Go.
/// Wrap an unknown error with [`Error::Other`].
#[macro_export(crate)]
macro_rules! annotate {
    ($inner: expr, $message: expr) => {
        Error::Other(tikv_util::box_err!("{}: {}", $message, $inner))
    };
    ($inner: expr, $format: literal, $($args: expr),+) => {
        annotate!($inner, format_args!($format, $($args),+))
    }
}

impl Error {
    pub fn report(&self, context: impl Display) {
        #[cfg(test)]
        println!(
            "backup stream meet error (context = {}, err = {})",
            context, self
        );
        #[cfg(not(test))]
        {
            // TODO: adapt the error_code module, use `tikv_util::error!` to replace this.
            error!("backup stream meet error"; "context" => %context, "err" => %self);
            metrics::STREAM_ERROR
                .with_label_values(&[self.kind()])
                .inc()
        }
    }

    #[cfg(not(test))]
    fn kind(&self) -> &'static str {
        match self {
            Error::Etcd(_) => "etcd",
            Error::Protobuf(_) => "protobuf",
            Error::NoSuchTask { .. } => "no_such_task",
            Error::MalformedMetadata(_) => "malformed_metadata",
            Error::Io(_) => "io",
            Error::Txn(_) => "transaction",
            Error::Other(_) => "unknown",
            Error::Sched(_) => "schedule",
            Error::Pd(_) => "pd",
        }
    }
}
