// Copyright 2016 TiKV Project Authors. Licensed under Apache-2.0.

use std::{error, result};

use error_code::{self, ErrorCode, ErrorCodeExt};
use futures::channel::mpsc::SendError;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum Error {
    #[error("cluster {0} is already bootstrapped")]
    ClusterBootstrapped(u64),
    #[error("cluster {0} is not bootstrapped")]
    ClusterNotBootstrapped(u64),
    #[error("feature is not supported in other cluster components")]
    Incompatible,
    #[error("{0}")]
    Grpc(#[from] grpcio::Error),
    #[error("{0}")]
    StreamDisconnect(#[from] SendError),
    #[error("unknown error {0:?}")]
    Other(#[from] Box<dyn error::Error + Sync + Send>),
    #[error("region is not found for key {}", log_wrappers::Value::key(.0))]
    RegionNotFound(Vec<u8>),
    #[error("store is tombstone {0:?}")]
    StoreTombstone(String),
    #[error("required watch revision is smaller than current compact/min revision. {0:?}")]
    DataCompacted(String),
    #[error(
        "the requested service gc safe point({requested}) isn't safe(safe point now is {current_minimal})"
    )]
    UnsafeServiceGcSafePoint {
        requested: txn_types::TimeStamp,
        current_minimal: txn_types::TimeStamp,
    },
}

pub type Result<T> = result::Result<T, Error>;

impl Error {
    pub fn retryable(&self) -> bool {
        match self {
            Error::Grpc(_)
            | Error::ClusterNotBootstrapped(_)
            | Error::StreamDisconnect(_)
            | Error::DataCompacted(_) => true,
            Error::Other(_)
            | Error::RegionNotFound(_)
            | Error::StoreTombstone(_)
            | Error::ClusterBootstrapped(_)
            | Error::Incompatible => false,
            Error::UnsafeServiceGcSafePoint { .. } => false,
        }
    }
}

impl ErrorCodeExt for Error {
    fn error_code(&self) -> ErrorCode {
        match self {
            Error::ClusterBootstrapped(_) => error_code::pd::CLUSTER_BOOTSTRAPPED,
            Error::ClusterNotBootstrapped(_) => error_code::pd::CLUSTER_NOT_BOOTSTRAPPED,
            Error::Incompatible => error_code::pd::INCOMPATIBLE,
            Error::Grpc(_) => error_code::pd::GRPC,
            Error::StreamDisconnect(_) => error_code::pd::STREAM_DISCONNECT,
            Error::RegionNotFound(_) => error_code::pd::REGION_NOT_FOUND,
            Error::StoreTombstone(_) => error_code::pd::STORE_TOMBSTONE,
            Error::DataCompacted(_) => error_code::pd::DATA_COMPACTED,
            Error::UnsafeServiceGcSafePoint { .. } => error_code::pd::STALE_SERVICE_GC_SAFE_POINT,
            Error::Other(_) => error_code::pd::UNKNOWN,
        }
    }
}
