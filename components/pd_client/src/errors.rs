// Copyright 2016 TiKV Project Authors. Licensed under Apache-2.0.

use std::{error, result};

use error_code::{self, ErrorCode, ErrorCodeExt};
use thiserror::Error;

#[derive(Debug, Error)]
pub enum Error {
    #[error("{0}")]
    Io(#[from] std::io::Error),
    #[error("cluster {0} is already bootstrapped")]
    ClusterBootstrapped(u64),
    #[error("cluster {0} is not bootstrapped")]
    ClusterNotBootstrapped(u64),
    #[error("feature is not supported in other cluster components")]
    Incompatible,
    #[error("{0}")]
    Grpc(#[from] grpcio::Error),
    #[error("unknown error {0:?}")]
    Other(#[from] Box<dyn error::Error + Sync + Send>),
    #[error("region is not found for key {}", log_wrappers::Value::key(.0))]
    RegionNotFound(Vec<u8>),
    #[error("store is tombstone {0:?}")]
    StoreTombstone(String),
}

pub type Result<T> = result::Result<T, Error>;

impl ErrorCodeExt for Error {
    fn error_code(&self) -> ErrorCode {
        match self {
            Error::Io(_) => error_code::pd::IO,
            Error::ClusterBootstrapped(_) => error_code::pd::CLUSTER_BOOTSTRAPPED,
            Error::ClusterNotBootstrapped(_) => error_code::pd::CLUSTER_NOT_BOOTSTRAPPED,
            Error::Incompatible => error_code::pd::INCOMPATIBLE,
            Error::Grpc(_) => error_code::pd::GRPC,
            Error::RegionNotFound(_) => error_code::pd::REGION_NOT_FOUND,
            Error::StoreTombstone(_) => error_code::pd::STORE_TOMBSTONE,
            Error::Other(_) => error_code::pd::UNKNOWN,
        }
    }
}
