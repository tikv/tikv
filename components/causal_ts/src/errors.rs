// Copyright 2022 TiKV Project Authors. Licensed under Apache-2.0.

use std::{error, sync::Arc};

use error_code::{self, ErrorCode, ErrorCodeExt};
use thiserror::Error;

#[derive(Debug, Error)]
pub enum Error {
    #[error("Pd {0}")]
    Pd(#[from] pd_client::Error),
    #[error("TSO {0}")]
    Tso(String),
    #[error("TSO batch({0}) used up")]
    TsoBatchUsedUp(u32),
    #[error("Batch renew error {0:?}")]
    BatchRenew(Arc<dyn error::Error + Sync + Send>),
    #[error("unknown error {0:?}")]
    Other(#[from] Box<dyn error::Error + Sync + Send>),
}

pub type Result<T> = std::result::Result<T, Error>;

impl ErrorCodeExt for Error {
    fn error_code(&self) -> ErrorCode {
        match self {
            Error::Pd(_) => error_code::causal_ts::PD,
            Error::Tso(_) => error_code::causal_ts::TSO,
            Error::TsoBatchUsedUp(_) => error_code::causal_ts::TSO_BATCH_USED_UP,
            Error::BatchRenew(_) => error_code::causal_ts::BATCH_RENEW,
            Error::Other(_) => error_code::UNKNOWN,
        }
    }
}
