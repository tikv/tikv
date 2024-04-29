// Copyright 2016 TiKV Project Authors. Licensed under Apache-2.0.

use std::{error::Error as StdError, result::Result as StdResult, time::Duration};

use error_code::{self, ErrorCode, ErrorCodeExt};
use thiserror::Error;

#[derive(Debug, Error)]
pub enum Error {
    #[error("required retry after {after:?}, hint: {reason:?}")]
    RequireDelay { after: Duration, reason: String },
    #[error("{0}")]
    Other(#[from] Box<dyn StdError + Sync + Send>),
}

pub type Result<T> = StdResult<T, Error>;

impl ErrorCodeExt for Error {
    fn error_code(&self) -> ErrorCode {
        error_code::raftstore::COPROCESSOR
    }
}
