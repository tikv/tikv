// Copyright 2016 TiKV Project Authors. Licensed under Apache-2.0.

use std::error::Error as StdError;
use std::result::Result as StdResult;

use thiserror::Error;

use error_code::{self, ErrorCode, ErrorCodeExt};

#[derive(Debug, Error)]
pub enum Error {
    #[error("{0}")]
    Other(#[from] Box<dyn StdError + Sync + Send>),
}

pub type Result<T> = StdResult<T, Error>;

impl ErrorCodeExt for Error {
    fn error_code(&self) -> ErrorCode {
        error_code::raftstore::COPROCESSOR
    }
}
