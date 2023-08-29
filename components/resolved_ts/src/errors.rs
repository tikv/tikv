// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

use thiserror::Error;

#[derive(Debug, Error)]
pub enum Error {
    #[error("Memory quota exceeded")]
    MemoryQuotaExceeded,
    #[error("Other error {0}")]
    Other(#[from] Box<dyn std::error::Error + Sync + Send>),
}

pub type Result<T> = std::result::Result<T, Error>;
