// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

use thiserror::Error;
use tikv_util::memory::MemoryQuotaExceeded;

#[derive(Debug, Error)]
pub enum Error {
    #[error("Memory quota exceeded")]
    MemoryQuotaExceeded(#[from] MemoryQuotaExceeded),
    #[error("Other error {0}")]
    Other(#[from] Box<dyn std::error::Error + Sync + Send>),
}

pub type Result<T> = std::result::Result<T, Error>;
