// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

use thiserror::Error;
<<<<<<< HEAD
use tikv::storage::kv::{Error as KvError, ErrorInner as EngineErrorInner};
use tikv::storage::mvcc::{Error as MvccError, ErrorInner as MvccErrorInner};
use tikv::storage::txn::{Error as TxnError, ErrorInner as TxnErrorInner};
use txn_types::Error as TxnTypesError;
=======
>>>>>>> c66bfe87c1 (resolved_ts: re-register region if memory quota exceeded  (#15411))

#[derive(Debug, Error)]
pub enum Error {
    #[error("Memory quota exceeded")]
    MemoryQuotaExceeded,
    #[error("Other error {0}")]
    Other(#[from] Box<dyn std::error::Error + Sync + Send>),
}

pub type Result<T> = std::result::Result<T, Error>;
