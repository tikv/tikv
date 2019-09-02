// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

#![recursion_limit = "200"]

#[macro_use(slog_error, slog_warn)]
extern crate slog;
#[macro_use]
extern crate slog_global;
#[macro_use]
extern crate prometheus;
#[macro_use]
extern crate lazy_static;
#[macro_use]
extern crate quick_error;
#[macro_use]
extern crate serde_derive;
#[allow(unused_extern_crates)]
extern crate tikv_alloc;

use std::fmt::Debug;
use std::sync::Arc;

pub mod util;

pub mod rocks;
pub use crate::rocks::{
    CFHandle, DBIterator, DBVector, Range, ReadOptions, Snapshot, SyncSnapshot, WriteBatch,
    WriteOptions, DB,
};
mod errors;
pub use crate::errors::*;
mod peekable;
pub use crate::peekable::*;
mod iterable;
pub use crate::iterable::*;
mod mutable;
pub use crate::mutable::*;
mod cf;
pub use crate::cf::*;

pub const DATA_KEY_PREFIX_LEN: usize = 1;

// This is for the DB and write batches to share the same API
trait Writable {
    fn put(&self, key: &[u8], value: &[u8]) -> Result<()>;
    fn put_cf(&self, cf: &CFHandle, key: &[u8], value: &[u8]) -> Result<()>;
    fn merge(&self, key: &[u8], value: &[u8]) -> Result<()>;
    fn merge_cf(&self, cf: &CFHandle, key: &[u8], value: &[u8]) -> Result<()>;
    fn delete(&self, key: &[u8]) -> Result<()>;
    fn delete_cf(&self, cf: &CFHandle, key: &[u8]) -> Result<()>;
    fn single_delete(&self, key: &[u8]) -> Result<()>;
    fn single_delete_cf(&self, cf: &CFHandle, key: &[u8]) -> Result<()>;
    fn delete_range(&self, begin_key: &[u8], end_key: &[u8]) -> Result<()>;
    fn delete_range_cf(
        &self,
        cf: &CFHandle,
        begin_key: &[u8],
        end_key: &[u8],
    ) -> Result<()>;
}

/// Key-Value Store engine.
pub trait KvEngine: std::fmt::Debug + Clone + Sync + Send {
    fn write(&self, batch: &WriteBatch) -> Result<()>;

    fn write_opt(&self, batch: &WriteBatch, writeopts: &WriteOptions) -> Result<()>;

    fn sync_wal(&self) -> Result<()>;
}

pub trait Engines: Clone + Debug + Send + Sync + 'static {
    fn shared_block_cache(&self) -> bool;

    fn write_kv(&self, wb: &WriteBatch) -> Result<()>;

    fn write_kv_opt(&self, wb: &WriteBatch, opts: &WriteOptions) -> Result<()>;

    fn sync_kv(&self) -> Result<()>;

    fn write_raft(&self, wb: &WriteBatch) -> Result<()>;

    fn write_raft_opt(&self, wb: &WriteBatch, opts: &WriteOptions) -> Result<()>;

    fn sync_raft(&self) -> Result<()>;

    fn kv(&self) -> &Arc<DB>;

    fn raft(&self) -> &Arc<DB>;
}
