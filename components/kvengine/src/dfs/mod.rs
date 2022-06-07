// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

mod s3;

use std::{fmt::Debug, io, result};

use async_trait::async_trait;
use bytes::Bytes;
pub use s3::S3FS;
use thiserror::Error;
use tokio::runtime::Runtime;

// DFS represents a distributed file system.
#[async_trait]
pub trait DFS: Sync + Send {
    /// read_file reads the whole file to memory.
    /// It can be used by remote compaction server that doesn't have local disk.
    async fn read_file(&self, file_id: u64, opts: Options) -> Result<Bytes>;

    /// Create creates a new File.
    /// The shard_id and shard_ver can be used determine where to write the file.
    async fn create(&self, file_id: u64, data: Bytes, opts: Options) -> Result<()>;

    /// remove removes the file from the DFS.
    async fn remove(&self, file_id: u64, opts: Options);

    /// get_runtime gets the tokio runtime for the DFS.
    fn get_runtime(&self) -> &tokio::runtime::Runtime;

    fn tenant_id(&self) -> u32;
}

pub struct InMemFS {
    files: dashmap::DashMap<u64, Bytes>,
    runtime: tokio::runtime::Runtime,
}

impl Default for InMemFS {
    fn default() -> Self {
        Self::new()
    }
}

impl InMemFS {
    pub fn new() -> Self {
        Self {
            files: dashmap::DashMap::new(),
            runtime: tokio::runtime::Builder::new_multi_thread()
                .worker_threads(1)
                .enable_all()
                .build()
                .unwrap(),
        }
    }
}

#[async_trait]
impl DFS for InMemFS {
    async fn read_file(&self, file_id: u64, _opts: Options) -> Result<Bytes> {
        if let Some(file) = self.files.get(&file_id).as_deref() {
            return Ok(file.clone());
        }
        Err(Error::NotExists(file_id))
    }

    async fn create(&self, file_id: u64, data: Bytes, _opts: Options) -> Result<()> {
        self.files.insert(file_id, data);
        Ok(())
    }

    async fn remove(&self, file_id: u64, _opts: Options) {
        self.files.remove(&file_id);
    }

    fn get_runtime(&self) -> &Runtime {
        &self.runtime
    }

    fn tenant_id(&self) -> u32 {
        0
    }
}

#[derive(Clone, Copy)]
pub struct Options {
    pub shard_id: u64,
    pub shard_ver: u64,
}

impl Options {
    pub fn new(shard_id: u64, shard_ver: u64) -> Self {
        Self {
            shard_id,
            shard_ver,
        }
    }
}

pub type Result<T> = result::Result<T, Error>;

#[derive(Debug, Error, Clone)]
pub enum Error {
    #[error("IO error: {0}")]
    Io(String),
    #[error("File {0} not exists")]
    NotExists(u64),
    #[error("S3 error {0}")]
    S3(String),
}

impl From<io::Error> for Error {
    #[inline]
    fn from(e: io::Error) -> Error {
        Error::Io(e.to_string())
    }
}

impl<E: Debug> From<rusoto_core::RusotoError<E>> for Error {
    fn from(err: rusoto_core::RusotoError<E>) -> Self {
        Error::S3(format!("{:?}", err))
    }
}
