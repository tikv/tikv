// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

mod s3;

use async_trait::async_trait;
use bytes::Bytes;
pub use s3::S3FS;
use std::fmt::Debug;
use std::path::{Path, PathBuf};
use std::{io, result, sync::Arc};
use thiserror::Error;
use tokio::runtime::Runtime;

// DFS represents a distributed file system.
#[async_trait]
pub trait DFS: Sync + Send {
    // open opens an existing file with fileID.
    // It may take a long time if the file need to be cached in local disk.
    fn open(&self, file_id: u64, opts: Options) -> Result<Arc<dyn File>>;

    // prefetch fetches the data from remote server to local disk cache for lower read latency.
    async fn prefetch(&self, file_id: u64, opts: Options) -> Result<()>;

    // read_file reads the whole file to memory.
    // It can be used by remote compaction server that doesn't have local disk.
    async fn read_file(&self, file_id: u64, opts: Options) -> Result<Bytes>;

    // Create creates a new File.
    // The shard_id and shard_ver can be used determine where to write the file.
    async fn create(&self, file_id: u64, data: Bytes, opts: Options) -> Result<()>;

    // remove removes the file from the DFS.
    async fn remove(&self, file_id: u64, opts: Options);

    // get_runtime gets the tokio runtime for the DFS.
    fn get_runtime(&self) -> &tokio::runtime::Runtime;

    fn local_dir(&self) -> &Path;

    fn tenant_id(&self) -> u32;
}

pub trait File: Sync + Send {
    // id returns the id of the file.
    fn id(&self) -> u64;

    // size returns the size of the file.
    fn size(&self) -> u64;

    // read reads the data at given offset.
    fn read(&self, off: u64, length: usize) -> Result<Bytes>;
}

pub struct InMemFS {
    files: dashmap::DashMap<u64, Arc<InMemFile>>,
    local_dir: PathBuf,
    runtime: tokio::runtime::Runtime,
}

impl InMemFS {
    pub fn new(local_dir: PathBuf) -> Self {
        Self {
            files: dashmap::DashMap::new(),
            local_dir,
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
    fn open(&self, file_id: u64, _opts: Options) -> Result<Arc<dyn File>> {
        if let Some(file) = self.files.get(&file_id).as_deref() {
            return Ok(file.clone());
        }
        Err(Error::NotExists(file_id))
    }

    async fn prefetch(&self, file_id: u64, _opts: Options) -> Result<()> {
        if self.files.contains_key(&file_id) {
            return Ok(());
        }
        return Err(Error::NotExists(file_id));
    }

    async fn read_file(&self, file_id: u64, _opts: Options) -> Result<Bytes> {
        if let Some(file) = self.files.get(&file_id).as_deref() {
            return Ok(file.data.slice(..));
        }
        return Err(Error::NotExists(file_id));
    }

    async fn create(&self, file_id: u64, data: Bytes, _opts: Options) -> Result<()> {
        let file = InMemFile::new(file_id, data);
        self.files.insert(file_id, Arc::new(file));
        Ok(())
    }

    async fn remove(&self, file_id: u64, _opts: Options) {
        self.files.remove(&file_id);
    }

    fn get_runtime(&self) -> &Runtime {
        &self.runtime
    }

    fn local_dir(&self) -> &Path {
        &self.local_dir
    }

    fn tenant_id(&self) -> u32 {
        return 0;
    }
}

#[derive(Clone)]
pub struct InMemFile {
    pub id: u64,
    data: Bytes,
    pub size: u64,
}

impl InMemFile {
    pub fn new(id: u64, data: Bytes) -> Self {
        let size = data.len() as u64;
        Self { id, data, size }
    }
}

impl File for InMemFile {
    fn id(&self) -> u64 {
        self.id
    }

    fn size(&self) -> u64 {
        self.size
    }

    fn read(&self, off: u64, length: usize) -> Result<Bytes> {
        let off_usize = off as usize;
        Ok(self.data.slice(off_usize..off_usize + length))
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

pub fn new_filename(file_id: u64) -> PathBuf {
    PathBuf::from(format!("{:016x}.sst", file_id))
}

pub fn new_tmp_filename(file_id: u64, tmp_id: u64) -> PathBuf {
    PathBuf::from(format!("{:016x}.{}.tmp", file_id, tmp_id))
}
