// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

mod s3;

use std::{
    fmt::Debug,
    io,
    io::{BufReader, Read, Write},
    ops::Deref,
    path::{Path, PathBuf},
    result,
    sync::{atomic::AtomicU64, Arc},
};

use async_trait::async_trait;
use bytes::Bytes;
use file_system;
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
            files: Default::default(),
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
}

#[derive(Clone)]
pub struct LocalFS {
    core: Arc<LocalFSCore>,
}

impl LocalFS {
    pub fn new(dir: &Path) -> Self {
        let core = Arc::new(LocalFSCore::new(dir));
        Self { core }
    }
    pub fn local_file_path(&self, file_id: u64) -> PathBuf {
        self.dir.join(self.filename(file_id))
    }
    pub fn filename(&self, file_id: u64) -> PathBuf {
        PathBuf::from(format!("{:016x}.sst", file_id))
    }
    pub fn tmp_file_path(&self, file_id: u64) -> PathBuf {
        let tmp_id = self
            .tmp_file_id
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        self.dir.join(self.new_tmp_filename(file_id, tmp_id))
    }
    pub fn new_tmp_filename(&self, file_id: u64, tmp_id: u64) -> PathBuf {
        PathBuf::from(format!("{:016x}.{}.tmp", file_id, tmp_id))
    }
}

impl Deref for LocalFS {
    type Target = LocalFSCore;

    fn deref(&self) -> &Self::Target {
        &self.core
    }
}

pub struct LocalFSCore {
    dir: PathBuf,
    tmp_file_id: AtomicU64,
    runtime: tokio::runtime::Runtime,
}

impl LocalFSCore {
    pub fn new(dir: &Path) -> Self {
        if !dir.exists() {
            std::fs::create_dir_all(dir).unwrap();
        }
        if !dir.is_dir() {
            panic!("path {:?} is not dir", dir);
        }
        Self {
            dir: dir.to_owned(),
            tmp_file_id: AtomicU64::new(0),
            runtime: tokio::runtime::Builder::new_multi_thread()
                .worker_threads(8)
                .enable_all()
                .build()
                .unwrap(),
        }
    }
}

#[async_trait]
impl DFS for LocalFS {
    async fn read_file(&self, file_id: u64, _opts: Options) -> Result<Bytes> {
        let local_file_name = self.local_file_path(file_id);
        let fd = std::fs::File::open(local_file_name)?;
        let mut reader = BufReader::new(fd);
        let mut buf = Vec::new();
        reader.read_to_end(&mut buf)?;
        Ok(Bytes::from(buf))
    }

    async fn create(&self, file_id: u64, data: Bytes, _opts: Options) -> Result<()> {
        let local_file_name = self.local_file_path(file_id);
        let tmp_file_name = self.tmp_file_path(file_id);
        let mut file = std::fs::File::create(&tmp_file_name)?;
        let mut start_off = 0;
        let write_batch_size = 256 * 1024;
        while start_off < data.len() {
            let end_off = std::cmp::min(start_off + write_batch_size, data.len());
            file.write_all(&data[start_off..end_off])?;
            file.sync_data()?;
            start_off = end_off;
        }
        std::fs::rename(&tmp_file_name, &local_file_name)?;
        file_system::sync_dir(&self.dir)?;
        Ok(())
    }

    async fn remove(&self, file_id: u64, _opts: Options) {
        let local_file_path = self.local_file_path(file_id);
        if let Err(err) = std::fs::remove_file(&local_file_path) {
            error!("failed to remove local file {:?}", err);
        }
    }

    fn get_runtime(&self) -> &Runtime {
        &self.runtime
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

#[cfg(test)]
mod tests {
    use std::os::unix::fs::MetadataExt;

    use super::*;
    use crate::dfs::LocalFS;

    #[test]
    fn test_local_fs() {
        crate::tests::init_logger();

        let local_dir = tempfile::tempdir().unwrap();
        let file_data = "abcdefgh".to_string().into_bytes();
        let localfs = LocalFS::new(local_dir.path());
        let (tx, rx) = tikv_util::mpsc::bounded(1);
        let file_id = 321u64;
        let fs = localfs.clone();
        let file_data_clone = file_data.clone();
        let f = async move {
            match fs
                .create(
                    file_id,
                    bytes::Bytes::from(file_data_clone),
                    Options::new(1, 1),
                )
                .await
            {
                Ok(_) => {
                    tx.send(true).unwrap();
                    println!("create ok");
                }
                Err(err) => {
                    tx.send(false).unwrap();
                    println!("create error {:?}", err)
                }
            }
        };
        localfs.runtime.spawn(f);
        assert!(rx.recv().unwrap());
        let fs = localfs.clone();
        let (tx, rx) = tikv_util::mpsc::bounded(1);
        let f = async move {
            let opts = Options::new(1, 1);
            match fs.read_file(file_id, opts).await {
                Ok(data) => {
                    assert_eq!(&data, &file_data);
                    tx.send(true).unwrap();
                    println!("prefetch ok");
                }
                Err(err) => {
                    tx.send(false).unwrap();
                    println!("prefetch failed {:?}", err)
                }
            }
        };
        localfs.runtime.spawn(f);
        assert!(rx.recv().unwrap());
        let local_file = localfs.local_file_path(file_id);
        let fd = std::fs::File::open(&local_file).unwrap();
        let meta = fd.metadata().unwrap();
        assert_eq!(meta.size(), 8u64);
        let fs = localfs.clone();
        let (tx, rx) = tikv_util::mpsc::bounded(1);
        let f = async move {
            fs.remove(file_id, Options::new(1, 1)).await;
            tx.send(true).unwrap();
        };
        localfs.runtime.spawn(f);
        assert!(rx.recv().unwrap());
        assert!(std::fs::File::open(&local_file).is_err())
    }
}
