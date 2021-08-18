use std::{io, result, sync::Arc};
use bytes::{Bytes};
use futures::executor::ThreadPool;
use thiserror::Error;
use async_trait::async_trait;

// DFS represents a distributed file system.
#[async_trait]
pub trait DFS: Sync + Send {
    // open opens an existing file with fileID.
	// It may take a long time if the file need to be cached in local disk.
    async fn open(&self, file_id: u64, opts: Options) -> Result<Arc<dyn File>>;

    // prefetch fetches the data from remote server to local disk cache for lower read latency.
    async fn prefetch(&self, file_id: u64, opts: Options) -> Result<()>;

    // read_file reads the whole file to memory.
	// It can be used by remote compaction server that doesn't have local disk.
    async fn read_file(&self, file_id: u64, opts: Options) -> Result<Bytes>;

    // Create creates a new File.
	// The shard_id and shard_ver can be used determine where to write the file.
    async fn create(&self, file_id: u64, data: Bytes, opts: Options) -> Result<()>;

    // remove removes the file from the DFS.
    async fn remove(&self, file_id: u64, opts: Options) -> Result<()>;

    fn get_future_pool(&self) -> ThreadPool;
}

pub trait File: Sync + Send {
    // id returns the id of the file.
    fn id(&self) -> u64;

    // size returns the size of the file.
    fn size(&self) -> u64;

    // read reads the data at given offset.
    fn read(&self, off: u64, length: usize) -> Result<Bytes>;

    // close releases the local resource like on-disk cache or memory of the file.
	// It should be called when a file will not be used locally but may be used by other nodes.
	// For example when a peer is moved out of the store.
	// Should not be called on process exit.
    fn close(&self) -> Result<()>;
}

pub struct InMemFS {
    files: dashmap::DashMap<u64, Arc<InMemFile>>,
    pool: ThreadPool,
}

impl InMemFS {
    pub fn new() -> Self {
        Self {
            files: dashmap::DashMap::new(),
            pool: ThreadPool::builder().pool_size(1).create().unwrap(),
        }
    }
}

#[async_trait]
impl DFS for InMemFS {
    async fn open(&self, file_id: u64, opts: Options) -> Result<Arc<dyn File>> {
        if let Some(file) = self.files.get(&file_id).as_deref() {
            return Ok(file.clone());
        }
        Err(Error::NotExists(file_id))
    }

    async fn prefetch(&self, file_id: u64, opts: Options) -> Result<()> {
        if self.files.contains_key(&file_id) {
            return Ok(())
        }
        return Err(Error::NotExists(file_id))
    }

    async fn read_file(&self, file_id: u64, opts: Options) -> Result<Bytes> {
        if let Some(file) = self.files.get(&file_id).as_deref() {
            return Ok(file.data.slice(..))
        }
        return Err(Error::NotExists(file_id))
    }

    async fn create(&self, file_id: u64, data: Bytes, opts: Options) -> Result<()> {
        let file = InMemFile::new(file_id, data);
        self.files.insert(file_id, Arc::new(file));
        Ok(())
    }

    async fn remove(&self, file_id: u64, opts: Options) -> Result<()> {
        self.files.remove(&file_id);
        Ok(())
    }

    fn get_future_pool(&self) -> ThreadPool {
        return self.pool.clone()
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
        Self {
            id: id,
            data: data,
            size: size,
        }
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
        Ok(self.data.slice(off_usize..off_usize+length))
    }

    fn close(&self) -> Result<()> {
        Ok(())
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
}

impl From<io::Error> for Error {
    #[inline]
    fn from(e: io::Error) -> Error {
        Error::Io(e.to_string())
    }
}
