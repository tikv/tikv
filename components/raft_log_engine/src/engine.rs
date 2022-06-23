// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

use std::{
    fs,
    io::{Read, Result as IoResult, Seek, SeekFrom, Write},
    path::{Path, PathBuf},
    sync::Arc,
};

use encryption::{DataKeyManager, DecrypterReader, EncrypterWriter};
use engine_traits::{
    CacheStats, EncryptionKeyManager, RaftEngine, RaftEngineDebug, RaftEngineReadOnly,
    RaftLogBatch as RaftLogBatchTrait, RaftLogGCTask, Result,
};
use file_system::{IOOp, IORateLimiter, IOType};
use kvproto::{
    metapb::Region,
    raft_serverpb::{RaftApplyState, RaftLocalState, RegionLocalState, StoreIdent},
};
use raft::eraftpb::Entry;
use raft_engine::{
    env::{DefaultFileSystem, FileSystem, Handle, WriteExt},
    Command, Engine as RawRaftEngine, Error as RaftEngineError, LogBatch, MessageExt,
};
pub use raft_engine::{Config as RaftEngineConfig, ReadableSize, RecoveryMode};
use tikv_util::Either;

// A special region ID representing global state.
const STORE_REGION_ID: u64 = 0;

#[derive(Clone)]
pub struct MessageExtTyped;

impl MessageExt for MessageExtTyped {
    type Entry = Entry;

    fn index(e: &Entry) -> u64 {
        e.index
    }
}

struct ManagedReader {
    inner: Either<
        <DefaultFileSystem as FileSystem>::Reader,
        DecrypterReader<<DefaultFileSystem as FileSystem>::Reader>,
    >,
    rate_limiter: Option<Arc<IORateLimiter>>,
}

impl Seek for ManagedReader {
    fn seek(&mut self, pos: SeekFrom) -> IoResult<u64> {
        match self.inner.as_mut() {
            Either::Left(reader) => reader.seek(pos),
            Either::Right(reader) => reader.seek(pos),
        }
    }
}

impl Read for ManagedReader {
    fn read(&mut self, buf: &mut [u8]) -> IoResult<usize> {
        let mut size = buf.len();
        if let Some(ref mut limiter) = self.rate_limiter {
            size = limiter.request(IOType::ForegroundRead, IOOp::Read, size);
        }
        match self.inner.as_mut() {
            Either::Left(reader) => reader.read(&mut buf[..size]),
            Either::Right(reader) => reader.read(&mut buf[..size]),
        }
    }
}

struct ManagedWriter {
    inner: Either<
        <DefaultFileSystem as FileSystem>::Writer,
        EncrypterWriter<<DefaultFileSystem as FileSystem>::Writer>,
    >,
    rate_limiter: Option<Arc<IORateLimiter>>,
}

impl Seek for ManagedWriter {
    fn seek(&mut self, pos: SeekFrom) -> IoResult<u64> {
        match self.inner.as_mut() {
            Either::Left(writer) => writer.seek(pos),
            Either::Right(writer) => writer.seek(pos),
        }
    }
}

impl Write for ManagedWriter {
    fn write(&mut self, buf: &[u8]) -> IoResult<usize> {
        let mut size = buf.len();
        if let Some(ref mut limiter) = self.rate_limiter {
            size = limiter.request(IOType::ForegroundWrite, IOOp::Write, size);
        }
        match self.inner.as_mut() {
            Either::Left(writer) => writer.write(&buf[..size]),
            Either::Right(writer) => writer.write(&buf[..size]),
        }
    }

    fn flush(&mut self) -> IoResult<()> {
        Ok(())
    }
}

impl WriteExt for ManagedWriter {
    fn truncate(&mut self, offset: usize) -> IoResult<()> {
        self.seek(SeekFrom::Start(offset as u64))?;
        match self.inner.as_mut() {
            Either::Left(writer) => writer.truncate(offset),
            Either::Right(writer) => writer.inner_mut().truncate(offset),
        }
    }

    fn sync(&mut self) -> IoResult<()> {
        match self.inner.as_mut() {
            Either::Left(writer) => writer.sync(),
            Either::Right(writer) => writer.inner_mut().sync(),
        }
    }

    fn allocate(&mut self, offset: usize, size: usize) -> IoResult<()> {
        match self.inner.as_mut() {
            Either::Left(writer) => writer.allocate(offset, size),
            Either::Right(writer) => writer.inner_mut().allocate(offset, size),
        }
    }
}

struct ManagedFileSystem {
    base_level_file_system: DefaultFileSystem,
    key_manager: Option<Arc<DataKeyManager>>,
    rate_limiter: Option<Arc<IORateLimiter>>,
}

impl ManagedFileSystem {
    fn new(
        key_manager: Option<Arc<DataKeyManager>>,
        rate_limiter: Option<Arc<IORateLimiter>>,
    ) -> Self {
        Self {
            base_level_file_system: DefaultFileSystem,
            key_manager,
            rate_limiter,
        }
    }
}

struct ManagedHandle {
    path: PathBuf,
    base: Arc<<DefaultFileSystem as FileSystem>::Handle>,
}

impl Handle for ManagedHandle {
    fn truncate(&self, offset: usize) -> IoResult<()> {
        self.base.truncate(offset)
    }

    fn file_size(&self) -> IoResult<usize> {
        self.base.file_size()
    }
}

impl FileSystem for ManagedFileSystem {
    type Handle = ManagedHandle;
    type Reader = ManagedReader;
    type Writer = ManagedWriter;

    fn create<P: AsRef<Path>>(&self, path: P) -> IoResult<Self::Handle> {
        let base = Arc::new(self.base_level_file_system.create(path.as_ref())?);
        if let Some(ref manager) = self.key_manager {
            manager.new_file(path.as_ref().to_str().unwrap())?;
        }
        Ok(ManagedHandle {
            path: path.as_ref().to_path_buf(),
            base,
        })
    }

    fn open<P: AsRef<Path>>(&self, path: P) -> IoResult<Self::Handle> {
        Ok(ManagedHandle {
            path: path.as_ref().to_path_buf(),
            base: Arc::new(self.base_level_file_system.open(path.as_ref())?),
        })
    }

    fn new_reader(&self, handle: Arc<Self::Handle>) -> IoResult<Self::Reader> {
        let base_reader = self
            .base_level_file_system
            .new_reader(handle.base.clone())?;
        if let Some(ref key_manager) = self.key_manager {
            Ok(ManagedReader {
                inner: Either::Right(key_manager.open_file_with_reader(&handle.path, base_reader)?),
                rate_limiter: self.rate_limiter.clone(),
            })
        } else {
            Ok(ManagedReader {
                inner: Either::Left(base_reader),
                rate_limiter: self.rate_limiter.clone(),
            })
        }
    }

    fn new_writer(&self, handle: Arc<Self::Handle>) -> IoResult<Self::Writer> {
        let base_writer = self
            .base_level_file_system
            .new_writer(handle.base.clone())?;

        if let Some(ref key_manager) = self.key_manager {
            Ok(ManagedWriter {
                inner: Either::Right(key_manager.open_file_with_writer(
                    &handle.path,
                    base_writer,
                    false,
                )?),
                rate_limiter: self.rate_limiter.clone(),
            })
        } else {
            Ok(ManagedWriter {
                inner: Either::Left(base_writer),
                rate_limiter: self.rate_limiter.clone(),
            })
        }
    }
}

#[derive(Clone)]
pub struct RaftLogEngine(Arc<RawRaftEngine<ManagedFileSystem>>);

impl RaftLogEngine {
    pub fn new(
        config: RaftEngineConfig,
        key_manager: Option<Arc<DataKeyManager>>,
        rate_limiter: Option<Arc<IORateLimiter>>,
    ) -> Result<Self> {
        let file_system = Arc::new(ManagedFileSystem::new(key_manager, rate_limiter));
        Ok(RaftLogEngine(Arc::new(
            RawRaftEngine::open_with_file_system(config, file_system).map_err(transfer_error)?,
        )))
    }

    /// If path is not an empty directory, we say db exists.
    pub fn exists(path: &str) -> bool {
        let path = Path::new(path);
        if !path.exists() || !path.is_dir() {
            return false;
        }
        fs::read_dir(&path).unwrap().next().is_some()
    }

    pub fn raft_groups(&self) -> Vec<u64> {
        self.0.raft_groups()
    }

    pub fn first_index(&self, raft_id: u64) -> Option<u64> {
        self.0.first_index(raft_id)
    }

    pub fn last_index(&self, raft_id: u64) -> Option<u64> {
        self.0.last_index(raft_id)
    }
}

#[derive(Default)]
pub struct RaftLogBatch(LogBatch);

const RAFT_LOG_STATE_KEY: &[u8] = b"R";
const STORE_IDENT_KEY: &[u8] = &[0x01];
const PREPARE_BOOTSTRAP_REGION_KEY: &[u8] = &[0x02];
const REGION_STATE_KEY: &[u8] = &[0x03];
const APPLY_STATE_KEY: &[u8] = &[0x04];

impl RaftLogBatchTrait for RaftLogBatch {
    fn append(&mut self, raft_group_id: u64, entries: Vec<Entry>) -> Result<()> {
        self.0
            .add_entries::<MessageExtTyped>(raft_group_id, &entries)
            .map_err(transfer_error)
    }

    fn cut_logs(&mut self, _: u64, _: u64, _: u64) {
        // It's unnecessary because overlapped entries can be handled in `append`.
    }

    fn put_raft_state(&mut self, raft_group_id: u64, state: &RaftLocalState) -> Result<()> {
        self.0
            .put_message(raft_group_id, RAFT_LOG_STATE_KEY.to_vec(), state)
            .map_err(transfer_error)
    }

    fn persist_size(&self) -> usize {
        self.0.approximate_size()
    }

    fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    fn merge(&mut self, mut src: Self) -> Result<()> {
        self.0.merge(&mut src.0).map_err(transfer_error)
    }

    fn put_store_ident(&mut self, ident: &StoreIdent) -> Result<()> {
        self.0
            .put_message(STORE_REGION_ID, STORE_IDENT_KEY.to_vec(), ident)
            .map_err(transfer_error)
    }

    fn put_prepare_bootstrap_region(&mut self, region: &Region) -> Result<()> {
        self.0
            .put_message(
                STORE_REGION_ID,
                PREPARE_BOOTSTRAP_REGION_KEY.to_vec(),
                region,
            )
            .map_err(transfer_error)
    }

    fn remove_prepare_bootstrap_region(&mut self) -> Result<()> {
        self.0
            .delete(STORE_REGION_ID, PREPARE_BOOTSTRAP_REGION_KEY.to_vec());
        Ok(())
    }

    fn put_region_state(&mut self, raft_group_id: u64, state: &RegionLocalState) -> Result<()> {
        self.0
            .put_message(raft_group_id, REGION_STATE_KEY.to_vec(), state)
            .map_err(transfer_error)
    }

    fn put_apply_state(&mut self, raft_group_id: u64, state: &RaftApplyState) -> Result<()> {
        self.0
            .put_message(raft_group_id, APPLY_STATE_KEY.to_vec(), state)
            .map_err(transfer_error)
    }
}

impl RaftEngineReadOnly for RaftLogEngine {
    fn get_raft_state(&self, raft_group_id: u64) -> Result<Option<RaftLocalState>> {
        self.0
            .get_message(raft_group_id, RAFT_LOG_STATE_KEY)
            .map_err(transfer_error)
    }

    fn get_entry(&self, raft_group_id: u64, index: u64) -> Result<Option<Entry>> {
        self.0
            .get_entry::<MessageExtTyped>(raft_group_id, index)
            .map_err(transfer_error)
    }

    fn fetch_entries_to(
        &self,
        raft_group_id: u64,
        begin: u64,
        end: u64,
        max_size: Option<usize>,
        to: &mut Vec<Entry>,
    ) -> Result<usize> {
        self.0
            .fetch_entries_to::<MessageExtTyped>(raft_group_id, begin, end, max_size, to)
            .map_err(transfer_error)
    }

    fn get_all_entries_to(&self, raft_group_id: u64, buf: &mut Vec<Entry>) -> Result<()> {
        if let Some(first) = self.0.first_index(raft_group_id) {
            let last = self.0.last_index(raft_group_id).unwrap();
            buf.reserve((last - first + 1) as usize);
            self.fetch_entries_to(raft_group_id, first, last + 1, None, buf)?;
        }
        Ok(())
    }

    fn is_empty(&self) -> Result<bool> {
        self.get_store_ident().map(|i| i.is_none())
    }

    fn get_store_ident(&self) -> Result<Option<StoreIdent>> {
        self.0
            .get_message(STORE_REGION_ID, STORE_IDENT_KEY)
            .map_err(transfer_error)
    }

    fn get_prepare_bootstrap_region(&self) -> Result<Option<Region>> {
        self.0
            .get_message(STORE_REGION_ID, PREPARE_BOOTSTRAP_REGION_KEY)
            .map_err(transfer_error)
    }

    fn get_region_state(&self, raft_group_id: u64) -> Result<Option<RegionLocalState>> {
        self.0
            .get_message(raft_group_id, REGION_STATE_KEY)
            .map_err(transfer_error)
    }

    fn get_apply_state(&self, raft_group_id: u64) -> Result<Option<RaftApplyState>> {
        self.0
            .get_message(raft_group_id, APPLY_STATE_KEY)
            .map_err(transfer_error)
    }
}

impl RaftEngineDebug for RaftLogEngine {
    fn scan_entries<F>(&self, raft_group_id: u64, mut f: F) -> Result<()>
    where
        F: FnMut(&Entry) -> Result<bool>,
    {
        if let Some(first_index) = self.first_index(raft_group_id) {
            for idx in first_index..=self.last_index(raft_group_id).unwrap() {
                if let Some(entry) = self.get_entry(raft_group_id, idx)? {
                    if !f(&entry)? {
                        break;
                    }
                }
            }
        }
        Ok(())
    }
}

impl RaftEngine for RaftLogEngine {
    type LogBatch = RaftLogBatch;

    fn log_batch(&self, capacity: usize) -> Self::LogBatch {
        RaftLogBatch(LogBatch::with_capacity(capacity))
    }

    fn sync(&self) -> Result<()> {
        self.0.sync().map_err(transfer_error)
    }

    fn consume(&self, batch: &mut Self::LogBatch, sync: bool) -> Result<usize> {
        self.0.write(&mut batch.0, sync).map_err(transfer_error)
    }

    fn consume_and_shrink(
        &self,
        batch: &mut Self::LogBatch,
        sync: bool,
        _: usize,
        _: usize,
    ) -> Result<usize> {
        self.0.write(&mut batch.0, sync).map_err(transfer_error)
    }

    fn clean(
        &self,
        raft_group_id: u64,
        _: u64,
        _: &RaftLocalState,
        batch: &mut RaftLogBatch,
    ) -> Result<()> {
        batch.0.add_command(raft_group_id, Command::Clean);
        Ok(())
    }

    fn append(&self, raft_group_id: u64, entries: Vec<Entry>) -> Result<usize> {
        let mut batch = Self::LogBatch::default();
        batch
            .0
            .add_entries::<MessageExtTyped>(raft_group_id, &entries)
            .map_err(transfer_error)?;
        self.0.write(&mut batch.0, false).map_err(transfer_error)
    }

    fn put_store_ident(&self, ident: &StoreIdent) -> Result<()> {
        let mut batch = Self::LogBatch::default();
        batch
            .0
            .put_message(STORE_REGION_ID, STORE_IDENT_KEY.to_vec(), ident)
            .map_err(transfer_error)?;
        self.0.write(&mut batch.0, true).map_err(transfer_error)?;
        Ok(())
    }

    fn put_raft_state(&self, raft_group_id: u64, state: &RaftLocalState) -> Result<()> {
        let mut batch = Self::LogBatch::default();
        batch
            .0
            .put_message(raft_group_id, RAFT_LOG_STATE_KEY.to_vec(), state)
            .map_err(transfer_error)?;
        self.0.write(&mut batch.0, false).map_err(transfer_error)?;
        Ok(())
    }

    fn gc(&self, raft_group_id: u64, from: u64, to: u64) -> Result<usize> {
        self.batch_gc(vec![RaftLogGCTask {
            raft_group_id,
            from,
            to,
        }])
    }

    fn batch_gc(&self, tasks: Vec<RaftLogGCTask>) -> Result<usize> {
        let mut batch = self.log_batch(tasks.len());
        let mut old_first_index = Vec::with_capacity(tasks.len());
        for task in &tasks {
            batch
                .0
                .add_command(task.raft_group_id, Command::Compact { index: task.to });
            old_first_index.push(self.0.first_index(task.raft_group_id));
        }

        self.0.write(&mut batch.0, false).map_err(transfer_error)?;

        let mut total = 0;
        for (old_first_index, task) in old_first_index.iter().zip(tasks) {
            let new_first_index = self.0.first_index(task.raft_group_id);
            if let (Some(old), Some(new)) = (old_first_index, new_first_index) {
                total += new.saturating_sub(*old);
            }
        }
        Ok(total as usize)
    }

    fn purge_expired_files(&self) -> Result<Vec<u64>> {
        self.0.purge_expired_files().map_err(transfer_error)
    }

    fn has_builtin_entry_cache(&self) -> bool {
        false
    }

    fn gc_entry_cache(&self, _raft_group_id: u64, _to: u64) {}

    /// Flush current cache stats.
    fn flush_stats(&self) -> Option<CacheStats> {
        None
    }

    fn dump_stats(&self) -> Result<String> {
        // Raft engine won't dump anything.
        Ok("".to_owned())
    }

    fn get_engine_size(&self) -> Result<u64> {
        Ok(self.0.get_used_size() as u64)
    }
}

fn transfer_error(e: RaftEngineError) -> engine_traits::Error {
    match e {
        RaftEngineError::EntryCompacted => engine_traits::Error::EntriesCompacted,
        RaftEngineError::EntryNotFound => engine_traits::Error::EntriesUnavailable,
        e => {
            let e = box_err!(e);
            engine_traits::Error::Other(e)
        }
    }
}
