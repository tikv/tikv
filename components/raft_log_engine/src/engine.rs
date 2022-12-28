// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

use std::{
    fs,
    io::{Read, Result as IoResult, Seek, SeekFrom, Write},
    path::{Path, PathBuf},
    sync::Arc,
};

use codec::number::NumberCodec;
use encryption::{DataKeyManager, DecrypterReader, EncrypterWriter};
use engine_traits::{
    CacheStats, EncryptionKeyManager, EncryptionMethod, PerfContextExt, PerfContextKind, PerfLevel,
    RaftEngine, RaftEngineDebug, RaftEngineReadOnly, RaftLogBatch as RaftLogBatchTrait, Result,
    CF_DEFAULT, CF_LOCK, CF_RAFT, CF_WRITE,
};
use file_system::{IoOp, IoRateLimiter, IoType};
use kvproto::{
    metapb::Region,
    raft_serverpb::{
        RaftApplyState, RaftLocalState, RegionLocalState, StoreIdent, StoreRecoverState,
    },
};
use raft::eraftpb::Entry;
use raft_engine::{
    env::{DefaultFileSystem, FileSystem, Handle, WriteExt},
    Command, Engine as RawRaftEngine, Error as RaftEngineError, LogBatch, MessageExt,
};
pub use raft_engine::{Config as RaftEngineConfig, ReadableSize, RecoveryMode};
use tikv_util::Either;

use crate::perf_context::RaftEnginePerfContext;

// A special region ID representing store state.
const STORE_STATE_ID: u64 = 0;

#[derive(Clone)]
pub struct MessageExtTyped;

impl MessageExt for MessageExtTyped {
    type Entry = Entry;

    fn index(e: &Entry) -> u64 {
        e.index
    }
}

pub struct ManagedReader {
    inner: Either<
        <DefaultFileSystem as FileSystem>::Reader,
        DecrypterReader<<DefaultFileSystem as FileSystem>::Reader>,
    >,
    rate_limiter: Option<Arc<IoRateLimiter>>,
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
            size = limiter.request(IoType::ForegroundRead, IoOp::Read, size);
        }
        match self.inner.as_mut() {
            Either::Left(reader) => reader.read(&mut buf[..size]),
            Either::Right(reader) => reader.read(&mut buf[..size]),
        }
    }
}

pub struct ManagedWriter {
    inner: Either<
        <DefaultFileSystem as FileSystem>::Writer,
        EncrypterWriter<<DefaultFileSystem as FileSystem>::Writer>,
    >,
    rate_limiter: Option<Arc<IoRateLimiter>>,
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
            size = limiter.request(IoType::ForegroundWrite, IoOp::Write, size);
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

    fn allocate(&mut self, offset: usize, size: usize) -> IoResult<()> {
        match self.inner.as_mut() {
            Either::Left(writer) => writer.allocate(offset, size),
            Either::Right(writer) => writer.inner_mut().allocate(offset, size),
        }
    }
}

pub struct ManagedFileSystem {
    base_file_system: DefaultFileSystem,
    key_manager: Option<Arc<DataKeyManager>>,
    rate_limiter: Option<Arc<IoRateLimiter>>,
}

impl ManagedFileSystem {
    pub fn new(
        key_manager: Option<Arc<DataKeyManager>>,
        rate_limiter: Option<Arc<IoRateLimiter>>,
    ) -> Self {
        Self {
            base_file_system: DefaultFileSystem,
            key_manager,
            rate_limiter,
        }
    }
}

pub struct ManagedHandle {
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

    fn sync(&self) -> IoResult<()> {
        self.base.sync()
    }
}

impl FileSystem for ManagedFileSystem {
    type Handle = ManagedHandle;
    type Reader = ManagedReader;
    type Writer = ManagedWriter;

    fn create<P: AsRef<Path>>(&self, path: P) -> IoResult<Self::Handle> {
        let base = Arc::new(self.base_file_system.create(path.as_ref())?);
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
            base: Arc::new(self.base_file_system.open(path.as_ref())?),
        })
    }

    fn delete<P: AsRef<Path>>(&self, path: P) -> IoResult<()> {
        if let Some(ref manager) = self.key_manager {
            manager.delete_file(path.as_ref().to_str().unwrap())?;
        }
        self.base_file_system.delete(path)
    }

    fn rename<P: AsRef<Path>>(&self, src_path: P, dst_path: P) -> IoResult<()> {
        if let Some(ref manager) = self.key_manager {
            // Note: `rename` will reuse the old entryption info from `src_path`.
            let src_str = src_path.as_ref().to_str().unwrap();
            let dst_str = dst_path.as_ref().to_str().unwrap();
            manager.link_file(src_str, dst_str)?;
            let r = self
                .base_file_system
                .rename(src_path.as_ref(), dst_path.as_ref());
            let del_file = if r.is_ok() { src_str } else { dst_str };
            if let Err(e) = manager.delete_file(del_file) {
                warn!("fail to remove encryption metadata during 'rename'"; "err" => ?e);
            }
            r
        } else {
            self.base_file_system.rename(src_path, dst_path)
        }
    }

    fn reuse<P: AsRef<Path>>(&self, src_path: P, dst_path: P) -> IoResult<()> {
        if let Some(ref manager) = self.key_manager {
            // Note: In contrast to `rename`, `reuse` will make sure the encryption
            // metadata is properly updated by rotating the encryption key for safety,
            // when encryption flag is true. It won't rewrite the data blocks with
            // the updated encryption metadata. Therefore, the old encrypted data
            // won't be accessible after this calling.
            let src_str = src_path.as_ref().to_str().unwrap();
            let dst_str = dst_path.as_ref().to_str().unwrap();
            manager.new_file(dst_path.as_ref().to_str().unwrap())?;
            let r = self
                .base_file_system
                .rename(src_path.as_ref(), dst_path.as_ref());
            let del_file = if r.is_ok() { src_str } else { dst_str };
            if let Err(e) = manager.delete_file(del_file) {
                warn!("fail to remove encryption metadata during 'reuse'"; "err" => ?e);
            }
            r
        } else {
            self.base_file_system.rename(src_path, dst_path)
        }
    }

    fn exists_metadata<P: AsRef<Path>>(&self, path: P) -> bool {
        if let Some(ref manager) = self.key_manager {
            if let Ok(info) = manager.get_file(path.as_ref().to_str().unwrap()) {
                if info.method != EncryptionMethod::Plaintext {
                    return true;
                }
            }
        }
        self.base_file_system.exists_metadata(path)
    }

    fn delete_metadata<P: AsRef<Path>>(&self, path: P) -> IoResult<()> {
        if let Some(ref manager) = self.key_manager {
            // Note: no error if the file doesn't exist.
            manager.delete_file(path.as_ref().to_str().unwrap())?;
        }
        self.base_file_system.delete_metadata(path)
    }

    fn new_reader(&self, handle: Arc<Self::Handle>) -> IoResult<Self::Reader> {
        let base_reader = self.base_file_system.new_reader(handle.base.clone())?;
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
        let base_writer = self.base_file_system.new_writer(handle.base.clone())?;

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

/// Convert a cf to id for encoding.
fn cf_to_id(cf: &str) -> u8 {
    match cf {
        CF_DEFAULT => 0,
        CF_LOCK => 1,
        CF_WRITE => 2,
        CF_RAFT => 3,
        _ => panic!("unrecognized cf {}", cf),
    }
}
const MAX_CF_ID: u8 = 3;

/// Encode a key in the format `{prefix}{num}`.
fn encode_key(prefix: &'static [u8], num: u64) -> [u8; 9] {
    debug_assert_eq!(prefix.len(), 1);
    let mut buf = [0; 9];
    buf[..prefix.len()].copy_from_slice(prefix);
    NumberCodec::encode_u64(&mut buf[prefix.len()..], num);
    buf
}

/// Encode a flush key in the format `{flush key prefix}{cf_id}{tablet_index}`.
fn encode_flushed_key(cf: &str, tablet_index: u64) -> [u8; 10] {
    debug_assert_eq!(FLUSH_STATE_KEY.len(), 1);
    let mut buf = [0; 10];
    buf[..FLUSH_STATE_KEY.len()].copy_from_slice(FLUSH_STATE_KEY);
    buf[FLUSH_STATE_KEY.len()] = cf_to_id(cf);
    NumberCodec::encode_u64(&mut buf[FLUSH_STATE_KEY.len() + 1..], tablet_index);
    buf
}

#[derive(Clone)]
pub struct RaftLogEngine(Arc<RawRaftEngine<ManagedFileSystem>>);

impl RaftLogEngine {
    pub fn new(
        config: RaftEngineConfig,
        key_manager: Option<Arc<DataKeyManager>>,
        rate_limiter: Option<Arc<IoRateLimiter>>,
    ) -> Result<Self> {
        let file_system = Arc::new(ManagedFileSystem::new(key_manager, rate_limiter));
        Ok(RaftLogEngine(Arc::new(
            RawRaftEngine::open_with_file_system(config, file_system).map_err(transfer_error)?,
        )))
    }

    pub fn path(&self) -> &str {
        self.0.path()
    }

    /// If path is not an empty directory, we say db exists.
    pub fn exists(path: &str) -> bool {
        let path = Path::new(path);
        if !path.exists() || !path.is_dir() {
            return false;
        }
        fs::read_dir(path).unwrap().next().is_some()
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

impl PerfContextExt for RaftLogEngine {
    type PerfContext = RaftEnginePerfContext;

    fn get_perf_context(&self, _level: PerfLevel, _kind: PerfContextKind) -> Self::PerfContext {
        RaftEnginePerfContext
    }
}

#[derive(Default)]
pub struct RaftLogBatch(LogBatch);

const RAFT_LOG_STATE_KEY: &[u8] = b"R";
const STORE_IDENT_KEY: &[u8] = &[0x01];
const PREPARE_BOOTSTRAP_REGION_KEY: &[u8] = &[0x02];
const REGION_STATE_KEY: &[u8] = &[0x03];
const APPLY_STATE_KEY: &[u8] = &[0x04];
const RECOVER_STATE_KEY: &[u8] = &[0x05];
const FLUSH_STATE_KEY: &[u8] = &[0x06];
// All keys are of the same length.
const KEY_PREFIX_LEN: usize = RAFT_LOG_STATE_KEY.len();

impl RaftLogBatchTrait for RaftLogBatch {
    fn append(&mut self, raft_group_id: u64, entries: Vec<Entry>) -> Result<()> {
        self.0
            .add_entries::<MessageExtTyped>(raft_group_id, &entries)
            .map_err(transfer_error)
    }

    fn cut_logs(&mut self, _: u64, _: u64, _: u64) {
        // It's unnecessary because overlapped entries can be handled in
        // `append`.
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
            .put_message(STORE_STATE_ID, STORE_IDENT_KEY.to_vec(), ident)
            .map_err(transfer_error)
    }

    fn put_prepare_bootstrap_region(&mut self, region: &Region) -> Result<()> {
        self.0
            .put_message(
                STORE_STATE_ID,
                PREPARE_BOOTSTRAP_REGION_KEY.to_vec(),
                region,
            )
            .map_err(transfer_error)
    }

    fn remove_prepare_bootstrap_region(&mut self) -> Result<()> {
        self.0
            .delete(STORE_STATE_ID, PREPARE_BOOTSTRAP_REGION_KEY.to_vec());
        Ok(())
    }

    fn put_region_state(
        &mut self,
        raft_group_id: u64,
        apply_index: u64,
        state: &RegionLocalState,
    ) -> Result<()> {
        let key = encode_key(REGION_STATE_KEY, apply_index);
        self.0
            .put_message(raft_group_id, key.to_vec(), state)
            .map_err(transfer_error)
    }

    fn put_apply_state(
        &mut self,
        raft_group_id: u64,
        apply_index: u64,
        state: &RaftApplyState,
    ) -> Result<()> {
        let key = encode_key(APPLY_STATE_KEY, apply_index);
        self.0
            .put_message(raft_group_id, key.to_vec(), state)
            .map_err(transfer_error)
    }

    fn put_flushed_index(
        &mut self,
        raft_group_id: u64,
        cf: &str,
        tablet_index: u64,
        apply_index: u64,
    ) -> Result<()> {
        let key = encode_flushed_key(cf, tablet_index);
        let mut value = vec![0; 8];
        NumberCodec::encode_u64(&mut value, apply_index);
        self.0.put(raft_group_id, key.to_vec(), value);
        Ok(())
    }

    fn put_recover_state(&mut self, state: &StoreRecoverState) -> Result<()> {
        self.0
            .put_message(STORE_STATE_ID, RECOVER_STATE_KEY.to_vec(), state)
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
            .get_message(STORE_STATE_ID, STORE_IDENT_KEY)
            .map_err(transfer_error)
    }

    fn get_prepare_bootstrap_region(&self) -> Result<Option<Region>> {
        self.0
            .get_message(STORE_STATE_ID, PREPARE_BOOTSTRAP_REGION_KEY)
            .map_err(transfer_error)
    }

    fn get_region_state(
        &self,
        raft_group_id: u64,
        apply_index: u64,
    ) -> Result<Option<RegionLocalState>> {
        let mut state = None;
        self.0
            .scan_messages(
                raft_group_id,
                Some(REGION_STATE_KEY),
                Some(APPLY_STATE_KEY),
                true,
                |key, value| {
                    let index = NumberCodec::decode_u64(&key[REGION_STATE_KEY.len()..]);
                    if index > apply_index {
                        true
                    } else {
                        state = Some(value);
                        false
                    }
                },
            )
            .map_err(transfer_error)?;
        Ok(state)
    }

    fn get_apply_state(
        &self,
        raft_group_id: u64,
        apply_index: u64,
    ) -> Result<Option<RaftApplyState>> {
        let mut state = None;
        self.0
            .scan_messages(
                raft_group_id,
                Some(APPLY_STATE_KEY),
                Some(RECOVER_STATE_KEY),
                true,
                |key, value| {
                    let index = NumberCodec::decode_u64(&key[REGION_STATE_KEY.len()..]);
                    if index > apply_index {
                        true
                    } else {
                        state = Some(value);
                        false
                    }
                },
            )
            .map_err(transfer_error)?;
        Ok(state)
    }

    fn get_flushed_index(&self, raft_group_id: u64, cf: &str) -> Result<Option<u64>> {
        let mut start = [0; 2];
        start[..FLUSH_STATE_KEY.len()].copy_from_slice(FLUSH_STATE_KEY);
        start[FLUSH_STATE_KEY.len()] = cf_to_id(cf);
        let mut end = start;
        end[FLUSH_STATE_KEY.len()] += 1;
        let mut index = None;
        self.0
            .scan_raw_messages(raft_group_id, Some(&start), Some(&end), true, |_, v| {
                index = Some(NumberCodec::decode_u64(v));
                false
            })
            .map_err(transfer_error)?;
        Ok(index)
    }

    fn get_recover_state(&self) -> Result<Option<StoreRecoverState>> {
        self.0
            .get_message(STORE_STATE_ID, RECOVER_STATE_KEY)
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

    fn gc(
        &self,
        raft_group_id: u64,
        _from: u64,
        to: u64,
        batch: &mut Self::LogBatch,
    ) -> Result<()> {
        batch
            .0
            .add_command(raft_group_id, Command::Compact { index: to });
        Ok(())
    }

    fn delete_all_but_one_states_before(
        &self,
        raft_group_id: u64,
        apply_index: u64,
        batch: &mut Self::LogBatch,
    ) -> Result<()> {
        // Makes sure REGION_STATE_KEY is the smallest and FLUSH_STATE_KEY is the
        // largest.
        debug_assert!(REGION_STATE_KEY < APPLY_STATE_KEY);
        debug_assert!(APPLY_STATE_KEY < FLUSH_STATE_KEY);

        let mut end = [0; KEY_PREFIX_LEN + 1];
        end[..KEY_PREFIX_LEN].copy_from_slice(FLUSH_STATE_KEY);
        end[KEY_PREFIX_LEN] = MAX_CF_ID + 1;
        let mut found_region_state = false;
        let mut found_apply_state = false;
        let mut found_flush_state = [false; MAX_CF_ID as usize + 1];
        self.0
            .scan_raw_messages(
                raft_group_id,
                Some(REGION_STATE_KEY),
                Some(&end),
                true,
                |key, _| {
                    match &key[..KEY_PREFIX_LEN] {
                        REGION_STATE_KEY
                            if NumberCodec::decode_u64(&key[KEY_PREFIX_LEN..]) <= apply_index =>
                        {
                            if found_region_state {
                                batch.0.delete(raft_group_id, key.to_vec());
                            } else {
                                found_region_state = true;
                            }
                        }
                        APPLY_STATE_KEY
                            if NumberCodec::decode_u64(&key[KEY_PREFIX_LEN..]) <= apply_index =>
                        {
                            if found_apply_state {
                                batch.0.delete(raft_group_id, key.to_vec());
                            } else {
                                found_apply_state = true;
                            }
                        }
                        FLUSH_STATE_KEY => {
                            let cf_id = key[KEY_PREFIX_LEN];
                            let tablet_index = NumberCodec::decode_u64(&key[KEY_PREFIX_LEN + 1..]);
                            if cf_id <= MAX_CF_ID && tablet_index <= apply_index {
                                if found_flush_state[cf_id as usize] {
                                    batch.0.delete(raft_group_id, key.to_vec());
                                } else {
                                    found_flush_state[cf_id as usize] = true;
                                }
                            }
                        }
                        _ => {}
                    }
                    true
                },
            )
            .map_err(transfer_error)?;
        Ok(())
    }

    fn need_manual_purge(&self) -> bool {
        true
    }

    fn manual_purge(&self) -> Result<Vec<u64>> {
        self.0.purge_expired_files().map_err(transfer_error)
    }

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

    fn get_engine_path(&self) -> &str {
        self.path()
    }

    fn for_each_raft_group<E, F>(&self, f: &mut F) -> std::result::Result<(), E>
    where
        F: FnMut(u64) -> std::result::Result<(), E>,
        E: From<engine_traits::Error>,
    {
        for id in self.0.raft_groups() {
            if id != STORE_STATE_ID {
                f(id)?;
            }
        }
        Ok(())
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

#[cfg(test)]
mod tests {
    use std::assert_matches::assert_matches;

    use engine_traits::ALL_CFS;

    use super::*;

    #[test]
    fn test_apply_related_states() {
        let dir = tempfile::tempdir().unwrap();
        let cfg = RaftEngineConfig {
            dir: dir.path().to_str().unwrap().to_owned(),
            ..Default::default()
        };
        let engine = RaftLogEngine::new(cfg, None, None).unwrap();
        assert_matches!(engine.get_region_state(2, u64::MAX), Ok(None));
        assert_matches!(engine.get_apply_state(2, u64::MAX), Ok(None));
        for cf in ALL_CFS {
            assert_matches!(engine.get_flushed_index(2, cf), Ok(None));
        }

        let mut wb = engine.log_batch(10);
        let mut region_state = RegionLocalState::default();
        region_state.mut_region().set_id(3);
        wb.put_region_state(2, 1, &region_state).unwrap();
        let mut apply_state = RaftApplyState::default();
        apply_state.set_applied_index(3);
        wb.put_apply_state(2, 3, &apply_state).unwrap();
        for cf in ALL_CFS.iter().take(2) {
            wb.put_flushed_index(2, cf, 5, 4).unwrap();
        }
        engine.consume(&mut wb, false).unwrap();

        for cf in ALL_CFS.iter().take(2) {
            assert_matches!(engine.get_flushed_index(2, cf), Ok(Some(4)));
        }
        for cf in ALL_CFS.iter().skip(2) {
            assert_matches!(engine.get_flushed_index(2, cf), Ok(None));
        }

        let mut region_state2 = region_state.clone();
        region_state2.mut_region().set_id(5);
        wb.put_region_state(2, 4, &region_state2).unwrap();
        let mut apply_state2 = apply_state.clone();
        apply_state2.set_applied_index(5);
        wb.put_apply_state(2, 5, &apply_state2).unwrap();
        for cf in ALL_CFS {
            wb.put_flushed_index(2, cf, 6, 5).unwrap();
        }
        engine.consume(&mut wb, false).unwrap();

        assert_matches!(engine.get_region_state(2, 0), Ok(None));
        assert_matches!(engine.get_region_state(2, 1), Ok(Some(s)) if s == region_state);
        assert_matches!(engine.get_region_state(2, 4), Ok(Some(s)) if s == region_state2);
        assert_matches!(engine.get_apply_state(2, 0), Ok(None));
        assert_matches!(engine.get_apply_state(2, 3), Ok(Some(s)) if s == apply_state);
        assert_matches!(engine.get_apply_state(2, 5), Ok(Some(s)) if s == apply_state2);
        for cf in ALL_CFS {
            assert_matches!(engine.get_flushed_index(2, cf), Ok(Some(5)));
        }
    }
}
