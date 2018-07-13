// Copyright 2018 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.
mod builder;
mod reader;
use self::builder::SnapshotGenerator;
pub use self::builder::SnapshotReceiver;
pub use self::reader::SnapshotSender;
use self::reader::{SnapshotApplyer, SnapshotLoader};

use std::fs::{self, File};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime};
use std::{fmt, io};

use protobuf::stream::CodedInputStream;
use protobuf::Message;
use regex::Regex;

use kvproto::metapb::Region;
use kvproto::raft_serverpb::{RaftSnapshotData, SnapshotCFFile, SnapshotMeta};
use raft::eraftpb::Snapshot as RaftSnapshot;
use rocksdb::DB;

use raftstore::store::engine::Snapshot as DbSnapshot;
use raftstore::store::peer_storage::SnapStaleNotifier;
use raftstore::store::Msg;
use raftstore::{Error, Result};
use storage::{CfName, CF_DEFAULT, CF_LOCK, CF_WRITE};
use util::collections::HashMap;
use util::file::delete_file_if_exist;
use util::io_limiter::{IOLimiter, LimitWriter};
use util::transport::SendCh;

const SNAPSHOT_VERSION: u64 = 2;
const SNAPSHOT_CFS: &[CfName] = &[CF_DEFAULT, CF_LOCK, CF_WRITE];

const SNAP_GEN_PREFIX: &str = "gen";
const SNAP_REV_PREFIX: &str = "rev";

const META_FILE_SUFFIX: &str = ".meta";
const SST_FILE_SUFFIX: &str = ".sst";
const TMP_FILE_SUFFIX: &str = ".tmp";
const CLONE_FILE_SUFFIX: &str = ".clone";

quick_error! {
    #[derive(Debug, Clone)]
    pub enum SnapError {
        Abort {
            description("abort")
            display("abort")
        }
        Registry(exists: &'static str, new: &'static str) {
            description("registry conflict")
            display("exists: {}, new: {}", exists, new)
        }
        NoSpace(size: u64, limit: u64) {
            description("disk space exceed")
            display("total snapshot size: {}, limit: {}", size, limit)
        }
    }
}

#[derive(Clone, Copy, Hash, PartialEq, Eq, PartialOrd, Ord, Debug)]
pub struct SnapKey {
    pub region_id: u64,
    pub term: u64,
    pub idx: u64,
}

impl fmt::Display for SnapKey {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}_{}_{}", self.region_id, self.term, self.idx)
    }
}

impl SnapKey {
    #[inline]
    pub fn new(region_id: u64, term: u64, idx: u64) -> SnapKey {
        SnapKey {
            region_id,
            term,
            idx,
        }
    }

    pub fn from_region_snap(region_id: u64, snap: &RaftSnapshot) -> SnapKey {
        let index = snap.get_metadata().get_index();
        let term = snap.get_metadata().get_term();
        SnapKey::new(region_id, term, index)
    }

    pub fn from_snap(snap: &RaftSnapshot) -> io::Result<SnapKey> {
        let mut snap_data = RaftSnapshotData::new();
        if let Err(e) = snap_data.merge_from_bytes(snap.get_data()) {
            return Err(io::Error::new(io::ErrorKind::Other, e));
        }

        Ok(SnapKey::from_region_snap(
            snap_data.get_region().get_id(),
            snap,
        ))
    }
}
#[derive(Clone)]
pub struct SnapManager {
    core: Arc<SnapManagerCore>,
    max_total_size: u64,
    io_limiter: Option<Arc<IOLimiter>>,
    ch: Option<SendCh<Msg>>,
}

impl SnapManager {
    pub fn new<T: Into<String>>(path: T, ch: Option<SendCh<Msg>>) -> SnapManager {
        SnapManagerBuilder::default().build(path, ch)
    }

    pub fn get_total_snap_size(&self) -> u64 {
        self.core.snap_size.load(Ordering::SeqCst)
    }

    pub fn stats(&self) -> SnapStats {
        SnapStats {
            sending_count: self.core.gen_registry.lock().unwrap().len(),
            receiving_count: self.core.apply_registry.lock().unwrap().len(),
        }
    }

    /// Inititalize the SnapManager, read snapshot metadatas from disk.
    pub fn init(&self) -> Result<()> {
        let path = Path::new(&self.core.base);
        if !path.exists() {
            fs::create_dir_all(path)?;
            return Ok(());
        }
        let all_meta_files = self.scan_meta_files(path)?;
        self.scan_and_delete_orphan_files(path, all_meta_files)?;
        Ok(())
    }

    /// Build a snapshot with metadata in `RaftSnapshotData`.
    /// To get the readable snapshot, call `get_snapshot_reader`.
    pub fn build_snapshot(
        &self,
        key: SnapKey,
        region: Region,
        db_snap: DbSnapshot,
        snap_stale_notifier: Arc<SnapStaleNotifier>,
    ) -> Option<Result<RaftSnapshotData>> {
        let mut registry = self.core.gen_registry.lock().unwrap();
        if let Some(entry) = registry.get(&key) {
            return entry
                .meta
                .as_ref()
                .map(|m| Ok(snapshot_data_from_meta(m.clone(), region)));
        }
        let notifier = Arc::clone(&snap_stale_notifier);
        registry.insert(key, SnapGenEntry::new(notifier));
        drop(registry);

        match snapshot_load(&self.core.base, true, key).unwrap_or_else(|| {
            self.gc_snapshots_if_need()?;
            let dir = self.core.base.to_owned();
            let io_limiter = self.io_limiter.clone();
            SnapshotGenerator::new(dir, key, io_limiter, snap_stale_notifier)
                .and_then(|mut gen| gen.build(&region, db_snap))
                .map(|meta| {
                    let size = get_size_from_snapshot_meta(&meta);
                    self.core.snap_size.fetch_add(size, Ordering::SeqCst);
                    meta
                })
        }) {
            Ok(meta) => {
                let mut registry = self.core.gen_registry.lock().unwrap();
                registry.get_mut(&key).unwrap().meta = Some(meta.clone());
                Some(Ok(snapshot_data_from_meta(meta, region)))
            }
            Err(e) => {
                error!("{} build_snapshot fail: {}", key, e);
                self.core.delete_snapshot(true, key, true);
                Some(Err(e))
            }
        }
    }

    pub fn get_snapshot_sender(&self, key: SnapKey) -> Result<SnapshotSender> {
        let registry = self.core.gen_registry.lock().unwrap();
        if let Some(gen_entry) = registry.get(&key) {
            match gen_entry.meta {
                None => {
                    error!("{} get_snapshot_sender while building", key);
                    Err(Error::Snapshot(SnapError::Registry("generate", "send")))
                }
                Some(ref m) => {
                    let dir = self.core.base.clone();
                    let meta = m.clone();
                    let notifier = Arc::clone(&gen_entry.snap_stale_notifier);
                    let ref_count = Arc::clone(&gen_entry.ref_count);
                    let sent_times = Arc::clone(&gen_entry.sent_times);
                    Ok(SnapshotSender::new(
                        dir, key, meta, notifier, ref_count, sent_times,
                    ))
                }
            }
        } else {
            error!("{} get_snapshot_sender without building", key);
            Err(Error::Snapshot(SnapError::Registry("none", "send")))
        }
    }

    pub fn get_snapshot_receiver(
        &self,
        key: SnapKey,
        data: &[u8],
    ) -> Result<Option<SnapshotReceiver>> {
        let meta = snapshot_meta_from_data(data)?;
        self.gc_snapshots_if_need()?;

        let mut registry = self.core.apply_registry.lock().unwrap();
        if let Some(entry) = registry.get(&key) {
            if entry.meta.is_some() {
                return Ok(None);
            }
            error!("{} get_snapshot_receiver multi times", key);
            return Err(Error::Snapshot(SnapError::Registry("receive", "receive")));
        }

        registry.insert(key, SnapApplyEntry::new());
        drop(registry);

        match snapshot_load(&self.core.base, false, key) {
            Some(Ok(_)) => return Ok(None),
            Some(Err(e)) => {
                error!("{} get_snapshot_receiver fail when load: {}", key, e);
                self.core.delete_snapshot(false, key, false);
            }
            None => {}
        };

        let dir = self.core.base.clone();
        let io_limiter = self.io_limiter.clone();
        let core = Arc::clone(&self.core);
        SnapshotReceiver::new(dir, key, io_limiter, meta, core).map(Some)
    }

    pub fn apply_snapshot(
        &self,
        key: SnapKey,
        options: ApplyOptions,
        notifier: Arc<SnapStaleNotifier>,
    ) -> Result<()> {
        let registry = self.core.apply_registry.lock().unwrap();
        if let Some(entry) = registry.get(&key) {
            if entry.applied.load(Ordering::SeqCst) {
                error!("{} apply_snapshot already applied", key);
                return Err(Error::Snapshot(SnapError::Registry("apply", "apply")));
            }
            if let Some(ref m) = entry.meta {
                let dir = self.core.base.clone();
                let applied = Arc::clone(&entry.applied);
                let applyer = SnapshotApplyer::new(dir, key, m.clone(), notifier, applied);
                return applyer.apply(options);
            }
            error!("{} apply_snapshot while receiving", key);
            return Err(Error::Snapshot(SnapError::Registry("receive", "apply")));
        }
        error!("{} apply_snapshot without receiving", key);
        Err(Error::Snapshot(SnapError::Registry("none", "apply")))
    }

    fn scan_meta_files(&self, path: &Path) -> io::Result<Vec<(bool, SnapKey)>> {
        let mut all_meta_files = Vec::new();
        for p in fs::read_dir(path)?
            .filter_map(|p| p.ok())
            .filter(|p| p.file_type().map(|ft| ft.is_file()).unwrap_or(false))
        {
            if let Some(s) = p.file_name().to_str() {
                if s.ends_with(TMP_FILE_SUFFIX) || s.ends_with(CLONE_FILE_SUFFIX) {
                    delete_file_if_exist(&p.path());
                } else if s.ends_with(META_FILE_SUFFIX) {
                    if let Some((for_send, snap_key)) = get_snap_key_from_file_name(s) {
                        all_meta_files.push((for_send, snap_key));
                    } else {
                        delete_file_if_exist(&p.path());
                        continue;
                    }

                    let snap_meta = match read_snapshot_meta(p.path()) {
                        Ok(meta) => meta,
                        Err(e) => {
                            warn!("SnapManager init meets {} for {:?}", e, p.path());
                            continue;
                        }
                    };
                    let size = get_size_from_snapshot_meta(&snap_meta);
                    self.core.snap_size.fetch_add(size, Ordering::SeqCst);
                }
            }
        }
        Ok(all_meta_files)
    }

    fn scan_and_delete_orphan_files(
        &self,
        path: &Path,
        meta_files: Vec<(bool, SnapKey)>,
    ) -> Result<()> {
        for p in fs::read_dir(path)?
            .filter_map(|p| p.ok())
            .filter(|p| p.file_type().map(|ft| ft.is_file()).unwrap_or(false))
        {
            if let Some(s) = p.file_name().to_str() {
                if s.ends_with(SST_FILE_SUFFIX) {
                    if let Some(t) = get_snap_key_from_file_name(s) {
                        if !meta_files.contains(&t) {
                            delete_file_if_exist(&p.path());
                        }
                    }
                }
            }
        }
        Ok(())
    }

    pub fn gc_snapshots(&self) -> Result<()> {
        let mut snap_keys = Vec::<(bool, SnapKey)>::new();
        for p in fs::read_dir(&self.core.base)?
            .filter_map(|p| p.ok())
            .filter(|p| p.file_type().map(|ft| ft.is_file()).unwrap_or(false))
        {
            if let Some(s) = p.file_name().to_str() {
                if s.ends_with(SST_FILE_SUFFIX) {
                    if let Some((for_send, snap_key)) = get_snap_key_from_file_name(s) {
                        snap_keys.push((for_send, snap_key));
                    }
                }
            }
        }

        snap_keys.sort_by_key(|&(for_send, key)| {
            let meta_path = get_meta_file_path(&self.core.base, for_send, key);
            fs::metadata(&meta_path)
                .and_then(|m| m.modified())
                .unwrap_or(SystemTime::UNIX_EPOCH)
        });

        for (for_send, key) in snap_keys {
            if for_send {
                let mut registry = self.core.gen_registry.lock().unwrap();
                if let Some(entry) = registry.get(&key) {
                    if entry.is_busy()
                        || (!entry.has_been_used()
                            && !self.core.snapshot_is_stale(for_send, key).unwrap_or(true))
                    {
                        continue;
                    }
                }
                self.core.delete_snapshot(for_send, key, false);
                registry.remove(&key);
            } else {
                let mut registry = self.core.apply_registry.lock().unwrap();
                if let Some(entry) = registry.get(&key) {
                    if entry.is_busy()
                        || (!entry.has_been_used()
                            && !self.core.snapshot_is_stale(for_send, key).unwrap_or(true))
                    {
                        continue;
                    }
                }
                self.core.delete_snapshot(for_send, key, false);
                registry.remove(&key);
            }
        }
        Ok(())
    }

    fn gc_snapshots_if_need(&self) -> Result<()> {
        if self.max_total_size > 0
            && self.core.snap_size.load(Ordering::SeqCst) > self.max_total_size
        {
            self.gc_snapshots()?;
            let size = self.core.snap_size.load(Ordering::SeqCst);
            let limit = self.max_total_size;
            if size > limit {
                return Err(Error::Snapshot(SnapError::NoSpace(size, limit)));
            }
        }
        Ok(())
    }
}

#[derive(Debug, Default)]
pub struct SnapManagerBuilder {
    max_write_bytes_per_sec: u64,
    max_total_size: u64,
    gc_timeout: Duration,
}

impl SnapManagerBuilder {
    pub fn max_write_bytes_per_sec(&mut self, bytes: u64) -> &mut SnapManagerBuilder {
        self.max_write_bytes_per_sec = bytes;
        self
    }

    pub fn max_total_size(&mut self, bytes: u64) -> &mut SnapManagerBuilder {
        self.max_total_size = bytes;
        self
    }

    pub fn gc_timeout(&mut self, timeout: Duration) -> &mut SnapManagerBuilder {
        self.gc_timeout = timeout;
        self
    }

    pub fn build<T: Into<String>>(&self, path: T, ch: Option<SendCh<Msg>>) -> SnapManager {
        let io_limiter = if self.max_write_bytes_per_sec > 0 {
            Some(Arc::new(IOLimiter::new(self.max_write_bytes_per_sec)))
        } else {
            None
        };
        SnapManager {
            core: Arc::new(SnapManagerCore {
                base: path.into(),
                gen_registry: Mutex::new(map![]),
                apply_registry: Mutex::new(map![]),
                snap_size: AtomicU64::new(0),
                gc_timeout: self.gc_timeout,
            }),
            max_total_size: self.max_total_size,
            io_limiter,
            ch,
        }
    }
}

/// `SnapStats` is for snapshot statistics.
#[derive(Default)]
pub struct SnapStats {
    pub sending_count: usize,
    pub receiving_count: usize,
}

pub struct ApplyOptions {
    pub db: Arc<DB>,
    pub region: Region,
    pub write_batch_size: usize,
}

struct SnapGenEntry {
    snap_stale_notifier: Arc<SnapStaleNotifier>,
    meta: Option<SnapshotMeta>,
    ref_count: Arc<AtomicUsize>,
    sent_times: Arc<AtomicUsize>,
}

impl SnapGenEntry {
    fn new(snap_stale_notifier: Arc<SnapStaleNotifier>) -> Self {
        SnapGenEntry {
            snap_stale_notifier,
            meta: None,
            ref_count: Arc::new(AtomicUsize::new(0)),
            sent_times: Arc::new(AtomicUsize::new(0)),
        }
    }

    fn is_busy(&self) -> bool {
        self.meta.is_none() || self.ref_count.load(Ordering::SeqCst) > 0
    }

    fn has_been_used(&self) -> bool {
        self.sent_times.load(Ordering::SeqCst) > 0
    }
}

#[derive(Clone)]
struct SnapApplyEntry {
    meta: Option<SnapshotMeta>,
    applied: Arc<AtomicBool>,
}

impl SnapApplyEntry {
    fn new() -> Self {
        SnapApplyEntry {
            meta: None,
            applied: Arc::new(AtomicBool::new(false)),
        }
    }

    fn is_busy(&self) -> bool {
        self.meta.is_none()
    }

    fn has_been_used(&self) -> bool {
        self.applied.load(Ordering::SeqCst)
    }
}

struct SnapManagerCore {
    base: String,
    gen_registry: Mutex<HashMap<SnapKey, SnapGenEntry>>,
    apply_registry: Mutex<HashMap<SnapKey, SnapApplyEntry>>,
    snap_size: AtomicU64,
    gc_timeout: Duration,
}

impl SnapManagerCore {
    fn snapshot_is_stale(&self, for_send: bool, key: SnapKey) -> Result<bool> {
        let meta_path = get_meta_file_path(&self.base, for_send, key);
        let modified = fs::metadata(&meta_path)?.modified()?;
        SystemTime::now()
            .duration_since(modified)
            .map(|dur| dur > self.gc_timeout)
            .map_err(|e| box_err!("get modified time fail: {}", e))
    }

    fn delete_snapshot(&self, for_send: bool, key: SnapKey, deregister: bool) {
        match read_snapshot_meta(&get_meta_file_path(&self.base, for_send, key)) {
            Ok(meta) => {
                let size = get_size_from_snapshot_meta(&meta);
                self.snap_size.fetch_sub(size, Ordering::SeqCst);
            }
            Err(_) => return,
        }

        delete_file_if_exist(&get_meta_file_path(&self.base, for_send, key));
        for cf in SNAPSHOT_CFS {
            delete_file_if_exist(&get_cf_file_path(&self.base, for_send, key, cf));
        }

        if deregister {
            if for_send {
                let mut r = self.gen_registry.lock().unwrap();
                r.remove(&key);
            } else {
                let mut r = self.apply_registry.lock().unwrap();
                r.remove(&key);
            }
        }
    }
}

// Read SnapshotMeta from meta file on disk.
fn read_snapshot_meta<P: AsRef<Path>>(path: P) -> io::Result<SnapshotMeta> {
    let mut file = File::open(path)?;
    let mut stream = CodedInputStream::new(&mut file);
    let mut meta = SnapshotMeta::new();
    meta.merge_from(&mut stream)?;
    Ok(meta)
}

fn snapshot_data_from_meta(meta: SnapshotMeta, region: Region) -> RaftSnapshotData {
    let mut snap_data = RaftSnapshotData::new();
    snap_data.set_version(SNAPSHOT_VERSION);
    snap_data.set_file_size(get_size_from_snapshot_meta(&meta));
    snap_data.set_meta(meta);
    snap_data.set_region(region);
    snap_data
}

fn snapshot_meta_from_data(data: &[u8]) -> io::Result<SnapshotMeta> {
    let mut snapshot_data = RaftSnapshotData::new();
    snapshot_data.merge_from_bytes(data)?;
    Ok(snapshot_data.take_meta())
}

fn get_size_from_snapshot_meta(meta: &SnapshotMeta) -> u64 {
    meta.get_cf_files().iter().fold(0, |mut acc, cf| {
        acc += cf.get_size() as u64;
        acc
    })
}

fn get_snap_key_from_file_name(s: &str) -> Option<(bool, SnapKey)> {
    let pattern = Regex::new("(gen|rev)_([0-9]+)_([0-9]+)_([0-9]+)").unwrap();
    let caps = pattern.captures(s)?;
    let for_send = caps.at(1)? == SNAP_GEN_PREFIX;
    let region_id = caps.at(2)?.parse().ok()?;
    let term = caps.at(3)?.parse().ok()?;
    let idx = caps.at(4)?.parse().ok()?;
    Some((for_send, SnapKey::new(region_id, term, idx)))
}

fn get_meta_file_path(dir: &str, sending: bool, key: SnapKey) -> PathBuf {
    let dir_path = PathBuf::from(dir);
    let file_name = if sending {
        format!("{}_{}{}", SNAP_GEN_PREFIX, key, META_FILE_SUFFIX)
    } else {
        format!("{}_{}{}", SNAP_REV_PREFIX, key, META_FILE_SUFFIX)
    };
    dir_path.join(&file_name)
}

fn get_cf_file_path(dir: &str, sending: bool, key: SnapKey, cf: &str) -> PathBuf {
    let dir_path = PathBuf::from(dir);
    let file_name = if sending {
        format!("{}_{}_{}{}", SNAP_GEN_PREFIX, key, cf, SST_FILE_SUFFIX)
    } else {
        format!("{}_{}_{}{}", SNAP_REV_PREFIX, key, cf, SST_FILE_SUFFIX)
    };
    dir_path.join(&file_name)
}

fn plain_file_used(cf: &str) -> bool {
    cf == CF_LOCK
}

fn snapshot_size_corrupt(expected: u64, got: u64) -> Error {
    Error::from(io::Error::new(
        io::ErrorKind::Other,
        format!(
            "snapshot size corrupted, expected: {}, got: {}",
            expected, got
        ),
    ))
}

fn snapshot_checksum_corrupt(expected: u32, got: u32) -> Error {
    Error::from(io::Error::new(
        io::ErrorKind::Other,
        format!(
            "snapshot checksum corrupted, expected: {}, got: {}",
            expected, got
        ),
    ))
}

fn snapshot_load(dir: &str, for_send: bool, key: SnapKey) -> Option<Result<SnapshotMeta>> {
    let meta_path = get_meta_file_path(dir, for_send, key);
    read_snapshot_meta(&meta_path)
        .ok()
        .map(|meta| SnapshotLoader::new(dir.to_owned(), for_send, key, meta).load())
}

fn stale_for_generate(key: SnapKey, snap_stale_notifier: &SnapStaleNotifier) -> bool {
    let compacted_term = snap_stale_notifier.compacted_term.load(Ordering::SeqCst);
    let compacted_idx = snap_stale_notifier.compacted_idx.load(Ordering::SeqCst);
    key.term < compacted_term || key.idx < compacted_idx
}

fn stale_for_apply(key: SnapKey, snap_stale_notifier: &SnapStaleNotifier) -> bool {
    let compacted_term = snap_stale_notifier.compacted_term.load(Ordering::SeqCst);
    let compacted_idx = snap_stale_notifier.compacted_idx.load(Ordering::SeqCst);
    let apply_canceled = snap_stale_notifier.apply_canceled.load(Ordering::SeqCst);
    key.term < compacted_term || key.idx < compacted_idx || apply_canceled
}

#[cfg(test)]
mod tests {
    use std::fs::OpenOptions;

    use tempdir::TempDir;

    use kvproto::metapb::Peer;

    use raftstore::store::engine::{Mutable, Peekable};
    use raftstore::store::keys;
    use storage::ALL_CFS;
    use util::rocksdb;

    use super::*;

    const TEST_KEY: &[u8] = b"akey";
    const TEST_STORE_ID: u64 = 1;
    const TEST_WRITE_BATCH_SIZE: usize = 10 * 1024 * 1024;

    fn get_test_region(region_id: u64, store_id: u64, peer_id: u64) -> Region {
        let mut peer = Peer::new();
        peer.set_store_id(store_id);
        peer.set_id(peer_id);
        let mut region = Region::new();
        region.set_id(region_id);
        region.set_start_key(b"a".to_vec());
        region.set_end_key(b"z".to_vec());
        region.mut_region_epoch().set_version(1);
        region.mut_region_epoch().set_conf_ver(1);
        region.mut_peers().push(peer.clone());
        region
    }

    fn assert_eq_db(expected_db: &DB, db: &DB) {
        let key = keys::data_key(TEST_KEY);
        for cf in SNAPSHOT_CFS {
            let p1: Option<Peer> = expected_db.get_msg_cf(cf, &key).unwrap();
            let p2: Option<Peer> = db.get_msg_cf(cf, &key).unwrap();
            let equal = match (&p1, &p2) {
                (&Some(ref p1), &Some(ref p2)) => p1 == p2,
                (&None, &None) => true,
                _ => false,
            };
            if !equal {
                panic!(
                    "cf {}: key {:?}, value {:?}, expected {:?}",
                    cf, key, p2, p1
                );
            }
        }
    }

    fn get_test_empty_db(path: &TempDir) -> Result<Arc<DB>> {
        let p = path.path().to_str().unwrap();
        let db = rocksdb::new_engine(p, ALL_CFS, None)?;
        Ok(Arc::new(db))
    }

    fn get_test_db(path: &TempDir) -> Result<Arc<DB>> {
        let p = path.path().to_str().unwrap();
        let db = rocksdb::new_engine(p, ALL_CFS, None)?;
        let key = keys::data_key(TEST_KEY);
        // write some data into each cf
        for (i, cf) in ALL_CFS.iter().enumerate() {
            let handle = rocksdb::get_cf_handle(&db, cf)?;
            let mut p = Peer::new();
            p.set_store_id(TEST_STORE_ID);
            p.set_id((i + 1) as u64);
            db.put_msg_cf(handle, &key[..], &p)?;
        }
        Ok(Arc::new(db))
    }

    fn sst_files_size<P: AsRef<Path>>(path: P) -> io::Result<u64> {
        let mut size = 0;
        for p in fs::read_dir(path)?
            .filter_map(|p| p.ok())
            .filter(|p| p.file_type().map(|ft| ft.is_file()).unwrap_or(false))
        {
            if let Some(s) = p.file_name().to_str() {
                if !s.ends_with(SST_FILE_SUFFIX) {
                    continue;
                }
                size += fs::metadata(p.path())?.len();
            }
        }
        Ok(size)
    }

    fn check_snap_size(snap_mgr: &SnapManager) {
        let snap_size = snap_mgr.core.snap_size.load(Ordering::SeqCst);
        let total_size = sst_files_size(PathBuf::from(&snap_mgr.core.base)).unwrap();
        assert_eq!(snap_size, total_size);
    }

    fn new_snap_stale_notifier() -> Arc<SnapStaleNotifier> {
        Arc::new(SnapStaleNotifier {
            compacted_term: AtomicU64::new(0),
            compacted_idx: AtomicU64::new(0),
            apply_canceled: AtomicBool::new(false),
        })
    }

    fn corrupt_file<P: AsRef<Path>>(path: P) {
        let f = OpenOptions::new().write(true).open(path).unwrap();
        f.set_len(0).unwrap();
    }

    #[test]
    fn test_get_snap_key_from_file_name() {
        for name in &[
            "gen_1_2_3.meta",
            "gen_1_2_3_lock.sst",
            "rev_1_2_3.meta",
            "rev_1_2_3_default.sst",
        ] {
            let (for_send, key) = get_snap_key_from_file_name(name).unwrap();
            assert_eq!(for_send, name.starts_with("gen"));
            assert_eq!(key, SnapKey::new(1, 2, 3));
        }
    }

    #[test]
    fn test_get_meta_file_path() {
        let key = SnapKey::new(1, 2, 3);
        for &(dir, for_send, key, expected) in &[
            ("abc", true, key, "abc/gen_1_2_3.meta"),
            ("abc/", false, key, "abc/rev_1_2_3.meta"),
            ("ab/c", false, key, "ab/c/rev_1_2_3.meta"),
            ("", false, key, "rev_1_2_3.meta"),
        ] {
            let path = get_meta_file_path(dir, for_send, key);
            assert_eq!(path.to_str().unwrap(), expected);
        }
    }

    #[test]
    fn test_get_cf_file_path() {
        let key = SnapKey::new(1, 2, 3);
        for &(dir, for_send, key, cf, expected) in &[
            ("abc", true, key, CF_LOCK, "abc/gen_1_2_3_lock.sst"),
            ("abc/", false, key, CF_WRITE, "abc/rev_1_2_3_write.sst"),
            ("ab/c", false, key, CF_DEFAULT, "ab/c/rev_1_2_3_default.sst"),
            ("", false, key, CF_LOCK, "rev_1_2_3_lock.sst"),
        ] {
            let path = get_cf_file_path(dir, for_send, key, cf);
            assert_eq!(path.to_str().unwrap(), expected);
        }
    }

    fn test_registry(get_db: fn(p: &TempDir) -> Result<Arc<DB>>) {
        let region = get_test_region(1, 1, 1);
        let dir = TempDir::new("test-snap-file-db-src").unwrap();
        let db = get_db(&dir).unwrap();

        let snap_dir = TempDir::new("test-snap-file-src").unwrap();
        let snap_mgr = SnapManager::new(snap_dir.path().to_str().unwrap(), None);
        let key = SnapKey::new(1, 1, 1);

        let do_build = || {
            let r = region.clone();
            let db_snap = DbSnapshot::new(Arc::clone(&db));
            snap_mgr.build_snapshot(key, r, db_snap, new_snap_stale_notifier())
        };

        let do_apply = |should_success: bool| {
            let dst_db_dir = TempDir::new("test-snap-file-db-dst").unwrap();
            let dst_db_path = dst_db_dir.path().to_str().unwrap();
            let dst_db = Arc::new(rocksdb::new_engine(dst_db_path, ALL_CFS, None).unwrap());
            let apply_options = ApplyOptions {
                db: Arc::clone(&dst_db),
                region: region.clone(),
                write_batch_size: TEST_WRITE_BATCH_SIZE,
            };
            let notifier = new_snap_stale_notifier();
            let res = snap_mgr.apply_snapshot(key, apply_options, notifier);
            if should_success {
                assert!(res.is_ok());
                assert_eq_db(db.as_ref(), dst_db.as_ref());
            } else {
                assert!(res.is_err());
            }
        };

        // Generate snapshot from db should success.
        do_build().unwrap().unwrap();
        // Get it from registry directly should success.
        do_build().unwrap().unwrap();
        check_snap_size(&snap_mgr);

        // Load the snapshot from disk should success.
        snap_mgr.core.gen_registry.lock().unwrap().remove(&key);
        do_build().unwrap().unwrap();
        check_snap_size(&snap_mgr);

        // If the disk file is corrupted, the load should fail.
        snap_mgr.core.gen_registry.lock().unwrap().remove(&key);
        corrupt_file(&get_cf_file_path(&snap_mgr.core.base, true, key, CF_LOCK));
        assert!(do_build().unwrap().is_err());
        // The corrupted snapshot is deleted so that snap_size is 0.
        snap_mgr.core.snap_size.store(0, Ordering::SeqCst);
        check_snap_size(&snap_mgr);

        // Rebuild it should success.
        let snap = do_build().unwrap().unwrap();
        check_snap_size(&snap_mgr);

        // Get sender after build should success.
        let mut sender = snap_mgr.get_snapshot_sender(key).unwrap();

        let entry = snap_mgr
            .core
            .gen_registry
            .lock()
            .unwrap()
            .remove(&key)
            .unwrap();
        let ref_count = Arc::clone(&entry.ref_count);
        let sent_times = Arc::clone(&entry.sent_times);

        // Get sender before build should fail.
        assert!(snap_mgr.get_snapshot_sender(key).is_err());
        snap_mgr
            .core
            .gen_registry
            .lock()
            .unwrap()
            .insert(key, entry);

        let data = snap.write_to_bytes().unwrap();
        let mut receiver = snap_mgr.get_snapshot_receiver(key, &data).unwrap().unwrap();

        // Get receiver while receiving should fail.
        assert!(snap_mgr.get_snapshot_receiver(key, &data).is_err());

        // Get receiver and do receiving should success.
        io::copy(&mut sender, &mut receiver).unwrap();
        receiver.save().unwrap();
        check_snap_size(&snap_mgr);

        assert_eq!(ref_count.load(Ordering::SeqCst), 1);
        drop(sender);
        assert_eq!(ref_count.load(Ordering::SeqCst), 0);

        // Can't get the receiver because the snapshot is already received.
        assert!(
            snap_mgr
                .get_snapshot_receiver(key, &data)
                .unwrap()
                .is_none()
        );

        // Get receiver should success because disk files are corrupted.
        snap_mgr.core.apply_registry.lock().unwrap().remove(&key);
        corrupt_file(&get_cf_file_path(&snap_mgr.core.base, false, key, CF_LOCK));
        let mut receiver = snap_mgr.get_snapshot_receiver(key, &data).unwrap().unwrap();
        check_snap_size(&snap_mgr);

        // Send and receive it again.
        do_build().unwrap().unwrap();
        let mut sender = snap_mgr.get_snapshot_sender(key).unwrap();
        io::copy(&mut sender, &mut receiver).unwrap();
        receiver.save().unwrap();
        check_snap_size(&snap_mgr);

        // Check the gen_entry's status.
        assert_eq!(ref_count.load(Ordering::SeqCst), 1);
        drop(sender);
        assert_eq!(ref_count.load(Ordering::SeqCst), 0);
        assert_eq!(sent_times.load(Ordering::SeqCst), 2);

        // Apply snapshot should fail because no such entry in registry.
        let entry = snap_mgr
            .core
            .apply_registry
            .lock()
            .unwrap()
            .remove(&key)
            .unwrap();
        do_apply(false);

        // Apply snapshot should fail because it's still receiving.
        let mut entry1 = entry.clone();
        entry1.meta = None;
        snap_mgr
            .core
            .apply_registry
            .lock()
            .unwrap()
            .insert(key, entry1);
        do_apply(false);

        // Apply snapshot should success now.
        entry.applied.store(false, Ordering::SeqCst);
        snap_mgr
            .core
            .apply_registry
            .lock()
            .unwrap()
            .insert(key, entry.clone());
        do_apply(true);
        assert!(entry.applied.load(Ordering::SeqCst));
    }

    #[test]
    fn test_registry_with_empty_snapshot() {
        test_registry(get_test_empty_db);
    }

    #[test]
    fn test_registry_with_non_empty_snapshot() {
        test_registry(get_test_db);
    }

    #[test]
    fn test_snap_mgr_create_dir() {
        // Ensure `mgr` creates the specified directory when it does not exist.
        let temp_dir = TempDir::new("test-snap-mgr-create-dir").unwrap();
        let temp_path = temp_dir.path().join("snap1");
        let path = temp_path.to_str().unwrap().to_owned();
        assert!(!temp_path.exists());
        let mut mgr = SnapManager::new(path, None);
        mgr.init().unwrap();
        assert!(temp_path.exists());

        // Ensure `init()` will return an error if specified target is a file.
        let temp_path2 = temp_dir.path().join("snap2");
        let path2 = temp_path2.to_str().unwrap().to_owned();
        File::create(temp_path2).unwrap();
        mgr = SnapManager::new(path2, None);
        assert!(mgr.init().is_err());
    }
}
