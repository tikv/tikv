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

use std::collections::HashSet;
use std::fs::{self, File};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::mpsc::{sync_channel, Receiver, SyncSender, TryRecvError};
use std::sync::{Arc, Mutex, MutexGuard};
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
use util::time::Instant;
use util::transport::SendCh;
use util::Either;

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
            display("SnapError::Registry, exists: {}, new: {}", exists, new)
        }
        NoSpace(size: u64, limit: u64) {
            description("disk space exceed")
            display("SnapError::NoSpace, size: {}, limit: {}", size, limit)
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
    snap_size: Arc<AtomicU64>,
    max_total_size: u64,
    gc_timeout: Duration,
    sending_count: Arc<AtomicUsize>,
    receiving_count: Arc<AtomicUsize>,
    io_limiter: Option<Arc<IOLimiter>>,
    ch: Option<SendCh<Msg>>,
}

impl SnapManager {
    pub fn new<T: Into<String>>(path: T, ch: Option<SendCh<Msg>>) -> SnapManager {
        SnapManagerBuilder::default().build(path, ch)
    }

    pub fn get_total_snap_size(&self) -> u64 {
        self.snap_size.load(Ordering::SeqCst)
    }

    pub fn stats(&self) -> SnapStats {
        SnapStats {
            sending_count: self.sending_count.load(Ordering::SeqCst),
            receiving_count: self.receiving_count.load(Ordering::SeqCst),
        }
    }

    /// Inititalize the SnapManager, read snapshot metadatas from disk.
    pub fn init(&self) -> Result<()> {
        let path = Path::new(&self.core.base);
        if !path.exists() {
            fs::create_dir_all(path)?;
            return Ok(());
        }
        self.scan_meta_files(path)?;
        Ok(())
    }

    /// Build a snapshot with metadata in `RaftSnapshotData`.
    /// To get the readable snapshot, call `get_snapshot_sender`.
    pub fn build_snapshot(
        &self,
        key: SnapKey,
        region: Region,
        db_snap: DbSnapshot,
        snap_stale_notifier: Arc<SnapStaleNotifier>,
    ) -> Result<Option<RaftSnapshotData>> {
        let notifier = Arc::clone(&snap_stale_notifier);
        let sender = match self.register_for_build(true, key, Some(snap_stale_notifier)) {
            Either::Left(tx) => tx,
            Either::Right(Some(m)) => return Ok(Some(snapshot_data_from_meta(m, region))),
            Either::Right(None) => {
                warn!("{} build_snapshot multi times in building", key);
                return Ok(None);
            }
        };
        self.notify_stats();

        let mut meta = None;
        match snapshot_load(&self.core.base, true, key) {
            Ok(m) => meta = m,
            Err(e) => {
                error!("{} build_sanpshot fail when load: {}", key, e);
                self.delete_snapshot(true, key);
            }
        };

        if meta.is_none() {
            self.gc_snapshots_if_need()?;
            let dir = self.core.base.to_owned();
            let io_limiter = self.io_limiter.clone();
            let size = Arc::clone(&self.snap_size);
            match SnapshotGenerator::new(dir, key, io_limiter, notifier, sender, size)
                .and_then(|mut gen| gen.build(&region, db_snap))
            {
                Ok(m) => meta = Some(m),
                Err(e) => {
                    error!("{} build_snapshot fail when generate: {}", key, e);
                    return Err(e);
                }
            }
        }

        Ok(meta.map(|m| snapshot_data_from_meta(m, region)))
    }

    pub fn get_snapshot_sender(&self, key: SnapKey) -> Result<SnapshotSender> {
        let mut registry = self.get_registry(true);
        if let Some(entry) = registry.get_mut(&key) {
            if let Some(meta) = entry.fetch_meta(&mut false) {
                let dir = self.core.base.clone();
                let notifier = entry.snap_stale_notifier.clone().unwrap();
                let ref_count = Arc::clone(&entry.ref_count);
                let used_times = Arc::clone(&entry.used_times);
                let s = SnapshotSender::new(dir, key, meta, notifier, ref_count, used_times);
                return Ok(s);
            }
        }
        error!("{} get_snapshot_sender without avaliable snapshot", key);
        Err(Error::Snapshot(SnapError::Registry("none", "send")))
    }

    pub fn get_snapshot_receiver(
        &self,
        key: SnapKey,
        data: &[u8],
    ) -> Result<Option<SnapshotReceiver>> {
        let meta = snapshot_meta_from_data(data)?;
        self.gc_snapshots_if_need()?;

        let sender = match self.register_for_build(false, key, None) {
            Either::Left(tx) => tx,
            Either::Right(Some(_)) => return Ok(None),
            Either::Right(None) => {
                warn!("{} get_snapshot_receiver multi times", key);
                return Err(Error::Snapshot(SnapError::Registry("receive", "receive")));
            }
        };
        self.notify_stats();

        match snapshot_load(&self.core.base, false, key) {
            Ok(Some(_)) => return Ok(None),
            Ok(None) => {}
            Err(e) => {
                error!("{} get_snapshot_receiver fail when load: {}", key, e);
                self.delete_snapshot(false, key);
            }
        };

        let dir = self.core.base.clone();
        let io_limiter = self.io_limiter.clone();
        let snap_size = Arc::clone(&self.snap_size);
        SnapshotReceiver::new(dir, key, io_limiter, meta, sender, snap_size).map(Some)
    }

    pub fn apply_snapshot(
        &self,
        key: SnapKey,
        options: ApplyOptions,
        notifier: Arc<SnapStaleNotifier>,
    ) -> Result<()> {
        let mut applyer = None;
        if let Some(entry) = self.get_registry(false).get_mut(&key) {
            if let Some(meta) = entry.fetch_meta(&mut false) {
                let dir = self.core.base.clone();
                let rc = Arc::clone(&entry.ref_count);
                let ut = Arc::clone(&entry.used_times);
                applyer = Some(SnapshotApplyer::new(dir, key, meta, notifier, rc, ut))
            }
        }
        match applyer {
            Some(applyer) => applyer.apply(options),
            None => {
                error!("{} apply_snapshot without avaliable snapshot", key);
                Err(Error::Snapshot(SnapError::Registry("none", "apply")))
            }
        }
    }

    fn scan_meta_files(&self, path: &Path) -> io::Result<()> {
        for p in fs::read_dir(path)?
            .filter_map(|p| p.ok())
            .filter(|p| p.file_type().map(|ft| ft.is_file()).unwrap_or(false))
        {
            if let Some(s) = p.file_name().to_str() {
                let path = p.path();
                if s.ends_with(TMP_FILE_SUFFIX) || s.ends_with(CLONE_FILE_SUFFIX) {
                    delete_file_if_exist(&path);
                } else if s.ends_with(META_FILE_SUFFIX) {
                    if let Ok(snap_meta) = read_snapshot_meta(&path) {
                        // The snapshot can be deleted during gc so that it's ok to add
                        // it into snap_size even if the cf files could be corrupted.
                        let size = get_size_from_snapshot_meta(&snap_meta);
                        self.snap_size.fetch_add(size, Ordering::SeqCst);
                    } else {
                        delete_file_if_exist(&path);
                    }
                }
            }
        }
        Ok(())
    }

    fn get_registry(&self, for_send: bool) -> MutexGuard<HashMap<SnapKey, SnapEntry>> {
        if for_send {
            self.core.gen_registry.lock().unwrap()
        } else {
            self.core.apply_registry.lock().unwrap()
        }
    }

    fn register_for_build(
        &self,
        for_send: bool,
        key: SnapKey,
        snap_stale_notifier: Option<Arc<SnapStaleNotifier>>,
    ) -> Either<SyncSender<SnapshotMeta>, Option<SnapshotMeta>> {
        let mut registry = self.get_registry(for_send);
        if let Some(entry) = registry.get_mut(&key) {
            let mut could_retry = false;
            match entry.fetch_meta(&mut could_retry) {
                Some(meta) => return Either::Right(Some(meta)),
                None => if could_retry {
                    let (tx, rx) = sync_channel(1);
                    entry.meta = Either::Left(rx);
                    return Either::Left(tx);
                } else {
                    return Either::Right(None);
                },
            }
        }
        let (tx, rx) = sync_channel(1);
        let entry = SnapEntry::new(snap_stale_notifier, rx);
        registry.insert(key, entry);

        if for_send {
            self.sending_count.fetch_add(1, Ordering::SeqCst);
        } else {
            self.receiving_count.fetch_add(1, Ordering::SeqCst);
        }
        Either::Left(tx)
    }

    fn deregister(&self, for_send: bool, key: SnapKey) -> Option<SnapEntry> {
        let res = self.get_registry(for_send).remove(&key);
        if res.is_some() {
            let old_value = if for_send {
                self.sending_count.fetch_sub(1, Ordering::SeqCst)
            } else {
                self.receiving_count.fetch_sub(1, Ordering::SeqCst)
            };
            assert!(old_value >= 1);
        }
        res
    }

    /// Gc snapshots which meet the following conditions:
    /// 1) Not in the register, which means it's not genearted in the current start up.
    /// 2) Generated and have been used, and is not busy.
    pub fn gc_snapshots(&self) -> Result<()> {
        let snap_size = self.snap_size.load(Ordering::SeqCst);
        info!("starting gc snapshots, total size: {}", snap_size);
        let t = Instant::now_coarse();

        let mut removed = 0;
        let mut snap_keys = HashSet::<(bool, SnapKey)>::new();
        for p in fs::read_dir(&self.core.base)?
            .filter_map(|p| p.ok())
            .filter(|p| p.file_type().map(|ft| ft.is_file()).unwrap_or(false))
        {
            if let Some(s) = p.file_name().to_str() {
                if s.ends_with(META_FILE_SUFFIX) || s.ends_with(SST_FILE_SUFFIX) {
                    if let Some((for_send, snap_key)) = get_snap_key_from_file_name(s) {
                        snap_keys.insert((for_send, snap_key));
                    }
                }
            }
        }

        for (for_send, key) in snap_keys {
            if let Some(entry) = self.get_registry(for_send).get_mut(&key) {
                if entry.check_is_busy()
                    || (!entry.has_been_used()
                        && !self.snapshot_is_stale(for_send, key).unwrap_or(true))
                {
                    continue;
                }
            }
            self.delete_snapshot(for_send, key);
            self.deregister(for_send, key);
            removed += 1;
        }

        let snap_size = self.snap_size.load(Ordering::SeqCst);
        info!(
            "gc snapshots success in {:?}, removed: {}, total size: {}",
            t.elapsed(),
            removed,
            snap_size
        );
        Ok(())
    }

    fn gc_snapshots_if_need(&self) -> Result<()> {
        if self.max_total_size > 0 && self.snap_size.load(Ordering::SeqCst) > self.max_total_size {
            self.gc_snapshots()?;
            let size = self.snap_size.load(Ordering::SeqCst);
            let limit = self.max_total_size;
            if size > limit {
                return Err(Error::Snapshot(SnapError::NoSpace(size, limit)));
            }
        }
        Ok(())
    }

    fn snapshot_is_stale(&self, for_send: bool, key: SnapKey) -> Result<bool> {
        let meta_path = get_meta_file_path(&self.core.base, for_send, key);
        let modified = fs::metadata(&meta_path)?.modified()?;
        SystemTime::now()
            .duration_since(modified)
            .map(|dur| dur > self.gc_timeout)
            .map_err(|e| box_err!("get modified time fail: {}", e))
    }

    fn delete_snapshot(&self, for_send: bool, key: SnapKey) {
        let meta_path = get_meta_file_path(&self.core.base, for_send, key);
        if let Ok(meta) = read_snapshot_meta(&meta_path) {
            let size = get_size_from_snapshot_meta(&meta);
            if delete_file_if_exist(&meta_path) {
                let old_size = self.snap_size.fetch_sub(size, Ordering::SeqCst);
                assert!(old_size >= size);
            }
        }

        for cf in SNAPSHOT_CFS {
            let cf_path = get_cf_file_path(&self.core.base, for_send, key, cf);
            delete_file_if_exist(&cf_path);
        }
    }

    fn notify_stats(&self) {
        if let Some(ch) = self.ch.as_ref() {
            if let Err(e) = ch.try_send(Msg::SnapshotStats) {
                error!("notify snapshot stats failed {:?}", e)
            }
        }
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
            }),
            snap_size: Arc::new(AtomicU64::new(0)),
            max_total_size: self.max_total_size,
            gc_timeout: self.gc_timeout,
            sending_count: Arc::new(AtomicUsize::new(0)),
            receiving_count: Arc::new(AtomicUsize::new(0)),
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

struct SnapEntry {
    snap_stale_notifier: Option<Arc<SnapStaleNotifier>>,
    meta: Either<Receiver<SnapshotMeta>, SnapshotMeta>,
    // ref_count and used_times are used for gc.
    ref_count: Arc<AtomicUsize>,
    used_times: Arc<AtomicUsize>,
}

impl SnapEntry {
    fn new(notifier: Option<Arc<SnapStaleNotifier>>, r: Receiver<SnapshotMeta>) -> Self {
        SnapEntry {
            snap_stale_notifier: notifier,
            meta: Either::Left(r),
            ref_count: Arc::new(AtomicUsize::new(0)),
            used_times: Arc::new(AtomicUsize::new(0)),
        }
    }

    fn fetch_meta(&mut self, could_retry: &mut bool) -> Option<SnapshotMeta> {
        let meta = match self.meta {
            Either::Left(ref r) => match r.try_recv() {
                Ok(meta) => meta,
                Err(TryRecvError::Empty) => return None,
                Err(TryRecvError::Disconnected) => {
                    *could_retry = true;
                    return None;
                }
            },
            Either::Right(ref meta) => return Some(meta.clone()),
        };
        self.meta = Either::Right(meta.clone());
        Some(meta)
    }

    fn check_is_busy(&mut self) -> bool {
        let mut could_retry = false;
        // It's still in building.
        (self.fetch_meta(&mut could_retry).is_none() && !could_retry)
            // Or it's currently used for sending or applying.
            || self.ref_count.load(Ordering::SeqCst) > 0
    }

    fn has_been_used(&self) -> bool {
        self.used_times.load(Ordering::SeqCst) > 0
    }
}

struct SnapManagerCore {
    base: String,
    gen_registry: Mutex<HashMap<SnapKey, SnapEntry>>,
    apply_registry: Mutex<HashMap<SnapKey, SnapEntry>>,
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

fn snapshot_load(dir: &str, for_send: bool, key: SnapKey) -> Result<Option<SnapshotMeta>> {
    let meta_path = get_meta_file_path(dir, for_send, key);
    if let Ok(meta) = read_snapshot_meta(&meta_path) {
        return SnapshotLoader::new(dir.to_owned(), for_send, key, meta)
            .load()
            .map(Some);
    }
    Ok(None)
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
    use std::sync::atomic::{AtomicBool, AtomicU64};
    use test::Bencher;

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

    pub fn get_test_region(region_id: u64, store_id: u64, peer_id: u64) -> Region {
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

    pub fn get_test_empty_db(path: &TempDir) -> Result<Arc<DB>> {
        let p = path.path().to_str().unwrap();
        let db = rocksdb::new_engine(p, ALL_CFS, None)?;
        Ok(Arc::new(db))
    }

    pub fn get_test_db(path: &TempDir) -> Result<Arc<DB>> {
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
        let snap_size = snap_mgr.snap_size.load(Ordering::SeqCst);
        let total_size = sst_files_size(PathBuf::from(&snap_mgr.core.base)).unwrap();
        assert_eq!(snap_size, total_size);
    }

    pub fn new_snap_stale_notifier() -> Arc<SnapStaleNotifier> {
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
        let dir = TempDir::new("test-snap-mgr-registry-db-src").unwrap();
        let db = get_db(&dir).unwrap();

        let snap_dir = TempDir::new("test-snap-mgr-registry-src").unwrap();
        let snap_mgr = SnapManager::new(snap_dir.path().to_str().unwrap(), None);
        let key = SnapKey::new(1, 1, 1);

        let do_build = || {
            let r = region.clone();
            let db_snap = DbSnapshot::new(Arc::clone(&db));
            snap_mgr.build_snapshot(key, r, db_snap, new_snap_stale_notifier())
        };

        let do_apply = |should_success: bool| {
            let dst_db_dir = TempDir::new("test-snap-mgr-registry-db-dst").unwrap();
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
        snap_mgr.deregister(true, key);
        do_build().unwrap().unwrap();
        check_snap_size(&snap_mgr);

        // If the disk file is corrupted, the snapshot will be rebuilt after load fail.
        snap_mgr.deregister(true, key);
        corrupt_file(&get_cf_file_path(&snap_mgr.core.base, true, key, CF_LOCK));
        assert!(do_build().unwrap().is_some());
        check_snap_size(&snap_mgr);

        // Rebuild it should success.
        let snap = do_build().unwrap().unwrap();
        check_snap_size(&snap_mgr);

        // Get sender after build should success.
        let mut sender = snap_mgr.get_snapshot_sender(key).unwrap();

        // Get sender before build should fail.
        let entry = snap_mgr.deregister(true, key).unwrap();
        assert!(snap_mgr.get_snapshot_sender(key).is_err());
        snap_mgr.get_registry(true).insert(key, entry);

        let data = snap.write_to_bytes().unwrap();
        let mut receiver = snap_mgr.get_snapshot_receiver(key, &data).unwrap().unwrap();

        // Get receiver while receiving should fail.
        assert!(snap_mgr.get_snapshot_receiver(key, &data).is_err());

        // Get receiver and do receiving should success.
        io::copy(&mut sender, &mut receiver).unwrap();
        receiver.save().unwrap();
        check_snap_size(&snap_mgr);

        let ref_count = Arc::clone(&snap_mgr.get_registry(true)[&key].ref_count);
        let used_times = Arc::clone(&snap_mgr.get_registry(true)[&key].used_times);
        assert_eq!(ref_count.load(Ordering::SeqCst), 1);
        drop(sender);
        assert_eq!(ref_count.load(Ordering::SeqCst), 0);
        assert_eq!(used_times.load(Ordering::SeqCst), 1);

        // Can't get the receiver because the snapshot is already received.
        let r = snap_mgr.get_snapshot_receiver(key, &data);
        assert!(r.unwrap().is_none());

        // Get receiver should success because disk files are corrupted.
        snap_mgr.deregister(false, key);
        corrupt_file(&get_cf_file_path(&snap_mgr.core.base, false, key, CF_LOCK));
        let mut receiver = snap_mgr.get_snapshot_receiver(key, &data).unwrap().unwrap();
        check_snap_size(&snap_mgr);

        // Send and receive it again.
        do_build().unwrap().unwrap();
        let mut sender = snap_mgr.get_snapshot_sender(key).unwrap();
        io::copy(&mut sender, &mut receiver).unwrap();
        receiver.save().unwrap();
        check_snap_size(&snap_mgr);

        // Apply snapshot should fail because no such entry in registry.
        let entry = snap_mgr.deregister(false, key).unwrap();
        do_apply(false);

        // Apply snapshot should fail because it's still receiving.
        let (_, rx) = sync_channel(1);
        let bad_entry = SnapEntry {
            snap_stale_notifier: entry.snap_stale_notifier.clone(),
            meta: Either::Left(rx),
            ref_count: Arc::clone(&entry.ref_count),
            used_times: Arc::clone(&entry.used_times),
        };
        snap_mgr.get_registry(false).insert(key, bad_entry);
        do_apply(false);

        // Apply snapshot should success now.
        let used_times = Arc::clone(&entry.used_times);
        snap_mgr.get_registry(false).insert(key, entry);
        do_apply(true);
        assert!(used_times.load(Ordering::SeqCst) > 0);
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

    #[test]
    fn test_snap_mgr_disk_usage() {
        let snap_dir = TempDir::new("test-snap-mgr-disk-usage-src").unwrap();
        let mut snap_mgr = SnapManager::new(snap_dir.path().to_str().unwrap(), None);
        // Set it to 1000 so that we can only keep 1 snapshot at most on disk.
        snap_mgr.max_total_size = 1000;
        snap_mgr.gc_timeout = Duration::from_secs(4 * 60 * 60); // default 4 hours.

        let dir = TempDir::new("test-snap-mgr-disk-usage-db-src").unwrap();
        let db = get_test_db(&dir).unwrap();
        let region = get_test_region(1, 1, 1);
        let key = SnapKey::new(1, 1, 1);

        let db_snap = DbSnapshot::new(Arc::clone(&db));
        let snap_data = snap_mgr
            .build_snapshot(key, region.clone(), db_snap, new_snap_stale_notifier())
            .unwrap()
            .unwrap();
        // The snapshot size should be 1918.
        assert_eq!(get_size_from_snapshot_meta(snap_data.get_meta()), 1918);
        assert_eq!(snap_mgr.snap_size.load(Ordering::SeqCst), 1918);

        let data = snap_data.write_to_bytes().unwrap();
        match snap_mgr.get_snapshot_receiver(key, &data) {
            Ok(_) => panic!("should fail"),
            // Can't receive it because can't gc snapshots generated but not sent.
            Err(e) => assert!(format!("{}", e).contains("NoSpace")),
        }

        let meta_path = get_meta_file_path(&snap_mgr.core.base, true, key);
        let ensure_meta_file_exists = || {
            let mut f = OpenOptions::new()
                .create(true)
                .write(true)
                .truncate(true)
                .open(&meta_path)
                .unwrap();
            snap_data.get_meta().write_to_writer(&mut f).unwrap();
        };

        let mut entry = snap_mgr.deregister(true, key).unwrap();
        let entry_meta = entry.fetch_meta(&mut false).unwrap();
        let clone_entry = || SnapEntry {
            snap_stale_notifier: None,
            meta: Either::Right(entry_meta.clone()),
            ref_count: Arc::clone(&entry.ref_count),
            used_times: Arc::clone(&entry.used_times),
        };

        // Can gc it if it's not registered.
        ensure_meta_file_exists();
        snap_mgr.snap_size.store(1918, Ordering::SeqCst);
        assert!(snap_mgr.get_snapshot_receiver(key, &data).is_ok());
        assert!(File::open(&meta_path).is_err());

        // Can gc it if it's used.
        ensure_meta_file_exists();
        snap_mgr.snap_size.store(1918, Ordering::SeqCst);
        let new_entry = clone_entry();
        new_entry.used_times.store(1, Ordering::SeqCst);
        snap_mgr.get_registry(true).insert(key, new_entry);
        snap_mgr.sending_count.fetch_add(1, Ordering::SeqCst);
        assert!(snap_mgr.get_snapshot_receiver(key, &data).is_ok());
        assert!(File::open(&meta_path).is_err());

        // Can gc it if it's stale.
        ensure_meta_file_exists();
        snap_mgr.snap_size.store(1918, Ordering::SeqCst);
        snap_mgr.gc_timeout = Duration::default();
        let new_entry = clone_entry();
        new_entry.used_times.store(1918, Ordering::SeqCst);
        snap_mgr.get_registry(true).insert(key, new_entry);
        snap_mgr.sending_count.fetch_add(1, Ordering::SeqCst);
        assert!(snap_mgr.get_snapshot_receiver(key, &data).is_ok());
        assert!(File::open(&meta_path).is_err());
    }

    #[bench]
    fn bench_stale_for_generate(b: &mut Bencher) {
        let notifier = SnapStaleNotifier {
            compacted_term: AtomicU64::new(0),
            compacted_idx: AtomicU64::new(0),
            apply_canceled: AtomicBool::new(false),
        };
        let key = SnapKey::new(10, 10, 10);
        b.iter(|| stale_for_generate(key, &notifier));
    }

    #[bench]
    fn bench_stale_for_apply(b: &mut Bencher) {
        let notifier = Arc::new(SnapStaleNotifier {
            compacted_term: AtomicU64::new(0),
            compacted_idx: AtomicU64::new(0),
            apply_canceled: AtomicBool::new(false),
        });
        let key = SnapKey::new(10, 10, 10);
        b.iter(|| stale_for_apply(key, notifier.as_ref()));
    }
}
