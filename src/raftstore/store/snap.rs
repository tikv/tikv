// Copyright 2017 TiKV Project Authors. Licensed under Apache-2.0.

use std::cmp::Reverse;
use std::fs;
use std::io::{self, ErrorKind};
use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, RwLock};
use std::u64;

use engine::rocks::Snapshot as DbSnapshot;
use kvproto::raft_serverpb::RaftSnapshotData;
use protobuf::Message;

use raftstore2::errors::Error as RaftStoreError;
use crate::raftstore::store::{RaftRouter, StoreMsg};
use raftstore2::Result as RaftStoreResult;
use engine::rocks::util::io_limiter::IOLimiter;
use tikv_util::collections::{HashMap, HashMapEntry as Entry};
use tikv_util::HandyRwLock;

pub use raftstore2::store::snap::{Error, Result};

pub use raftstore2::store::snap::{
    SNAPSHOT_CFS,
    SNAPSHOT_VERSION,
    SNAP_GEN_PREFIX,
    TMP_FILE_SUFFIX,
    SST_FILE_SUFFIX,
    plain_file_used,
    check_abort,
    SnapKey,
    SnapshotStatistics,
    ApplyOptions,
    Snapshot,
    copy_snapshot,
    SnapshotDeleter,
    Snap,
    SnapEntry,
    SnapStats,
};

struct SnapManagerCore {
    base: String,
    registry: HashMap<SnapKey, Vec<SnapEntry>>,
    snap_size: Arc<AtomicU64>,
}

fn notify_stats(ch: Option<&RaftRouter>) {
    if let Some(ch) = ch {
        if let Err(e) = ch.send_control(StoreMsg::SnapshotStats) {
            error!(
                "failed to notify snapshot stats";
                "err" => ?e,
            )
        }
    }
}

/// `SnapManagerCore` trace all current processing snapshots.
#[derive(Clone)]
pub struct SnapManager {
    // directory to store snapfile.
    core: Arc<RwLock<SnapManagerCore>>,
    router: Option<RaftRouter>,
    limiter: Option<Arc<IOLimiter>>,
    max_total_size: u64,
}

impl SnapManager {
    pub fn new<T: Into<String>>(path: T, router: Option<RaftRouter>) -> SnapManager {
        SnapManagerBuilder::default().build(path, router)
    }

    pub fn init(&self) -> io::Result<()> {
        // Use write lock so only one thread initialize the directory at a time.
        let core = self.core.wl();
        let path = Path::new(&core.base);
        if !path.exists() {
            fs::create_dir_all(path)?;
            return Ok(());
        }
        if !path.is_dir() {
            return Err(io::Error::new(
                ErrorKind::Other,
                format!("{} should be a directory", path.display()),
            ));
        }
        for f in fs::read_dir(path)? {
            let p = f?;
            if p.file_type()?.is_file() {
                if let Some(s) = p.file_name().to_str() {
                    if s.ends_with(TMP_FILE_SUFFIX) {
                        fs::remove_file(p.path())?;
                    } else if s.ends_with(SST_FILE_SUFFIX) {
                        let len = p.metadata()?.len();
                        core.snap_size.fetch_add(len, Ordering::SeqCst);
                    }
                }
            }
        }
        Ok(())
    }

    // Return all snapshots which is idle not being used.
    pub fn list_idle_snap(&self) -> io::Result<Vec<(SnapKey, bool)>> {
        let core = self.core.rl();
        let path = Path::new(&core.base);
        let read_dir = fs::read_dir(path)?;
        // Remove the duplicate snap keys.
        let mut v: Vec<_> = read_dir
            .filter_map(|p| {
                let p = match p {
                    Err(e) => {
                        error!(
                            "failed to list content of directory";
                            "directory" => %core.base,
                            "err" => ?e,
                        );
                        return None;
                    }
                    Ok(p) => p,
                };
                match p.file_type() {
                    Ok(t) if t.is_file() => {}
                    _ => return None,
                }
                let file_name = p.file_name();
                let name = match file_name.to_str() {
                    None => return None,
                    Some(n) => n,
                };
                let is_sending = name.starts_with(SNAP_GEN_PREFIX);
                let numbers: Vec<u64> = name.split('.').next().map_or_else(
                    || vec![],
                    |s| {
                        s.split('_')
                            .skip(1)
                            .filter_map(|s| s.parse().ok())
                            .collect()
                    },
                );
                if numbers.len() != 3 {
                    error!(
                        "failed to parse snapkey";
                        "snap_key" => %name,
                    );
                    return None;
                }
                let snap_key = SnapKey::new(numbers[0], numbers[1], numbers[2]);
                if core.registry.contains_key(&snap_key) {
                    // Skip those registered snapshot.
                    return None;
                }
                Some((snap_key, is_sending))
            })
            .collect();
        v.sort();
        v.dedup();
        Ok(v)
    }

    #[inline]
    pub fn has_registered(&self, key: &SnapKey) -> bool {
        self.core.rl().registry.contains_key(key)
    }

    pub fn get_snapshot_for_building(
        &self,
        key: &SnapKey,
        snap: &DbSnapshot,
    ) -> RaftStoreResult<Box<dyn Snapshot>> {
        let mut old_snaps = None;
        while self.get_total_snap_size() > self.max_total_snap_size() {
            if old_snaps.is_none() {
                let snaps = self.list_idle_snap()?;
                let mut key_and_snaps = Vec::with_capacity(snaps.len());
                for (key, is_sending) in snaps {
                    if !is_sending {
                        continue;
                    }
                    let snap = match self.get_snapshot_for_sending(&key) {
                        Ok(snap) => snap,
                        Err(_) => continue,
                    };
                    if let Ok(modified) = snap.meta().and_then(|m| m.modified()) {
                        key_and_snaps.push((key, snap, modified));
                    }
                }
                key_and_snaps.sort_by_key(|&(_, _, modified)| Reverse(modified));
                old_snaps = Some(key_and_snaps);
            }
            match old_snaps.as_mut().unwrap().pop() {
                Some((key, snap, _)) => self.delete_snapshot(&key, snap.as_ref(), false),
                None => return Err(RaftStoreError::Snapshot(Error::TooManySnapshots)),
            };
        }

        let (dir, snap_size) = {
            let core = self.core.rl();
            (core.base.clone(), Arc::clone(&core.snap_size))
        };
        let f = Snap::new_for_building(
            dir,
            key,
            snap,
            snap_size,
            Box::new(self.clone()),
            self.limiter.clone(),
        )?;
        Ok(Box::new(f))
    }

    pub fn get_snapshot_for_sending(&self, key: &SnapKey) -> RaftStoreResult<Box<dyn Snapshot>> {
        let core = self.core.rl();
        let s = Snap::new_for_sending(
            &core.base,
            key,
            Arc::clone(&core.snap_size),
            Box::new(self.clone()),
        )?;
        Ok(Box::new(s))
    }

    pub fn get_snapshot_for_receiving(
        &self,
        key: &SnapKey,
        data: &[u8],
    ) -> RaftStoreResult<Box<dyn Snapshot>> {
        let core = self.core.rl();
        let mut snapshot_data = RaftSnapshotData::new();
        snapshot_data.merge_from_bytes(data)?;
        let f = Snap::new_for_receiving(
            &core.base,
            key,
            snapshot_data.take_meta(),
            Arc::clone(&core.snap_size),
            Box::new(self.clone()),
            self.limiter.clone(),
        )?;
        Ok(Box::new(f))
    }

    pub fn get_snapshot_for_applying(&self, key: &SnapKey) -> RaftStoreResult<Box<dyn Snapshot>> {
        let core = self.core.rl();
        let s = Snap::new_for_applying(
            &core.base,
            key,
            Arc::clone(&core.snap_size),
            Box::new(self.clone()),
        )?;
        if !s.exists() {
            return Err(RaftStoreError::Other(From::from(
                format!("snapshot of {:?} not exists.", key).to_string(),
            )));
        }
        Ok(Box::new(s))
    }

    /// Get the approximate size of snap file exists in snap directory.
    ///
    /// Return value is not guaranteed to be accurate.
    pub fn get_total_snap_size(&self) -> u64 {
        let core = self.core.rl();
        core.snap_size.load(Ordering::SeqCst)
    }

    pub fn max_total_snap_size(&self) -> u64 {
        self.max_total_size
    }

    pub fn register(&self, key: SnapKey, entry: SnapEntry) {
        debug!(
            "register snapshot";
            "key" => %key,
            "entry" => ?entry,
        );
        let mut core = self.core.wl();
        match core.registry.entry(key) {
            Entry::Occupied(mut e) => {
                if e.get().contains(&entry) {
                    warn!(
                        "snap key is registered more than once!";
                        "key" => %e.key(),
                    );
                    return;
                }
                e.get_mut().push(entry);
            }
            Entry::Vacant(e) => {
                e.insert(vec![entry]);
            }
        }

        notify_stats(self.router.as_ref());
    }

    pub fn deregister(&self, key: &SnapKey, entry: &SnapEntry) {
        debug!(
            "deregister snapshot";
            "key" => %key,
            "entry" => ?entry,
        );
        let mut need_clean = false;
        let mut handled = false;
        let mut core = self.core.wl();
        if let Some(e) = core.registry.get_mut(key) {
            let last_len = e.len();
            e.retain(|e| e != entry);
            need_clean = e.is_empty();
            handled = last_len > e.len();
        }
        if need_clean {
            core.registry.remove(key);
        }
        if handled {
            notify_stats(self.router.as_ref());
            return;
        }
        warn!(
            "stale deregister snapshot";
            "key" => %key,
            "entry" => ?entry,
        );
    }

    pub fn stats(&self) -> SnapStats {
        let core = self.core.rl();
        // send_count, generating_count, receiving_count, applying_count
        let (mut sending_cnt, mut receiving_cnt) = (0, 0);
        for v in core.registry.values() {
            let (mut is_sending, mut is_receiving) = (false, false);
            for s in v {
                match *s {
                    SnapEntry::Sending | SnapEntry::Generating => is_sending = true,
                    SnapEntry::Receiving | SnapEntry::Applying => is_receiving = true,
                }
            }
            if is_sending {
                sending_cnt += 1;
            }
            if is_receiving {
                receiving_cnt += 1;
            }
        }

        SnapStats {
            sending_count: sending_cnt,
            receiving_count: receiving_cnt,
        }
    }
}

impl SnapshotDeleter for SnapManager {
    fn delete_snapshot(&self, key: &SnapKey, snap: &dyn Snapshot, check_entry: bool) -> bool {
        let core = self.core.rl();
        if check_entry {
            if let Some(e) = core.registry.get(key) {
                if e.len() > 1 {
                    info!(
                        "skip to delete snapshot since it's registered more than once";
                        "snapshot" => %snap.path(),
                        "registered_entries" => ?e,
                    );
                    return false;
                }
            }
        } else if core.registry.contains_key(key) {
            info!(
                "skip to delete snapshot since it's registered";
                "snapshot" => %snap.path(),
            );
            return false;
        }
        snap.delete();
        true
    }
}

#[derive(Debug, Default)]
pub struct SnapManagerBuilder {
    max_write_bytes_per_sec: u64,
    max_total_size: u64,
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
    pub fn build<T: Into<String>>(&self, path: T, router: Option<RaftRouter>) -> SnapManager {
        let limiter = if self.max_write_bytes_per_sec > 0 {
            Some(Arc::new(IOLimiter::new(self.max_write_bytes_per_sec)))
        } else {
            None
        };
        let max_total_size = if self.max_total_size > 0 {
            self.max_total_size
        } else {
            u64::MAX
        };
        SnapManager {
            core: Arc::new(RwLock::new(SnapManagerCore {
                base: path.into(),
                registry: map![],
                snap_size: Arc::new(AtomicU64::new(0)),
            })),
            router,
            limiter,
            max_total_size,
        }
    }
}

#[cfg(test)]
pub mod tests {
    use std::cmp;
    use std::fs::{self, File, OpenOptions};
    use std::io::{self, Read, Seek, SeekFrom, Write};
    use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
    use std::sync::Arc;

    use engine::rocks;
    use engine::rocks::util::CFOptions;
    use engine::rocks::{DBOptions, Env, DB};
    use engine::{Iterable, Mutable, Peekable, Snapshot as DbSnapshot};
    use engine::{ALL_CFS, CF_DEFAULT, CF_LOCK, CF_RAFT, CF_WRITE};
    use kvproto::metapb::{Peer, Region};
    use kvproto::raft_serverpb::{
        RaftApplyState, RaftSnapshotData, RegionLocalState, SnapshotMeta,
    };
    use protobuf::Message;
    use std::path::PathBuf;
    use tempdir::TempDir;

    use super::{
        ApplyOptions, Snap, SnapEntry, SnapKey, SnapManager, SnapManagerBuilder, Snapshot,
        SnapshotDeleter, SnapshotStatistics, META_FILE_SUFFIX, SNAPSHOT_CFS, SNAP_GEN_PREFIX,
    };

    use crate::raftstore::store::keys;
    use crate::raftstore::store::peer_storage::JOB_STATUS_RUNNING;
    use crate::raftstore::Result;

    const TEST_STORE_ID: u64 = 1;
    const TEST_KEY: &[u8] = b"akey";
    const TEST_WRITE_BATCH_SIZE: usize = 10 * 1024 * 1024;
    const TEST_META_FILE_BUFFER_SIZE: usize = 1000;
    const BYTE_SIZE: usize = 1;

    #[derive(Clone)]
    struct DummyDeleter;
    type DBBuilder = fn(
        p: &TempDir,
        db_opt: Option<DBOptions>,
        cf_opts: Option<Vec<CFOptions<'_>>>,
    ) -> Result<Arc<DB>>;

    impl SnapshotDeleter for DummyDeleter {
        fn delete_snapshot(&self, _: &SnapKey, snap: &dyn Snapshot, _: bool) -> bool {
            snap.delete();
            true
        }
    }

    pub fn open_test_empty_db(
        path: &TempDir,
        db_opt: Option<DBOptions>,
        cf_opts: Option<Vec<CFOptions<'_>>>,
    ) -> Result<Arc<DB>> {
        let p = path.path().to_str().unwrap();
        let db = rocks::util::new_engine(p, db_opt, ALL_CFS, cf_opts)?;
        Ok(Arc::new(db))
    }

    pub fn open_test_db(
        path: &TempDir,
        db_opt: Option<DBOptions>,
        cf_opts: Option<Vec<CFOptions<'_>>>,
    ) -> Result<Arc<DB>> {
        let p = path.path().to_str().unwrap();
        let db = rocks::util::new_engine(p, db_opt, ALL_CFS, cf_opts)?;
        let key = keys::data_key(TEST_KEY);
        // write some data into each cf
        for (i, cf) in ALL_CFS.iter().enumerate() {
            let handle = rocks::util::get_cf_handle(&db, cf)?;
            let mut p = Peer::new();
            p.set_store_id(TEST_STORE_ID);
            p.set_id((i + 1) as u64);
            db.put_msg_cf(handle, &key[..], &p)?;
        }
        Ok(Arc::new(db))
    }

    pub fn get_test_db_for_regions(
        path: &TempDir,
        db_opt: Option<DBOptions>,
        cf_opts: Option<Vec<CFOptions<'_>>>,
        regions: &[u64],
    ) -> Result<Arc<DB>> {
        let kv = open_test_db(path, db_opt, cf_opts)?;
        for &region_id in regions {
            // Put apply state into kv engine.
            let mut apply_state = RaftApplyState::new();
            apply_state.set_applied_index(10);
            apply_state.mut_truncated_state().set_index(10);
            let handle = rocks::util::get_cf_handle(&kv, CF_RAFT)?;
            kv.put_msg_cf(handle, &keys::apply_state_key(region_id), &apply_state)?;

            // Put region info into kv engine.
            let region = gen_test_region(region_id, 1, 1);
            let mut region_state = RegionLocalState::new();
            region_state.set_region(region);
            let handle = rocks::util::get_cf_handle(&kv, CF_RAFT)?;
            kv.put_msg_cf(handle, &keys::region_state_key(region_id), &region_state)?;
        }
        Ok(kv)
    }

    pub fn get_kv_count(snap: &DbSnapshot) -> usize {
        let mut kv_count = 0;
        for cf in SNAPSHOT_CFS {
            snap.scan_cf(
                cf,
                &keys::data_key(b"a"),
                &keys::data_key(b"z"),
                false,
                |_, _| {
                    kv_count += 1;
                    Ok(true)
                },
            )
            .unwrap();
        }
        kv_count
    }

    pub fn gen_test_region(region_id: u64, store_id: u64, peer_id: u64) -> Region {
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

    pub fn assert_eq_db(expected_db: Arc<DB>, db: &DB) {
        let key = keys::data_key(TEST_KEY);
        for cf in SNAPSHOT_CFS {
            let p1: Option<Peer> = expected_db.get_msg_cf(cf, &key[..]).unwrap();
            if p1.is_some() {
                let p2: Option<Peer> = db.get_msg_cf(cf, &key[..]).unwrap();
                if !p2.is_some() {
                    panic!("cf {}: expect key {:?} has value", cf, key);
                }
                let p1 = p1.unwrap();
                let p2 = p2.unwrap();
                if p2 != p1 {
                    panic!(
                        "cf {}: key {:?}, value {:?}, expected {:?}",
                        cf, key, p2, p1
                    );
                }
            }
        }
    }

    #[test]
    fn test_gen_snapshot_meta() {
        let mut cf_file = Vec::with_capacity(super::SNAPSHOT_CFS.len());
        for (i, cf) in super::SNAPSHOT_CFS.iter().enumerate() {
            let f = super::CfFile {
                cf,
                size: 100 * (i + 1) as u64,
                checksum: 1000 * (i + 1) as u32,
                ..Default::default()
            };
            cf_file.push(f);
        }
        let meta = super::gen_snapshot_meta(&cf_file).unwrap();
        for (i, cf_file_meta) in meta.get_cf_files().iter().enumerate() {
            if cf_file_meta.get_cf() != cf_file[i].cf {
                panic!(
                    "{}: expect cf {}, got {}",
                    i,
                    cf_file[i].cf,
                    cf_file_meta.get_cf()
                );
            }
            if cf_file_meta.get_size() != cf_file[i].size {
                panic!(
                    "{}: expect cf size {}, got {}",
                    i,
                    cf_file[i].size,
                    cf_file_meta.get_size()
                );
            }
            if cf_file_meta.get_checksum() != cf_file[i].checksum {
                panic!(
                    "{}: expect cf checksum {}, got {}",
                    i,
                    cf_file[i].checksum,
                    cf_file_meta.get_checksum()
                );
            }
        }
    }

    #[test]
    fn test_display_path() {
        let dir = TempDir::new("test-display-path").unwrap();
        let key = SnapKey::new(1, 1, 1);
        let prefix = format!("{}_{}", SNAP_GEN_PREFIX, key);
        let display_path = Snap::get_display_path(dir.path(), &prefix);
        assert_ne!(display_path, "");
    }

    fn gen_db_options_with_encryption() -> DBOptions {
        let env = Arc::new(Env::new_default_ctr_encrypted_env(b"abcd").unwrap());
        let mut db_opt = DBOptions::new();
        db_opt.set_env(env);
        db_opt
    }

    #[test]
    fn test_empty_snap_file() {
        test_snap_file(open_test_empty_db, None);
        test_snap_file(open_test_empty_db, Some(gen_db_options_with_encryption()));
    }

    #[test]
    fn test_non_empty_snap_file() {
        test_snap_file(open_test_db, None);
        test_snap_file(open_test_db, Some(gen_db_options_with_encryption()));
    }

    fn test_snap_file(get_db: DBBuilder, db_opt: Option<DBOptions>) {
        let region_id = 1;
        let region = gen_test_region(region_id, 1, 1);
        let src_db_dir = TempDir::new("test-snap-file-db-src").unwrap();
        let db = get_db(&src_db_dir, db_opt.clone(), None).unwrap();
        let snapshot = DbSnapshot::new(Arc::clone(&db));

        let src_dir = TempDir::new("test-snap-file-src").unwrap();
        let key = SnapKey::new(region_id, 1, 1);
        let size_track = Arc::new(AtomicU64::new(0));
        let deleter = Box::new(DummyDeleter {});
        let mut s1 = Snap::new_for_building(
            src_dir.path(),
            &key,
            &snapshot,
            Arc::clone(&size_track),
            deleter.clone(),
            None,
        )
        .unwrap();
        // Ensure that this snapshot file doesn't exist before being built.
        assert!(!s1.exists());
        assert_eq!(size_track.load(Ordering::SeqCst), 0);

        let mut snap_data = RaftSnapshotData::new();
        snap_data.set_region(region.clone());
        let mut stat = SnapshotStatistics::new();
        s1.build(
            &snapshot,
            &region,
            &mut snap_data,
            &mut stat,
            deleter.clone(),
        )
        .unwrap();

        // Ensure that this snapshot file does exist after being built.
        assert!(s1.exists());
        let total_size = s1.total_size().unwrap();
        // Ensure the `size_track` is modified correctly.
        let size = size_track.load(Ordering::SeqCst);
        assert_eq!(size, total_size);
        assert_eq!(stat.size as u64, size);
        assert_eq!(stat.kv_count, get_kv_count(&snapshot));

        // Ensure this snapshot could be read for sending.
        let mut s2 = Snap::new_for_sending(
            src_dir.path(),
            &key,
            Arc::clone(&size_track),
            deleter.clone(),
        )
        .unwrap();
        assert!(s2.exists());

        // TODO check meta data correct.
        let _ = s2.meta().unwrap();

        let dst_dir = TempDir::new("test-snap-file-dst").unwrap();

        let mut s3 = Snap::new_for_receiving(
            dst_dir.path(),
            &key,
            snap_data.take_meta(),
            Arc::clone(&size_track),
            deleter.clone(),
            None,
        )
        .unwrap();
        assert!(!s3.exists());

        // Ensure snapshot data could be read out of `s2`, and write into `s3`.
        let copy_size = io::copy(&mut s2, &mut s3).unwrap();
        assert_eq!(copy_size, size);
        assert!(!s3.exists());
        s3.save().unwrap();
        assert!(s3.exists());

        // Ensure the tracked size is handled correctly after receiving a snapshot.
        assert_eq!(size_track.load(Ordering::SeqCst), size * 2);

        // Ensure `delete()` works to delete the source snapshot.
        s2.delete();
        assert!(!s2.exists());
        assert!(!s1.exists());
        assert_eq!(size_track.load(Ordering::SeqCst), size);

        // Ensure a snapshot could be applied to DB.
        let mut s4 =
            Snap::new_for_applying(dst_dir.path(), &key, Arc::clone(&size_track), deleter).unwrap();
        assert!(s4.exists());

        let dst_db_dir = TempDir::new("test-snap-file-db-dst").unwrap();
        let dst_db_path = dst_db_dir.path().to_str().unwrap();
        // Change arbitrarily the cf order of ALL_CFS at destination db.
        let dst_cfs = [CF_WRITE, CF_DEFAULT, CF_LOCK, CF_RAFT];
        let dst_db =
            Arc::new(rocks::util::new_engine(dst_db_path, db_opt, &dst_cfs, None).unwrap());
        let options = ApplyOptions {
            db: Arc::clone(&dst_db),
            region: region.clone(),
            abort: Arc::new(AtomicUsize::new(JOB_STATUS_RUNNING)),
            write_batch_size: TEST_WRITE_BATCH_SIZE,
        };
        // Verify thte snapshot applying is ok.
        assert!(s4.apply(options).is_ok());

        // Ensure `delete()` works to delete the dest snapshot.
        s4.delete();
        assert!(!s4.exists());
        assert!(!s3.exists());
        assert_eq!(size_track.load(Ordering::SeqCst), 0);

        // Verify the data is correct after applying snapshot.
        assert_eq_db(db, dst_db.as_ref());
    }

    #[test]
    fn test_empty_snap_validation() {
        test_snap_validation(open_test_empty_db);
    }

    #[test]
    fn test_non_empty_snap_validation() {
        test_snap_validation(open_test_db);
    }

    fn test_snap_validation(get_db: DBBuilder) {
        let region_id = 1;
        let region = gen_test_region(region_id, 1, 1);
        let db_dir = TempDir::new("test-snap-validation-db").unwrap();
        let db = get_db(&db_dir, None, None).unwrap();
        let snapshot = DbSnapshot::new(Arc::clone(&db));

        let dir = TempDir::new("test-snap-validation").unwrap();
        let key = SnapKey::new(region_id, 1, 1);
        let size_track = Arc::new(AtomicU64::new(0));
        let deleter = Box::new(DummyDeleter {});
        let mut s1 = Snap::new_for_building(
            dir.path(),
            &key,
            &snapshot,
            Arc::clone(&size_track),
            deleter.clone(),
            None,
        )
        .unwrap();
        assert!(!s1.exists());

        let mut snap_data = RaftSnapshotData::new();
        snap_data.set_region(region.clone());
        let mut stat = SnapshotStatistics::new();
        s1.build(
            &snapshot,
            &region,
            &mut snap_data,
            &mut stat,
            deleter.clone(),
        )
        .unwrap();
        assert!(s1.exists());

        let mut s2 = Snap::new_for_building(
            dir.path(),
            &key,
            &snapshot,
            Arc::clone(&size_track),
            deleter.clone(),
            None,
        )
        .unwrap();
        assert!(s2.exists());

        s2.build(&snapshot, &region, &mut snap_data, &mut stat, deleter)
            .unwrap();
        assert!(s2.exists());
    }

    // Make all the snapshot in the specified dir corrupted to have incorrect size.
    fn corrupt_snapshot_size_in<T: Into<PathBuf>>(dir: T) {
        let dir_path = dir.into();
        let read_dir = fs::read_dir(dir_path).unwrap();
        for p in read_dir {
            if p.is_ok() {
                let e = p.as_ref().unwrap();
                if !e
                    .file_name()
                    .into_string()
                    .unwrap()
                    .ends_with(META_FILE_SUFFIX)
                {
                    let mut f = OpenOptions::new().append(true).open(e.path()).unwrap();
                    f.write_all(b"xxxxx").unwrap();
                }
            }
        }
    }

    // Make all the snapshot in the specified dir corrupted to have incorrect checksum.
    fn corrupt_snapshot_checksum_in<T: Into<PathBuf>>(dir: T) -> Vec<SnapshotMeta> {
        let dir_path = dir.into();
        let mut res = Vec::new();
        let read_dir = fs::read_dir(dir_path).unwrap();
        for p in read_dir {
            if p.is_ok() {
                let e = p.as_ref().unwrap();
                if e.file_name()
                    .into_string()
                    .unwrap()
                    .ends_with(META_FILE_SUFFIX)
                {
                    let mut snapshot_meta = SnapshotMeta::new();
                    let mut buf = Vec::with_capacity(TEST_META_FILE_BUFFER_SIZE);
                    {
                        let mut f = OpenOptions::new().read(true).open(e.path()).unwrap();
                        f.read_to_end(&mut buf).unwrap();
                    }

                    snapshot_meta.merge_from_bytes(&buf).unwrap();

                    for cf in snapshot_meta.mut_cf_files().iter_mut() {
                        let corrupted_checksum = cf.get_checksum() + 100;
                        cf.set_checksum(corrupted_checksum);
                    }

                    buf.clear();
                    snapshot_meta.write_to_vec(&mut buf).unwrap();
                    {
                        let mut f = OpenOptions::new()
                            .write(true)
                            .truncate(true)
                            .open(e.path())
                            .unwrap();
                        f.write_all(&buf[..]).unwrap();
                        f.flush().unwrap();
                    }

                    res.push(snapshot_meta);
                }
            }
        }
        res
    }

    // Make all the snapshot meta files in the specified corrupted to have incorrect content.
    fn corrupt_snapshot_meta_file<T: Into<PathBuf>>(dir: T) -> usize {
        let mut total = 0;
        let dir_path = dir.into();
        let read_dir = fs::read_dir(dir_path).unwrap();
        for p in read_dir {
            if p.is_ok() {
                let e = p.as_ref().unwrap();
                if e.file_name()
                    .into_string()
                    .unwrap()
                    .ends_with(META_FILE_SUFFIX)
                {
                    let mut f = OpenOptions::new()
                        .read(true)
                        .write(true)
                        .open(e.path())
                        .unwrap();
                    // Make the last byte of the meta file corrupted
                    // by turning over all bits of it
                    let pos = SeekFrom::End(-(BYTE_SIZE as i64));
                    f.seek(pos).unwrap();
                    let mut buf = [0; BYTE_SIZE];
                    f.read_exact(&mut buf[..]).unwrap();
                    buf[0] ^= u8::max_value();
                    f.seek(pos).unwrap();
                    f.write_all(&buf[..]).unwrap();
                    total += 1;
                }
            }
        }
        total
    }

    fn copy_snapshot(
        from_dir: &TempDir,
        to_dir: &TempDir,
        key: &SnapKey,
        size_track: Arc<AtomicU64>,
        snapshot_meta: SnapshotMeta,
        deleter: Box<DummyDeleter>,
    ) {
        let mut from = Snap::new_for_sending(
            from_dir.path(),
            key,
            Arc::clone(&size_track),
            deleter.clone(),
        )
        .unwrap();
        assert!(from.exists());

        let mut to = Snap::new_for_receiving(
            to_dir.path(),
            key,
            snapshot_meta,
            Arc::clone(&size_track),
            deleter,
            None,
        )
        .unwrap();

        assert!(!to.exists());
        let _ = io::copy(&mut from, &mut to).unwrap();
        to.save().unwrap();
        assert!(to.exists());
    }

    #[test]
    fn test_snap_corruption_on_size_or_checksum() {
        let region_id = 1;
        let region = gen_test_region(region_id, 1, 1);
        let db_dir = TempDir::new("test-snap-corruption-db").unwrap();
        let db = open_test_db(&db_dir, None, None).unwrap();
        let snapshot = DbSnapshot::new(db);

        let dir = TempDir::new("test-snap-corruption").unwrap();
        let key = SnapKey::new(region_id, 1, 1);
        let size_track = Arc::new(AtomicU64::new(0));
        let deleter = Box::new(DummyDeleter {});
        let mut s1 = Snap::new_for_building(
            dir.path(),
            &key,
            &snapshot,
            Arc::clone(&size_track),
            deleter.clone(),
            None,
        )
        .unwrap();
        assert!(!s1.exists());

        let mut snap_data = RaftSnapshotData::new();
        snap_data.set_region(region.clone());
        let mut stat = SnapshotStatistics::new();
        s1.build(
            &snapshot,
            &region,
            &mut snap_data,
            &mut stat,
            deleter.clone(),
        )
        .unwrap();
        assert!(s1.exists());

        corrupt_snapshot_size_in(dir.path());

        assert!(
            Snap::new_for_sending(dir.path(), &key, Arc::clone(&size_track), deleter.clone())
                .is_err()
        );

        let mut s2 = Snap::new_for_building(
            dir.path(),
            &key,
            &snapshot,
            Arc::clone(&size_track),
            deleter.clone(),
            None,
        )
        .unwrap();
        assert!(!s2.exists());
        s2.build(
            &snapshot,
            &region,
            &mut snap_data,
            &mut stat,
            deleter.clone(),
        )
        .unwrap();
        assert!(s2.exists());

        let dst_dir = TempDir::new("test-snap-corruption-dst").unwrap();
        copy_snapshot(
            &dir,
            &dst_dir,
            &key,
            Arc::clone(&size_track),
            snap_data.get_meta().clone(),
            deleter.clone(),
        );

        let mut metas = corrupt_snapshot_checksum_in(dst_dir.path());
        assert_eq!(1, metas.len());
        let snap_meta = metas.pop().unwrap();

        let mut s5 = Snap::new_for_applying(
            dst_dir.path(),
            &key,
            Arc::clone(&size_track),
            deleter.clone(),
        )
        .unwrap();
        assert!(s5.exists());

        let dst_db_dir = TempDir::new("test-snap-corruption-dst-db").unwrap();
        let dst_db = open_test_empty_db(&dst_db_dir, None, None).unwrap();
        let options = ApplyOptions {
            db: Arc::clone(&dst_db),
            region: region.clone(),
            abort: Arc::new(AtomicUsize::new(JOB_STATUS_RUNNING)),
            write_batch_size: TEST_WRITE_BATCH_SIZE,
        };
        assert!(s5.apply(options).is_err());

        corrupt_snapshot_size_in(dst_dir.path());
        assert!(Snap::new_for_receiving(
            dst_dir.path(),
            &key,
            snap_meta,
            Arc::clone(&size_track),
            deleter.clone(),
            None,
        )
        .is_err());
        assert!(Snap::new_for_applying(
            dst_dir.path(),
            &key,
            Arc::clone(&size_track),
            deleter.clone()
        )
        .is_err());
    }

    #[test]
    fn test_snap_corruption_on_meta_file() {
        let region_id = 1;
        let region = gen_test_region(region_id, 1, 1);
        let db_dir = TempDir::new("test-snapshot-corruption-meta-db").unwrap();
        let db = open_test_db(&db_dir, None, None).unwrap();
        let snapshot = DbSnapshot::new(db);

        let dir = TempDir::new("test-snap-corruption-meta").unwrap();
        let key = SnapKey::new(region_id, 1, 1);
        let size_track = Arc::new(AtomicU64::new(0));
        let deleter = Box::new(DummyDeleter {});
        let mut s1 = Snap::new_for_building(
            dir.path(),
            &key,
            &snapshot,
            Arc::clone(&size_track),
            deleter.clone(),
            None,
        )
        .unwrap();
        assert!(!s1.exists());

        let mut snap_data = RaftSnapshotData::new();
        snap_data.set_region(region.clone());
        let mut stat = SnapshotStatistics::new();
        s1.build(
            &snapshot,
            &region,
            &mut snap_data,
            &mut stat,
            deleter.clone(),
        )
        .unwrap();
        assert!(s1.exists());

        assert_eq!(1, corrupt_snapshot_meta_file(dir.path()));

        assert!(
            Snap::new_for_sending(dir.path(), &key, Arc::clone(&size_track), deleter.clone())
                .is_err()
        );

        let mut s2 = Snap::new_for_building(
            dir.path(),
            &key,
            &snapshot,
            Arc::clone(&size_track),
            deleter.clone(),
            None,
        )
        .unwrap();
        assert!(!s2.exists());
        s2.build(
            &snapshot,
            &region,
            &mut snap_data,
            &mut stat,
            deleter.clone(),
        )
        .unwrap();
        assert!(s2.exists());

        let dst_dir = TempDir::new("test-snap-corruption-meta-dst").unwrap();
        copy_snapshot(
            &dir,
            &dst_dir,
            &key,
            Arc::clone(&size_track),
            snap_data.get_meta().clone(),
            deleter.clone(),
        );

        assert_eq!(1, corrupt_snapshot_meta_file(dst_dir.path()));

        assert!(Snap::new_for_applying(
            dst_dir.path(),
            &key,
            Arc::clone(&size_track),
            deleter.clone()
        )
        .is_err());
        assert!(Snap::new_for_receiving(
            dst_dir.path(),
            &key,
            snap_data.take_meta(),
            Arc::clone(&size_track),
            deleter.clone(),
            None,
        )
        .is_err());
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
    fn test_snap_mgr_v2() {
        let temp_dir = TempDir::new("test-snap-mgr-v2").unwrap();
        let path = temp_dir.path().to_str().unwrap().to_owned();
        let mgr = SnapManager::new(path.clone(), None);
        mgr.init().unwrap();
        assert_eq!(mgr.get_total_snap_size(), 0);

        let db_dir = TempDir::new("test-snap-mgr-delete-temp-files-v2-db").unwrap();
        let snapshot = DbSnapshot::new(open_test_db(&db_dir, None, None).unwrap());
        let key1 = SnapKey::new(1, 1, 1);
        let size_track = Arc::new(AtomicU64::new(0));
        let deleter = Box::new(mgr.clone());
        let mut s1 = Snap::new_for_building(
            &path,
            &key1,
            &snapshot,
            Arc::clone(&size_track),
            deleter.clone(),
            None,
        )
        .unwrap();
        let mut region = gen_test_region(1, 1, 1);
        let mut snap_data = RaftSnapshotData::new();
        snap_data.set_region(region.clone());
        let mut stat = SnapshotStatistics::new();
        s1.build(
            &snapshot,
            &region,
            &mut snap_data,
            &mut stat,
            deleter.clone(),
        )
        .unwrap();
        let mut s =
            Snap::new_for_sending(&path, &key1, Arc::clone(&size_track), deleter.clone()).unwrap();
        let expected_size = s.total_size().unwrap();
        let mut s2 = Snap::new_for_receiving(
            &path,
            &key1,
            snap_data.get_meta().clone(),
            Arc::clone(&size_track),
            deleter.clone(),
            None,
        )
        .unwrap();
        let n = io::copy(&mut s, &mut s2).unwrap();
        assert_eq!(n, expected_size);
        s2.save().unwrap();

        let key2 = SnapKey::new(2, 1, 1);
        region.set_id(2);
        snap_data.set_region(region);
        let s3 = Snap::new_for_building(
            &path,
            &key2,
            &snapshot,
            Arc::clone(&size_track),
            deleter.clone(),
            None,
        )
        .unwrap();
        let s4 = Snap::new_for_receiving(
            &path,
            &key2,
            snap_data.take_meta(),
            Arc::clone(&size_track),
            deleter.clone(),
            None,
        )
        .unwrap();

        assert!(s1.exists());
        assert!(s2.exists());
        assert!(!s3.exists());
        assert!(!s4.exists());

        let mgr = SnapManager::new(path, None);
        mgr.init().unwrap();
        assert_eq!(mgr.get_total_snap_size(), expected_size * 2);

        assert!(s1.exists());
        assert!(s2.exists());
        assert!(!s3.exists());
        assert!(!s4.exists());

        mgr.get_snapshot_for_sending(&key1).unwrap().delete();
        assert_eq!(mgr.get_total_snap_size(), expected_size);
        mgr.get_snapshot_for_applying(&key1).unwrap().delete();
        assert_eq!(mgr.get_total_snap_size(), 0);
    }

    fn check_registry_around_deregister(mgr: SnapManager, key: &SnapKey, entry: &SnapEntry) {
        let snap_keys = mgr.list_idle_snap().unwrap();
        assert!(snap_keys.is_empty());
        assert!(mgr.has_registered(key));
        mgr.deregister(key, entry);
        let mut snap_keys = mgr.list_idle_snap().unwrap();
        assert_eq!(snap_keys.len(), 1);
        let snap_key = snap_keys.pop().unwrap().0;
        assert_eq!(snap_key, *key);
        assert!(!mgr.has_registered(&snap_key));
    }

    #[test]
    fn test_snap_deletion_on_registry() {
        let src_temp_dir = TempDir::new("test-snap-deletion-on-registry-src").unwrap();
        let src_path = src_temp_dir.path().to_str().unwrap().to_owned();
        let src_mgr = SnapManager::new(src_path.clone(), None);
        src_mgr.init().unwrap();

        let src_db_dir = TempDir::new("test-snap-deletion-on-registry-src-db").unwrap();
        let db = open_test_db(&src_db_dir, None, None).unwrap();
        let snapshot = DbSnapshot::new(db);

        let key = SnapKey::new(1, 1, 1);
        let region = gen_test_region(1, 1, 1);

        // Ensure the snapshot being built will not be deleted on GC.
        src_mgr.register(key.clone(), SnapEntry::Generating);
        let mut s1 = src_mgr.get_snapshot_for_building(&key, &snapshot).unwrap();
        let mut snap_data = RaftSnapshotData::new();
        snap_data.set_region(region.clone());
        let mut stat = SnapshotStatistics::new();
        s1.build(
            &snapshot,
            &region,
            &mut snap_data,
            &mut stat,
            Box::new(src_mgr.clone()),
        )
        .unwrap();
        let mut v = vec![];
        snap_data.write_to_vec(&mut v).unwrap();

        check_registry_around_deregister(src_mgr.clone(), &key, &SnapEntry::Generating);

        // Ensure the snapshot being sent will not be deleted on GC.
        src_mgr.register(key.clone(), SnapEntry::Sending);
        let mut s2 = src_mgr.get_snapshot_for_sending(&key).unwrap();
        let expected_size = s2.total_size().unwrap();

        let dst_temp_dir = TempDir::new("test-snap-deletion-on-registry-dst").unwrap();
        let dst_path = dst_temp_dir.path().to_str().unwrap().to_owned();
        let dst_mgr = SnapManager::new(dst_path.clone(), None);
        dst_mgr.init().unwrap();

        // Ensure the snapshot being received will not be deleted on GC.
        dst_mgr.register(key.clone(), SnapEntry::Receiving);
        let mut s3 = dst_mgr.get_snapshot_for_receiving(&key, &v[..]).unwrap();
        let n = io::copy(&mut s2, &mut s3).unwrap();
        assert_eq!(n, expected_size);
        s3.save().unwrap();

        check_registry_around_deregister(src_mgr.clone(), &key, &SnapEntry::Sending);
        check_registry_around_deregister(dst_mgr.clone(), &key, &SnapEntry::Receiving);

        // Ensure the snapshot to be applied will not be deleted on GC.
        let mut snap_keys = dst_mgr.list_idle_snap().unwrap();
        assert_eq!(snap_keys.len(), 1);
        let snap_key = snap_keys.pop().unwrap().0;
        assert_eq!(snap_key, key);
        assert!(!dst_mgr.has_registered(&snap_key));
        dst_mgr.register(key.clone(), SnapEntry::Applying);
        let s4 = dst_mgr.get_snapshot_for_applying(&key).unwrap();
        let s5 = dst_mgr.get_snapshot_for_applying(&key).unwrap();
        dst_mgr.delete_snapshot(&key, s4.as_ref(), false);
        assert!(s5.exists());
    }

    #[test]
    fn test_snapshot_max_total_size() {
        let regions: Vec<u64> = (0..20).collect();
        let kv_path = TempDir::new("test-snapshot-max-total-size-db").unwrap();
        let kv = get_test_db_for_regions(&kv_path, None, None, &regions).unwrap();

        let snapfiles_path = TempDir::new("test-snapshot-max-total-size-snapshots").unwrap();
        let max_total_size = 10240;
        let snap_mgr = SnapManagerBuilder::default()
            .max_total_size(max_total_size)
            .build(snapfiles_path.path().to_str().unwrap(), None);
        let snapshot = DbSnapshot::new(kv);

        // Add an oldest snapshot for receiving.
        let recv_key = SnapKey::new(100, 100, 100);
        let recv_head = {
            let mut stat = SnapshotStatistics::new();
            let mut snap_data = RaftSnapshotData::new();
            let mut s = snap_mgr
                .get_snapshot_for_building(&recv_key, &snapshot)
                .unwrap();
            s.build(
                &snapshot,
                &gen_test_region(100, 1, 1),
                &mut snap_data,
                &mut stat,
                Box::new(snap_mgr.clone()),
            )
            .unwrap();
            snap_data.write_to_bytes().unwrap()
        };
        let recv_remain = {
            let mut data = Vec::with_capacity(1024);
            let mut s = snap_mgr.get_snapshot_for_sending(&recv_key).unwrap();
            s.read_to_end(&mut data).unwrap();
            assert!(snap_mgr.delete_snapshot(&recv_key, s.as_ref(), true));
            data
        };
        let mut s = snap_mgr
            .get_snapshot_for_receiving(&recv_key, &recv_head)
            .unwrap();
        s.write_all(&recv_remain).unwrap();
        s.save().unwrap();

        for (i, region_id) in regions.into_iter().enumerate() {
            let key = SnapKey::new(region_id, 1, 1);
            let region = gen_test_region(region_id, 1, 1);
            let mut s = snap_mgr.get_snapshot_for_building(&key, &snapshot).unwrap();
            let mut snap_data = RaftSnapshotData::new();
            let mut stat = SnapshotStatistics::new();
            s.build(
                &snapshot,
                &region,
                &mut snap_data,
                &mut stat,
                Box::new(snap_mgr.clone()),
            )
            .unwrap();

            // TODO: this size may change in different RocksDB version.
            let snap_size = 1438;
            let max_snap_count = (max_total_size + snap_size - 1) / snap_size;
            // The first snap_size is for region 100.
            // That snapshot won't be deleted because it's not for generating.
            assert_eq!(
                snap_mgr.get_total_snap_size(),
                snap_size * cmp::min(max_snap_count, (i + 2) as u64)
            );
        }
    }
}
