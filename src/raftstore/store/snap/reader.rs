use std::fs::File;
use std::io::{self, BufReader, Read};
use std::mem;
use std::sync::atomic::{AtomicUsize, Ordering};

use rocksdb::{IngestExternalFileOptions, Writable, WriteBatch};

use raftstore::store::keys;
use raftstore::store::metrics::*;
use raftstore::store::snap::*;
use raftstore::store::util::check_key_in_region;
use util::codec::bytes::CompactBytesFromFileDecoder;
use util::file::{calc_crc32, delete_file_if_exist, get_file_size};
use util::rocksdb::{get_cf_handle, prepare_sst_for_ingestion};

struct SnapshotBase {
    dir: String,
    for_send: bool,
    key: SnapKey,
    snapshot_meta: SnapshotMeta,
}

impl SnapshotBase {
    fn new(dir: String, for_send: bool, key: SnapKey, snapshot_meta: SnapshotMeta) -> Self {
        SnapshotBase {
            dir,
            for_send,
            key,
            snapshot_meta,
        }
    }
}

pub struct SnapshotSender {
    inner: SnapshotBase,
    snap_stale_notifier: Arc<SnapStaleNotifier>,
    ref_count: Arc<AtomicUsize>,
    sent_times: Arc<AtomicUsize>,

    cur_cf_file: Option<File>,
    cur_cf_pos: usize,
}

impl SnapshotSender {
    pub(super) fn new(
        dir: String,
        key: SnapKey,
        snapshot_meta: SnapshotMeta,
        snap_stale_notifier: Arc<SnapStaleNotifier>,
        ref_count: Arc<AtomicUsize>,
        sent_times: Arc<AtomicUsize>,
    ) -> Self {
        let inner = SnapshotBase::new(dir, true, key, snapshot_meta);
        ref_count.fetch_add(1, Ordering::SeqCst);
        SnapshotSender {
            inner,
            snap_stale_notifier,
            ref_count,
            sent_times,
            cur_cf_file: None,
            cur_cf_pos: 0,
        }
    }

    pub fn total_size(&self) -> u64 {
        get_size_from_snapshot_meta(&self.inner.snapshot_meta)
    }
}

impl Drop for SnapshotSender {
    fn drop(&mut self) {
        self.ref_count.fetch_sub(1, Ordering::SeqCst);
        self.sent_times.fetch_add(1, Ordering::SeqCst);
    }
}

impl Read for SnapshotSender {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        if buf.is_empty() {
            return Ok(0);
        }
        let inner = &mut self.inner;
        while self.cur_cf_pos < inner.snapshot_meta.get_cf_files().len() {
            if stale_for_generate(inner.key, self.snap_stale_notifier.as_ref()) {
                let err = io::Error::new(io::ErrorKind::Other, "stale snapshot".to_owned());
                return Err(err);
            }

            let cf = &inner.snapshot_meta.get_cf_files()[self.cur_cf_pos];
            if cf.get_size() > 0 {
                if self.cur_cf_file.is_none() {
                    let (dir, for_send, key) = (&inner.dir, inner.for_send, inner.key);
                    let cf_file_path = gen_cf_file_path(dir, for_send, key, cf.get_cf());
                    self.cur_cf_file = Some(File::open(&cf_file_path)?);
                }
                match self.cur_cf_file.as_mut().unwrap().read(buf) {
                    Ok(0) => {}
                    Ok(n) => return Ok(n),
                    e => return e,
                }
            }
            self.cur_cf_pos += 1;
            self.cur_cf_file = None;
        }
        Ok(0)
    }
}

pub(super) struct SnapshotApplyer {
    inner: SnapshotBase,
    snap_stale_notifier: Arc<SnapStaleNotifier>,
    ref_count: Arc<AtomicUsize>,
    applied_times: Arc<AtomicUsize>,
}

impl SnapshotApplyer {
    pub(super) fn new(
        dir: String,
        key: SnapKey,
        snapshot_meta: SnapshotMeta,
        snap_stale_notifier: Arc<SnapStaleNotifier>,
        ref_count: Arc<AtomicUsize>,
        applied_times: Arc<AtomicUsize>,
    ) -> Self {
        let inner = SnapshotBase::new(dir, false, key, snapshot_meta);
        ref_count.fetch_add(1, Ordering::SeqCst);
        SnapshotApplyer {
            inner,
            snap_stale_notifier,
            ref_count,
            applied_times,
        }
    }

    pub(super) fn apply(&self, options: ApplyOptions) -> Result<()> {
        for cf in self.inner.snapshot_meta.get_cf_files() {
            if cf.get_size() == 0 {
                continue;
            }
            let cf = cf.get_cf();
            if plain_file_used(cf) {
                self.apply_plain_cf_file(cf, &options)?;
            } else {
                let (dir, key) = (&self.inner.dir, self.inner.key);
                let cf_path = gen_cf_file_path(dir, false, key, cf);
                let clone_path = gen_cf_clone_file_path(dir, false, key, cf);
                prepare_sst_for_ingestion(&cf_path, &clone_path)?;

                let db = options.db.as_ref();
                let mut options = IngestExternalFileOptions::new();
                options.move_files(true);
                let handle = box_try!(get_cf_handle(db, cf));
                let _timer = INGEST_SST_DURATION_SECONDS.start_coarse_timer();
                db.ingest_external_file_cf(handle, &options, &[clone_path.to_str().unwrap()])?;
            }
        }
        Ok(())
    }

    fn apply_plain_cf_file(&self, cf: &str, options: &ApplyOptions) -> Result<()> {
        let path = gen_cf_file_path(&self.inner.dir, false, self.inner.key, cf);
        let mut reader = BufReader::new(File::open(path)?);

        let handle = box_try!(get_cf_handle(options.db.as_ref(), cf));
        let mut wb = WriteBatch::new();
        let mut finished = false;
        while !finished {
            if stale_for_apply(self.inner.key, self.snap_stale_notifier.as_ref()) {
                return Err(Error::Snapshot(SnapError::Abort));
            }

            let key = reader.decode_compact_bytes()?;
            if key.is_empty() {
                finished = true;
            } else {
                box_try!(check_key_in_region(keys::origin_key(&key), &options.region));
                let value = reader.decode_compact_bytes()?;
                box_try!(wb.put_cf(handle, &key, &value));
            }
            if wb.data_size() >= options.write_batch_size || finished {
                box_try!(options.db.write(wb));
                wb = WriteBatch::new();
            }
        }
        Ok(())
    }
}

impl Drop for SnapshotApplyer {
    fn drop(&mut self) {
        self.applied_times.fetch_add(1, Ordering::SeqCst);
        self.ref_count.fetch_sub(1, Ordering::SeqCst);

        for cf in self.inner.snapshot_meta.get_cf_files() {
            if cf.get_size() == 0 || plain_file_used(cf.get_cf()) {
                continue;
            }
            let (dir, key) = (&self.inner.dir, self.inner.key);
            let clone_path = gen_cf_clone_file_path(dir, false, key, cf.get_cf());
            delete_file_if_exist(&clone_path);
        }
    }
}

fn gen_cf_clone_file_path(dir: &str, for_send: bool, key: SnapKey, cf: &str) -> PathBuf {
    let mut cf_path = gen_cf_file_path(dir, for_send, key, cf);
    let file_name = format!(
        "{}{}",
        cf_path.file_name().and_then(|n| n.to_str()).unwrap(),
        CLONE_FILE_SUFFIX,
    );
    cf_path.set_file_name(file_name);
    cf_path
}

pub(super) fn snapshot_load(
    dir: &str,
    for_send: bool,
    key: SnapKey,
) -> Result<Option<SnapshotMeta>> {
    let meta_path = gen_meta_file_path(dir, for_send, key);
    if let Ok(meta) = read_snapshot_meta(&meta_path) {
        let mut inner = SnapshotBase::new(dir.to_owned(), for_send, key, meta);
        for cf in inner.snapshot_meta.get_cf_files() {
            if cf.get_size() == 0 {
                continue;
            }
            let (dir, for_send, key) = (&inner.dir, inner.for_send, inner.key);
            let cf_file_path = gen_cf_file_path(dir, for_send, key, cf.get_cf());

            let checksum = calc_crc32(&cf_file_path)?;
            if checksum != cf.get_checksum() {
                return Err(snapshot_checksum_corrupt(cf.get_checksum(), checksum));
            }

            let size = get_file_size(&cf_file_path)?;
            if size != cf.get_size() {
                return Err(snapshot_size_corrupt(cf.get_size(), size));
            }
        }
        let meta = mem::replace(&mut inner.snapshot_meta, SnapshotMeta::new());
        return Ok(Some(meta));
    }
    Ok(None)
}

#[cfg(test)]
mod tests {
    use tempdir::TempDir;

    use super::*;
    use raftstore::store::snap::tests::*;

    #[test]
    fn test_gen_cf_clone_file_path() {
        let key = SnapKey::new(1, 2, 3);
        for &(dir, for_send, key, cf, expected) in &[
            ("abc", true, key, CF_LOCK, "abc/gen_1_2_3_lock.sst.clone"),
            (
                "abc/",
                false,
                key,
                CF_WRITE,
                "abc/rev_1_2_3_write.sst.clone",
            ),
            (
                "ab/c",
                false,
                key,
                CF_DEFAULT,
                "ab/c/rev_1_2_3_default.sst.clone",
            ),
            ("", false, key, CF_LOCK, "rev_1_2_3_lock.sst.clone"),
        ] {
            let path = gen_cf_clone_file_path(dir, for_send, key, cf);
            assert_eq!(path.to_str().unwrap(), expected);
        }
    }

    #[test]
    fn test_snapshot_sender() {
        let tmp_dir = TempDir::new("test-snapshot-sender").unwrap();
        let tmp_path = tmp_dir.path().to_str().unwrap().to_owned();
        let snap_mgr = SnapManager::new(tmp_path.clone(), None);

        let dir = snap_mgr.core.dir.clone();
        let key = SnapKey::new(1, 1, 1);
        let snap_stale_notifier = new_snap_stale_notifier();

        let notifier = Arc::clone(&snap_stale_notifier);
        let (tx, _) = sync_channel(1);
        let size_tracker = Arc::new(AtomicU64::new(0));
        let mut generator =
            SnapshotGenerator::new(dir.clone(), key, None, notifier, tx, size_tracker).unwrap();

        let region = get_test_region(1, 1, 1);
        let tmp_db_dir = TempDir::new("test-sanpshot-sender-db").unwrap();
        let test_db = get_test_empty_db(&tmp_db_dir).unwrap();
        let db_snap = DbSnapshot::new(Arc::clone(&test_db));
        let meta = generator.build(&region, db_snap).unwrap();
        drop(generator); // Set the ref_count to 0.

        let ref_count = Arc::new(AtomicUsize::new(0));
        let used_times = Arc::new(AtomicUsize::new(0));

        let get_sender = || {
            let notifier = Arc::clone(&snap_stale_notifier);
            let rc = Arc::clone(&ref_count);
            let ut = Arc::clone(&used_times);
            SnapshotSender::new(dir.clone(), key, meta.clone(), notifier, rc, ut)
        };

        let mut s1 = get_sender();
        assert_eq!(ref_count.load(Ordering::SeqCst), 1);
        let mut s2 = get_sender();
        assert_eq!(ref_count.load(Ordering::SeqCst), 2);

        let mut buf = Vec::with_capacity(10);
        assert_eq!(s1.read_to_end(&mut buf).unwrap(), 1);
        assert_eq!(s2.read_to_end(&mut buf).unwrap(), 1);
        assert_eq!(&buf[0], &buf[1]);

        drop(s1);
        assert_eq!(used_times.load(Ordering::SeqCst), 1);
        drop(s2);
        assert_eq!(used_times.load(Ordering::SeqCst), 2);

        let mut s3 = get_sender();
        let p = gen_cf_file_path(&dir, true, key, CF_LOCK);
        assert!(delete_file_if_exist(&p));
        assert!(s3.read_to_end(&mut buf).is_err());
    }
}
