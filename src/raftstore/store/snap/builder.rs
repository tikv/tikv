use std::cmp;
use std::fs::{self, File, OpenOptions};
use std::io::{self, prelude::*};
use std::sync::mpsc::SyncSender;
use std::sync::Arc;
use std::time::Instant;

use crc::crc32::{self, Digest, Hasher32};
use kvproto::metapb::Region;
use rocksdb::{DBCompressionType, EnvOptions, SstFileWriter};

use raftstore::store::engine::{Iterable, Snapshot as DbSnapshot};
use raftstore::store::keys::{enc_end_key, enc_start_key};
use raftstore::store::metrics::*;
use raftstore::store::snap::*;
use raftstore::Result as RaftStoreResult;
use storage::CfName;
use util::codec::bytes::BytesEncoder;
use util::file::{calc_crc32, delete_file_if_exist, get_file_size};
use util::rocksdb::get_fastest_supported_compression_type;
use util::time::duration_to_sec;
use util::Either;

const TMP_FILE_SUFFIX: &str = ".tmp";

struct CfFile {
    cf: CfName,
    tmp_cf_path: PathBuf,
    tmp_cf_file: Either<File, SstFileWriter>,
    digest: Digest,
    written_bytes: u64,
}

impl CfFile {
    fn new(cf: CfName, tmp_cf_path: PathBuf, tmp_cf_file: Either<File, SstFileWriter>) -> Self {
        CfFile {
            cf,
            tmp_cf_path,
            tmp_cf_file,
            digest: Digest::new(crc32::IEEE),
            written_bytes: 0,
        }
    }

    fn scan_cf(
        &mut self,
        db_snap: &DbSnapshot,
        region_id: u64,
        start_key: &[u8],
        end_key: &[u8],
        io_limiter: Option<&IOLimiter>,
        snap_key: SnapKey,
        snap_stale_notifier: &SnapStaleNotifier,
    ) -> RaftStoreResult<usize> {
        let (mut cf_key_count, mut cf_size) = (0, 0);
        match self.tmp_cf_file {
            Either::Left(ref mut file) => {
                db_snap.scan_cf(self.cf, start_key, end_key, false, |key, value| {
                    cf_key_count += 1;
                    cf_size += key.len() + value.len();
                    file.encode_compact_bytes(key)?;
                    file.encode_compact_bytes(value)?;
                    Ok(!stale_for_generate(snap_key, snap_stale_notifier))
                })?;
                // use an empty byte array to indicate that cf reaches an end.
                file.encode_compact_bytes(b"")?;
                file.flush()?;
            }
            Either::Right(ref mut writer) => {
                let mut bytes = 0;
                let base = io_limiter.map_or(0, |l| l.get_max_bytes_per_time());
                db_snap.scan_cf(self.cf, &start_key, &end_key, false, |key, value| {
                    let l = key.len() + value.len();
                    cf_key_count += 1;
                    cf_size += l;
                    if let Some(ref limiter) = io_limiter {
                        if bytes >= base {
                            bytes = 0;
                            limiter.request(base);
                        }
                        bytes += l as i64;
                    }
                    writer.put(key, value)?;
                    Ok(!stale_for_generate(snap_key, snap_stale_notifier))
                })?;
                if cf_key_count > 0 {
                    writer.finish()?;
                }
            }
        }

        // TODO: calculate written bytes and crc32 while scaning.
        self.written_bytes = get_file_size(&self.tmp_cf_path)?;
        self.digest = Digest::new_with_initial(crc32::IEEE, calc_crc32(&self.tmp_cf_path)?);

        SNAPSHOT_CF_KV_COUNT
            .with_label_values(&[self.cf])
            .observe(cf_key_count as f64);
        SNAPSHOT_CF_SIZE
            .with_label_values(&[self.cf])
            .observe(cf_size as f64);
        info!(
            "[region {}] scan snapshot on cf {}, key count {}, size {}",
            region_id, self.cf, cf_key_count, cf_size
        );
        Ok(cf_key_count)
    }

    fn save(&self, to: &PathBuf) -> io::Result<()> {
        if self.written_bytes > 0 {
            fs::rename(&self.tmp_cf_path, &to)?;
        } else {
            delete_file_if_exist(&self.tmp_cf_path);
        }
        Ok(())
    }
}

struct Inner {
    dir: String,
    for_send: bool,
    key: SnapKey,

    io_limiter: Option<Arc<IOLimiter>>,
    tmp_meta_path: PathBuf,
    tmp_meta_file: File,
    tmp_cf_files: Vec<CfFile>,
    success: bool,
}

impl Inner {
    fn new(
        dir: String,
        for_send: bool,
        key: SnapKey,
        io_limiter: Option<Arc<IOLimiter>>,
    ) -> Result<Inner> {
        let mut open_options = OpenOptions::new();
        open_options.write(true).create_new(true);
        let tmp_meta_path = get_meta_tmp_file_path(&dir, for_send, key);
        let tmp_meta_file = open_options.open(&tmp_meta_path)?;
        Ok(Inner {
            dir,
            for_send,
            key,
            io_limiter,
            tmp_meta_path,
            tmp_meta_file,
            tmp_cf_files: vec![],
            success: false,
        })
    }

    // Rename tmp files to cf files and meta file.
    // Then collect meta infos into a `SnapshotMeta` and return it.
    fn save(&mut self) -> Result<SnapshotMeta> {
        let mut snapshot_meta = SnapshotMeta::new();
        for cf_builder in &self.tmp_cf_files {
            let to = get_cf_file_path(&self.dir, self.for_send, self.key, cf_builder.cf);
            cf_builder.save(&to)?;

            let mut snap_cf_file = SnapshotCFFile::new();
            snap_cf_file.set_cf(cf_builder.cf.to_owned());
            snap_cf_file.set_size(cf_builder.written_bytes);
            snap_cf_file.set_checksum(cf_builder.digest.sum32());
            snapshot_meta.mut_cf_files().push(snap_cf_file);
        }

        snapshot_meta.write_to_writer(&mut self.tmp_meta_file)?;
        let meta_path = get_meta_file_path(&self.dir, self.for_send, self.key);
        fs::rename(&self.tmp_meta_path, &meta_path)?;
        self.success = true;
        Ok(snapshot_meta)
    }
}

impl Drop for Inner {
    fn drop(&mut self) {
        if self.success {
            return;
        }
        for cf_builder in &mut self.tmp_cf_files {
            let p = get_cf_tmp_file_path(&self.dir, self.for_send, self.key, cf_builder.cf);
            delete_file_if_exist(&p);
            let p = get_cf_file_path(&self.dir, self.for_send, self.key, cf_builder.cf);
            delete_file_if_exist(&p);
        }
        let p = get_meta_tmp_file_path(&self.dir, self.for_send, self.key);
        delete_file_if_exist(&p);
        let p = get_meta_file_path(&self.dir, self.for_send, self.key);
        delete_file_if_exist(&p);
    }
}

pub(super) struct SnapshotGenerator {
    inner: Inner,
    snap_stale_notifier: Arc<SnapStaleNotifier>,
    sender: SyncSender<SnapshotMeta>,
    size_tracker: Arc<AtomicU64>,
}

impl SnapshotGenerator {
    /// Open a new SnapshotGenerator. It will lock a tmp meta file
    /// so that there will be always 1 generator at most for a key.
    pub(super) fn new(
        dir: String,
        key: SnapKey,
        io_limiter: Option<Arc<IOLimiter>>,
        snap_stale_notifier: Arc<SnapStaleNotifier>,
        sender: SyncSender<SnapshotMeta>,
        size_tracker: Arc<AtomicU64>,
    ) -> Result<SnapshotGenerator> {
        let inner = Inner::new(dir, true, key, io_limiter)?;
        Ok(SnapshotGenerator {
            inner,
            snap_stale_notifier,
            sender,
            size_tracker,
        })
    }

    /// Build the snapshot with the given `db_snap` for `region`.
    pub(super) fn build(&mut self, region: &Region, db_snap: DbSnapshot) -> Result<SnapshotMeta> {
        let t = Instant::now();
        let (start_key, end_key) = (enc_start_key(region), enc_end_key(region));
        let mut snap_key_count = 0;

        let mut open_options = OpenOptions::new();
        open_options.write(true).create_new(true);

        // Scan every cf, write data into the tmp file respectively.
        let inner = &mut self.inner;
        for cf in SNAPSHOT_CFS.iter() {
            let path = get_cf_tmp_file_path(&inner.dir, inner.for_send, inner.key, cf);
            let file = if plain_file_used(cf) {
                Either::Left(open_options.open(&path)?)
            } else {
                Either::Right(get_sst_file_writer(&db_snap, cf, &path)?)
            };
            inner.tmp_cf_files.push(CfFile::new(cf, path, file));

            let cf_builder = inner.tmp_cf_files.last_mut().unwrap();
            snap_key_count += cf_builder.scan_cf(
                &db_snap,
                region.get_id(),
                &start_key,
                &end_key,
                inner.io_limiter.as_ref().map(|arc| arc.as_ref()),
                inner.key,
                self.snap_stale_notifier.as_ref(),
            )?;
        }

        // Rename all tmp files to cf files.
        let snapshot_meta = inner.save()?;
        let total_size = get_size_from_snapshot_meta(&snapshot_meta);

        SNAPSHOT_BUILD_TIME_HISTOGRAM.observe(duration_to_sec(t.elapsed()) as f64);
        SNAPSHOT_KV_COUNT_HISTOGRAM.observe(snap_key_count as f64);
        SNAPSHOT_SIZE_HISTOGRAM.observe(total_size as f64);
        info!(
            "[region {}] scan snapshot {}, size {}, key count {}, takes {:?}",
            region.get_id(),
            get_display_path(&inner.dir, inner.for_send, inner.key),
            total_size,
            snap_key_count,
            t.elapsed(),
        );

        self.size_tracker.fetch_add(total_size, Ordering::SeqCst);
        let _ = self.sender.send(snapshot_meta.clone());
        Ok(snapshot_meta)
    }
}

pub struct SnapshotReceiver {
    inner: Inner,
    snapshot_meta: SnapshotMeta,
    sender: SyncSender<SnapshotMeta>,
    size_tracker: Arc<AtomicU64>,
    cur_cf_pos: usize,
}

impl SnapshotReceiver {
    pub(super) fn new(
        dir: String,
        key: SnapKey,
        io_limiter: Option<Arc<IOLimiter>>,
        snapshot_meta: SnapshotMeta,
        sender: SyncSender<SnapshotMeta>,
        size_tracker: Arc<AtomicU64>,
    ) -> Result<Self> {
        let inner = Inner::new(dir, false, key, io_limiter)?;
        Ok(SnapshotReceiver {
            inner,
            snapshot_meta,
            sender,
            size_tracker,
            cur_cf_pos: 0,
        })
    }

    pub fn save(&mut self) -> Result<()> {
        let mut got_meta = self.inner.save()?;
        let got_total_size = get_size_from_snapshot_meta(&got_meta);
        let total_size = get_size_from_snapshot_meta(&self.snapshot_meta);
        if got_total_size != total_size {
            self.inner.success = false;
            return Err(snapshot_size_corrupt(total_size, got_total_size));
        }

        let cf_files = got_meta.take_cf_files().into_iter();
        for (got_cf, want_cf) in cf_files.zip(self.snapshot_meta.get_cf_files()) {
            let expected = want_cf.get_size();
            let got = got_cf.get_size();
            if expected != got {
                self.inner.success = false;
                return Err(snapshot_size_corrupt(expected, got));
            }

            let expected = want_cf.get_checksum();
            let got = got_cf.get_checksum();
            if expected != got {
                self.inner.success = false;
                return Err(snapshot_checksum_corrupt(expected, got));
            }
        }

        self.size_tracker.fetch_add(total_size, Ordering::SeqCst);
        let _ = self.sender.send(self.snapshot_meta.clone());
        Ok(())
    }
}

impl Write for SnapshotReceiver {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        if buf.is_empty() {
            return Ok(0);
        }
        let inner = &mut self.inner;
        while self.cur_cf_pos < SNAPSHOT_CFS.len() {
            let cf_meta = &self.snapshot_meta.get_cf_files()[self.cur_cf_pos];

            if self.cur_cf_pos >= inner.tmp_cf_files.len() {
                let cf = &SNAPSHOT_CFS[self.cur_cf_pos];
                let (dir, for_send, key) = (&inner.dir, inner.for_send, inner.key);
                let path = get_cf_tmp_file_path(dir, for_send, key, cf);
                let file = OpenOptions::new().write(true).create_new(true).open(&path)?;
                let cf_file = CfFile::new(cf, path, Either::Left(file));
                inner.tmp_cf_files.push(cf_file);
            }

            let cf_file = inner.tmp_cf_files.last_mut().unwrap();
            if cf_file.written_bytes >= cf_meta.get_size() {
                self.cur_cf_pos += 1;
                continue;
            }

            let io_limiter = inner.io_limiter.clone();
            let mut limit_writer = match cf_file.tmp_cf_file {
                Either::Left(ref mut f) => LimitWriter::new(io_limiter, f),
                _ => unreachable!(),
            };

            let mut bytes = (cf_meta.get_size() - cf_file.written_bytes) as usize;
            bytes = cmp::min(buf.len(), bytes);

            limit_writer.write_all(&buf[0..bytes])?;
            cf_file.digest.write(&buf[0..bytes]);
            cf_file.written_bytes += bytes as u64;
            return Ok(bytes);
        }
        Err(io::Error::new(
            io::ErrorKind::Other,
            "extra bytes when write snapshot".to_owned(),
        ))
    }

    fn flush(&mut self) -> io::Result<()> {
        if let Some(cf_file) = self.inner.tmp_cf_files.last_mut() {
            match cf_file.tmp_cf_file {
                Either::Left(ref mut f) => f.flush()?,
                _ => unreachable!(),
            }
        }
        Ok(())
    }
}

fn get_meta_tmp_file_path(dir: &str, for_send: bool, key: SnapKey) -> PathBuf {
    let mut meta_path = get_meta_file_path(dir, for_send, key);
    let file_name = format!(
        "{}{}",
        meta_path.file_name().and_then(|n| n.to_str()).unwrap(),
        TMP_FILE_SUFFIX,
    );
    meta_path.set_file_name(file_name);
    meta_path
}

fn get_cf_tmp_file_path(dir: &str, for_send: bool, key: SnapKey, cf: &str) -> PathBuf {
    let mut cf_path = get_cf_file_path(dir, for_send, key, cf);
    let file_name = format!(
        "{}{}",
        cf_path.file_name().and_then(|n| n.to_str()).unwrap(),
        TMP_FILE_SUFFIX
    );
    cf_path.set_file_name(file_name);
    cf_path
}

fn get_display_path(dir: &str, for_send: bool, key: SnapKey) -> String {
    let prefix = if for_send {
        SNAP_GEN_PREFIX
    } else {
        SNAP_REV_PREFIX
    };
    let cf_names = "(".to_owned() + &SNAPSHOT_CFS.join("|") + ")";
    format!("{}/{}_{}_{}{}", dir, prefix, key, cf_names, SST_FILE_SUFFIX)
}

fn get_sst_file_writer(
    db_snap: &DbSnapshot,
    cf: &str,
    tmp_cf_path: &PathBuf,
) -> RaftStoreResult<SstFileWriter> {
    let handle = db_snap.cf_handle(cf)?;
    let mut io_options = db_snap.get_db().get_options_cf(handle).clone();
    io_options.compression(get_fastest_supported_compression_type());
    // in rocksdb 5.5.1, SstFileWriter will try to use bottommost_compression and
    // compression_per_level first, so to make sure our specified compression type
    // being used, we must set them empty or disabled.
    io_options.compression_per_level(&[]);
    io_options.bottommost_compression(DBCompressionType::Disable);
    let mut writer = SstFileWriter::new(EnvOptions::new(), io_options);
    box_try!(writer.open(tmp_cf_path.as_path().to_str().unwrap()));
    Ok(writer)
}

#[cfg(test)]
mod tests {
    use tempdir::TempDir;

    use super::*;
    use raftstore::store::snap::tests::*;

    #[test]
    fn test_get_meta_tmp_file_path() {
        let key = SnapKey::new(1, 2, 3);
        for &(dir, for_send, key, expected) in &[
            ("abc", true, key, "abc/gen_1_2_3.meta.tmp"),
            ("abc/", false, key, "abc/rev_1_2_3.meta.tmp"),
            ("ab/c", false, key, "ab/c/rev_1_2_3.meta.tmp"),
            ("", false, key, "rev_1_2_3.meta.tmp"),
        ] {
            let path = get_meta_tmp_file_path(dir, for_send, key);
            assert_eq!(path.to_str().unwrap(), expected);
        }
    }

    #[test]
    fn test_get_cf_tmp_file_path() {
        let key = SnapKey::new(1, 2, 3);
        for &(dir, for_send, key, cf, expected) in &[
            ("abc", true, key, CF_LOCK, "abc/gen_1_2_3_lock.sst.tmp"),
            ("abc/", false, key, CF_WRITE, "abc/rev_1_2_3_write.sst.tmp"),
            (
                "ab/c",
                false,
                key,
                CF_DEFAULT,
                "ab/c/rev_1_2_3_default.sst.tmp",
            ),
            ("", false, key, CF_LOCK, "rev_1_2_3_lock.sst.tmp"),
        ] {
            let path = get_cf_tmp_file_path(dir, for_send, key, cf);
            assert_eq!(path.to_str().unwrap(), expected);
        }
    }

    #[test]
    fn test_snapshot_generator() {
        let tmp_dir = TempDir::new("test-snapshot-generator").unwrap();
        let tmp_path = tmp_dir.path().to_str().unwrap().to_owned();
        let snap_mgr = SnapManager::new(tmp_path.clone(), None);

        let dir = snap_mgr.core.base.clone();
        let key = SnapKey::new(1, 1, 1);
        let snap_stale_notifier = new_snap_stale_notifier();

        // Can't init SnapshotGenerator because tmp meta file exists.
        let p = get_meta_tmp_file_path(&dir, true, key);
        OpenOptions::new()
            .create_new(true)
            .write(true)
            .open(&p)
            .unwrap();

        let notifier = Arc::clone(&snap_stale_notifier);
        let (tx, _) = sync_channel(1);
        let size_tracker = Arc::new(AtomicU64::new(0));
        let r = SnapshotGenerator::new(dir.clone(), key, None, notifier, tx, size_tracker);
        assert!(r.is_err());
        assert!(File::open(&p).is_ok()); // The tmp meta file shouldn't be deleted.

        // Can't save the snapshot because some tmp files are removed.
        delete_file_if_exist(&p);
        let region = get_test_region(1, 1, 1);
        let tmp_db_dir = TempDir::new("test-sanpshot-generator-db").unwrap();
        let test_db = get_test_empty_db(&tmp_db_dir).unwrap();
        let db_snap = DbSnapshot::new(Arc::clone(&test_db));

        let notifier = Arc::clone(&snap_stale_notifier);
        let (tx, _) = sync_channel(1);
        let size_tracker = Arc::new(AtomicU64::new(0));
        let mut generator =
            SnapshotGenerator::new(dir.clone(), key, None, notifier, tx, size_tracker).unwrap();
        assert!(delete_file_if_exist(&p));
        assert!(generator.build(&region, db_snap).is_err());

        // After the generator is dropped, there won't be any tmp files.
        drop(generator);
        assert_eq!(fs::read_dir(&tmp_path).unwrap().count(), 0);
    }

    #[test]
    fn test_snapshot_receiver() {
        let tmp_dir = TempDir::new("test-snapshot-generator").unwrap();
        let tmp_path = tmp_dir.path().to_str().unwrap().to_owned();
        let snap_mgr = SnapManager::new(tmp_path.clone(), None);

        let dir = snap_mgr.core.base.clone();
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

        let data = snapshot_data_from_meta(meta, region.clone());
        let data = data.write_to_bytes().unwrap();

        let mut receiver = snap_mgr.get_snapshot_receiver(key, &data).unwrap().unwrap();
        assert!(snap_mgr.get_snapshot_receiver(key, &data).is_err());

        // Can't save because finished cfs is less than SNAPSHOT_CFS.
        assert!(receiver.save().is_err());

        // Can't save because the bad checksum.
        assert!(receiver.write(&[0u8]).is_ok());
        assert!(receiver.save().is_err());

        // After the receiver is droped, no garbage should exist.
        drop(receiver);
        let rev_files = fs::read_dir(&tmp_path)
            .unwrap()
            .filter(|p| match p {
                Ok(ref p) => p
                    .file_name()
                    .into_string()
                    .unwrap()
                    .starts_with(SNAP_REV_PREFIX),

                Err(_) => false,
            })
            .map(|p| {
                println!("rev file: {:?}", p);
            });
        assert_eq!(rev_files.count(), 0);

        let mut receiver = snap_mgr.get_snapshot_receiver(key, &data).unwrap().unwrap();

        // Write correct data into the receiver.
        let p = get_cf_file_path(&dir, true, key, CF_LOCK);
        let mut buf = Vec::with_capacity(10);
        assert_eq!(File::open(p).unwrap().read_to_end(&mut buf).unwrap(), 1);

        // Can't write extra bytes to snapshot.
        assert!(receiver.write_all(&buf).is_ok());
        assert!(receiver.write(&[0u8]).is_err());

        // Don't need to receive it any more.
        assert!(receiver.save().is_ok());
        drop(receiver);
        let receiver = snap_mgr.get_snapshot_receiver(key, &data).unwrap();
        assert!(receiver.is_none());
    }
}
