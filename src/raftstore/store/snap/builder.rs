use std::cmp;
use std::fs::{self, File, OpenOptions};
use std::io::{self, prelude::*};
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
use util::file::{calc_crc32, delete_dir_if_exist, get_file_size};
use util::rocksdb::get_fastest_supported_compression_type;
use util::time::duration_to_sec;
use util::Either;

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
                let dir = io_limiter.map_or(0, |l| l.get_max_bytes_per_time());
                db_snap.scan_cf(self.cf, &start_key, &end_key, false, |key, value| {
                    let l = key.len() + value.len();
                    cf_key_count += 1;
                    cf_size += l;
                    if let Some(ref limiter) = io_limiter {
                        if bytes >= dir {
                            bytes = 0;
                            limiter.request(dir);
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
}

struct SnapshotBase {
    dir: String,
    for_send: bool,
    key: SnapKey,

    io_limiter: Option<Arc<IOLimiter>>,
    tmp_cf_files: Vec<CfFile>,
    success: bool,
}

impl SnapshotBase {
    fn new(
        dir: String,
        for_send: bool,
        key: SnapKey,
        io_limiter: Option<Arc<IOLimiter>>,
    ) -> SnapshotBase {
        SnapshotBase {
            dir,
            for_send,
            key,
            io_limiter,
            tmp_cf_files: vec![],
            success: false,
        }
    }

    // Rename tmp files to cf files and meta file.
    // Then collect meta infos into a `SnapshotMeta` and return it.
    fn save(&mut self) -> Result<SnapshotMeta> {
        let mut snapshot_meta = SnapshotMeta::new();
        for cf_builder in &self.tmp_cf_files {
            let mut snap_cf_file = SnapshotCFFile::new();
            snap_cf_file.set_cf(cf_builder.cf.to_owned());
            snap_cf_file.set_size(cf_builder.written_bytes);
            snap_cf_file.set_checksum(cf_builder.digest.sum32());
            snapshot_meta.mut_cf_files().push(snap_cf_file);
        }
        let tmp_meta_path = gen_meta_tmp_file_path(&self.dir, self.for_send, self.key);
        let mut tmp_meta_file = create_new_file_at(&tmp_meta_path)?;
        snapshot_meta.write_to_writer(&mut tmp_meta_file)?;
        tmp_meta_file.sync_all()?;

        let tmp_snap_dir = gen_tmp_snap_dir(&self.dir, self.for_send, self.key);
        let snap_dir = gen_snap_dir(&self.dir, self.for_send, self.key);
        fs::rename(&tmp_snap_dir, &snap_dir)?;

        self.success = true;
        Ok(snapshot_meta)
    }
}

impl Drop for SnapshotBase {
    fn drop(&mut self) {
        if self.success {
            return;
        }
        let tmp_snap_dir = gen_tmp_snap_dir(&self.dir, self.for_send, self.key);
        delete_dir_if_exist(&tmp_snap_dir);
    }
}

pub(super) struct SnapshotGenerator {
    inner: SnapshotBase,
    snap_stale_notifier: Arc<SnapStaleNotifier>,
    size_tracker: Arc<AtomicU64>,
}

impl SnapshotGenerator {
    pub(super) fn new(
        dir: String,
        key: SnapKey,
        io_limiter: Option<Arc<IOLimiter>>,
        snap_stale_notifier: Arc<SnapStaleNotifier>,
        size_tracker: Arc<AtomicU64>,
    ) -> SnapshotGenerator {
        let inner = SnapshotBase::new(dir, true, key, io_limiter);
        SnapshotGenerator {
            inner,
            snap_stale_notifier,
            size_tracker,
        }
    }

    /// Build the snapshot with the given `db_snap` for `region`.
    pub(super) fn build(&mut self, region: &Region, db_snap: DbSnapshot) -> Result<SnapshotMeta> {
        let t = Instant::now();
        let (start_key, end_key) = (enc_start_key(region), enc_end_key(region));
        let mut snap_key_count = 0;

        // Scan every cf, write data into the tmp file respectively.
        let inner = &mut self.inner;
        for cf in SNAPSHOT_CFS.iter() {
            let path = gen_cf_tmp_file_path(&inner.dir, inner.for_send, inner.key, cf);
            let file = if plain_file_used(cf) {
                Either::Left(create_new_file_at(&path)?)
            } else {
                Either::Right(gen_sst_file_writer(&db_snap, cf, &path)?)
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
            gen_display_path(&inner.dir, inner.for_send, inner.key),
            total_size,
            snap_key_count,
            t.elapsed(),
        );

        self.size_tracker.fetch_add(total_size, Ordering::SeqCst);
        Ok(snapshot_meta)
    }
}

pub struct SnapshotReceiver {
    inner: SnapshotBase,
    snapshot_meta: SnapshotMeta,
    size_tracker: Arc<AtomicU64>,
    cur_cf_pos: usize,
}

impl SnapshotReceiver {
    pub(super) fn new(
        dir: String,
        key: SnapKey,
        io_limiter: Option<Arc<IOLimiter>>,
        snapshot_meta: SnapshotMeta,
        size_tracker: Arc<AtomicU64>,
    ) -> SnapshotReceiver {
        let inner = SnapshotBase::new(dir, false, key, io_limiter);
        SnapshotReceiver {
            inner,
            snapshot_meta,
            size_tracker,
            cur_cf_pos: 0,
        }
    }

    pub fn save(&mut self) -> Result<()> {
        self.flush()?;
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
        Ok(())
    }

    // Flush the give cf file. Only used for receive snapshots.
    fn flush_cf_file(cf_file: &mut CfFile) -> io::Result<()> {
        match cf_file.tmp_cf_file {
            // To avoid the nvme delay-alloc issue, sync for cf files.
            Either::Left(ref mut f) => f.sync_all(),
            _ => unreachable!(),
        }
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
                let path = gen_cf_tmp_file_path(dir, for_send, key, cf);
                let file = OpenOptions::new().write(true).create_new(true).open(&path)?;
                let cf_file = CfFile::new(cf, path, Either::Left(file));
                inner.tmp_cf_files.push(cf_file);
            }

            let cf_file = inner.tmp_cf_files.last_mut().unwrap();
            if cf_file.written_bytes == cf_meta.get_size() {
                Self::flush_cf_file(cf_file)?;
                self.cur_cf_pos += 1;
                continue;
            }

            let io_limiter = inner.io_limiter.clone();
            let mut limit_writer = match cf_file.tmp_cf_file {
                Either::Left(ref mut f) => LimitWriter::new(io_limiter, f),
                // For receiving snapshots, the tmp file must not be a `Writer`.
                _ => unreachable!(),
            };

            let mut left_bytes = (cf_meta.get_size() - cf_file.written_bytes) as usize;
            left_bytes = cmp::min(buf.len(), left_bytes);

            limit_writer.write_all(&buf[0..left_bytes])?;
            cf_file.digest.write(&buf[0..left_bytes]);
            cf_file.written_bytes += left_bytes as u64;
            return Ok(left_bytes);
        }
        Err(io::Error::new(
            io::ErrorKind::Other,
            "extra bytes when write snapshot".to_owned(),
        ))
    }

    fn flush(&mut self) -> io::Result<()> {
        if let Some(cf_file) = self.inner.tmp_cf_files.last_mut() {
            Self::flush_cf_file(cf_file)?;
        }
        Ok(())
    }
}

fn create_new_file_at<P: AsRef<Path>>(path: P) -> io::Result<File> {
    OpenOptions::new().create_new(true).write(true).open(path)
}

fn gen_meta_tmp_file_path(dir: &str, for_send: bool, key: SnapKey) -> PathBuf {
    let tmp_snap_dir = gen_tmp_snap_dir(dir, for_send, key);
    tmp_snap_dir.join(META_FILE_NAME)
}

fn gen_cf_tmp_file_path(dir: &str, for_send: bool, key: SnapKey, cf: &str) -> PathBuf {
    let tmp_snap_dir = gen_tmp_snap_dir(dir, for_send, key);
    let file_name = format!("{}{}", cf, SST_FILE_SUFFIX);
    tmp_snap_dir.join(&file_name)
}

fn gen_display_path(dir: &str, for_send: bool, key: SnapKey) -> String {
    let snap_dir = gen_snap_dir(dir, for_send, key);
    snap_dir.to_str().unwrap().to_owned()
}

fn gen_sst_file_writer(
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
    use super::*;

    #[test]
    fn test_gen_meta_tmp_file_path() {
        let key = SnapKey::new(1, 2, 3);
        for &(dir, for_send, key, expected) in &[
            ("abc", true, key, "abc/gen_1_2_3.tmp/meta"),
            ("abc/", false, key, "abc/rev_1_2_3.tmp/meta"),
            ("ab/c", false, key, "ab/c/rev_1_2_3.tmp/meta"),
            ("", false, key, "rev_1_2_3.tmp/meta"),
        ] {
            let path = gen_meta_tmp_file_path(dir, for_send, key);
            assert_eq!(path.to_str().unwrap(), expected);
        }
    }

    #[test]
    fn test_gen_cf_tmp_file_path() {
        let key = SnapKey::new(1, 2, 3);
        for &(dir, for_send, key, cf, expected) in &[
            ("abc", true, key, CF_LOCK, "abc/gen_1_2_3.tmp/lock.sst"),
            ("abc/", false, key, CF_WRITE, "abc/rev_1_2_3.tmp/write.sst"),
            (
                "ab/c",
                false,
                key,
                CF_DEFAULT,
                "ab/c/rev_1_2_3.tmp/default.sst",
            ),
            ("", false, key, CF_LOCK, "rev_1_2_3.tmp/lock.sst"),
        ] {
            let path = gen_cf_tmp_file_path(dir, for_send, key, cf);
            assert_eq!(path.to_str().unwrap(), expected);
        }
    }
}
