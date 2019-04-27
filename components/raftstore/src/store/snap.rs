use std::fmt::{self, Display, Formatter};
use std::fs::{self, Metadata};
use std::fs::{File, OpenOptions};
use std::io::{self, BufReader, ErrorKind, Read, Write};
use std::path::Path;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Instant;
use std::{str, thread, time, u64};

use crc::crc32::{self, Digest, Hasher32};
use engine::rocks::util::{
    get_fastest_supported_compression_type, prepare_sst_for_ingestion, validate_sst_for_ingestion,
};
use engine::rocks::Snapshot as DbSnapshot;
use engine::rocks::{
    self, CFHandle, DBCompressionType, EnvOptions, IngestExternalFileOptions, SstFileWriter,
    Writable, WriteBatch, DB,
};
use engine::Iterable;
use engine::{CfName, CF_DEFAULT, CF_LOCK, CF_WRITE};
use kvproto::metapb::Region;
use kvproto::raft_serverpb::RaftSnapshotData;
use kvproto::raft_serverpb::{SnapshotCFFile, SnapshotMeta};
use protobuf::Message;
use protobuf::RepeatedField;
use raft::eraftpb::Snapshot as RaftSnapshot;

use crate::errors::Error as RaftStoreError;
use tikv_misc::keys::{self, enc_end_key, enc_start_key};
use tikv_misc::store_util::check_key_in_region;
use crate::Result as RaftStoreResult;
use engine::rocks::util::io_limiter::{IOLimiter, LimitWriter};
use tikv_util::codec::bytes::{BytesEncoder, CompactBytesFromFileDecoder};
use tikv_util::file::{calc_crc32, delete_file_if_exist, file_exists, get_file_size};
use tikv_util::time::duration_to_sec;

use super::metrics::{
    INGEST_SST_DURATION_SECONDS, SNAPSHOT_BUILD_TIME_HISTOGRAM, SNAPSHOT_CF_KV_COUNT,
    SNAPSHOT_CF_SIZE,
};
use tikv_misc::peer_storage::JOB_STATUS_CANCELLING;

quick_error! {
    #[derive(Debug)]
    pub enum Error {
        Abort {
            description("abort")
            display("abort")
        }
        TooManySnapshots {
            description("too many snapshots")
        }
        Other(err: Box<dyn std::error::Error + Sync + Send>) {
            from()
            cause(err.as_ref())
            description(err.description())
            display("snap failed {:?}", err)
        }
    }
}

pub type Result<T> = std::result::Result<T, Error>;

// Data in CF_RAFT should be excluded for a snapshot.
pub const SNAPSHOT_CFS: &[CfName] = &[CF_DEFAULT, CF_LOCK, CF_WRITE];

pub const SNAPSHOT_VERSION: u64 = 2;

/// Name prefix for the self-generated snapshot file.
pub const SNAP_GEN_PREFIX: &str = "gen";
/// Name prefix for the received snapshot file.
const SNAP_REV_PREFIX: &str = "rev";

pub const TMP_FILE_SUFFIX: &str = ".tmp";
pub const SST_FILE_SUFFIX: &str = ".sst";
const CLONE_FILE_SUFFIX: &str = ".clone";
const META_FILE_SUFFIX: &str = ".meta";

const DELETE_RETRY_MAX_TIMES: u32 = 6;
const DELETE_RETRY_TIME_MILLIS: u64 = 500;

// CF_LOCK is relatively small, so we use plain file for performance issue.
#[inline]
pub fn plain_file_used(cf: &str) -> bool {
    cf == CF_LOCK
}

#[inline]
pub fn check_abort(status: &AtomicUsize) -> Result<()> {
    if status.load(Ordering::Relaxed) == JOB_STATUS_CANCELLING {
        return Err(Error::Abort);
    }
    Ok(())
}

#[derive(Clone, Hash, PartialEq, Eq, PartialOrd, Ord, Debug)]
pub struct SnapKey {
    pub region_id: u64,
    pub term: u64,
    pub idx: u64,
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
            return Err(io::Error::new(ErrorKind::Other, e));
        }

        Ok(SnapKey::from_region_snap(
            snap_data.get_region().get_id(),
            snap,
        ))
    }
}

impl Display for SnapKey {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "{}_{}_{}", self.region_id, self.term, self.idx)
    }
}

#[derive(Default)]
pub struct SnapshotStatistics {
    pub size: u64,
    pub kv_count: usize,
}

impl SnapshotStatistics {
    pub fn new() -> SnapshotStatistics {
        SnapshotStatistics {
            ..Default::default()
        }
    }
}

pub struct ApplyOptions {
    pub db: Arc<DB>,
    pub region: Region,
    pub abort: Arc<AtomicUsize>,
    pub write_batch_size: usize,
}

/// `Snapshot` is a trait for snapshot.
/// It's used in these scenarios:
///   1. build local snapshot
///   2. read local snapshot and then replicate it to remote raftstores
///   3. receive snapshot from remote raftstore and write it to local storage
///   4. apply snapshot
///   5. snapshot gc
pub trait Snapshot: Read + Write + Send {
    fn build(
        &mut self,
        snap: &DbSnapshot,
        region: &Region,
        snap_data: &mut RaftSnapshotData,
        stat: &mut SnapshotStatistics,
        deleter: Box<dyn SnapshotDeleter>,
    ) -> RaftStoreResult<()>;
    fn path(&self) -> &str;
    fn exists(&self) -> bool;
    fn delete(&self);
    fn meta(&self) -> io::Result<Metadata>;
    fn total_size(&self) -> io::Result<u64>;
    fn save(&mut self) -> io::Result<()>;
    fn apply(&mut self, options: ApplyOptions) -> Result<()>;
}

// A helper function to copy snapshot.
// Only used in tests.
pub fn copy_snapshot(mut from: Box<dyn Snapshot>, mut to: Box<dyn Snapshot>) -> io::Result<()> {
    if !to.exists() {
        io::copy(&mut from, &mut to)?;
        to.save()?;
    }
    Ok(())
}

/// `SnapshotDeleter` is a trait for deleting snapshot.
/// It's used to ensure that the snapshot deletion happens under the protection of locking
/// to avoid race case for concurrent read/write.
pub trait SnapshotDeleter {
    // Return true if it successfully delete the specified snapshot.
    fn delete_snapshot(&self, key: &SnapKey, snap: &dyn Snapshot, check_entry: bool) -> bool;
}

// Try to delete the specified snapshot using deleter, return true if the deletion is done.
pub fn retry_delete_snapshot(
    deleter: Box<dyn SnapshotDeleter>,
    key: &SnapKey,
    snap: &dyn Snapshot,
) -> bool {
    let d = time::Duration::from_millis(DELETE_RETRY_TIME_MILLIS);
    for _ in 0..DELETE_RETRY_MAX_TIMES {
        if deleter.delete_snapshot(key, snap, true) {
            return true;
        }
        thread::sleep(d);
    }
    false
}

fn gen_snapshot_meta(cf_files: &[CfFile]) -> RaftStoreResult<SnapshotMeta> {
    let mut meta = Vec::with_capacity(cf_files.len());
    for cf_file in cf_files {
        if SNAPSHOT_CFS.iter().find(|&cf| cf_file.cf == *cf).is_none() {
            return Err(box_err!(
                "failed to encode invalid snapshot cf {}",
                cf_file.cf
            ));
        }

        let mut cf_file_meta = SnapshotCFFile::new();
        cf_file_meta.set_cf(cf_file.cf.to_owned());
        cf_file_meta.set_size(cf_file.size);
        cf_file_meta.set_checksum(cf_file.checksum);
        meta.push(cf_file_meta);
    }
    let mut snapshot_meta = SnapshotMeta::new();
    snapshot_meta.set_cf_files(RepeatedField::from_vec(meta));
    Ok(snapshot_meta)
}

fn check_file_size(path: &PathBuf, expected_size: u64) -> RaftStoreResult<()> {
    let size = get_file_size(path)?;
    if size != expected_size {
        return Err(box_err!(
            "invalid size {} for snapshot cf file {}, expected {}",
            size,
            path.display(),
            expected_size
        ));
    }
    Ok(())
}

fn check_file_checksum(path: &PathBuf, expected_checksum: u32) -> RaftStoreResult<()> {
    let checksum = calc_crc32(path)?;
    if checksum != expected_checksum {
        return Err(box_err!(
            "invalid checksum {} for snapshot cf file {}, expected {}",
            checksum,
            path.display(),
            expected_checksum
        ));
    }
    Ok(())
}

fn check_file_size_and_checksum(
    path: &PathBuf,
    expected_size: u64,
    expected_checksum: u32,
) -> RaftStoreResult<()> {
    check_file_size(path, expected_size).and_then(|_| check_file_checksum(path, expected_checksum))
}

#[derive(Default)]
struct CfFile {
    pub cf: CfName,
    pub path: PathBuf,
    pub tmp_path: PathBuf,
    pub clone_path: PathBuf,
    pub sst_writer: Option<SstFileWriter>,
    pub file: Option<File>,
    pub kv_count: u64,
    pub size: u64,
    pub written_size: u64,
    pub checksum: u32,
    pub write_digest: Option<Digest>,
}

#[derive(Default)]
struct MetaFile {
    pub meta: SnapshotMeta,
    pub path: PathBuf,
    pub file: Option<File>,

    // for writing snapshot
    pub tmp_path: PathBuf,
}

pub struct Snap {
    key: SnapKey,
    display_path: String,
    cf_files: Vec<CfFile>,
    cf_index: usize,
    meta_file: MetaFile,
    size_track: Arc<AtomicU64>,
    limiter: Option<Arc<IOLimiter>>,
    hold_tmp_files: bool,
}

impl Snap {
    fn new<T: Into<PathBuf>>(
        dir: T,
        key: &SnapKey,
        size_track: Arc<AtomicU64>,
        is_sending: bool,
        to_build: bool,
        deleter: Box<dyn SnapshotDeleter>,
        limiter: Option<Arc<IOLimiter>>,
    ) -> RaftStoreResult<Snap> {
        let dir_path = dir.into();
        if !dir_path.exists() {
            fs::create_dir_all(dir_path.as_path())?;
        }
        let snap_prefix = if is_sending {
            SNAP_GEN_PREFIX
        } else {
            SNAP_REV_PREFIX
        };
        let prefix = format!("{}_{}", snap_prefix, key);
        let display_path = Snap::get_display_path(&dir_path, &prefix);

        let mut cf_files = Vec::with_capacity(SNAPSHOT_CFS.len());
        for cf in SNAPSHOT_CFS {
            let filename = format!("{}_{}{}", prefix, cf, SST_FILE_SUFFIX);
            let path = dir_path.join(&filename);
            let tmp_path = dir_path.join(format!("{}{}", filename, TMP_FILE_SUFFIX));
            let clone_path = dir_path.join(format!("{}{}", filename, CLONE_FILE_SUFFIX));
            let cf_file = CfFile {
                cf,
                path,
                tmp_path,
                clone_path,
                ..Default::default()
            };
            cf_files.push(cf_file);
        }

        let meta_filename = format!("{}{}", prefix, META_FILE_SUFFIX);
        let meta_path = dir_path.join(&meta_filename);
        let meta_tmp_path = dir_path.join(format!("{}{}", meta_filename, TMP_FILE_SUFFIX));
        let meta_file = MetaFile {
            path: meta_path,
            tmp_path: meta_tmp_path,
            ..Default::default()
        };

        let mut s = Snap {
            key: key.clone(),
            display_path,
            cf_files,
            cf_index: 0,
            meta_file,
            size_track,
            limiter,
            hold_tmp_files: false,
        };

        // load snapshot meta if meta_file exists
        if file_exists(&s.meta_file.path) {
            if let Err(e) = s.load_snapshot_meta() {
                if !to_build {
                    return Err(e);
                }
                warn!(
                    "failed to load existent snapshot meta when try to build snapshot";
                    "snapshot" => %s.path(),
                    "err" => ?e,
                );
                if !retry_delete_snapshot(deleter, key, &s) {
                    warn!(
                        "failed to delete snapshot because it's already registered elsewhere";
                        "snapshot" => %s.path(),
                    );
                    return Err(e);
                }
            }
        }
        Ok(s)
    }

    pub fn new_for_building<T: Into<PathBuf>>(
        dir: T,
        key: &SnapKey,
        snap: &DbSnapshot,
        size_track: Arc<AtomicU64>,
        deleter: Box<dyn SnapshotDeleter>,
        limiter: Option<Arc<IOLimiter>>,
    ) -> RaftStoreResult<Snap> {
        let mut s = Snap::new(dir, key, size_track, true, true, deleter, limiter)?;
        s.init_for_building(snap)?;
        Ok(s)
    }

    pub fn new_for_sending<T: Into<PathBuf>>(
        dir: T,
        key: &SnapKey,
        size_track: Arc<AtomicU64>,
        deleter: Box<dyn SnapshotDeleter>,
    ) -> RaftStoreResult<Snap> {
        let mut s = Snap::new(dir, key, size_track, true, false, deleter, None)?;

        if !s.exists() {
            // Skip the initialization below if it doesn't exists.
            return Ok(s);
        }
        for cf_file in &mut s.cf_files {
            // initialize cf file size and reader
            if cf_file.size > 0 {
                let file = File::open(&cf_file.path)?;
                cf_file.file = Some(file);
            }
        }
        Ok(s)
    }

    pub fn new_for_receiving<T: Into<PathBuf>>(
        dir: T,
        key: &SnapKey,
        snapshot_meta: SnapshotMeta,
        size_track: Arc<AtomicU64>,
        deleter: Box<dyn SnapshotDeleter>,
        limiter: Option<Arc<IOLimiter>>,
    ) -> RaftStoreResult<Snap> {
        let mut s = Snap::new(dir, key, size_track, false, false, deleter, limiter)?;
        s.set_snapshot_meta(snapshot_meta)?;
        if s.exists() {
            return Ok(s);
        }

        let f = OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&s.meta_file.tmp_path)?;
        s.meta_file.file = Some(f);
        s.hold_tmp_files = true;

        for cf_file in &mut s.cf_files {
            if cf_file.size == 0 {
                continue;
            }
            let f = OpenOptions::new()
                .write(true)
                .create_new(true)
                .open(&cf_file.tmp_path)?;
            cf_file.file = Some(f);
            cf_file.write_digest = Some(Digest::new(crc32::IEEE));
        }
        Ok(s)
    }

    pub fn new_for_applying<T: Into<PathBuf>>(
        dir: T,
        key: &SnapKey,
        size_track: Arc<AtomicU64>,
        deleter: Box<dyn SnapshotDeleter>,
    ) -> RaftStoreResult<Snap> {
        let s = Snap::new(dir, key, size_track, false, false, deleter, None)?;
        Ok(s)
    }

    fn init_for_building(&mut self, snap: &DbSnapshot) -> RaftStoreResult<()> {
        if self.exists() {
            return Ok(());
        }
        let file = OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&self.meta_file.tmp_path)?;
        self.meta_file.file = Some(file);
        self.hold_tmp_files = true;

        for cf_file in &mut self.cf_files {
            if plain_file_used(cf_file.cf) {
                let f = OpenOptions::new()
                    .write(true)
                    .create_new(true)
                    .open(&cf_file.tmp_path)?;
                cf_file.file = Some(f);
            } else {
                let handle = snap.cf_handle(cf_file.cf)?;
                let mut io_options = snap.get_db().get_options_cf(handle).clone();
                io_options.compression(get_fastest_supported_compression_type());
                // in rocksdb 5.5.1, SstFileWriter will try to use bottommost_compression and
                // compression_per_level first, so to make sure our specified compression type
                // being used, we must set them empty or disabled.
                io_options.compression_per_level(&[]);
                io_options.bottommost_compression(DBCompressionType::Disable);

                // When open db with encrypted env, we need to send the same env to the SstFileWriter.
                if let Some(env) = snap.get_db().env() {
                    io_options.set_env(env);
                }
                let mut writer = SstFileWriter::new(EnvOptions::new(), io_options);
                box_try!(writer.open(cf_file.tmp_path.as_path().to_str().unwrap()));
                cf_file.sst_writer = Some(writer);
            }
        }
        Ok(())
    }

    fn read_snapshot_meta(&mut self) -> RaftStoreResult<SnapshotMeta> {
        let buf = fs::read(&self.meta_file.path)?;
        let mut snapshot_meta = SnapshotMeta::new();
        snapshot_meta.merge_from_bytes(&buf)?;
        Ok(snapshot_meta)
    }

    fn set_snapshot_meta(&mut self, snapshot_meta: SnapshotMeta) -> RaftStoreResult<()> {
        if snapshot_meta.get_cf_files().len() != self.cf_files.len() {
            return Err(box_err!(
                "invalid cf number of snapshot meta, expect {}, got {}",
                SNAPSHOT_CFS.len(),
                snapshot_meta.get_cf_files().len()
            ));
        }
        for (i, cf_file) in self.cf_files.iter_mut().enumerate() {
            let meta = snapshot_meta.get_cf_files().get(i).unwrap();
            if meta.get_cf() != cf_file.cf {
                return Err(box_err!(
                    "invalid {} cf in snapshot meta, expect {}, got {}",
                    i,
                    cf_file.cf,
                    meta.get_cf()
                ));
            }
            if file_exists(&cf_file.path) {
                // Check only the file size for `exists()` to work correctly.
                check_file_size(&cf_file.path, meta.get_size())?;
            }
            cf_file.size = meta.get_size();
            cf_file.checksum = meta.get_checksum();
        }
        self.meta_file.meta = snapshot_meta;
        Ok(())
    }

    fn load_snapshot_meta(&mut self) -> RaftStoreResult<()> {
        let snapshot_meta = self.read_snapshot_meta()?;
        self.set_snapshot_meta(snapshot_meta)?;
        // check if there is a data corruption when the meta file exists
        // but cf files are deleted.
        if !self.exists() {
            return Err(box_err!(
                "snapshot {} is corrupted, some cf file is missing",
                self.path()
            ));
        }
        Ok(())
    }

    fn get_display_path(dir_path: impl AsRef<Path>, prefix: &str) -> String {
        let cf_names = "(".to_owned() + &SNAPSHOT_CFS.join("|") + ")";
        format!(
            "{}/{}_{}{}",
            dir_path.as_ref().display(),
            prefix,
            cf_names,
            SST_FILE_SUFFIX
        )
    }

    fn validate(&self, db: Arc<DB>) -> RaftStoreResult<()> {
        for cf_file in &self.cf_files {
            if cf_file.size == 0 {
                // Skip empty file. The checksum of this cf file should be 0 and
                // this is checked when loading the snapshot meta.
                continue;
            }
            if plain_file_used(cf_file.cf) {
                check_file_size_and_checksum(&cf_file.path, cf_file.size, cf_file.checksum)?;
            } else {
                prepare_sst_for_ingestion(&cf_file.path, &cf_file.clone_path)?;
                validate_sst_for_ingestion(
                    &db,
                    cf_file.cf,
                    &cf_file.clone_path,
                    cf_file.size,
                    cf_file.checksum,
                )?;
            }
        }
        Ok(())
    }

    fn switch_to_cf_file(&mut self, cf: &str) -> io::Result<()> {
        match self.cf_files.iter().position(|x| x.cf == cf) {
            Some(index) => {
                self.cf_index = index;
                Ok(())
            }
            None => Err(io::Error::new(
                ErrorKind::Other,
                format!("fail to find cf {}", cf),
            )),
        }
    }

    fn add_kv(&mut self, k: &[u8], v: &[u8]) -> RaftStoreResult<()> {
        let cf_file = &mut self.cf_files[self.cf_index];
        if let Some(writer) = cf_file.sst_writer.as_mut() {
            if let Err(e) = writer.put(k, v) {
                let io_error = io::Error::new(ErrorKind::Other, e);
                return Err(RaftStoreError::from(io_error));
            }
            cf_file.kv_count += 1;
            Ok(())
        } else {
            let e = box_err!("can't find sst writer");
            Err(RaftStoreError::Snapshot(e))
        }
    }

    fn save_cf_files(&mut self) -> io::Result<()> {
        for cf_file in &mut self.cf_files {
            if plain_file_used(cf_file.cf) {
                let _ = cf_file.file.take();
            } else if cf_file.kv_count == 0 {
                let _ = cf_file.sst_writer.take().unwrap();
            } else {
                let mut writer = cf_file.sst_writer.take().unwrap();
                if let Err(e) = writer.finish() {
                    return Err(io::Error::new(ErrorKind::Other, e));
                }
            }
            let size = get_file_size(&cf_file.tmp_path)?;
            // The size of a sst file is larger than 0 doesn't mean it contains some key value pairs.
            // For example, if we provide a encrypted env to the SstFileWriter, RocksDB will append
            // some meta data to the header. So here we should use the kv count instead of the file size
            // to indicate if the sst file is empty.
            if cf_file.kv_count > 0 {
                fs::rename(&cf_file.tmp_path, &cf_file.path)?;
                cf_file.size = size;
                // add size
                self.size_track.fetch_add(size, Ordering::SeqCst);
                cf_file.checksum = calc_crc32(&cf_file.path)?;
            } else {
                // Clean up the `tmp_path` if this cf file is empty.
                delete_file_if_exist(&cf_file.tmp_path).unwrap();
            }
        }
        Ok(())
    }

    fn save_meta_file(&mut self) -> RaftStoreResult<()> {
        let mut v = vec![];
        box_try!(self.meta_file.meta.write_to_vec(&mut v));
        {
            let mut f = self.meta_file.file.take().unwrap();
            f.write_all(&v[..])?;
            f.flush()?;
        }
        fs::rename(&self.meta_file.tmp_path, &self.meta_file.path)?;
        self.hold_tmp_files = false;
        Ok(())
    }

    fn do_build(
        &mut self,
        snap: &DbSnapshot,
        region: &Region,
        stat: &mut SnapshotStatistics,
        deleter: Box<dyn SnapshotDeleter>,
    ) -> RaftStoreResult<()> {
        fail_point!("snapshot_enter_do_build");
        if self.exists() {
            match self.validate(snap.get_db()) {
                Ok(()) => return Ok(()),
                Err(e) => {
                    error!(
                        "snapshot is corrupted, will rebuild";
                        "region_id" => region.get_id(),
                        "snapshot" => %self.path(),
                        "err" => ?e,
                    );
                    if !retry_delete_snapshot(deleter, &self.key, self) {
                        error!(
                            "failed to delete corrupted snapshot because it's \
                             already registered elsewhere";
                            "region_id" => region.get_id(),
                            "snapshot" => %self.path(),
                        );
                        return Err(e);
                    }
                    self.init_for_building(snap)?;
                }
            }
        }

        let mut snap_key_count = 0;
        let (begin_key, end_key) = (enc_start_key(region), enc_end_key(region));
        for cf in SNAPSHOT_CFS {
            self.switch_to_cf_file(cf)?;
            let (cf_key_count, cf_size) = if plain_file_used(cf) {
                self.build_plain_cf_file(snap, cf, &begin_key, &end_key)?
            } else {
                let mut key_count = 0;
                let mut size = 0;
                let base = self
                    .limiter
                    .as_ref()
                    .map_or(0 as i64, |l| l.get_max_bytes_per_time());
                let mut bytes: i64 = 0;
                snap.scan_cf(cf, &begin_key, &end_key, false, |key, value| {
                    let l = key.len() + value.len();
                    if let Some(ref limiter) = self.limiter {
                        if bytes >= base {
                            bytes = 0;
                            limiter.request(base);
                        }
                        bytes += l as i64;
                    }
                    size += l;
                    key_count += 1;
                    box_try!(self.add_kv(key, value));
                    Ok(true)
                })?;
                (key_count, size)
            };
            snap_key_count += cf_key_count;
            SNAPSHOT_CF_KV_COUNT
                .with_label_values(&[cf])
                .observe(cf_key_count as f64);
            SNAPSHOT_CF_SIZE
                .with_label_values(&[cf])
                .observe(cf_size as f64);
            info!(
                "scan snapshot of one cf";
                "region_id" => region.get_id(),
                "snapshot" => self.path(),
                "cf" => cf,
                "key_count" => cf_key_count,
                "size" => cf_size,
            );
        }

        self.save_cf_files()?;
        stat.kv_count = snap_key_count;
        // save snapshot meta to meta file
        let snapshot_meta = gen_snapshot_meta(&self.cf_files[..])?;
        self.meta_file.meta = snapshot_meta;
        self.save_meta_file()?;

        Ok(())
    }

    fn build_plain_cf_file(
        &mut self,
        snap: &DbSnapshot,
        cf: &str,
        start_key: &[u8],
        end_key: &[u8],
    ) -> RaftStoreResult<(usize, usize)> {
        let mut cf_key_count = 0;
        let mut cf_size = 0;
        {
            // If the relative files are deleted after `Snap::new` and
            // `init_for_building`, the file could be None.
            let file = match self.cf_files[self.cf_index].file.as_mut() {
                Some(f) => f,
                None => {
                    let e = box_err!("cf_file is none for cf {}", cf);
                    return Err(RaftStoreError::Snapshot(e));
                }
            };
            snap.scan_cf(cf, start_key, end_key, false, |key, value| {
                cf_key_count += 1;
                cf_size += key.len() + value.len();
                box_try!(file.encode_compact_bytes(key));
                box_try!(file.encode_compact_bytes(value));
                Ok(true)
            })?;
            if cf_key_count > 0 {
                // use an empty byte array to indicate that cf reaches an end.
                box_try!(file.encode_compact_bytes(b""));
            }
        }

        // update kv count for plain file
        let cf_file = &mut self.cf_files[self.cf_index];
        cf_file.kv_count = cf_key_count as u64;
        Ok((cf_key_count, cf_size))
    }
}

fn apply_plain_cf_file<D: CompactBytesFromFileDecoder>(
    decoder: &mut D,
    options: &ApplyOptions,
    handle: &CFHandle,
) -> Result<()> {
    let wb = WriteBatch::new();
    let mut batch_size = 0;
    loop {
        check_abort(&options.abort)?;
        let key = box_try!(decoder.decode_compact_bytes());
        if key.is_empty() {
            if batch_size > 0 {
                box_try!(options.db.write(&wb));
            }
            break;
        }
        box_try!(check_key_in_region(keys::origin_key(&key), &options.region));
        batch_size += key.len();
        let value = box_try!(decoder.decode_compact_bytes());
        batch_size += value.len();
        box_try!(wb.put_cf(handle, &key, &value));
        if batch_size >= options.write_batch_size {
            box_try!(options.db.write(&wb));
            wb.clear();
            batch_size = 0;
        }
    }
    Ok(())
}

impl fmt::Debug for Snap {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.debug_struct("Snap")
            .field("key", &self.key)
            .field("display_path", &self.display_path)
            .finish()
    }
}

impl Snapshot for Snap {
    fn build(
        &mut self,
        snap: &DbSnapshot,
        region: &Region,
        snap_data: &mut RaftSnapshotData,
        stat: &mut SnapshotStatistics,
        deleter: Box<dyn SnapshotDeleter>,
    ) -> RaftStoreResult<()> {
        let t = Instant::now();
        self.do_build(snap, region, stat, deleter)?;

        let total_size = self.total_size()?;
        stat.size = total_size;
        // set snapshot meta data
        snap_data.set_file_size(total_size);
        snap_data.set_version(SNAPSHOT_VERSION);
        snap_data.set_meta(self.meta_file.meta.clone());

        SNAPSHOT_BUILD_TIME_HISTOGRAM.observe(duration_to_sec(t.elapsed()) as f64);
        info!(
            "scan snapshot";
            "region_id" => region.get_id(),
            "snapshot" => self.path(),
            "key_count" => stat.kv_count,
            "size" => total_size,
            "takes" => ?t.elapsed(),
        );

        Ok(())
    }

    fn path(&self) -> &str {
        &self.display_path
    }

    fn exists(&self) -> bool {
        self.cf_files
            .iter()
            .all(|cf_file| cf_file.size == 0 || file_exists(&cf_file.path))
            && file_exists(&self.meta_file.path)
    }

    fn delete(&self) {
        debug!(
            "deleting snapshot file";
            "snapshot" => %self.path(),
        );
        for cf_file in &self.cf_files {
            delete_file_if_exist(&cf_file.clone_path).unwrap();
            if self.hold_tmp_files {
                delete_file_if_exist(&cf_file.tmp_path).unwrap();
            }
            if delete_file_if_exist(&cf_file.path).unwrap() {
                self.size_track.fetch_sub(cf_file.size, Ordering::SeqCst);
            }
        }
        delete_file_if_exist(&self.meta_file.path).unwrap();
        if self.hold_tmp_files {
            delete_file_if_exist(&self.meta_file.tmp_path).unwrap();
        }
    }

    fn meta(&self) -> io::Result<Metadata> {
        fs::metadata(&self.meta_file.path)
    }

    fn total_size(&self) -> io::Result<u64> {
        Ok(self.cf_files.iter().fold(0, |acc, x| acc + x.size))
    }

    fn save(&mut self) -> io::Result<()> {
        debug!(
            "saving to snapshot file";
            "snapshot" => %self.path(),
        );
        for cf_file in &mut self.cf_files {
            if cf_file.size == 0 {
                // Skip empty cf file.
                continue;
            }

            // Check each cf file has been fully written, and the checksum matches.
            {
                let mut file = cf_file.file.take().unwrap();
                file.flush()?;
            }
            if cf_file.written_size != cf_file.size {
                return Err(io::Error::new(
                    ErrorKind::Other,
                    format!(
                        "snapshot file {} for cf {} size mismatches, \
                         real size {}, expected size {}",
                        cf_file.path.display(),
                        cf_file.cf,
                        cf_file.written_size,
                        cf_file.size
                    ),
                ));
            }
            let checksum = cf_file.write_digest.as_ref().unwrap().sum32();
            if checksum != cf_file.checksum {
                return Err(io::Error::new(
                    ErrorKind::Other,
                    format!(
                        "snapshot file {} for cf {} checksum \
                         mismatches, real checksum {}, expected \
                         checksum {}",
                        cf_file.path.display(),
                        cf_file.cf,
                        checksum,
                        cf_file.checksum
                    ),
                ));
            }

            fs::rename(&cf_file.tmp_path, &cf_file.path)?;
            self.size_track.fetch_add(cf_file.size, Ordering::SeqCst);
        }
        // write meta file
        let mut v = vec![];
        self.meta_file.meta.write_to_vec(&mut v)?;
        {
            let mut meta_file = self.meta_file.file.take().unwrap();
            meta_file.write_all(&v[..])?;
            meta_file.sync_all()?;
        }
        fs::rename(&self.meta_file.tmp_path, &self.meta_file.path)?;
        self.hold_tmp_files = false;
        Ok(())
    }

    fn apply(&mut self, options: ApplyOptions) -> Result<()> {
        box_try!(self.validate(Arc::clone(&options.db)));

        for cf_file in &mut self.cf_files {
            if cf_file.size == 0 {
                // Skip empty cf file.
                continue;
            }

            check_abort(&options.abort)?;
            let cf_handle = box_try!(rocks::util::get_cf_handle(&options.db, cf_file.cf));
            if plain_file_used(cf_file.cf) {
                let file = box_try!(File::open(&cf_file.path));
                apply_plain_cf_file(&mut BufReader::new(file), &options, cf_handle)?;
            } else {
                let _timer = INGEST_SST_DURATION_SECONDS.start_coarse_timer();
                let mut ingest_opt = IngestExternalFileOptions::new();
                ingest_opt.move_files(true);
                let path = cf_file.clone_path.to_str().unwrap();
                box_try!(options.db.ingest_external_file_optimized(
                    cf_handle,
                    &ingest_opt,
                    &[path]
                ));
            }
        }
        Ok(())
    }
}

impl Read for Snap {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        if buf.is_empty() {
            return Ok(0);
        }
        while self.cf_index < self.cf_files.len() {
            let cf_file = &mut self.cf_files[self.cf_index];
            if cf_file.size == 0 {
                self.cf_index += 1;
                continue;
            }
            match cf_file.file.as_mut().unwrap().read(buf) {
                Ok(0) => {
                    // EOF. Switch to next file.
                    self.cf_index += 1;
                }
                Ok(n) => {
                    return Ok(n);
                }
                e => return e,
            }
        }
        Ok(0)
    }
}

impl Write for Snap {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        if buf.is_empty() {
            return Ok(0);
        }

        let mut next_buf = buf;
        while self.cf_index < self.cf_files.len() {
            let cf_file = &mut self.cf_files[self.cf_index];
            if cf_file.size == 0 {
                self.cf_index += 1;
                continue;
            }

            let left = (cf_file.size - cf_file.written_size) as usize;
            if left == 0 {
                self.cf_index += 1;
                continue;
            }

            let mut file = LimitWriter::new(self.limiter.clone(), cf_file.file.as_mut().unwrap());
            let digest = cf_file.write_digest.as_mut().unwrap();

            if next_buf.len() > left {
                file.write_all(&next_buf[0..left])?;
                digest.write(&next_buf[0..left]);
                cf_file.written_size += left as u64;
                self.cf_index += 1;
                next_buf = &next_buf[left..];
            } else {
                file.write_all(next_buf)?;
                digest.write(next_buf);
                cf_file.written_size += next_buf.len() as u64;
                return Ok(buf.len());
            }
        }
        let n = buf.len() - next_buf.len();
        Ok(n)
    }

    fn flush(&mut self) -> io::Result<()> {
        if let Some(cf_file) = self.cf_files.get_mut(self.cf_index) {
            let file = cf_file.file.as_mut().unwrap();
            file.flush()?;
        }
        Ok(())
    }
}

impl Drop for Snap {
    fn drop(&mut self) {
        // cleanup if some of the cf files and meta file is partly written
        if self
            .cf_files
            .iter()
            .any(|cf_file| file_exists(&cf_file.tmp_path))
            || file_exists(&self.meta_file.tmp_path)
        {
            self.delete();
            return;
        }
        // cleanup if data corruption happens and any file goes missing
        if !self.exists() {
            self.delete();
            return;
        }
    }
}

#[derive(PartialEq, Debug)]
pub enum SnapEntry {
    Generating = 1,
    Sending = 2,
    Receiving = 3,
    Applying = 4,
}

/// `SnapStats` is for snapshot statistics.
pub struct SnapStats {
    pub sending_count: usize,
    pub receiving_count: usize,
}

