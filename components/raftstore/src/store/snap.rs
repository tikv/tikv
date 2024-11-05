// Copyright 2017 TiKV Project Authors. Licensed under Apache-2.0.
use std::{
    borrow::Cow,
    cell::RefCell,
    cmp::{self, Ordering as CmpOrdering, Reverse},
    error::Error as StdError,
    fmt::{self, Display, Formatter},
    io::{self, Cursor, ErrorKind, Read, Write},
    path::{Path, PathBuf},
    result, str,
    sync::{
        atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering},
        Arc, Mutex, RwLock,
    },
    thread::{self, current},
    time::{self, Duration},
    u64,
};

use collections::{HashMap, HashMapEntry as Entry};
use encryption::{create_aes_ctr_crypter, DataKeyManager, Iv};
use engine_traits::{
    name_to_cf, CfName, Iterable, KvEngine, SstCompressionType, SstWriter, SstWriterBuilder,
    CF_DEFAULT, CF_LOCK, CF_WRITE,
};
use error_code::{self, ErrorCode, ErrorCodeExt};
use fail::fail_point;
use file_system::{
    calc_crc32, calc_crc32_and_size, calc_crc32_bytes, delete_dir_if_exist, delete_file_if_exist,
    file_exists, get_file_size, sync_dir, File, Metadata, OpenOptions,
};
use keys::{enc_end_key, enc_start_key};
use kvproto::{
    encryptionpb::EncryptionMethod,
    metapb::Region,
    pdpb::SnapshotStat,
    raft_serverpb::{RaftSnapshotData, SnapshotCfFile, SnapshotMeta},
};
use openssl::symm::{Cipher, Crypter, Mode};
use protobuf::Message;
use raft::eraftpb::Snapshot as RaftSnapshot;
use thiserror::Error;
use tikv_util::{
    box_err, box_try,
    codec::bytes::BytesEncoder,
    debug, error, info,
    time::{duration_to_sec, Instant, Limiter, UnixSecs},
    warn, HandyRwLock,
};

use crate::{
    coprocessor::CoprocessorHost,
    store::{metrics::*, peer_storage::JOB_STATUS_CANCELLING},
    Error as RaftStoreError, Result as RaftStoreResult,
};

#[path = "snap/io.rs"]
pub mod snap_io;

// Data in CF_RAFT should be excluded for a snapshot.
pub const SNAPSHOT_CFS: &[CfName] = &[CF_DEFAULT, CF_LOCK, CF_WRITE];
pub const SNAPSHOT_CFS_ENUM_PAIR: &[(CfNames, CfName)] = &[
    (CfNames::default, CF_DEFAULT),
    (CfNames::lock, CF_LOCK),
    (CfNames::write, CF_WRITE),
];
pub const SNAPSHOT_VERSION: u64 = 2;
pub const TABLET_SNAPSHOT_VERSION: u64 = 3;
pub const IO_LIMITER_CHUNK_SIZE: usize = 4 * 1024;

/// Name prefix for the self-generated snapshot file.
const SNAP_GEN_PREFIX: &str = "gen";
/// Name prefix for the received snapshot file.
const SNAP_REV_PREFIX: &str = "rev";
const DEL_RANGE_PREFIX: &str = "del_range";

const TMP_FILE_SUFFIX: &str = ".tmp";
const SST_FILE_SUFFIX: &str = ".sst";
const CLONE_FILE_SUFFIX: &str = ".clone";
const META_FILE_SUFFIX: &str = ".meta";

const DELETE_RETRY_MAX_TIMES: u32 = 6;
const DELETE_RETRY_TIME_MILLIS: u64 = 500;

// TTL for the recv snap concurrency limiter, specified in seconds. This TTL
// should be longer than the typical snapshot generation and transmission time.
// If the TTL is too short, the limiter might permit more snapshots than
// expected to be sent, leading to the receiver dropping them and the sender
// regenerating them, which is what the concurrency limiter is designed to
// prevent.
const RECV_SNAP_CONCURRENCY_LIMITER_TTL_SECS: u64 = 60;

#[derive(Debug, Error)]
pub enum Error {
    #[error("abort")]
    Abort,

    #[error("too many snapshots")]
    TooManySnapshots,

    #[error("snap failed {0:?}")]
    Other(#[from] Box<dyn StdError + Sync + Send>),
}

impl From<io::Error> for Error {
    fn from(e: io::Error) -> Self {
        Error::Other(Box::new(e))
    }
}

impl From<engine_traits::Error> for Error {
    fn from(e: engine_traits::Error) -> Self {
        Error::Other(Box::new(e))
    }
}

pub type Result<T> = result::Result<T, Error>;

impl ErrorCodeExt for Error {
    fn error_code(&self) -> ErrorCode {
        match self {
            Error::Abort => error_code::raftstore::SNAP_ABORT,
            Error::TooManySnapshots => error_code::raftstore::SNAP_TOO_MANY,
            Error::Other(_) => error_code::raftstore::SNAP_UNKNOWN,
        }
    }
}

// TODO: use `with_added_extension` of `PathBuf` after rust toolchain is updated
fn with_added_extension(path: &PathBuf, ext: &str) -> PathBuf {
    let mut new_path = path.clone();
    let new_ext = format!("{}.{}", path.extension().unwrap().to_str().unwrap(), ext);
    new_path.set_extension(new_ext);
    new_path
}

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
        let mut snap_data = RaftSnapshotData::default();
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
pub struct ApplyOptions<EK>
where
    EK: KvEngine,
{
    pub db: EK,
    pub region: Region,
    pub abort: Arc<AtomicUsize>,
    pub write_batch_size: usize,
    pub coprocessor_host: CoprocessorHost<EK>,
    pub ingest_copy_symlink: bool,
}

// A helper function to copy snapshot.
// Only used in tests.
pub fn copy_snapshot(mut from: &Arc<Snapshot>, mut to: &Arc<Snapshot>) -> io::Result<()> {
    if !to.exists() {
        io::copy(&mut &**from, &mut &**to)?;
        to.save()?;
    }
    Ok(())
}

// Try to delete the specified snapshot, return true if the deletion is done.
fn retry_delete_snapshot(mgr: &SnapManagerCore, key: &SnapKey, snap: &Snapshot) -> bool {
    let d = time::Duration::from_millis(DELETE_RETRY_TIME_MILLIS);
    for _ in 0..DELETE_RETRY_MAX_TIMES {
        if mgr.delete_snapshot(key, snap) {
            return true;
        }
        thread::sleep(d);
    }
    false
}

fn calc_checksum_and_size(
    path: &Path,
    encryption_key_manager: Option<&Arc<DataKeyManager>>,
) -> RaftStoreResult<(u32, u64)> {
    let (checksum, size) = if let Some(mgr) = encryption_key_manager {
        // Crc32 and file size need to be calculated based on decrypted contents.
        let file_name = path.to_str().unwrap();
        let mut r = snap_io::get_decrypter_reader(file_name, mgr)?;
        calc_crc32_and_size(&mut r)?
    } else {
        (calc_crc32(path)?, get_file_size(path)?)
    };
    Ok((checksum, size))
}

fn check_file_size(got_size: u64, expected_size: u64, path: &Path) -> RaftStoreResult<()> {
    if got_size != expected_size {
        return Err(box_err!(
            "invalid size {} for snapshot cf file {}, expected {}",
            got_size,
            path.display(),
            expected_size
        ));
    }
    Ok(())
}

fn check_file_checksum(
    got_checksum: u32,
    expected_checksum: u32,
    path: &Path,
) -> RaftStoreResult<()> {
    if got_checksum != expected_checksum {
        return Err(box_err!(
            "invalid checksum {} for snapshot cf file {}, expected {}",
            got_checksum,
            path.display(),
            expected_checksum
        ));
    }
    Ok(())
}

fn check_file_size_and_checksum(
    path: &Path,
    expected_size: u64,
    expected_checksum: u32,
    encryption_key_manager: Option<&Arc<DataKeyManager>>,
) -> RaftStoreResult<()> {
    let (checksum, size) = calc_checksum_and_size(path, encryption_key_manager)?;
    check_file_size(size, expected_size, path)?;
    check_file_checksum(checksum, expected_checksum, path)?;
    Ok(())
}

struct CfFileForReceiving {
    file: File,
    encrypter: Option<(Cipher, Crypter)>,
    written_size: u64,
    write_digest: crc32fast::Hasher,
    path: PathBuf,
    size: u64,
    checksum: u32,
}

impl CfFileForReceiving {
    pub fn save(mut self) -> io::Result<()> {
        self.file.flush()?;
        self.file.sync_all()?;

        if self.written_size != self.size {
            return Err(io::Error::new(
                ErrorKind::InvalidData,
                format!(
                    "snapshot file {} size mismatches, \
                    real size {}, expected size {}",
                    self.path.display(),
                    self.written_size,
                    self.size
                ),
            ));
        }

        let checksum = self.write_digest.finalize();
        if checksum != self.checksum {
            return Err(io::Error::new(
                ErrorKind::InvalidData,
                format!(
                    "snapshot file {} checksum mismatches, \
                    real checksum {}, expected checksum {}",
                    self.path.display(),
                    checksum,
                    self.checksum
                ),
            ));
        }
        // remove the tmp suffix to mark the file finalized
        file_system::rename(
            with_added_extension(&self.path, TMP_FILE_SUFFIX),
            &self.path,
        )
    }
}

// In memory snapshot
struct CfFileForSending {
    data: Cursor<Vec<u8>>,
    size: u64,
    checksum: u32,
}

struct CfFileForApplying {
    path: PathBuf,
    size: u64,
    checksum: u32,
}

#[derive(Default)]
pub struct CfFile {
    pub cf: CfName,
    pub kv_count: u64,
    pub file_for_sending: Vec<CfFileForSending>,
    pub file_for_receiving: Vec<CfFileForReceiving>,
    pub file_for_applying: Vec<CfFileForApplying>,
}

impl CfFile {
    pub fn new(cf: CfName) -> CfFile {
        CfFile {
            cf,
            kv_count: 0,
            file_for_sending: vec![],
            file_for_receiving: vec![],
            file_for_applying: vec![],
        }
    }
}

pub struct SnapshotFiles {
    dir_path: PathBuf,
    file_prefix: String,
    cf_files: Vec<CfFile>,
}

impl SnapshotFiles {
    pub fn new(dir_path: PathBuf, file_prefix: String) -> Self {
        SnapshotFiles {
            dir_path,
            file_prefix,
            cf_files: Vec::with_capacity(SNAPSHOT_CFS.len()),
        }
    }

    // Validate and set SnapshotMeta of this Snapshot.
    pub fn from_snapshot_meta(
        mut self,
        snapshot_meta: &SnapshotMeta,
        for_receiving: bool,
        mgr: &SnapManagerCore,
    ) -> RaftStoreResult<Self> {
        info!(
            "set_snapshot_meta total cf files count: {}",
            snapshot_meta.get_cf_files().len()
        );

        let mut current_cf = None;
        let mut cf_file = None;
        let mut cf_count = 0;
        for meta in snapshot_meta.get_cf_files() {
            // switch cf
            if current_cf == None || current_cf != Some(meta.get_cf()) {
                cf_count += 1;
                current_cf = Some(meta.get_cf());
                if let Some(cf_file) = cf_file {
                    self.cf_files.push(cf_file);
                }
                cf_file = Some(CfFile::new(name_to_cf(meta.get_cf()).unwrap()));
            }
            if meta.get_size() != 0 {
                if for_receiving {
                    self.add_file_for_receiving(
                        cf_file.as_mut().unwrap(),
                        meta.get_size(),
                        meta.get_checksum(),
                        mgr,
                    );
                } else {
                    self.add_file_for_applying(
                        cf_file.as_mut().unwrap(),
                        meta.get_size(),
                        meta.get_checksum(),
                    );
                }
            }
        }
        if let Some(cf_file) = cf_file {
            self.cf_files.push(cf_file);
        }

        if cf_count != SNAPSHOT_CFS.len() {
            return Err(box_err!(
                "invalid cf number of snapshot meta, expect {}, got {}",
                SNAPSHOT_CFS.len(),
                cf_count
            ));
        }
        Ok(self)
    }

    pub fn get_cf_files(&self) -> Vec<SnapshotCfFile> {
        let mut cf_files = vec![];
        for cf_file in self.cf_files.iter() {
            for file in cf_file.file_for_sending.iter() {
                let mut f = SnapshotCfFile::default();
                f.set_cf(cf_file.cf.to_string());
                f.set_size(file.size);
                f.set_checksum(file.checksum);
                cf_files.push(f);
            }
        }
        cf_files
    }

    fn gen_file_name(&self, cf: CfName, file_id: usize, suffix: &str) -> String {
        if file_id == 0 {
            // for backward compatibility
            format!("{}_{}{}", self.file_prefix, cf, suffix)
        } else {
            assert!(file_id < 10000);
            format!("{}_{}_{:04}{}", self.file_prefix, cf, file_id, suffix)
        }
    }

    /// Build a snapshot file for the given column family in plain format.
    /// If there are no key-value pairs fetched, no files will be created at
    /// `path`, otherwise the file will be created and synchronized.
    pub fn build_plain_cf_file<EK: KvEngine>(
        &mut self,
        cf: CfName,
        snap: &EK::Snapshot,
        start_key: &[u8],
        end_key: &[u8],
    ) -> Result<BuildStats> {
        let mut data = vec![];
        let mut stats = BuildStats::default();
        box_try!(snap.scan(cf, start_key, end_key, false, |key, value| {
            stats.total_count += 1;
            stats.total_size += key.len() + value.len();
            box_try!(BytesEncoder::encode_compact_bytes(&mut data, key));
            box_try!(BytesEncoder::encode_compact_bytes(&mut data, value));
            Ok(true)
        }));

        if stats.total_count > 0 {
            box_try!(BytesEncoder::encode_compact_bytes(&mut data, b""));
        }
        let mut cf_file = CfFile::new(cf);
        cf_file.kv_count = stats.total_count;
        self.add_file_for_sending(&mut cf_file, data);
        self.cf_files.push(cf_file);

        Ok(stats)
    }

    /// Build a snapshot file for the given column family in sst format.
    /// If there are no key-value pairs fetched, no files will be created at
    /// `path`, otherwise the file will be created and synchronized.
    pub fn build_sst_cf_file_list<EK: KvEngine>(
        &mut self,
        cf: CfName,
        engine: &EK,
        snap: &EK::Snapshot,
        start_key: &[u8],
        end_key: &[u8],
        raw_size_per_file: u64,
        io_limiter: &Limiter,
    ) -> Result<BuildStats> {
        let instant = Instant::now();
        let mut stats = BuildStats::default();
        let mut cf_file = CfFile::new(cf);

        let mut name = self.gen_file_name(cf, 0, SST_FILE_SUFFIX);
        let sst_writer = RefCell::new(create_sst_file_writer::<EK>(engine, cf, &name)?);
        let mut finish_sst_writer = |mut sst_writer: EK::SstWriter| -> Result<Vec<u8>> {
            let mut buf = vec![];
            let (_, mut sst_reader) = sst_writer.finish_read()?;
            sst_reader.read_to_end(&mut buf)?;
            Ok(buf)
        };

        let mut remained_quota = 0;
        let mut file_length: usize = 0;
        box_try!(snap.scan(cf, start_key, end_key, false, |key, value| {
            let entry_len = key.len() + value.len();
            if file_length + entry_len > raw_size_per_file as usize {
                file_length = 0;
                name = self.gen_file_name(cf, cf_file.file_for_sending.len() + 1, SST_FILE_SUFFIX);
                match create_sst_file_writer::<EK>(engine, cf, &name) {
                    Ok(new_sst_writer) => {
                        let old_writer = sst_writer.replace(new_sst_writer);
                        let buf = box_try!(finish_sst_writer(old_writer));
                        self.add_file_for_sending(&mut cf_file, buf);
                    }
                    Err(e) => {
                        let io_error = io::Error::new(ErrorKind::Other, e);
                        return Err(io_error.into());
                    }
                }
            }

            while entry_len > remained_quota {
                // It's possible to acquire more than necessary, but let it be.
                io_limiter.blocking_consume(IO_LIMITER_CHUNK_SIZE);
                remained_quota += IO_LIMITER_CHUNK_SIZE;
            }
            remained_quota -= entry_len;

            stats.total_count += 1;
            stats.total_size += entry_len;
            if let Err(e) = sst_writer.borrow_mut().put(key, value) {
                let io_error = io::Error::new(ErrorKind::Other, e);
                return Err(io_error.into());
            }

            file_length += entry_len;
            Ok(true)
        }));
        let buf = if stats.total_count > 0 {
            let buf = finish_sst_writer(sst_writer.into_inner())?;
            info!(
                "build_sst_cf_file_list builds {} files in cf {}. Total keys {}, total size {}. raw_size_per_file {}, total takes {:?}",
                cf_file.file_for_sending.len(),
                cf,
                stats.total_count,
                stats.total_size,
                raw_size_per_file,
                instant.saturating_elapsed(),
            );
            buf
        } else {
            vec![]
        };
        cf_file.kv_count = stats.total_count;
        self.add_file_for_sending(&mut cf_file, buf);
        self.cf_files.push(cf_file);

        Ok(stats)
    }

    fn add_file_for_sending(&mut self, cf_file: &mut CfFile, buf: Vec<u8>) {
        cf_file.file_for_sending.push(CfFileForSending {
            checksum: calc_crc32_bytes(&buf),
            size: buf.len() as u64,
            data: Cursor::new(buf),
        });
    }

    fn add_file_for_applying(&mut self, cf_file: &mut CfFile, size: u64, checksum: u32) {
        let file_path = self.dir_path.join(self.gen_file_name(
            cf_file.cf,
            cf_file.file_for_applying.len(),
            SST_FILE_SUFFIX,
        ));
        cf_file.file_for_applying.push(CfFileForApplying {
            path: file_path,
            size,
            checksum,
        });
    }

    fn add_file_for_receiving(
        &mut self,
        cf_file: &mut CfFile,
        size: u64,
        checksum: u32,
        mgr: &SnapManagerCore,
    ) -> RaftStoreResult<()> {
        let file_path = self.dir_path.join(self.gen_file_name(
            cf_file.cf,
            cf_file.file_for_receiving.len(),
            SST_FILE_SUFFIX,
        ));
        let file = OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(with_added_extension(&file_path, TMP_FILE_SUFFIX))?;
        let encrypter = if let Some(mgr) = &mgr.encryption_key_manager {
            let enc_info = mgr.new_file(file_path.to_str().unwrap())?;
            let mthd = enc_info.method;
            if mthd != EncryptionMethod::Plaintext {
                Some(
                    create_aes_ctr_crypter(
                        mthd,
                        &enc_info.key,
                        Mode::Encrypt,
                        Iv::from_slice(&enc_info.iv)?,
                    )
                    .map_err(|e| RaftStoreError::Snapshot(box_err!(e)))?,
                )
            } else {
                None
            }
        } else {
            None
        };

        cf_file.file_for_receiving.push(CfFileForReceiving {
            file,
            encrypter,
            written_size: 0,
            write_digest: crc32fast::Hasher::new(),
            path: file_path,
            size,
            checksum,
        });
        Ok(())
    }

    fn save(&mut self) -> io::Result<()> {
        for cf_file in &mut self.cf_files {
            if cf_file.file_for_receiving.is_empty() {
                // Skip empty cf file.
                continue;
            }

            // Check each cf file has been fully written, and the checksum matches.
            for (mut file) in cf_file.file_for_receiving.drain(..) {
                cf_file.file_for_applying.push(CfFileForApplying {
                    path: file.path.clone(),
                    size: file.size,
                    checksum: file.checksum,
                });
                file.save()?;
            }
        }
        Ok(())
    }

    fn delete(&self, mgr: &SnapManagerCore) -> io::Result<()> {
        for cf_file in &self.cf_files {
            for file in &cf_file.file_for_receiving {
                delete_file_if_exist(&file.path)?;
                if let Some(ref mgr) = mgr.encryption_key_manager {
                    mgr.delete_file(&file.path.to_str().unwrap(), None)?;
                }
                delete_file_if_exist(with_added_extension(&file.path, TMP_FILE_SUFFIX))?;
            }

            for file in &cf_file.file_for_applying {
                delete_file_if_exist(&file.path)?;
                if let Some(ref mgr) = mgr.encryption_key_manager {
                    mgr.delete_file(file.path.to_str().unwrap(), None)?;
                }
                // new_for_applying is also used for gc, so tmp files may exist.
                delete_file_if_exist(with_added_extension(&file.path, TMP_FILE_SUFFIX))?;
                delete_file_if_exist(with_added_extension(&file.path, CLONE_FILE_SUFFIX))?;
            }
        }
        Ok(())
    }
}

fn create_sst_file_writer<E>(engine: &E, cf: CfName, path: &str) -> Result<E::SstWriter>
where
    E: KvEngine,
{
    let builder = E::SstWriterBuilder::new()
        .set_in_memory(true)
        .set_db(engine)
        .set_cf(cf)
        .set_compression_type(Some(SstCompressionType::Zstd));
    let writer = box_try!(builder.build(path));
    Ok(writer)
}

#[derive(Default)]
pub struct BuildStats {
    pub total_count: u64,
    pub total_size: usize,
}

struct MetaFile {
    pub file: File,
    pub meta: SnapshotMeta,
    pub path: PathBuf,
}

impl MetaFile {
    pub fn save(&mut self) -> io::Result<()> {
        let mut buf = vec![];
        self.meta.write_to_vec(&mut buf)?;
        self.file.write_all(&buf)?;
        self.file.sync_all()?;
        // remove the tmp suffix to mark the file finalized
        file_system::rename(
            with_added_extension(&self.path, TMP_FILE_SUFFIX),
            &self.path,
        )
    }

    fn delete(&self) -> io::Result<()> {
        if !self.meta.tablet_snap_path.is_empty() {
            delete_dir_if_exist(&self.meta.tablet_snap_path)?;
        }
        delete_file_if_exist(&self.path)?;
        delete_file_if_exist(with_added_extension(&self.path, TMP_FILE_SUFFIX))?;
        Ok(())
    }
}
pub enum SnapshotInner {
    Building,
    Sending {
        // for each CF, there may be multiple files
        data: SnapshotFiles,

        // read cursor related
        cf_index: usize,
        cf_file_index: usize,
    },
    Receiving {
        data: SnapshotFiles,
        meta_file: MetaFile,

        // write cursor related
        cf_index: usize,
        cf_file_index: usize,
    },
    Applying {
        data: SnapshotFiles,
        meta_file: MetaFile,
    },
}

pub struct SnapshotInfo {
    dir_path: PathBuf,
    display_path: String,
    snap_key: SnapKey,

    mgr: SnapManagerCore,
}

pub struct Snapshot {
    info: SnapshotInfo,
    pub inner: Mutex<SnapshotInner>,
}

impl fmt::Display for Snapshot {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.debug_struct("Snapshot")
            .field("snap_key", &self.info.snap_key)
            .field("display_path", &self.info.display_path)
            .finish()
    }
}

impl Snapshot {
    fn new_for_building<T: Into<PathBuf>>(
        dir_path: T,
        key: &SnapKey,
        mgr: &SnapManagerCore,
    ) -> Self {
        let dir_path = dir_path.into();
        let info = SnapshotInfo {
            display_path: Self::get_display_path(&dir_path, SNAP_GEN_PREFIX),
            dir_path,
            snap_key: key.clone(),
            mgr: mgr.clone(),
        };
        Snapshot {
            info,
            inner: Mutex::new(SnapshotInner::Building),
        }
    }

    pub fn build<EK: KvEngine>(
        &self,
        engine: &EK,
        kv_snap: &EK::Snapshot,
        region: &Region,
        allow_multi_files_snapshot: bool,
        for_balance: bool,
        start: UnixSecs,
    ) -> RaftStoreResult<RaftSnapshotData> {
        let info = &self.info;
        fail_point!("snapshot_enter_do_build");
        let t = Instant::now();

        let (begin_key, end_key) = (enc_start_key(region), enc_end_key(region));
        let mut total_size = 0;
        let mut total_count = 0;

        let prefix = format!("{}_{}", SNAP_GEN_PREFIX, info.snap_key);
        let mut cf_files = SnapshotFiles::new(info.dir_path.clone(), prefix);
        for (cf_enum, cf) in SNAPSHOT_CFS_ENUM_PAIR {
            let cf_stats = if plain_file_used(cf) {
                cf_files.build_plain_cf_file::<EK>(cf, kv_snap, &begin_key, &end_key)?
            } else {
                cf_files.build_sst_cf_file_list::<EK>(
                    cf,
                    engine,
                    kv_snap,
                    &begin_key,
                    &end_key,
                    info.mgr
                        .get_actual_max_per_file_size(allow_multi_files_snapshot),
                    &info.mgr.limiter,
                )?
            };
            total_size += cf_stats.total_size;
            total_count += cf_stats.total_count;

            SNAPSHOT_LIMIT_GENERATE_BYTES.inc_by(cf_stats.total_size as u64);
            SNAPSHOT_CF_KV_COUNT
                .get(*cf_enum)
                .observe(cf_stats.total_count as f64);
            SNAPSHOT_CF_SIZE
                .get(*cf_enum)
                .observe(cf_stats.total_size as f64);
            info!(
                "scan snapshot of one cf";
                "region_id" => region.get_id(),
                "snapshot" => &info.display_path,
                "cf" => cf,
                "key_count" => cf_stats.total_count,
                "size" => cf_stats.total_size,
            );
        }

        let mut snapshot_meta = SnapshotMeta::default();
        snapshot_meta.set_cf_files(cf_files.get_cf_files().into());
        snapshot_meta.set_for_balance(for_balance);
        snapshot_meta.set_start(start.into_inner());
        snapshot_meta.set_generate_duration_sec(t.saturating_elapsed().as_secs());

        let mut snapshot_msg_payload = RaftSnapshotData::default();
        snapshot_msg_payload.set_region(region.clone());
        snapshot_msg_payload.set_file_size(total_size as u64);
        snapshot_msg_payload.set_version(SNAPSHOT_VERSION);
        snapshot_msg_payload.set_meta(snapshot_meta);

        *self.inner.lock().unwrap() = SnapshotInner::Sending {
            data: cf_files,
            cf_index: 0,
            cf_file_index: 0,
        };

        SNAPSHOT_BUILD_TIME_HISTOGRAM.observe(duration_to_sec(t.saturating_elapsed()));
        SNAPSHOT_KV_COUNT_HISTOGRAM.observe(total_count as f64);
        SNAPSHOT_SIZE_HISTOGRAM.observe(total_size as f64);
        info!(
            "scan snapshot";
            "region_id" => region.get_id(),
            "snapshot" => %info.snap_key,
            "key_count" => total_count,
            "size" => total_size,
            "takes" => ?t.saturating_elapsed(),
        );

        Ok(snapshot_msg_payload)
    }

    fn new_for_receiving<T: Into<PathBuf>>(
        dir: T,
        key: &SnapKey,
        mgr: &SnapManagerCore,
        snapshot_meta: SnapshotMeta,
    ) -> RaftStoreResult<Self> {
        let dir_path = dir.into();
        if !dir_path.exists() {
            file_system::create_dir_all(dir_path.as_path())?;
        }
        let prefix = format!("{}_{}", SNAP_REV_PREFIX, key);
        let display_path = Self::get_display_path(&dir_path, &prefix);

        let data = SnapshotFiles::new(dir_path.clone(), prefix.clone()).from_snapshot_meta(
            &snapshot_meta,
            true,
            mgr,
        )?;
        let meta_path = dir_path.join(format!("{}{}", prefix, META_FILE_SUFFIX));
        let file = OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(with_added_extension(&meta_path, TMP_FILE_SUFFIX))?;
        let meta_file = MetaFile {
            file,
            path: meta_path,
            meta: snapshot_meta,
        };

        let info = SnapshotInfo {
            dir_path,
            display_path,
            snap_key: key.clone(),
            mgr: mgr.clone(),
        };

        Ok(Snapshot {
            info,
            inner: Mutex::new(SnapshotInner::Receiving {
                data,
                meta_file,
                cf_index: 0,
                cf_file_index: 0,
            }),
        })
    }

    fn new_for_applying<T: Into<PathBuf>>(
        dir: T,
        key: &SnapKey,
        mgr: &SnapManagerCore,
    ) -> RaftStoreResult<Self> {
        let dir_path = dir.into();
        if !dir_path.exists() {
            file_system::create_dir_all(dir_path.as_path())?;
        }
        let prefix = format!("{}_{}", SNAP_REV_PREFIX, key);
        let display_path = Self::get_display_path(&dir_path, &prefix);
        let meta_path = dir_path.join(format!("{}{}", prefix, META_FILE_SUFFIX));

        let buf = file_system::read(&meta_path)?;
        let mut snapshot_meta = SnapshotMeta::default();
        snapshot_meta.merge_from_bytes(&buf)?;

        let meta_file = MetaFile {
            file: OpenOptions::new().write(true).open(&meta_path)?,
            meta: snapshot_meta,
            path: meta_path,
        };

        let info = SnapshotInfo {
            dir_path,
            display_path,
            snap_key: key.clone(),
            mgr: mgr.clone(),
        };
        let prefix = format!("{}_{}", SNAP_REV_PREFIX, key);
        let data = SnapshotFiles::new(info.dir_path.clone(), prefix).from_snapshot_meta(
            &meta_file.meta,
            false,
            mgr,
        )?;

        Ok(Snapshot {
            info,
            inner: Mutex::new(SnapshotInner::Applying { data, meta_file }),
        })
    }

    fn get_display_path(dir_path: impl AsRef<Path>, prefix: &str) -> String {
        let cf_names = "(".to_owned() + SNAPSHOT_CFS.join("|").as_str() + ")";
        format!(
            "{}/{}_{}{}",
            dir_path.as_ref().display(),
            prefix,
            cf_names,
            SST_FILE_SUFFIX
        )
    }

    fn delete(&self) -> io::Result<()> {
        debug!(
            "deleting snapshot file";
            "snapshot" => %self.path(),
        );

        match *self.inner.lock().unwrap() {
            SnapshotInner::Building { .. } => {
                // Do nothing.
            }
            SnapshotInner::Sending { ref data, .. } => {
                // Do nothing. The in memory data will be release when the
                // snapshot is dropped.
            }
            SnapshotInner::Receiving {
                ref data,
                ref meta_file,
                ..
            } => {
                data.delete(&self.info.mgr);
                meta_file.delete();
            }
            SnapshotInner::Applying {
                ref data,
                ref meta_file,
            } => {
                data.delete(&self.info.mgr);
                meta_file.delete();
            }
        };
        Ok(())
    }

    // // This is only used for v2 compatibility.
    // fn new_for_tablet_snapshot<T: Into<PathBuf>>(
    //     dir: T,
    //     key: &SnapKey,
    //     mgr: &SnapManagerCore,
    //     tablet_snapshot_path: &str,
    //     for_balance: bool,
    // ) -> RaftStoreResult<Self> {
    //     let mut s = Self::new(dir, key, false, CheckPolicy::ErrNotAllowed, mgr)?;
    //     s.init_for_building()?;
    //     let mut meta = gen_snapshot_meta(&s.cf_files[..], for_balance)?;
    //     meta.tablet_snap_path = tablet_snapshot_path.to_string();
    //     s.meta_file.meta = Some(meta);
    //     s.save_meta_file()?;
    //     Ok(s)
    // }

    // #[cfg(any(test, feature = "testexport"))]
    // pub fn tablet_snap_path(&self) -> Option<String> {
    //     Some(self.meta_file.meta.as_ref()?.tablet_snap_path.clone())
    // }

    pub fn apply<EK: KvEngine>(&self, options: ApplyOptions<EK>) -> Result<()> {
        let SnapshotInner::Applying {
            ref mut data,
            ref meta_file,
        } = *self.inner.lock().unwrap()
        else {
            unreachable!()
        };

        for cf_file in &data.cf_files {
            for file in &cf_file.file_for_applying {
                if file.size == 0 {
                    // Skip empty file. The checksum of this cf file should be 0 and
                    // this is checked when loading the snapshot meta.
                    continue;
                }

                box_try!(check_file_size_and_checksum(
                    &file.path,
                    file.size,
                    file.checksum,
                    self.info.mgr.encryption_key_manager.as_ref(),
                ));
                if !plain_file_used(cf_file.cf) {
                    if options.ingest_copy_symlink && is_symlink(&file.path)? {
                        box_try!(sst_importer::copy_sst_for_ingestion(
                            &file.path,
                            &with_added_extension(&file.path, CLONE_FILE_SUFFIX),
                            self.info.mgr.encryption_key_manager.as_deref(),
                        ));
                    } else {
                        box_try!(sst_importer::prepare_sst_for_ingestion(
                            &file.path,
                            &with_added_extension(&file.path, CLONE_FILE_SUFFIX),
                            self.info.mgr.encryption_key_manager.as_deref(),
                        ));
                    }
                }
            }
        }

        let abort_checker = ApplyAbortChecker(options.abort);
        let coprocessor_host = options.coprocessor_host;
        let region = options.region;
        let key_mgr = self.info.mgr.encryption_key_manager.as_ref();
        for cf_file in &mut data.cf_files {
            if cf_file.file_for_applying.is_empty() {
                // Skip empty cf file.
                continue;
            }
            let cf = cf_file.cf;
            if plain_file_used(cf_file.cf) {
                let path = &cf_file.file_for_applying[0].path;
                let batch_size = options.write_batch_size;
                let cb = |kv: &[(Vec<u8>, Vec<u8>)]| {
                    coprocessor_host.post_apply_plain_kvs_from_snapshot(&region, cf, kv)
                };
                snap_io::apply_plain_cf_file(
                    path.to_str().unwrap(),
                    key_mgr,
                    &abort_checker,
                    &options.db,
                    cf,
                    batch_size,
                    cb,
                )?;
            } else {
                let clone_files = cf_file
                    .file_for_applying
                    .iter()
                    .map(|f| with_added_extension(&f.path, CLONE_FILE_SUFFIX))
                    .collect::<Vec<_>>();

                let clone_file_paths: Vec<&str> =
                    clone_files.iter().map(|p| p.to_str().unwrap()).collect();
                snap_io::apply_sst_cf_file(&clone_file_paths, &options.db, cf)?;
                // TODO(@Connor1996): Make it compatible
                // coprocessor_host.post_apply_sst_from_snapshot(&region, cf,
                // file.path);
            }
        }
        Ok(())
    }

    pub fn path(&self) -> &str {
        &self.info.display_path
    }

    pub fn exists(&self) -> bool {
        let inner = self.inner.lock().unwrap();

        match *inner {
            SnapshotInner::Building { .. } => false,
            SnapshotInner::Sending { .. } => true,
            SnapshotInner::Receiving {
                ref data,
                ref meta_file,
                ..
            }
            | SnapshotInner::Applying {
                ref data,
                ref meta_file,
            } => {
                data.cf_files.iter().all(|cf_file| {
                    cf_file.file_for_receiving.is_empty()
                        || cf_file
                            .file_for_receiving
                            .iter()
                            .all(|file| file_exists(&file.path))
                }) && file_exists(&meta_file.path)
            }
        }
    }

    pub fn total_size(&self) -> u64 {
        match *self.inner.lock().unwrap() {
            SnapshotInner::Sending { ref data, .. } => data
                .cf_files
                .iter()
                .map(|cf| cf.file_for_sending.iter().map(|f| f.size).sum::<u64>())
                .sum(),
            SnapshotInner::Receiving { ref data, .. } => data
                .cf_files
                .iter()
                .map(|cf| cf.file_for_receiving.iter().map(|f| f.size).sum::<u64>())
                .sum(),
            SnapshotInner::Applying { ref data, .. } => data
                .cf_files
                .iter()
                .map(|cf| cf.file_for_applying.iter().map(|f| f.size).sum::<u64>())
                .sum(),
            SnapshotInner::Building => 0,
        }
    }

    pub fn total_count(&self) -> u64 {
        let inner = self.inner.lock().unwrap();
        match *inner {
            SnapshotInner::Sending { ref data, .. } => {
                data.cf_files.iter().map(|cf| cf.kv_count).sum()
            }
            SnapshotInner::Receiving { ref data, .. } => {
                data.cf_files.iter().map(|cf| cf.kv_count).sum()
            }
            SnapshotInner::Applying { ref data, .. } => {
                data.cf_files.iter().map(|cf| cf.kv_count).sum()
            }
            SnapshotInner::Building => 0,
        }
    }

    pub fn save(&self) -> io::Result<()> {
        let mut inner = self.inner.lock().unwrap();
        match std::mem::replace(&mut *inner, SnapshotInner::Building) {
            SnapshotInner::Receiving {
                mut data,
                mut meta_file,
                ..
            } => {
                debug!(
                    "saving to snapshot file";
                            "snapshot" => self.path(),
                );
                data.save()?;
                sync_dir(&self.info.dir_path)?;

                // write meta file
                meta_file.save()?;
                sync_dir(&self.info.dir_path)?;
                *inner = SnapshotInner::Applying { data, meta_file };
            }
            _ => unreachable!(),
        };
        Ok(())
    }
}

// To check whether a procedure about apply snapshot aborts or not.
struct ApplyAbortChecker(Arc<AtomicUsize>);
impl snap_io::StaleDetector for ApplyAbortChecker {
    fn is_stale(&self) -> bool {
        self.0.load(Ordering::Relaxed) == JOB_STATUS_CANCELLING
    }
}

impl Read for &Snapshot {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        match *self.inner.lock().unwrap() {
            SnapshotInner::Sending {
                ref mut cf_index,
                ref mut cf_file_index,
                ref mut data,
            } => {
                if buf.is_empty() {
                    return Ok(0);
                }
                while *cf_index < data.cf_files.len() {
                    let cf_file = &mut data.cf_files[*cf_index];
                    if *cf_file_index >= cf_file.file_for_sending.len() {
                        *cf_index += 1;
                        *cf_file_index = 0;
                        continue;
                    }
                    let reader = cf_file.file_for_sending.get_mut(*cf_file_index).unwrap();
                    match reader.data.read(buf) {
                        Ok(0) => {
                            // EOF. Switch to next file.
                            *cf_file_index += 1;
                        }
                        Ok(n) => return Ok(n),
                        e => return e,
                    }
                }
                Ok(0)
            }
            _ => unreachable!(),
        }
    }
}

impl Write for &Snapshot {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        match *self.inner.lock().unwrap() {
            SnapshotInner::Receiving {
                ref mut data,
                ref mut cf_index,
                ref mut cf_file_index,
                ..
            } => {
                if buf.is_empty() {
                    return Ok(0);
                }

                let (mut next_buf, mut written_bytes) = (buf, 0);
                while *cf_index < data.cf_files.len() {
                    let cf_file = &mut data.cf_files[*cf_index];
                    if *cf_file_index >= cf_file.file_for_receiving.len() {
                        *cf_file_index = 0;
                        *cf_index += 1;
                        continue;
                    }

                    let file = cf_file.file_for_receiving.get_mut(*cf_file_index).unwrap();
                    assert_ne!(file.size, 0);
                    let left = (file.size - file.written_size) as usize;
                    assert!(left > 0 && !next_buf.is_empty());
                    let (write_len, switch, finished) = match next_buf.len().cmp(&left) {
                        CmpOrdering::Greater => (left, true, false),
                        CmpOrdering::Equal => (left, true, true),
                        CmpOrdering::Less => (next_buf.len(), false, true),
                    };

                    file.write_digest.update(&next_buf[0..write_len]);
                    file.written_size += write_len as u64;
                    written_bytes += write_len;

                    let encrypt_buffer = if let Some((cipher, crypter)) = file.encrypter.as_mut() {
                        let mut encrypt_buffer = vec![0; write_len + cipher.block_size()];
                        let mut bytes =
                            crypter.update(&next_buf[0..write_len], &mut encrypt_buffer)?;
                        if switch {
                            bytes += crypter.finalize(&mut encrypt_buffer)?;
                        }
                        encrypt_buffer.truncate(bytes);
                        Cow::Owned(encrypt_buffer)
                    } else {
                        Cow::Borrowed(&next_buf[0..write_len])
                    };
                    let encrypt_len = encrypt_buffer.len();

                    let mut start = 0;
                    loop {
                        let acquire = cmp::min(IO_LIMITER_CHUNK_SIZE, encrypt_len - start);
                        self.info.mgr.limiter.blocking_consume(acquire);
                        file.file
                            .write_all(&encrypt_buffer[start..start + acquire])?;
                        if start + acquire == encrypt_len {
                            break;
                        }
                        start += acquire;
                    }
                    if switch {
                        next_buf = &next_buf[write_len..];
                        *cf_file_index += 1;
                        if *cf_file_index >= cf_file.file_for_receiving.len() {
                            *cf_file_index = 0;
                            *cf_index += 1;
                        }
                    }
                    if finished {
                        break;
                    }
                }
                Ok(written_bytes)
            }
            _ => unreachable!(),
        }
    }

    fn flush(&mut self) -> io::Result<()> {
        match *self.inner.lock().unwrap() {
            SnapshotInner::Receiving {
                ref mut data,
                ref mut cf_index,
                ref mut cf_file_index,
                ..
            } => {
                if let Some(cf_file) = data.cf_files.get_mut(*cf_index) {
                    for file in &mut cf_file.file_for_receiving {
                        file.file.flush()?;
                    }
                }
                Ok(())
            }
            _ => unreachable!(),
        }
    }
}

impl Drop for Snapshot {
    fn drop(&mut self) {
        self.delete();
    }
}

/// `SnapStats` is for snapshot statistics.
pub struct SnapStats {
    pub sending_count: usize,
    pub receiving_count: usize,
    pub stats: Vec<SnapshotStat>,
}

#[derive(Clone)]
struct SnapManagerCore {
    // directory to store snapfile.
    base: String,

    registry: Arc<RwLock<HashMap<SnapKey, Arc<Snapshot>>>>,
    limiter: Limiter,
    recv_concurrency_limiter: Arc<SnapRecvConcurrencyLimiter>,
    temp_sst_id: Arc<AtomicU64>,
    encryption_key_manager: Option<Arc<DataKeyManager>>,
    max_per_file_size: Arc<AtomicU64>,
    enable_multi_snapshot_files: Arc<AtomicBool>,
    stats: Arc<Mutex<Vec<SnapshotStat>>>,
}

/// `SnapManagerCore` trace all current processing snapshots.
#[derive(Clone)]
pub struct SnapManager {
    core: SnapManagerCore,
    max_total_size: Arc<AtomicU64>,

    // only used to receive snapshot from v2
    tablet_snap_manager: Option<TabletSnapManager>,
}

impl SnapManager {
    pub fn new<T: Into<String>>(path: T) -> Self {
        SnapManagerBuilder::default().build(path)
    }

    pub fn init(&self) -> io::Result<()> {
        let enc_enabled = self.core.encryption_key_manager.is_some();
        info!(
            "Initializing SnapManager, encryption is enabled: {}",
            enc_enabled
        );

        // Use write lock so only one thread initialize the directory at a time.
        let _lock = self.core.registry.wl();
        let path = Path::new(&self.core.base);
        if !path.exists() {
            file_system::create_dir_all(path)?;
            return Ok(());
        }
        if !path.is_dir() {
            return Err(io::Error::new(
                ErrorKind::Other,
                format!("{} should be a directory", path.display()),
            ));
        }
        for f in file_system::read_dir(path)? {
            let p = f?;
            if p.file_type()?.is_file() {
                if let Some(s) = p.file_name().to_str() {
                    if s.ends_with(TMP_FILE_SUFFIX) {
                        file_system::remove_file(p.path())?;
                    }
                }
            }
        }

        Ok(())
    }

    // [PerformanceCriticalPath]?? I/O involved API should be called in background
    // thread Return all snapshots which is idle not being used.
    pub fn list_idle_snap(&self) -> io::Result<Vec<(SnapKey, bool)>> {
        // Use a lock to protect the directory when scanning.
        let registry = self.core.registry.rl();
        let read_dir = file_system::read_dir(Path::new(&self.core.base))?;
        // Remove the duplicate snap keys.
        let mut v: Vec<_> = read_dir
            .filter_map(|p| {
                let p = match p {
                    Err(e) => {
                        error!(
                            "failed to list content of directory";
                            "directory" => %&self.core.base,
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
                if name.starts_with(DEL_RANGE_PREFIX) {
                    // This is a temp file to store delete keys and ingest them into Engine.
                    return None;
                }

                let is_sending = name.starts_with(SNAP_GEN_PREFIX);
                let numbers: Vec<u64> = name.split('.').next().map_or_else(Vec::new, |s| {
                    s.split('_')
                        .skip(1)
                        .filter_map(|s| s.parse().ok())
                        .collect()
                });
                if numbers.len() < 3 {
                    error!(
                        "failed to parse snapkey";
                        "snap_key" => %name,
                    );
                    return None;
                }
                let snap_key = SnapKey::new(numbers[0], numbers[1], numbers[2]);
                if registry.contains_key(&snap_key) {
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

    pub fn get_temp_path_for_ingest(&self) -> String {
        let sst_id = self.core.temp_sst_id.fetch_add(1, Ordering::SeqCst);
        let filename = format!(
            "{}_{}{}{}",
            DEL_RANGE_PREFIX, sst_id, SST_FILE_SUFFIX, TMP_FILE_SUFFIX
        );
        let path = PathBuf::from(&self.core.base).join(filename);
        path.to_str().unwrap().to_string()
    }

    pub fn has_registered(&self, key: &SnapKey) -> bool {
        self.core.registry.rl().contains_key(key)
    }

    /// Get a `Snapshot` can be used for `build`. Concurrent calls are allowed
    /// because only one caller can lock temporary disk files.
    ///
    /// NOTE: it calculates snapshot size by scanning the base directory.
    /// Don't call it in raftstore thread until the size limitation mechanism
    /// gets refactored.
    pub fn get_snapshot_for_building(&self, key: &SnapKey) -> RaftStoreResult<Arc<Snapshot>> {
        if self.get_total_snap_memory_size() > self.max_total_snap_size() {
            return Err(RaftStoreError::Snapshot(Error::TooManySnapshots));
        }

        let base = &self.core.base;
        let s = Arc::new(Snapshot::new_for_building(base, key, &self.core));
        self.register(key.clone(), s.clone())?;
        Ok(s)
    }

    pub fn get_snapshot_for_sending(&self, key: &SnapKey) -> RaftStoreResult<Arc<Snapshot>> {
        self.core
            .registry
            .rl()
            .get(key)
            .ok_or_else(|| {
                RaftStoreError::Other(From::from(format!("snapshot {:?} not found", key)))
            })
            .map(|s| s.clone())
    }

    /// Get a `Snapshot` can be used for writing and then `save`. Concurrent
    /// calls are allowed because only one caller can lock temporary disk
    /// files.
    pub fn get_snapshot_for_receiving(
        &self,
        key: &SnapKey,
        snapshot_meta: SnapshotMeta,
    ) -> RaftStoreResult<Arc<Snapshot>> {
        let base = &self.core.base;
        let s = Arc::new(Snapshot::new_for_receiving(
            base,
            key,
            &self.core,
            snapshot_meta,
        )?);

        self.register(key.clone(), s.clone())?;
        Ok(s)
    }

    // Tablet snapshot is the snapshot sent from raftstore-v2.
    // We enable v1 to receive it to enable tiflash node to receive and apply
    // snapshot from raftstore-v2.
    // To make it easy, we maintain an empty `store::snapshot` with tablet snapshot
    // path storing in it. So tiflash node can detect it and apply properly.
    pub fn gen_empty_snapshot_for_tablet_snapshot(
        &self,
        tablet_snap_key: &TabletSnapKey,
        for_balance: bool,
    ) -> RaftStoreResult<()> {
        let _lock = self.core.registry.rl();
        let base = &self.core.base;
        let tablet_snap_path = self
            .tablet_snap_manager
            .as_ref()
            .unwrap()
            .final_recv_path(tablet_snap_key);
        let snap_key = SnapKey::new(
            tablet_snap_key.region_id,
            tablet_snap_key.term,
            tablet_snap_key.idx,
        );
        // let _ = Snapshot::new_for_tablet_snapshot(
        //     base,
        //     &snap_key,
        //     &self.core,
        //     tablet_snap_path.to_str().unwrap(),
        //     for_balance,
        // )?;
        Ok(())
    }

    pub fn get_snapshot_for_applying(&self, key: &SnapKey) -> RaftStoreResult<Arc<Snapshot>> {
        let base = &self.core.base;

        match self.core.registry.rl().get(key) {
            Some(s) => return Ok(s.clone()),
            None => {}
        }

        let s = Arc::new(Snapshot::new_for_applying(base, key, &self.core)?);
        if !s.exists() {
            return Err(RaftStoreError::Other(From::from(format!(
                "snapshot of {:?} not exists.",
                key
            ))));
        }
        self.register(key.clone(), s.clone())?;
        Ok(s)
    }

    pub fn get_snapshot_for_gc(&self, key: &SnapKey) -> RaftStoreResult<Arc<Snapshot>> {
        let base = &self.core.base;
        let s = Arc::new(Snapshot::new_for_applying(base, key, &self.core)?);
        Ok(s)
    }

    pub fn get_snapshot_meta(&self, key: &SnapKey) -> io::Result<Metadata> {
        let prefix = format!("{}_{}", SNAP_REV_PREFIX, key);
        let meta_path = Path::new(&self.core.base).join(format!("{}{}", prefix, META_FILE_SUFFIX));
        file_system::metadata(meta_path)
    }

    /// Get the approximate size of snap file exists in snap directory.
    ///
    /// Return value is not guaranteed to be accurate.
    ///
    /// NOTE: don't call it in raftstore thread.
    pub fn get_total_snap_size(&self) -> Result<u64> {
        let size_v1 = self.core.get_total_snap_size()?;
        let size_v2 = self
            .tablet_snap_manager
            .as_ref()
            .map(|s| s.total_snap_size().unwrap_or(0))
            .unwrap_or(0);
        Ok(size_v1 + size_v2)
    }

    pub fn get_total_snap_memory_size(&self) -> u64 {
        // TODO(@Connor1996): implement it
        0
    }

    pub fn max_total_snap_size(&self) -> u64 {
        self.max_total_size.load(Ordering::Acquire)
    }

    pub fn set_max_total_snap_size(&self, max_total_size: u64) {
        self.max_total_size.store(max_total_size, Ordering::Release);
    }

    pub fn set_max_per_file_size(&mut self, max_per_file_size: u64) {
        if max_per_file_size == 0 {
            self.core
                .max_per_file_size
                .store(u64::MAX, Ordering::Release);
        } else {
            self.core
                .max_per_file_size
                .store(max_per_file_size, Ordering::Release);
        }
    }

    pub fn get_actual_max_per_file_size(&self, allow_multi_files_snapshot: bool) -> u64 {
        self.core
            .get_actual_max_per_file_size(allow_multi_files_snapshot)
    }

    pub fn set_enable_multi_snapshot_files(&mut self, enable_multi_snapshot_files: bool) {
        self.core
            .enable_multi_snapshot_files
            .store(enable_multi_snapshot_files, Ordering::Release);
    }

    pub fn set_speed_limit(&self, bytes_per_sec: f64) {
        self.core.limiter.set_speed_limit(bytes_per_sec);
    }

    pub fn get_speed_limit(&self) -> f64 {
        self.core.limiter.speed_limit()
    }

    pub fn set_concurrent_recv_snap_limit(&self, limit: usize) {
        self.core.recv_concurrency_limiter.set_limit(limit);
    }

    pub fn collect_stat(&self, snap: SnapshotStat) {
        debug!(
            "collect snapshot stat";
            "region_id" => snap.region_id,
            "total_size" => snap.get_transport_size(),
            "total_duration_sec" => snap.get_total_duration_sec(),
            "generate_duration_sec" => snap.get_generate_duration_sec(),
            "send_duration_sec" => snap.get_generate_duration_sec(),
        );
        self.core.stats.lock().unwrap().push(snap);
    }

    pub fn register(&self, key: SnapKey, snap: Arc<Snapshot>) -> Result<()> {
        debug!(
            "register snapshot";
            "key" => %key,
        );
        match self.core.registry.wl().entry(key) {
            Entry::Occupied(mut e) => {
                return Err(Error::Other(box_err!(
                    "snap key {} is registered more than once",
                    e.key()
                )));
            }
            Entry::Vacant(e) => {
                e.insert(snap);
            }
        }
        Ok(())
    }

    pub fn deregister(&self, key: &SnapKey) {
        debug!(
            "deregister snapshot";
            "key" => %key,
        );
        let registry = &mut self.core.registry.wl();
        let snap = registry.remove(key);
    }

    pub fn stats(&self) -> SnapStats {
        // send_count, generating_count, receiving_count, applying_count
        let (mut sending_count, mut receiving_count) = (0, 0);
        for snap in self.core.registry.rl().values() {
            match *snap.inner.lock().unwrap() {
                SnapshotInner::Sending { .. } | SnapshotInner::Building { .. } => {
                    sending_count += 1
                }
                SnapshotInner::Receiving { .. } | SnapshotInner::Applying { .. } => {
                    receiving_count += 1
                }
            }
        }

        let stats = std::mem::take(self.core.stats.lock().unwrap().as_mut());
        SnapStats {
            sending_count,
            receiving_count,
            stats,
        }
    }

    pub fn delete_snapshot(&self, key: &SnapKey, snap: &Snapshot) -> bool {
        self.core.delete_snapshot(key, snap)
    }

    pub fn tablet_snap_manager(&self) -> Option<&TabletSnapManager> {
        self.tablet_snap_manager.as_ref()
    }

    pub fn limiter(&self) -> &Limiter {
        &self.core.limiter
    }

    /// recv_snap_precheck is part of the snapshot recv precheck process, which
    /// aims to reduce unnecessary snapshot drops and regenerations. When a
    /// leader wants to generate a snapshot for a follower, it first sends a
    /// precheck message. Upon receiving the message, the follower uses this
    /// function to consult the concurrency limiter, determining if it can
    /// receive a new snapshot. If the precheck is successful, the leader will
    /// proceed to generate and send the snapshot.
    pub fn recv_snap_precheck(&self, region_id: u64) -> bool {
        self.core.recv_concurrency_limiter.try_recv(region_id)
    }

    /// recv_snap_complete is part of the snapshot recv precheck process, and
    /// should be called when a follower finishes receiving a snapshot.
    pub fn recv_snap_complete(&self, region_id: u64) {
        self.core.recv_concurrency_limiter.finish_recv(region_id)
    }

    /// Adjusts the capacity of the snapshot receive concurrency limiter to
    /// account for the number of pending applies. This prevents more snapshots
    /// to be generated if there are too many snapshots waiting to be applied.
    pub fn set_pending_apply_count(&self, num_pending_applies: usize) {
        self.core
            .recv_concurrency_limiter
            .set_reserved_capacity(num_pending_applies)
    }
}

impl SnapManagerCore {
    fn get_total_snap_size(&self) -> Result<u64> {
        let mut total_size = 0;
        for entry in file_system::read_dir(&self.base)? {
            let (entry, metadata) = match entry.and_then(|e| e.metadata().map(|m| (e, m))) {
                Ok((e, m)) => (e, m),
                Err(e) if e.kind() == ErrorKind::NotFound => continue,
                Err(e) => return Err(Error::from(e)),
            };

            let path = entry.path();
            let path_s = path.to_str().unwrap();
            if !metadata.is_file()
                // Ignore cloned files as they are hard links on posix systems.
                || path_s.ends_with(CLONE_FILE_SUFFIX)
                || path_s.ends_with(META_FILE_SUFFIX)
            {
                continue;
            }
            total_size += metadata.len();
        }
        Ok(total_size)
    }

    // Return true if it successfully delete the specified snapshot.
    fn delete_snapshot(&self, key: &SnapKey, snap: &Snapshot) -> bool {
        let registry = self.registry.rl();
        if registry.contains_key(key) {
            info!(
                "skip to delete snapshot since it's registered";
                "snapshot" => %snap.path(),
            );
            return false;
        }
        snap.delete();
        true
    }

    pub fn get_actual_max_per_file_size(&self, allow_multi_files_snapshot: bool) -> u64 {
        if !allow_multi_files_snapshot {
            return u64::MAX;
        }

        if self.enable_multi_snapshot_files.load(Ordering::Relaxed) {
            return self.max_per_file_size.load(Ordering::Relaxed);
        }
        u64::MAX
    }
}

/// `SnapRecvConcurrencyLimiter` enforces a limit on the number of simultaneous
/// snapshot receives. It is consulted before a snapshot is generated. It
/// employs a TTL mechanism to automatically evict operations that have been
/// pending longer than the specified TTL. The TTL helps to handle scenarios
/// where a snapshot fails to be sent for any reason. Note that a limit of 0
/// means there's no limit.
#[derive(Clone)]
pub struct SnapRecvConcurrencyLimiter {
    limit: Arc<AtomicUsize>,
    reserved_capacity: Arc<AtomicUsize>,
    ttl_secs: u64,
    timestamps: Arc<Mutex<HashMap<u64, Instant>>>,
}

impl SnapRecvConcurrencyLimiter {
    // Note that a limit of 0 means there's no limit.
    pub fn new(limit: usize, ttl_secs: u64) -> Self {
        SnapRecvConcurrencyLimiter {
            limit: Arc::new(AtomicUsize::new(limit)),
            reserved_capacity: Arc::new(AtomicUsize::new(0)),
            ttl_secs,
            timestamps: Arc::new(Mutex::new(HashMap::with_capacity_and_hasher(
                limit,
                Default::default(),
            ))),
        }
    }

    // Attempts to add a snapshot receive operation if below the concurrency
    // limit. Returns true if the operation is allowed, false otherwise.
    pub fn try_recv(&self, region_id: u64) -> bool {
        let mut timestamps = self.timestamps.lock().unwrap();
        let current_time = Instant::now();
        self.evict_expired_timestamps(&mut timestamps, current_time);

        let limit = self.limit.load(Ordering::Relaxed);
        if limit == 0 {
            // 0 means no limit. In that case, we avoid inserting into the hash
            // map to prevent it from growing indefinitely.
            return true;
        }

        let reserved_capacity = self.reserved_capacity.load(Ordering::Relaxed);
        // Insert into the map if its size is within limit. If the region id is
        // already present in the map, update its timestamp.
        if timestamps.len() + reserved_capacity < limit || timestamps.contains_key(&region_id) {
            timestamps.insert(region_id, current_time);
            return true;
        }
        false
    }

    fn evict_expired_timestamps(
        &self,
        timestamps: &mut HashMap<u64, Instant>,
        current_time: Instant,
    ) {
        timestamps.retain(|region_id, timestamp| {
            if current_time.duration_since(*timestamp) <= Duration::from_secs(self.ttl_secs) {
                true
            } else {
                // This shouldn't happen if the TTL is set properly. When it
                // does happen, the limiter may permit more snapshots than the
                // configured limit to be sent and trigger the receiver busy
                // error.
                warn!(
                    "region {} expired in the snap recv concurrency limiter",
                    region_id
                );
                false
            }
        });
        timestamps.shrink_to(self.limit.load(Ordering::Relaxed));
    }

    // Completes a snapshot receive operation by removing a timestamp from the
    // queue.
    pub fn finish_recv(&self, region_id: u64) {
        self.timestamps.lock().unwrap().remove(&region_id);
    }

    pub fn set_limit(&self, limit: usize) {
        self.limit.store(limit, Ordering::Relaxed);
    }

    // Set the reserved capacity of the limiter. The reserved capacity is
    // unavailable for use. The actual available capacity is calculated as
    // the total limit minus the reserved capacity.
    pub fn set_reserved_capacity(&self, reserved_cap: usize) {
        self.reserved_capacity
            .store(reserved_cap, Ordering::Relaxed);
    }
}

#[derive(Clone, Default)]
pub struct SnapManagerBuilder {
    max_write_bytes_per_sec: i64,
    max_total_size: u64,
    max_per_file_size: u64,
    enable_multi_snapshot_files: bool,
    enable_receive_tablet_snapshot: bool,
    key_manager: Option<Arc<DataKeyManager>>,
    concurrent_recv_snap_limit: usize,
}

impl SnapManagerBuilder {
    #[must_use]
    pub fn max_write_bytes_per_sec(mut self, bytes: i64) -> SnapManagerBuilder {
        self.max_write_bytes_per_sec = bytes;
        self
    }
    #[must_use]
    pub fn max_total_size(mut self, bytes: u64) -> SnapManagerBuilder {
        self.max_total_size = bytes;
        self
    }

    #[must_use]
    pub fn concurrent_recv_snap_limit(mut self, limit: usize) -> SnapManagerBuilder {
        self.concurrent_recv_snap_limit = limit;
        self
    }

    pub fn max_per_file_size(mut self, bytes: u64) -> SnapManagerBuilder {
        self.max_per_file_size = bytes;
        self
    }
    pub fn enable_multi_snapshot_files(mut self, enabled: bool) -> SnapManagerBuilder {
        self.enable_multi_snapshot_files = enabled;
        self
    }
    pub fn enable_receive_tablet_snapshot(mut self, enabled: bool) -> SnapManagerBuilder {
        self.enable_receive_tablet_snapshot = enabled;
        self
    }
    #[must_use]
    pub fn encryption_key_manager(mut self, m: Option<Arc<DataKeyManager>>) -> SnapManagerBuilder {
        self.key_manager = m;
        self
    }
    pub fn build<T: Into<String>>(self, path: T) -> SnapManager {
        let limiter = Limiter::new(if self.max_write_bytes_per_sec > 0 {
            self.max_write_bytes_per_sec as f64
        } else {
            f64::INFINITY
        });
        let max_total_size = if self.max_total_size > 0 {
            self.max_total_size
        } else {
            u64::MAX
        };
        let path = path.into();
        assert!(!path.is_empty());
        let mut path_v2 = path.clone();
        path_v2.push_str("_v2");
        let tablet_snap_manager = if self.enable_receive_tablet_snapshot {
            Some(TabletSnapManager::new(&path_v2, self.key_manager.clone()).unwrap())
        } else {
            None
        };

        let mut snapshot = SnapManager {
            core: SnapManagerCore {
                base: path,
                registry: Default::default(),
                limiter,
                recv_concurrency_limiter: Arc::new(SnapRecvConcurrencyLimiter::new(
                    self.concurrent_recv_snap_limit,
                    RECV_SNAP_CONCURRENCY_LIMITER_TTL_SECS,
                )),
                temp_sst_id: Arc::new(AtomicU64::new(0)),
                encryption_key_manager: self.key_manager,
                max_per_file_size: Arc::new(AtomicU64::new(u64::MAX)),
                enable_multi_snapshot_files: Arc::new(AtomicBool::new(
                    self.enable_multi_snapshot_files,
                )),
                stats: Default::default(),
            },
            max_total_size: Arc::new(AtomicU64::new(max_total_size)),
            tablet_snap_manager,
        };
        snapshot.set_max_per_file_size(self.max_per_file_size); // set actual max_per_file_size
        snapshot
    }
}

#[derive(Clone, Hash, PartialEq, Eq, PartialOrd, Ord, Debug)]
pub struct TabletSnapKey {
    pub region_id: u64,
    pub to_peer: u64,
    pub term: u64,
    pub idx: u64,
}

impl TabletSnapKey {
    #[inline]
    pub fn new(region_id: u64, to_peer: u64, term: u64, idx: u64) -> TabletSnapKey {
        TabletSnapKey {
            region_id,
            to_peer,
            term,
            idx,
        }
    }

    pub fn from_region_snap(region_id: u64, to_peer: u64, snap: &RaftSnapshot) -> TabletSnapKey {
        let index = snap.get_metadata().get_index();
        let term = snap.get_metadata().get_term();
        TabletSnapKey::new(region_id, to_peer, term, index)
    }

    pub fn from_path<T: Into<PathBuf>>(path: T) -> Result<TabletSnapKey> {
        let path = path.into();
        let name = path.file_name().unwrap().to_str().unwrap();
        let numbers: Vec<u64> = name
            .split('_')
            .skip(1)
            .filter_map(|s| s.parse().ok())
            .collect();
        if numbers.len() < 4 {
            return Err(box_err!("invalid tablet snapshot file name:{}", name));
        }
        Ok(TabletSnapKey::new(
            numbers[0], numbers[1], numbers[2], numbers[3],
        ))
    }
}

impl Display for TabletSnapKey {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{}_{}_{}_{}",
            self.region_id, self.to_peer, self.term, self.idx
        )
    }
}

pub struct ReceivingGuard<'a> {
    receiving: &'a Mutex<Vec<TabletSnapKey>>,
    key: TabletSnapKey,
}

impl Drop for ReceivingGuard<'_> {
    fn drop(&mut self) {
        let mut receiving = self.receiving.lock().unwrap();
        let pos = receiving.iter().position(|k| k == &self.key).unwrap();
        receiving.swap_remove(pos);
    }
}

/// `TabletSnapManager` manager tablet snapshot and shared between raftstore v2.
/// It's similar `SnapManager`, but simpler in tablet version.
///
///  TODO:
///     - clean up expired tablet checkpointer
#[derive(Clone)]
pub struct TabletSnapManager {
    // directory to store snapfile.
    base: PathBuf,
    key_manager: Option<Arc<DataKeyManager>>,
    receiving: Arc<Mutex<Vec<TabletSnapKey>>>,
    stats: Arc<Mutex<HashMap<TabletSnapKey, (Instant, SnapshotStat)>>>,
    sending_count: Arc<AtomicUsize>,
    recving_count: Arc<AtomicUsize>,
}

impl TabletSnapManager {
    pub fn new<T: Into<PathBuf>>(
        path: T,
        key_manager: Option<Arc<DataKeyManager>>,
    ) -> io::Result<Self> {
        let path = path.into();
        if !path.exists() {
            file_system::create_dir_all(&path)?;
        }
        if !path.is_dir() {
            return Err(io::Error::new(
                ErrorKind::Other,
                format!("{} should be a directory", path.display()),
            ));
        }
        encryption::clean_up_dir(&path, SNAP_GEN_PREFIX, key_manager.as_deref())?;
        encryption::clean_up_trash(&path, key_manager.as_deref())?;
        Ok(Self {
            base: path,
            key_manager,
            receiving: Arc::default(),
            stats: Arc::default(),
            sending_count: Arc::default(),
            recving_count: Arc::default(),
        })
    }

    pub fn begin_snapshot(&self, key: TabletSnapKey, start: Instant, generate_duration_sec: u64) {
        let mut stat = SnapshotStat::default();
        stat.set_generate_duration_sec(generate_duration_sec);
        self.stats.lock().unwrap().insert(key, (start, stat));
    }

    pub fn finish_snapshot(&self, key: TabletSnapKey, send: Instant) {
        let region_id = key.region_id;
        self.stats
            .lock()
            .unwrap()
            .entry(key)
            .and_modify(|(start, stat)| {
                stat.set_send_duration_sec(send.saturating_elapsed().as_secs());
                stat.set_total_duration_sec(start.saturating_elapsed().as_secs());
                stat.set_region_id(region_id);
            });
    }

    pub fn stats(&self) -> SnapStats {
        let stats: Vec<SnapshotStat> = self
            .stats
            .lock()
            .unwrap()
            .extract_if(|_, (_, stat)| stat.get_region_id() > 0)
            .map(|(_, (_, stat))| stat)
            .filter(|stat| stat.get_total_duration_sec() > 1)
            .collect();
        SnapStats {
            sending_count: self.sending_count.load(Ordering::SeqCst),
            receiving_count: self.recving_count.load(Ordering::SeqCst),
            stats,
        }
    }

    pub fn tablet_gen_path(&self, key: &TabletSnapKey) -> PathBuf {
        let prefix = format!("{}_{}", SNAP_GEN_PREFIX, key);
        PathBuf::from(&self.base).join(prefix)
    }

    pub fn final_recv_path(&self, key: &TabletSnapKey) -> PathBuf {
        let prefix = format!("{}_{}", SNAP_REV_PREFIX, key);
        PathBuf::from(&self.base).join(prefix)
    }

    pub fn tmp_recv_path(&self, key: &TabletSnapKey) -> PathBuf {
        let prefix = format!("{}_{}{}", SNAP_REV_PREFIX, key, TMP_FILE_SUFFIX);
        PathBuf::from(&self.base).join(prefix)
    }

    pub fn delete_snapshot(&self, key: &TabletSnapKey) -> bool {
        let path = self.tablet_gen_path(key);
        debug!("delete tablet snapshot file";"path" => %path.display());
        if path.exists() {
            if let Err(e) = encryption::trash_dir_all(&path, self.key_manager.as_deref()) {
                error!(
                    "delete snapshot failed";
                    "path" => %path.display(),
                    "err" => ?e,
                );
                return false;
            }
        }
        true
    }

    pub fn list_snapshot(&self) -> Result<Vec<PathBuf>> {
        let mut paths = Vec::new();
        for entry in file_system::read_dir(&self.base)? {
            let entry = match entry {
                Ok(e) => e,
                Err(e) if e.kind() == ErrorKind::NotFound => continue,
                Err(e) => return Err(Error::from(e)),
            };

            let path = entry.path();
            if path.file_name().and_then(|n| n.to_str()).map_or(true, |n| {
                !n.starts_with(SNAP_GEN_PREFIX) || n.ends_with(TMP_FILE_SUFFIX)
            }) {
                continue;
            }
            paths.push(path);
        }
        Ok(paths)
    }

    pub fn total_snap_size(&self) -> Result<u64> {
        let mut total_size = 0;
        for entry in file_system::read_dir(&self.base)? {
            let entry = match entry {
                Ok(e) => e,
                Err(e) if e.kind() == ErrorKind::NotFound => continue,
                Err(e) => return Err(Error::from(e)),
            };

            let path = entry.path();
            // Generated snapshots are just checkpoints, only counts received snapshots.
            if !path
                .file_name()
                .and_then(|n| n.to_str())
                .map_or(true, |n| n.starts_with(SNAP_REV_PREFIX))
            {
                continue;
            }
            let entries = match file_system::read_dir(path) {
                Ok(entries) => entries,
                Err(e) if e.kind() == ErrorKind::NotFound => continue,
                Err(e) => return Err(Error::from(e)),
            };
            for e in entries {
                match e.and_then(|e| e.metadata()) {
                    Ok(m) => total_size += m.len(),
                    Err(e) if e.kind() == ErrorKind::NotFound => continue,
                    Err(e) => return Err(Error::from(e)),
                }
            }
        }
        Ok(total_size)
    }

    #[inline]
    pub fn root_path(&self) -> &Path {
        self.base.as_path()
    }

    pub fn start_receive(&self, key: TabletSnapKey) -> Option<ReceivingGuard<'_>> {
        let mut receiving = self.receiving.lock().unwrap();
        if receiving.iter().any(|k| k == &key) {
            return None;
        }
        receiving.push(key.clone());
        Some(ReceivingGuard {
            receiving: &self.receiving,
            key,
        })
    }

    pub fn sending_count(&self) -> &Arc<AtomicUsize> {
        &self.sending_count
    }

    pub fn recving_count(&self) -> &Arc<AtomicUsize> {
        &self.recving_count
    }

    #[inline]
    pub fn key_manager(&self) -> &Option<Arc<DataKeyManager>> {
        &self.key_manager
    }
}

fn is_symlink<P: AsRef<Path>>(path: P) -> Result<bool> {
    let metadata = box_try!(std::fs::symlink_metadata(path));
    Ok(metadata.is_symlink())
}

#[cfg(test)]
pub mod tests {
    use std::{
        cmp, fs,
        io::{self, Read, Seek, SeekFrom, Write},
        path::{Path, PathBuf},
        sync::{
            atomic::{AtomicBool, AtomicU64, AtomicUsize},
            Arc,
        },
    };

    use encryption::{DataKeyManager, EncryptionConfig, FileConfig, MasterKeyConfig};
    use encryption_export::data_key_manager_from_config;
    use engine_test::{
        ctor::{CfOptions, DbOptions, KvEngineConstructorExt, RaftDbOptions},
        kv::KvTestEngine,
        raft::RaftTestEngine,
    };
    use engine_traits::{
        Engines, ExternalSstFileInfo, KvEngine, RaftEngine, RaftLogBatch,
        Snapshot as EngineSnapshot, SstExt, SstWriter, SstWriterBuilder, SyncMutable, ALL_CFS,
        CF_DEFAULT, CF_LOCK, CF_RAFT, CF_WRITE,
    };
    use kvproto::{
        encryptionpb::EncryptionMethod,
        metapb::{Peer, Region},
        raft_serverpb::{RaftApplyState, RegionLocalState, SnapshotMeta},
    };
    use protobuf::Message;
    use raft::eraftpb::Entry;
    use tempfile::{Builder, TempDir};
    use tikv_util::time::Limiter;

    use super::*;
    use crate::{
        coprocessor::CoprocessorHost,
        store::{peer_storage::JOB_STATUS_RUNNING, INIT_EPOCH_CONF_VER, INIT_EPOCH_VER},
        Result,
    };

    const TEST_STORE_ID: u64 = 1;
    const TEST_KEY: &[u8] = b"akey";
    const TEST_WRITE_BATCH_SIZE: usize = 10 * 1024 * 1024;
    const TEST_META_FILE_BUFFER_SIZE: usize = 1000;
    const BYTE_SIZE: usize = 1;

    type DbBuilder<E> = fn(
        p: &Path,
        db_opt: Option<DbOptions>,
        cf_opts: Option<Vec<(&'static str, CfOptions)>>,
    ) -> Result<E>;

    pub fn open_test_empty_db<E>(
        path: &Path,
        db_opt: Option<DbOptions>,
        cf_opts: Option<Vec<(&'static str, CfOptions)>>,
    ) -> Result<E>
    where
        E: KvEngine + KvEngineConstructorExt,
    {
        let p = path.to_str().unwrap();
        let db_opt = db_opt.unwrap_or_default();
        let cf_opts = cf_opts.unwrap_or_else(|| {
            ALL_CFS
                .iter()
                .map(|cf| (*cf, CfOptions::default()))
                .collect()
        });
        let db = E::new_kv_engine_opt(p, db_opt, cf_opts).unwrap();
        Ok(db)
    }

    pub fn open_test_db<E>(
        path: &Path,
        db_opt: Option<DbOptions>,
        cf_opts: Option<Vec<(&'static str, CfOptions)>>,
    ) -> Result<E>
    where
        E: KvEngine + KvEngineConstructorExt,
    {
        let db = open_test_empty_db::<E>(path, db_opt, cf_opts).unwrap();
        let key = keys::data_key(TEST_KEY);
        // write some data into each cf
        for (i, cf) in db.cf_names().into_iter().enumerate() {
            let mut p = Peer::default();
            p.set_store_id(TEST_STORE_ID);
            p.set_id((i + 1) as u64);
            db.put_msg_cf(cf, &key[..], &p)?;
        }
        Ok(db)
    }

    pub fn open_test_db_with_100keys<E>(
        path: &Path,
        db_opt: Option<DbOptions>,
        cf_opts: Option<Vec<(&'static str, CfOptions)>>,
    ) -> Result<E>
    where
        E: KvEngine + KvEngineConstructorExt,
    {
        let db = open_test_empty_db::<E>(path, db_opt, cf_opts).unwrap();
        // write some data into each cf
        for (i, cf) in db.cf_names().into_iter().enumerate() {
            let mut p = Peer::default();
            p.set_store_id(TEST_STORE_ID);
            p.set_id((i + 1) as u64);
            for k in 0..100 {
                let key = keys::data_key(format!("akey{}", k).as_bytes());
                db.put_msg_cf(cf, &key[..], &p)?;
            }
        }
        Ok(db)
    }

    pub fn get_test_db_for_regions(
        path: &TempDir,
        raft_db_opt: Option<RaftDbOptions>,
        kv_db_opt: Option<DbOptions>,
        kv_cf_opts: Option<Vec<(&'static str, CfOptions)>>,
        regions: &[u64],
    ) -> Result<Engines<KvTestEngine, RaftTestEngine>> {
        let p = path.path();
        let kv: KvTestEngine = open_test_db(p.join("kv").as_path(), kv_db_opt, kv_cf_opts)?;
        let raft: RaftTestEngine =
            engine_test::raft::new_engine(p.join("raft").to_str().unwrap(), raft_db_opt)?;
        let mut lb = raft.log_batch(regions.len() * 128);
        for &region_id in regions {
            // Put apply state into kv engine.
            let mut apply_state = RaftApplyState::default();
            let mut apply_entry = Entry::default();
            apply_state.set_applied_index(10);
            apply_entry.set_index(10);
            apply_entry.set_term(0);
            apply_state.mut_truncated_state().set_index(10);
            kv.put_msg_cf(CF_RAFT, &keys::apply_state_key(region_id), &apply_state)?;
            lb.append(region_id, None, vec![apply_entry])?;

            // Put region info into kv engine.
            let region = gen_test_region(region_id, 1, 1);
            let mut region_state = RegionLocalState::default();
            region_state.set_region(region);
            kv.put_msg_cf(CF_RAFT, &keys::region_state_key(region_id), &region_state)?;
        }
        raft.consume(&mut lb, false).unwrap();
        Ok(Engines::new(kv, raft))
    }

    pub fn get_kv_count(snap: &impl EngineSnapshot) -> u64 {
        let mut kv_count = 0;
        for cf in SNAPSHOT_CFS {
            snap.scan(
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
        let mut peer = Peer::default();
        peer.set_store_id(store_id);
        peer.set_id(peer_id);
        let mut region = Region::default();
        region.set_id(region_id);
        region.set_start_key(b"a".to_vec());
        region.set_end_key(b"z".to_vec());
        region.mut_region_epoch().set_version(INIT_EPOCH_VER);
        region.mut_region_epoch().set_conf_ver(INIT_EPOCH_CONF_VER);
        region.mut_peers().push(peer);
        region
    }

    pub fn assert_eq_db(expected_db: &impl KvEngine, db: &impl KvEngine) {
        let key = keys::data_key(TEST_KEY);
        for cf in SNAPSHOT_CFS {
            let p1: Option<Peer> = expected_db.get_msg_cf(cf, &key[..]).unwrap();
            if let Some(p1) = p1 {
                let p2: Option<Peer> = db.get_msg_cf(cf, &key[..]).unwrap();
                if let Some(p2) = p2 {
                    if p2 != p1 {
                        panic!(
                            "cf {}: key {:?}, value {:?}, expected {:?}",
                            cf, key, p2, p1
                        );
                    }
                } else {
                    panic!("cf {}: expect key {:?} has value", cf, key);
                }
            }
        }
    }

    fn create_manager_core(path: &str, max_per_file_size: u64) -> SnapManagerCore {
        SnapManagerCore {
            base: path.to_owned(),
            registry: Default::default(),
            recv_concurrency_limiter: Arc::new(SnapRecvConcurrencyLimiter::new(
                0,
                RECV_SNAP_CONCURRENCY_LIMITER_TTL_SECS,
            )),
            limiter: Limiter::new(f64::INFINITY),
            temp_sst_id: Arc::new(AtomicU64::new(0)),
            encryption_key_manager: None,
            max_per_file_size: Arc::new(AtomicU64::new(max_per_file_size)),
            enable_multi_snapshot_files: Arc::new(AtomicBool::new(true)),
            stats: Default::default(),
        }
    }

    fn create_encryption_key_manager(prefix: &str) -> (TempDir, Arc<DataKeyManager>) {
        let dir = Builder::new().prefix(prefix).tempdir().unwrap();
        let master_path = dir.path().join("master_key");

        let mut f = OpenOptions::new()
            .create_new(true)
            .append(true)
            .open(&master_path)
            .unwrap();
        // A 32 bytes key (in hex) followed by one '\n'.
        f.write_all(&[b'A'; 64]).unwrap();
        f.write_all(&[b'\n'; 1]).unwrap();

        let dict_path = dir.path().join("dict");
        file_system::create_dir_all(&dict_path).unwrap();

        let key_path = master_path.to_str().unwrap().to_owned();
        let dict_path = dict_path.to_str().unwrap().to_owned();

        let enc_cfg = EncryptionConfig {
            data_encryption_method: EncryptionMethod::Aes128Ctr,
            master_key: MasterKeyConfig::File {
                config: FileConfig { path: key_path },
            },
            ..Default::default()
        };
        let key_manager = data_key_manager_from_config(&enc_cfg, &dict_path)
            .unwrap()
            .map(|x| Arc::new(x));
        (dir, key_manager.unwrap())
    }

    pub fn gen_db_options_with_encryption(prefix: &str) -> (TempDir, DbOptions) {
        let (_enc_dir, key_manager) = create_encryption_key_manager(prefix);
        let mut db_opts = DbOptions::default();
        db_opts.set_key_manager(Some(key_manager));
        (_enc_dir, db_opts)
    }

    /*
    #[test]
    fn test_gen_snapshot_meta() {
        let mut cf_file = Vec::with_capacity(super::SNAPSHOT_CFS.len());
        for (i, cf) in super::SNAPSHOT_CFS.iter().enumerate() {
            let f = super::CfFile {
                cf,
                size: vec![100 * (i + 1) as u64, 100 * (i + 2) as u64],
                checksum: vec![1000 * (i + 1) as u32, 1000 * (i + 2) as u32],
                ..Default::default()
            };
            cf_file.push(f);
        }
        let meta = super::gen_snapshot_meta(&cf_file, false).unwrap();
        let cf_files = meta.get_cf_files();
        assert_eq!(cf_files.len(), super::SNAPSHOT_CFS.len() * 2); // each CF has two snapshot files;
        for (i, cf_file_meta) in meta.get_cf_files().iter().enumerate() {
            let cf_file_idx = i / 2;
            let size_idx = i % 2;
            if cf_file_meta.get_cf() != cf_file[cf_file_idx].cf {
                panic!(
                    "{}: expect cf {}, got {}",
                    i,
                    cf_file[cf_file_idx].cf,
                    cf_file_meta.get_cf()
                );
            }
            if cf_file_meta.get_size() != cf_file[cf_file_idx].size[size_idx] {
                panic!(
                    "{}: expect cf size {}, got {}",
                    i,
                    cf_file[cf_file_idx].size[size_idx],
                    cf_file_meta.get_size()
                );
            }
            if cf_file_meta.get_checksum() != cf_file[cf_file_idx].checksum[size_idx] {
                panic!(
                    "{}: expect cf checksum {}, got {}",
                    i,
                    cf_file[cf_file_idx].checksum[size_idx],
                    cf_file_meta.get_checksum()
                );
            }
        }
    }
    */

    #[test]
    fn test_display_path() {
        let dir = Builder::new()
            .prefix("test-display-path")
            .tempdir()
            .unwrap();
        let key = SnapKey::new(1, 1, 1);
        let prefix = format!("{}_{}", SNAP_GEN_PREFIX, key);
        let display_path = Snapshot::get_display_path(dir.path(), &prefix);
        assert_ne!(display_path, "");
    }

    #[test]
    fn test_empty_snap_file() {
        test_snap_file(open_test_empty_db, u64::MAX);
        test_snap_file(open_test_empty_db, 100);
    }

    #[test]
    fn test_non_empty_snap_file() {
        test_snap_file(open_test_db, u64::MAX);
        test_snap_file(open_test_db_with_100keys, 100);
        test_snap_file(open_test_db_with_100keys, 500);
    }

    fn test_snap_file(get_db: DbBuilder<KvTestEngine>, max_file_size: u64) {
        let region_id = 1;
        let region = gen_test_region(region_id, 1, 1);
        let src_db_dir = Builder::new()
            .prefix("test-snap-file-db-src")
            .tempdir()
            .unwrap();
        let db = get_db(src_db_dir.path(), None, None).unwrap();
        let snapshot = db.snapshot(None);

        let src_dir = Builder::new()
            .prefix("test-snap-file-db-src")
            .tempdir()
            .unwrap();

        let key = SnapKey::new(region_id, 1, 1);

        let mgr_core = create_manager_core(src_dir.path().to_str().unwrap(), max_file_size);
        let mut s1 = Snapshot::new_for_building(src_dir.path(), &key, &mgr_core);

        // Ensure that this snapshot file doesn't exist before being built.
        assert!(!s1.exists());
        assert_eq!(mgr_core.get_total_snap_size().unwrap(), 0);

        let mut snap_data = s1
            .build(&db, &snapshot, &region, true, false, UnixSecs::now())
            .unwrap();
        assert!(s1.exists());
        let size = s1.total_size();
        assert_eq!(s1.total_count(), get_kv_count(&snapshot));

        let mut s3 =
            Snapshot::new_for_receiving(src_dir.path(), &key, &mgr_core, snap_data.take_meta())
                .unwrap();
        assert!(!s3.exists());

        // Ensure snapshot data could be read out of `s1`, and write into `s3`.
        let copy_size = io::copy(&mut &s1, &mut &s3).unwrap();
        assert_eq!(copy_size, size);
        assert!(!s3.exists());
        s3.save().unwrap();
        assert!(s3.exists());

        // Ensure the tracked size is handled correctly after receiving a snapshot.
        assert_eq!(mgr_core.get_total_snap_size().unwrap(), size);

        s1.delete();
        drop(s1);
        // assert_eq!(mgr_core.get_total_memory_snap_size().unwrap(), 0);

        let dst_db_dir = Builder::new()
            .prefix("test-snap-file-dst")
            .tempdir()
            .unwrap();
        let dst_db_path = dst_db_dir.path().to_str().unwrap();
        // Change arbitrarily the cf order of ALL_CFS at destination db.
        let dst_cfs = [CF_WRITE, CF_DEFAULT, CF_LOCK, CF_RAFT];
        let dst_db = engine_test::kv::new_engine(dst_db_path, &dst_cfs).unwrap();
        let options = ApplyOptions {
            db: dst_db.clone(),
            region,
            abort: Arc::new(AtomicUsize::new(JOB_STATUS_RUNNING)),
            write_batch_size: TEST_WRITE_BATCH_SIZE,
            coprocessor_host: CoprocessorHost::<KvTestEngine>::default(),
            ingest_copy_symlink: false,
        };
        // Verify the snapshot applying is ok.
        s3.apply(options).unwrap();

        // Ensure `delete()` works to delete the dest snapshot.
        s3.delete();
        assert!(!s3.exists());
        assert_eq!(mgr_core.get_total_snap_size().unwrap(), 0);

        // Verify the data is correct after applying snapshot.
        assert_eq_db(&db, &dst_db);
    }
    /*
    #[test]
    fn test_empty_snap_validation() {
        test_snap_validation(open_test_empty_db, u64::MAX);
        test_snap_validation(open_test_empty_db, 100);
    }

    #[test]
    fn test_non_empty_snap_validation() {
        test_snap_validation(open_test_db, u64::MAX);
        test_snap_validation(open_test_db_with_100keys, 500);
    }

    fn test_snap_validation(get_db: DbBuilder<KvTestEngine>, max_file_size: u64) {
        let region_id = 1;
        let region = gen_test_region(region_id, 1, 1);
        let db_dir = Builder::new()
            .prefix("test-snap-validation-db")
            .tempdir()
            .unwrap();
        let db = get_db(db_dir.path(), None, None).unwrap();
        let snapshot = db.snapshot(None);

        let dir = Builder::new()
            .prefix("test-snap-validation")
            .tempdir()
            .unwrap();
        let key = SnapKey::new(region_id, 1, 1);
        let mgr_core = create_manager_core(dir.path().to_str().unwrap(), max_file_size);
        let mut s1 = Snapshot::new_for_building(dir.path(), &key, &mgr_core);
        assert!(!s1.exists());

        let _ = s1
            .build(&db, &snapshot, &region, true, false, UnixSecs::now())
            .unwrap();
        assert!(s1.exists());

        let mut s2 = Snapshot::new_for_building(dir.path(), &key, &mgr_core);
        assert!(s2.exists());

        let _ = s2
            .build(&db, &snapshot, &region, true, false, UnixSecs::now())
            .unwrap();
        assert!(s2.exists());
    }
    */

    // Make all the snapshot in the specified dir corrupted to have incorrect
    // checksum.
    fn corrupt_snapshot_checksum_in<T: Into<PathBuf>>(dir: T) -> Vec<SnapshotMeta> {
        let dir_path = dir.into();
        let mut res = Vec::new();
        let read_dir = file_system::read_dir(dir_path).unwrap();
        for p in read_dir {
            if p.is_ok() {
                let e = p.as_ref().unwrap();
                if e.file_name()
                    .into_string()
                    .unwrap()
                    .ends_with(META_FILE_SUFFIX)
                {
                    let mut snapshot_meta = SnapshotMeta::default();
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

                    let buf = snapshot_meta.write_to_bytes().unwrap();
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

    // Make all the snapshot meta files in the specified corrupted to have incorrect
    // content.
    fn corrupt_snapshot_meta_file<T: Into<PathBuf>>(dir: T) -> usize {
        let mut total = 0;
        let dir_path = dir.into();
        let read_dir = file_system::read_dir(dir_path).unwrap();
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
    /*
    #[test]
    fn test_snap_corruption_on_checksum() {
        let region_id = 1;
        let region = gen_test_region(region_id, 1, 1);
        let db_dir = Builder::new()
            .prefix("test-snap-corruption-db")
            .tempdir()
            .unwrap();
        let db: KvTestEngine = open_test_db(db_dir.path(), None, None).unwrap();
        let snapshot = db.snapshot(None);

        let dir = Builder::new()
            .prefix("test-snap-corruption")
            .tempdir()
            .unwrap();
        let key = SnapKey::new(region_id, 1, 1);
        let mgr_core = create_manager_core(dir.path().to_str().unwrap(), u64::MAX);
        let mut s1 = Snapshot::new_for_building(dir.path(), &key, &mgr_core);
        assert!(!s1.exists());

        let snap_data = s1
            .build(&db, &snapshot, &region, true, false, UnixSecs::now())
            .unwrap();
        assert!(s1.exists());

        let dst_dir = Builder::new()
            .prefix("test-snap-corruption-dst")
            .tempdir()
            .unwrap();
        let mut s2 = Snapshot::new_for_receiving(
            dst_dir.path(),
            &key,
            &mgr_core,
            snap_data.get_meta().clone(),
        )
        .unwrap();
        let copy_size = io::copy(&mut &s1, &mut &s2).unwrap();
        assert!(!s2.exists());
        s2.save().unwrap();
        assert!(s2.exists());

        let metas = corrupt_snapshot_checksum_in(dst_dir.path());
        assert_eq!(1, metas.len());
        assert!(s2.exists());

        let dst_db_dir = Builder::new()
            .prefix("test-snap-corruption-dst-db")
            .tempdir()
            .unwrap();
        let dst_db: KvTestEngine = open_test_empty_db(dst_db_dir.path(), None, None).unwrap();
        let options = ApplyOptions {
            db: dst_db,
            region,
            abort: Arc::new(AtomicUsize::new(JOB_STATUS_RUNNING)),
            write_batch_size: TEST_WRITE_BATCH_SIZE,
            coprocessor_host: CoprocessorHost::<KvTestEngine>::default(),
            ingest_copy_symlink: false,
        };
        s2.apply(options).unwrap_err();
    }
    */
    /*
            #[test]
            fn test_snap_corruption_on_meta_file() {
                let region_id = 1;
                let region = gen_test_region(region_id, 1, 1);
                let db_dir = Builder::new()
                    .prefix("test-snapshot-corruption-meta-db")
                    .tempdir()
                    .unwrap();
                let db: KvTestEngine = open_test_db_with_100keys(db_dir.path(), None, None).unwrap();
                let snapshot = db.snapshot(None);

                let dir = Builder::new()
                    .prefix("test-snap-corruption-meta")
                    .tempdir()
                    .unwrap();
                let key = SnapKey::new(region_id, 1, 1);
                let mgr_core = create_manager_core(dir.path().to_str().unwrap(), 500);
                let mut s1 = Snapshot::new_for_building(dir.path(), &key, &mgr_core);
                assert!(!s1.exists());

                let _ = s1
                    .build(&db, &snapshot, &region, true, false, UnixSecs::now())
                    .unwrap();
                assert!(s1.exists());

                assert_eq!(1, corrupt_snapshot_meta_file(dir.path()));

                Snapshot::new_for_sending(dir.path(), &key, &mgr_core).unwrap_err();

                let mut s2 = Snapshot::new_for_building(dir.path(), &key, &mgr_core);
                assert!(!s2.exists());
                let mut snap_data = s2
                    .build(&db, &snapshot, &region, true, false, UnixSecs::now())
                    .unwrap();
                assert!(s2.exists());

                let dst_dir = Builder::new()
                    .prefix("test-snap-corruption-meta-dst")
                    .tempdir()
                    .unwrap();
                copy_snapshot(
                    &dir,
                    &dst_dir,
                    &key,
                    &mgr_core,
                    snap_data.get_meta().clone(),
                );

                assert_eq!(1, corrupt_snapshot_meta_file(dst_dir.path()));

                Snapshot::new_for_applying(dst_dir.path(), &key, &mgr_core).unwrap_err();
                Snapshot::new_for_receiving(dst_dir.path(), &key, &mgr_core, snap_data.take_meta())
                    .unwrap_err();
            }
    */
    #[test]
    fn test_snap_mgr_create_dir() {
        // Ensure `mgr` creates the specified directory when it does not exist.
        let temp_dir = Builder::new()
            .prefix("test-snap-mgr-create-dir")
            .tempdir()
            .unwrap();
        let temp_path = temp_dir.path().join("snap1");
        let path = temp_path.to_str().unwrap().to_owned();
        assert!(!temp_path.exists());
        let mut mgr = SnapManager::new(path);
        mgr.init().unwrap();
        assert!(temp_path.exists());

        // Ensure `init()` will return an error if specified target is a file.
        let temp_path2 = temp_dir.path().join("snap2");
        let path2 = temp_path2.to_str().unwrap().to_owned();
        File::create(temp_path2).unwrap();
        mgr = SnapManager::new(path2);
        mgr.init().unwrap_err();
    }
    /*
                #[test]
                fn test_snap_mgr_v2() {
                    let temp_dir = Builder::new().prefix("test-snap-mgr-v2").tempdir().unwrap();
                    let path = temp_dir.path().to_str().unwrap().to_owned();
                    let mgr = SnapManager::new(path.clone());
                    mgr.init().unwrap();
                    assert_eq!(mgr.get_total_snap_size().unwrap(), 0);

                    let db_dir = Builder::new()
                        .prefix("test-snap-mgr-delete-temp-files-v2-db")
                        .tempdir()
                        .unwrap();
                    let db: KvTestEngine = open_test_db(db_dir.path(), None, None).unwrap();
                    let snapshot = db.snapshot(None);
                    let key1 = SnapKey::new(1, 1, 1);
                    let mgr_core = create_manager_core(&path, u64::MAX);
                    let mut s1 = Snapshot::new_for_building(&path, &key1, &mgr_core);
                    let mut region = gen_test_region(1, 1, 1);
                    let mut snap_data = s1
                        .build(&db, &snapshot, &region, true, false, UnixSecs::now())
                        .unwrap();
                    let mut s = Snapshot::new_for_sending(&path, &key1, &mgr_core);
                    let expected_size = s.total_size();
                    let mut s2 =
                        Snapshot::new_for_receiving(&path, &key1, &mgr_core, snap_data.get_meta().clone())
                            .unwrap();
                    let n = io::copy(&mut s, &mut s2).unwrap();
                    assert_eq!(n, expected_size);
                    s2.save().unwrap();

                    let key2 = SnapKey::new(2, 1, 1);
                    region.set_id(2);
                    snap_data.set_region(region);
                    let s3 = Snapshot::new_for_building(&path, &key2, &mgr_core);
                    let s4 =
                        Snapshot::new_for_receiving(&path, &key2, &mgr_core, snap_data.take_meta()).unwrap();

                    assert!(s1.exists());
                    assert!(s2.exists());
                    assert!(!s3.exists());
                    assert!(!s4.exists());

                    let mgr = SnapManager::new(path);
                    mgr.init().unwrap();
                    assert_eq!(mgr.get_total_snap_size().unwrap(), expected_size * 2);

                    assert!(s1.exists());
                    assert!(s2.exists());
                    assert!(!s3.exists());
                    assert!(!s4.exists());

                    mgr.get_snapshot_for_sending(&key1).unwrap().delete();
                    assert_eq!(mgr.get_total_snap_size().unwrap(), expected_size);
                    mgr.get_snapshot_for_applying(&key1).unwrap().delete();
                    assert_eq!(mgr.get_total_snap_size().unwrap(), 0);
                }
    */
    fn check_registry_around_deregister(mgr: &SnapManager, key: &SnapKey) {
        let snap_keys = mgr.list_idle_snap().unwrap();
        assert!(snap_keys.is_empty());
        assert!(mgr.has_registered(key));
        mgr.deregister(key);
        let mut snap_keys = mgr.list_idle_snap().unwrap();
        assert_eq!(snap_keys.len(), 1);
        let snap_key = snap_keys.pop().unwrap().0;
        assert_eq!(snap_key, *key);
        assert!(!mgr.has_registered(&snap_key));
    }

    #[test]
    fn test_snap_deletion_on_registry() {
        let src_temp_dir = Builder::new()
            .prefix("test-snap-deletion-on-registry-src")
            .tempdir()
            .unwrap();
        let src_path = src_temp_dir.path().to_str().unwrap().to_owned();
        let src_mgr = SnapManager::new(src_path);
        src_mgr.init().unwrap();

        let src_db_dir = Builder::new()
            .prefix("test-snap-deletion-on-registry-src-db")
            .tempdir()
            .unwrap();
        let db: KvTestEngine = open_test_db(src_db_dir.path(), None, None).unwrap();
        let snapshot = db.snapshot(None);

        let key = SnapKey::new(1, 1, 1);
        let region = gen_test_region(1, 1, 1);

        // Ensure the snapshot being built will not be deleted on GC.
        let mut s1 = src_mgr.get_snapshot_for_building(&key).unwrap();
        let mut snap_data = s1
            .build(&db, &snapshot, &region, true, false, UnixSecs::now())
            .unwrap();

        // Ensure the snapshot being sent will not be deleted on GC.
        let expected_size = s1.total_size();

        let dst_temp_dir = Builder::new()
            .prefix("test-snap-deletion-on-registry-dst")
            .tempdir()
            .unwrap();
        let dst_path = dst_temp_dir.path().to_str().unwrap().to_owned();
        let dst_mgr = SnapManager::new(dst_path);
        dst_mgr.init().unwrap();

        // Ensure the snapshot being received will not be deleted on GC.
        let mut s3 = dst_mgr
            .get_snapshot_for_receiving(&key, snap_data.take_meta())
            .unwrap();
        let n = io::copy(&mut &*s1, &mut &*s3).unwrap();
        assert_eq!(n, expected_size);
        s3.save().unwrap();

        check_registry_around_deregister(&dst_mgr, &key);

        // Ensure the snapshot to be applied will not be deleted on GC.
        let mut snap_keys = dst_mgr.list_idle_snap().unwrap();
        assert_eq!(snap_keys.len(), 1);
        let snap_key = snap_keys.pop().unwrap().0;
        assert_eq!(snap_key, key);
        assert!(!dst_mgr.has_registered(&snap_key));
        let s4 = dst_mgr.get_snapshot_for_applying(&key).unwrap();
        let s5 = dst_mgr.get_snapshot_for_applying(&key).unwrap();
        dst_mgr.delete_snapshot(&key, s4.as_ref());
        assert!(s5.exists());
    }
    /*
        #[test]
        fn test_snapshot_max_total_size() {
            let regions: Vec<u64> = (0..20).collect();
            let kv_path = Builder::new()
                .prefix("test-snapshot-max-total-size-db")
                .tempdir()
                .unwrap();
            // Disable property collection so that the total snapshot size
            // isn't dependent on them.
            let kv_cf_opts = ALL_CFS
                .iter()
                .map(|cf| {
                    let mut cf_opts = CfOptions::new();
                    cf_opts.set_no_range_properties(true);
                    cf_opts.set_no_table_properties(true);
                    (*cf, cf_opts)
                })
                .collect();
            let engine =
                get_test_db_for_regions(&kv_path, None, None, Some(kv_cf_opts), &regions).unwrap();

            let snapfiles_path = Builder::new()
                .prefix("test-snapshot-max-total-size-snapshots")
                .tempdir()
                .unwrap();
            let max_total_size = 10240;
            let snap_mgr = SnapManagerBuilder::default()
                .max_total_size(max_total_size)
                .build::<_>(snapfiles_path.path().to_str().unwrap());
            snap_mgr.init().unwrap();
            let snapshot = engine.kv.snapshot(None);

            // Add an oldest snapshot for receiving.
            let recv_key = SnapKey::new(100, 100, 100);
            let mut recv_head = {
                let mut s = snap_mgr.get_snapshot_for_building(&recv_key).unwrap();
                s.build(
                    &engine.kv,
                    &snapshot,
                    &gen_test_region(100, 1, 1),
                    true,
                    false,
                    UnixSecs::now(),
                )
                .unwrap()
            };
            let recv_remain = {
                let mut data = Vec::with_capacity(1024);
                let mut s = snap_mgr.get_snapshot_for_sending(&recv_key).unwrap();
                s.as_ref().read_to_end(&mut data).unwrap();
                snap_mgr.deregister(&recv_key);
                assert!(snap_mgr.delete_snapshot(&recv_key, s.as_ref()));
                data
            };
            let mut s = snap_mgr
                .get_snapshot_for_receiving(&recv_key, recv_head.take_meta())
                .unwrap();
            s.as_ref().write_all(&recv_remain).unwrap();
            s.save().unwrap();

            let snap_size = snap_mgr.get_total_snap_size().unwrap();
            let max_snap_count = (max_total_size + snap_size - 1) / snap_size;
            for (i, region_id) in regions.into_iter().enumerate() {
                let key = SnapKey::new(region_id, 1, 1);
                let region = gen_test_region(region_id, 1, 1);

                let mut s = snap_mgr.get_snapshot_for_building(&key).unwrap();
                let mut head = s
                    .build(&engine.kv, &snapshot, &region, true, false, UnixSecs::now())
                    .unwrap();
                let mut data = Vec::with_capacity(1024);
                s.as_ref().read_to_end(&mut data).unwrap();
                snap_mgr.deregister(&key);

                let mut s = snap_mgr
                    .get_snapshot_for_receiving(&key, head.take_meta())
                    .unwrap();
                s.as_ref().write_all(&data).unwrap();
                s.save().unwrap();

                // The first snap_size is for region 100.
                // That snapshot won't be deleted because it's not for generating.
                assert_eq!(
                    snap_mgr.get_total_snap_size().unwrap(),
                    snap_size * cmp::min(max_snap_count, (i + 2) as u64)
                );
            }
        }
    */

    #[test]
    fn test_snap_temp_file_delete() {
        let src_temp_dir = Builder::new()
            .prefix("test_snap_temp_file_delete_snap")
            .tempdir()
            .unwrap();
        let mgr_path = src_temp_dir.path().to_str().unwrap();
        let src_mgr = SnapManager::new(mgr_path.to_owned());
        src_mgr.init().unwrap();
        let kv_temp_dir = Builder::new()
            .prefix("test_snap_temp_file_delete_kv")
            .tempdir()
            .unwrap();
        let engine = open_test_db(kv_temp_dir.path(), None, None).unwrap();
        let sst_path = src_mgr.get_temp_path_for_ingest();
        let mut writer = <KvTestEngine as SstExt>::SstWriterBuilder::new()
            .set_db(&engine)
            .build(&sst_path)
            .unwrap();
        writer.put(b"a", b"a").unwrap();
        let r = writer.finish().unwrap();
        assert!(file_system::file_exists(&sst_path));
        assert_eq!(r.file_path().to_str().unwrap(), sst_path.as_str());
        drop(src_mgr);
        let src_mgr = SnapManager::new(mgr_path.to_owned());
        src_mgr.init().unwrap();
        // The sst_path will be deleted by SnapManager because it is a temp filet.
        assert!(!file_system::file_exists(&sst_path));
    }

    #[test]
    fn test_snapshot_stats() {
        let snap_dir = Builder::new()
            .prefix("test_snapshot_stats")
            .tempdir()
            .unwrap();
        let start = Instant::now();
        let mgr = TabletSnapManager::new(snap_dir.path(), None).unwrap();
        let key = TabletSnapKey::new(1, 1, 1, 1);
        mgr.begin_snapshot(key.clone(), start - time::Duration::from_secs(2), 1);
        // filter out the snapshot that is not finished
        assert!(mgr.stats().stats.is_empty());
        mgr.finish_snapshot(key.clone(), start - time::Duration::from_secs(1));
        let stats = mgr.stats().stats;
        assert_eq!(stats.len(), 1);
        assert_eq!(stats[0].get_total_duration_sec(), 2);
        assert!(mgr.stats().stats.is_empty());

        // filter out the total duration seconds less than one sencond.
        let path = mgr.tablet_gen_path(&key);
        std::fs::create_dir_all(&path).unwrap();
        assert!(path.exists());
        mgr.delete_snapshot(&key);
        assert_eq!(mgr.stats().stats.len(), 0);
        assert!(!path.exists());
    }
    /*
        #[test]
        fn test_build_with_encryption() {
            let (_enc_dir, key_manager) =
                create_encryption_key_manager("test_build_with_encryption_enc");

            let snap_dir = Builder::new()
                .prefix("test_build_with_encryption_snap")
                .tempdir()
                .unwrap();
            let _mgr_path = snap_dir.path().to_str().unwrap();
            let snap_mgr = SnapManagerBuilder::default()
                .encryption_key_manager(Some(key_manager))
                .build(snap_dir.path().to_str().unwrap());
            snap_mgr.init().unwrap();

            let kv_dir = Builder::new()
                .prefix("test_build_with_encryption_kv")
                .tempdir()
                .unwrap();
            let db: KvTestEngine = open_test_db(kv_dir.path(), None, None).unwrap();
            let snapshot = db.snapshot(None);
            let key = SnapKey::new(1, 1, 1);
            let region = gen_test_region(1, 1, 1);

            // Test one snapshot can be built multi times. DataKeyManager should be handled
            // correctly.
            for _ in 0..2 {
                let mut s1 = snap_mgr.get_snapshot_for_building(&key).unwrap();
                let _ = s1
                    .build(&db, &snapshot, &region, true, false, UnixSecs::now())
                    .unwrap();
                assert!(snap_mgr.delete_snapshot(&key, &s1, false));
            }
        }

        #[test]
        fn test_generate_snap_for_tablet_snapshot() {
            let snap_dir = Builder::new().prefix("test_snapshot").tempdir().unwrap();
            let snap_mgr = SnapManagerBuilder::default()
                .enable_receive_tablet_snapshot(true)
                .build(snap_dir.path().to_str().unwrap());
            snap_mgr.init().unwrap();
            let tablet_snap_key = TabletSnapKey::new(1, 2, 3, 4);
            snap_mgr
                .gen_empty_snapshot_for_tablet_snapshot(&tablet_snap_key, false)
                .unwrap();

            let snap_key = SnapKey::new(1, 3, 4);
            let s = snap_mgr.get_snapshot_for_applying(&snap_key).unwrap();
            let expect_path = snap_mgr
                .tablet_snap_manager()
                .as_ref()
                .unwrap()
                .final_recv_path(&tablet_snap_key);
            assert_eq!(expect_path.to_str().unwrap(), s.tablet_snap_path().unwrap());
        }

        #[test]
        fn test_init_enable_receive_tablet_snapshot() {
            let builder = SnapManagerBuilder::default().enable_receive_tablet_snapshot(true);
            let snap_dir = Builder::new()
                .prefix("test_snap_path_does_not_exist")
                .tempdir()
                .unwrap();
            let path = snap_dir.path().join("snap");
            let snap_mgr = builder.build(path.as_path().to_str().unwrap());
            snap_mgr.init().unwrap();

            assert!(path.exists());
            let mut path = path.as_path().to_str().unwrap().to_string();
            path.push_str("_v2");
            assert!(Path::new(&path).exists());

            let builder = SnapManagerBuilder::default().enable_receive_tablet_snapshot(true);
            let snap_dir = Builder::new()
                .prefix("test_snap_path_exist")
                .tempdir()
                .unwrap();
            let path = snap_dir.path();
            let snap_mgr = builder.build(path.to_str().unwrap());
            snap_mgr.init().unwrap();

            let mut path = path.to_str().unwrap().to_string();
            path.push_str("_v2");
            assert!(Path::new(&path).exists());

            let builder = SnapManagerBuilder::default().enable_receive_tablet_snapshot(true);
            let snap_dir = Builder::new()
                .prefix("test_tablet_snap_path_exist")
                .tempdir()
                .unwrap();
            let path = snap_dir.path().join("snap/v2");
            fs::create_dir_all(path).unwrap();
            let path = snap_dir.path().join("snap");
            let snap_mgr = builder.build(path.to_str().unwrap());
            snap_mgr.init().unwrap();
            assert!(path.exists());
        }

        #[test]
        fn test_from_path() {
            let snap_dir = Builder::new().prefix("test_from_path").tempdir().unwrap();
            let path = snap_dir.path().join("gen_1_2_3_4");
            let key = TabletSnapKey::from_path(path).unwrap();
            let expect_key = TabletSnapKey::new(1, 2, 3, 4);
            assert_eq!(expect_key, key);
            let path = snap_dir.path().join("gen_1_2_3_4.tmp");
            TabletSnapKey::from_path(path).unwrap_err();
        }

        #[test]
        fn test_snap_recv_limiter() {
            let ttl_secs = 60;

            let limiter = SnapRecvConcurrencyLimiter::new(1, ttl_secs);
            limiter.finish_recv(10); // calling finish_recv() on an empty limiter is fine.
            assert!(limiter.try_recv(1)); // first recv should succeed

            // limiter.try_recv(2) should fail because we've reached the limit. But
            // calling limiter.try_recv(1) should succeed again due to idempotence.
            assert!(!limiter.try_recv(2));
            assert!(limiter.try_recv(1));

            // After finish_recv(1) is called, try_recv(2) should succeed.
            limiter.finish_recv(1);
            assert!(limiter.try_recv(2));

            // Dynamically change the limit to 2, which will allow one more receive.
            limiter.set_limit(2);
            assert!(limiter.try_recv(1));
            assert!(!limiter.try_recv(3));

            limiter.finish_recv(1);
            limiter.finish_recv(2);
            // If we reserve a capacity of 1, the limiter will only allow one receive.
            limiter.set_reserved_capacity(1);
            assert!(limiter.try_recv(1));
            assert!(!limiter.try_recv(2));

            // Test the evict_expired_timestamps function.
            let t_now = Instant::now();
            let mut timestamps = [
                (1, t_now - Duration::from_secs(ttl_secs + 2)), // expired
                (2, t_now - Duration::from_secs(ttl_secs + 1)), // expired
                (3, t_now - Duration::from_secs(ttl_secs - 1)), // alive
                (4, t_now),                                     // alive
            ]
            .iter()
            .cloned()
            .collect();

            limiter.evict_expired_timestamps(&mut timestamps, t_now);
            assert_eq!(timestamps.len(), 2);
            assert!(timestamps.contains_key(&3));
            assert!(timestamps.contains_key(&4));
            // Test the expiring logic in try_recv(1) with a 0s TTL, which
            // effectively means there's no limit.
            let limiter = SnapRecvConcurrencyLimiter::new(1, 0);
            assert!(limiter.try_recv(1));
            assert!(limiter.try_recv(2));
            assert!(limiter.try_recv(3));

            // After canceling the limit, the capacity of the VecDeque should be 0.
            limiter.set_limit(0);
            assert!(limiter.try_recv(1));
            assert!(limiter.timestamps.lock().unwrap().capacity() == 0);

            // Test initializing a limiter with no limit.
            let limiter = SnapRecvConcurrencyLimiter::new(0, 0);
            assert!(limiter.try_recv(1));
            assert!(limiter.timestamps.lock().unwrap().capacity() == 0);
        }
    */
}
