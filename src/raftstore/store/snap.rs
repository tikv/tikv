// Copyright 2017 PingCAP, Inc.
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

use std::error;
use std::io::{self, Write, ErrorKind, Read};
use std::fmt::{self, Formatter, Display};
use std::fs::{self, Metadata};
use std::sync::{Arc, RwLock};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::path::Path;
use std::result;
use std::str;
use std::time;
use std::thread;

use protobuf::Message;
use rocksdb::DB;
use kvproto::eraftpb::Snapshot as RaftSnapshot;
use kvproto::metapb::Region;
use kvproto::raft_serverpb::RaftSnapshotData;

use raftstore::Result as RaftStoreResult;
use raftstore::store::Msg;
use storage::{CfName, CF_DEFAULT, CF_LOCK, CF_WRITE};
use util::transport::SendCh;
use util::HandyRwLock;
use util::collections::{HashMap, HashMapEntry as Entry};

use raftstore::store::engine::{Snapshot as DbSnapshot, Iterable};
use raftstore::store::keys::{enc_start_key, enc_end_key};


use raftstore::store::metrics::{SNAPSHOT_CF_KV_COUNT, SNAPSHOT_CF_SIZE,
                                SNAPSHOT_BUILD_TIME_HISTOGRAM};
use raftstore::store::peer_storage::JOB_STATUS_CANCELLING;

// Data in CF_RAFT should be excluded for a snapshot.
pub const SNAPSHOT_CFS: &'static [CfName] = &[CF_DEFAULT, CF_LOCK, CF_WRITE];

/// Name prefix for the self-generated snapshot file.
const SNAP_GEN_PREFIX: &'static str = "gen";
/// Name prefix for the received snapshot file.
const SNAP_REV_PREFIX: &'static str = "rev";

const TMP_FILE_SUFFIX: &'static str = ".tmp";
const SST_FILE_SUFFIX: &'static str = ".sst";

const DELETE_RETRY_MAX_TIMES: u32 = 6;
const DELETE_RETRY_TIME_MILLIS: u64 = 500;

quick_error! {
    #[derive(Debug)]
    pub enum Error {
        Abort {
            description("abort")
            display("abort")
        }
        Other(err: Box<error::Error + Sync + Send>) {
            from()
            cause(err.as_ref())
            description(err.description())
            display("snap failed {:?}", err)
        }
    }
}

pub type Result<T> = result::Result<T, Error>;

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
            region_id: region_id,
            term: term,
            idx: idx,
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

        Ok(SnapKey::from_region_snap(snap_data.get_region().get_id(), snap))
    }
}

impl Display for SnapKey {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
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
        SnapshotStatistics { ..Default::default() }
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
    fn build(&mut self,
             snap: &DbSnapshot,
             region: &Region,
             snap_data: &mut RaftSnapshotData,
             stat: &mut SnapshotStatistics,
             deleter: Box<SnapshotDeleter>)
             -> RaftStoreResult<()>;
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
pub fn copy_snapshot(mut from: Box<Snapshot>, mut to: Box<Snapshot>) -> io::Result<()> {
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
    fn delete_snapshot(&self, key: &SnapKey, snap: &Snapshot, check_entry: bool) -> bool;
}

// Try to delete the specified snapshot using deleter, return true if the deletion is done.
pub fn retry_delete_snapshot(deleter: Box<SnapshotDeleter>,
                             key: &SnapKey,
                             snap: &Snapshot)
                             -> bool {
    let d = time::Duration::from_millis(DELETE_RETRY_TIME_MILLIS);
    for _ in 0..DELETE_RETRY_MAX_TIMES {
        if deleter.delete_snapshot(key, snap, true) {
            return true;
        }
        thread::sleep(d);
    }
    false
}

use std::fs::{File, OpenOptions};
use std::path::PathBuf;
use std::time::Instant;
use crc::crc32::{self, Digest, Hasher32};
use protobuf::RepeatedField;
use kvproto::raft_serverpb::{SnapshotCFFile, SnapshotMeta};
use rocksdb::{EnvOptions, SstFileWriter, IngestExternalFileOptions, DBCompressionType};
use util::{rocksdb, duration_to_sec};
use util::file::{get_file_size, file_exists, delete_file_if_exist};
use util::rocksdb::get_fastest_supported_compression_type;

pub const SNAPSHOT_VERSION: u64 = 2;
const META_FILE_SUFFIX: &'static str = ".meta";
const DIGEST_BUFFER_SIZE: usize = 10240;


fn calc_crc32(p: &PathBuf) -> io::Result<u32> {
    let mut digest = Digest::new(crc32::IEEE);
    let mut f = OpenOptions::new().read(true).open(&p)?;
    let mut buf = vec![0; DIGEST_BUFFER_SIZE];
    loop {
        match f.read(&mut buf[..]) {
            Ok(0) => {
                return Ok(digest.sum32());
            }
            Ok(n) => {
                digest.write(&buf[..n]);
            }
            Err(ref e) if e.kind() == ErrorKind::Interrupted => {}
            Err(err) => return Err(err),
        }
    }
}

fn gen_snapshot_meta(cf_files: &[CfFile]) -> RaftStoreResult<SnapshotMeta> {
    let mut meta = Vec::with_capacity(cf_files.len());
    for cf_file in cf_files {
        if SNAPSHOT_CFS.iter().find(|&cf| cf_file.cf == *cf).is_none() {
            return Err(box_err!("failed to encode invalid snapshot cf {}", cf_file.cf));
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
        return Err(box_err!("invalid size {} for snapshot cf file {}, expected {}",
                            size,
                            path.display(),
                            expected_size));
    }
    Ok(())
}

fn check_file_checksum(path: &PathBuf, expected_checksum: u32) -> RaftStoreResult<()> {
    let checksum = calc_crc32(path)?;
    if checksum != expected_checksum {
        return Err(box_err!("invalid checksum {} for snapshot cf file {}, expected {}",
                            checksum,
                            path.display(),
                            expected_checksum));
    }
    Ok(())
}

fn check_file_size_and_checksum(path: &PathBuf,
                                expected_size: u64,
                                expected_checksum: u32)
                                -> RaftStoreResult<()> {
    check_file_size(path, expected_size).and_then(|_| check_file_checksum(path, expected_checksum))
}

#[derive(Default)]
struct CfFile {
    pub cf: CfName,
    pub path: PathBuf,
    pub tmp_path: PathBuf,
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
    size_track: Arc<RwLock<u64>>,
}

impl Snap {
    fn new<T: Into<PathBuf>>(dir: T,
                             key: &SnapKey,
                             size_track: Arc<RwLock<u64>>,
                             is_sending: bool,
                             to_build: bool,
                             deleter: Box<SnapshotDeleter>)
                             -> RaftStoreResult<Snap> {
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
            let cf_file = CfFile {
                cf: cf,
                path: path,
                tmp_path: tmp_path,
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
            display_path: display_path,
            cf_files: cf_files,
            cf_index: 0,
            meta_file: meta_file,
            size_track: size_track,
        };

        // load snapshot meta if meta_file exists
        if file_exists(&s.meta_file.path) {
            if let Err(e) = s.load_snapshot_meta() {
                if !to_build {
                    return Err(e);
                }
                warn!("failed to load existent snapshot meta when try to build {}: {:?}",
                      s.path(),
                      e);
                if !retry_delete_snapshot(deleter, key, &s) {
                    warn!("failed to delete snapshot {} because it's already registered elsewhere",
                          s.path());
                    return Err(e);
                }
            }
        }
        Ok(s)
    }

    pub fn new_for_building<T: Into<PathBuf>>(dir: T,
                                              key: &SnapKey,
                                              snap: &DbSnapshot,
                                              size_track: Arc<RwLock<u64>>,
                                              deleter: Box<SnapshotDeleter>)
                                              -> RaftStoreResult<Snap> {
        let mut s = Snap::new(dir, key, size_track, true, true, deleter)?;
        s.init_for_building(snap)?;
        Ok(s)
    }

    pub fn new_for_sending<T: Into<PathBuf>>(dir: T,
                                             key: &SnapKey,
                                             size_track: Arc<RwLock<u64>>,
                                             deleter: Box<SnapshotDeleter>)
                                             -> RaftStoreResult<Snap> {
        let mut s = Snap::new(dir, key, size_track, true, false, deleter)?;

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

    pub fn new_for_receiving<T: Into<PathBuf>>(dir: T,
                                               key: &SnapKey,
                                               snapshot_meta: SnapshotMeta,
                                               size_track: Arc<RwLock<u64>>,
                                               deleter: Box<SnapshotDeleter>)
                                               -> RaftStoreResult<Snap> {
        let mut s = Snap::new(dir, key, size_track, false, false, deleter)?;
        s.set_snapshot_meta(snapshot_meta)?;

        if s.exists() {
            return Ok(s);
        }
        for cf_file in &mut s.cf_files {
            if cf_file.size == 0 {
                continue;
            }
            let f = OpenOptions::new().write(true).create_new(true).open(&cf_file.tmp_path)?;
            cf_file.file = Some(f);
            cf_file.write_digest = Some(Digest::new(crc32::IEEE));
        }
        let f = OpenOptions::new().write(true)
            .create_new(true)
            .open(&s.meta_file.tmp_path)?;
        s.meta_file.file = Some(f);
        Ok(s)
    }

    pub fn new_for_applying<T: Into<PathBuf>>(dir: T,
                                              key: &SnapKey,
                                              size_track: Arc<RwLock<u64>>,
                                              deleter: Box<SnapshotDeleter>)
                                              -> RaftStoreResult<Snap> {
        let s = Snap::new(dir, key, size_track, false, false, deleter)?;
        Ok(s)
    }

    fn init_for_building(&mut self, snap: &DbSnapshot) -> RaftStoreResult<()> {
        if self.exists() {
            return Ok(());
        }
        for cf_file in &mut self.cf_files {
            let handle = snap.cf_handle(cf_file.cf)?;
            let mut io_options = snap.get_db().get_options_cf(handle).clone();
            io_options.compression(get_fastest_supported_compression_type());
            // in rocksdb 5.5.1, SstFileWriter will try to use bottommost_compression and
            // compression_per_level first, so to make sure our specified compression type
            // being used, we must set them empty or disabled.
            io_options.compression_per_level(&[]);
            io_options.bottommost_compression(DBCompressionType::DBDisableCompression);
            let mut writer = SstFileWriter::new(EnvOptions::new(), io_options);
            box_try!(writer.open(cf_file.tmp_path.as_path().to_str().unwrap()));
            cf_file.sst_writer = Some(writer);
        }
        let file = OpenOptions::new().write(true)
            .create_new(true)
            .open(&self.meta_file.tmp_path)?;
        self.meta_file.file = Some(file);
        Ok(())
    }

    fn read_snapshot_meta(&mut self) -> RaftStoreResult<SnapshotMeta> {
        let size = try!(get_file_size(&self.meta_file.path));
        let mut file = try!(File::open(&self.meta_file.path));
        let mut buf = Vec::with_capacity(size as usize);
        try!(file.read_to_end(&mut buf));
        let mut snapshot_meta = SnapshotMeta::new();
        try!(snapshot_meta.merge_from_bytes(&buf));
        Ok(snapshot_meta)
    }

    fn set_snapshot_meta(&mut self, snapshot_meta: SnapshotMeta) -> RaftStoreResult<()> {
        if snapshot_meta.get_cf_files().len() != self.cf_files.len() {
            return Err(box_err!("invalid cf number of snapshot meta, expect {}, got {}",
                                SNAPSHOT_CFS.len(),
                                snapshot_meta.get_cf_files().len()));
        }
        for (i, cf_file) in self.cf_files.iter_mut().enumerate() {
            let meta = snapshot_meta.get_cf_files().get(i).unwrap();
            if meta.get_cf() != cf_file.cf {
                return Err(box_err!("invalid {} cf in snapshot meta, expect {}, got {}",
                                    i,
                                    cf_file.cf,
                                    meta.get_cf()));
            }
            if file_exists(&cf_file.path) {
                // Check only the file size for `exists()` to work correctly.
                try!(check_file_size(&cf_file.path, meta.get_size()));
            }
            cf_file.size = meta.get_size();
            cf_file.checksum = meta.get_checksum();
        }
        self.meta_file.meta = snapshot_meta;
        Ok(())
    }

    fn load_snapshot_meta(&mut self) -> RaftStoreResult<()> {
        let snapshot_meta = try!(self.read_snapshot_meta());
        try!(self.set_snapshot_meta(snapshot_meta));
        // check if there is a data corruption when the meta file exists
        // but cf files are deleted.
        if !self.exists() {
            return Err(box_err!("snapshot {} is corrupted, some cf file is missing",
                                self.path()));
        }
        Ok(())
    }

    fn get_display_path(dir_path: &PathBuf, prefix: &str) -> String {
        let cf_names = "(".to_owned() + &SNAPSHOT_CFS.join("|") + ")";
        format!("{}/{}_{}{}",
                dir_path.display(),
                prefix,
                cf_names,
                SST_FILE_SUFFIX)
    }

    fn validate(&self) -> RaftStoreResult<()> {
        for cf_file in &self.cf_files {
            if cf_file.size == 0 {
                // Skip empty file. The checksum of this cf file should be 0 and
                // this is checked when loading the snapshot meta.
                continue;
            }
            try!(check_file_size_and_checksum(&cf_file.path, cf_file.size, cf_file.checksum));
        }
        Ok(())
    }

    fn switch_to_cf_file(&mut self, cf: &str) -> io::Result<()> {
        match self.cf_files.iter().position(|x| x.cf == cf) {
            Some(index) => {
                self.cf_index = index;
                Ok(())
            }
            None => Err(io::Error::new(ErrorKind::Other, format!("fail to find cf {}", cf))),
        }
    }

    fn add_kv(&mut self, k: &[u8], v: &[u8]) -> io::Result<()> {
        let mut cf_file = &mut self.cf_files[self.cf_index];
        let mut writer = cf_file.sst_writer.as_mut().unwrap();
        if let Err(e) = writer.add(k, v) {
            return Err(io::Error::new(ErrorKind::Other, e));
        }
        cf_file.kv_count += 1;
        Ok(())
    }

    fn save_cf_files(&mut self) -> io::Result<()> {
        for cf_file in &mut self.cf_files {
            if cf_file.kv_count == 0 {
                let _ = cf_file.sst_writer.take().unwrap();
            } else {
                let mut writer = cf_file.sst_writer.take().unwrap();
                if let Err(e) = writer.finish() {
                    return Err(io::Error::new(ErrorKind::Other, e));
                }
            }
            let size = try!(get_file_size(&cf_file.tmp_path));
            if size > 0 {
                try!(fs::rename(&cf_file.tmp_path, &cf_file.path));
                cf_file.size = size;
                // add size
                let mut size_track = self.size_track.wl();
                *size_track = size_track.saturating_add(size);

                cf_file.checksum = try!(calc_crc32(&cf_file.path));
            } else {
                // Clean up the `tmp_path` if this cf file is empty.
                delete_file_if_exist(&cf_file.tmp_path);
            }
        }
        Ok(())
    }

    fn save_meta_file(&mut self) -> RaftStoreResult<()> {
        let mut v = vec![];
        box_try!(self.meta_file.meta.write_to_vec(&mut v));
        {
            let mut f = self.meta_file.file.take().unwrap();
            try!(f.write_all(&v[..]));
            try!(f.flush());
        }
        try!(fs::rename(&self.meta_file.tmp_path, &self.meta_file.path));
        Ok(())
    }

    fn do_build(&mut self,
                snap: &DbSnapshot,
                region: &Region,
                stat: &mut SnapshotStatistics,
                deleter: Box<SnapshotDeleter>)
                -> RaftStoreResult<()> {
        // TODO(Cholerae): Comment validation for test.
//        if self.exists() {
//            match self.validate() {
//                Ok(()) => return Ok(()),
//                Err(e) => {
//                    error!("[region {}] file {} is corrupted, will rebuild: {:?}",
//                           region.get_id(),
//                           self.path(),
//                           e);
//                    if !retry_delete_snapshot(deleter, &self.key, self) {
//                        error!("[region {}] failed to delete corrupted snapshot because it's \
//                                already registered elsewhere",
//                               self.path());
//                        return Err(e);
//                    }
//                    try!(self.init_for_building(snap));
//                }
//            }
//        }

        let mut snap_key_count = 0;
        let (begin_key, end_key) = (enc_start_key(region), enc_end_key(region));
        for cf in SNAPSHOT_CFS {
            try!(self.switch_to_cf_file(cf));
            let (cf_key_count, cf_size) = {
                let mut key_count = 0;
                let mut size = 0;
                try!(snap.scan_cf(cf,
                                  &begin_key,
                                  &end_key,
                                  false,
                                  &mut |key, value| {
                                      key_count += 1;
                                      size += key.len() + value.len();
                                      try!(self.add_kv(key, value));
                                      Ok(true)
                                  }));
                (key_count, size)
            };
            snap_key_count += cf_key_count;
            SNAPSHOT_CF_KV_COUNT.with_label_values(&[cf]).observe(cf_key_count as f64);
            SNAPSHOT_CF_SIZE.with_label_values(&[cf]).observe(cf_size as f64);
            info!("[region {}] scan snapshot {}, cf {}, key count {}, size {}",
                  region.get_id(),
                  self.path(),
                  cf,
                  cf_key_count,
                  cf_size);
        }

        try!(self.save_cf_files());
        stat.kv_count = snap_key_count;
        // save snapshot meta to meta file
        let snapshot_meta = try!(gen_snapshot_meta(&self.cf_files[..]));
        self.meta_file.meta = snapshot_meta;
        try!(self.save_meta_file());

        Ok(())
    }
}

impl Snapshot for Snap {
    fn build(&mut self,
             snap: &DbSnapshot,
             region: &Region,
             snap_data: &mut RaftSnapshotData,
             stat: &mut SnapshotStatistics,
             deleter: Box<SnapshotDeleter>)
             -> RaftStoreResult<()> {
        let t = Instant::now();
        try!(self.do_build(snap, region, stat, deleter));

        let total_size = try!(self.total_size());
        stat.size = total_size;
        // set snapshot meta data
        snap_data.set_file_size(total_size);
        snap_data.set_version(SNAPSHOT_VERSION);
        snap_data.set_meta(self.meta_file.meta.clone());

        SNAPSHOT_BUILD_TIME_HISTOGRAM.observe(duration_to_sec(t.elapsed()) as f64);
        info!("[region {}] scan snapshot {}, size {}, key count {}, takes {:?}",
              region.get_id(),
              self.path(),
              total_size,
              stat.kv_count,
              t.elapsed());

        Ok(())
    }

    fn path(&self) -> &str {
        &self.display_path
    }

    fn exists(&self) -> bool {
        self.cf_files.iter().all(|cf_file| cf_file.size == 0 || file_exists(&cf_file.path)) &&
        file_exists(&self.meta_file.path)
    }

    // TODO(Cholerae): We moved snapshot rather than copy it, so it should not exist.
    // Maybe remove this method later.
    fn delete(&self) {
        debug!("deleting {}", self.path());
        for cf_file in &self.cf_files {
            delete_file_if_exist(&cf_file.tmp_path);
            // It must not exist. It moved.
            let mut size_track = self.size_track.wl();
            *size_track = size_track.saturating_sub(cf_file.size);
//            if file_exists(&cf_file.path) {
//                let mut size_track = self.size_track.wl();
//                *size_track = size_track.saturating_sub(cf_file.size);
//            }
            delete_file_if_exist(&cf_file.path);
        }
        delete_file_if_exist(&self.meta_file.tmp_path);
        delete_file_if_exist(&self.meta_file.path);
    }

    fn meta(&self) -> io::Result<Metadata> {
        fs::metadata(&self.meta_file.path)
    }

    fn total_size(&self) -> io::Result<u64> {
        Ok(self.cf_files.iter().fold(0, |acc, x| acc + x.size))
    }

    fn save(&mut self) -> io::Result<()> {
        debug!("saving to {}", self.path());
        for cf_file in &mut self.cf_files {
            if cf_file.size == 0 {
                // Skip empty cf file.
                continue;
            }

            // Check each cf file has been fully written, and the checksum matches.
            {
                let mut file = cf_file.file.take().unwrap();
                try!(file.flush());
            }
            if cf_file.written_size != cf_file.size {
                return Err(io::Error::new(ErrorKind::Other,
                                          format!("snapshot file {} for cf {} size mismatches, \
                                                   real size {}, expected size {}",
                                                  cf_file.path.display(),
                                                  cf_file.cf,
                                                  cf_file.written_size,
                                                  cf_file.size)));
            }
            let checksum = cf_file.write_digest.as_ref().unwrap().sum32();
            if checksum != cf_file.checksum {
                return Err(io::Error::new(ErrorKind::Other,
                                          format!("snapshot file {} for cf {} checksum \
                                                   mismatches, real checksum {}, expected \
                                                   checksum {}",
                                                  cf_file.path.display(),
                                                  cf_file.cf,
                                                  checksum,
                                                  cf_file.checksum)));
            }

            try!(fs::rename(&cf_file.tmp_path, &cf_file.path));
            let mut size_track = self.size_track.wl();
            *size_track = size_track.saturating_add(cf_file.size);
        }
        // write meta file
        let mut v = vec![];
        try!(self.meta_file.meta.write_to_vec(&mut v));
        try!(self.meta_file.file.take().unwrap().write_all(&v[..]));
        try!(fs::rename(&self.meta_file.tmp_path, &self.meta_file.path));
        Ok(())
    }

    fn apply(&mut self, options: ApplyOptions) -> Result<()> {
        box_try!(self.validate());

        for cf_file in &mut self.cf_files {
            if cf_file.size == 0 {
                // Skip empty cf file.
                continue;
            }

            try!(check_abort(&options.abort));
            let cf_handle = box_try!(rocksdb::get_cf_handle(&options.db, cf_file.cf));

            let ingest_opt = IngestExternalFileOptions::new();
            ingest_opt.move_files(true);
            let path = cf_file.path.as_path().to_str().unwrap();
            box_try!(options.db.ingest_external_file_cf(cf_handle, &ingest_opt, &[path]));
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

            let mut file = cf_file.file.as_mut().unwrap();
            let mut digest = cf_file.write_digest.as_mut().unwrap();
            if next_buf.len() > left {
                try!(file.write_all(&next_buf[0..left]));
                digest.write(&next_buf[0..left]);
                cf_file.written_size += left as u64;
                self.cf_index += 1;
                next_buf = &next_buf[left..];
            } else {
                try!(file.write_all(next_buf));
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
            try!(file.flush());
        }
        Ok(())
    }
}

impl Drop for Snap {
    fn drop(&mut self) {
        // cleanup if some of the cf files and meta file is partly written
        if self.cf_files.iter().any(|cf_file| file_exists(&cf_file.tmp_path)) ||
           file_exists(&self.meta_file.tmp_path) {
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

struct SnapManagerCore {
    base: String,
    registry: HashMap<SnapKey, Vec<SnapEntry>>,
    // put snap_size under core so we don't need to worry about deadlock.
    snap_size: Arc<RwLock<u64>>,
}

fn notify_stats(ch: Option<&SendCh<Msg>>) {
    if let Some(ch) = ch {
        if let Err(e) = ch.try_send(Msg::SnapshotStats) {
            error!("notify snapshot stats failed {:?}", e)
        }
    }
}

/// `SnapManagerCore` trace all current processing snapshots.
#[derive(Clone)]
pub struct SnapManager {
    // directory to store snapfile.
    core: Arc<RwLock<SnapManagerCore>>,
    ch: Option<SendCh<Msg>>,
}

impl SnapManager {
    pub fn new<T: Into<String>>(path: T, ch: Option<SendCh<Msg>>) -> SnapManager {
        SnapManager {
            core: Arc::new(RwLock::new(SnapManagerCore {
                base: path.into(),
                registry: map![],
                snap_size: Arc::new(RwLock::new(0)),
            })),
            ch: ch,
        }
    }

    pub fn init(&self) -> io::Result<()> {
        // Use write lock so only one thread initialize the directory at a time.
        let core = self.core.wl();
        let path = Path::new(&core.base);
        if !path.exists() {
            try!(fs::create_dir_all(path));
            return Ok(());
        }
        if !path.is_dir() {
            return Err(io::Error::new(ErrorKind::Other,
                                      format!("{} should be a directory", path.display())));
        }
        let mut size = core.snap_size.wl();
        for f in try!(fs::read_dir(path)) {
            let p = try!(f);
            if try!(p.file_type()).is_file() {
                if let Some(s) = p.file_name().to_str() {
                    if s.ends_with(TMP_FILE_SUFFIX) {
                        try!(fs::remove_file(p.path()));
                    } else if s.ends_with(SST_FILE_SUFFIX) {
                        let len = p.metadata()?.len();
                        *size += len;
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
        let read_dir = try!(fs::read_dir(path));
        // Remove the duplicate snap keys.
        let mut v: Vec<_> = read_dir.filter_map(|p| {
                let p = match p {
                    Err(e) => {
                        error!("failed to list content of {}: {:?}", core.base, e);
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
                let numbers: Vec<u64> = name.split('.')
                    .next()
                    .map_or_else(|| vec![], |s| {
                        s.split('_')
                            .skip(1)
                            .filter_map(|s| s.parse().ok())
                            .collect()
                    });
                if numbers.len() != 3 {
                    error!("failed to parse snapkey from {}", name);
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

    pub fn get_snapshot_for_building(&self,
                                     key: &SnapKey,
                                     snap: &DbSnapshot)
                                     -> RaftStoreResult<Box<Snapshot>> {
        let (dir, snap_size) = {
            let core = self.core.rl();
            (core.base.clone(), core.snap_size.clone())
        };
        let f = try!(Snap::new_for_building(dir, key, snap, snap_size, Box::new(self.clone())));
        Ok(Box::new(f))
    }

    pub fn get_snapshot_for_sending(&self, key: &SnapKey) -> RaftStoreResult<Box<Snapshot>> {
        let core = self.core.rl();
        let s = try!(Snap::new_for_sending(&core.base,
                                           key,
                                           core.snap_size.clone(),
                                           Box::new(self.clone())));
        Ok(Box::new(s))
    }

    pub fn get_snapshot_for_receiving(&self,
                                      key: &SnapKey,
                                      data: &[u8])
                                      -> RaftStoreResult<Box<Snapshot>> {
        let core = self.core.rl();
        let mut snapshot_data = RaftSnapshotData::new();
        try!(snapshot_data.merge_from_bytes(data));
        let f = try!(Snap::new_for_receiving(&core.base,
                                             key,
                                             snapshot_data.take_meta(),
                                             core.snap_size.clone(),
                                             Box::new(self.clone())));
        Ok(Box::new(f))
    }

    pub fn get_snapshot_for_applying(&self, key: &SnapKey) -> RaftStoreResult<Box<Snapshot>> {
        let core = self.core.rl();
        let s = Snap::new_for_applying(&core.base,
                                       key,
                                       core.snap_size.clone(),
                                       Box::new(self.clone()))?;
        assert!(s.exists());
        Ok(Box::new(s))
    }

    /// Get the approximate size of snap file exists in snap directory.
    ///
    /// Return value is not guaranteed to be accurate.
    #[allow(let_and_return)]
    pub fn get_total_snap_size(&self) -> u64 {
        let core = self.core.rl();
        let size = *core.snap_size.rl();
        size
    }

    pub fn register(&self, key: SnapKey, entry: SnapEntry) {
        debug!("register [key: {}, entry: {:?}]", key, entry);
        let mut core = self.core.wl();
        match core.registry.entry(key) {
            Entry::Occupied(mut e) => {
                if e.get().contains(&entry) {
                    warn!("{} is registered more than 1 time!!!", e.key());
                    return;
                }
                e.get_mut().push(entry);
            }
            Entry::Vacant(e) => {
                e.insert(vec![entry]);
            }
        }

        notify_stats(self.ch.as_ref());
    }

    pub fn deregister(&self, key: &SnapKey, entry: &SnapEntry) {
        debug!("deregister [key: {}, entry: {:?}]", key, entry);
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
            notify_stats(self.ch.as_ref());
            return;
        }
        warn!("stale deregister key: {} {:?}", key, entry);
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
    fn delete_snapshot(&self, key: &SnapKey, snap: &Snapshot, check_entry: bool) -> bool {
        let core = self.core.rl();
        if check_entry {
            if let Some(e) = core.registry.get(key) {
                if e.len() > 1 {
                    info!("skip to delete {} since it's registered more than 1, registered \
                           entries {:?}",
                          snap.path(),
                          e);
                    return false;
                }
            }
        } else if core.registry.contains_key(key) {
            info!("skip to delete {} since it's registered", snap.path());
            return false;
        }
        snap.delete();
        true
    }
}

#[cfg(test)]
mod test {
    use std::io::{self, Read, Write, Seek, SeekFrom};
    use std::fs::{self, File, OpenOptions};
    use std::sync::*;
    use std::sync::atomic::AtomicUsize;
    use tempdir::TempDir;
    use protobuf::Message;

    use super::{SnapEntry, SnapKey, Snapshot, SnapshotDeleter, SnapManager, META_FILE_SUFFIX,
                Snap, SnapshotStatistics, ApplyOptions, SNAPSHOT_CFS, SNAP_GEN_PREFIX};

    use std::path::PathBuf;
    use kvproto::metapb::{Peer, Region};
    use kvproto::raft_serverpb::{SnapshotMeta, RaftSnapshotData};
    use rocksdb::DB;

    use storage::{ALL_CFS, CF_DEFAULT, CF_LOCK, CF_WRITE, CF_RAFT};
    use util::{rocksdb, HandyRwLock};
    use raftstore::Result;
    use raftstore::store::keys;
    use raftstore::store::engine::{Snapshot as DbSnapshot, Mutable, Peekable, Iterable};
    use raftstore::store::peer_storage::JOB_STATUS_RUNNING;

    const TEST_STORE_ID: u64 = 1;
    const TEST_KEY: &[u8] = b"akey";
    const TEST_WRITE_BATCH_SIZE: usize = 10 * 1024 * 1024;
    const TEST_META_FILE_BUFFER_SIZE: usize = 1000;
    const BYTE_SIZE: usize = 1;

    #[derive(Clone)]
    struct DummyDeleter;

    impl SnapshotDeleter for DummyDeleter {
        fn delete_snapshot(&self, _: &SnapKey, snap: &Snapshot, _: bool) -> bool {
            snap.delete();
            true
        }
    }

    pub fn get_test_empty_db(path: &TempDir) -> Result<Arc<DB>> {
        let p = path.path().to_str().unwrap();
        let db = try!(rocksdb::new_engine(p, ALL_CFS));
        Ok(Arc::new(db))
    }

    pub fn get_test_db(path: &TempDir) -> Result<Arc<DB>> {
        let p = path.path().to_str().unwrap();
        let db = rocksdb::new_engine(p, ALL_CFS)?;
        let key = keys::data_key(TEST_KEY);
        // write some data into each cf
        for (i, cf) in ALL_CFS.iter().enumerate() {
            let handle = try!(rocksdb::get_cf_handle(&db, cf));
            let mut p = Peer::new();
            p.set_store_id(TEST_STORE_ID);
            p.set_id((i + 1) as u64);
            db.put_msg_cf(handle, &key[..], &p)?;
        }
        Ok(Arc::new(db))
    }

    pub fn get_kv_count(snap: &DbSnapshot) -> usize {
        let mut kv_count = 0;
        for cf in SNAPSHOT_CFS {
            snap.scan_cf(cf,
                         &keys::data_key(b"a"),
                         &keys::data_key(b"z"),
                         false,
                         &mut |_, _| {
                             kv_count += 1;
                             Ok(true)
                         })
                .unwrap();
        }
        kv_count
    }

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
                    panic!("cf {}: key {:?}, value {:?}, expected {:?}",
                           cf,
                           key,
                           p2,
                           p1);
                }
            }
        }
    }

    #[test]
    fn test_gen_snapshot_meta() {
        let mut cf_file = Vec::with_capacity(super::SNAPSHOT_CFS.len());
        for (i, cf) in super::SNAPSHOT_CFS.iter().enumerate() {
            let f = super::CfFile {
                cf: cf,
                size: 100 * (i + 1) as u64,
                checksum: 1000 * (i + 1) as u32,
                ..Default::default()
            };
            cf_file.push(f);
        }
        let meta = super::gen_snapshot_meta(&cf_file).unwrap();
        for (i, cf_file_meta) in meta.get_cf_files().iter().enumerate() {
            if cf_file_meta.get_cf() != cf_file[i].cf {
                panic!("{}: expect cf {}, got {}",
                       i,
                       cf_file[i].cf,
                       cf_file_meta.get_cf());
            }
            if cf_file_meta.get_size() != cf_file[i].size {
                panic!("{}: expect cf size {}, got {}",
                       i,
                       cf_file[i].size,
                       cf_file_meta.get_size());
            }
            if cf_file_meta.get_checksum() != cf_file[i].checksum {
                panic!("{}: expect cf checksum {}, got {}",
                       i,
                       cf_file[i].checksum,
                       cf_file_meta.get_checksum());
            }
        }
    }

    #[test]
    fn test_display_path() {
        let dir = TempDir::new("test-display-path").unwrap();
        let key = SnapKey::new(1, 1, 1);
        let prefix = format!("{}_{}", SNAP_GEN_PREFIX, key);
        let display_path = Snap::get_display_path(&dir.into_path(), &prefix);
        assert_ne!(display_path, "");
    }

    #[test]
    fn test_empty_snap_file() {
        test_snap_file(get_test_empty_db);
    }

    #[test]
    fn test_non_empty_snap_file() {
        test_snap_file(get_test_db);
    }

    fn test_snap_file(get_db: fn(p: &TempDir) -> Result<Arc<DB>>) {
        let region_id = 1;
        let region = get_test_region(region_id, 1, 1);
        let src_db_dir = TempDir::new("test-snap-file-db-src").unwrap();
        let db = get_db(&src_db_dir).unwrap();
        let snapshot = DbSnapshot::new(db.clone());

        let src_dir = TempDir::new("test-snap-file-src").unwrap();
        let key = SnapKey::new(region_id, 1, 1);
        let size_track = Arc::new(RwLock::new(0));
        let deleter = Box::new(DummyDeleter {});
        let mut s1 = Snap::new_for_building(src_dir.path(),
                                            &key,
                                            &snapshot,
                                            size_track.clone(),
                                            deleter.clone())
            .unwrap();
        // Ensure that this snapshot file doesn't exist before being built.
        assert!(!s1.exists());
        assert_eq!(*size_track.rl(), 0);

        let mut snap_data = RaftSnapshotData::new();
        snap_data.set_region(region.clone());
        let mut stat = SnapshotStatistics::new();
        s1.build(&snapshot,
                   &region,
                   &mut snap_data,
                   &mut stat,
                   deleter.clone())
            .unwrap();

        // Ensure that this snapshot file does exist after being built.
        assert!(s1.exists());
        let total_size = s1.total_size().unwrap();
        // Ensure the `size_track` is modified correctly.
        let size = *size_track.rl();
        assert_eq!(size, total_size);
        assert_eq!(stat.size as u64, size);
        assert_eq!(stat.kv_count, get_kv_count(&snapshot));

        // Ensure this snapshot could be read for sending.
        let mut s2 =
            Snap::new_for_sending(src_dir.path(), &key, size_track.clone(), deleter.clone())
                .unwrap();
        assert!(s2.exists());

        // TODO check meta data correct.
        let _ = s2.meta().unwrap();

        let dst_dir = TempDir::new("test-snap-file-dst").unwrap();

        let mut s3 = Snap::new_for_receiving(dst_dir.path(),
                                             &key,
                                             snap_data.take_meta(),
                                             size_track.clone(),
                                             deleter.clone())
            .unwrap();
        assert!(!s3.exists());

        // Ensure snapshot data could be read out of `s2`, and write into `s3`.
        let copy_size = io::copy(&mut s2, &mut s3).unwrap();
        assert_eq!(copy_size, size);
        assert!(!s3.exists());
        s3.save().unwrap();
        assert!(s3.exists());

        // Ensure the tracked size is handled correctly after receiving a snapshot.
        assert_eq!(*size_track.rl(), size * 2);

        // Ensure `delete()` works to delete the source snapshot.
        s2.delete();
        assert!(!s2.exists());
        assert!(!s1.exists());
        assert_eq!(*size_track.rl(), size);

        // Ensure a snapshot could be applied to DB.
        let mut s4 = Snap::new_for_applying(dst_dir.path(), &key, size_track.clone(), deleter)
            .unwrap();
        assert!(s4.exists());

        let dst_db_dir = TempDir::new("test-snap-file-db-dst").unwrap();
        let dst_db_path = dst_db_dir.path().to_str().unwrap();
        // Change arbitrarily the cf order of ALL_CFS at destination db.
        let dst_cfs = [CF_WRITE, CF_DEFAULT, CF_LOCK, CF_RAFT];
        let dst_db = Arc::new(rocksdb::new_engine(dst_db_path, &dst_cfs).unwrap());
        let options = ApplyOptions {
            db: dst_db.clone(),
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
        assert_eq!(*size_track.rl(), 0);

        // Verify the data is correct after applying snapshot.
        assert_eq_db(db, dst_db.as_ref());
    }

    #[test]
    fn test_empty_snap_validation() {
        test_snap_validation(get_test_empty_db);
    }

    #[test]
    fn test_non_empty_snap_validation() {
        test_snap_validation(get_test_db);
    }

    fn test_snap_validation(get_db: fn(p: &TempDir) -> Result<Arc<DB>>) {
        let region_id = 1;
        let region = get_test_region(region_id, 1, 1);
        let db_dir = TempDir::new("test-snap-validation-db").unwrap();
        let db = get_db(&db_dir).unwrap();
        let snapshot = DbSnapshot::new(db.clone());

        let dir = TempDir::new("test-snap-validation").unwrap();
        let key = SnapKey::new(region_id, 1, 1);
        let size_track = Arc::new(RwLock::new(0));
        let deleter = Box::new(DummyDeleter {});
        let mut s1 = Snap::new_for_building(dir.path(),
                                            &key,
                                            &snapshot,
                                            size_track.clone(),
                                            deleter.clone())
            .unwrap();
        assert!(!s1.exists());

        let mut snap_data = RaftSnapshotData::new();
        snap_data.set_region(region.clone());
        let mut stat = SnapshotStatistics::new();
        s1.build(&snapshot,
                   &region,
                   &mut snap_data,
                   &mut stat,
                   deleter.clone())
            .unwrap();
        assert!(s1.exists());

        let mut s2 = Snap::new_for_building(dir.path(),
                                            &key,
                                            &snapshot,
                                            size_track.clone(),
                                            deleter.clone())
            .unwrap();
        assert!(s2.exists());

        s2.build(&snapshot, &region, &mut snap_data, &mut stat, deleter).unwrap();
        assert!(s2.exists());
    }

    // Make all the snapshot in the specified dir corrupted to have incorrect size.
    fn corrupt_snapshot_size_in<T: Into<PathBuf>>(dir: T) {
        let dir_path = dir.into();
        let read_dir = fs::read_dir(dir_path).unwrap();
        for p in read_dir {
            if p.is_ok() {
                let e = p.as_ref().unwrap();
                if !e.file_name().into_string().unwrap().ends_with(META_FILE_SUFFIX) {
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
                if e.file_name().into_string().unwrap().ends_with(META_FILE_SUFFIX) {
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
                if e.file_name().into_string().unwrap().ends_with(META_FILE_SUFFIX) {
                    let mut f = OpenOptions::new().read(true).write(true).open(e.path()).unwrap();
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

    fn copy_snapshot(from_dir: &TempDir,
                     to_dir: &TempDir,
                     key: &SnapKey,
                     size_track: Arc<RwLock<u64>>,
                     snapshot_meta: SnapshotMeta,
                     deleter: Box<DummyDeleter>) {
        let mut from =
            Snap::new_for_sending(from_dir.path(), key, size_track.clone(), deleter.clone())
                .unwrap();
        assert!(from.exists());

        let mut to = Snap::new_for_receiving(to_dir.path(),
                                             key,
                                             snapshot_meta,
                                             size_track.clone(),
                                             deleter)
            .unwrap();

        assert!(!to.exists());
        let _ = io::copy(&mut from, &mut to).unwrap();
        to.save().unwrap();
        assert!(to.exists());
    }

    #[test]
    fn test_snap_corruption_on_size_or_checksum() {
        let region_id = 1;
        let region = get_test_region(region_id, 1, 1);
        let db_dir = TempDir::new("test-snap-corruption-db").unwrap();
        let db = get_test_db(&db_dir).unwrap();
        let snapshot = DbSnapshot::new(db);

        let dir = TempDir::new("test-snap-corruption").unwrap();
        let key = SnapKey::new(region_id, 1, 1);
        let size_track = Arc::new(RwLock::new(0));
        let deleter = Box::new(DummyDeleter {});
        let mut s1 = Snap::new_for_building(dir.path(),
                                            &key,
                                            &snapshot,
                                            size_track.clone(),
                                            deleter.clone())
            .unwrap();
        assert!(!s1.exists());

        let mut snap_data = RaftSnapshotData::new();
        snap_data.set_region(region.clone());
        let mut stat = SnapshotStatistics::new();
        s1.build(&snapshot,
                   &region,
                   &mut snap_data,
                   &mut stat,
                   deleter.clone())
            .unwrap();
        assert!(s1.exists());

        corrupt_snapshot_size_in(dir.path());

        assert!(Snap::new_for_sending(dir.path(), &key, size_track.clone(), deleter.clone())
            .is_err());

        let mut s2 = Snap::new_for_building(dir.path(),
                                            &key,
                                            &snapshot,
                                            size_track.clone(),
                                            deleter.clone())
            .unwrap();
        assert!(!s2.exists());
        s2.build(&snapshot,
                   &region,
                   &mut snap_data,
                   &mut stat,
                   deleter.clone())
            .unwrap();
        assert!(s2.exists());

        let dst_dir = TempDir::new("test-snap-corruption-dst").unwrap();
        copy_snapshot(&dir,
                      &dst_dir,
                      &key,
                      size_track.clone(),
                      snap_data.get_meta().clone(),
                      deleter.clone());

        let mut metas = corrupt_snapshot_checksum_in(dst_dir.path());
        assert_eq!(1, metas.len());
        let snap_meta = metas.pop().unwrap();

        let mut s5 =
            Snap::new_for_applying(dst_dir.path(), &key, size_track.clone(), deleter.clone())
                .unwrap();
        assert!(s5.exists());

        let dst_db_dir = TempDir::new("test-snap-corruption-dst-db").unwrap();
        let dst_db = get_test_empty_db(&dst_db_dir).unwrap();
        let options = ApplyOptions {
            db: dst_db.clone(),
            region: region.clone(),
            abort: Arc::new(AtomicUsize::new(JOB_STATUS_RUNNING)),
            write_batch_size: TEST_WRITE_BATCH_SIZE,
        };
        assert!(s5.apply(options).is_err());

        corrupt_snapshot_size_in(dst_dir.path());
        assert!(Snap::new_for_receiving(dst_dir.path(),
                                        &key,
                                        snap_meta,
                                        size_track.clone(),
                                        deleter.clone())
            .is_err());
        assert!(Snap::new_for_applying(dst_dir.path(), &key, size_track.clone(), deleter.clone())
            .is_err());
    }

    #[test]
    fn test_snap_corruption_on_meta_file() {
        let region_id = 1;
        let region = get_test_region(region_id, 1, 1);
        let db_dir = TempDir::new("test-snapshot-corruption-meta-db").unwrap();
        let db = get_test_db(&db_dir).unwrap();
        let snapshot = DbSnapshot::new(db);

        let dir = TempDir::new("test-snap-corruption-meta").unwrap();
        let key = SnapKey::new(region_id, 1, 1);
        let size_track = Arc::new(RwLock::new(0));
        let deleter = Box::new(DummyDeleter {});
        let mut s1 = Snap::new_for_building(dir.path(),
                                            &key,
                                            &snapshot,
                                            size_track.clone(),
                                            deleter.clone())
            .unwrap();
        assert!(!s1.exists());

        let mut snap_data = RaftSnapshotData::new();
        snap_data.set_region(region.clone());
        let mut stat = SnapshotStatistics::new();
        s1.build(&snapshot,
                   &region,
                   &mut snap_data,
                   &mut stat,
                   deleter.clone())
            .unwrap();
        assert!(s1.exists());

        assert_eq!(1, corrupt_snapshot_meta_file(dir.path()));

        assert!(Snap::new_for_sending(dir.path(), &key, size_track.clone(), deleter.clone())
            .is_err());

        let mut s2 = Snap::new_for_building(dir.path(),
                                            &key,
                                            &snapshot,
                                            size_track.clone(),
                                            deleter.clone())
            .unwrap();
        assert!(!s2.exists());
        s2.build(&snapshot,
                   &region,
                   &mut snap_data,
                   &mut stat,
                   deleter.clone())
            .unwrap();
        assert!(s2.exists());

        let dst_dir = TempDir::new("test-snap-corruption-meta-dst").unwrap();
        copy_snapshot(&dir,
                      &dst_dir,
                      &key,
                      size_track.clone(),
                      snap_data.get_meta().clone(),
                      deleter.clone());

        assert_eq!(1, corrupt_snapshot_meta_file(dst_dir.path()));

        assert!(Snap::new_for_applying(dst_dir.path(), &key, size_track.clone(), deleter.clone())
            .is_err());
        assert!(Snap::new_for_receiving(dst_dir.path(),
                                        &key,
                                        snap_data.take_meta(),
                                        size_track.clone(),
                                        deleter.clone())
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
        let snapshot = DbSnapshot::new(super::test::get_test_db(&db_dir).unwrap());
        let key1 = SnapKey::new(1, 1, 1);
        let size_track = Arc::new(RwLock::new(0));
        let deleter = Box::new(mgr.clone());
        let mut s1 =
            Snap::new_for_building(&path, &key1, &snapshot, size_track.clone(), deleter.clone())
                .unwrap();
        let mut region = super::test::get_test_region(1, 1, 1);
        let mut snap_data = RaftSnapshotData::new();
        snap_data.set_region(region.clone());
        let mut stat = SnapshotStatistics::new();
        s1.build(&snapshot,
                   &region,
                   &mut snap_data,
                   &mut stat,
                   deleter.clone())
            .unwrap();
        let mut s = Snap::new_for_sending(&path, &key1, size_track.clone(), deleter.clone())
            .unwrap();
        let expected_size = s.total_size().unwrap();
        let mut s2 = Snap::new_for_receiving(&path,
                                             &key1,
                                             snap_data.get_meta().clone(),
                                             size_track.clone(),
                                             deleter.clone())
            .unwrap();
        let n = io::copy(&mut s, &mut s2).unwrap();
        assert_eq!(n, expected_size);
        s2.save().unwrap();

        let key2 = SnapKey::new(2, 1, 1);
        region.set_id(2);
        snap_data.set_region(region);
        let s3 =
            Snap::new_for_building(&path, &key2, &snapshot, size_track.clone(), deleter.clone())
                .unwrap();
        let s4 = Snap::new_for_receiving(&path,
                                         &key2,
                                         snap_data.take_meta(),
                                         size_track.clone(),
                                         deleter.clone())
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

    #[test]
    fn test_snap_deletion_on_registry() {
        test_snap_deletion_on_registry_impl();
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

    fn test_snap_deletion_on_registry_impl() {
        let src_temp_dir = TempDir::new("test-snap-deletion-on-registry-src").unwrap();
        let src_path = src_temp_dir.path().to_str().unwrap().to_owned();
        let src_mgr = SnapManager::new(src_path.clone(), None);
        src_mgr.init().unwrap();

        let src_db_dir = TempDir::new("test-snap-deletion-on-registry-src-db").unwrap();
        let db = super::test::get_test_db(&src_db_dir).unwrap();
        let snapshot = DbSnapshot::new(db);

        let key = SnapKey::new(1, 1, 1);
        let region = super::test::get_test_region(1, 1, 1);

        // Ensure the snapshot being built will not be deleted on GC.
        src_mgr.register(key.clone(), SnapEntry::Generating);
        let mut s1 = src_mgr.get_snapshot_for_building(&key, &snapshot).unwrap();
        let mut snap_data = RaftSnapshotData::new();
        snap_data.set_region(region.clone());
        let mut stat = SnapshotStatistics::new();
        s1.build(&snapshot,
                   &region,
                   &mut snap_data,
                   &mut stat,
                   Box::new(src_mgr.clone()))
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
}
