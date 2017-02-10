use std::io::{self, Write, ErrorKind, Read};
use std::fmt::{self, Formatter, Display};
use std::cmp;
use std::fs::{self, File, OpenOptions, Metadata};
use std::collections::HashMap;
use std::collections::hash_map::Entry;
use std::sync::{Arc, RwLock};
use std::path::{Path, PathBuf};

use crc::crc32::{self, Digest, Hasher32};
use byteorder::{BigEndian, WriteBytesExt, ReadBytesExt};
use protobuf::Message;

use kvproto::eraftpb::Snapshot;
use kvproto::raft_serverpb::RaftSnapshotData;
use raftstore::store::Msg;
use raftstore::Result;
use util::transport::SendCh;
use util::HandyRwLock;
use storage::{ALL_CFS, CF_RAFT};
use rocksdb::{EnvOptions, Options, SstFileWriter};
use super::peer_storage::decode_cf_file_sizes;

/// Name prefix for the self-generated snapshot file.
const SNAP_GEN_PREFIX: &'static str = "gen";
/// Name prefix for the received snapshot file.
const SNAP_REV_PREFIX: &'static str = "rev";

const TMP_FILE_SUFFIX: &'static str = ".tmp";
const SST_FILE_SUFFIX: &'static str = ".sst";

const CRC32_BYTES_COUNT: usize = 4;
const DEFAULT_READ_BUFFER_SIZE: usize = 4096;

fn get_snapshot_cfs() -> Vec<String> {
    let size = ALL_CFS.len();
    let mut cfs = Vec::with_capacity(size);
    for cf in ALL_CFS {
        if *cf == CF_RAFT {
            continue;
        }
        cfs.push(String::from(*cf));
    }
    cfs
}

fn file_exists(file: &str) -> bool {
    let path = Path::new(file);
    path.exists() && path.is_file()
}

fn delete_file(file: &str) -> bool {
    if let Err(e) = fs::remove_file(file) {
        warn!("failed to delete file {}: {:?}", file, e);
        return false;
    }
    true
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

    #[inline]
    pub fn from_region_snap(region_id: u64, snap: &Snapshot) -> SnapKey {
        let index = snap.get_metadata().get_index();
        let term = snap.get_metadata().get_term();
        SnapKey::new(region_id, term, index)
    }

    pub fn from_snap(snap: &Snapshot) -> io::Result<SnapKey> {
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

pub enum SnapshotFileWriter {
    V1(SnapshotFileV1),
    V2(RecvSnapshotFile),
}

impl SnapshotFileWriter {
    pub fn init(&mut self) -> io::Result<()> {
        match *self {
            SnapshotFileWriter::V1(ref mut f) => f.init(),
            SnapshotFileWriter::V2(ref mut f) => f.init(),
        }
    }

    pub fn path(&self) -> String {
        match *self {
            SnapshotFileWriter::V1(ref f) => f.path(),
            SnapshotFileWriter::V2(ref f) => f.path(),
        }
    }

    pub fn exists(&self) -> bool {
        match *self {
            SnapshotFileWriter::V1(ref f) => f.exists(),
            SnapshotFileWriter::V2(ref f) => f.exists(),
        }
    }

    pub fn save(&mut self) -> io::Result<()> {
        match *self {
            SnapshotFileWriter::V1(ref mut f) => f.save(),
            SnapshotFileWriter::V2(ref mut f) => f.save(),
        }
    }
}

impl Write for SnapshotFileWriter {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        match *self {
            SnapshotFileWriter::V1(ref mut f) => f.write(buf),
            SnapshotFileWriter::V2(ref mut f) => f.write(buf),
        }
    }

    fn flush(&mut self) -> io::Result<()> {
        match *self {
            SnapshotFileWriter::V1(ref mut f) => f.flush(),
            SnapshotFileWriter::V2(ref mut f) => f.flush(),
        }
    }
}

pub enum SnapshotFileSendReader {
    V1((SnapshotFileV1, SnapValidationReader)),
    V2(SendSnapshotFileReader),
}

impl SnapshotFileSendReader {
    pub fn exists(&self) -> bool {
        match *self {
            SnapshotFileSendReader::V1((ref f, _)) => f.exists(),
            SnapshotFileSendReader::V2(ref f) => f.exists(),
        }
    }

    pub fn path(&self) -> String {
        match *self {
            SnapshotFileSendReader::V1((ref f, _)) => f.path(),
            SnapshotFileSendReader::V2(ref f) => f.path(),
        }
    }

    pub fn meta(&self) -> io::Result<Metadata> {
        match *self {
            SnapshotFileSendReader::V1((ref f, _)) => f.meta(),
            SnapshotFileSendReader::V2(ref f) => f.meta(),
        }
    }

    pub fn delete(&self) {
        match *self {
            SnapshotFileSendReader::V1((ref f, _)) => f.delete(),
            SnapshotFileSendReader::V2(ref f) => f.delete(),
        }
    }

    pub fn total_size(&self) -> io::Result<u64> {
        match *self {
            SnapshotFileSendReader::V1((ref f, _)) => {
                let meta = try!(f.meta());
                Ok(meta.len())
            }
            SnapshotFileSendReader::V2(ref f) => f.total_size(),
        }
    }
}

impl Read for SnapshotFileSendReader {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        match *self {
            SnapshotFileSendReader::V1((_, ref mut f)) => f.read(buf),
            SnapshotFileSendReader::V2(ref mut f) => f.read(buf),
        }
    }
}

pub enum SnapshotFileRecvReader {
    V1(SnapshotFileV1),
    V2(RecvSnapshotFileReader),
}

impl SnapshotFileRecvReader {
    pub fn delete(&self) {
        match *self {
            SnapshotFileRecvReader::V1(ref f) => f.delete(),
            SnapshotFileRecvReader::V2(ref f) => f.delete(),
        }
    }
}

/// A structure represents the snapshot file.
///
/// All changes to the file will be written to `tmp_file` first, and use
/// `save` method to make them persistent. When saving a crc32 checksum
/// will be appended to the file end automatically.
pub struct SnapshotFileV1 {
    file: PathBuf,
    digest: Digest,
    size_track: Arc<RwLock<u64>>,
    // File is the file obj represent the tmpfile, string is the actual path to
    // tmpfile.
    tmp_file: Option<(File, String)>,
}

impl SnapshotFileV1 {
    fn new<T: Into<PathBuf>>(snap_dir: T,
                             size_track: Arc<RwLock<u64>>,
                             is_sending: bool,
                             key: &SnapKey)
                             -> io::Result<SnapshotFileV1> {
        let mut file_path = snap_dir.into();
        if !file_path.exists() {
            try!(fs::create_dir_all(file_path.as_path()));
        }
        let prefix = if is_sending {
            SNAP_GEN_PREFIX
        } else {
            SNAP_REV_PREFIX
        };
        let file_name = format!("{}_{}.snap", prefix, key);
        file_path.push(&file_name);

        let f = SnapshotFileV1 {
            file: file_path,
            digest: Digest::new(crc32::IEEE),
            size_track: size_track,
            tmp_file: None,
        };
        Ok(f)
    }

    pub fn init(&mut self) -> io::Result<()> {
        if self.exists() || self.tmp_file.is_some() {
            return Ok(());
        }

        let tmp_path = format!("{}{}", self.path(), TMP_FILE_SUFFIX);
        let tmp_f = try!(OpenOptions::new().write(true).create_new(true).open(&tmp_path));
        self.tmp_file = Some((tmp_f, tmp_path));
        Ok(())
    }

    pub fn meta(&self) -> io::Result<Metadata> {
        self.file.metadata()
    }

    /// Get a validation reader.
    pub fn reader(&self) -> io::Result<SnapValidationReader> {
        SnapValidationReader::open(self.path())
    }

    pub fn exists(&self) -> bool {
        self.file.exists() && self.file.is_file()
    }

    pub fn delete(&self) {
        if let Err(e) = self.try_delete() {
            error!("failed to delete {}: {:?}", self.path(), e);
        }
    }

    pub fn try_delete(&self) -> io::Result<()> {
        debug!("deleting {}", self.path());
        if !self.exists() {
            return Ok(());
        }
        let size = try!(self.meta()).len();
        try!(fs::remove_file(self.path()));
        let mut size_track = self.size_track.wl();
        *size_track = size_track.saturating_sub(size);
        Ok(())
    }

    /// Use the content in temporary files replace the target file.
    ///
    /// Please note that this method can only be called once.
    fn save(&mut self) -> io::Result<()> {
        self.save_impl(false)
    }

    /// Same as `save`, but will automatically append the checksum to
    /// the end of file.
    pub fn save_with_checksum(&mut self) -> io::Result<()> {
        self.save_impl(true)
    }

    fn save_impl(&mut self, append_checksum: bool) -> io::Result<()> {
        debug!("saving to {}", self.file.as_path().display());
        if let Some((mut f, path)) = self.tmp_file.take() {
            if append_checksum {
                try!(f.write_u32::<BigEndian>(self.digest.sum32()));
            }
            try!(f.flush());
            let file_len = try!(fs::metadata(&path)).len();
            let mut size_track = self.size_track.wl();
            try!(fs::rename(path, self.file.as_path()));
            *size_track = size_track.saturating_add(file_len);
        }
        Ok(())
    }

    pub fn path(&self) -> String {
        self.file.as_path().to_str().unwrap().to_string()
    }
}

impl Write for SnapshotFileV1 {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        if self.tmp_file.is_none() {
            return Ok(0);
        }
        let written = try!(self.tmp_file.as_mut().unwrap().0.write(buf));
        self.digest.write(&buf[..written]);
        Ok(written)
    }

    fn flush(&mut self) -> io::Result<()> {
        if self.tmp_file.is_none() {
            return Ok(());
        }
        self.tmp_file.as_mut().unwrap().0.flush()
    }
}

impl Drop for SnapshotFileV1 {
    fn drop(&mut self) {
        if let Some((_, path)) = self.tmp_file.take() {
            debug!("deleting {}", path);
            if let Err(e) = fs::remove_file(&path) {
                warn!("failed to delete temporary file {}: {:?}", path, e);
            }
        }
    }
}

/// A reader that calculate checksum and verify it without read
/// it from the beginning again.
pub struct SnapValidationReader {
    reader: File,
    digest: Digest,
    left: usize,
    res: Option<u32>,
}

impl SnapValidationReader {
    /// Open the snap file located at specified path.
    fn open<P: AsRef<Path>>(path: P) -> io::Result<SnapValidationReader> {
        let reader = try!(File::open(path.as_ref()));
        let digest = Digest::new(crc32::IEEE);
        let len = try!(reader.metadata()).len();
        if len < CRC32_BYTES_COUNT as u64 {
            return Err(io::Error::new(ErrorKind::InvalidInput,
                                      format!("file length {} < {}", len, CRC32_BYTES_COUNT)));
        }
        let left = len as usize - CRC32_BYTES_COUNT;
        Ok(SnapValidationReader {
            reader: reader,
            digest: digest,
            left: left,
            res: None,
        })
    }

    /// Validate the file
    ///
    /// If the reader will be consumed after calling this method, no further data can be
    /// read from this reader again.
    pub fn validate(&mut self) -> io::Result<()> {
        if self.res.is_none() {
            if self.left > 0 {
                let cap = cmp::min(self.left, DEFAULT_READ_BUFFER_SIZE);
                let mut buf = vec![0; cap];
                while self.left > 0 {
                    try!(self.read(&mut buf));
                }
            }
            self.res = Some(try!(self.reader.read_u32::<BigEndian>()));
        }
        if self.res.unwrap() != self.digest.sum32() {
            let msg = format!("crc not correct: {} != {}",
                              self.res.unwrap(),
                              self.digest.sum32());
            return Err(io::Error::new(ErrorKind::InvalidData, msg));
        }
        Ok(())
    }
}

impl Read for SnapValidationReader {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        if self.left == 0 {
            return Ok(0);
        }
        let read = if buf.len() < self.left {
            try!(self.reader.read(buf))
        } else {
            try!(self.reader.read(&mut buf[..self.left]))
        };
        self.digest.write(&buf[..read]);
        self.left -= read;
        Ok(read)
    }
}

// new format

struct SendCfFile {
    pub cf: String,
    pub writer: Option<SstFileWriter>,
    pub kv_count: u64,
    pub path: String,
    pub tmp_path: String,
}

pub struct SendSnapshotFile {
    index: usize,
    cf_files: Vec<SendCfFile>,
    size_track: Arc<RwLock<u64>>,
}

impl SendSnapshotFile {
    fn new<T: Into<PathBuf>>(snap_dir: T,
                             key: &SnapKey,
                             size_track: Arc<RwLock<u64>>)
                             -> io::Result<SendSnapshotFile> {
        let dir_path = snap_dir.into();
        if !dir_path.exists() {
            try!(fs::create_dir_all(dir_path.as_path()));
        }

        let prefix = format!("{}_{}", SNAP_GEN_PREFIX, key);
        let snapshot_cfs = get_snapshot_cfs();
        let mut cf_files = Vec::with_capacity(snapshot_cfs.len());
        for cf in snapshot_cfs {
            let filename = format!("{}_{}{}", prefix, cf, SST_FILE_SUFFIX);
            let path = dir_path.join(filename).as_path().to_str().unwrap().to_string();
            let tmp_filename = format!("{}_{}{}", prefix, cf, TMP_FILE_SUFFIX);
            let tmp_path = dir_path.join(tmp_filename).as_path().to_str().unwrap().to_string();
            let cf_file = SendCfFile {
                cf: cf,
                writer: None,
                kv_count: 0,
                tmp_path: tmp_path,
                path: path,
            };
            cf_files.push(cf_file);
        }

        let f = SendSnapshotFile {
            index: 0,
            cf_files: cf_files,
            size_track: size_track,
        };
        Ok(f)
    }

    pub fn init(&mut self) -> io::Result<()> {
        for f in &mut self.cf_files {
            let env_opt = EnvOptions::new();
            let io_options = Options::new();
            let mut writer = SstFileWriter::new(&env_opt, &io_options);
            if let Err(e) = writer.open(&f.tmp_path) {
                return Err(io::Error::new(ErrorKind::Other, e));
            }
            f.writer = Some(writer);
        }

        Ok(())
    }

    pub fn next_file(&mut self, cf: String) -> bool {
        let mut cf_found = false;
        let mut next_index = 0;
        for f in &mut self.cf_files {
            if f.cf == cf {
                cf_found = true;
                break;
            } else {
                next_index += 1;
            }
        }
        if !cf_found {
            return false;
        }
        self.index = next_index;
        true
    }

    pub fn add_kv(&mut self, k: &[u8], v: &[u8]) -> io::Result<()> {
        let mut cf_file = &mut self.cf_files[self.index];
        let mut writer = cf_file.writer.as_mut().unwrap();
        if let Err(e) = writer.add(k, v) {
            return Err(io::Error::new(ErrorKind::Other, e));
        }
        cf_file.kv_count += 1;
        Ok(())
    }

    pub fn save_all(&mut self) -> io::Result<()> {
        for f in &mut self.cf_files {
            if f.kv_count == 0 {
                try!(fs::rename(&f.tmp_path, &f.path));
            } else {
                let writer = f.writer.as_mut().unwrap();
                if let Err(e) = writer.finish() {
                    return Err(io::Error::new(ErrorKind::Other, e));
                }
                let mut size_track = self.size_track.wl();
                try!(fs::rename(&f.tmp_path, &f.path));
                let size = try!(fs::metadata(&f.path)).len();
                *size_track = size_track.saturating_add(size);
            }
        }
        Ok(())
    }

    pub fn list_cf_sizes(&self) -> io::Result<Vec<(String, u64)>> {
        let mut res = Vec::with_capacity(self.cf_files.len());
        for f in &self.cf_files {
            let size = try!(fs::metadata(&f.path)).len();
            res.push((f.cf.clone(), size));
        }
        Ok(res)
    }

    pub fn exists(&self) -> bool {
        for f in &self.cf_files {
            if !file_exists(&f.path) {
                return false;
            }
        }
        true
    }
}

impl Drop for SendSnapshotFile {
    fn drop(&mut self) {
        let mut to_cleanup = false;
        for f in &self.cf_files {
            if file_exists(&f.tmp_path) {
                to_cleanup = true;
                break;
            }
        }
        if to_cleanup {
            for f in &self.cf_files {
                delete_file(&f.tmp_path);
                delete_file(&f.path);
            }
        }
    }
}

struct SendCfFileReader {
    pub cf: String,
    pub file: File,
    pub path: String,
}

pub struct SendSnapshotFileReader {
    dir: PathBuf,
    prefix: String,
    index: usize,
    cf_readers: Vec<SendCfFileReader>,
    size_track: Arc<RwLock<u64>>,
}

impl SendSnapshotFileReader {
    fn new<T: Into<PathBuf>>(snap_dir: T,
                             key: &SnapKey,
                             size_track: Arc<RwLock<u64>>)
                             -> io::Result<SendSnapshotFileReader> {
        let dir_path = snap_dir.into();

        let prefix = format!("{}_{}", SNAP_GEN_PREFIX, key);
        let snapshot_cfs = get_snapshot_cfs();
        let mut cf_readers = Vec::with_capacity(snapshot_cfs.len());
        for cf in snapshot_cfs {
            let filename = format!("{}_{}{}", prefix, cf, SST_FILE_SUFFIX);
            let path = dir_path.join(filename).as_path().to_str().unwrap().to_string();
            if !file_exists(&path) {
                return Err(io::Error::new(ErrorKind::NotFound,
                                          format!("send snapshot file for snap key {} not found",
                                                  key)));
            }
            let f = try!(OpenOptions::new().read(true).open(&path));
            let r = SendCfFileReader {
                cf: cf,
                file: f,
                path: path,
            };
            cf_readers.push(r);
        }
        Ok(SendSnapshotFileReader {
            dir: dir_path,
            prefix: prefix,
            index: 0,
            cf_readers: cf_readers,
            size_track: size_track,
        })
    }

    pub fn exists(&self) -> bool {
        for f in &self.cf_readers {
            if !file_exists(&f.path) {
                return false;
            }
        }
        true
    }

    pub fn path(&self) -> String {
        let mut cf_names = String::from("");
        for (i, f) in self.cf_readers.iter().enumerate() {
            if i == 0 {
                cf_names += "(";
                cf_names += &f.cf;
            } else {
                cf_names += "|";
                cf_names += &f.cf;
            }
        }
        cf_names += ")";
        format!("{}/{}_{}{}",
                self.dir.as_path().to_str().unwrap().to_string(),
                self.prefix,
                cf_names,
                SST_FILE_SUFFIX)
    }

    pub fn delete(&self) {
        if let Err(e) = self.try_delete() {
            error!("failed to delete snapshot file {}: {:?}", self.path(), e);
        }
    }

    pub fn try_delete(&self) -> io::Result<()> {
        debug!("deleting {}", self.path());
        for f in &self.cf_readers {
            let size = try!(fs::metadata(&f.path)).len();
            let mut size_track = self.size_track.wl();
            try!(fs::remove_file(&f.path));
            *size_track = size_track.saturating_sub(size);
        }
        Ok(())
    }

    pub fn meta(&self) -> io::Result<Metadata> {
        let f = try!(OpenOptions::new().open(&self.cf_readers[self.index].path));
        f.metadata()
    }

    pub fn total_size(&self) -> io::Result<u64> {
        let mut total_size = 0;
        for f in &self.cf_readers {
            total_size += try!(fs::metadata(&f.path)).len();
        }
        Ok(total_size)
    }
}

impl Read for SendSnapshotFileReader {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        if buf.len() == 0 {
            return Ok(0);
        }
        while self.index < self.cf_readers.len() {
            match self.cf_readers[self.index].file.read(buf) {
                Ok(n) => {
                    if n == 0 {
                        // EOF. switch to next file
                        self.index += 1;
                        continue;
                    }
                    return Ok(n);
                }
                e => return e,
            }
        }
        Ok(0)
    }
}

struct RecvCfFile {
    pub cf: String,
    pub file: Option<File>,
    pub path: String,
    pub tmp_path: String,
    pub size: u64,
    pub written_size: u64,
}

pub struct RecvSnapshotFile {
    dir: PathBuf,
    prefix: String,
    index: usize,
    cf_files: Vec<RecvCfFile>,
    size_track: Arc<RwLock<u64>>,
}

impl RecvSnapshotFile {
    fn new<T: Into<PathBuf>>(snap_dir: T,
                             key: &SnapKey,
                             cf_sizes: Vec<(String, u64)>,
                             size_track: Arc<RwLock<u64>>)
                             -> io::Result<RecvSnapshotFile> {
        let dir_path = snap_dir.into();
        if !dir_path.exists() {
            try!(fs::create_dir_all(dir_path.as_path()));
        }

        let prefix = format!("{}_{}", SNAP_REV_PREFIX, key);

        let mut cf_files = Vec::with_capacity(cf_sizes.len());
        for (cf, size) in cf_sizes {
            let tmp_filename = format!("{}_{}{}", prefix, cf, TMP_FILE_SUFFIX);
            let tmp_path = dir_path.join(tmp_filename).as_path().to_str().unwrap().to_string();
            let filename = format!("{}_{}{}", prefix, cf, SST_FILE_SUFFIX);
            let path = dir_path.join(filename).as_path().to_str().unwrap().to_string();
            let cf_file = RecvCfFile {
                cf: cf,
                file: None,
                tmp_path: tmp_path,
                path: path,
                size: size,
                written_size: 0,
            };
            cf_files.push(cf_file);
        }
        Ok(RecvSnapshotFile {
            dir: dir_path,
            prefix: prefix,
            index: 0,
            cf_files: cf_files,
            size_track: size_track,
        })
    }

    pub fn init(&mut self) -> io::Result<()> {
        for f in &mut self.cf_files {
            let tmp_file = try!(OpenOptions::new().write(true).create_new(true).open(&f.tmp_path));
            f.file = Some(tmp_file);
        }
        Ok(())
    }

    pub fn exists(&self) -> bool {
        for f in &self.cf_files {
            if !file_exists(&f.path) {
                return false;
            }
        }
        true
    }

    pub fn path(&self) -> String {
        let mut cf_names = String::from("");
        for (i, f) in self.cf_files.iter().enumerate() {
            if i == 0 {
                cf_names += "(";
                cf_names += &f.cf;
            } else {
                cf_names += "|";
                cf_names += &f.cf;
            }
        }
        cf_names += ")";
        format!("{}/{}_{}{}",
                self.dir.as_path().to_str().unwrap().to_string(),
                self.prefix,
                cf_names,
                SST_FILE_SUFFIX)
    }

    fn save(&mut self) -> io::Result<()> {
        debug!("saving to {}", self.path());
        // check that all cf files have been fully written
        for cf_file in &mut self.cf_files {
            let mut file = cf_file.file.as_mut().unwrap();
            try!(file.flush());
            let tmp_path_size = try!(fs::metadata(&cf_file.tmp_path)).len();
            if tmp_path_size < cf_file.size {
                return Err(io::Error::new(ErrorKind::Other,
                                          format!("snapshot file for cf {} not fully written, \
                                                   diff {}",
                                                  cf_file.cf,
                                                  cf_file.size - tmp_path_size)));
            }
        }
        for cf_file in &mut self.cf_files {
            let mut size_track = self.size_track.wl();
            try!(fs::rename(&cf_file.tmp_path, &cf_file.path));
            *size_track = size_track.saturating_add(cf_file.size as u64);
        }
        Ok(())
    }

    pub fn delete(&mut self) {
        if let Err(e) = self.try_delete() {
            error!("failed to delete snapshot file {}: {:?}", self.path(), e);
        }
    }

    pub fn try_delete(&self) -> io::Result<()> {
        debug!("deleting {}", self.path());
        for f in &self.cf_files {
            delete_file(&f.tmp_path);
            delete_file(&f.path);
        }
        Ok(())
    }
}

impl Write for RecvSnapshotFile {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        if buf.len() == 0 {
            return Ok(0);
        }
        let mut next_buf = buf;
        while self.index < self.cf_files.len() {
            let cf_file = &mut self.cf_files[self.index];
            let left = (cf_file.size - cf_file.written_size) as usize;
            if left == 0 {
                self.index += 1;
                continue;
            }
            let mut file = cf_file.file.as_mut().unwrap();
            if next_buf.len() > left {
                try!(file.write_all(&next_buf[0..left]));
                cf_file.written_size += left as u64;
                self.index += 1;
                next_buf = &next_buf[left..];
            } else {
                try!(file.write_all(next_buf));
                cf_file.written_size = next_buf.len() as u64;
                return Ok(buf.len());
            }
        }
        let n = buf.len() - next_buf.len();
        Ok(n)
    }

    fn flush(&mut self) -> io::Result<()> {
        if let Some(cf_file) = self.cf_files.get_mut(self.index) {
            let file = cf_file.file.as_mut().unwrap();
            try!(file.flush());
        }
        Ok(())
    }
}

impl Drop for RecvSnapshotFile {
    fn drop(&mut self) {
        let mut to_cleanup = false;
        for f in &self.cf_files {
            if file_exists(&f.tmp_path) {
                to_cleanup = true;
                break;
            }
        }
        if to_cleanup {
            for f in &self.cf_files {
                delete_file(&f.tmp_path);
                delete_file(&f.path);
            }
        }
    }
}

struct RecvCfFileReader {
    pub cf: String,
    pub path: String,
    pub size: u64,
}

pub struct RecvSnapshotFileReader {
    cf_files: Vec<RecvCfFileReader>,
    size_track: Arc<RwLock<u64>>,
}

impl RecvSnapshotFileReader {
    fn new<T: Into<PathBuf>>(snap_dir: T,
                             key: &SnapKey,
                             size_track: Arc<RwLock<u64>>)
                             -> io::Result<RecvSnapshotFileReader> {
        let dir_path = snap_dir.into();

        let prefix = format!("{}_{}", SNAP_REV_PREFIX, key);
        let snapshot_cfs = get_snapshot_cfs();
        let mut cf_files = Vec::with_capacity(snapshot_cfs.len());
        for cf in snapshot_cfs {
            let filename = format!("{}_{}{}", prefix, cf, SST_FILE_SUFFIX);
            let path = dir_path.join(filename).as_path().to_str().unwrap().to_string();
            if !file_exists(&path) {
                error!("recv snapshot path {} not exists", path);
                return Err(io::Error::new(ErrorKind::NotFound,
                                          format!("recv snapshot file for snap key {} not found",
                                                  key)));
            }
            let size = try!(fs::metadata(&path)).len();
            let f = RecvCfFileReader {
                cf: cf,
                path: path.to_string(),
                size: size,
            };
            cf_files.push(f);
        }
        Ok(RecvSnapshotFileReader {
            cf_files: cf_files,
            size_track: size_track,
        })
    }

    pub fn list_cf_files(&self) -> Vec<(String, String, u64)> {
        let mut res = Vec::with_capacity(self.cf_files.len());
        for cf_file in &self.cf_files {
            if cf_file.size == 0 {
                continue;
            }
            res.push((cf_file.cf.clone(), cf_file.path.clone(), cf_file.size));
        }
        res
    }

    pub fn delete(&self) {
        let mut size_track = self.size_track.wl();
        for cf_file in &self.cf_files {
            delete_file(&cf_file.path);
            *size_track = size_track.saturating_sub(cf_file.size);
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

/// `SnapManagerCore` trace all current processing snapshots.
pub struct SnapManagerCore {
    // directory to store snapfile.
    base: String,
    registry: HashMap<SnapKey, Vec<SnapEntry>>,
    ch: Option<SendCh<Msg>>,
    snap_size: Arc<RwLock<u64>>,
}

impl SnapManagerCore {
    pub fn new<T: Into<String>>(path: T, ch: Option<SendCh<Msg>>) -> SnapManagerCore {
        SnapManagerCore {
            base: path.into(),
            registry: map![],
            ch: ch,
            snap_size: Arc::new(RwLock::new(0)),
        }
    }

    pub fn init(&self) -> io::Result<()> {
        let path = Path::new(&self.base);
        if !path.exists() {
            try!(fs::create_dir_all(path));
            return Ok(());
        }
        if !path.is_dir() {
            return Err(io::Error::new(ErrorKind::Other,
                                      format!("{} should be a directory", path.display())));
        }
        let mut size = self.snap_size.wl();
        for f in try!(fs::read_dir(path)) {
            let p = try!(f);
            if try!(p.file_type()).is_file() {
                if let Some(s) = p.file_name().to_str() {
                    if s.ends_with(TMP_FILE_SUFFIX) {
                        try!(fs::remove_file(p.path()));
                    } else {
                        *size += try!(p.metadata()).len();
                    }
                }
            }
        }
        Ok(())
    }

    pub fn list_snap(&self) -> io::Result<Vec<(SnapKey, bool, bool)>> {
        let path = Path::new(&self.base);
        let read_dir = try!(fs::read_dir(path));
        let mut all_keys: Vec<(SnapKey, bool, bool)> = read_dir.filter_map(|p| {
                let p = match p {
                    Err(e) => {
                        error!("failed to list content of {}: {:?}", self.base, e);
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
                let name_parts: Vec<&str> = name.split('.')
                    .next()
                    .map_or_else(|| vec![], |s| s.split('_').skip(1).collect());
                let is_v1 = name_parts.len() == 3;
                let numbers: Vec<u64> = name_parts.iter().filter_map(|s| s.parse().ok()).collect();
                if numbers.len() != 3 {
                    error!("failed to parse snapkey from {}", name);
                    return None;
                }
                Some((SnapKey::new(numbers[0], numbers[1], numbers[2]), is_sending, is_v1))
            })
            .collect();
        all_keys.sort();
        all_keys.dedup();
        Ok(all_keys)
    }

    #[inline]
    pub fn has_registered(&self, key: &SnapKey) -> bool {
        self.registry.contains_key(key)
    }

    #[inline]
    pub fn get_send_snapshot_file(&self, key: &SnapKey) -> io::Result<SendSnapshotFile> {
        SendSnapshotFile::new(&self.base, key, self.snap_size.clone())
    }

    #[inline]
    pub fn get_snapshot_file_writer(&self,
                                    key: &SnapKey,
                                    data: &[u8])
                                    -> Result<SnapshotFileWriter> {
        if data.len() == 0 {
            let f = try!(SnapshotFileV1::new(&self.base, self.snap_size.clone(), false, key));
            Ok(SnapshotFileWriter::V1(f))
        } else {
            let cf_file_sizes = try!(decode_cf_file_sizes(data));
            let f =
                try!(RecvSnapshotFile::new(&self.base, key, cf_file_sizes, self.snap_size.clone()));
            Ok(SnapshotFileWriter::V2(f))
        }
    }

    #[inline]
    pub fn get_snapshot_file_trans_reader(&self, key: &SnapKey) -> Result<SnapshotFileSendReader> {
        let v1 = try!(SnapshotFileV1::new(&self.base, self.snap_size.clone(), false, key));
        if v1.exists() {
            let reader = try!(v1.reader());
            return Ok(SnapshotFileSendReader::V1((v1, reader)));
        }
        let v2 = try!(SendSnapshotFileReader::new(&self.base, key, self.snap_size.clone()));
        Ok(SnapshotFileSendReader::V2(v2))
    }

    #[inline]
    pub fn get_snapshot_file_apply_reader(&self, key: &SnapKey) -> Result<SnapshotFileRecvReader> {
        let v1 = try!(SnapshotFileV1::new(&self.base, self.snap_size.clone(), false, key));
        if v1.exists() {
            return Ok(SnapshotFileRecvReader::V1(v1));
        }
        let v2 = try!(RecvSnapshotFileReader::new(&self.base, key, self.snap_size.clone()));
        Ok(SnapshotFileRecvReader::V2(v2))
    }

    #[inline]
    pub fn get_snapshot_file_send_reader(&self,
                                         key: &SnapKey,
                                         is_v1: bool)
                                         -> Result<SnapshotFileSendReader> {
        if is_v1 {
            let v1 = try!(SnapshotFileV1::new(&self.base, self.snap_size.clone(), false, key));
            let reader = try!(v1.reader());
            Ok(SnapshotFileSendReader::V1((v1, reader)))
        } else {
            let v2 = try!(SendSnapshotFileReader::new(&self.base, key, self.snap_size.clone()));
            Ok(SnapshotFileSendReader::V2(v2))
        }
    }

    #[inline]
    pub fn get_snapshot_file_recv_reader(&self,
                                         key: &SnapKey,
                                         is_v1: bool)
                                         -> io::Result<SnapshotFileRecvReader> {
        if is_v1 {
            let f = try!(SnapshotFileV1::new(&self.base, self.snap_size.clone(), false, key));
            Ok(SnapshotFileRecvReader::V1(f))
        } else {
            let f = try!(RecvSnapshotFileReader::new(&self.base, key, self.snap_size.clone()));
            Ok(SnapshotFileRecvReader::V2(f))
        }
    }

    /// Get the approximate size of snap file exists in snap directory.
    ///
    /// Return value is not guaranteed to be accurate.
    pub fn get_total_snap_size(&self) -> u64 {
        *self.snap_size.rl()
    }

    pub fn register(&mut self, key: SnapKey, entry: SnapEntry) {
        debug!("register [key: {}, entry: {:?}]", key, entry);
        match self.registry.entry(key) {
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

        self.notify_stats();
    }

    pub fn deregister(&mut self, key: &SnapKey, entry: &SnapEntry) {
        debug!("deregister [key: {}, entry: {:?}]", key, entry);
        let mut need_clean = false;
        let mut handled = false;
        if let Some(e) = self.registry.get_mut(key) {
            let last_len = e.len();
            e.retain(|e| e != entry);
            need_clean = e.is_empty();
            handled = last_len > e.len();
        }
        if need_clean {
            self.registry.remove(key);
        }
        if handled {
            self.notify_stats();
            return;
        }
        warn!("stale deregister key: {} {:?}", key, entry);
    }

    fn notify_stats(&self) {
        if let Some(ref ch) = self.ch {
            if let Err(e) = ch.try_send(Msg::SnapshotStats) {
                error!("notify snapshot stats failed {:?}", e)
            }
        }
    }

    pub fn stats(&self) -> SnapStats {
        // send_count, generating_count, receiving_count, applying_count
        let (mut sending_cnt, mut receiving_cnt) = (0, 0);
        for v in self.registry.values() {
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

pub type SnapManager = Arc<RwLock<SnapManagerCore>>;

pub fn new_snap_mgr<T: Into<String>>(path: T, ch: Option<SendCh<Msg>>) -> SnapManager {
    Arc::new(RwLock::new(SnapManagerCore::new(path, ch)))
}

#[cfg(test)]
mod test {
    use std::path::Path;
    use std::fs::File;
    use std::io::*;
    use std::sync::*;

    use tempdir::TempDir;

    use util::HandyRwLock;
    use super::*;

    fn write_test_snapshot_file(f: &mut SendSnapshotFile) {
        // Write at least one key-value to the SendSnapshotFile
        // because it's not allowed to finish a rocksdb sst file writer with no entries.
        for cf in super::get_snapshot_cfs() {
            if !f.next_file(cf) {
                continue;
            }
            f.add_kv(b"k", b"v").unwrap();
        }
    }

    #[test]
    fn test_snap_mgr() {
        let path = TempDir::new("test-snap-mgr").unwrap();

        // `mgr` should create the specified directory when it does not exist.
        let path1 = path.path().to_str().unwrap().to_owned() + "/snap1";
        let p = Path::new(&path1);
        assert!(!p.exists());
        let mut mgr = new_snap_mgr(path1.clone(), None);
        mgr.wl().init().unwrap();
        assert!(p.exists());

        // if target is a file, an error should be returned.
        let path2 = path.path().to_str().unwrap().to_owned() + "/snap2";
        File::create(&path2).unwrap();
        mgr = new_snap_mgr(path2, None);
        assert!(mgr.wl().init().is_err());

        // if temporary files exist, they should be deleted.
        let path3 = path.path().to_str().unwrap().to_owned() + "/snap3";
        let key1 = SnapKey::new(1, 1, 1);
        let size_track = Arc::new(RwLock::new(0));
        let mut f1 = SendSnapshotFile::new(&path3, &key1, size_track.clone()).unwrap();
        f1.init().unwrap();
        let mut f2 = SendSnapshotFile::new(&path3, &key1, size_track.clone()).unwrap();
        f2.init().unwrap();
        let key2 = SnapKey::new(2, 1, 1);
        let mut f3 = SendSnapshotFile::new(&path3, &key2, size_track.clone()).unwrap();
        f3.init().unwrap();
        write_test_snapshot_file(&mut f3);
        f3.save_all().unwrap();
        let mut f4 = SendSnapshotFile::new(&path3, &key2, size_track.clone()).unwrap();
        f4.init().unwrap();
        write_test_snapshot_file(&mut f4);
        f4.save_all().unwrap();
        assert!(!f1.exists());
        assert!(!f2.exists());
        assert!(f3.exists());
        assert!(f4.exists());
        mgr = new_snap_mgr(path3, None);
        mgr.wl().init().unwrap();
        assert!(!f1.exists());
        assert!(!f2.exists());
        assert!(f3.exists());
        assert!(f4.exists());
    }

    #[test]
    fn test_snap_size() {
        let path = TempDir::new("test-snap-mgr").unwrap();
        let path_str = path.path().to_str().unwrap();
        let mut mgr = new_snap_mgr(path_str, None);
        mgr.wl().init().unwrap();
        assert_eq!(mgr.rl().get_total_snap_size(), 0);

        let snap_key1 = SnapKey::new(1, 1, 1);
        let size_track = Arc::new(RwLock::new(0));
        let mut f1 = SendSnapshotFile::new(path_str, &snap_key1, size_track.clone()).unwrap();
        f1.init().unwrap();
        write_test_snapshot_file(&mut f1);
        f1.save_all().unwrap();
        let cf_sizes = f1.list_cf_sizes().unwrap();
        let mut total_size = 0;
        for &(_, size) in &cf_sizes {
            total_size += size;
        }

        let mut reader = SendSnapshotFileReader::new(path_str, &snap_key1, size_track.clone())
            .unwrap();
        let buf_len = total_size as usize;
        let mut buf = Vec::with_capacity(buf_len);
        buf.resize(buf_len, 0);
        let read_size = reader.read_to_end(&mut buf).unwrap();
        assert_eq!(read_size as u64, total_size);

        let mut f2 =
            RecvSnapshotFile::new(path_str, &snap_key1, cf_sizes.clone(), size_track.clone())
                .unwrap();
        f2.init().unwrap();
        f2.write_all(&buf[0..read_size]).unwrap();
        f2.save().unwrap();

        let snap_key2 = SnapKey::new(2, 1, 1);
        let mut f3 = SendSnapshotFile::new(path_str, &snap_key2, size_track.clone()).unwrap();
        f3.init().unwrap();
        write_test_snapshot_file(&mut f3);

        mgr = new_snap_mgr(path_str, None);
        mgr.wl().init().unwrap();
        // temporary file should not be count in snap size.
        assert_eq!(mgr.rl().get_total_snap_size(), total_size * 2);
        mgr.rl().get_snapshot_file_trans_reader(&snap_key1).unwrap().delete();
        assert_eq!(mgr.rl().get_total_snap_size(), total_size);
        mgr.rl().get_snapshot_file_apply_reader(&snap_key1).unwrap().delete();
        assert_eq!(mgr.rl().get_total_snap_size(), 0);

        let snap_key3 = SnapKey::new(3, 1, 1);
        let mut f4 = mgr.rl().get_send_snapshot_file(&snap_key3).unwrap();
        f4.init().unwrap();
        write_test_snapshot_file(&mut f4);
        assert_eq!(mgr.rl().get_total_snap_size(), 0);
        f4.save_all().unwrap();
        let mut total_size = 0;
        for (_, size) in f4.list_cf_sizes().unwrap() {
            total_size += size;
        }
        assert_eq!(mgr.rl().get_total_snap_size(), total_size);
    }
}
