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
use std::cmp;
use std::fs::{self, File, OpenOptions, Metadata};
use std::collections::HashMap;
use std::collections::hash_map::Entry;
use std::sync::{Arc, RwLock};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::path::{Path, PathBuf};
use std::result;
use std::str;
use std::time::Instant;

use byteorder::BigEndian;
use protobuf::Message;

use rocksdb::{DB, WriteBatch};
use kvproto::eraftpb::Snapshot;
use kvproto::metapb::Region;
use kvproto::raft_serverpb::RaftSnapshotData;
use raftstore::store::Msg;
use util::transport::SendCh;
use util::{HandyRwLock, rocksdb};
use raft;

use super::engine::Snapshot as DbSnapshot;
use super::peer_storage::JOB_STATUS_CANCELLING;
use super::keys::{self, enc_start_key, enc_end_key};
use super::util;

/// Name prefix for the self-generated snapshot file.
const SNAP_GEN_PREFIX: &'static str = "gen";
/// Name prefix for the received snapshot file.
const SNAP_REV_PREFIX: &'static str = "rev";

const TMP_FILE_SUFFIX: &'static str = ".tmp";
const CRC32_BYTES_COUNT: usize = 4;
const DEFAULT_READ_BUFFER_SIZE: usize = 4096;

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

pub trait SendSnapshotFileBuilder {
    fn build(&mut self, snap: &DbSnapshot, region: &Region) -> raft::Result<()>;
    fn total_size(&self) -> io::Result<u64>;
}

pub trait SnapshotFileCommon {
    fn path(&self) -> &str;
    fn exists(&self) -> bool;
}

pub trait SendSnapshotFile: SnapshotFileCommon + Read {
    fn meta(&self) -> io::Result<Metadata>;
    fn total_size(&self) -> io::Result<u64>;
    fn delete(&self);
}

pub trait RecvSnapshotFileWriter: SnapshotFileCommon + Write + Send {
    fn save(&mut self) -> io::Result<()>;
}

pub struct ApplyOptions<'a> {
    pub db: Arc<DB>,
    pub region: &'a Region,
    pub abort: Arc<AtomicUsize>,
    pub write_batch_size: usize,
    pub snapshot_size: &'a mut usize,
    pub snapshot_kv_count: &'a mut usize,
}

pub trait RecvSnapshotFileApplier: SnapshotFileCommon {
    fn delete(&self);
    fn apply(&mut self, options: ApplyOptions) -> Result<()>;
}

// A helper function to copy snapshot file from send reader to recv writer.
// Only used in tests.
pub fn copy_snapshot_file(mut reader: Box<SendSnapshotFile>,
                          mut writer: Box<RecvSnapshotFileWriter>)
                          -> io::Result<()> {
    if !writer.exists() {
        try!(io::copy(reader.as_mut(), writer.as_mut()));
        try!(writer.save());
    }
    Ok(())
}

mod v1 {
    use crc::crc32::{self, Digest, Hasher32};
    use byteorder::{WriteBytesExt, ReadBytesExt};
    use rocksdb::Writable;
    use util::codec::bytes::{BytesEncoder, CompactBytesDecoder};

    use super::super::engine::Iterable;
    use super::*;

    pub struct SendSnapFile {
        // the corresponding snapshot file
        snap_file: SnapFile,
        // reader to the os file
        reader: File,
    }

    impl SendSnapFile {
        pub fn new<T: Into<PathBuf>>(snap_dir: T,
                                     size_track: Arc<RwLock<u64>>,
                                     key: &SnapKey)
                                     -> io::Result<SendSnapFile> {
            let f = try!(SnapFile::new(snap_dir, size_track, true, key));
            let reader = try!(f.raw_reader());
            Ok(SendSnapFile {
                snap_file: f,
                reader: reader,
            })
        }
    }

    impl SnapshotFileCommon for SendSnapFile {
        fn path(&self) -> &str {
            self.snap_file.path()
        }

        fn exists(&self) -> bool {
            self.snap_file.exists()
        }
    }

    impl SendSnapshotFile for SendSnapFile {
        fn meta(&self) -> io::Result<Metadata> {
            self.snap_file.meta()
        }

        fn total_size(&self) -> io::Result<u64> {
            self.snap_file.total_size()
        }

        fn delete(&self) {
            self.snap_file.delete()
        }
    }

    impl Read for SendSnapFile {
        fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
            self.reader.read(buf)
        }
    }

    /// A structure represents the snapshot file.
    ///
    /// All changes to the file will be written to `tmp_file` first, and use
    /// `save` method to make them persistent. When saving a crc32 checksum
    /// will be appended to the file end automatically.
    pub struct SnapFile {
        file: PathBuf,
        digest: Digest,
        size_track: Arc<RwLock<u64>>,
        // File is the file obj represent the tmpfile, string is the actual path to
        // tmpfile.
        tmp_file: Option<(File, String)>,
    }

    impl SnapFile {
        pub fn new<T: Into<PathBuf>>(snap_dir: T,
                                     size_track: Arc<RwLock<u64>>,
                                     is_sending: bool,
                                     key: &SnapKey)
                                     -> io::Result<SnapFile> {
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

            let mut f = SnapFile {
                file: file_path,
                digest: Digest::new(crc32::IEEE),
                size_track: size_track,
                tmp_file: None,
            };
            try!(f.init());
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

        pub fn raw_reader(&self) -> io::Result<File> {
            File::open(self.path())
        }

        /// Get a validation reader.
        pub fn reader(&self) -> io::Result<SnapValidationReader> {
            SnapValidationReader::open(self.path())
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
    }

    impl SnapshotFileCommon for SnapFile {
        fn path(&self) -> &str {
            self.file.as_path().to_str().unwrap()
        }

        fn exists(&self) -> bool {
            self.file.exists() && self.file.is_file()
        }
    }

    impl RecvSnapshotFileApplier for SnapFile {
        fn delete(&self) {
            self.delete()
        }

        fn apply(&mut self, options: ApplyOptions) -> Result<()> {
            let mut reader = box_try!(self.reader());
            loop {
                try!(check_abort(&options.abort));
                let cf = box_try!(reader.decode_compact_bytes());
                if cf.is_empty() {
                    break;
                }
                let handle = box_try!(rocksdb::get_cf_handle(&options.db, unsafe {
                    str::from_utf8_unchecked(&cf)
                }));
                let mut wb = WriteBatch::new();
                let mut batch_size = 0;
                loop {
                    try!(check_abort(&options.abort));
                    let key = box_try!(reader.decode_compact_bytes());
                    if key.is_empty() {
                        box_try!(options.db.write(wb));
                        *options.snapshot_size += batch_size;
                        break;
                    }
                    *options.snapshot_kv_count += 1;
                    box_try!(util::check_key_in_region(keys::origin_key(&key), options.region));
                    batch_size += key.len();
                    let value = box_try!(reader.decode_compact_bytes());
                    batch_size += value.len();
                    box_try!(wb.put_cf(handle, &key, &value));
                    if batch_size >= options.write_batch_size {
                        box_try!(options.db.write(wb));
                        *options.snapshot_size += batch_size;
                        wb = WriteBatch::new();
                        batch_size = 0;
                    }
                }
            }

            box_try!(reader.validate());
            Ok(())
        }
    }

    impl RecvSnapshotFileWriter for SnapFile {
        /// Use the content in temporary files replace the target file.
        ///
        /// Please note that this method can only be called once.
        fn save(&mut self) -> io::Result<()> {
            self.save_impl(false)
        }
    }

    impl Write for SnapFile {
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

    impl SendSnapshotFileBuilder for SnapFile {
        fn build(&mut self, snap: &DbSnapshot, region: &Region) -> raft::Result<()> {
            if self.exists() {
                match self.reader().and_then(|mut r| r.validate()) {
                    Ok(()) => return Ok(()),
                    Err(e) => {
                        error!("[region {}] file {} is invalid, will regenerate: {:?}",
                               region.get_id(),
                               self.path(),
                               e);
                        try!(self.try_delete());
                        try!(self.init());
                    }
                }
            }

            let t = Instant::now();
            let mut snap_size = 0;
            let mut snap_key_cnt = 0;
            let (begin_key, end_key) = (enc_start_key(region), enc_end_key(region));
            for cf in snap.cf_names() {
                box_try!(self.encode_compact_bytes(cf.as_bytes()));
                try!(snap.scan_cf(cf,
                                  &begin_key,
                                  &end_key,
                                  false,
                                  &mut |key, value| {
                    snap_size += key.len();
                    snap_size += value.len();
                    snap_key_cnt += 1;
                    try!(self.encode_compact_bytes(key));
                    try!(self.encode_compact_bytes(value));
                    Ok(true)
                }));
                // use an empty byte array to indicate that cf reaches an end.
                box_try!(self.encode_compact_bytes(b""));
            }
            // use an empty byte array to indicate that kvpair reaches an end.
            box_try!(self.encode_compact_bytes(b""));
            try!(self.save_with_checksum());

            info!("[region {}] scan snapshot, size {}, key count {}, takes {:?}",
                  region.get_id(),
                  snap_size,
                  snap_key_cnt,
                  t.elapsed());
            Ok(())
        }

        fn total_size(&self) -> io::Result<u64> {
            let meta = try!(self.meta());
            Ok(meta.len())
        }
    }

    impl Drop for SnapFile {
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

    #[cfg(test)]
    mod test {
        use std::path::Path;
        use std::sync::{Arc, RwLock};
        use std::fs::OpenOptions;
        use tempdir::TempDir;
        use util::HandyRwLock;
        use super::*;

        #[test]
        fn test_snap_file() {
            let dir = TempDir::new("test-snap-mgr").unwrap();
            let str = dir.path().to_str().unwrap().to_owned() + "/snap1";
            let path = Path::new(&str);
            assert!(!path.exists());
            let key = SnapKey::new(1, 1, 1);
            let test_data = b"test_data";
            let exp_len = (test_data.len() + super::CRC32_BYTES_COUNT) as u64;
            let size_track = Arc::new(RwLock::new(0));
            let mut f1 = SnapFile::new(&path, size_track.clone(), true, &key).unwrap();
            let mut f2 = SnapFile::new(&path, size_track.clone(), false, &key).unwrap();
            assert!(!f1.exists());
            assert!(Path::new(&f1.tmp_file.as_ref().unwrap().1).exists());
            assert!(!f2.exists());
            assert!(Path::new(&f2.tmp_file.as_ref().unwrap().1).exists());
            f1.write_all(test_data).unwrap();
            f2.write_all(test_data).unwrap();
            assert!(!f1.exists());
            assert!(Path::new(&f1.tmp_file.as_ref().unwrap().1).exists());
            assert!(!f2.exists());
            assert!(Path::new(&f2.tmp_file.as_ref().unwrap().1).exists());
            let key2 = SnapKey::new(2, 1, 1);
            let mut f3 = SnapFile::new(&path, size_track.clone(), true, &key2).unwrap();
            let mut f4 = SnapFile::new(&path, size_track.clone(), false, &key2).unwrap();
            f3.write_all(test_data).unwrap();
            f4.write_all(test_data).unwrap();
            assert_eq!(*size_track.rl(), 0);
            f3.save_with_checksum().unwrap();
            f4.save_with_checksum().unwrap();
            assert!(f3.exists());
            assert!(f4.exists());
            assert_eq!(*size_track.rl(), exp_len * 2);
        }

        #[test]
        fn test_snap_validate() {
            let path = TempDir::new("test-snap-mgr").unwrap();
            let path_str = path.path().to_str().unwrap();

            let key1 = SnapKey::new(1, 1, 1);
            let size_track = Arc::new(RwLock::new(0));
            let mut f1 = SnapFile::new(path_str, size_track.clone(), false, &key1).unwrap();
            f1.write_all(b"testdata").unwrap();
            f1.save_with_checksum().unwrap();
            let mut reader = f1.reader().unwrap();
            reader.validate().unwrap();

            // read partially should not affect validation.
            reader = f1.reader().unwrap();
            reader.read(&mut [0, 0]).unwrap();
            reader.validate().unwrap();

            // read fully should not affect validation.
            reader = f1.reader().unwrap();
            while reader.read(&mut [0, 0]).unwrap() != 0 {}
            reader.validate().unwrap();

            let mut f = OpenOptions::new().write(true).open(f1.path()).unwrap();
            f.write_all(b"et").unwrap();
            reader = f1.reader().unwrap();
            reader.validate().unwrap_err();
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

    pub fn list_snap(&self) -> io::Result<Vec<(SnapKey, bool)>> {
        let path = Path::new(&self.base);
        let read_dir = try!(fs::read_dir(path));
        Ok(read_dir.filter_map(|p| {
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
                Some((SnapKey::new(numbers[0], numbers[1], numbers[2]), is_sending))
            })
            .collect())
    }

    #[inline]
    pub fn has_registered(&self, key: &SnapKey) -> bool {
        self.registry.contains_key(key)
    }

    pub fn get_send_snapshot_file_builder(&self,
                                          key: &SnapKey)
                                          -> io::Result<Box<SendSnapshotFileBuilder>> {
        let f = try!(v1::SnapFile::new(&self.base, self.snap_size.clone(), true, key));
        Ok(Box::new(f))
    }

    pub fn get_send_snapshot_file(&self, key: &SnapKey) -> io::Result<Box<SendSnapshotFile>> {
        let reader = try!(v1::SendSnapFile::new(&self.base, self.snap_size.clone(), key));
        Ok(Box::new(reader))
    }

    pub fn get_recv_snapshot_file_writer(&self,
                                         key: &SnapKey,
                                         _data: &[u8])
                                         -> io::Result<Box<RecvSnapshotFileWriter>> {
        let f = try!(v1::SnapFile::new(&self.base, self.snap_size.clone(), false, key));
        Ok(Box::new(f))
    }

    pub fn get_recv_snapshot_file_applier(&self,
                                          key: &SnapKey)
                                          -> io::Result<Box<RecvSnapshotFileApplier>> {
        let f = try!(v1::SnapFile::new(&self.base, self.snap_size.clone(), false, key));
        Ok(Box::new(f))
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
    use std::io::*;
    use std::sync::*;

    use tempdir::TempDir;

    use util::HandyRwLock;
    use super::*;
    use super::v1::*;

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
        let f1 = SnapFile::new(&path3, size_track.clone(), true, &key1).unwrap();
        let f2 = SnapFile::new(&path3, size_track.clone(), false, &key1).unwrap();
        let key2 = SnapKey::new(2, 1, 1);
        let mut f3 = SnapFile::new(&path3, size_track.clone(), true, &key2).unwrap();
        f3.save_with_checksum().unwrap();
        let mut f4 = SnapFile::new(&path3, size_track.clone(), false, &key2).unwrap();
        f4.save_with_checksum().unwrap();
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

        let key1 = SnapKey::new(1, 1, 1);
        let test_data = b"test_data";
        let exp_len = (test_data.len() + super::CRC32_BYTES_COUNT) as u64;
        let size_track = Arc::new(RwLock::new(0));
        let mut f1 = SnapFile::new(path_str, size_track.clone(), true, &key1).unwrap();
        let mut f2 = SnapFile::new(path_str, size_track.clone(), false, &key1).unwrap();
        f1.write_all(test_data).unwrap();
        f2.write_all(test_data).unwrap();
        let key2 = SnapKey::new(2, 1, 1);
        let mut f3 = SnapFile::new(path_str, size_track.clone(), true, &key2).unwrap();
        f3.write_all(test_data).unwrap();
        f3.save_with_checksum().unwrap();
        let mut f4 = SnapFile::new(path_str, size_track.clone(), false, &key2).unwrap();
        f4.write_all(test_data).unwrap();
        f4.save_with_checksum().unwrap();

        mgr = new_snap_mgr(path_str, None);
        mgr.wl().init().unwrap();
        // temporary file should not be count in snap size.
        assert_eq!(mgr.rl().get_total_snap_size(), exp_len * 2);

        mgr.rl().get_send_snapshot_file(&key2).unwrap().delete();
        assert_eq!(mgr.rl().get_total_snap_size(), exp_len);
        mgr.rl().get_recv_snapshot_file_applier(&key2).unwrap().delete();
        assert_eq!(mgr.rl().get_total_snap_size(), 0);
    }
}
