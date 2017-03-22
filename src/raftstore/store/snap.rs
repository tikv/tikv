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

use protobuf::Message;
use rocksdb::DB;
use kvproto::eraftpb::Snapshot as RaftSnapshot;
use kvproto::metapb::Region;
use kvproto::raft_serverpb::RaftSnapshotData;

use raft;
use raftstore::store::Msg;
use storage::CF_RAFT;
use util::transport::SendCh;
use util::HandyRwLock;
use util::collections::{HashMap, HashMapEntry as Entry};

use super::engine::Snapshot as DbSnapshot;
use super::peer_storage::JOB_STATUS_CANCELLING;

/// Name prefix for the self-generated snapshot file.
const SNAP_GEN_PREFIX: &'static str = "gen";
/// Name prefix for the received snapshot file.
const SNAP_REV_PREFIX: &'static str = "rev";

const TMP_FILE_SUFFIX: &'static str = ".tmp";

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

pub struct ApplyContext {
    pub db: Arc<DB>,
    pub region: Region,
    pub abort: Arc<AtomicUsize>,
    pub write_batch_size: usize,
    pub snapshot_size: usize,
    pub snapshot_kv_count: usize,
}

/// `Snapshot` is a trait for snapshot.
/// It's used in these scenarios:
///   1. build local snapshot
///   1. read local snapshot and then replicate it to remote raftstores
///   2. receive snapshot from remote raftstore and write it to local storage
///   3. snapshot gc
///   4. apply snapshot
pub trait Snapshot: Read + Write + Send {
    fn build(&mut self,
             snap: &DbSnapshot,
             region: &Region,
             snap_data: &mut RaftSnapshotData)
             -> raft::Result<()>;
    fn path(&self) -> &str;
    fn exists(&self) -> bool;
    fn delete(&self);
    fn meta(&self) -> io::Result<Metadata>;
    fn total_size(&self) -> io::Result<u64>;
    fn save(&mut self) -> io::Result<()>;
    fn apply(&mut self, context: &mut ApplyContext) -> Result<()>;
}

// A helper function to copy snapshot.
// Only used in tests.
pub fn copy_snapshot(mut from: Box<Snapshot>, mut to: Box<Snapshot>) -> io::Result<()> {
    if !to.exists() {
        try!(io::copy(&mut from, &mut to));
        try!(to.save());
    }
    Ok(())
}

fn need_to_pack(cf: &str) -> bool {
    // Data in CF_RAFT should be excluded for a snapshot.
    // Check CF_RAFT here and skip it, even though CF_RAFT should not occur
    // in the range [begin_key, end_key) of a RocksDB snapshot usually.
    cf != CF_RAFT
}

mod v1 {
    use std::cmp;
    use std::io::{self, Read, Write, ErrorKind};
    use std::fs::{self, File, OpenOptions, Metadata};
    use std::path::{Path, PathBuf};
    use std::sync::{Arc, RwLock};
    use std::str;
    use std::time::Instant;
    use crc::crc32::{self, Digest, Hasher32};
    use byteorder::{BigEndian, WriteBytesExt, ReadBytesExt};
    use rocksdb::{Writable, WriteBatch};
    use kvproto::metapb::Region;
    use kvproto::raft_serverpb::RaftSnapshotData;
    use raft;
    use util::{HandyRwLock, rocksdb};
    use util::codec::bytes::{BytesEncoder, CompactBytesDecoder};

    use super::super::engine::{Snapshot as DbSnapshot, Iterable};
    use super::super::keys::{self, enc_start_key, enc_end_key};
    use super::super::util;
    use super::{SNAP_GEN_PREFIX, SNAP_REV_PREFIX, TMP_FILE_SUFFIX, Result, SnapKey, Snapshot,
                ApplyContext, check_abort, need_to_pack};

    pub const CRC32_BYTES_COUNT: usize = 4;
    const DEFAULT_READ_BUFFER_SIZE: usize = 4096;

    /// A structure represents the snapshot.
    ///
    /// All changes to the file will be written to `tmp_file` first, and use
    /// `save` method to make them persistent. When saving a crc32 checksum
    /// will be appended to the file end automatically.
    pub struct Snap {
        file: PathBuf,
        digest: Option<Digest>,
        size_track: Arc<RwLock<u64>>,
        // File is the file obj represent the tmpfile, string is the actual path to
        // tmpfile.
        tmp_file: Option<(File, String)>,
        reader: Option<File>,
    }

    impl Snap {
        pub fn new_for_writing<T: Into<PathBuf>>(dir: T,
                                                 size_track: Arc<RwLock<u64>>,
                                                 is_sending: bool,
                                                 key: &SnapKey)
                                                 -> io::Result<Snap> {
            let mut path = dir.into();
            try!(Snap::prepare_path(&mut path, is_sending, key));

            let mut f = Snap {
                file: path,
                digest: Some(Digest::new(crc32::IEEE)),
                size_track: size_track,
                tmp_file: None,
                reader: None,
            };
            try!(f.init_for_writing());
            Ok(f)
        }

        pub fn new_for_reading<T: Into<PathBuf>>(dir: T,
                                                 size_track: Arc<RwLock<u64>>,
                                                 is_sending: bool,
                                                 key: &SnapKey)
                                                 -> io::Result<Snap> {
            let mut path = dir.into();
            try!(Snap::prepare_path(&mut path, is_sending, key));

            let f = Snap {
                file: path,
                digest: None,
                size_track: size_track,
                tmp_file: None,
                reader: None,
            };
            Ok(f)
        }

        fn prepare_path(path: &mut PathBuf, is_sending: bool, key: &SnapKey) -> io::Result<()> {
            if !path.exists() {
                try!(fs::create_dir_all(path.as_path()));
            }
            let prefix = if is_sending {
                SNAP_GEN_PREFIX
            } else {
                SNAP_REV_PREFIX
            };
            let file_name = format!("{}_{}.snap", prefix, key);
            path.push(&file_name);
            Ok(())
        }

        fn init_for_writing(&mut self) -> io::Result<()> {
            if self.exists() || self.tmp_file.is_some() {
                return Ok(());
            }

            let tmp_path = format!("{}{}", self.path(), TMP_FILE_SUFFIX);
            let tmp_f = try!(OpenOptions::new().write(true).create_new(true).open(&tmp_path));
            self.tmp_file = Some((tmp_f, tmp_path));
            Ok(())
        }

        /// Get a validation reader.
        fn get_validation_reader(&self) -> io::Result<SnapValidationReader> {
            SnapValidationReader::open(self.path())
        }

        fn try_delete(&self) -> io::Result<()> {
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
                    try!(f.write_u32::<BigEndian>(self.digest.as_ref().unwrap().sum32()));
                }
                try!(f.flush());
                let file_len = try!(fs::metadata(&path)).len();
                let mut size_track = self.size_track.wl();
                try!(fs::rename(path, self.file.as_path()));
                *size_track = size_track.saturating_add(file_len);
            }
            Ok(())
        }

        fn do_build(&mut self, snap: &DbSnapshot, region: &Region) -> raft::Result<()> {
            if self.exists() {
                match self.get_validation_reader().and_then(|r| r.validate()) {
                    Ok(()) => return Ok(()),
                    Err(e) => {
                        error!("[region {}] file {} is invalid, will rebuild: {:?}",
                               region.get_id(),
                               self.path(),
                               e);
                        try!(self.try_delete());
                        try!(self.init_for_writing());
                    }
                }
            }

            let t = Instant::now();
            let mut snap_size = 0;
            let mut snap_key_cnt = 0;
            let (begin_key, end_key) = (enc_start_key(region), enc_end_key(region));
            for cf in snap.cf_names() {
                if !need_to_pack(cf) {
                    continue;
                }
                box_try!(self.encode_compact_bytes(cf.as_bytes()));
                try!(snap.scan_cf(cf,
                                  &begin_key,
                                  &end_key,
                                  false,
                                  &mut |key, value| {
                    snap_size += key.len() + value.len();
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
    }

    impl Snapshot for Snap {
        fn build(&mut self,
                 snap: &DbSnapshot,
                 region: &Region,
                 snap_data: &mut RaftSnapshotData)
                 -> raft::Result<()> {
            try!(self.do_build(snap, region));
            let size = try!(self.total_size());
            snap_data.set_file_size(size);
            Ok(())
        }

        fn path(&self) -> &str {
            self.file.as_path().to_str().unwrap()
        }

        fn exists(&self) -> bool {
            self.file.exists() && self.file.is_file()
        }

        fn delete(&self) {
            if let Err(e) = self.try_delete() {
                error!("failed to delete {}: {:?}", self.path(), e);
            }
        }

        fn meta(&self) -> io::Result<Metadata> {
            self.file.metadata()
        }

        fn total_size(&self) -> io::Result<u64> {
            let meta = try!(self.meta());
            Ok(meta.len())
        }

        /// Use the content in temporary files replace the target file.
        ///
        /// Please note that this method can only be called once.
        fn save(&mut self) -> io::Result<()> {
            self.save_impl(false)
        }

        fn apply(&mut self, context: &mut ApplyContext) -> Result<()> {
            let mut reader = box_try!(self.get_validation_reader());
            loop {
                try!(check_abort(&context.abort));
                let cf = box_try!(reader.decode_compact_bytes());
                if cf.is_empty() {
                    break;
                }
                let handle = box_try!(rocksdb::get_cf_handle(&context.db, unsafe {
                    str::from_utf8_unchecked(&cf)
                }));
                let mut wb = WriteBatch::new();
                let mut batch_size = 0;
                loop {
                    try!(check_abort(&context.abort));
                    let key = box_try!(reader.decode_compact_bytes());
                    if key.is_empty() {
                        box_try!(context.db.write(wb));
                        context.snapshot_size += batch_size;
                        break;
                    }
                    context.snapshot_kv_count += 1;
                    box_try!(util::check_key_in_region(keys::origin_key(&key), &context.region));
                    batch_size += key.len();
                    let value = box_try!(reader.decode_compact_bytes());
                    batch_size += value.len();
                    box_try!(wb.put_cf(handle, &key, &value));
                    if batch_size >= context.write_batch_size {
                        box_try!(context.db.write(wb));
                        context.snapshot_size += batch_size;
                        wb = WriteBatch::new();
                        batch_size = 0;
                    }
                }
            }

            box_try!(reader.validate());
            Ok(())
        }
    }

    impl Read for Snap {
        fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
            if self.reader.is_none() {
                let reader = try!(File::open(&self.file));
                self.reader = Some(reader);
            }
            self.reader.as_mut().unwrap().read(buf)
        }
    }

    impl Write for Snap {
        fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
            if self.tmp_file.is_none() {
                return Ok(0);
            }
            let written = try!(self.tmp_file.as_mut().unwrap().0.write(buf));
            self.digest.as_mut().unwrap().write(&buf[..written]);
            Ok(written)
        }

        fn flush(&mut self) -> io::Result<()> {
            if self.tmp_file.is_none() {
                return Ok(());
            }
            self.tmp_file.as_mut().unwrap().0.flush()
        }
    }

    impl Drop for Snap {
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
        /// If the reader is consumed after calling this method, no further data can be
        /// read from this reader again.
        pub fn validate(mut self) -> io::Result<()> {
            if self.res.is_none() {
                if self.left > 0 {
                    let cap = cmp::min(self.left, DEFAULT_READ_BUFFER_SIZE);
                    let mut buf = vec![0; cap];
                    while self.left > 0 {
                        let _ = try!(self.read(&mut buf));
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
        use std::io::{Read, Write};
        use std::path::Path;
        use std::sync::{Arc, RwLock};
        use std::fs::OpenOptions;
        use tempdir::TempDir;
        use util::HandyRwLock;
        use super::super::{SnapKey, Snapshot};
        use super::Snap;

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
            let mut f1 = Snap::new_for_writing(&path, size_track.clone(), true, &key).unwrap();
            let mut f2 = Snap::new_for_writing(&path, size_track.clone(), false, &key).unwrap();
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
            let mut f3 = Snap::new_for_writing(&path, size_track.clone(), true, &key2).unwrap();
            let mut f4 = Snap::new_for_writing(&path, size_track.clone(), false, &key2).unwrap();
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
            let mut f1 = Snap::new_for_writing(path_str, size_track.clone(), false, &key1).unwrap();
            f1.write_all(b"testdata").unwrap();
            f1.save_with_checksum().unwrap();
            let mut reader = f1.get_validation_reader().unwrap();
            reader.validate().unwrap();

            // read partially should not affect validation.
            reader = f1.get_validation_reader().unwrap();
            reader.read_exact(&mut [0, 0]).unwrap();
            reader.validate().unwrap();

            // read fully should not affect validation.
            reader = f1.get_validation_reader().unwrap();
            while reader.read(&mut [0, 0]).unwrap() != 0 {}
            reader.validate().unwrap();

            let mut f = OpenOptions::new().write(true).open(f1.path()).unwrap();
            f.write_all(b"et").unwrap();
            reader = f1.get_validation_reader().unwrap();
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
                    } else {
                        *size += try!(p.metadata()).len();
                    }
                }
            }
        }
        Ok(())
    }

    pub fn list_snap(&self) -> io::Result<Vec<(SnapKey, bool)>> {
        let core = self.core.rl();
        let path = Path::new(&core.base);
        let read_dir = try!(fs::read_dir(path));
        Ok(read_dir.filter_map(|p| {
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
                Some((SnapKey::new(numbers[0], numbers[1], numbers[2]), is_sending))
            })
            .collect())
    }

    #[inline]
    pub fn has_registered(&self, key: &SnapKey) -> bool {
        self.core.rl().registry.contains_key(key)
    }

    pub fn get_snapshot_to_build(&self, key: &SnapKey) -> io::Result<Box<Snapshot>> {
        let core = self.core.rl();
        let f = try!(v1::Snap::new_for_writing(&core.base, core.snap_size.clone(), true, key));
        Ok(Box::new(f))
    }

    pub fn get_snapshot_for_sending(&self, key: &SnapKey) -> io::Result<Box<Snapshot>> {
        let core = self.core.rl();
        let reader = try!(v1::Snap::new_for_reading(&core.base, core.snap_size.clone(), true, key));
        Ok(Box::new(reader))
    }

    pub fn get_snapshot_for_receiving(&self,
                                      key: &SnapKey,
                                      _data: &[u8])
                                      -> io::Result<Box<Snapshot>> {
        let core = self.core.rl();
        let f = try!(v1::Snap::new_for_writing(&core.base, core.snap_size.clone(), false, key));
        Ok(Box::new(f))
    }

    pub fn get_snapshot_for_applying(&self, key: &SnapKey) -> io::Result<Box<Snapshot>> {
        let core = self.core.rl();
        let f = try!(v1::Snap::new_for_reading(&core.base, core.snap_size.clone(), false, key));
        Ok(Box::new(f))
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

#[cfg(test)]
mod test {
    use std::path::Path;
    use std::fs::File;
    use std::io::Write;
    use std::sync::*;

    use tempdir::TempDir;

    use super::*;
    use super::v1::{Snap, CRC32_BYTES_COUNT};

    #[test]
    fn test_snap_mgr() {
        let path = TempDir::new("test-snap-mgr").unwrap();

        // `mgr` should create the specified directory when it does not exist.
        let path1 = path.path().to_str().unwrap().to_owned() + "/snap1";
        let p = Path::new(&path1);
        assert!(!p.exists());
        let mut mgr = SnapManager::new(path1.clone(), None);
        mgr.init().unwrap();
        assert!(p.exists());

        // if target is a file, an error should be returned.
        let path2 = path.path().to_str().unwrap().to_owned() + "/snap2";
        File::create(&path2).unwrap();
        mgr = SnapManager::new(path2, None);
        assert!(mgr.init().is_err());

        // if temporary files exist, they should be deleted.
        let path3 = path.path().to_str().unwrap().to_owned() + "/snap3";
        let key1 = SnapKey::new(1, 1, 1);
        let size_track = Arc::new(RwLock::new(0));
        let f1 = Snap::new_for_writing(&path3, size_track.clone(), true, &key1).unwrap();
        let f2 = Snap::new_for_writing(&path3, size_track.clone(), false, &key1).unwrap();
        let key2 = SnapKey::new(2, 1, 1);
        let mut f3 = Snap::new_for_writing(&path3, size_track.clone(), true, &key2).unwrap();
        f3.save_with_checksum().unwrap();
        let mut f4 = Snap::new_for_writing(&path3, size_track.clone(), false, &key2).unwrap();
        f4.save_with_checksum().unwrap();
        assert!(!f1.exists());
        assert!(!f2.exists());
        assert!(f3.exists());
        assert!(f4.exists());
        mgr = SnapManager::new(path3, None);
        mgr.init().unwrap();
        assert!(!f1.exists());
        assert!(!f2.exists());
        assert!(f3.exists());
        assert!(f4.exists());
    }

    #[test]
    fn test_snap_size() {
        let path = TempDir::new("test-snap-mgr").unwrap();
        let path_str = path.path().to_str().unwrap();
        let mut mgr = SnapManager::new(path_str, None);
        mgr.init().unwrap();
        assert_eq!(mgr.get_total_snap_size(), 0);

        let key1 = SnapKey::new(1, 1, 1);
        let test_data = b"test_data";
        let exp_len = (test_data.len() + CRC32_BYTES_COUNT) as u64;
        let size_track = Arc::new(RwLock::new(0));
        let mut f1 = Snap::new_for_writing(path_str, size_track.clone(), true, &key1).unwrap();
        let mut f2 = Snap::new_for_writing(path_str, size_track.clone(), false, &key1).unwrap();
        f1.write_all(test_data).unwrap();
        f2.write_all(test_data).unwrap();
        let key2 = SnapKey::new(2, 1, 1);
        let mut f3 = Snap::new_for_writing(path_str, size_track.clone(), true, &key2).unwrap();
        f3.write_all(test_data).unwrap();
        f3.save_with_checksum().unwrap();
        let mut f4 = Snap::new_for_writing(path_str, size_track.clone(), false, &key2).unwrap();
        f4.write_all(test_data).unwrap();
        f4.save_with_checksum().unwrap();

        mgr = SnapManager::new(path_str, None);
        mgr.init().unwrap();
        // temporary file should not be count in snap size.
        assert_eq!(mgr.get_total_snap_size(), exp_len * 2);

        mgr.get_snapshot_for_sending(&key2).unwrap().delete();
        assert_eq!(mgr.get_total_snap_size(), exp_len);
        mgr.get_snapshot_for_applying(&key2).unwrap().delete();
        assert_eq!(mgr.get_total_snap_size(), 0);
    }
}
