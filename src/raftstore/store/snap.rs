use std::io::{self, Write, ErrorKind, Seek, SeekFrom, Read};
use std::fmt::{self, Formatter, Display};
use std::fs::{self, File, OpenOptions, Metadata};
use std::collections::HashMap;
use std::collections::hash_map::Entry;
use std::sync::{Arc, RwLock};
use std::path::{Path, PathBuf};

use crc::crc32::{self, Digest, Hasher32};
use byteorder::{BigEndian, WriteBytesExt, ReadBytesExt};
use protobuf::Message;

use kvproto::raftpb::Snapshot;
use kvproto::raft_serverpb::RaftSnapshotData;


#[derive(Clone, Hash, PartialEq, Eq)]
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

/// Name prefix for the self-generated snapshot file.
const SNAP_GEN_PREFIX: &'static str = "gen";
/// Name prefix for the received snapshot file.
const SNAP_REV_PREFIX: &'static str = "rev";

/// A structure represents the snapshot file.
///
/// All changes to the file will be written to `tmp_file` first, and use
/// `save` method to make them persistent. When saving a crc32 checksum
/// will be appended to the file end automatically.
pub struct SnapFile {
    file: PathBuf,
    digest: Digest,
    tmp_file: Option<(File, String)>,
}

impl SnapFile {
    fn new<T: Into<PathBuf>>(snap_dir: T, is_sending: bool, key: &SnapKey) -> io::Result<SnapFile> {
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
            tmp_file: None,
        };
        try!(f.init());
        Ok(f)
    }

    pub fn init(&mut self) -> io::Result<()> {
        if self.exists() || self.tmp_file.is_some() {
            return Ok(());
        }

        let tmp_path = format!("{}.tmp", self.path().display());
        let tmp_f = try!(OpenOptions::new().write(true).create_new(true).open(&tmp_path));
        self.tmp_file = Some((tmp_f, tmp_path));
        Ok(())
    }

    pub fn meta(&self) -> io::Result<Metadata> {
        self.file.metadata()
    }

    /// Validate whether current file is broken.
    pub fn validate(&self) -> io::Result<()> {
        let mut reader = try!(File::open(self.path()));
        let mut digest = Digest::new(crc32::IEEE);
        let len = try!(reader.metadata()).len();
        if len < 4 {
            return Err(io::Error::new(ErrorKind::InvalidInput, format!("file length {} < 4", len)));
        }
        let to_read = len as usize - 4;
        let mut total_read = 0;
        let mut buffer = vec![0; 4098];
        loop {
            let read = try!(reader.read(&mut buffer));
            if total_read + read >= to_read {
                digest.write(&buffer[..to_read - total_read]);
                try!(reader.seek(SeekFrom::End(-4)));
                break;
            }
            digest.write(&buffer);
            total_read += read;
        }
        let sum = try!(reader.read_u32::<BigEndian>());
        if sum != digest.sum32() {
            return Err(io::Error::new(ErrorKind::InvalidData,
                                      format!("crc not correct: {} != {}", sum, digest.sum32())));
        }
        Ok(())
    }

    pub fn exists(&self) -> bool {
        self.file.exists() && self.file.is_file()
    }

    pub fn delete(&self) {
        if let Err(e) = self.try_delete() {
            error!("failed to delete {}: {:?}", self.path().display(), e);
        }
    }

    pub fn try_delete(&self) -> io::Result<()> {
        fs::remove_file(self.path())
    }

    /// Use the content in temporary files replace the target file.
    ///
    /// Please note that this method can only be called once.
    pub fn save(&mut self) -> io::Result<()> {
        if let Some((mut f, path)) = self.tmp_file.take() {
            try!(f.write_u32::<BigEndian>(self.digest.sum32()));
            try!(f.flush());
            try!(fs::rename(path, self.file.as_path()));
        }
        Ok(())
    }

    pub fn path(&self) -> &Path {
        self.file.as_path()
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

impl Drop for SnapFile {
    fn drop(&mut self) {
        if let Some((_, path)) = self.tmp_file.take() {
            if let Err(e) = fs::remove_file(&path) {
                warn!("failed to delete temporary file {}: {:?}", path, e);
            }
        }
    }
}

/// `SnapManagerCore` trace all current processing snapshots.
pub struct SnapManagerCore {
    // directory to store snapfile.
    base: String,
    // bool indicate if the snapshot is being generated by the peer on current store.
    // usize indicate the times the snapkey has been registered.
    registry: HashMap<SnapKey, (bool, usize)>,
}

impl SnapManagerCore {
    pub fn new<T: Into<String>>(path: T) -> SnapManagerCore {
        SnapManagerCore {
            base: path.into(),
            registry: map![],
        }
    }

    pub fn try_recover(&self) -> io::Result<()> {
        let path = Path::new(&self.base);
        if !path.exists() {
            try!(fs::create_dir_all(path));
        }
        if !path.is_dir() {
            return Err(io::Error::new(ErrorKind::Other,
                                      format!("{} should be a directory", path.display())));
        }
        for path in try!(fs::read_dir(path)) {
            let p = try!(path);
            if !try!(p.file_type()).is_file() {
                continue;
            }
            try!(fs::remove_file(p.path()));
            // TODO: resume applying when suitable
        }
        Ok(())
    }

    #[inline]
    pub fn get_snap_file(&self, key: &SnapKey, is_sending: bool) -> io::Result<SnapFile> {
        SnapFile::new(&self.base, is_sending, key)
    }

    pub fn register(&mut self, key: SnapKey, is_sending: bool) {
        debug!("register [key: {}, is_sending: {}]", key, is_sending);
        match self.registry.entry(key) {
            Entry::Occupied(mut e) => {
                if e.get().0 == is_sending {
                    e.get_mut().1 += 1;
                } else {
                    info!("seems leadership changed, cleanup old snapfiles");
                    if let Ok(f) = SnapFile::new(&self.base, !is_sending, e.key()) {
                        f.delete();
                    }
                    e.insert((is_sending, 1));
                }
            }
            Entry::Vacant(e) => {
                e.insert((is_sending, 1));
            }
        }
    }

    pub fn deregister(&mut self, key: &SnapKey, is_sending: bool) {
        debug!("deregister [key: {}, is_sending: {}]", key, is_sending);
        let mut need_cleanup = false;
        match self.registry.get_mut(key) {
            Some(&mut (b, ref mut cnt)) if b == is_sending => {
                *cnt -= 1;
                need_cleanup = *cnt == 0;
            }
            _ => {}
        };
        if need_cleanup {
            self.registry.remove(key);
            if let Ok(f) = self.get_snap_file(key, is_sending) {
                f.delete();
            }
        }
    }
}

pub type SnapManager = Arc<RwLock<SnapManagerCore>>;

pub fn new_snap_mgr<T: Into<String>>(path: T) -> SnapManager {
    Arc::new(RwLock::new(SnapManagerCore::new(path)))
}
