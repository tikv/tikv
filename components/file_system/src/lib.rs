// Copyright 2020 TiKV Project Authors. Licensed under Apache-2.0.

#![feature(test)]
#![feature(duration_consts_float)]

#[macro_use]
extern crate lazy_static;
#[cfg(test)]
extern crate test;
#[allow(unused_extern_crates)]
extern crate tikv_alloc;

#[cfg(any(test, feature = "testexport"))]
use std::cell::Cell;
pub use std::{
    convert::TryFrom,
    fs::{
        canonicalize, create_dir, create_dir_all, hard_link, metadata, read_dir, read_link,
        remove_dir, remove_dir_all, remove_file, rename, set_permissions, symlink_metadata,
        DirBuilder, DirEntry, FileType, Metadata, Permissions, ReadDir,
    },
};
use std::{
    io::{self, ErrorKind, Read, Write},
    path::Path,
    pin::Pin,
    str::FromStr,
    sync::{Arc, Mutex},
    task::{ready, Context, Poll},
};

pub use file::{File, OpenOptions};
pub use io_stats::{
    fetch_io_bytes, get_io_type, get_thread_io_bytes_total, init as init_io_stats_collector,
    set_io_type,
};
pub use metrics_manager::{BytesFetcher, MetricsManager};
use online_config::ConfigValue;
use openssl::{
    error::ErrorStack,
    hash::{self, Hasher, MessageDigest},
};
pub use rate_limiter::{
    get_io_rate_limiter, set_io_rate_limiter, IoBudgetAdjustor, IoRateLimitMode, IoRateLimiter,
    IoRateLimiterStatistics,
};
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use strum::{EnumCount, EnumIter};
use tokio::io::{AsyncRead, ReadBuf};

mod file;
mod io_stats;
mod metrics;
mod metrics_manager;
mod rate_limiter;

#[derive(Clone, Copy, Debug, PartialEq)]
pub enum IoOp {
    Read,
    Write,
}

#[repr(C)]
#[derive(Clone, Copy, Debug, PartialEq, Hash, EnumCount, EnumIter)]
pub enum IoType {
    Other = 0,
    // Including coprocessor and storage read.
    ForegroundRead = 1,
    // Including scheduler worker, raftstore and apply. Scheduler worker only
    // does read related works, but it's on the path of foreground write, so
    // account it as foreground-write instead of foreground-read.
    ForegroundWrite = 2,
    Flush = 3,
    LevelZeroCompaction = 4,
    Compaction = 5,
    Replication = 6,
    LoadBalance = 7,
    Gc = 8,
    Import = 9,
    Export = 10,
    RewriteLog = 11,
}

impl IoType {
    pub fn as_str(&self) -> &str {
        match *self {
            IoType::Other => "other",
            IoType::ForegroundRead => "foreground_read",
            IoType::ForegroundWrite => "foreground_write",
            IoType::Flush => "flush",
            IoType::LevelZeroCompaction => "level_zero_compaction",
            IoType::Compaction => "compaction",
            IoType::Replication => "replication",
            IoType::LoadBalance => "load_balance",
            IoType::Gc => "gc",
            IoType::Import => "import",
            IoType::Export => "export",
            IoType::RewriteLog => "log_rewrite",
        }
    }
}

pub struct WithIoType {
    previous_io_type: IoType,
}

impl WithIoType {
    pub fn new(new_io_type: IoType) -> WithIoType {
        let previous_io_type = get_io_type();
        set_io_type(new_io_type);
        WithIoType { previous_io_type }
    }
}

impl Drop for WithIoType {
    fn drop(&mut self) {
        set_io_type(self.previous_io_type);
    }
}

#[repr(C)]
#[derive(Debug, Copy, Clone, Default, PartialEq, Eq)]
pub struct IoBytes {
    pub read: u64,
    pub write: u64,
}

impl std::ops::Sub for IoBytes {
    type Output = Self;

    fn sub(self, other: Self) -> Self::Output {
        Self {
            read: self.read.saturating_sub(other.read),
            write: self.write.saturating_sub(other.write),
        }
    }
}

impl std::ops::AddAssign for IoBytes {
    fn add_assign(&mut self, rhs: Self) {
        self.read += rhs.read;
        self.write += rhs.write;
    }
}

#[cfg(not(any(test, feature = "testexport")))]
fn get_thread_io_bytes_stats() -> Result<IoBytes, String> {
    get_thread_io_bytes_total()
}

/// Simulates getting the IO bytes stats for the current thread in test
/// scenarios.
///
/// This function retrieves the thread-local IO stats and adds the current
/// mock delta values (both read and write) to it. The mock delta is updated
/// on each invocation, simulating incremental IO operations. This is useful
/// for testing scenarios where the IO stats change over time.
#[cfg(any(test, feature = "testexport"))]
fn get_thread_io_bytes_stats() -> Result<IoBytes, String> {
    fail::fail_point!("failed_to_get_thread_io_bytes_stats", |_| {
        Err("get_thread_io_bytes_total failed".into())
    });
    thread_local! {
        static TOTAL_BYTES: Cell<IoBytes> = Cell::new(IoBytes::default());
    }
    TOTAL_BYTES.with(|stats| {
        let mut current_stats = stats.get();

        // Add the mock IO bytes to the stats.
        current_stats.read += (|| {
            fail::fail_point!("delta_read_io_bytes", |d| d
                .unwrap()
                .parse::<u64>()
                .unwrap());
            0
        })();
        current_stats.write += (|| {
            fail::fail_point!("delta_write_io_bytes", |d| d
                .unwrap()
                .parse::<u64>()
                .unwrap());
            0
        })();

        stats.set(current_stats);
        Ok(current_stats)
    })
}

/// A utility struct to track I/O bytes with error-tolerant initialization.
///
/// This struct is used to compute the delta (difference) of I/O bytes between
/// successive calls to `get_thread_io_bytes_total`. It handles cases where the
/// first call to `get_thread_io_bytes_total` may fail by ignoring I/O bytes
/// until a successful value is obtained.
///
/// Detail explanation:
/// 1. On the first successful call to `get_thread_io_bytes_total`, the value is
///    treated as the initial baseline.
/// 2. If `get_thread_io_bytes_total` fails initially, all I/O bytes before a
///    successful call are ignored.
/// 3. Once initialized, this struct calculates the delta between successive
///    values from `get_thread_io_bytes_total`.
pub struct IoBytesTracker {
    // A flag indicating whether the tracker has been successfully initialized.
    initialized: bool,
    // Stores the previous successfully fetched I/O bytes. Used to calculate deltas.
    prev_io_bytes: IoBytes,
    // Stores the initial successfully fetched I/O bytes.
    initial_io_bytes: IoBytes,
}
impl IoBytesTracker {
    /// Creates a new `IoBytesTracker` and attempts to initialize it.
    ///
    /// If `get_thread_io_bytes_total` succeeds during initialization,
    /// the tracker is marked as initialized and ready to compute deltas.
    /// Otherwise, it will defer initialization until the next successful
    /// `update`.
    pub fn new() -> Self {
        let mut tracker = IoBytesTracker {
            initialized: false,
            prev_io_bytes: IoBytes::default(),
            initial_io_bytes: IoBytes::default(),
        };
        tracker.update(); // Attempt to initialize immediately
        tracker
    }

    /// Update the tracker with the current I/O bytes.
    /// If initialization failed previously, it will initialize on the first
    /// successful fetch. Returns the delta of I/O bytes if initialized,
    /// otherwise returns None.
    pub fn update(&mut self) -> Option<IoBytes> {
        match get_thread_io_bytes_stats() {
            Ok(current_io_bytes) => {
                if self.initialized {
                    let read_delta = current_io_bytes.read - self.prev_io_bytes.read;
                    let write_delta = current_io_bytes.write - self.prev_io_bytes.write;
                    self.prev_io_bytes = current_io_bytes;
                    Some(IoBytes {
                        read: read_delta,
                        write: write_delta,
                    })
                } else {
                    // Initialize on the first successful fetch
                    self.prev_io_bytes = current_io_bytes;
                    self.initialized = true;
                    self.initial_io_bytes = current_io_bytes;
                    None // No delta to report yet
                }
            }
            Err(_) => {
                // Skip updates if the current fetch fails
                None
            }
        }
    }

    /// Returns the total accumulated I/O bytes.
    pub fn get_total_io_bytes(&self) -> IoBytes {
        IoBytes {
            read: self.prev_io_bytes.read - self.initial_io_bytes.read,
            write: self.prev_io_bytes.write - self.initial_io_bytes.write,
        }
    }
}

impl Default for IoBytesTracker {
    fn default() -> Self {
        Self::new()
    }
}

#[repr(u32)]
#[derive(Debug, Clone, PartialEq, Copy, EnumCount)]
pub enum IoPriority {
    Low = 0,
    Medium = 1,
    High = 2,
}

impl IoPriority {
    pub fn as_str(&self) -> &str {
        match *self {
            IoPriority::Low => "low",
            IoPriority::Medium => "medium",
            IoPriority::High => "high",
        }
    }

    fn from_u32(i: u32) -> Self {
        match i {
            0 => IoPriority::Low,
            1 => IoPriority::Medium,
            2 => IoPriority::High,
            _ => panic!("unknown io priority {}", i),
        }
    }
}

impl std::str::FromStr for IoPriority {
    type Err = String;
    fn from_str(s: &str) -> Result<IoPriority, String> {
        match s {
            "low" => Ok(IoPriority::Low),
            "medium" => Ok(IoPriority::Medium),
            "high" => Ok(IoPriority::High),
            s => Err(format!("expect: low, medium or high, got: {:?}", s)),
        }
    }
}

impl Serialize for IoPriority {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(self.as_str())
    }
}

impl<'de> Deserialize<'de> for IoPriority {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        use serde::de::{Error, Unexpected, Visitor};
        struct StrVistor;
        impl<'de> Visitor<'de> for StrVistor {
            type Value = IoPriority;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                write!(formatter, "a IO priority")
            }

            fn visit_str<E>(self, value: &str) -> Result<IoPriority, E>
            where
                E: Error,
            {
                let p = match IoPriority::from_str(&value.trim().to_lowercase()) {
                    Ok(p) => p,
                    _ => {
                        return Err(E::invalid_value(
                            Unexpected::Other("invalid IO priority"),
                            &self,
                        ));
                    }
                };
                Ok(p)
            }
        }

        deserializer.deserialize_str(StrVistor)
    }
}

impl From<IoPriority> for ConfigValue {
    fn from(mode: IoPriority) -> ConfigValue {
        ConfigValue::String(mode.as_str().to_owned())
    }
}

impl TryFrom<ConfigValue> for IoPriority {
    type Error = String;
    fn try_from(c: ConfigValue) -> Result<IoPriority, Self::Error> {
        if let ConfigValue::String(s) = c {
            Self::from_str(s.as_str())
        } else {
            panic!("expect: ConfigValue::String, got: {:?}", c);
        }
    }
}

/// Indicates how large a buffer to pre-allocate before reading the entire file.
fn initial_buffer_size(file: &File) -> io::Result<usize> {
    // Allocate one extra byte so the buffer doesn't need to grow before the
    // final `read` call at the end of the file.  Don't worry about `usize`
    // overflow because reading will fail regardless in that case.
    file.metadata().map(|m| m.len() as usize + 1)
}

/// Write a slice as the entire contents of a file.
pub fn write<P: AsRef<Path>, C: AsRef<[u8]>>(path: P, contents: C) -> io::Result<()> {
    File::create(path)?.write_all(contents.as_ref())
}

/// Read the entire contents of a file into a bytes vector.
pub fn read<P: AsRef<Path>>(path: P) -> io::Result<Vec<u8>> {
    let mut file = File::open(path)?;
    let mut bytes = Vec::with_capacity(initial_buffer_size(&file)?);
    file.read_to_end(&mut bytes)?;
    Ok(bytes)
}

/// Read the entire contents of a file into a string.
pub fn read_to_string<P: AsRef<Path>>(path: P) -> io::Result<String> {
    let mut file = File::open(path)?;
    let mut string = String::with_capacity(initial_buffer_size(&file)?);
    file.read_to_string(&mut string)?;
    Ok(string)
}

fn copy_imp(from: &Path, to: &Path, sync: bool) -> io::Result<u64> {
    if !from.is_file() {
        return Err(io::Error::new(
            ErrorKind::InvalidInput,
            "the source path is not an existing regular file",
        ));
    }

    let mut reader = File::open(from)?;
    let mut writer = File::create(to)?;
    let perm = reader.metadata()?.permissions();

    let ret = io::copy(&mut reader, &mut writer)?;
    set_permissions(to, perm)?;
    if sync {
        writer.sync_all()?;
        if let Some(parent) = to.parent() {
            sync_dir(parent)?;
        }
    }
    Ok(ret)
}

/// Copies the contents of one file to another. This function will also
/// copy the permission bits of the original file to the destination file.
pub fn copy<P: AsRef<Path>, Q: AsRef<Path>>(from: P, to: Q) -> io::Result<u64> {
    copy_imp(from.as_ref(), to.as_ref(), false /* sync */)
}

/// Copies the contents and permission bits of one file to another, then
/// synchronizes.
pub fn copy_and_sync<P: AsRef<Path>, Q: AsRef<Path>>(from: P, to: Q) -> io::Result<u64> {
    copy_imp(from.as_ref(), to.as_ref(), true /* sync */)
}

pub fn get_file_size<P: AsRef<Path>>(path: P) -> io::Result<u64> {
    let meta = metadata(path)?;
    Ok(meta.len())
}

pub fn file_exists<P: AsRef<Path>>(file: P) -> bool {
    let path = file.as_ref();
    path.exists() && path.is_file()
}

/// Deletes given path from file system. Returns `true` on success, `false` if
/// the file doesn't exist. Otherwise the raw error will be returned.
pub fn delete_file_if_exist<P: AsRef<Path>>(file: P) -> io::Result<bool> {
    match remove_file(&file) {
        Ok(_) => Ok(true),
        Err(ref e) if e.kind() == ErrorKind::NotFound => Ok(false),
        Err(e) => Err(e),
    }
}

/// Deletes given path from file system. Returns `true` on success, `false` if
/// the directory doesn't exist. Otherwise the raw error will be returned.
pub fn delete_dir_if_exist<P: AsRef<Path>>(dir: P) -> io::Result<bool> {
    match remove_dir_all(&dir) {
        Ok(_) => Ok(true),
        Err(ref e) if e.kind() == ErrorKind::NotFound => Ok(false),
        Err(e) => Err(e),
    }
}

/// Creates a new, empty directory at the provided path. Returns `true` on
/// success, `false` if the directory already exists. Otherwise the raw error
/// will be returned.
pub fn create_dir_if_not_exist<P: AsRef<Path>>(dir: P) -> io::Result<bool> {
    match create_dir(&dir) {
        Ok(_) => Ok(true),
        Err(ref e) if e.kind() == ErrorKind::AlreadyExists => Ok(false),
        Err(e) => Err(e),
    }
}

/// Call fsync on directory by its path
pub fn sync_dir<P: AsRef<Path>>(path: P) -> io::Result<()> {
    // File::open will not error when opening a directory
    // because it just call libc::open and do not do the file or dir check
    File::open(path)?.sync_all()
}

const DIGEST_BUFFER_SIZE: usize = 1024 * 1024;

/// Calculates the given file's Crc32 checksum.
pub fn calc_crc32<P: AsRef<Path>>(path: P) -> io::Result<u32> {
    let mut digest = crc32fast::Hasher::new();
    let mut f = OpenOptions::new().read(true).open(path)?;
    let mut buf = vec![0; DIGEST_BUFFER_SIZE];
    loop {
        match f.read(&mut buf[..]) {
            Ok(0) => {
                return Ok(digest.finalize());
            }
            Ok(n) => {
                digest.update(&buf[..n]);
            }
            Err(ref e) if e.kind() == ErrorKind::Interrupted => {}
            Err(err) => return Err(err),
        }
    }
}

/// Calculates crc32 and decrypted size for a given reader.
pub fn calc_crc32_and_size<R: Read>(reader: &mut R) -> io::Result<(u32, u64)> {
    let mut digest = crc32fast::Hasher::new();
    let (mut buf, mut fsize) = (vec![0; DIGEST_BUFFER_SIZE], 0);
    loop {
        match reader.read(&mut buf[..]) {
            Ok(0) => {
                return Ok((digest.finalize(), fsize as u64));
            }
            Ok(n) => {
                digest.update(&buf[..n]);
                fsize += n;
            }
            Err(ref e) if e.kind() == ErrorKind::Interrupted => {}
            Err(err) => return Err(err),
        }
    }
}

/// Calculates the given content's CRC32 checksum.
pub fn calc_crc32_bytes(contents: &[u8]) -> u32 {
    let mut digest = crc32fast::Hasher::new();
    digest.update(contents);
    digest.finalize()
}

pub fn sha256(input: &[u8]) -> Result<Vec<u8>, ErrorStack> {
    hash::hash(MessageDigest::sha256(), input).map(|digest| digest.to_vec())
}

/// Wrapper of a reader which computes its SHA-256 hash while reading.
pub struct Sha256Reader<R> {
    reader: R,
    hasher: Arc<Mutex<Hasher>>,
}

impl<R> Sha256Reader<R> {
    /// Creates a new `Sha256Reader`, wrapping the given reader.
    pub fn new(reader: R) -> Result<(Self, Arc<Mutex<Hasher>>), ErrorStack> {
        let hasher = Arc::new(Mutex::new(Hasher::new(MessageDigest::sha256())?));
        Ok((
            Sha256Reader {
                reader,
                hasher: hasher.clone(),
            },
            hasher,
        ))
    }
}

impl<R: Read> Read for Sha256Reader<R> {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        let len = self.reader.read(buf)?;
        self.hasher.lock().unwrap().update(&buf[..len])?;
        Ok(len)
    }
}

impl<R: AsyncRead + Unpin> AsyncRead for Sha256Reader<R> {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        let initial_filled_len = buf.filled().len();
        ready!(Pin::new(&mut self.reader).poll_read(cx, buf))?;

        let filled_len = buf.filled().len();
        if initial_filled_len == filled_len {
            return Poll::Ready(Ok(()));
        }
        let new_data = &buf.filled()[initial_filled_len..filled_len];

        // Update the hasher with the read data
        let mut hasher = self
            .hasher
            .lock()
            .expect("failed to lock hasher in Sha256Reader async read");
        if let Err(e) = hasher.update(new_data) {
            return Poll::Ready(Err(io::Error::new(ErrorKind::Other, e)));
        }

        Poll::Ready(Ok(()))
    }
}

pub const SPACE_PLACEHOLDER_FILE: &str = "space_placeholder_file";

/// Create a file with hole, to reserve space for TiKV.
pub fn reserve_space_for_recover<P: AsRef<Path>>(data_dir: P, file_size: u64) -> io::Result<()> {
    let path = data_dir.as_ref().join(SPACE_PLACEHOLDER_FILE);
    if file_exists(&path) {
        if get_file_size(&path)? == file_size {
            return Ok(());
        }
        delete_file_if_exist(&path)?;
    }
    fn do_reserve(dir: &Path, path: &Path, file_size: u64) -> io::Result<()> {
        let f = File::create(path)?;
        f.allocate(file_size)?;
        f.sync_all()?;
        sync_dir(dir)
    }
    if file_size > 0 {
        let res = do_reserve(data_dir.as_ref(), &path, file_size);
        if res.is_err() {
            let _ = delete_file_if_exist(&path);
        }
        res
    } else {
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::{io::Write, iter};

    use rand::{distributions::Alphanumeric, thread_rng, Rng};
    use tempfile::{Builder, TempDir};

    use super::*;

    #[test]
    fn test_get_file_size() {
        let tmp_dir = TempDir::new().unwrap();
        let dir_path = tmp_dir.path().to_path_buf();

        // Ensure it works to get the size of an empty file.
        let empty_file = dir_path.join("empty_file");
        {
            let _ = OpenOptions::new()
                .write(true)
                .create_new(true)
                .open(&empty_file)
                .unwrap();
        }
        assert_eq!(get_file_size(&empty_file).unwrap(), 0);

        // Ensure it works to get the size of an non-empty file.
        let non_empty_file = dir_path.join("non_empty_file");
        let size = 5;
        let v = vec![0; size];
        {
            let mut f = OpenOptions::new()
                .write(true)
                .create_new(true)
                .open(&non_empty_file)
                .unwrap();
            f.write_all(&v[..]).unwrap();
        }
        assert_eq!(get_file_size(&non_empty_file).unwrap(), size as u64);

        // Ensure it works for non-existent file.
        let non_existent_file = dir_path.join("non_existent_file");
        get_file_size(non_existent_file).unwrap_err();
    }

    #[test]
    fn test_file_exists() {
        let tmp_dir = TempDir::new().unwrap();
        let dir_path = tmp_dir.path().to_path_buf();

        assert_eq!(file_exists(&dir_path), false);

        let existent_file = dir_path.join("empty_file");
        {
            let _ = OpenOptions::new()
                .write(true)
                .create_new(true)
                .open(&existent_file)
                .unwrap();
        }
        assert_eq!(file_exists(&existent_file), true);

        let non_existent_file = dir_path.join("non_existent_file");
        assert_eq!(file_exists(non_existent_file), false);
    }

    #[test]
    fn test_delete_file_if_exist() {
        let tmp_dir = TempDir::new().unwrap();
        let dir_path = tmp_dir.path().to_path_buf();

        let existent_file = dir_path.join("empty_file");
        {
            let _ = OpenOptions::new()
                .write(true)
                .create_new(true)
                .open(&existent_file)
                .unwrap();
        }
        assert_eq!(file_exists(&existent_file), true);
        delete_file_if_exist(&existent_file).unwrap();
        assert_eq!(file_exists(&existent_file), false);

        let non_existent_file = dir_path.join("non_existent_file");
        delete_file_if_exist(non_existent_file).unwrap();
    }

    fn gen_rand_file<P: AsRef<Path>>(path: P, size: usize) -> u32 {
        let mut rng = thread_rng();
        let s: Vec<u8> = iter::repeat(())
            .map(|()| rng.sample(Alphanumeric))
            .take(size)
            .collect();
        write(path, s.as_slice()).unwrap();
        calc_crc32_bytes(s.as_slice())
    }

    #[test]
    fn test_calc_crc32() {
        let tmp_dir = TempDir::new().unwrap();

        let small_file = tmp_dir.path().join("small.txt");
        let small_checksum = gen_rand_file(&small_file, 1024);
        assert_eq!(calc_crc32(&small_file).unwrap(), small_checksum);

        let large_file = tmp_dir.path().join("large.txt");
        let large_checksum = gen_rand_file(&large_file, DIGEST_BUFFER_SIZE * 4);
        assert_eq!(calc_crc32(&large_file).unwrap(), large_checksum);
    }

    #[test]
    fn test_create_delete_dir() {
        let tmp_dir = TempDir::new().unwrap();
        let subdir = tmp_dir.path().join("subdir");

        assert!(!delete_dir_if_exist(&subdir).unwrap());
        assert!(create_dir_if_not_exist(&subdir).unwrap());
        assert!(!create_dir_if_not_exist(&subdir).unwrap());
        assert!(delete_dir_if_exist(&subdir).unwrap());
    }

    #[test]
    fn test_sync_dir() {
        let tmp_dir = TempDir::new().unwrap();
        sync_dir(tmp_dir.path()).unwrap();
        let non_existent_file = tmp_dir.path().join("non_existent_file");
        sync_dir(non_existent_file).unwrap_err();
    }

    #[test]
    fn test_sha256() {
        let tmp_dir = TempDir::new().unwrap();
        let large_file = tmp_dir.path().join("large.txt");
        gen_rand_file(&large_file, DIGEST_BUFFER_SIZE * 4);

        let large_file_bytes = read(&large_file).unwrap();
        let direct_sha256 = sha256(&large_file_bytes).unwrap();

        let large_file_reader = File::open(&large_file).unwrap();
        let (mut sha256_reader, sha256_hasher) = Sha256Reader::new(large_file_reader).unwrap();
        let ret = sha256_reader.read_to_end(&mut Vec::new());

        assert_eq!(ret.unwrap(), DIGEST_BUFFER_SIZE * 4);
        assert_eq!(
            sha256_hasher.lock().unwrap().finish().unwrap().to_vec(),
            direct_sha256
        );
    }

    #[test]
    fn test_reserve_space_for_recover() {
        let tmp_dir = Builder::new()
            .prefix("test_reserve_space_for_recover")
            .tempdir()
            .unwrap();
        let data_path = tmp_dir.path();
        let file_path = data_path.join(SPACE_PLACEHOLDER_FILE);
        let file = file_path.as_path();
        let reserve_size = 4096 * 4;
        assert!(!file.exists());
        reserve_space_for_recover(data_path, reserve_size).unwrap();
        assert!(file.exists());
        let meta = file.metadata().unwrap();
        assert_eq!(meta.len(), reserve_size);
        reserve_space_for_recover(data_path, 0).unwrap();
        assert!(!file.exists());
    }

    #[cfg(feature = "failpoints")]
    #[test]
    fn test_io_bytes_tracker_normal() {
        fail::cfg("delta_read_io_bytes", "return(100)").unwrap();
        fail::cfg("delta_write_io_bytes", "return(50)").unwrap();
        let mut io_tracker = IoBytesTracker::new();
        assert_eq!(io_tracker.prev_io_bytes.read, 100);
        assert_eq!(io_tracker.prev_io_bytes.write, 50);
        assert_eq!(io_tracker.initialized, true);
        let io_bytes = io_tracker.update();
        assert_eq!(io_bytes.unwrap().read, 100);
        assert_eq!(io_bytes.unwrap().write, 50);
        assert_eq!(io_tracker.prev_io_bytes.read, 200);
        assert_eq!(io_tracker.prev_io_bytes.write, 100);

        let total_io_bytes = io_tracker.get_total_io_bytes();
        assert_eq!(total_io_bytes.read, 100);
        assert_eq!(total_io_bytes.write, 50);
        let _ = io_tracker.update();
        let total_io_bytes = io_tracker.get_total_io_bytes();
        assert_eq!(total_io_bytes.read, 200);
        assert_eq!(total_io_bytes.write, 100);
    }

    #[cfg(feature = "failpoints")]
    #[test]
    fn test_io_bytes_tracker_initialization_failure() {
        fail::cfg("failed_to_get_thread_io_bytes_stats", "1*return").unwrap();
        fail::cfg("delta_read_io_bytes", "return(100)").unwrap();
        fail::cfg("delta_write_io_bytes", "return(50)").unwrap();

        let mut io_tracker = IoBytesTracker::new();
        assert_eq!(io_tracker.initialized, false);
        assert_eq!(io_tracker.prev_io_bytes.read, 0);
        assert_eq!(io_tracker.prev_io_bytes.write, 0);
        let io_bytes = io_tracker.update();
        assert!(io_bytes.is_none());
        assert_eq!(io_tracker.prev_io_bytes.read, 100);
        assert_eq!(io_tracker.prev_io_bytes.write, 50);
        let io_bytes = io_tracker.update();
        assert_eq!(io_bytes.unwrap().read, 100);
        assert_eq!(io_bytes.unwrap().write, 50);
        assert_eq!(io_tracker.prev_io_bytes.read, 200);
        assert_eq!(io_tracker.prev_io_bytes.write, 100);
    }
}
