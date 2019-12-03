// Copyright 2016 TiKV Project Authors. Licensed under Apache-2.0.

use chrono::{DateTime, Duration, Utc};
use std::fs::{self, File, OpenOptions};
use std::io::{self, ErrorKind, Write};
use std::path::{Path, PathBuf};

/// Adds `Duration` to the initial date and time.
fn compute_rotation_time(initial: &DateTime<Utc>, timespan: Duration) -> DateTime<Utc> {
    *initial + timespan
}

/// Rotates file path with given timestamp.
fn rotation_file_path_with_timestamp(
    file_path: impl AsRef<Path>,
    timestamp: &DateTime<Utc>,
) -> PathBuf {
    let mut file_path = file_path.as_ref().as_os_str().to_os_string();
    file_path.push(format!(".{}", timestamp.format("%Y-%m-%d-%H:%M:%S")));
    file_path.into()
}

/// Opens log file with append mode. Creates a new log file if it doesn't exist.
fn open_log_file(path: impl AsRef<Path>) -> io::Result<File> {
    let path = path.as_ref();
    let parent = path
        .parent()
        .expect("Unable to get parent directory of log file");
    if !parent.is_dir() {
        fs::create_dir_all(parent)?
    }
    OpenOptions::new().append(true).create(true).open(path)
}

// Rename file with a timestamp
fn rename_with_timestamp(path: impl AsRef<Path>) -> io::Result<PathBuf> {
    fail_point!("file_log_rename", |t| {
        if let Some(t) = t {
            Err(match t.as_ref() {
                "NotFound" => ErrorKind::NotFound,
                "PermissionDenied" => ErrorKind::PermissionDenied,
                "AlreadyExists" => ErrorKind::AlreadyExists,
                "InvalidInput" => ErrorKind::InvalidInput,
                "InvalidData" => ErrorKind::InvalidData,
                "WriteZero" => ErrorKind::WriteZero,
                "UnexpectedEof" => ErrorKind::UnexpectedEof,
                _ => ErrorKind::Other,
            }
            .into())
        } else {
            let mut buf = PathBuf::new();
            buf.push("rotated_file");
            Ok(buf)
        }
    });

    // Note: renaming files while they're open only works on Linux and macOS.
    let new_path = rotation_file_path_with_timestamp(&path, &Utc::now());
    fs::rename(&path, &new_path).map(|_| new_path)
}

/// A trait that describes a file rotation operation
pub trait Rotator: Send {
    /// Return if the file need to be rotated.
    fn should_rotate(&self, file: &File) -> io::Result<bool>;

    /// Execute rotation and return a renamed file path
    fn rotate(&mut self, path: &Path, file: &mut File) -> io::Result<Option<PathBuf>>;

    /// Check if the option is enabled in configuration
    /// Return if the `rotator` is valid.
    fn validate(&self) -> bool;
}

/// This `FileLogger` will iterate over a series of `Rotators`,
/// once the context trigger the `Rotator`, it will execute a rotation.
///
/// After rotating, the original log file would be renamed to "{original name}.{%Y-%m-%d-%H:%M:%S}"
/// Note: log file will *not* be compressed or otherwise modified.
pub struct RotatingFileLogger {
    rotators: Vec<Box<dyn Rotator>>,
    path: PathBuf,
    file: File,

    #[cfg(test)]
    pub renamed: File,
}

impl RotatingFileLogger {
    pub fn new(path: impl AsRef<Path>) -> RotatingFileLoggerBuilder {
        RotatingFileLoggerBuilder {
            rotators: vec![],
            path: path.as_ref().to_path_buf(),
        }
    }
}

/// Builder for `RotatingFileLogger`
pub struct RotatingFileLoggerBuilder {
    rotators: Vec<Box<dyn Rotator>>,
    path: PathBuf,

    #[cfg(test)]
    renamed: PathBuf,
}

impl RotatingFileLoggerBuilder {
    pub fn add_rotator<R: 'static + Rotator>(mut self, rotator: R) -> Self {
        if rotator.validate() {
            self.rotators.push(Box::new(rotator));
        }
        self
    }

    pub fn build(self) -> io::Result<RotatingFileLogger> {
        Ok(RotatingFileLogger {
            rotators: self.rotators,
            file: open_log_file(&self.path)?,
            path: self.path,
        })
    }
}

impl Write for RotatingFileLogger {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        self.file.write(bytes)
    }

    fn flush(&mut self) -> io::Result<()> {
        for rotator in self.rotators.iter_mut() {
            if rotator.should_rotate(&self.file)? {
                let _ = rotator.rotate(&self.path, &mut self.file)?;
                break;
            }
        }
        self.file.flush()
    }

    #[cfg(test)]
    fn flush(&mut self) -> io::Result<()> {
        for rotator in self.rotators.iter_mut() {
            if rotator.should_rotate(&self.file)? {
                self.renamed = rotator.rotate(&self.path, &mut self.file)?;
                break;
            }
        }
        self.file.flush()
    }
}

impl Drop for RotatingFileLogger {
    fn drop(&mut self) {
        let _ = self.file.flush();
    }
}

pub struct RotateByTime {
    rotation_timespan: Duration,
    next_rotation_time: DateTime<Utc>,
}

impl RotateByTime {
    pub fn new(rotation_timespan: Duration) -> Self {
        Self {
            rotation_timespan,
            next_rotation_time: compute_rotation_time(&Utc::now(), rotation_timespan),
        }
    }

    #[cfg(test)]
    pub fn new_for_test(next_rotation_time: Datetime<Utc>, rotation_timespan: Duration) -> Self {
        Self {
            next_rotation_time,
            rotation_timespan,
        }
    }
}

impl Rotator for RotateByTime {
    fn should_rotate(&self, _: &File) -> io::Result<bool> {
        Ok(Utc::now() > self.next_rotation_time)
    }

    fn rotate(&mut self, path: &Path, file: &mut File) -> io::Result<Option<PathBuf>> {
        file.flush()?;

        let renamed = match rename_with_timestamp(path) {
            Ok(path) => Some(path),
            Err(ref e) if e.kind() == ErrorKind::NotFound => None,
            Err(e) => return Err(e),
        };

        self.next_rotation_time = compute_rotation_time(&Utc::now(), self.rotation_timespan);

        *file = open_log_file(&path)?;

        Ok(renamed)
    }

    fn validate(&self) -> bool {
        !self.rotation_timespan.is_zero()
    }
}

pub struct RotateBySize {
    rotation_size: u64,
}

impl RotateBySize {
    pub fn new(rotation_size: u64) -> Self {
        RotateBySize { rotation_size }
    }
}

impl Rotator for RotateBySize {
    fn should_rotate(&self, file: &File) -> io::Result<bool> {
        Ok(file.metadata()?.len() > self.rotation_size)
    }

    fn rotate(&mut self, path: &Path, file: &mut File) -> io::Result<Option<PathBuf>> {
        file.flush()?;

        let renamed = match rename_with_timestamp(path) {
            Ok(path) => Some(path),
            Err(ref e) if e.kind() == ErrorKind::NotFound => None,
            Err(e) => return Err(e),
        };

        *file = open_log_file(&path)?;

        Ok(renamed)
    }

    fn validate(&self) -> bool {
        self.rotation_size != 0
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs::OpenOptions;
    use std::io::prelude::*;
    use std::path::Path;

    use chrono::{Duration, Utc};
    use tempfile::TempDir;
    use utime;

    fn file_exists(file: impl AsRef<Path>) -> bool {
        let path = file.as_ref();
        path.exists() && path.is_file()
    }

    fn create_file_with_path(path: impl AsRef<Path>) -> (PathBuf, File) {
        let tmp_dir = TempDir::new().unwrap();
        let path = tmp_dir.path().join(path).to_str().unwrap().to_string();
        let file = open_log_file(&path).unwrap();

        (path.into(), file)
    }

    #[test]
    fn test_rotate_by_time() {
        let (path, mut log_file) = create_file_with_path("test_rotate_by_time.log");
        let next_rotation_time = Utc::now() - Duration::days(1);

        // Should rotate right now.
        let mut logger = RotatingFileLogger::new(path)
            .add_rotator(RotateByTime::new_for_test(
                next_rotation_time,
                Duration::days(1),
            ))
            .build()
            .unwrap();
        // Rotate
        logger.flush().unwrap();

        assert!(file_exist(logger.renamed));
    }

    #[test]
    fn test_rotate_by_size() {
        let (path, mut log_file) = create_file_with_path("test_rotate_by_size.log");
        let rotation_size = 1024;

        let mut logger = RotatingFileLogger::new(path)
            .add_rotator(RotateBySize::new(rotation_size))
            .build()
            .unwrap();

        log_file.write_all(&[0xff; 1023]);
        logger.flush().unwrap();
    }
}

//#[cfg(test)]
//mod tests {
//    use std::fs::OpenOptions;
//    use std::io::prelude::*;
//    use std::path::Path;
//
//    use chrono::{Duration, Utc};
//    use tempfile::TempDir;
//    use utime;
//
//    use super::RotatingFileLogger;
//
//    fn file_exists(file: impl AsRef<Path>) -> bool {
//        let path = file.as_ref();
//        path.exists() && path.is_file()
//    }
//
//    #[test]
//    fn test_rotating_file_logger() {
//        let tmp_dir = TempDir::new().unwrap();
//        let log_file = tmp_dir
//            .path()
//            .join("test_rotating_file_logger.log")
//            .to_str()
//            .unwrap()
//            .to_string();
//        // create a file with mtime == one day ago
//        {
//            let mut file = OpenOptions::new()
//                .append(true)
//                .create(true)
//                .open(&log_file)
//                .unwrap();
//            file.write_all(b"hello world!").unwrap();
//        }
//        let now = Utc::now();
//        let one_day = Duration::days(1);
//        let one_day_ago = now - one_day;
//        let one_day_ago_ts = one_day_ago.timestamp() as u64;
//        utime::set_file_times(&log_file, one_day_ago_ts, one_day_ago_ts).unwrap();
//        // initialize the logger
//        let mut logger = RotatingFileLogger::new(&log_file, one_day).unwrap();
//        assert!(logger.should_rotate());
//        let rotated_file = logger.rotate().unwrap().unwrap();
//        // check the rotated file exist
//        assert!(file_exists(&rotated_file));
//        assert!(!logger.should_rotate());
//    }
//
//    #[test]
//    fn test_failing_to_rotate_file_will_not_cause_panic() {
//        let tmp_dir = TempDir::new().unwrap();
//        let log_file = tmp_dir.path().join("test_rotating_file_logger.log");
//        let mut logger = RotatingFileLogger::new(&log_file, Duration::days(1)).unwrap();
//        // trigger fail point
//        fail::cfg("file_log_rename", "return(NotFound)").unwrap();
//        logger.rotate().unwrap();
//        fail::remove("file_log_rename");
//        // dropping the logger still should not panic.
//        drop(logger);
//    }
//}
