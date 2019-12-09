// Copyright 2016 TiKV Project Authors. Licensed under Apache-2.0.

use std::fs::{self, File, OpenOptions};
use std::io::{self, Error, ErrorKind, Write};
use std::path::{Path, PathBuf};

use crate::config::{ReadableDuration, ReadableSize};

/// Opens log file with append mode. Creates a new log file if it doesn't exist.
fn open_log_file(path: impl AsRef<Path>) -> io::Result<File> {
    let path = path.as_ref();
    let parent = path.parent().ok_or_else(|| {
        Error::new(
            ErrorKind::Other,
            "Unable to get parent directory of log file",
        )
    })?;
    if !parent.is_dir() {
        fs::create_dir_all(parent)?
    }
    OpenOptions::new().append(true).create(true).open(path)
}

/// A trait that describes a file rotation operation.
pub trait Rotator: Send {
    /// Return if the file need to be rotated.
    fn should_rotate(&self, file: &File) -> io::Result<bool>;

    /// Execute rotation and return a renamed file path.
    fn rotate(&mut self, file: &mut File, old_path: &Path, new_path: &Path) -> io::Result<()> {
        file.flush()?;
        fs::rename(old_path, new_path)?;

        *file = open_log_file(old_path)?;
        Ok(())
    }

    /// Check if the option is enabled in configuration.
    /// Return true if the `rotator` is valid.
    fn is_enable(&self) -> bool;
}

/// This `FileLogger` will iterate over a series of `Rotators`,
/// once the context trigger the `Rotator`, it will execute a rotation.
///
/// After rotating, the original log file would be renamed to "{original name}.{%Y-%m-%d-%H:%M:%S}".
/// Note: log file will *not* be compressed or otherwise modified.
pub struct RotatingFileLogger<N>
where
    N: 'static + Send + Fn(&Path) -> io::Result<PathBuf>,
{
    path: PathBuf,
    file: File,
    rename: N,
    rotators: Vec<Box<dyn Rotator>>,
}

/// Builder for `RotatingFileLogger`.
pub struct RotatingFileLoggerBuilder<N>
where
    N: 'static + Send + Fn(&Path) -> io::Result<PathBuf>,
{
    rotators: Vec<Box<dyn Rotator>>,
    path: PathBuf,
    rename: N,
}

impl<N> RotatingFileLoggerBuilder<N>
where
    N: 'static + Send + Fn(&Path) -> io::Result<PathBuf>,
{
    pub fn new(path: impl AsRef<Path>, rename: N) -> Self {
        RotatingFileLoggerBuilder {
            path: path.as_ref().to_path_buf(),
            rotators: vec![],
            rename,
        }
    }

    pub fn add_rotator<R: 'static + Rotator>(mut self, rotator: R) -> Self {
        if rotator.is_enable() {
            self.rotators.push(Box::new(rotator));
        }
        self
    }

    pub fn build(self) -> io::Result<RotatingFileLogger<N>> {
        Ok(RotatingFileLogger {
            rotators: self.rotators,
            file: open_log_file(&self.path)?,
            path: self.path,
            rename: self.rename,
        })
    }
}

impl<N> Write for RotatingFileLogger<N>
where
    N: 'static + Send + Fn(&Path) -> io::Result<PathBuf>,
{
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        self.file.write(bytes)
    }

    fn flush(&mut self) -> io::Result<()> {
        for rotator in self.rotators.iter_mut() {
            if rotator.should_rotate(&self.file)? {
                let new_path = (self.rename)(&self.path)?;
                rotator.rotate(&mut self.file, &self.path, &new_path)?;
                break;
            }
        }
        self.file.flush()
    }
}

impl<N> Drop for RotatingFileLogger<N>
where
    N: 'static + Send + Fn(&Path) -> io::Result<PathBuf>,
{
    fn drop(&mut self) {
        let _ = self.file.flush();
    }
}

pub struct RotateByTime {
    rotation_timespan: ReadableDuration,
}

impl RotateByTime {
    pub fn new(rotation_timespan: ReadableDuration) -> Self {
        Self { rotation_timespan }
    }
}

impl Rotator for RotateByTime {
    fn should_rotate(&self, file: &File) -> io::Result<bool> {
        let metadata = file.metadata()?;
        // Calculates how long the file has been written.
        let file_life = metadata
            .modified()?
            // TODO: change it to `created()`, however, in some Linux distros it isn't supported.
            .duration_since(metadata.accessed()?)
            .map_err(|_| {
                io::Error::new(
                    ErrorKind::Other,
                    "File accessed time is later than last-modified time",
                )
            })?;
        Ok(file_life > self.rotation_timespan.0)
    }

    fn is_enable(&self) -> bool {
        !self.rotation_timespan.is_zero()
    }
}

pub struct RotateBySize {
    rotation_size: ReadableSize,
}

impl RotateBySize {
    pub fn new(rotation_size: ReadableSize) -> Self {
        RotateBySize { rotation_size }
    }
}

impl Rotator for RotateBySize {
    fn should_rotate(&self, file: &File) -> io::Result<bool> {
        Ok(file.metadata()?.len() > self.rotation_size.0)
    }

    fn is_enable(&self) -> bool {
        self.rotation_size.0 != 0
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use fail::FailScenario;
    use std::ffi::OsStr;
    use std::time::{Duration, SystemTime, UNIX_EPOCH};
    use tempfile::TempDir;

    fn file_exists(file: impl AsRef<Path>) -> bool {
        let path = file.as_ref();
        path.exists() && path.is_file()
    }

    fn rename_with_subffix(
        path: impl AsRef<Path>,
        suffix: impl AsRef<OsStr>,
    ) -> io::Result<PathBuf> {
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
        let mut new_path = path.as_ref().to_path_buf().into_os_string();
        new_path.push(suffix);

        Ok(PathBuf::from(new_path))
    }

    #[test]
    fn test_should_rotate_by_time() {
        let tmp_dir = TempDir::new().unwrap();
        let path = tmp_dir.path().join("test_should_rotate_by_time.log");
        let suffix = ".backup";

        // Set last accessed time to NOW.
        let accessed = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs();

        // Set the last modification time to 1 minute later.
        let last_modified = SystemTime::now()
            .checked_add(Duration::from_secs(60))
            .unwrap()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs();

        // Create a file.
        open_log_file(path.clone()).unwrap();

        // Modify last_modified time.
        utime::set_file_times(path.clone(), accessed, last_modified).unwrap();

        let mut logger = RotatingFileLoggerBuilder::new(path.clone(), move |path| {
            rename_with_subffix(path, suffix)
        })
        .add_rotator(RotateByTime::new(ReadableDuration(Duration::from_secs(5))))
        .build()
        .unwrap();

        // Rotate normally
        logger.flush().unwrap();

        let mut new_path = PathBuf::from(path).into_os_string();
        new_path.push(suffix);

        assert!(file_exists(new_path));
    }

    #[test]
    fn test_should_not_rotate_by_time() {
        let tmp_dir = TempDir::new().unwrap();
        let path = tmp_dir.path().join("test_should_rotate_by_time.log");
        let suffix = ".backup";

        // Set last accessed time to NOW.
        let accessed = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs();

        // Set the last modification time to 1 minute later.
        let last_modified = SystemTime::now()
            .checked_add(Duration::from_secs(60))
            .unwrap()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs();

        // Create a file.
        open_log_file(path.clone()).unwrap();

        // Modify last_modified time.
        utime::set_file_times(path.clone(), accessed, last_modified).unwrap();

        let mut logger = RotatingFileLoggerBuilder::new(path.clone(), move |path| {
            rename_with_subffix(path, suffix)
        })
        .add_rotator(RotateByTime::new(ReadableDuration(Duration::from_secs(
            120,
        ))))
        .build()
        .unwrap();

        // Rotate normally
        logger.flush().unwrap();

        let mut new_path = PathBuf::from(path).into_os_string();
        new_path.push(suffix);

        assert!(!file_exists(new_path));
    }

    #[test]
    fn test_rotate_by_size() {
        let tmp_dir = TempDir::new().unwrap();
        let path = tmp_dir.path().join("test_should_rotate_by_time.log");
        let suffix = ".backup";

        let mut logger = RotatingFileLoggerBuilder::new(path.clone(), move |path| {
            rename_with_subffix(path, suffix)
        })
        .add_rotator(RotateBySize::new(ReadableSize::kb(1)))
        .build()
        .unwrap();

        let mut new_path = PathBuf::from(path).into_os_string();
        new_path.push(suffix);

        // Should not rotate.
        logger.write_all(&[0xff; 1024]).unwrap();
        logger.flush().unwrap();
        assert!(!file_exists(new_path.clone()));

        // Triggers rotation.
        logger.write_all(&[0xff; 1024]).unwrap();
        logger.flush().unwrap();
        assert!(file_exists(new_path));
    }

    #[test]
    fn test_failing_to_rotate_file_will_not_cause_panic() {
        let tmp_dir = TempDir::new().unwrap();
        let path = tmp_dir.path().join("test_no_panic.log");
        let suffix = ".backup";

        let mut logger = RotatingFileLoggerBuilder::new(path.clone(), move |path| {
            rename_with_subffix(path, suffix)
        })
        .add_rotator(RotateBySize::new(ReadableSize::kb(1)))
        .build()
        .unwrap();

        let mut new_path = PathBuf::from(path).into_os_string();
        new_path.push(suffix);

        logger.write_all(&[0xff; 1025]).unwrap();
        // trigger fail point
        let scenario = FailScenario::setup();
        fail::cfg("file_log_rename", "return(NotFound)").unwrap();
        assert!(logger.flush().is_err());
        fail::remove("file_log_rename");
        scenario.teardown();

        // dropping the logger still should not panic.
        drop(logger);
    }
}
