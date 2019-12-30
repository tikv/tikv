// Copyright 2016 TiKV Project Authors. Licensed under Apache-2.0.

use std::fs::{self, File, OpenOptions};
use std::io::{self, Error, ErrorKind, Write};
use std::path::{Path, PathBuf};
use std::time::{Duration, SystemTime};

use chrono::{DateTime, Local};

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
    /// Check if the option is enabled in configuration.
    /// Return true if the `rotator` is valid.
    fn is_enabled(&self) -> bool;

    /// Call by operator, initializes the states of rotators.
    fn prepare(&mut self, file: &File) -> io::Result<()>;

    /// Return if the file need to be rotated.
    fn should_rotate(&self) -> bool;

    /// Call by operator, update rotators' state while the operator try to write some data.
    fn on_write(&mut self, data: &[u8]) -> io::Result<()>;

    /// Call by operator, update rotators' state while the operator execute a rotation.
    fn on_rotate(&mut self) -> io::Result<()>;
}

/// This `FileLogger` will iterate over a series of `Rotators`,
/// once the context trigger the `Rotator`, it will execute a rotation.
///
/// After rotating, the original log file would be renamed to "{original name}.{%Y-%m-%d-%H:%M:%S}".
/// Note: log file will *not* be compressed or otherwise modified.
pub struct RotatingFileLogger {
    path: PathBuf,
    file: File,
    rename: Box<dyn Send + Fn(&Path) -> io::Result<PathBuf>>,
    rotators: Vec<Box<dyn Rotator>>,
}

/// Builder for `RotatingFileLogger`.
pub struct RotatingFileLoggerBuilder {
    rotators: Vec<Box<dyn Rotator>>,
    path: PathBuf,
    rename: Box<dyn Send + Fn(&Path) -> io::Result<PathBuf>>,
}

impl RotatingFileLoggerBuilder {
    pub fn new<F>(path: impl AsRef<Path>, rename: F) -> Self
    where
        F: 'static + Send + Fn(&Path) -> io::Result<PathBuf>,
    {
        RotatingFileLoggerBuilder {
            path: path.as_ref().to_path_buf(),
            rotators: vec![],
            rename: Box::new(rename),
        }
    }

    pub fn add_rotator<R: 'static + Rotator>(mut self, rotator: R) -> Self {
        if rotator.is_enabled() {
            self.rotators.push(Box::new(rotator));
        }
        self
    }

    pub fn build(mut self) -> io::Result<RotatingFileLogger> {
        let file = open_log_file(&self.path)?;

        for rotator in self.rotators.iter_mut() {
            rotator.prepare(&file)?;
        }

        Ok(RotatingFileLogger {
            rotators: self.rotators,
            path: self.path,
            rename: self.rename,
            file,
        })
    }
}

impl Write for RotatingFileLogger {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        // Updates all roators' states.
        for rotator in self.rotators.iter_mut() {
            rotator.on_write(bytes)?;
        }
        self.file.write(bytes)
    }

    fn flush(&mut self) -> io::Result<()> {
        for rotator in self.rotators.iter() {
            if rotator.should_rotate() {
                self.file.flush()?;

                let new_path = (self.rename)(&self.path)?;
                fs::rename(&self.path, &new_path)?;
                self.file = open_log_file(&self.path)?;

                // Updates all roators' states.
                for rotator in self.rotators.iter_mut() {
                    rotator.on_rotate()?;
                }

                return Ok(());
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
    rotation_timespan: ReadableDuration,
    next_rotation_time: Option<SystemTime>,
}

impl RotateByTime {
    pub fn new(rotation_timespan: ReadableDuration) -> Self {
        Self {
            rotation_timespan,
            next_rotation_time: None,
        }
    }

    fn next_rotation_time(begin: SystemTime, duration: Duration) -> io::Result<SystemTime> {
        begin
            .checked_add(duration)
            .ok_or_else(|| Error::new(ErrorKind::Other, "Next rotation time is out of range."))
    }
}

impl Rotator for RotateByTime {
    fn is_enabled(&self) -> bool {
        !self.rotation_timespan.is_zero()
    }

    fn prepare(&mut self, file: &File) -> io::Result<()> {
        // Try to get the creation time first,
        // if fail to acquire, try to acquire access time.
        let metadata = file.metadata()?;
        let birth = metadata.created().or_else(|_| metadata.accessed())?;
        self.next_rotation_time = Some(Self::next_rotation_time(birth, self.rotation_timespan.0)?);
        Ok(())
    }

    fn should_rotate(&self) -> bool {
        assert!(self.next_rotation_time.is_some());
        Local::now() > DateTime::<Local>::from(self.next_rotation_time.unwrap())
    }

    fn on_write(&mut self, _: &[u8]) -> io::Result<()> {
        Ok(())
    }

    fn on_rotate(&mut self) -> io::Result<()> {
        assert!(self.next_rotation_time.is_some());
        self.next_rotation_time = Some(Self::next_rotation_time(
            SystemTime::now(),
            self.rotation_timespan.0,
        )?);
        Ok(())
    }
}

pub struct RotateBySize {
    rotation_size: ReadableSize,
    file_size: u64,
}

impl RotateBySize {
    pub fn new(rotation_size: ReadableSize) -> Self {
        RotateBySize {
            rotation_size,
            file_size: 0,
        }
    }
}

impl Rotator for RotateBySize {
    fn is_enabled(&self) -> bool {
        self.rotation_size.0 != 0
    }

    fn prepare(&mut self, file: &File) -> io::Result<()> {
        self.file_size = file.metadata()?.len();
        Ok(())
    }

    fn should_rotate(&self) -> bool {
        self.file_size > self.rotation_size.0
    }

    fn on_write(&mut self, data: &[u8]) -> io::Result<()> {
        self.file_size += data.len() as u64;
        Ok(())
    }

    fn on_rotate(&mut self) -> io::Result<()> {
        self.file_size = 0;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

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
        let mut new_path = path.as_ref().to_path_buf().into_os_string();
        new_path.push(suffix);

        Ok(PathBuf::from(new_path))
    }

    fn rename_fail() -> io::Result<PathBuf> {
        Err(Error::from(ErrorKind::NotFound))
    }

    #[test]
    fn test_should_rotate_by_time() {
        let tmp_dir = TempDir::new().unwrap();
        let path = tmp_dir.path().join("test_should_rotate_by_time.log");
        let suffix = ".backup";

        // Set the last modification time to 1 minute before.
        let last_modified = SystemTime::now()
            .checked_sub(Duration::from_secs(60))
            .unwrap()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs();

        // Create a file.
        open_log_file(path.clone()).unwrap();

        // Modify last_modified time.
        let accessed = last_modified;
        utime::set_file_times(path.clone(), accessed, last_modified).unwrap();

        let mut logger = RotatingFileLoggerBuilder::new(path.clone(), move |path| {
            rename_with_subffix(path, suffix)
        })
        .add_rotator(RotateByTime::new(ReadableDuration(Duration::from_secs(30))))
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
        let path = tmp_dir.path().join("test_should_not_rotate_by_time.log");
        let suffix = ".backup";

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
        let accessed = last_modified;
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
        let path = tmp_dir.path().join("test_should_rotate_by_size.log");
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
    fn test_update_rotate_by_size() {
        let tmp_dir = TempDir::new().unwrap();
        let path = tmp_dir.path().join("test_update_rotate_by_size.log");
        let suffix = ".backup";

        // Set the last modification time to 1 minute before.
        let last_modified = SystemTime::now()
            .checked_sub(Duration::from_secs(120))
            .unwrap()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs();

        // Create a file.
        open_log_file(path.clone()).unwrap();

        // Modify last_modified time.
        let accessed = last_modified;
        utime::set_file_times(path.clone(), accessed, last_modified).unwrap();

        let mut logger = RotatingFileLoggerBuilder::new(path.clone(), move |path| {
            rename_with_subffix(path, suffix)
        })
        .add_rotator(RotateByTime::new(ReadableDuration(Duration::from_secs(60))))
        .add_rotator(RotateBySize::new(ReadableSize::kb(1)))
        .build()
        .unwrap();

        let mut new_path = PathBuf::from(path).into_os_string();
        new_path.push(suffix);

        logger.write_all(&[0xff; 2048]).unwrap();
        // Triggers rotate by time.
        logger.flush().unwrap();

        assert!(file_exists(new_path));

        // Should update `RotateBySize`'s state.
        assert_eq!(logger.rotators[1].should_rotate(), false);
    }

    #[test]
    fn test_update_rotate_by_time() {
        let tmp_dir = TempDir::new().unwrap();
        let path = tmp_dir.path().join("test_update_rotate_by_time.log");
        let suffix = ".backup";

        // Set the last modification time to 1 minute before.
        let last_modified = SystemTime::now()
            .checked_sub(Duration::from_secs(120))
            .unwrap()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs();

        // Create a file.
        open_log_file(path.clone()).unwrap();

        // Modify last_modified time.
        let accessed = last_modified;
        utime::set_file_times(path.clone(), accessed, last_modified).unwrap();

        let mut logger = RotatingFileLoggerBuilder::new(path.clone(), move |path| {
            rename_with_subffix(path, suffix)
        })
        .add_rotator(RotateBySize::new(ReadableSize::kb(1)))
        .add_rotator(RotateByTime::new(ReadableDuration(Duration::from_secs(60))))
        .build()
        .unwrap();

        let mut new_path = PathBuf::from(path).into_os_string();
        new_path.push(suffix);

        logger.write_all(&[0xff; 2048]).unwrap();
        // Triggers rotate by size.
        logger.flush().unwrap();

        assert!(file_exists(new_path.clone()));

        // Should update `RotateByTime`'s state.
        assert_eq!(logger.rotators[1].should_rotate(), false);

        logger.write_all(&[0xff; 2048]).unwrap();
        assert_eq!(logger.rotators[1].should_rotate(), false);
        assert!(file_exists(new_path));
    }

    #[test]
    fn test_failing_to_rotate_file_will_not_cause_panic() {
        let tmp_dir = TempDir::new().unwrap();
        let path = tmp_dir.path().join("test_no_panic.log");
        let suffix = ".backup";

        let mut logger = RotatingFileLoggerBuilder::new(path.clone(), |_| rename_fail())
            .add_rotator(RotateBySize::new(ReadableSize::kb(1)))
            .build()
            .unwrap();

        let mut new_path = PathBuf::from(path).into_os_string();
        new_path.push(suffix);

        // Rename failed.
        logger.write_all(&[0xff; 1025]).unwrap();
        assert!(logger.flush().is_err());

        // dropping the logger still should not panic.
        drop(logger);
    }
}
