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

/// This FileLogger rotates logs according to a time span.
/// After rotating, the original log file would be renamed to "{original name}.{%Y-%m-%d-%H:%M:%S}"
/// Note: log file will *not* be compressed or otherwise modified.
pub struct RotatingFileLogger {
    rotation_timespan: Duration,
    next_rotation_time: DateTime<Utc>,
    file_path: PathBuf,
    file: File,
}

impl RotatingFileLogger {
    pub fn new(file_path: impl AsRef<Path>, rotation_timespan: Duration) -> io::Result<Self> {
        let file_path = file_path.as_ref().to_path_buf();
        let file = open_log_file(&file_path)?;
        let file_attr = fs::metadata(&file_path)?;
        let file_modified_time = file_attr.modified().unwrap().into();
        let next_rotation_time = compute_rotation_time(&file_modified_time, rotation_timespan);
        Ok(Self {
            next_rotation_time,
            file_path,
            rotation_timespan,
            file,
        })
    }

    fn should_rotate(&mut self) -> bool {
        Utc::now() > self.next_rotation_time
    }

    fn rename(&self) -> io::Result<()> {
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
                Ok(())
            }
        });
        // Note: renaming files while they're open only works on Linux and macOS.
        let new_path = rotation_file_path_with_timestamp(&self.file_path, &Utc::now());
        fs::rename(&self.file_path, new_path)
    }

    /// Rotates the current file and updates the next rotation time.
    fn rotate(&mut self) -> io::Result<()> {
        self.flush()?;

        match self.rename() {
            Ok(_) => Ok(()),
            Err(ref e) if e.kind() == ErrorKind::NotFound => Ok(()),
            Err(e) => Err(e),
        }?;

        let new_file = open_log_file(&self.file_path)?;
        self.update_rotation_time();
        self.file = new_file;
        Ok(())
    }

    /// Updates the next rotation time.
    fn update_rotation_time(&mut self) {
        let now = Utc::now();
        self.next_rotation_time = compute_rotation_time(&now, self.rotation_timespan);
    }

    /// Flushes the log file, without rotation.
    fn flush(&mut self) -> io::Result<()> {
        self.file.flush()
    }
}

impl Write for RotatingFileLogger {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        self.file.write(bytes)
    }

    fn flush(&mut self) -> io::Result<()> {
        if self.should_rotate() {
            self.rotate()?;
        };
        self.file.flush()
    }
}

impl Drop for RotatingFileLogger {
    fn drop(&mut self) {
        let _ = self.file.flush();
    }
}

#[cfg(test)]
mod tests {
    use std::fs::OpenOptions;
    use std::io::prelude::*;
    use std::path::Path;

    use chrono::{Duration, Utc};
    use tempfile::TempDir;
    use utime;

    use super::{rotation_file_path_with_timestamp, RotatingFileLogger};

    fn file_exists(file: impl AsRef<Path>) -> bool {
        let path = file.as_ref();
        path.exists() && path.is_file()
    }

    #[test]
    fn test_rotating_file_logger() {
        let tmp_dir = TempDir::new().unwrap();
        let log_file = tmp_dir
            .path()
            .join("test_rotating_file_logger.log")
            .to_str()
            .unwrap()
            .to_string();
        // create a file with mtime == one day ago
        {
            let mut file = OpenOptions::new()
                .append(true)
                .create(true)
                .open(&log_file)
                .unwrap();
            file.write_all(b"hello world!").unwrap();
        }
        let now = Utc::now();
        let one_day = Duration::days(1);
        let one_day_ago = now - one_day;
        let one_day_ago_ts = one_day_ago.timestamp() as u64;
        utime::set_file_times(&log_file, one_day_ago_ts, one_day_ago_ts).unwrap();
        // initialize the logger
        let mut logger = RotatingFileLogger::new(&log_file, one_day).unwrap();
        assert!(logger.should_rotate());
        logger.rotate().unwrap();
        // check the rotated file exist
        let rotated_file = rotation_file_path_with_timestamp(&log_file, &now);
        assert!(file_exists(&rotated_file));
        assert!(!logger.should_rotate());
    }

    #[test]
    fn test_failing_to_rotate_file_will_not_cause_panic() {
        let tmp_dir = TempDir::new().unwrap();
        let log_file = tmp_dir.path().join("test_rotating_file_logger.log");
        let mut logger = RotatingFileLogger::new(&log_file, Duration::days(1)).unwrap();
        // trigger fail point
        fail::cfg("file_log_rename", "return(NotFound)").unwrap();
        logger.rotate().unwrap();
        fail::remove("file_log_rename");
        // dropping the logger still should not panic.
        drop(logger);
    }
}
