// Copyright 2016 TiKV Project Authors. Licensed under Apache-2.0.

use std::{
    fmt::{self, Display, Formatter},
    fs::{self, DirEntry, File, OpenOptions},
    io::{self, Error, ErrorKind, Write},
    path::{Path, PathBuf},
};

use chrono::{DateTime, Duration, Local};

use crate::{
    config::{ReadableDuration, ReadableSize},
    worker::{LazyWorker, Runnable},
};

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
/// After rotating, the original log file would be renamed to "{original name}.{"%Y-%m-%dT%H-%M-%S%.3f"}".
/// Note: log file will *not* be compressed or otherwise modified.
pub struct RotatingFileLogger {
    path: PathBuf,
    file: File,
    rename: Box<dyn Send + Fn(&Path) -> io::Result<PathBuf>>,
    rotators: Vec<Box<dyn Rotator>>,
    archive_worker: LazyWorker<Task>,
}

/// Builder for `RotatingFileLogger`.
pub struct RotatingFileLoggerBuilder {
    rotators: Vec<Box<dyn Rotator>>,
    path: PathBuf,
    rename: Box<dyn Send + Fn(&Path) -> io::Result<PathBuf>>,
    max_backups: usize,
    max_days: ReadableDuration,
}

impl RotatingFileLoggerBuilder {
    pub fn new<F>(
        path: impl AsRef<Path>,
        rename: F,
        max_backups: usize,
        max_days: ReadableDuration,
    ) -> Self
    where
        F: 'static + Send + Fn(&Path) -> io::Result<PathBuf>,
    {
        RotatingFileLoggerBuilder {
            path: path.as_ref().to_path_buf(),
            rotators: vec![],
            rename: Box::new(rename),
            max_backups,
            max_days,
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
        let mut worker = LazyWorker::new("archive-worker");
        assert!(worker.start(Runner::new(&self.path, self.max_backups, self.max_days)));
        worker.scheduler().schedule(Task::Archive).unwrap();

        for rotator in self.rotators.iter_mut() {
            rotator.prepare(&file)?;
        }

        Ok(RotatingFileLogger {
            rotators: self.rotators,
            path: self.path,
            rename: self.rename,
            file,
            archive_worker: worker,
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

                // Updates all rotators' states.
                for rotator in self.rotators.iter_mut() {
                    rotator.on_rotate()?;
                }

                self.archive_worker
                    .scheduler()
                    .schedule(Task::Archive)
                    .unwrap();

                return Ok(());
            }
        }
        self.file.flush()
    }
}

impl Drop for RotatingFileLogger {
    fn drop(&mut self) {
        let _ = self.file.flush();
        self.archive_worker.stop();
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

#[derive(Debug)]
pub enum Task {
    Archive,
}

impl Display for Task {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "{:?}", self)
    }
}

struct LogInfo {
    f: DirEntry,
    dt: DateTime<Local>,
}

fn dt_from_file_name(path: &Path, prefix: &str) -> Option<DateTime<Local>> {
    let file_name_with_dt = path.file_stem().unwrap();
    let mut dt = file_name_with_dt
        .to_str()
        .unwrap()
        .to_string()
        .replace(prefix, "");
    if !dt.is_empty() {
        // We must add *a timezone* as `DateTime::parse_from_str` requires it
        dt.push_str(&Local::now().offset().to_string());
        match DateTime::parse_from_str(&dt.as_str()[1..], "%Y-%m-%dT%H-%M-%S%.3f %z") {
            Ok(t) => return Some(t.with_timezone(&Local)),
            Err(_) => return None,
        }
    }
    None
}

pub struct Runner {
    log_dir: PathBuf,
    file_name: String,
    max_backups: usize,
    max_days: ReadableDuration,
}

impl Runner {
    pub fn new<P: AsRef<Path>>(path: P, max_backups: usize, max_days: ReadableDuration) -> Runner {
        let mut path = path.as_ref().to_owned();
        let file_name = path.file_stem().unwrap().to_str().unwrap().to_owned();
        assert!(path.pop());
        Runner {
            log_dir: path,
            file_name,
            max_backups,
            max_days,
        }
    }

    fn list_old_logs(&self) -> Result<Vec<LogInfo>, Error> {
        let mut logs = Vec::new();
        for f in fs::read_dir(&self.log_dir)? {
            let f = f?;
            if f.file_type()?.is_file() {
                if let Some(dt) = dt_from_file_name(f.path().as_path(), &self.file_name) {
                    logs.push(LogInfo { f, dt });
                }
            }
        }
        logs.sort_by(|l1, l2| l2.dt.cmp(&l1.dt));
        Ok(logs)
    }
}

impl Runnable for Runner {
    type Task = Task;

    fn run(&mut self, _: Task) {
        if self.max_backups == 0 && self.max_days.is_zero() {
            return;
        }
        let mut logs = self.list_old_logs().unwrap();
        let mut remove = Vec::new();
        if self.max_backups > 0 && self.max_backups < logs.len() {
            remove = logs.split_off(self.max_backups);
        }
        if !self.max_days.is_zero() {
            let threshold = Local::now() - Duration::from_std(self.max_days.0).unwrap();
            while let Some(log) = logs.last() {
                if log.dt < threshold {
                    remove.push(logs.pop().unwrap());
                } else {
                    break;
                }
            }
        }
        for log in remove {
            fs::remove_file(log.f.path())
                .unwrap_or_else(|e| error!("achieve log: {:?} failed, err: {}", log.f.path(), e));
        }
    }
}

#[cfg(test)]
mod tests {
    use std::{ffi::OsStr, ops::Add, time::Duration};

    use tempfile::TempDir;

    use super::*;

    fn file_exists(file: impl AsRef<Path>) -> bool {
        let path = file.as_ref();
        path.exists() && path.is_file()
    }

    fn rename_with_suffix(
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
    fn test_rotate_by_size() {
        let tmp_dir = TempDir::new().unwrap();
        let path = tmp_dir.path().join("test_should_rotate_by_size.log");
        let suffix = ".backup";

        let mut logger = RotatingFileLoggerBuilder::new(
            path.clone(),
            move |path| rename_with_suffix(path, suffix),
            0,
            ReadableDuration(Duration::from_secs(0)),
        )
        .add_rotator(RotateBySize::new(ReadableSize::kb(1)))
        .build()
        .unwrap();

        let mut new_path = path.into_os_string();
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

        let mut logger = RotatingFileLoggerBuilder::new(
            path.clone(),
            |_| rename_fail(),
            0,
            ReadableDuration(Duration::from_secs(0)),
        )
        .add_rotator(RotateBySize::new(ReadableSize::kb(1)))
        .build()
        .unwrap();

        let mut new_path = path.into_os_string();
        new_path.push(suffix);

        // Rename failed.
        logger.write_all(&[0xff; 1025]).unwrap();
        assert!(logger.flush().is_err());

        // dropping the logger still should not panic.
        drop(logger);
    }

    fn rename_by_timestamp(path: &Path) -> io::Result<PathBuf> {
        let mut new_path = path.parent().unwrap().to_path_buf();
        let mut new_fname = path.file_stem().unwrap().to_os_string();
        let dt = Local::now().format("%Y-%m-%dT%H-%M-%S%.3f");
        new_fname.push(format!("-{}", dt));
        if let Some(ext) = path.extension() {
            new_fname.push(".");
            new_fname.push(ext);
        };
        new_path.push(new_fname);
        Ok(new_path)
    }

    fn rename_with_old_timestamp(path: &Path, t: ReadableDuration) -> io::Result<PathBuf> {
        let mut new_path = path.parent().unwrap().to_path_buf();
        let mut new_fname = path.file_stem().unwrap().to_os_string();
        let dt = (Local::now() - chrono::Duration::from_std(t.0).unwrap())
            .format("%Y-%m-%dT%H-%M-%S%.3f");
        new_fname.push(format!("-{}", dt));
        if let Some(ext) = path.extension() {
            new_fname.push(".");
            new_fname.push(ext);
        };
        new_path.push(new_fname);
        Ok(new_path)
    }

    fn logs_nr(path: &Path) -> usize {
        let log_dir = path.parent().unwrap().as_os_str();
        let mut i = 0;
        for f in fs::read_dir(log_dir).unwrap() {
            let f = f.unwrap();
            if f.file_type().unwrap().is_file() {
                i += 1;
            }
        }
        i
    }

    #[test]
    fn test_archive_by_max_backups_at_startup() {
        let tmp_dir = TempDir::new().unwrap();
        let path = tmp_dir
            .path()
            .join("test_should_archive_by_max_backups_at_startup.log");
        for _ in 1..10 {
            let new_path = rename_by_timestamp(&path).unwrap();
            open_log_file(new_path.clone()).unwrap();
            // Avoid generating the same file name when rotating
            std::thread::sleep(std::time::Duration::from_millis(5));
        }

        let logger = RotatingFileLoggerBuilder::new(
            path.clone(),
            rename_by_timestamp,
            3,
            ReadableDuration::days(0),
        )
        .build()
        .unwrap();

        std::thread::sleep(std::time::Duration::from_secs(1));
        assert_eq!(4, logs_nr(&path));
        drop(logger);
    }

    #[test]
    fn test_archive_by_max_backups_after_rotate() {
        let tmp_dir = TempDir::new().unwrap();
        let path = tmp_dir
            .path()
            .join("test_should_archive_by_max_backups.log");
        let mut logger = RotatingFileLoggerBuilder::new(
            path.clone(),
            rename_by_timestamp,
            3,
            ReadableDuration::days(0),
        )
        .add_rotator(RotateBySize::new(ReadableSize::kb(1)))
        .build()
        .unwrap();

        for _ in 0..20 {
            logger.write_all(&[0xff; 1025]).unwrap();
            logger.flush().unwrap();
            // Avoid generating the same file name when rotating
            std::thread::sleep(std::time::Duration::from_millis(10));
        }

        std::thread::sleep(std::time::Duration::from_secs(3));
        assert_eq!(4, logs_nr(&path));
    }

    #[test]
    fn test_archive_by_max_days_at_startup() {
        let tmp_dir = TempDir::new().unwrap();
        let path = tmp_dir
            .path()
            .join("test_should_archive_by_max_days_at_startup.log");
        for i in 1..10 {
            let new_path = rename_with_old_timestamp(
                &path,
                ReadableDuration::days(i).add(ReadableDuration::hours(12)),
            )
            .unwrap();
            open_log_file(new_path.clone()).unwrap();
            // Avoid generating the same file name when rotating
            std::thread::sleep(std::time::Duration::from_millis(5));
        }

        let logger = RotatingFileLoggerBuilder::new(
            path.clone(),
            rename_by_timestamp,
            0,
            ReadableDuration::days(3),
        )
        .build()
        .unwrap();

        std::thread::sleep(std::time::Duration::from_secs(1));
        assert_eq!(3, logs_nr(&path));
        drop(logger);
    }

    #[test]
    fn test_archive_by_max_days_after_rotate() {
        let tmp_dir = TempDir::new().unwrap();
        let path = tmp_dir
            .path()
            .join("test_should_archive_by_max_days_after_rotate.log");

        let mut logger = RotatingFileLoggerBuilder::new(
            path.clone(),
            rename_by_timestamp,
            0,
            ReadableDuration::days(3),
        )
        .add_rotator(RotateBySize::new(ReadableSize::kb(1)))
        .build()
        .unwrap();

        for i in 1..10 {
            let new_path = rename_with_old_timestamp(
                &path,
                ReadableDuration::days(i).add(ReadableDuration::hours(12)),
            )
            .unwrap();
            open_log_file(new_path.clone()).unwrap();
            // Avoid generating the same file name when rotating
            std::thread::sleep(std::time::Duration::from_millis(100));
        }
        assert_eq!(10, logs_nr(&path));

        logger.write_all(&[0xff; 1025]).unwrap();
        logger.flush().unwrap();
        std::thread::sleep(std::time::Duration::from_secs(1));
        assert_eq!(4, logs_nr(&path));
    }

    #[test]
    fn test_archive_with_illegal_log_name_will_not_cause_panic() {
        let tmp_dir = TempDir::new().unwrap();
        let path = tmp_dir.path().join("test_archive_will_not_cause_panic.log");
        let new_path1 = tmp_dir
            .path()
            .join("test_archive_will_not_cause_panic.T.log");
        open_log_file(new_path1).unwrap();
        let new_path2 = tmp_dir
            .path()
            .join("test_archive_will_not_cause_panic.2019-08-23T18:11:02.123.log");
        open_log_file(new_path2).unwrap();
        let new_path3 = tmp_dir
            .path()
            .join("test_panic.2019-08-23T18-11-02.123.log");
        open_log_file(new_path3).unwrap();
        let new_path4 = tmp_dir.path().join("test_panic.log.2");
        open_log_file(new_path4).unwrap();

        RotatingFileLoggerBuilder::new(path, rename_by_timestamp, 3, ReadableDuration::days(0))
            .build()
            .unwrap();
        std::thread::sleep(std::time::Duration::from_secs(1));
    }

    #[test]
    fn test_get_datetime_from_filename() {
        let tmp_dir = TempDir::new().unwrap();
        let path = tmp_dir.path().join("t.g.d.f.2019-08-23T18-11-02.123.log");
        let dt = dt_from_file_name(&path, "t.g.d.f");
        assert!(dt.is_some());
        let path = tmp_dir.path().join("t.g.d.f.2019-08-23T18:11:02.123.log");
        let dt = dt_from_file_name(&path, "t.g.d.f");
        assert!(dt.is_none());
        let path = tmp_dir
            .path()
            .join("t.g.d.f.2019-08-23T18-11-02.123.log.log");
        let dt = dt_from_file_name(&path, "t.g.d.f");
        assert!(dt.is_none());
        let path = tmp_dir
            .path()
            .join("t.g.d.f.2019-08-23T18-11-02.123+00:00.log");
        let dt = dt_from_file_name(&path, "t.g.d.f");
        assert!(dt.is_none());
        let path = tmp_dir
            .path()
            .join("2019-08-23T18-11-02.123.t.g.d.f.2019-08-23T18-11-02.123.log");
        let dt = dt_from_file_name(&path, "t.g.d.f");
        assert!(dt.is_none());
        let path = tmp_dir
            .path()
            .join("2019-08-23T18-11-02.123.t.g.d.f.2019-08-23T18-11-02.123.log");
        let dt = dt_from_file_name(&path, "2019-08-23T18-11-02.123.t.g.d.f");
        assert!(dt.is_some());
        let path = tmp_dir.path().join("t.g.d.f.2019-08-23.log");
        let dt = dt_from_file_name(&path, "t.g.d.f");
        assert!(dt.is_none());
        let path = tmp_dir.path().join("t.g.d.f.log");
        let dt = dt_from_file_name(&path, "t.g.d.f");
        assert!(dt.is_none());
    }
}
