// Copyright 2016 PingCAP, Inc.
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

use std::time::{Duration, SystemTime, UNIX_EPOCH};
use time::{self, Timespec, Tm};
use std::fs::{self, File, OpenOptions};
use std::io::{self, Write};
use std::fmt::Arguments;
use std::path::Path;
use std::sync::Mutex;

use super::logger::LogWriter;

const ONE_DAY_SECONDS: u64 = 60 * 60 * 24;

fn systemtime_to_tm(t: SystemTime) -> Tm {
    let duration = t.duration_since(UNIX_EPOCH).unwrap();
    let spec = Timespec::new(duration.as_secs() as i64, duration.subsec_nanos() as i32);
    time::at(spec)
}

fn compute_rotate_time(tm: Tm) -> Tm {
    let day_start_tm = Tm {
        tm_hour: 0,
        tm_min: 0,
        tm_sec: 0,
        tm_nsec: 0,
        ..tm
    };
    let duration = time::Duration::from_std(Duration::new(ONE_DAY_SECONDS, 0)).unwrap();
    (day_start_tm.to_utc() + duration).to_local()
}

/// Returns a Tm at the time one day before the given Tm.
/// It expects the argument `tm` to be in local timezone. The resulting Tm is in local timezone.
fn one_day_before(tm: Tm) -> Tm {
    let duration = time::Duration::from_std(Duration::new(ONE_DAY_SECONDS, 0)).unwrap();
    (tm.to_utc() - duration).to_local()
}

fn open_log_file(path: &str) -> io::Result<File> {
    let p = Path::new(path);
    let parent = p.parent().unwrap();
    if !parent.is_dir() {
        fs::create_dir_all(parent)?
    }
    OpenOptions::new().append(true).create(true).open(path)
}

#[derive(Clone, Copy)]
enum RotateType {
    ByDay(Tm),
    BySize(usize),
}

struct RotatingFileLoggerCore {
    rotate_type: RotateType,
    file_path: String,
    file: File,
}

impl RotatingFileLoggerCore {
    fn new(path: &str, rotate_type: RotateType) -> io::Result<RotatingFileLoggerCore> {
        let file = open_log_file(path)?;

        let mut ret = RotatingFileLoggerCore {
            rotate_type: rotate_type,
            file_path: path.to_string(),
            file: file,
        };
        match rotate_type {
            RotateType::ByDay(_) => {
                let file_attr = fs::metadata(path).unwrap();
                let file_modified_time = file_attr.modified().unwrap();
                let rotate_time = compute_rotate_time(systemtime_to_tm(file_modified_time));
                ret.rotate_type = RotateType::ByDay(rotate_time);
                Ok(ret)
            }
            _ => Ok(ret),
        }
    }

    fn open(&mut self) {
        self.file = open_log_file(&self.file_path).unwrap()
    }

    fn should_rotate(&mut self) -> bool {
        match self.rotate_type {
            RotateType::ByDay(t) => time::now() > t,
            RotateType::BySize(s) => match fs::metadata(&self.file_path) {
                Ok(metadata) => metadata.len() as usize >= s,
                _ => false,
            },
        }
    }

    fn do_rotate(&mut self, now: Option<Tm> /* this parameter used in test*/) {
        self.close();
        let mut s = self.file_path.clone();
        s.push_str(".");
        match self.rotate_type {
            RotateType::ByDay(t) => {
                s.push_str(&time::strftime("%Y%m%d", &one_day_before(t)).unwrap());
                self.update_rotate_time();
            }
            RotateType::BySize(_) => {
                let now = match now {
                    Some(now) => now,
                    None => time::now(),
                };
                s.push_str(&time::strftime("%Y%m%d_%H%M%S", &now).unwrap());
            }
        }
        fs::rename(&self.file_path, &s).unwrap();
        self.open()
    }

    fn update_rotate_time(&mut self) {
        let now = time::now();
        self.rotate_type = RotateType::ByDay(compute_rotate_time(now));
    }

    fn close(&mut self) {
        self.file.flush().unwrap()
    }
}

/// A log implemetation which writes to file and rotates by day.
pub struct RotatingFileLogger {
    core: Mutex<RotatingFileLoggerCore>,
}

impl RotatingFileLogger {
    pub fn new(file_path: &str, rotate_size: Option<usize>) -> io::Result<RotatingFileLogger> {
        let rotate_type = match rotate_size {
            Some(s) => RotateType::BySize(s),
            None => RotateType::ByDay(time::now()),
        };

        let core = RotatingFileLoggerCore::new(file_path, rotate_type)?;
        let ret = RotatingFileLogger {
            core: Mutex::new(core),
        };
        Ok(ret)
    }
}

impl LogWriter for RotatingFileLogger {
    fn write(&self, args: Arguments) {
        let mut core = self.core.lock().unwrap();
        if core.should_rotate() {
            core.do_rotate(None);
        }
        let _ = core.file.write_fmt(args);
    }
}

impl Drop for RotatingFileLogger {
    fn drop(&mut self) {
        let mut core = self.core.lock().unwrap();
        core.close()
    }
}

#[cfg(test)]
mod tests {
    use time::{self, Timespec};
    use std::io::prelude::*;
    use std::fs::OpenOptions;
    use std::path::Path;

    use tempdir::TempDir;
    use utime;

    use super::{RotateType, RotatingFileLoggerCore, ONE_DAY_SECONDS};

    #[test]
    fn test_one_day_before() {
        let tm = time::strptime("2016-08-30", "%Y-%m-%d").unwrap().to_local();
        let one_day_ago = time::strptime("2016-08-29", "%Y-%m-%d").unwrap().to_local();
        assert_eq!(one_day_ago, super::one_day_before(tm));
    }

    fn file_exists(file: &str) -> bool {
        let path = Path::new(file);
        path.exists() && path.is_file()
    }

    #[test]
    fn test_rotating_file_logger_by_time() {
        let tmp_dir = TempDir::new("").unwrap();
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
        let ts = time::now().to_timespec();
        let one_day_ago = Timespec::new(ts.sec - ONE_DAY_SECONDS as i64, ts.nsec);
        let time_in_sec = one_day_ago.sec as u64;
        utime::set_file_times(&log_file, time_in_sec, time_in_sec).unwrap();
        // initialize the logger
        let mut core =
            RotatingFileLoggerCore::new(&log_file, RotateType::ByDay(time::now())).unwrap();
        assert!(core.should_rotate());
        core.do_rotate(None);
        // check the rotated file exist
        let mut rotated_file = log_file.clone();
        rotated_file.push_str(".");
        let file_suffix_time =
            super::one_day_before(super::compute_rotate_time(time::at(one_day_ago)));
        rotated_file.push_str(&time::strftime("%Y%m%d", &file_suffix_time).unwrap());
        assert!(file_exists(&rotated_file));
        assert!(!core.should_rotate());
    }
    #[test]
    fn test_rorating_file_logger_by_size() {
        let tmp_dir = TempDir::new("rorate_by_size").unwrap();
        let log_file = tmp_dir
            .path()
            .join("test_rotating_file_logger.log")
            .to_str()
            .unwrap()
            .to_string();

        // file size == rotate size
        {
            let mut file = OpenOptions::new()
                .append(true)
                .create(true)
                .open(&log_file)
                .unwrap();
            file.write_all(b"abcdefg").unwrap();
        }
        // initialize the logger
        let mut core = RotatingFileLoggerCore::new(&log_file, RotateType::BySize(7)).unwrap();
        assert!(core.should_rotate());
        let t = time::now();
        core.do_rotate(Some(t));
        // check the rotated file exist
        let mut rotated_file = log_file.clone();
        rotated_file.push_str(".");
        rotated_file.push_str(&time::strftime("%Y%m%d_%H%M%S", &t).unwrap());
        assert!(file_exists(&rotated_file));
        assert!(!core.should_rotate());

        // file size > rotate size
        {
            let mut file = OpenOptions::new()
                .append(true)
                .create(true)
                .open(&log_file)
                .unwrap();
            file.write_all(b"abcdefghijklmn").unwrap();
        }
        assert!(core.should_rotate());
    }
}
