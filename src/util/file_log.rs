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

use std::time::{SystemTime, Duration, UNIX_EPOCH};
use time::{self, Timespec, Tm};
use std::fs::{self, OpenOptions, File};
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

fn compute_rollover_time(tm: Tm) -> Tm {
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
        try!(fs::create_dir_all(parent))
    }
    OpenOptions::new()
        .append(true)
        .create(true)
        .open(path)
}

struct RotatingFileLoggerCore {
    rollover_time: Tm,
    file_path: String,
    file: File,
}

impl RotatingFileLoggerCore {
    fn new(path: &str) -> io::Result<RotatingFileLoggerCore> {
        let file = try!(open_log_file(path));
        let file_attr = fs::metadata(path).unwrap();
        let file_modified_time = file_attr.modified().unwrap();
        let rollover_time = compute_rollover_time(systemtime_to_tm(file_modified_time));
        let ret = RotatingFileLoggerCore {
            rollover_time: rollover_time,
            file_path: path.to_string(),
            file: file,
        };
        Ok(ret)
    }

    fn open(&mut self) {
        self.file = open_log_file(&self.file_path).unwrap()
    }

    fn should_rollover(&mut self) -> bool {
        time::now() > self.rollover_time
    }

    fn do_rollover(&mut self) {
        self.close();
        let mut s = self.file_path.clone();
        s.push_str(".");
        s.push_str(&time::strftime("%Y%m%d", &one_day_before(self.rollover_time)).unwrap());
        fs::rename(&self.file_path, &s).unwrap();
        self.update_rollover_time();
        self.open()
    }

    fn update_rollover_time(&mut self) {
        let now = time::now();
        self.rollover_time = compute_rollover_time(now);
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
    pub fn new(file_path: &str) -> io::Result<RotatingFileLogger> {
        let core = try!(RotatingFileLoggerCore::new(file_path));
        let ret = RotatingFileLogger { core: Mutex::new(core) };
        Ok(ret)
    }
}

impl LogWriter for RotatingFileLogger {
    fn write(&self, args: Arguments) {
        let mut core = self.core.lock().unwrap();
        if core.should_rollover() {
            core.do_rollover()
        };
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
    extern crate log;
    extern crate rand;
    extern crate utime;
    use time::{self, Timespec};
    use std::io::prelude::*;
    use std::fs::OpenOptions;
    use std::path::Path;
    use tempdir::TempDir;
    use super::{RotatingFileLoggerCore, ONE_DAY_SECONDS};

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
    fn test_rotating_file_logger() {
        let tmp_dir = TempDir::new("").unwrap();
        let log_file =
            tmp_dir.path().join("test_rotating_file_logger.log").to_str().unwrap().to_string();
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
        let mut core = RotatingFileLoggerCore::new(&log_file).unwrap();
        assert!(core.should_rollover());
        core.do_rollover();
        // check the rotated file exist
        let mut rotated_file = log_file.clone();
        rotated_file.push_str(".");
        let file_suffix_time =
            super::one_day_before(super::compute_rollover_time(time::at(one_day_ago)));
        rotated_file.push_str(&time::strftime("%Y%m%d", &file_suffix_time).unwrap());
        assert!(file_exists(&rotated_file));
        assert!(!core.should_rollover());
    }
}
