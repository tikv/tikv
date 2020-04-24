// Copyright 2016 TiKV Project Authors. Licensed under Apache-2.0.

use std::error::Error;
use std::fmt::{self, Write};
use std::fs;
use std::net::{SocketAddrV4, SocketAddrV6};
use std::ops::{Div, Mul};
use std::path::Path;
use std::str::{self, FromStr};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, RwLock, RwLockReadGuard};
use std::time::Duration;

use serde::de::{self, Unexpected, Visitor};
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use url;

use super::time::Instant;
use crate::slow_log;
use configuration::ConfigValue;

quick_error! {
    #[derive(Debug)]
    pub enum ConfigError {
        Limit(msg: String) {
            description(msg)
            display("{}", msg)
        }
        Address(msg: String) {
            description(msg)
            display("config address error: {}", msg)
        }
        StoreLabels(msg: String) {
            description(msg)
            display("store label error: {}", msg)
        }
        Value(msg: String) {
            description(msg)
            display("config value error: {}", msg)
        }
        FileSystem(msg: String) {
            description(msg)
            display("config fs: {}", msg)
        }
    }
}

const UNIT: u64 = 1;
const DATA_MAGNITUDE: u64 = 1024;
pub const KB: u64 = UNIT * DATA_MAGNITUDE;
pub const MB: u64 = KB * DATA_MAGNITUDE;
pub const GB: u64 = MB * DATA_MAGNITUDE;

// Make sure it will not overflow.
const TB: u64 = (GB as u64) * (DATA_MAGNITUDE as u64);
const PB: u64 = (TB as u64) * (DATA_MAGNITUDE as u64);

const TIME_MAGNITUDE_1: u64 = 1000;
const TIME_MAGNITUDE_2: u64 = 60;
const TIME_MAGNITUDE_3: u64 = 24;
const MS: u64 = UNIT;
const SECOND: u64 = MS * TIME_MAGNITUDE_1;
const MINUTE: u64 = SECOND * TIME_MAGNITUDE_2;
const HOUR: u64 = MINUTE * TIME_MAGNITUDE_2;
const DAY: u64 = HOUR * TIME_MAGNITUDE_3;

#[derive(Clone, Debug, Copy, PartialEq)]
pub struct ReadableSize(pub u64);

impl From<ReadableSize> for ConfigValue {
    fn from(size: ReadableSize) -> ConfigValue {
        ConfigValue::Size(size.0)
    }
}

impl Into<ReadableSize> for ConfigValue {
    fn into(self) -> ReadableSize {
        if let ConfigValue::Size(s) = self {
            ReadableSize(s)
        } else {
            panic!("expect: ConfigValue::Size, got: {:?}", self);
        }
    }
}

impl ReadableSize {
    pub const fn kb(count: u64) -> ReadableSize {
        ReadableSize(count * KB)
    }

    pub const fn mb(count: u64) -> ReadableSize {
        ReadableSize(count * MB)
    }

    pub const fn gb(count: u64) -> ReadableSize {
        ReadableSize(count * GB)
    }

    pub const fn as_mb(self) -> u64 {
        self.0 / MB
    }
}

impl Div<u64> for ReadableSize {
    type Output = ReadableSize;

    fn div(self, rhs: u64) -> ReadableSize {
        ReadableSize(self.0 / rhs)
    }
}

impl Div<ReadableSize> for ReadableSize {
    type Output = u64;

    fn div(self, rhs: ReadableSize) -> u64 {
        self.0 / rhs.0
    }
}

impl Mul<u64> for ReadableSize {
    type Output = ReadableSize;

    fn mul(self, rhs: u64) -> ReadableSize {
        ReadableSize(self.0 * rhs)
    }
}

impl Serialize for ReadableSize {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let size = self.0;
        let mut buffer = String::new();
        if size == 0 {
            write!(buffer, "{}KiB", size).unwrap();
        } else if size % PB == 0 {
            write!(buffer, "{}PiB", size / PB).unwrap();
        } else if size % TB == 0 {
            write!(buffer, "{}TiB", size / TB).unwrap();
        } else if size % GB as u64 == 0 {
            write!(buffer, "{}GiB", size / GB).unwrap();
        } else if size % MB as u64 == 0 {
            write!(buffer, "{}MiB", size / MB).unwrap();
        } else if size % KB as u64 == 0 {
            write!(buffer, "{}KiB", size / KB).unwrap();
        } else {
            return serializer.serialize_u64(size);
        }
        serializer.serialize_str(&buffer)
    }
}

impl FromStr for ReadableSize {
    type Err = String;

    fn from_str(s: &str) -> Result<ReadableSize, String> {
        let size_str = s.trim();
        if size_str.is_empty() {
            return Err(format!("{:?} is not a valid size.", s));
        }

        if !size_str.is_ascii() {
            return Err(format!("ASCII string is expected, but got {:?}", s));
        }

        // size: digits and '.' as decimal separator
        let size_len = size_str
            .to_string()
            .chars()
            .take_while(|c| char::is_ascii_digit(c) || *c == '.')
            .count();

        // unit: alphabetic characters
        let (size, unit) = size_str.split_at(size_len);

        let unit = match unit.trim() {
            "K" | "KB" | "KiB" => KB,
            "M" | "MB" | "MiB" => MB,
            "G" | "GB" | "GiB" => GB,
            "T" | "TB" | "TiB" => TB,
            "P" | "PB" | "PiB" => PB,
            "B" | "" => UNIT,
            _ => {
                return Err(format!(
                    "only B, KB, KiB, MB, MiB, GB, GiB, TB, TiB, PB, and PiB are supported: {:?}",
                    s
                ));
            }
        };

        match size.parse::<f64>() {
            Ok(n) => Ok(ReadableSize((n * unit as f64) as u64)),
            Err(_) => Err(format!("invalid size string: {:?}", s)),
        }
    }
}

impl<'de> Deserialize<'de> for ReadableSize {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        struct SizeVisitor;

        impl<'de> Visitor<'de> for SizeVisitor {
            type Value = ReadableSize;

            fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
                formatter.write_str("valid size")
            }

            fn visit_i64<E>(self, size: i64) -> Result<ReadableSize, E>
            where
                E: de::Error,
            {
                if size >= 0 {
                    self.visit_u64(size as u64)
                } else {
                    Err(E::invalid_value(Unexpected::Signed(size), &self))
                }
            }

            fn visit_u64<E>(self, size: u64) -> Result<ReadableSize, E>
            where
                E: de::Error,
            {
                Ok(ReadableSize(size))
            }

            fn visit_str<E>(self, size_str: &str) -> Result<ReadableSize, E>
            where
                E: de::Error,
            {
                size_str.parse().map_err(E::custom)
            }
        }

        deserializer.deserialize_any(SizeVisitor)
    }
}

#[derive(Clone, Copy, Debug, PartialEq)]
pub struct ReadableDuration(pub Duration);

impl From<ReadableDuration> for Duration {
    fn from(readable: ReadableDuration) -> Duration {
        readable.0
    }
}

impl From<ReadableDuration> for ConfigValue {
    fn from(duration: ReadableDuration) -> ConfigValue {
        ConfigValue::Duration(duration.0.as_millis() as u64)
    }
}

impl Into<ReadableDuration> for ConfigValue {
    fn into(self) -> ReadableDuration {
        if let ConfigValue::Duration(d) = self {
            ReadableDuration(Duration::from_millis(d))
        } else {
            panic!("expect: ConfigValue::Duration, got: {:?}", self);
        }
    }
}

impl FromStr for ReadableDuration {
    type Err = String;

    fn from_str(dur_str: &str) -> Result<ReadableDuration, String> {
        let dur_str = dur_str.trim();
        if !dur_str.is_ascii() {
            return Err(format!("unexpect ascii string: {}", dur_str));
        }
        let err_msg = "valid duration, only d, h, m, s, ms are supported.".to_owned();
        let mut left = dur_str.as_bytes();
        let mut last_unit = DAY + 1;
        let mut dur = 0f64;
        while let Some(idx) = left.iter().position(|c| b"dhms".contains(c)) {
            let (first, second) = left.split_at(idx);
            let unit = if second.starts_with(b"ms") {
                left = &left[idx + 2..];
                MS
            } else {
                let u = match second[0] {
                    b'd' => DAY,
                    b'h' => HOUR,
                    b'm' => MINUTE,
                    b's' => SECOND,
                    _ => return Err(err_msg),
                };
                left = &left[idx + 1..];
                u
            };
            if unit >= last_unit {
                return Err("d, h, m, s, ms should occur in given order.".to_owned());
            }
            // do we need to check 12h360m?
            let number_str = unsafe { str::from_utf8_unchecked(first) };
            dur += match number_str.trim().parse::<f64>() {
                Ok(n) => n * unit as f64,
                Err(_) => return Err(err_msg),
            };
            last_unit = unit;
        }
        if !left.is_empty() {
            return Err(err_msg);
        }
        if dur.is_sign_negative() {
            return Err("duration should be positive.".to_owned());
        }
        let secs = dur as u64 / SECOND as u64;
        let millis = (dur as u64 % SECOND as u64) as u32 * 1_000_000;
        Ok(ReadableDuration(Duration::new(secs, millis)))
    }
}

impl ReadableDuration {
    pub fn secs(secs: u64) -> ReadableDuration {
        ReadableDuration(Duration::new(secs, 0))
    }

    pub fn millis(millis: u64) -> ReadableDuration {
        ReadableDuration(Duration::new(
            millis / 1000,
            (millis % 1000) as u32 * 1_000_000,
        ))
    }

    pub fn minutes(minutes: u64) -> ReadableDuration {
        ReadableDuration::secs(minutes * 60)
    }

    pub fn hours(hours: u64) -> ReadableDuration {
        ReadableDuration::minutes(hours * 60)
    }

    pub fn days(days: u64) -> ReadableDuration {
        ReadableDuration::hours(days * 24)
    }

    pub fn as_secs(&self) -> u64 {
        self.0.as_secs()
    }

    pub fn as_millis(&self) -> u64 {
        crate::time::duration_to_ms(self.0)
    }

    pub fn is_zero(&self) -> bool {
        self.0.as_nanos() == 0
    }
}

impl fmt::Display for ReadableDuration {
    #[inline]
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut dur = crate::time::duration_to_ms(self.0);
        let mut written = false;
        if dur >= DAY {
            written = true;
            write!(f, "{}d", dur / DAY)?;
            dur %= DAY;
        }
        if dur >= HOUR {
            written = true;
            write!(f, "{}h", dur / HOUR)?;
            dur %= HOUR;
        }
        if dur >= MINUTE {
            written = true;
            write!(f, "{}m", dur / MINUTE)?;
            dur %= MINUTE;
        }
        if dur >= SECOND {
            written = true;
            write!(f, "{}s", dur / SECOND)?;
            dur %= SECOND;
        }
        if dur > 0 {
            written = true;
            write!(f, "{}ms", dur)?;
        }
        if !written {
            write!(f, "0s")?;
        }
        Ok(())
    }
}

impl Serialize for ReadableDuration {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut buffer = String::new();
        write!(buffer, "{}", self).unwrap();
        serializer.serialize_str(&buffer)
    }
}

impl<'de> Deserialize<'de> for ReadableDuration {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        struct DurVisitor;

        impl<'de> Visitor<'de> for DurVisitor {
            type Value = ReadableDuration;

            fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
                formatter.write_str("valid duration")
            }

            fn visit_str<E>(self, dur_str: &str) -> Result<ReadableDuration, E>
            where
                E: de::Error,
            {
                dur_str.parse().map_err(E::custom)
            }
        }

        deserializer.deserialize_str(DurVisitor)
    }
}

pub fn canonicalize_path(path: &str) -> Result<String, Box<dyn Error>> {
    canonicalize_sub_path(path, "")
}

pub fn canonicalize_sub_path(path: &str, sub_path: &str) -> Result<String, Box<dyn Error>> {
    let mut path = Path::new(path).canonicalize()?;
    if !sub_path.is_empty() {
        path = path.join(Path::new(sub_path));
    }
    if path.exists() && path.is_file() {
        return Err(format!("{}/{} is not a directory!", path.display(), sub_path).into());
    }
    Ok(format!("{}", path.display()))
}

pub fn ensure_dir_exist(path: &str) -> Result<(), Box<dyn Error>> {
    if !path.is_empty() {
        let p = Path::new(path);
        if !p.exists() {
            fs::create_dir_all(p)?;
        }
    }
    Ok(())
}

#[cfg(unix)]
pub fn check_max_open_fds(expect: u64) -> Result<(), ConfigError> {
    use std::mem;

    unsafe {
        let mut fd_limit = mem::zeroed();
        let mut err = libc::getrlimit(libc::RLIMIT_NOFILE, &mut fd_limit);
        if err != 0 {
            return Err(ConfigError::Limit("check_max_open_fds failed".to_owned()));
        }
        if fd_limit.rlim_cur >= expect {
            return Ok(());
        }

        let prev_limit = fd_limit.rlim_cur;
        fd_limit.rlim_cur = expect;
        if fd_limit.rlim_max < expect {
            // If the process is not started by privileged user, this will fail.
            fd_limit.rlim_max = expect;
        }
        err = libc::setrlimit(libc::RLIMIT_NOFILE, &fd_limit);
        if err == 0 {
            return Ok(());
        }
        Err(ConfigError::Limit(format!(
            "the maximum number of open file descriptors is too \
             small, got {}, expect greater or equal to {}",
            prev_limit, expect
        )))
    }
}

#[cfg(not(unix))]
pub fn check_max_open_fds(_: u64) -> Result<(), ConfigError> {
    Ok(())
}

#[cfg(target_os = "linux")]
mod check_kernel {
    use std::fs;

    use super::ConfigError;

    // pub for tests.
    pub type Checker = dyn Fn(i64, i64) -> bool;

    // pub for tests.
    pub fn check_kernel_params(
        param_path: &str,
        expect: i64,
        checker: Box<Checker>,
    ) -> Result<(), ConfigError> {
        let buffer = fs::read_to_string(param_path)
            .map_err(|e| ConfigError::Limit(format!("check_kernel_params failed {}", e)))?;

        let got = buffer
            .trim_matches('\n')
            .parse::<i64>()
            .map_err(|e| ConfigError::Limit(format!("check_kernel_params failed {}", e)))?;

        let mut param = String::new();
        // skip 3, ["", "proc", "sys", ...]
        for path in param_path.split('/').skip(3) {
            param.push_str(path);
            param.push('.');
        }
        param.pop();

        if !checker(got, expect) {
            return Err(ConfigError::Limit(format!(
                "kernel parameters {} got {}, expect {}",
                param, got, expect
            )));
        }

        info!("kernel parameters"; "param" => param, "value" => got);
        Ok(())
    }

    /// `check_kernel_params` checks kernel parameters, following are checked so far:
    ///   - `net.core.somaxconn` should be greater or equal to 32768.
    ///   - `net.ipv4.tcp_syncookies` should be 0
    ///   - `vm.swappiness` shoud be 0
    ///
    /// Note that: It works on **Linux** only.
    pub fn check_kernel() -> Vec<ConfigError> {
        let params: Vec<(&str, i64, Box<Checker>)> = vec![
            // Check net.core.somaxconn.
            (
                "/proc/sys/net/core/somaxconn",
                32768,
                Box::new(|got, expect| got >= expect),
            ),
            // Check net.ipv4.tcp_syncookies.
            (
                "/proc/sys/net/ipv4/tcp_syncookies",
                0,
                Box::new(|got, expect| got == expect),
            ),
            // Check vm.swappiness.
            (
                "/proc/sys/vm/swappiness",
                0,
                Box::new(|got, expect| got == expect),
            ),
        ];

        let mut errors = Vec::with_capacity(params.len());
        for (param_path, expect, checker) in params {
            if let Err(e) = check_kernel_params(param_path, expect, checker) {
                errors.push(e);
            }
        }

        errors
    }
}

#[cfg(target_os = "linux")]
pub use self::check_kernel::check_kernel;

#[cfg(not(target_os = "linux"))]
pub fn check_kernel() -> Vec<ConfigError> {
    Vec::new()
}

#[cfg(target_os = "linux")]
mod check_data_dir {
    use libc;
    use std::ffi::{CStr, CString};
    use std::fs;
    use std::path::Path;
    use std::sync::Mutex;

    use super::{canonicalize_path, ConfigError};

    #[derive(Debug, Default)]
    struct FsInfo {
        tp: String,
        opts: String,
        mnt_dir: String,
        fsname: String,
    }

    fn get_fs_info(path: &str, mnt_file: &str) -> Result<FsInfo, ConfigError> {
        lazy_static! {
            // According `man 3 getmntent`, The pointer returned by `getmntent` points
            // to a static area of memory which is overwritten by subsequent calls.
            // So we use a lock to protect it in order to avoid `make dev` fail.
            static ref GETMNTENT_LOCK: Mutex<()> = Mutex::new(());
        }

        let op = "data-dir.fsinfo.get";

        unsafe {
            let _lock = GETMNTENT_LOCK.lock().unwrap();

            let profile = CString::new(mnt_file).unwrap();
            let retype = CString::new("r").unwrap();
            let afile = libc::setmntent(profile.as_ptr(), retype.as_ptr());
            let mut fs = FsInfo::default();
            loop {
                let ent = libc::getmntent(afile);
                if ent.is_null() {
                    break;
                }
                let ent = &*ent;
                let cur_dir = CStr::from_ptr(ent.mnt_dir).to_str().unwrap();
                if path.starts_with(&cur_dir) && cur_dir.len() >= fs.mnt_dir.len() {
                    fs.tp = CStr::from_ptr(ent.mnt_type).to_str().unwrap().to_owned();
                    fs.opts = CStr::from_ptr(ent.mnt_opts).to_str().unwrap().to_owned();
                    fs.fsname = CStr::from_ptr(ent.mnt_fsname).to_str().unwrap().to_owned();
                    fs.mnt_dir = cur_dir.to_owned();
                }
            }

            libc::endmntent(afile);
            if fs.mnt_dir.is_empty() {
                return Err(ConfigError::FileSystem(format!(
                    "{}: path: {:?} not find in mountable",
                    op, path
                )));
            }
            Ok(fs)
        }
    }

    fn get_rotational_info(fsname: &str) -> Result<String, ConfigError> {
        let op = "data-dir.rotation.get";
        // get device path
        let device = match fs::canonicalize(fsname) {
            Ok(path) => format!("{}", path.display()),
            Err(_) => String::from(fsname),
        };
        let dev = device.trim_start_matches("/dev/");
        let block_dir = "/sys/block";
        let mut device_dir = format!("{}/{}", block_dir, dev);
        if !Path::new(&device_dir).exists() {
            let dir = fs::read_dir(&block_dir).map_err(|e| {
                ConfigError::FileSystem(format!(
                    "{}: read block dir {:?} failed: {:?}",
                    op, block_dir, e
                ))
            })?;
            let mut find = false;
            for entry in dir {
                if entry.is_err() {
                    continue;
                }
                let entry = entry.unwrap();
                let mut cur_path = entry.path();
                cur_path.push(dev);
                if cur_path.exists() {
                    device_dir = entry.path().to_str().unwrap().to_owned();
                    find = true;
                    break;
                }
            }
            if !find {
                return Err(ConfigError::FileSystem(format!(
                    "{}: {:?} no device find in block",
                    op, fsname
                )));
            }
        }

        let rota_path = format!("{}/queue/rotational", device_dir);
        if !Path::new(&rota_path).exists() {
            return Err(ConfigError::FileSystem(format!(
                "{}: block {:?} has no rotational file",
                op, device_dir
            )));
        }

        let buffer = fs::read_to_string(&rota_path).map_err(|e| {
            ConfigError::FileSystem(format!("{}: {:?} failed: {:?}", op, rota_path, e))
        })?;
        Ok(buffer.trim_matches('\n').to_owned())
    }

    // check device && fs
    pub fn check_data_dir(data_path: &str, mnt_file: &str) -> Result<(), ConfigError> {
        let op = "data-dir.check";
        let real_path = match canonicalize_path(data_path) {
            Ok(path) => path,
            Err(e) => {
                return Err(ConfigError::FileSystem(format!(
                    "{}: path: {:?} canonicalize failed: {:?}",
                    op, data_path, e
                )));
            }
        };

        // TODO check ext4 nodelalloc
        let fs_info = get_fs_info(&real_path, mnt_file)?;
        info!("data dir"; "data_path" => data_path, "mount_fs" => ?fs_info);

        if get_rotational_info(&fs_info.fsname)? != "0" {
            warn!("not on SSD device"; "data_path" => data_path);
        }
        Ok(())
    }

    #[cfg(test)]
    mod tests {
        use std::fs::File;
        use std::io::Write;
        use std::os::unix::fs::symlink;
        use tempfile::Builder;

        use super::*;

        fn create_file(fpath: &str, buf: &[u8]) {
            let mut file = File::create(fpath).unwrap();
            file.write_all(buf).unwrap();
            file.flush().unwrap();
        }

        #[test]
        fn test_get_fs_info() {
            let tmp_dir = Builder::new().prefix("test-get-fs-info").tempdir().unwrap();
            let mninfo = br#"tmpfs /home tmpfs rw,nosuid,noexec,relatime,size=1628744k,mode=755 0 0
/dev/sda4 /home/shirly ext4 rw,relatime,errors=remount-ro,data=ordered 0 0
/dev/sdb /data1 ext4 rw,relatime,errors=remount-ro,data=ordered 0 0
securityfs /sys/kernel/security securityfs rw,nosuid,nodev,noexec,relatime 0 0
"#;
            let mnt_file = format!("{}", tmp_dir.into_path().join("mnt.txt").display());
            create_file(&mnt_file, mninfo);
            let f = get_fs_info("/home/shirly/1111", &mnt_file).unwrap();
            assert_eq!(f.fsname, "/dev/sda4");
            assert_eq!(f.mnt_dir, "/home/shirly");

            // not found
            let f2 = get_fs_info("/tmp", &mnt_file);
            assert!(f2.is_err());
        }

        #[test]
        fn test_get_rotational_info() {
            // test device not exist
            let ret = get_rotational_info("/dev/invalid");
            assert!(ret.is_err());
        }

        #[test]
        fn test_check_data_dir() {
            // test invalid data_path
            let ret = check_data_dir("/sys/invalid", "/proc/mounts");
            assert!(ret.is_err());
            // get real path's fs_info
            let tmp_dir = Builder::new().prefix("test-get-fs-info").tempdir().unwrap();
            let data_path = format!("{}/data1", tmp_dir.path().display());
            let fs_info = get_fs_info(&data_path, "/proc/mounts").unwrap();

            // data_path may not mountted on a normal device on container
            if !fs_info.fsname.starts_with("/dev") {
                return;
            }

            // test with real path
            let ret = check_data_dir(&data_path, "/proc/mounts");
            assert!(ret.is_ok());

            // test with device mapper
            // get real_path's rotational info
            let expect = get_rotational_info(&fs_info.fsname).unwrap();
            // ln -s fs_info.fsname tmp_device
            let tmp_device = format!("{}/tmp_device", tmp_dir.path().display());
            // /dev/xxx may not exists in container.
            if !Path::new(&fs_info.fsname).exists() {
                return;
            }
            symlink(&fs_info.fsname, &tmp_device).unwrap();
            // mount info: data_path=>tmp_device
            let mninfo = format!(
                "{} {} ext4 rw,relatime,errors=remount-ro,data=ordered 0 0",
                &tmp_device, &data_path
            );
            let mnt_file = format!("{}/mnt.txt", tmp_dir.path().display());
            create_file(&mnt_file, mninfo.as_bytes());
            // check info
            let res = check_data_dir(&data_path, &mnt_file);
            assert!(res.is_ok());
            // check rotational info
            let get = get_rotational_info(&tmp_device).unwrap();
            assert_eq!(expect, get);
        }
    }
}

#[cfg(target_os = "linux")]
pub fn check_data_dir(data_path: &str) -> Result<(), ConfigError> {
    self::check_data_dir::check_data_dir(data_path, "/proc/mounts")
}

#[cfg(not(target_os = "linux"))]
pub fn check_data_dir(_data_path: &str) -> Result<(), ConfigError> {
    Ok(())
}

fn get_file_count(data_path: &str, extension: &str) -> Result<usize, ConfigError> {
    let op = "data-dir.file-count.get";
    let dir = fs::read_dir(data_path).map_err(|e| {
        ConfigError::FileSystem(format!(
            "{}: read file dir {:?} failed: {:?}",
            op, data_path, e
        ))
    })?;
    let mut file_count = 0;
    for entry in dir {
        let entry = entry.map_err(|e| {
            ConfigError::FileSystem(format!(
                "{}: read file in file dir {:?} failed: {:?}",
                op, data_path, e
            ))
        })?;
        let path = entry.path();
        if path.is_file() {
            if let Some(ext) = path.extension() {
                if extension.is_empty() || extension == ext {
                    file_count += 1;
                }
            } else if extension.is_empty() {
                file_count += 1;
            }
        }
    }
    Ok(file_count)
}

// check dir is empty of file with certain extension, empty string for any extension.
pub fn check_data_dir_empty(data_path: &str, extension: &str) -> Result<(), ConfigError> {
    let op = "data-dir.empty.check";
    let dir = Path::new(data_path);
    if dir.exists() && !dir.is_file() {
        let count = get_file_count(data_path, extension)?;
        if count > 0 {
            return Err(ConfigError::Limit(format!(
                "{}: the number of file with extension {} in directory {} is non-zero, \
                 got {}, expect 0.",
                op, extension, data_path, count,
            )));
        }
    }
    Ok(())
}

/// `check_addr` validates an address. Addresses are formed like "Host:Port".
/// More details about **Host** and **Port** can be found in WHATWG URL Standard.
pub fn check_addr(addr: &str) -> Result<(), ConfigError> {
    // Try to validate "IPv4:Port" and "[IPv6]:Port".
    if SocketAddrV4::from_str(addr).is_ok() {
        return Ok(());
    }
    if SocketAddrV6::from_str(addr).is_ok() {
        return Ok(());
    }

    let parts: Vec<&str> = addr
        .split(':')
        .filter(|s| !s.is_empty()) // "Host:" or ":Port" are invalid.
        .collect();

    // ["Host", "Port"]
    if parts.len() != 2 {
        return Err(ConfigError::Address(format!("invalid addr: {:?}", addr)));
    }

    // Check Port.
    let port: u16 = parts[1].parse().map_err(|_| {
        ConfigError::Address(format!("invalid addr, parse port failed: {:?}", addr))
    })?;
    // Port = 0 is invalid.
    if port == 0 {
        return Err(ConfigError::Address(format!(
            "invalid addr, port can not be 0: {:?}",
            addr
        )));
    }

    // Check Host.
    if let Err(e) = url::Host::parse(parts[0]) {
        return Err(ConfigError::Address(format!("invalid addr: {:?}", e)));
    }

    Ok(())
}

#[derive(Default)]
pub struct VersionTrack<T> {
    value: RwLock<T>,
    version: AtomicU64,
}

impl<T> VersionTrack<T> {
    pub fn new(value: T) -> Self {
        VersionTrack {
            value: RwLock::new(value),
            version: AtomicU64::new(1),
        }
    }

    /// Update the value
    pub fn update<F>(&self, f: F)
    where
        F: FnOnce(&mut T),
    {
        f(&mut self.value.write().unwrap());
        self.version.fetch_add(1, Ordering::Release);
    }

    pub fn value(&self) -> RwLockReadGuard<'_, T> {
        self.value.read().unwrap()
    }

    pub fn tracker(self: Arc<Self>, tag: String) -> Tracker<T> {
        Tracker {
            tag,
            version: self.version.load(Ordering::Relaxed),
            inner: self,
        }
    }
}

#[derive(Clone, Default)]
pub struct Tracker<T> {
    tag: String,
    inner: Arc<VersionTrack<T>>,
    version: u64,
}

impl<T> Tracker<T> {
    // The update of `value` and `version` is not atomic
    // so there maybe false positive.
    pub fn any_new(&mut self) -> Option<RwLockReadGuard<'_, T>> {
        let v = self.inner.version.load(Ordering::Acquire);
        if self.version < v {
            self.version = v;
            match self.inner.value.try_read() {
                Ok(value) => Some(value),
                Err(_) => {
                    let t = Instant::now_coarse();
                    let value = self.inner.value.read().unwrap();
                    slow_log!(t.elapsed(), "{} tracker get updated value", self.tag);
                    Some(value)
                }
            }
        } else {
            None
        }
    }
}

use std::collections::HashMap;

/// TomlLine use to parse one line content of a toml file
#[derive(Debug)]
enum TomlLine {
    // the `Keys` from "[`Keys`]"
    Table(String),
    // the `Keys` from "`Keys` = value"
    KVPair(String),
    // Comment, empty line, etc.
    Unknown,
}

impl TomlLine {
    fn encode_kv(key: &str, val: &str) -> String {
        format!("{} = {}", key, val)
    }

    // parse kv pair from format of "`Keys` = value"
    fn parse_kv(s: &str) -> TomlLine {
        let mut v: Vec<_> = s.split('=').map(|s| s.trim().to_owned()).rev().collect();
        if v.is_empty() || v.len() > 2 || TomlLine::parse_key(v[v.len() - 1].as_str()).is_none() {
            return TomlLine::Unknown;
        }
        TomlLine::KVPair(v.pop().unwrap())
    }

    fn parse(s: &str) -> TomlLine {
        let s = s.trim();
        // try to parse table from format of "[`Keys`]"
        if let Some(k) = s.strip_prefix('[').map(|s| s.strip_suffix(']')).flatten() {
            return match TomlLine::parse_key(k) {
                Some(k) => TomlLine::Table(k),
                None => TomlLine::Unknown,
            };
        }
        // remove one prefix of '#' if exist
        let kv = s.strip_prefix('#').unwrap_or(s);
        TomlLine::parse_kv(kv)
    }

    // Parse `Keys`, only bare keys and dotted keys are supportted
    // bare keys only contains chars of A-Za-z0-9_-
    // dotted keys are a sequence of bare key joined with a '.'
    fn parse_key(s: &str) -> Option<String> {
        if s.is_empty() || s.starts_with('.') || s.ends_with('.') {
            return None;
        }
        let ks: Vec<_> = s.split('.').map(str::trim).collect();
        let is_valid_key = |s: &&str| -> bool {
            s.chars()
                .all(|c| c.is_ascii_alphanumeric() || "-_".contains(c))
        };
        if ks.iter().all(is_valid_key) {
            return Some(ks.join("."));
        }
        None
    }

    fn concat_key(k1: &str, k2: &str) -> String {
        match (k1.is_empty(), k2.is_empty()) {
            (false, false) => format!("{}.{}", k1, k2),
            (_, true) => k1.to_owned(),
            (true, _) => k2.to_owned(),
        }
    }

    // get the prefix keys: "`Keys`.bare_key" -> Keys
    fn get_prefix(key: &str) -> String {
        key.rsplitn(2, '.').last().unwrap().to_owned()
    }
}

/// TomlWriter use to update the config file and only cover the most commom toml
/// format that used by tikv config file, toml format like: quoted keys, multi-line
/// value, inline table, etc, are not supported, see https://github.com/toml-lang/toml
/// for more detail.
pub struct TomlWriter {
    dst: Vec<u8>,
    current_table: String,
}

impl TomlWriter {
    pub fn new() -> TomlWriter {
        TomlWriter {
            dst: Vec::new(),
            current_table: "".to_owned(),
        }
    }

    pub fn write_change(&mut self, src: String, mut change: HashMap<String, String>) {
        for line in src.lines() {
            match TomlLine::parse(&line) {
                TomlLine::Table(keys) => {
                    self.write_current_table(&mut change);
                    self.write(line.as_bytes());
                    self.current_table = keys;
                }
                TomlLine::KVPair(keys) => {
                    match change.remove(&TomlLine::concat_key(&self.current_table, &keys)) {
                        None => self.write(line.as_bytes()),
                        Some(chg) => self.write(TomlLine::encode_kv(&keys, &chg).as_bytes()),
                    }
                }
                TomlLine::Unknown => self.write(line.as_bytes()),
            }
        }
        if change.is_empty() {
            return;
        }
        while !change.is_empty() {
            self.current_table = TomlLine::get_prefix(change.keys().last().unwrap());
            self.new_line();
            self.write(format!("[{}]", self.current_table).as_bytes());
            self.write_current_table(&mut change);
        }
        self.new_line();
    }

    fn write_current_table(&mut self, change: &mut HashMap<String, String>) {
        let keys: Vec<_> = change
            .keys()
            .filter_map(|k| k.split('.').last())
            .map(str::to_owned)
            .collect();
        for k in keys {
            if let Some(chg) = change.remove(&TomlLine::concat_key(&self.current_table, &k)) {
                self.write(TomlLine::encode_kv(&k, &chg).as_bytes());
            }
        }
    }

    fn write(&mut self, s: &[u8]) {
        self.dst.extend_from_slice(s);
        self.new_line();
    }
    fn new_line(&mut self) {
        self.dst.push(b'\n');
    }

    pub fn finish(self) -> Vec<u8> {
        self.dst
    }
}

#[cfg(test)]
mod tests {
    use std::fs::File;
    use std::io::Write;
    use std::path::Path;

    use super::*;
    use tempfile::Builder;
    use toml;

    #[test]
    fn test_readable_size() {
        let s = ReadableSize::kb(2);
        assert_eq!(s.0, 2048);
        assert_eq!(s.as_mb(), 0);
        let s = ReadableSize::mb(2);
        assert_eq!(s.0, 2 * 1024 * 1024);
        assert_eq!(s.as_mb(), 2);
        let s = ReadableSize::gb(2);
        assert_eq!(s.0, 2 * 1024 * 1024 * 1024);
        assert_eq!(s.as_mb(), 2048);

        assert_eq!((ReadableSize::mb(2) / 2).0, MB);
        assert_eq!((ReadableSize::mb(1) / 2).0, 512 * KB);
        assert_eq!(ReadableSize::mb(2) / ReadableSize::kb(1), 2048);
    }

    #[test]
    fn test_parse_readable_size() {
        #[derive(Serialize, Deserialize)]
        struct SizeHolder {
            s: ReadableSize,
        }

        let legal_cases = vec![
            (0, "0KiB"),
            (2 * KB, "2KiB"),
            (4 * MB, "4MiB"),
            (5 * GB, "5GiB"),
            (7 * TB, "7TiB"),
            (11 * PB, "11PiB"),
        ];
        for (size, exp) in legal_cases {
            let c = SizeHolder {
                s: ReadableSize(size),
            };
            let res_str = toml::to_string(&c).unwrap();
            let exp_str = format!("s = {:?}\n", exp);
            assert_eq!(res_str, exp_str);
            let res_size: SizeHolder = toml::from_str(&exp_str).unwrap();
            assert_eq!(res_size.s.0, size);
        }

        let c = SizeHolder {
            s: ReadableSize(512),
        };
        let res_str = toml::to_string(&c).unwrap();
        assert_eq!(res_str, "s = 512\n");
        let res_size: SizeHolder = toml::from_str(&res_str).unwrap();
        assert_eq!(res_size.s.0, c.s.0);

        let decode_cases = vec![
            (" 0.5 PB", PB / 2),
            ("0.5 TB", TB / 2),
            ("0.5GB ", GB / 2),
            ("0.5MB", MB / 2),
            ("0.5KB", KB / 2),
            ("0.5P", PB / 2),
            ("0.5T", TB / 2),
            ("0.5G", GB / 2),
            ("0.5M", MB / 2),
            ("0.5K", KB / 2),
            ("23", 23),
            ("1", 1),
            ("1024B", KB),
            // units with binary prefixes
            (" 0.5 PiB", PB / 2),
            ("1PiB", PB),
            ("0.5 TiB", TB / 2),
            ("2 TiB", TB * 2),
            ("0.5GiB ", GB / 2),
            ("787GiB ", GB * 787),
            ("0.5MiB", MB / 2),
            ("3MiB", MB * 3),
            ("0.5KiB", KB / 2),
            ("1 KiB", KB),
        ];
        for (src, exp) in decode_cases {
            let src = format!("s = {:?}", src);
            let res: SizeHolder = toml::from_str(&src).unwrap();
            assert_eq!(res.s.0, exp);
        }

        let illegal_cases = vec![
            "0.5kb", "0.5kB", "0.5Kb", "0.5k", "0.5g", "b", "gb", "1b", "B", "1K24B", " 5_KB",
            "4B7", "5M_",
        ];
        for src in illegal_cases {
            let src_str = format!("s = {:?}", src);
            assert!(toml::from_str::<SizeHolder>(&src_str).is_err(), "{}", src);
        }
    }

    #[test]
    fn test_duration_construction() {
        let mut dur = ReadableDuration::secs(1);
        assert_eq!(dur.0, Duration::new(1, 0));
        assert_eq!(dur.as_secs(), 1);
        assert_eq!(dur.as_millis(), 1000);
        dur = ReadableDuration::millis(1001);
        assert_eq!(dur.0, Duration::new(1, 1_000_000));
        assert_eq!(dur.as_secs(), 1);
        assert_eq!(dur.as_millis(), 1001);
        dur = ReadableDuration::minutes(2);
        assert_eq!(dur.0, Duration::new(2 * 60, 0));
        assert_eq!(dur.as_secs(), 120);
        assert_eq!(dur.as_millis(), 120000);
        dur = ReadableDuration::hours(2);
        assert_eq!(dur.0, Duration::new(2 * 3600, 0));
        assert_eq!(dur.as_secs(), 7200);
        assert_eq!(dur.as_millis(), 7200000);
    }

    #[test]
    fn test_parse_readable_duration() {
        #[derive(Serialize, Deserialize)]
        struct DurHolder {
            d: ReadableDuration,
        }

        let legal_cases = vec![
            (0, 0, "0s"),
            (0, 1, "1ms"),
            (2, 0, "2s"),
            (24 * 3600, 0, "1d"),
            (2 * 24 * 3600, 10, "2d10ms"),
            (4 * 60, 0, "4m"),
            (5 * 3600, 0, "5h"),
            (3600 + 2 * 60, 0, "1h2m"),
            (5 * 24 * 3600 + 3600 + 2 * 60, 0, "5d1h2m"),
            (3600 + 2, 5, "1h2s5ms"),
            (3 * 24 * 3600 + 7 * 3600 + 2, 5, "3d7h2s5ms"),
        ];
        for (secs, ms, exp) in legal_cases {
            let d = DurHolder {
                d: ReadableDuration(Duration::new(secs, ms * 1_000_000)),
            };
            let res_str = toml::to_string(&d).unwrap();
            let exp_str = format!("d = {:?}\n", exp);
            assert_eq!(res_str, exp_str);
            let res_dur: DurHolder = toml::from_str(&exp_str).unwrap();
            assert_eq!(res_dur.d.0, d.d.0);
        }

        let decode_cases = vec![(" 0.5 h2m ", 3600 / 2 + 2 * 60, 0)];
        for (src, secs, ms) in decode_cases {
            let src = format!("d = {:?}", src);
            let res: DurHolder = toml::from_str(&src).unwrap();
            assert_eq!(res.d.0, Duration::new(secs, ms * 1_000_000));
        }

        let illegal_cases = vec!["1H", "1M", "1S", "1MS", "1h1h", "h"];
        for src in illegal_cases {
            let src_str = format!("d = {:?}", src);
            assert!(toml::from_str::<DurHolder>(&src_str).is_err(), "{}", src);
        }
        assert!(toml::from_str::<DurHolder>("d = 23").is_err());
    }

    #[test]
    fn test_canonicalize_path() {
        let tmp = Builder::new()
            .prefix("test-canonicalize")
            .tempdir()
            .unwrap();
        let tmp_dir = tmp.path();

        let res_path1 = canonicalize_sub_path(tmp_dir.to_str().unwrap(), "test1.dump").unwrap();
        assert!(!Path::new(&res_path1).exists());
        assert_eq!(
            Path::new(&res_path1),
            tmp_dir.canonicalize().unwrap().join("test1.dump")
        );

        let path2 = format!("{}", tmp_dir.to_path_buf().join("test2").display());
        assert!(canonicalize_path(&path2).is_err());
        ensure_dir_exist(&path2).unwrap();
        let res_path2 = canonicalize_path(&path2).unwrap();
        assert_eq!(
            Path::new(&res_path2),
            Path::new(&path2).canonicalize().unwrap()
        );

        let path2 = format!("{}", tmp_dir.to_path_buf().join("test2.dump").display());
        {
            File::create(&path2).unwrap();
        }
        assert!(canonicalize_path(&path2).is_err());
        assert!(Path::new(&path2).exists());
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn test_check_kernel() {
        use super::check_kernel::{check_kernel_params, Checker};
        use std::i64;

        // The range of vm.swappiness is from 0 to 100.
        let table: Vec<(&str, i64, Box<Checker>, bool)> = vec![
            (
                "/proc/sys/vm/swappiness",
                i64::MAX,
                Box::new(|got, expect| got == expect),
                false,
            ),
            (
                "/proc/sys/vm/swappiness",
                i64::MAX,
                Box::new(|got, expect| got < expect),
                true,
            ),
        ];

        for (path, expect, checker, is_ok) in table {
            assert_eq!(check_kernel_params(path, expect, checker).is_ok(), is_ok);
        }
    }

    #[test]
    fn test_check_addrs() {
        let table = vec![
            ("127.0.0.1:8080", true),
            ("[::1]:8080", true),
            ("localhost:8080", true),
            ("pingcap.com:8080", true),
            ("funnydomain:8080", true),
            ("127.0.0.1", false),
            ("[::1]", false),
            ("localhost", false),
            ("pingcap.com", false),
            ("funnydomain", false),
            ("funnydomain:", false),
            ("root@google.com:8080", false),
            ("http://google.com:8080", false),
            ("google.com:8080/path", false),
            ("http://google.com:8080/path", false),
            ("http://google.com:8080/path?lang=en", false),
            ("http://google.com:8080/path?lang=en#top", false),
            ("ftp://ftp.is.co.za/rfc/rfc1808.txt", false),
            ("http://www.ietf.org/rfc/rfc2396.txt", false),
            ("ldap://[2001:db8::7]/c=GB?objectClass?one", false),
            ("mailto:John.Doe@example.com", false),
            ("news:comp.infosystems.www.servers.unix", false),
            ("tel:+1-816-555-1212", false),
            ("telnet://192.0.2.16:80/", false),
            ("urn:oasis:names:specification:docbook:dtd:xml:4.1.2", false),
            (":8080", false),
            ("8080", false),
            ("8080:", false),
        ];

        for (addr, is_ok) in table {
            assert_eq!(check_addr(addr).is_ok(), is_ok);
        }
    }

    fn create_file(fpath: &str, buf: &[u8]) {
        let mut file = File::create(fpath).unwrap();
        file.write_all(buf).unwrap();
        file.flush().unwrap();
    }

    #[test]
    fn test_get_file_count() {
        let tmp_path = Builder::new()
            .prefix("test-get-file-count")
            .tempdir()
            .unwrap()
            .into_path();
        let count = get_file_count(tmp_path.to_str().unwrap(), "txt").unwrap();
        assert_eq!(count, 0);
        let tmp_file = format!("{}", tmp_path.join("test-get-file-count.txt").display());
        create_file(&tmp_file, b"");
        let count = get_file_count(tmp_path.to_str().unwrap(), "").unwrap();
        assert_eq!(count, 1);
        let count = get_file_count(tmp_path.to_str().unwrap(), "txt").unwrap();
        assert_eq!(count, 1);
        let count = get_file_count(tmp_path.to_str().unwrap(), "xt").unwrap();
        assert_eq!(count, 0);
    }

    #[test]
    fn test_check_data_dir_empty() {
        // test invalid data_path
        let ret = check_data_dir_empty("/sys/invalid", "txt");
        assert!(ret.is_ok());
        // test empty data_path
        let tmp_path = Builder::new()
            .prefix("test-get-file-count")
            .tempdir()
            .unwrap()
            .into_path();
        let ret = check_data_dir_empty(tmp_path.to_str().unwrap(), "txt");
        assert!(ret.is_ok());
        // test non-empty data_path
        let tmp_file = format!("{}", tmp_path.join("test-get-file-count.txt").display());
        create_file(&tmp_file, b"");
        let ret = check_data_dir_empty(tmp_path.to_str().unwrap(), "");
        assert!(ret.is_err());
        let ret = check_data_dir_empty(tmp_path.to_str().unwrap(), "txt");
        assert!(ret.is_err());
        let ret = check_data_dir_empty(tmp_path.to_str().unwrap(), "xt");
        assert!(ret.is_ok());
    }

    #[test]
    fn test_multi_tracker() {
        use super::*;
        use std::sync::Arc;

        #[derive(Debug, Default, PartialEq)]
        struct Value {
            v1: u64,
            v2: bool,
        }

        let count = 10;
        let vc = Arc::new(VersionTrack::new(Value::default()));
        let mut trackers = Vec::with_capacity(count);
        for _ in 0..count {
            trackers.push(vc.clone().tracker("test-tracker".to_owned()));
        }

        assert!(trackers.iter_mut().all(|tr| tr.any_new().is_none()));

        vc.update(|v| {
            v.v1 = 1000;
            v.v2 = true;
        });
        for tr in trackers.iter_mut() {
            let incoming = tr.any_new();
            assert!(incoming.is_some());
            let incoming = incoming.unwrap();
            assert_eq!(incoming.v1, 1000);
            assert_eq!(incoming.v2, true);
        }

        assert!(trackers.iter_mut().all(|tr| tr.any_new().is_none()));
    }

    #[test]
    fn test_toml_writer() {
        let cfg = r#"
## commet1
log-level = "info"

[readpool.storage]
## commet2
normal-concurrency = 1
# high-concurrency = 1

## commet3
[readpool.coprocessor]
normal-concurrency = 1

[rocksdb.defaultcf]
compression-per-level = ["no", "no", "no", "no", "no", "no", "no"]
"#;
        let mut m = HashMap::new();
        m.insert("log-file".to_owned(), "log-file-name".to_owned());
        m.insert("readpool.storage.xxx".to_owned(), "zzz".to_owned());
        m.insert(
            "readpool.storage.high-concurrency".to_owned(),
            "345".to_owned(),
        );
        m.insert(
            "readpool.coprocessor.normal-concurrency".to_owned(),
            "123".to_owned(),
        );
        m.insert("not-in-file-config1.xxx.yyy".to_owned(), "100".to_owned());
        m.insert(
            "rocksdb.defaultcf.compression-per-level".to_owned(),
            "[\"no\", \"no\", \"lz4\", \"lz4\", \"lz4\", \"zstd\", \"zstd\"]".to_owned(),
        );

        let mut t = TomlWriter::new();
        t.write_change(cfg.to_owned(), m);
        let expect = r#"
## commet1
log-level = "info"

log-file = log-file-name
[readpool.storage]
## commet2
normal-concurrency = 1
high-concurrency = 345

## commet3
xxx = zzz
[readpool.coprocessor]
normal-concurrency = 123

[rocksdb.defaultcf]
compression-per-level = ["no", "no", "lz4", "lz4", "lz4", "zstd", "zstd"]

[not-in-file-config1.xxx]
yyy = 100

"#;
        assert_eq!(expect.as_bytes(), t.finish().as_slice());
    }
}
