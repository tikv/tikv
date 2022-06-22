// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

#[derive(Clone, Copy, Debug)]
pub struct IoStat {
    pub read: u64,
    pub write: u64,
}

#[derive(Clone, Copy, Debug)]
pub struct DiskStat {
    pub reads: u64,
    pub time_reading: u64,
    pub writes: u64,
    pub time_writing: u64,
    pub sectors_read: u64,
    pub sectors_write: u64,
}

/// An inspector for a thread.
pub trait ThreadInspector {
    type DiskID;

    /// Disk read and write bytes from the backend storage layer. `None` means it's not available
    /// for the platform.
    fn io_stat(&self) -> Result<Option<IoStat>, String> {
        Ok(None)
    }

    /// Get the device name for the given `path`.
    fn get_device(_path: &str) -> Result<Option<Self::DiskID>, String> {
        Ok(None)
    }

    /// Disk statistics for the given device.
    fn disk_stat(_dev: &Self::DiskID) -> Result<Option<DiskStat>, String> {
        Ok(None)
    }
}

#[cfg(target_os = "linux")]
mod linux {
    use std::{
        fs::{read_to_string, File},
        os::unix::io::AsRawFd,
        path::Path,
    };

    use procfs::process::Process;

    use super::{DiskStat, IoStat, ThreadInspector};
    use crate::sys::thread;

    pub struct Impl(Process);

    impl From<procfs::process::Io> for IoStat {
        fn from(io: procfs::process::Io) -> IoStat {
            IoStat {
                read: io.read_bytes,
                write: io.write_bytes,
            }
        }
    }
    impl From<procfs::DiskStat> for DiskStat {
        fn from(disk: procfs::DiskStat) -> DiskStat {
            DiskStat {
                reads: disk.reads,
                time_reading: disk.time_reading,
                writes: disk.writes,
                time_writing: disk.time_writing,
                sectors_read: disk.sectors_read,
                sectors_write: disk.sectors_written,
            }
        }
    }

    impl ThreadInspector for Impl {
        type DiskID = (u32, u32);

        fn io_stat(&self) -> Result<Option<IoStat>, String> {
            self.0
                .io()
                .map_err(|e| format!("Process::io: {}", e))
                .map(|x| Some(x.into()))
        }

        fn get_device(path: &str) -> Result<Option<Self::DiskID>, String> {
            let (major, minor) = disk_identify(path)?;
            Ok(Some((major, minor)))
        }

        fn disk_stat(dev: &Self::DiskID) -> Result<Option<DiskStat>, String> {
            let path = "/proc/diskstats";
            let lines = read_to_string(&path).map_err(|e| format!("open({}): {}", path, e))?;
            for line in lines.split('\n').map(|x| x.trim()) {
                let stat = procfs::DiskStat::from_line(line)
                    .map_err(|e| format!("parse disk stat: {}", e))?;
                if stat.major as u32 == dev.0 && stat.minor as u32 == dev.1 {
                    return Ok(Some(stat.into()));
                }
            }
            Err(format!("disk stat for {}:{} not found", dev.0, dev.1))
        }
    }

    pub fn self_thread_inspector() -> Result<Impl, String> {
        let pid = thread::process_id();
        let tid = thread::thread_id();
        let root = format!("/proc/{}/task/{}", pid, tid);
        let process = Process::new_with_root(root.into())
            .map_err(|e| format!("procfs::Process::new: {}", e))?;
        Ok(Impl(process))
    }

    fn disk_identify<P: AsRef<Path>>(path: P) -> Result<(u32, u32), String> {
        let f = File::open(&path).map_err(|e| format!("open({:?}): {}", path.as_ref(), e))?;
        let fd = f.as_raw_fd();
        unsafe {
            let mut stat: libc::stat = std::mem::zeroed();
            if libc::fstat(fd as _, &mut stat as *mut _) != 0 {
                let errno = *libc::__errno_location();
                return Err(format!("fstat errno: {}", errno));
            }
            Ok((libc::major(stat.st_dev), libc::minor(stat.st_dev)))
        }
    }
}

#[cfg(target_os = "linux")]
pub use self::linux::{self_thread_inspector, Impl as ThreadInspectorImpl};

#[cfg(not(target_os = "linux"))]
mod notlinux {
    use super::ThreadInspector;
    pub struct Impl;
    impl ThreadInspector for Impl {
        type DiskID = ();
    }

    pub fn self_thread_inspector() -> Result<Impl, String> {
        Ok(Impl)
    }
}

#[cfg(not(target_os = "linux"))]
pub use self::notlinux::{self_thread_inspector, Impl as ThreadInspectorImpl};

#[cfg(target_os = "linux")]
#[cfg(test)]
mod tests {
    use std::io::Write;

    use super::*;

    fn page_size() -> u64 {
        unsafe { libc::sysconf(libc::_SC_PAGE_SIZE) as u64 }
    }

    #[test]
    fn test_thread_inspector_io_stat() {
        let inspector = self_thread_inspector().unwrap();
        let io1 = inspector.io_stat().unwrap().unwrap();

        let mut f = tempfile::tempfile().unwrap();
        f.write_all(b"abcdefg").unwrap();
        f.sync_all().unwrap();

        let io2 = inspector.io_stat().unwrap().unwrap();
        assert_eq!(io2.write - io1.write, page_size())
    }

    #[test]
    fn test_thread_inspector_disk_stat() {
        let device = ThreadInspectorImpl::get_device(".").unwrap().unwrap();
        assert!(ThreadInspectorImpl::disk_stat(&device).unwrap().is_some());
    }
}
