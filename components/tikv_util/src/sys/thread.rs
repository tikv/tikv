// Copyright 2022 TiKV Project Authors. Licensed under Apache-2.0.

//! This module provides unified APIs for accessing thread/process related information.
//! Only Linux platform is implemented correctly, for other platform, it only guarantees
//! successful compilation.

use std::io;

/// A cross-platform CPU statistics data structure.
#[derive(Default, PartialEq)]
pub struct ThreadStat {
    // libc::clock_t is not used here because the definition of
    // clock_t is different on linux and bsd.
    pub s_time: i64,
    pub u_time: i64,
}

impl ThreadStat {
    /// Gets the cpu time in seconds.
    #[inline]
    pub fn total_cpu_time(&self) -> f64 {
        cpu_total(self.s_time, self.u_time)
    }
}

#[inline]
fn cpu_total(sys_time: i64, user_time: i64) -> f64 {
    (sys_time + user_time) as f64 / clock_tick() as f64
}

#[cfg(target_os = "linux")]
pub mod linux {
    #[inline]
    pub fn cpu_total(stat: &super::FullStat) -> f64 {
        super::cpu_total(stat.stime, stat.utime)
    }
}

#[cfg(target_os = "linux")]
mod imp {
    use libc::c_int;
    use std::fs;
    use std::io::{self, Error};
    use std::iter::FromIterator;

    pub use libc::pid_t as Pid;
    pub use procinfo::pid::{self, Stat as FullStat};

    lazy_static::lazy_static! {
        // getconf CLK_TCK
        static ref CLOCK_TICK: i64 = {
            unsafe {
                libc::sysconf(libc::_SC_CLK_TCK)
            }
        };

        static ref PROCESS_ID: Pid = unsafe { libc::getpid() };
    }

    #[inline]
    pub fn clock_tick() -> i64 {
        *CLOCK_TICK
    }

    /// Gets the ID of the current process.
    #[inline]
    pub fn process_id() -> Pid {
        *PROCESS_ID
    }

    /// Gets the ID of the current thread.
    #[inline]
    pub fn thread_id() -> Pid {
        thread_local! {
            static TID: Pid = unsafe { libc::syscall(libc::SYS_gettid) as Pid };
        }
        TID.with(|t| *t)
    }

    /// Gets thread ids of the given process id.
    /// WARN: Don't call this function frequently. Otherwise there will be a lot of memory fragments.
    pub fn thread_ids<C: FromIterator<Pid>>(pid: Pid) -> io::Result<C> {
        let dir = fs::read_dir(format!("/proc/{}/task", pid))?;
        Ok(dir
            .filter_map(|task| {
                let file_name = match task {
                    Ok(t) => t.file_name(),
                    Err(e) => {
                        error!("read task failed"; "pid" => pid, "err" => ?e);
                        return None;
                    }
                };

                match file_name.to_str() {
                    Some(tid) => match tid.parse() {
                        Ok(tid) => Some(tid),
                        Err(e) => {
                            error!("read task failed"; "pid" => pid, "err" => ?e);
                            None
                        }
                    },
                    None => {
                        error!("read task failed"; "pid" => pid);
                        None
                    }
                }
            })
            .collect())
    }

    pub fn full_thread_stat(pid: Pid, tid: Pid) -> io::Result<FullStat> {
        pid::stat_task(pid, tid)
    }

    pub fn set_priority(pri: i32) -> io::Result<()> {
        // Unsafe due to FFI.
        unsafe {
            let tid = libc::syscall(libc::SYS_gettid);
            if libc::setpriority(libc::PRIO_PROCESS as u32, tid as u32, pri) != 0 {
                let e = Error::last_os_error();
                return Err(e);
            }
            Ok(())
        }
    }

    pub fn get_priority() -> io::Result<i32> {
        // Unsafe due to FFI.
        unsafe {
            let tid = libc::syscall(libc::SYS_gettid);
            clear_errno();
            let ret = libc::getpriority(libc::PRIO_PROCESS as u32, tid as u32);
            if ret == -1 {
                let e = Error::last_os_error();
                if let Some(errno) = e.raw_os_error() {
                    if errno != 0 {
                        return Err(e);
                    }
                }
            }
            Ok(ret)
        }
    }

    // Sadly the std lib does not have any support for setting `errno`, so we
    // have to implement this ourselves.
    extern "C" {
        #[link_name = "__errno_location"]
        fn errno_location() -> *mut c_int;
    }

    fn clear_errno() {
        // Unsafe due to FFI.
        unsafe {
            *errno_location() = 0;
        }
    }

    #[cfg(test)]
    mod tests {
        use super::*;
        use crate::sys::HIGH_PRI;
        use std::io::ErrorKind;

        #[test]
        fn test_set_priority() {
            // priority is a value in range -20 to 19, the default priority
            // is 0, lower priorities cause more favorable scheduling.
            assert_eq!(get_priority().unwrap(), 0);
            set_priority(10).unwrap();
            assert_eq!(get_priority().unwrap(), 10);

            // only users who have `SYS_NICE_CAP` capability can increase priority.
            let ret = set_priority(HIGH_PRI);
            if let Err(e) = ret {
                assert_eq!(e.kind(), ErrorKind::PermissionDenied);
            } else {
                assert_eq!(get_priority().unwrap(), HIGH_PRI);
            }
        }
    }
}

#[cfg(not(target_os = "linux"))]
mod imp {
    use std::io;
    use std::iter::FromIterator;

    pub type Pid = u32;

    #[derive(Default)]
    pub struct FullStat {
        pub stime: i64,
        pub utime: i64,
        pub command: String,
    }

    lazy_static::lazy_static! {
        static ref PROCESS_ID: Pid = std::process::id();
    }

    #[inline]
    pub fn clock_tick() -> i64 {
        1
    }

    /// Gets the ID of the current process.
    #[inline]
    pub fn process_id() -> Pid {
        *PROCESS_ID
    }

    /// Gets the ID of the current thread.
    #[inline]
    pub fn thread_id() -> Pid {
        u64::from(std::thread::current().id().as_u64()) as Pid
    }

    pub fn thread_ids<C: FromIterator<Pid>>(_pid: Pid) -> io::Result<C> {
        Ok(std::iter::once(thread_id()).collect())
    }

    pub fn full_thread_stat(_pid: Pid, _tid: Pid) -> io::Result<FullStat> {
        Ok(FullStat::default())
    }

    pub fn set_priority(_: i32) -> io::Result<()> {
        Ok(())
    }

    pub fn get_priority() -> io::Result<i32> {
        Ok(0)
    }
}

pub use self::imp::*;

pub fn thread_stat(pid: Pid, tid: Pid) -> io::Result<ThreadStat> {
    let full_stat = full_thread_stat(pid, tid)?;
    Ok(ThreadStat {
        s_time: full_stat.stime as _,
        u_time: full_stat.utime as _,
    })
}

pub fn current_thread_stat() -> io::Result<ThreadStat> {
    thread_stat(process_id(), thread_id())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_thread_id() {
        let id = thread_id();
        assert_ne!(id, 0);
        std::thread::spawn(move || {
            // Two threads should have different ids.
            assert_ne!(thread_id(), id);
        })
        .join()
        .unwrap();
    }

    #[test]
    #[cfg(target_os = "linux")]
    fn test_thread_ids() {
        let ids: std::io::Result<Vec<_>> = thread_ids(process_id());
        assert!(ids.is_ok());
        assert!(!ids.unwrap().is_empty());
    }
}
