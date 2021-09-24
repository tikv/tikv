// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

#![allow(dead_code)]

use collections::HashSet;

/// Gets the ID of the current process.
#[cfg(target_os = "linux")]
pub fn process_id() -> usize {
    lazy_static::lazy_static! {
        static ref PID: libc::pid_t = unsafe { libc::getpid() };
    }
    *PID as _
}

#[cfg(not(target_os = "linux"))]
pub fn process_id() -> usize {
    std::process::id() as _
}

/// Gets the ID of the current thread.
#[cfg(target_os = "linux")]
pub fn thread_id() -> usize {
    unsafe { libc::syscall(libc::SYS_gettid) as usize }
}

#[cfg(not(target_os = "linux"))]
pub fn thread_id() -> usize {
    thread_id::get()
}

/// Get all thread id collections under the current process.
#[cfg(target_os = "linux")]
pub fn thread_ids() -> Option<HashSet<usize>> {
    std::fs::read_dir(format!("/proc/{}/task", process_id()))
        .ok()
        .map(|dir| {
            dir.filter_map(|task| {
                let file_name = task.ok().map(|t| t.file_name());
                file_name.and_then(|f| f.to_str().and_then(|tid| tid.parse().ok()))
            })
            .map(|id: libc::pid_t| id as usize)
            .collect::<HashSet<usize>>()
        })
}

#[cfg(not(target_os = "linux"))]
pub fn thread_ids() -> Option<HashSet<usize>> {
    None
}

#[cfg(target_os = "linux")]
pub fn clock_tick() -> u64 {
    lazy_static::lazy_static! {
        static ref CLK_TCK: libc::c_long = unsafe { libc::sysconf(libc::_SC_CLK_TCK) };
    }
    *CLK_TCK as _
}

#[cfg(not(target_os = "linux"))]
pub fn clock_tick() -> u64 {
    1
}

#[derive(Default)]
pub struct Stat {
    pub stime: libc::clock_t,
    pub utime: libc::clock_t,
}

#[cfg(target_os = "linux")]
impl From<procinfo::pid::Stat> for Stat {
    fn from(stat: procinfo::pid::Stat) -> Self {
        Self {
            stime: stat.stime,
            utime: stat.utime,
        }
    }
}

#[cfg(target_os = "linux")]
pub fn stat_task(pid: usize, tid: usize) -> std::io::Result<Stat> {
    procinfo::pid::stat_task(pid as _, tid as _).map(Into::into)
}

#[cfg(not(target_os = "linux"))]
pub fn stat_task(_pid: usize, _tid: usize) -> std::io::Result<Stat> {
    Ok(Stat::default())
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
        let ids = thread_ids();
        assert!(matches!(ids, Some(_)));
        assert!(!ids.unwrap().is_empty());
    }
}
