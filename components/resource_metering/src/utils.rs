// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

#![allow(dead_code)]

use collections::HashSet;

/// Gets the ID of the current thread.
#[cfg(target_os = "linux")]
pub fn thread_id() -> usize {
    unsafe { libc::syscall(libc::SYS_gettid) as usize }
}

/// Get all thread id collections under the current process.
#[cfg(target_os = "linux")]
pub fn thread_ids() -> Option<HashSet<usize>> {
    lazy_static::lazy_static! {
        static ref PID: libc::pid_t = unsafe { libc::getpid() };
    }
    std::fs::read_dir(format!("/proc/{}/task", *PID))
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
pub fn thread_id() -> usize {
    thread_id::get()
}

#[cfg(not(target_os = "linux"))]
pub fn thread_ids() -> Option<HashSet<usize>> {
    None
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
        assert!(ids.unwrap().len() > 0);
    }
}
