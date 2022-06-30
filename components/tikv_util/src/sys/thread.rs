// Copyright 2022 TiKV Project Authors. Licensed under Apache-2.0.

//! This module provides unified APIs for accessing thread/process related information.
//! Only Linux platform is implemented correctly, for other platform, it only guarantees
//! successful compilation.

use std::{io, io::Result, sync::Mutex, thread};

use collections::HashMap;

/// A cross-platform CPU statistics data structure.
#[derive(Debug, Copy, Clone, Default, PartialEq)]
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
    (sys_time + user_time) as f64 / ticks_per_second() as f64
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
    use std::{
        fs,
        io::{self, Error},
        iter::FromIterator,
    };

    use libc::c_int;
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
    pub fn ticks_per_second() -> i64 {
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
        use std::io::ErrorKind;

        use super::*;
        use crate::sys::HIGH_PRI;

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

#[cfg(target_os = "macos")]
#[allow(bad_style)]
mod imp {
    use std::{io, iter::FromIterator, mem::size_of, ptr::null_mut, slice};

    use libc::*;

    pub type Pid = mach_port_t;

    type task_inspect_t = mach_port_t;
    type thread_act_t = mach_port_t;
    type thread_act_array_t = *mut thread_act_t;

    extern "C" {
        fn task_threads(
            target_task: task_inspect_t,
            act_list: *mut thread_act_array_t,
            act_listCnt: *mut mach_msg_type_number_t,
        ) -> kern_return_t;
    }

    const MICRO_SEC_PER_SEC: i64 = 1_000_000;

    #[derive(Default)]
    pub struct FullStat {
        pub stime: i64,
        pub utime: i64,
        pub command: String,
    }

    /// Unlike Linux, the unit of `stime` and `utime` is microseconds instead of ticks.
    /// See [`full_thread_stat()`]
    #[inline]
    pub fn ticks_per_second() -> i64 {
        MICRO_SEC_PER_SEC
    }

    /// Gets the ID of the current process.
    #[inline]
    pub fn process_id() -> Pid {
        unsafe { mach_task_self_ }
    }

    /// Gets the ID of the current thread.
    #[inline]
    pub fn thread_id() -> Pid {
        unsafe { mach_thread_self() }
    }

    pub fn thread_ids<C: FromIterator<Pid>>(pid: Pid) -> io::Result<C> {
        // https://www.gnu.org/software/hurd/gnumach-doc/Task-Information.html

        unsafe {
            let mut act_list: thread_act_array_t = null_mut();
            let mut act_count: mach_msg_type_number_t = 0;
            let ret = task_threads(pid, &mut act_list, &mut act_count);
            if ret != KERN_SUCCESS {
                return Err(io::Error::from_raw_os_error(ret));
            }

            let pids = slice::from_raw_parts_mut(act_list, act_count as _)
                .iter()
                .copied()
                .collect();

            vm_deallocate(
                pid,
                act_list as vm_address_t,
                size_of::<thread_act_t>() * act_count as usize,
            );

            Ok(pids)
        }
    }

    pub fn full_thread_stat(_pid: Pid, tid: Pid) -> io::Result<FullStat> {
        // https://www.gnu.org/software/hurd/gnumach-doc/Thread-Information.html

        unsafe {
            let flavor = THREAD_BASIC_INFO;
            let mut info = std::mem::zeroed::<thread_basic_info>();
            let mut thread_info_cnt = THREAD_BASIC_INFO_COUNT;

            let ret = thread_info(
                tid,
                flavor as task_flavor_t,
                (&mut info as *mut _) as thread_info_t,
                &mut thread_info_cnt,
            );
            if ret != KERN_SUCCESS {
                return Err(io::Error::from_raw_os_error(ret));
            }

            Ok(FullStat {
                stime: info.system_time.seconds as i64 * 1_000_000
                    + info.system_time.microseconds as i64,
                utime: info.user_time.seconds as i64 * 1_000_000
                    + info.user_time.microseconds as i64,
                ..Default::default()
            })
        }
    }

    pub fn set_priority(_: i32) -> io::Result<()> {
        Ok(())
    }

    pub fn get_priority() -> io::Result<i32> {
        Ok(0)
    }
}

#[cfg(not(any(target_os = "linux", target_os = "macos")))]
mod imp {
    use std::{io, iter::FromIterator};

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
    pub fn ticks_per_second() -> i64 {
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

pub trait StdThreadBuildWrapper {
    fn spawn_wrapper<F, T>(self, f: F) -> io::Result<thread::JoinHandle<T>>
    where
        F: FnOnce() -> T,
        F: Send + 'static,
        T: Send + 'static;
}

pub trait ThreadBuildWrapper {
    fn after_start_wrapper<F>(&mut self, f: F) -> &mut Self
    where
        F: Fn() + Send + Sync + 'static;

    fn before_stop_wrapper<F>(&mut self, f: F) -> &mut Self
    where
        F: Fn() + Send + Sync + 'static;
}

lazy_static::lazy_static! {
    pub static ref THREAD_NAME_HASHMAP: Mutex<HashMap<Pid, String>> = Mutex::new(HashMap::default());
}

pub(crate) fn add_thread_name_to_map() {
    if let Some(name) = std::thread::current().name() {
        let tid = thread_id();
        THREAD_NAME_HASHMAP
            .lock()
            .unwrap()
            .insert(tid, name.to_string());
        debug!("tid {} thread name is {}", tid, name);
    }
}

pub(crate) fn remove_thread_name_from_map() {
    let tid = thread_id();
    THREAD_NAME_HASHMAP.lock().unwrap().remove(&tid);
}

impl StdThreadBuildWrapper for std::thread::Builder {
    fn spawn_wrapper<F, T>(self, f: F) -> Result<std::thread::JoinHandle<T>>
    where
        F: FnOnce() -> T,
        F: Send + 'static,
        T: Send + 'static,
    {
        #[allow(clippy::disallowed_methods)]
        self.spawn(|| {
            add_thread_name_to_map();
            let res = f();
            remove_thread_name_from_map();
            res
        })
    }
}

impl ThreadBuildWrapper for tokio::runtime::Builder {
    fn after_start_wrapper<F>(&mut self, f: F) -> &mut Self
    where
        F: Fn() + Send + Sync + 'static,
    {
        #[allow(clippy::disallowed_methods)]
        self.on_thread_start(move || {
            add_thread_name_to_map();
            f();
        })
    }

    fn before_stop_wrapper<F>(&mut self, f: F) -> &mut Self
    where
        F: Fn() + Send + Sync + 'static,
    {
        #[allow(clippy::disallowed_methods)]
        self.on_thread_stop(move || {
            f();
            remove_thread_name_from_map();
        })
    }
}

impl ThreadBuildWrapper for futures::executor::ThreadPoolBuilder {
    fn after_start_wrapper<F>(&mut self, f: F) -> &mut Self
    where
        F: Fn() + Send + Sync + 'static,
    {
        #[allow(clippy::disallowed_methods)]
        self.after_start(move |_| {
            add_thread_name_to_map();
            f();
        })
    }

    fn before_stop_wrapper<F>(&mut self, f: F) -> &mut Self
    where
        F: Fn() + Send + Sync + 'static,
    {
        #[allow(clippy::disallowed_methods)]
        self.before_stop(move |_| {
            f();
            remove_thread_name_from_map();
        })
    }
}

#[cfg(test)]
mod tests {
    use std::{
        collections::HashSet,
        sync,
        sync::{Arc, Condvar, Mutex},
    };

    use futures::executor::block_on;

    use super::*;
    use crate::yatp_pool::{DefaultTicker, YatpPoolBuilder};

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
    #[cfg(any(target_os = "linux", target_os = "macos"))]
    fn test_thread_ids() {
        const THREAD_COUNT: usize = 10;
        let (tx, rx) = crossbeam::channel::bounded(THREAD_COUNT);

        #[allow(clippy::mutex_atomic)]
        let stop_threads_cvar = Arc::new((Mutex::new(false), Condvar::new()));

        let threads = (0..THREAD_COUNT)
            .map(|_| {
                let tx = tx.clone();
                let stop_threads_cvar = stop_threads_cvar.clone();
                std::thread::spawn(move || {
                    tx.send(thread_id()).unwrap();

                    let (lock, cvar) = &*stop_threads_cvar;
                    let _guard = cvar
                        .wait_while(lock.lock().unwrap(), |stop| !*stop)
                        .unwrap();
                })
            })
            .collect::<Vec<_>>();

        let actual_tids = rx.iter().take(THREAD_COUNT).collect::<Vec<_>>();
        let ids = thread_ids::<HashSet<_>>(process_id()).unwrap();
        for tid in &actual_tids {
            assert!(ids.contains(tid));
        }

        {
            let (lock, cvar) = &*stop_threads_cvar;
            *lock.lock().unwrap() = true;
            cvar.notify_all();
        }
        for thread in threads {
            thread.join().unwrap();
        }

        let stopped_tids = actual_tids;
        let ids = thread_ids::<HashSet<_>>(process_id()).unwrap();
        for tid in &stopped_tids {
            assert!(!ids.contains(tid));
        }
    }

    #[test]
    fn test_thread_name_wrapper() {
        let thread_name = "thread_for_test";

        let (tx, rx) = sync::mpsc::sync_channel(10);

        let get_name = move || {
            let tid = thread_id();
            if let Some(name) = THREAD_NAME_HASHMAP.lock().unwrap().get(&tid) {
                tx.clone().send(name.to_string()).unwrap();
            } else {
                panic!("thread not found");
            }
        };

        // test std thread builder
        std::thread::Builder::new()
            .name(thread_name.to_string())
            .spawn_wrapper(get_name.clone())
            .unwrap()
            .join()
            .unwrap();

        let name = rx.recv().unwrap();
        assert_eq!(name, thread_name);

        // test Yatp
        let get_name_fn = get_name.clone();
        block_on(
            YatpPoolBuilder::new(DefaultTicker {})
                .name_prefix(thread_name)
                .after_start(|| {})
                .before_stop(|| {})
                .build_future_pool()
                .spawn_handle(async move { get_name_fn() })
                .unwrap(),
        )
        .unwrap();

        let name = rx.recv().unwrap();
        assert!(name.contains(thread_name));

        // test tokio thread builder
        let get_name_fn = get_name;
        block_on(
            tokio::runtime::Builder::new_multi_thread()
                .thread_name(thread_name)
                .after_start_wrapper(|| {})
                .before_stop_wrapper(|| {})
                .build()
                .unwrap()
                .spawn(async move { get_name_fn() }),
        )
        .unwrap();

        let name = rx.recv().unwrap();
        assert_eq!(name, thread_name);
    }
}
