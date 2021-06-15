// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

use crate::tag::TagCell;

use crossbeam::queue::SegQueue;
use lazy_static::lazy_static;

#[cfg(target_os = "linux")]
pub use linux::*;
#[cfg(not(target_os = "linux"))]
pub use other_os::*;

#[derive(Debug, Default)]
pub struct CpuTimeInstant(u64);

impl CpuTimeInstant {
    pub fn minus_ms(&self, other: &CpuTimeInstant) -> u64 {
        self.0 - other.0
    }
}

#[cfg(target_os = "linux")]
mod linux {
    use lazy_static::lazy_static;
    use libc::{self, pid_t};

    lazy_static! {
        static ref PID: pid_t = unsafe { libc::getpid() };
        static ref CLK_TCK: libc::c_long = unsafe { libc::sysconf(libc::_SC_CLK_TCK) };
    }

    #[derive(Eq, PartialEq, Clone, Copy, Hash, Debug)]
    pub struct ThreadId(pid_t);

    impl ThreadId {
        pub fn current() -> Self {
            Self(unsafe { libc::syscall(libc::SYS_gettid) as libc::pid_t })
        }
    }

    impl super::CpuTimeInstant {
        pub fn now(id: &ThreadId) -> Option<Self> {
            STAT_TASK_COUNT.inc();

            if let Ok(stat) = procinfo::pid::stat_task(*PID, id.0) {
                let cpu_ticks = (stat.utime as u64).wrapping_add(stat.stime as u64);
                let ms = cpu_ticks * 1_000 / (*CLK_TCK as u64);
                return Some(super::CpuTimeInstant(ms));
            }

            None
        }
    }

    lazy_static! {
        static ref STAT_TASK_COUNT: prometheus::IntCounter = prometheus::register_int_counter!(
            "tikv_req_cpu_stat_task_count",
            "Counter of stat_task call"
        )
        .unwrap();
    }
}

#[cfg(not(target_os = "linux"))]
mod other_os {
    use std::thread;
    use std::time::Instant;

    use lazy_static::lazy_static;

    #[derive(Eq, PartialEq, Clone, Copy, Hash, Debug)]
    pub struct ThreadId(thread::ThreadId);

    impl ThreadId {
        pub fn current() -> Self {
            Self(thread::current().id())
        }
    }

    impl super::CpuTimeInstant {
        pub fn now(_id: &ThreadId) -> Option<Self> {
            lazy_static! {
                static ref ANCHOR: Instant = Instant::now();
            }

            Some(super::CpuTimeInstant(ANCHOR.elapsed().as_millis() as _))
        }
    }
}

pub struct ThreadRegister;

lazy_static! {
    static ref THREAD_REGISTRATIONS: SegQueue<ThreadRegistrationMsg> = SegQueue::new();
}

struct ThreadRegistrationMsg {
    thread_id: ThreadId,
    shared_ptr: TagCell,
}

impl ThreadRegister {
    pub fn register(shared_ptr: TagCell) {
        THREAD_REGISTRATIONS.push(ThreadRegistrationMsg {
            thread_id: ThreadId::current(),
            shared_ptr,
        });
    }

    pub fn consume_registrations(mut handler: impl FnMut(ThreadId, TagCell)) {
        let rs = &*THREAD_REGISTRATIONS;
        while let Some(ThreadRegistrationMsg {
            thread_id,
            shared_ptr,
        }) = rs.pop()
        {
            handler(thread_id, shared_ptr);
        }
    }
}
