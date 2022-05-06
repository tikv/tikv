// Copyright 2020 TiKV Project Authors. Licensed under Apache-2.0.
// Modified from https://github.com/rust-lang/cargo/blob/426fae51f39ebf6c545a2c12f78bc09fbfdb7aa9/src/cargo/util/cpu.rs
// TODO: Maybe use https://github.com/heim-rs/heim is better after https://github.com/heim-rs/heim/issues/233 is fixed.

use std::io;

use derive_more::{Add, Sub};

#[derive(Debug, Clone, Copy, Add, Sub)]
pub struct LiunxStyleCpuTime {
    pub user: u64,
    pub nice: u64,
    pub system: u64,
    pub idle: u64,
    pub iowait: u64,
    pub irq: u64,
    pub softirq: u64,
    pub steal: u64,
    pub guest: u64,
    pub guest_nice: u64,
}

impl LiunxStyleCpuTime {
    pub fn total(&self) -> u64 {
        // Note: guest(_nice) is not counted, since it is already in user.
        // See https://unix.stackexchange.com/questions/178045/proc-stat-is-guest-counted-into-user-time
        self.user
            + self.system
            + self.idle
            + self.nice
            + self.iowait
            + self.irq
            + self.softirq
            + self.steal
    }

    pub fn current() -> io::Result<LiunxStyleCpuTime> {
        imp::current()
    }
}

#[cfg(target_os = "linux")]
mod imp {
    use std::{
        fs::File,
        io::{self, Read},
    };

    pub fn current() -> io::Result<super::LiunxStyleCpuTime> {
        let mut state = String::new();
        File::open("/proc/stat")?.read_to_string(&mut state)?;

        (|| {
            let mut parts = state.lines().next()?.split_whitespace();
            if parts.next()? != "cpu" {
                return None;
            }
            Some(super::LiunxStyleCpuTime {
                user: parts.next()?.parse::<u64>().ok()?,
                nice: parts.next()?.parse::<u64>().ok()?,
                system: parts.next()?.parse::<u64>().ok()?,
                idle: parts.next()?.parse::<u64>().ok()?,
                iowait: parts.next()?.parse::<u64>().ok()?,
                irq: parts.next()?.parse::<u64>().ok()?,
                softirq: parts.next()?.parse::<u64>().ok()?,
                steal: parts.next()?.parse::<u64>().ok()?,
                guest: parts.next()?.parse::<u64>().ok()?,
                guest_nice: parts.next()?.parse::<u64>().ok()?,
            })
        })()
        .ok_or_else(|| io::Error::new(io::ErrorKind::Other, "first line of /proc/stat malformed"))
    }
}

#[cfg(target_os = "macos")]
mod imp {
    use std::{io, ptr};

    use libc::*;

    pub fn current() -> io::Result<super::LiunxStyleCpuTime> {
        // There's scant little documentation on `host_processor_info`
        // throughout the internet, so this is just modeled after what everyone
        // else is doing. For now this is modeled largely after libuv.

        unsafe {
            let mut num_cpus_u = 0;
            let mut cpu_info = ptr::null_mut();
            let mut msg_type = 0;
            let ret = host_processor_info(
                mach_host_self(),
                PROCESSOR_CPU_LOAD_INFO as processor_flavor_t,
                &mut num_cpus_u,
                &mut cpu_info,
                &mut msg_type,
            );
            if ret != KERN_SUCCESS {
                return Err(io::Error::from_raw_os_error(ret));
            }

            let mut ret = super::LiunxStyleCpuTime {
                user: 0,
                system: 0,
                idle: 0,
                iowait: 0,
                irq: 0,
                softirq: 0,
                steal: 0,
                guest: 0,
                nice: 0,
                guest_nice: 0,
            };
            let mut current = cpu_info as *const processor_cpu_load_info_data_t;
            for _ in 0..num_cpus_u {
                ret.user += (*current).cpu_ticks[CPU_STATE_USER as usize] as u64;
                ret.system += (*current).cpu_ticks[CPU_STATE_SYSTEM as usize] as u64;
                ret.idle += (*current).cpu_ticks[CPU_STATE_IDLE as usize] as u64;
                ret.nice += (*current).cpu_ticks[CPU_STATE_NICE as usize] as u64;
                current = current.offset(1);
            }
            vm_deallocate(mach_task_self_, cpu_info as vm_address_t, msg_type as usize);
            Ok(ret)
        }
    }
}

#[cfg(not(any(target_os = "linux", target_os = "macos")))]
mod imp {
    use std::io;

    pub fn current() -> io::Result<super::LiunxStyleCpuTime> {
        Err(io::Error::new(
            io::ErrorKind::Other,
            "unsupported platform to learn CPU state",
        ))
    }
}
