// Copyright 2020 TiKV Project Authors. Licensed under Apache-2.0.
// Modified from https://github.com/rust-lang/cargo/blob/426fae51f39ebf6c545a2c12f78bc09fbfdb7aa9/src/cargo/util/cpu.rs
// TODO: Maybe use https://github.com/heim-rs/heim is better after https://github.com/heim-rs/heim/issues/233 is fixed.

use std::io;

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
    use std::fs::File;
    use std::io::{self, Read};

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
#[allow(bad_style)]
mod imp {
    use std::io;
    use std::ptr;

    type host_t = u32;
    type mach_port_t = u32;
    type vm_map_t = mach_port_t;
    type vm_offset_t = usize;
    type vm_size_t = usize;
    type vm_address_t = vm_offset_t;
    type processor_flavor_t = i32;
    type natural_t = u32;
    type processor_info_array_t = *mut i32;
    type mach_msg_type_number_t = i32;
    type kern_return_t = i32;

    const PROESSOR_CPU_LOAD_INFO: processor_flavor_t = 2;
    const CPU_STATE_USER: usize = 0;
    const CPU_STATE_SYSTEM: usize = 1;
    const CPU_STATE_IDLE: usize = 2;
    const CPU_STATE_NICE: usize = 3;
    const CPU_STATE_MAX: usize = 4;

    extern "C" {
        static mut mach_task_self_: mach_port_t;

        fn mach_host_self() -> mach_port_t;
        fn host_processor_info(
            host: host_t,
            flavor: processor_flavor_t,
            out_processor_count: *mut natural_t,
            out_processor_info: *mut processor_info_array_t,
            out_processor_infoCnt: *mut mach_msg_type_number_t,
        ) -> kern_return_t;
        fn vm_deallocate(
            target_task: vm_map_t,
            address: vm_address_t,
            size: vm_size_t,
        ) -> kern_return_t;
    }

    #[repr(C)]
    struct processor_cpu_load_info_data_t {
        cpu_ticks: [u32; CPU_STATE_MAX],
    }

    pub fn current() -> io::Result<super::LiunxStyleCpuTime> {
        // There's scant little documentation on `host_processor_info`
        // throughout the internet, so this is just modeled after what everyone
        // else is doing. For now this is modeled largely after libuv.

        unsafe {
            let mut num_cpus_u = 0;
            let mut cpu_info = ptr::null_mut();
            let mut msg_type = 0;
            let err = host_processor_info(
                mach_host_self(),
                PROESSOR_CPU_LOAD_INFO,
                &mut num_cpus_u,
                &mut cpu_info,
                &mut msg_type,
            );
            if err != 0 {
                return Err(io::Error::last_os_error());
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
                ret.user += (*current).cpu_ticks[CPU_STATE_USER] as u64;
                ret.system += (*current).cpu_ticks[CPU_STATE_SYSTEM] as u64;
                ret.idle += (*current).cpu_ticks[CPU_STATE_IDLE] as u64;
                ret.nice += (*current).cpu_ticks[CPU_STATE_NICE] as u64;
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
