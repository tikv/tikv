// Copyright 2020 TiKV Project Authors. Licensed under Apache-2.0.
// Modified from https://github.com/rust-lang/cargo/blob/426fae51f39ebf6c545a2c12f78bc09fbfdb7aa9/src/cargo/util/cpu.rs
// TODO: Maybe use https://github.com/heim-rs/heim is better after https://github.com/heim-rs/heim/issues/233 is fixed.

use std::{
    io, mem,
    time::{Duration, Instant},
};

use derive_more::{Add, Sub};

#[derive(Add, Sub)]
pub struct LinuxStyleCpuTime {
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

impl LinuxStyleCpuTime {
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

    pub fn current() -> io::Result<LinuxStyleCpuTime> {
        imp::current()
    }
}

pub use std::io::Result;

pub use imp::cpu_time;

/// A struct to monitor process cpu usage
#[derive(Clone, Copy)]
pub struct ProcessStat {
    current_time: Instant,
    cpu_time: Duration,
}

impl ProcessStat {
    pub fn cur_proc_stat() -> io::Result<Self> {
        Ok(ProcessStat {
            current_time: Instant::now(),
            cpu_time: imp::cpu_time()?,
        })
    }

    /// return the cpu usage from last invoke,
    /// or when this struct created if it is the first invoke.
    pub fn cpu_usage(&mut self) -> io::Result<f64> {
        let new_time = imp::cpu_time()?;
        let old_time = mem::replace(&mut self.cpu_time, new_time);

        let old_now = mem::replace(&mut self.current_time, Instant::now());
        let real_time = self.current_time.duration_since(old_now).as_secs_f64();

        if real_time > 0.0 {
            let cpu_time = new_time
                .checked_sub(old_time)
                .map(|dur| dur.as_secs_f64())
                .unwrap_or(0.0);

            Ok(cpu_time / real_time)
        } else {
            Ok(0.0)
        }
    }
}

#[cfg(target_os = "linux")]
mod imp {
    use std::{fs::File, io, io::Read, time::Duration};

    pub fn current() -> io::Result<super::LinuxStyleCpuTime> {
        let mut state = String::new();
        File::open("/proc/stat")?.read_to_string(&mut state)?;

        (|| {
            let mut parts = state.lines().next()?.split_whitespace();
            if parts.next()? != "cpu" {
                return None;
            }
            Some(super::LinuxStyleCpuTime {
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

    pub fn cpu_time() -> io::Result<Duration> {
        let mut time = libc::timespec {
            tv_sec: 0,
            tv_nsec: 0,
        };

        if unsafe { libc::clock_gettime(libc::CLOCK_PROCESS_CPUTIME_ID, &mut time) } == 0 {
            Ok(Duration::new(time.tv_sec as u64, time.tv_nsec as u32))
        } else {
            Err(io::Error::last_os_error())
        }
    }
}

#[cfg(target_os = "macos")]
mod imp {
    use std::{io, ptr};

    use libc::*;

    pub fn current() -> io::Result<super::LinuxStyleCpuTime> {
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

            let mut ret = super::LinuxStyleCpuTime {
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

    pub fn cpu_time() -> io::Result<std::time::Duration> {
        let mut time = unsafe { std::mem::zeroed() };

        if unsafe { libc::getrusage(libc::RUSAGE_SELF, &mut time) } == 0 {
            let sec = time.ru_utime.tv_sec as u64 + time.ru_stime.tv_sec as u64;
            let nsec = (time.ru_utime.tv_usec as u32 + time.ru_stime.tv_usec as u32) * 1000;

            Ok(std::time::Duration::new(sec, nsec))
        } else {
            Err(io::Error::last_os_error())
        }
    }
}

#[cfg(not(any(target_os = "linux", target_os = "macos")))]
mod imp {
    use std::io;

    pub fn current() -> io::Result<super::LinuxStyleCpuTime> {
        Err(io::Error::new(
            io::ErrorKind::Other,
            "unsupported platform to learn CPU state",
        ))
    }

    use std::{io, mem, time::Duration};

    use scopeguard::defer;
    use winapi::{
        shared::{
            minwindef::FILETIME,
            ntdef::{FALSE, NULL},
        },
        um::{
            handleapi::CloseHandle,
            processthreadsapi::{
                GetCurrentProcess, GetCurrentThreadId, GetProcessTimes, GetSystemTimes,
                GetThreadTimes, OpenThread,
            },
            sysinfoapi::{GetSystemInfo, SYSTEM_INFO},
            winnt::THREAD_QUERY_INFORMATION,
        },
    };

    /// convert to u64, unit 100 ns
    fn filetime_to_ns100(ft: FILETIME) -> u64 {
        ((ft.dwHighDateTime as u64) << 32) + ft.dwLowDateTime as u64
    }

    fn get_sys_times() -> io::Result<(u64, u64, u64)> {
        let mut idle = FILETIME::default();
        let mut kernel = FILETIME::default();
        let mut user = FILETIME::default();

        let ret = unsafe { GetSystemTimes(&mut idle, &mut kernel, &mut user) };
        if ret == 0 {
            return Err(io::Error::last_os_error());
        }

        let idle = filetime_to_ns100(idle);
        let kernel = filetime_to_ns100(kernel);
        let user = filetime_to_ns100(user);
        Ok((idle, kernel, user))
    }

    fn get_thread_times(tid: u32) -> io::Result<(u64, u64)> {
        let handler = unsafe { OpenThread(THREAD_QUERY_INFORMATION, FALSE as i32, tid) };
        if handler == NULL {
            return Err(io::Error::last_os_error());
        }
        defer! {{
            unsafe { CloseHandle(handler) };
        }}

        let mut create_time = FILETIME::default();
        let mut exit_time = FILETIME::default();
        let mut kernel_time = FILETIME::default();
        let mut user_time = FILETIME::default();

        let ret = unsafe {
            GetThreadTimes(
                handler,
                &mut create_time,
                &mut exit_time,
                &mut kernel_time,
                &mut user_time,
            )
        };
        if ret == 0 {
            return Err(io::Error::last_os_error());
        }

        let kernel_time = filetime_to_ns100(kernel_time);
        let user_time = filetime_to_ns100(user_time);
        Ok((kernel_time, user_time))
    }

    #[inline]
    pub fn cpu_time() -> io::Result<Duration> {
        let (kernel_time, user_time) = unsafe {
            let process = GetCurrentProcess();
            let mut create_time = mem::zeroed();
            let mut exit_time = mem::zeroed();
            let mut kernel_time = mem::zeroed();
            let mut user_time = mem::zeroed();

            let ret = GetProcessTimes(
                process,
                &mut create_time,
                &mut exit_time,
                &mut kernel_time,
                &mut user_time,
            );

            if ret != 0 {
                (kernel_time, user_time)
            } else {
                return Err(io::Error::last_os_error());
            }
        };

        let kt = filetime_to_ns100(kernel_time);
        let ut = filetime_to_ns100(user_time);

        // convert ns
        //
        // Note: make it ns unit may overflow in some cases.
        // For example, a machine with 128 cores runs for one year.
        let cpu = (kt + ut) * 100;

        // make it un-normalized
        let cpu = cpu * processor_numbers()? as u64;

        Ok(Duration::from_nanos(cpu))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // this test should be executed alone.
    #[test]
    fn test_process_usage() {
        let mut stat = ProcessStat::cur_proc_stat().unwrap();

        std::thread::sleep(std::time::Duration::from_secs(1));

        let usage = stat.cpu_usage().unwrap();

        assert!(usage < 0.01);

        let num = 1;
        for _ in 0..num * 10 {
            std::thread::spawn(move || {
                loop {
                    let _ = (0..10_000_000).into_iter().sum::<u128>();
                }
            });
        }

        std::thread::sleep(std::time::Duration::from_secs(1));

        let usage = stat.cpu_usage().unwrap();

        assert!(usage > 0.9_f64)
    }
}
