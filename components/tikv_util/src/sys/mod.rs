// Copyright 2017 TiKV Project Authors. Licensed under Apache-2.0.

pub mod cpu_time;

#[cfg(target_os = "linux")]
mod cgroup;

// re-export some traits for ease of use
pub use sysinfo::{DiskExt, NetworkExt, ProcessExt, ProcessorExt, SystemExt};

use std::sync::Mutex;

lazy_static! {
    pub static ref SYS_INFO: Mutex<sysinfo::System> = Mutex::new(sysinfo::System::new());
}

#[cfg(target_os = "linux")]
pub mod sys_quota {
    use super::super::config::KB;
    use super::{cgroup::CGroupSys, SystemExt, SYS_INFO};

    pub struct SysQuota {
        cgroup: CGroupSys,
    }

    impl SysQuota {
        pub fn new() -> Self {
            Self {
                cgroup: CGroupSys::default(),
            }
        }

        pub fn cpu_cores_quota(&self) -> f64 {
            let cpu_num = num_cpus::get() as f64;
            let quota = match self.cgroup.cpu_cores_quota() {
                Some(cgroup_quota) if cgroup_quota > 0.0 && cgroup_quota < cpu_num => cgroup_quota,
                _ => cpu_num,
            };
            super::limit_cpu_cores_quota_by_env_var(quota)
        }

        pub fn memory_limit_in_bytes(&self) -> u64 {
            let total_mem = {
                let mut system = SYS_INFO.lock().unwrap();
                system.refresh_memory();
                system.get_total_memory() * KB
            };
            let cgroup_memory_limits = self.cgroup.memory_limit_in_bytes();
            if cgroup_memory_limits <= 0 {
                total_mem
            } else {
                std::cmp::min(total_mem, cgroup_memory_limits as u64)
            }
        }

        pub fn log_quota(&self) {
            info!(
                "memory limit in bytes: {}, cpu cores quota: {}",
                self.memory_limit_in_bytes(),
                self.cpu_cores_quota()
            );
        }
    }
}

#[cfg(not(target_os = "linux"))]
pub mod sys_quota {
    use super::super::config::KB;
    use super::{env_var_cpu_cores_quota, SystemExt, SYS_INFO};

    pub struct SysQuota {}

    impl SysQuota {
        pub fn new() -> Self {
            Self {}
        }

        pub fn cpu_cores_quota(&self) -> f64 {
            let cpu_num = num_cpus::get() as f64;
            super::limit_cpu_cores_quota_by_env_var(cpu_num)
        }

        pub fn memory_limit_in_bytes(&self) -> u64 {
            let mut system = SYS_INFO.lock().unwrap();
            system.refresh_memory();
            system.get_total_memory() * KB
        }

        pub fn log_quota(&self) {
            info!(
                "memory limit in bytes: {}, cpu cores quota: {}",
                self.memory_limit_in_bytes(),
                self.cpu_cores_quota()
            );
        }
    }
}

pub const HIGH_PRI: i32 = -1;

const CPU_CORES_QUOTA_ENV_VAR_KEY: &str = "TIKV_CPU_CORES_QUOTA";

fn limit_cpu_cores_quota_by_env_var(quota: f64) -> f64 {
    match std::env::var(CPU_CORES_QUOTA_ENV_VAR_KEY)
        .ok()
        .and_then(|value| value.parse().ok())
    {
        Some(env_var_quota) if quota.is_sign_positive() => f64::min(quota, env_var_quota),
        _ => quota,
    }
}

#[cfg(target_os = "linux")]
pub mod thread {
    use libc::{self, c_int};
    use std::io::Error;

    pub fn set_priority(pri: i32) -> Result<(), Error> {
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

    pub fn get_priority() -> Result<i32, Error> {
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
        use super::super::HIGH_PRI;
        use super::*;
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
pub mod thread {
    use std::io::Error;

    pub fn set_priority(_: i32) -> Result<(), Error> {
        Ok(())
    }

    pub fn get_priority() -> Result<i32, Error> {
        Ok(0)
    }
}
