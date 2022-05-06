// Copyright 2017 TiKV Project Authors. Licensed under Apache-2.0.

#[cfg(target_os = "linux")]
mod cgroup;
pub mod cpu_time;
pub mod disk;
pub mod inspector;
pub mod thread;

// re-export some traits for ease of use
use std::sync::atomic::{AtomicU64, Ordering};

use fail::fail_point;
#[cfg(target_os = "linux")]
use lazy_static::lazy_static;
use sysinfo::RefreshKind;
pub use sysinfo::{DiskExt, NetworkExt, ProcessExt, ProcessorExt, SystemExt};

use crate::config::{ReadableSize, KIB};

pub const HIGH_PRI: i32 = -1;
const CPU_CORES_QUOTA_ENV_VAR_KEY: &str = "TIKV_CPU_CORES_QUOTA";

static GLOBAL_MEMORY_USAGE: AtomicU64 = AtomicU64::new(0);
static MEMORY_USAGE_HIGH_WATER: AtomicU64 = AtomicU64::new(u64::MAX);

#[cfg(target_os = "linux")]
lazy_static! {
    static ref SELF_CGROUP: cgroup::CGroupSys = cgroup::CGroupSys::new().unwrap_or_default();
}

pub struct SysQuota;
impl SysQuota {
    #[cfg(target_os = "linux")]
    pub fn cpu_cores_quota() -> f64 {
        let mut cpu_num = num_cpus::get() as f64;
        let cpuset_cores = SELF_CGROUP.cpuset_cores().len() as f64;
        let cpu_quota = SELF_CGROUP.cpu_quota().unwrap_or(0.);

        if cpuset_cores != 0. {
            cpu_num = cpu_num.min(cpuset_cores);
        }

        if cpu_quota != 0. {
            cpu_num = cpu_num.min(cpu_quota);
        }

        limit_cpu_cores_quota_by_env_var(cpu_num)
    }

    #[cfg(not(target_os = "linux"))]
    pub fn cpu_cores_quota() -> f64 {
        let cpu_num = num_cpus::get() as f64;
        limit_cpu_cores_quota_by_env_var(cpu_num)
    }

    #[cfg(target_os = "linux")]
    pub fn memory_limit_in_bytes() -> u64 {
        let total_mem = Self::sysinfo_memory_limit_in_bytes();
        let cgroup_memory_limit = SELF_CGROUP.memory_limit_in_bytes();
        if cgroup_memory_limit <= 0 {
            total_mem
        } else {
            std::cmp::min(total_mem, cgroup_memory_limit as u64)
        }
    }

    #[cfg(not(target_os = "linux"))]
    pub fn memory_limit_in_bytes() -> u64 {
        Self::sysinfo_memory_limit_in_bytes()
    }

    pub fn log_quota() {
        #[cfg(target_os = "linux")]
        info!(
            "cgroup quota: memory={:?}, cpu={:?}, cores={:?}",
            SELF_CGROUP.memory_limit_in_bytes(),
            SELF_CGROUP.cpu_quota(),
            SELF_CGROUP.cpuset_cores(),
        );

        info!(
            "memory limit in bytes: {}, cpu cores quota: {}",
            Self::memory_limit_in_bytes(),
            Self::cpu_cores_quota()
        );
    }

    fn sysinfo_memory_limit_in_bytes() -> u64 {
        let system = sysinfo::System::new_with_specifics(RefreshKind::new().with_memory());
        system.get_total_memory() * KIB
    }
}

/// Get the current global memory usage in bytes. Users need to call `record_global_memory_usage`
/// to refresh it periodically.
pub fn get_global_memory_usage() -> u64 {
    GLOBAL_MEMORY_USAGE.load(Ordering::Acquire)
}

/// Record the current global memory usage of the process.
#[cfg(target_os = "linux")]
pub fn record_global_memory_usage() {
    let s = procinfo::pid::statm_self().unwrap();
    let usage = s.resident * page_size::get();
    GLOBAL_MEMORY_USAGE.store(usage as u64, Ordering::Release);
}

#[cfg(not(target_os = "linux"))]
pub fn record_global_memory_usage() {
    GLOBAL_MEMORY_USAGE.store(0, Ordering::Release);
}

/// Register the high water mark so that `memory_usage_reaches_high_water` is available.
pub fn register_memory_usage_high_water(mark: u64) {
    MEMORY_USAGE_HIGH_WATER.store(mark, Ordering::Release);
}

pub fn memory_usage_reaches_high_water(usage: &mut u64) -> bool {
    fail_point!("memory_usage_reaches_high_water", |_| true);
    *usage = get_global_memory_usage();
    *usage >= MEMORY_USAGE_HIGH_WATER.load(Ordering::Acquire)
}

fn limit_cpu_cores_quota_by_env_var(quota: f64) -> f64 {
    match std::env::var(CPU_CORES_QUOTA_ENV_VAR_KEY)
        .ok()
        .and_then(|value| value.parse().ok())
    {
        Some(env_var_quota) if quota.is_sign_positive() => f64::min(quota, env_var_quota),
        _ => quota,
    }
}

fn read_size_in_cache(level: usize, field: &str) -> Option<u64> {
    std::fs::read_to_string(format!(
        "/sys/devices/system/cpu/cpu0/cache/index{}/{}",
        level, field
    ))
    .ok()
    .and_then(|s| s.parse::<ReadableSize>().ok())
    .map(|s| s.0)
}

/// Gets the size of given level cache.
///
/// It will only return `Some` on Linux.
pub fn cache_size(level: usize) -> Option<u64> {
    read_size_in_cache(level, "size")
}

/// Gets the size of given level cache line.
///
/// It will only return `Some` on Linux.
pub fn cache_line_size(level: usize) -> Option<u64> {
    read_size_in_cache(level, "coherency_line_size")
}
