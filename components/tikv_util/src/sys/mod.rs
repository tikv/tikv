// Copyright 2017 TiKV Project Authors. Licensed under Apache-2.0.

// re-export some traits for ease of use
use std::{
    path::Path,
    sync::atomic::{AtomicU64, Ordering},
};

use fail::fail_point;
#[cfg(target_os = "linux")]
use lazy_static::lazy_static;
#[cfg(target_os = "linux")]
use mnt::get_mount;
use sysinfo::RefreshKind;
pub use sysinfo::{CpuExt, DiskExt, NetworkExt, ProcessExt, SystemExt};

use crate::config::ReadableSize;

#[cfg(target_os = "linux")]
mod cgroup;
pub mod cpu_time;
pub mod disk;
pub mod inspector;
pub mod ioload;
pub mod thread;

pub const HIGH_PRI: i32 = -1;
const CPU_CORES_QUOTA_ENV_VAR_KEY: &str = "TIKV_CPU_CORES_QUOTA";

static GLOBAL_MEMORY_USAGE: AtomicU64 = AtomicU64::new(0);
static MEMORY_USAGE_HIGH_WATER: AtomicU64 = AtomicU64::new(u64::MAX);

// MEMORY_HIGH_WATER_MARGIN_SPLIT_POINT is used to decide whether to use a fixed
// margin or a percentage-based margin, ensuring a reasonable margin value on
// both large and small memory machines.
const MEMORY_HIGH_WATER_MARGIN_SPLIT_POINT: u64 = 10 * 1024 * 1024 * 1024; // 10GB
const MEMORY_HIGH_WATER_FIXED_MARGIN: u64 = 1024 * 1024 * 1024; // 1GB
const MEMORY_HIGH_WATER_PERCENTAGE_MARGIN: f64 = 0.9; // Empirical value.

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
        if let Some(cgroup_memory_limit) = SELF_CGROUP.memory_limit_in_bytes() {
            std::cmp::min(total_mem, cgroup_memory_limit)
        } else {
            total_mem
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
        system.total_memory()
    }
}

/// Get the current global memory usage in bytes. Users need to call
/// `record_global_memory_usage` to refresh it periodically.
pub fn get_global_memory_usage() -> u64 {
    fail_point!("mock_memory_usage", |t| {
        t.unwrap().parse::<u64>().unwrap()
    });
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

/// Register the high water mark so that `memory_usage_reaches_high_water` is
/// available.
pub fn register_memory_usage_high_water(mark: u64) {
    MEMORY_USAGE_HIGH_WATER.store(mark, Ordering::Release);
}

fn get_memory_usage_high_water() -> u64 {
    fail_point!("mock_memory_usage_high_water", |t| {
        t.unwrap().parse::<u64>().unwrap()
    });
    MEMORY_USAGE_HIGH_WATER.load(Ordering::Acquire)
}

pub fn memory_usage_reaches_high_water(usage: &mut u64) -> bool {
    fail_point!("memory_usage_reaches_high_water", |_| true);

    *usage = get_global_memory_usage();
    let high_water = get_memory_usage_high_water();

    *usage >= high_water
}

pub fn memory_usage_reaches_near_high_water(usage: &mut u64) -> bool {
    *usage = get_global_memory_usage();
    let high_water = get_memory_usage_high_water();

    memory_usage_reaches_near_high_water_internal(*usage, high_water)
}

// Internal function for easier testing.
fn memory_usage_reaches_near_high_water_internal(usage: u64, high_water: u64) -> bool {
    if usage > MEMORY_HIGH_WATER_MARGIN_SPLIT_POINT {
        usage >= high_water - MEMORY_HIGH_WATER_FIXED_MARGIN
    } else {
        (usage as f64) >= (high_water as f64) * MEMORY_HIGH_WATER_PERCENTAGE_MARGIN
    }
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

#[cfg(target_os = "linux")]
pub fn path_in_diff_mount_point(path1: impl AsRef<Path>, path2: impl AsRef<Path>) -> bool {
    let (path1, path2) = (path1.as_ref(), path2.as_ref());
    let empty_path = |p: &Path| p.to_str().map_or(false, |s| s.is_empty());
    if empty_path(path1) || empty_path(path2) {
        return false;
    }
    match (get_mount(path1), get_mount(path2)) {
        (Err(e1), _) => {
            warn!("Get mount point error for path {}, {}", path1.display(), e1);
            false
        }
        (_, Err(e2)) => {
            warn!("Get mount point error for path {}, {}", path2.display(), e2);
            false
        }
        (Ok(None), _) => {
            warn!("No mount point for {}", path1.display());
            false
        }
        (_, Ok(None)) => {
            warn!("No mount point for {}", path2.display());
            false
        }
        (Ok(Some(mount1)), Ok(Some(mount2))) => mount1 != mount2,
    }
}

#[cfg(not(target_os = "linux"))]
pub fn path_in_diff_mount_point(_path1: impl AsRef<Path>, _path2: impl AsRef<Path>) -> bool {
    false
}

#[cfg(test)]
mod tests {
    use super::*;

    #[cfg(target_os = "linux")]
    #[test]
    fn test_path_in_diff_mount_point() {
        let (empty_path1, path2) = ("", "/");
        let result = path_in_diff_mount_point(empty_path1, path2);
        assert_eq!(result, false);

        let (no_mount_point_path, path2) = ("no_mount_point_path_w943nn", "/");
        let result = path_in_diff_mount_point(no_mount_point_path, path2);
        assert_eq!(result, false);

        let (not_existed_path, path2) = ("/non_existed_path_eu2yndh", "/");
        let result = path_in_diff_mount_point(not_existed_path, path2);
        assert_eq!(result, false);

        let (normal_path1, normal_path2) = ("/", "/");
        let result = path_in_diff_mount_point(normal_path1, normal_path2);
        assert_eq!(result, false);
    }

    #[test]
    fn test_get_disk_space_stats() {
        let (capacity, available) = disk::get_disk_space_stats("./").unwrap();
        assert!(capacity > 0);
        assert!(available > 0);
        assert!(capacity >= available);

        disk::get_disk_space_stats("/non-exist-path").unwrap_err();
    }
    #[test]
    fn test_near_high_water() {
        // Below MEMORY_HIGH_WATER_MARGIN_SPLIT_POINT.
        let is_near = memory_usage_reaches_near_high_water_internal(
            8500 * 1024 * 1024,
            9 * 1024 * 1024 * 1024,
        );
        assert!(is_near);

        // Above MEMORY_HIGH_WATER_MARGIN_SPLIT_POINT.
        let is_near = memory_usage_reaches_near_high_water_internal(
            11 * 1024 * 1024 * 1024,
            12 * 1024 * 1024 * 1024,
        );
        assert!(is_near);
    }

    #[test]
    fn test_not_near_high_water() {
        // Below MEMORY_HIGH_WATER_MARGIN_SPLIT_POINT.
        let is_near = memory_usage_reaches_near_high_water_internal(
            8 * 1024 * 1024 * 1024,
            9 * 1024 * 1024 * 1024,
        );
        assert!(!is_near);

        // Above MEMORY_HIGH_WATER_MARGIN_SPLIT_POINT.
        let is_near = memory_usage_reaches_near_high_water_internal(
            10 * 1024 * 1024 * 1024,
            12 * 1024 * 1024 * 1024,
        );
        assert!(!is_near);
    }
}
