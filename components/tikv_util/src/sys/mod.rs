// Copyright 2017 TiKV Project Authors. Licensed under Apache-2.0.

// re-export some traits for ease of use
use std::{
    collections::HashMap,
    path::Path,
    sync::atomic::{AtomicU64, Ordering},
};

use fail::fail_point;
#[cfg(target_os = "linux")]
use lazy_static::lazy_static;
pub use sysinfo::{Cpu, Disks, NetworkData, Process, System};
use sysinfo::{LoadAvg, MemoryRefreshKind, Networks, Pid, ProcessesToUpdate, RefreshKind};

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

    #[cfg(target_os = "linux")]
    pub fn cpu_cores_quota_current() -> f64 {
        let cgroup = cgroup::CGroupSys::new().unwrap_or_default();
        let mut cpu_num = num_cpus::get() as f64;
        let cpuset_cores = cgroup.cpuset_cores().len() as f64;
        let cpu_quota = cgroup.cpu_quota().unwrap_or(0.);

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

    #[cfg(not(target_os = "linux"))]
    pub fn cpu_cores_quota_current() -> f64 {
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

    #[cfg(target_os = "linux")]
    pub fn memory_limit_in_bytes_current() -> u64 {
        let cgroup = cgroup::CGroupSys::new().unwrap_or_default();
        let total_mem = Self::sysinfo_memory_limit_in_bytes();
        if let Some(cgroup_memory_limit) = cgroup.memory_limit_in_bytes() {
            std::cmp::min(total_mem, cgroup_memory_limit)
        } else {
            total_mem
        }
    }

    #[cfg(not(target_os = "linux"))]
    pub fn memory_limit_in_bytes() -> u64 {
        Self::sysinfo_memory_limit_in_bytes()
    }

    #[cfg(not(target_os = "linux"))]
    pub fn memory_limit_in_bytes_current() -> u64 {
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
        let system = sysinfo::System::new_with_specifics(
            RefreshKind::new().with_memory(MemoryRefreshKind::everything()),
        );
        // If cgroup limits are available, use the minimum of cgroup and system limits.
        // TODO: replace self-defined `CgroupSys` with sysinfo::CgroupLimits later.
        std::cmp::min(
            system.total_memory(),
            system
                .cgroup_limits()
                .map_or(u64::MAX, |cgroups| cgroups.total_memory),
        )
    }
}

/// A wrapper of `sysinfo::System`, `sysinfo::Disks` and `sysinfo::Networks` to
/// provide more functionalities. It's used to get system information and
/// refresh them periodically.
///
/// Typically, it's used in `DiagnosticsService` to provide system information.
pub struct SystemInfo {
    system: System,
    disks: Disks,
    networks: Networks,
}

impl Default for SystemInfo {
    fn default() -> Self {
        SystemInfo::new()
    }
}

impl SystemInfo {
    #[inline]
    fn new() -> Self {
        SystemInfo {
            system: System::new_all(),
            disks: Disks::new_with_refreshed_list(),
            networks: Networks::new_with_refreshed_list(),
        }
    }

    #[inline]
    pub fn system(&self) -> &System {
        &self.system
    }

    #[inline]
    pub fn disks(&self) -> &Disks {
        &self.disks
    }

    #[inline]
    pub fn networks(&self) -> &Networks {
        &self.networks
    }

    #[inline]
    pub fn processes(&self) -> &HashMap<Pid, Process> {
        self.system.processes()
    }

    #[inline]
    pub fn system_load_average(&self) -> LoadAvg {
        sysinfo::System::load_average()
    }

    #[inline]
    pub fn refresh_all(&mut self) {
        self.system.refresh_all();
        self.refresh_networks();
        self.refresh_disks();
    }

    #[inline]
    pub fn refresh_cpu(&mut self) {
        self.system.refresh_cpu_all();
    }

    #[inline]
    pub fn refresh_memory(&mut self) {
        self.system.refresh_memory();
    }

    #[inline]
    pub fn refresh_processes(&mut self) {
        self.system.refresh_processes(ProcessesToUpdate::All, true);
    }

    #[inline]
    pub fn refresh_networks(&mut self) {
        self.networks.refresh_list();
        self.networks.refresh();
    }

    #[inline]
    pub fn refresh_disks(&mut self) {
        self.disks.refresh_list();
        self.disks.refresh();
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
    let get_mount = |path| -> std::io::Result<_> {
        let mounts = std::fs::File::open("/proc/mounts")?;
        let mount_point = get_path_mount_point(Box::new(std::io::BufReader::new(mounts)), path);
        Ok(mount_point)
    };

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

#[cfg(target_os = "linux")]
fn get_path_mount_point(mounts: Box<dyn std::io::BufRead>, path: &Path) -> Option<String> {
    use std::io::BufRead;

    // (fs_file, mount point)
    let mut ret = None;
    // Each filesystem is described on a separate line. Fields on each line are
    // separated by tabs or spaces. Lines starting with '#' are comments.
    // Blank lines are ignored.
    // See man 5 fstab.
    for line in mounts.lines() {
        let line = match line {
            Ok(line) => line,
            Err(e) => {
                warn!("fail to read mounts line, error {}", e);
                continue;
            }
        };
        if line.is_empty() || line.starts_with('#') {
            continue;
        }
        // We only care about the second field (fs_file).
        let mut idx = 0;
        for field in line.split(&[' ', '\t']) {
            if field.is_empty() {
                continue;
            }
            if idx == 1 {
                if path.starts_with(field) {
                    // Keep the longest match.
                    if ret.as_ref().map_or(0, |r: &(String, String)| r.0.len()) < field.len() {
                        ret = Some((field.to_owned(), line.clone()));
                    }
                }
                break;
            }
            idx += 1;
        }
    }
    ret.map(|r| r.1)
}

#[cfg(not(target_os = "linux"))]
pub fn path_in_diff_mount_point(_path1: impl AsRef<Path>, _path2: impl AsRef<Path>) -> bool {
    false
}

#[cfg(unix)]
pub fn hostname() -> Option<String> {
    let mut buf = [0u8; 128];
    let os_name = nix::unistd::gethostname(&mut buf).ok()?;
    Some(os_name.to_string_lossy().to_string())
}

#[cfg(not(unix))]
pub fn hostname() -> Option<String> {
    None
}

#[cfg(test)]
mod tests {
    use super::*;

    #[cfg(unix)]
    #[test]
    fn test_hostname() {
        use std::process::Command;

        let hn = Command::new("hostname").output().map(|mut output| {
            output.stdout.pop();
            // Remove the '\n'.
            output.stdout
        });
        match hn {
            Err(err) => eprintln!(
                "cannot run test_hostname: `hostname` CLI not installed or failed to run: {}",
                err
            ),
            Ok(hn) => assert_eq!(&hn, hostname().unwrap().as_bytes()),
        }
    }

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

    #[cfg(target_os = "linux")]
    #[test]
    fn test_get_path_mount_point() {
        let mounts = "
sysfs /sys sysfs rw,nosuid,nodev,noexec,relatime 0 0
proc /proc proc rw,nosuid,nodev,noexec,relatime 0 0
tmpfs /sys/fs/cgroup tmpfs ro,nosuid,nodev,noexec,mode=755 0 0
cgroup /sys/fs/cgroup/systemd cgroup rw,nosuid,nodev,noexec,relatime,xattr,release_agent=/usr/lib/systemd/systemd-cgroups-agent,name=systemd 0 0
pstore /sys/fs/pstore pstore rw,nosuid,nodev,noexec,relatime 0 0
bpf /sys/fs/bpf bpf rw,nosuid,nodev,noexec,relatime,mode=700 0 0
none /sys/kernel/tracing tracefs rw,relatime 0 0
configfs /sys/kernel/config configfs rw,relatime 0 0
systemd-1 /proc/sys/fs/binfmt_misc autofs rw,relatime,fd=32,pgrp=1,timeout=0,minproto=5,maxproto=5,direct,pipe_ino=16122 0 0
mqueue /dev/mqueue mqueue rw,relatime 0 0
/dev/vda2 /boot ext4 rw,relatime 0 0
/dev/vda3 / ext4 rw,relatime 0 0

# Double spaces in below.
/dev/nvme1n1  /data/nvme1n1  xfs  rw,seclabel,relatime,attr2,inode64,logbufs=8,logbsize=32k,noquota 0 0
# \\t in below.
/dev/nvme0n1\t/data/nvme0n1/data ext4 rw,seclabel,relatime 0 0
";
        let reader = mounts.as_bytes();
        let check = |path: &str, expected: Option<&str>| {
            let mp = get_path_mount_point(Box::new(reader), Path::new(path));
            if let Some(expected) = expected {
                assert!(
                    mp.as_ref().unwrap().starts_with(expected),
                    "{:?}: {:?}",
                    mp,
                    expected
                );
            } else {
                assert!(mp.is_none(), "{:?}: {:?}", mp, expected);
            };
        };
        check("/data/nvme1n1", Some("/dev/nvme1n1  /data/nvme1n1  xfs"));
        check(
            "/data/nvme0n1/data/tikv",
            Some("/dev/nvme0n1\t/data/nvme0n1/data ext4"),
        );
        check("/data/nvme0n1", Some("/dev/vda3 / ext4"));
        check("/home", Some("/dev/vda3 / ext4"));
        check("unknown/path", None);
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
