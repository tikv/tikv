// Copyright 2020 TiKV Project Authors. Licensed under Apache-2.0.

use std::collections::{HashMap, HashSet};
use std::fs::{read_to_string, File};
use std::io::prelude::*;
use std::mem::MaybeUninit;
use std::process;

// ## Differences between cgroup v1 and v2:
// ### memory subsystem, memory limitation
// v2:
//   * path: /sys/fs/cgroup/<path-to-group>/memory.max
//   * content: a number or "max" for no limitation
// v1:
//   * path: /sys/fs/cgroup/memory/<path-to-group>/memory.limit_in_bytes
//   * content: a number or "-1" for no limitation
// ### cpu subsystem, cpu quota
// v2:
//   * path: /sys/fs/cgroup/<path-to-group>/cpu.max
//   * content: 2 numbers seperated with a blank, "max" indicates no limitation.
// v1:
//   * path: /sys/fs/cgroup/cpu/<path-to-group>/cpu.cfs_quota_us
//   * content: a number
//   * path: /sys/fs/cgroup/cpu/<path-to-group>/cpu.cfs_period_us
//   * content: a number
// ### cpuset subsystem, cpus
// v2:
//   * path: /sys/fs/cgroup/<path-to-group>/cpuset.cpus
// v1:
//   * path: /sys/fs/cgroup/cpuset/<path-to-group>/cpuset.cpus

pub struct CGroupSys {
    cgroups: HashMap<String, String>,
    is_v2: bool,
}

impl CGroupSys {
    pub fn new() -> Self {
        let lines = read_to_string(&format!("/proc/{}/cgroup", process::id())).unwrap();
        let is_v2 = is_cgroup2_unified_mode();
        let cgroups = if !is_v2 {
            parse_proc_cgroup_v1(&lines)
        } else {
            parse_proc_cgroup_v2(&lines)
        };
        CGroupSys { cgroups, is_v2 }
    }

    /// -1 means no limit.
    pub fn memory_limit_in_bytes(&self) -> i64 {
        if self.is_v2 {
            let group = self.cgroups.get("").unwrap();
            let path = format!("/sys/fs/cgroup/{}/memory.max", group);
            if let Ok(mut f) = File::open(&path) {
                let mut buffer = String::new();
                f.read_to_string(&mut buffer).unwrap();
                return parse_memory_max(buffer.trim());
            }
        } else {
            let group = self.cgroups.get("memory").unwrap();
            let path = format!("/sys/fs/cgroup/memory/{}/memory.limit_in_bytes", group);
            if let Ok(mut f) = File::open(&path) {
                let mut buffer = String::new();
                f.read_to_string(&mut buffer).unwrap();
                return parse_memory_max(buffer.trim());
            }
        }
        -1
    }

    pub fn cpuset_cores(&self) -> HashSet<usize> {
        if self.is_v2 {
            let group = self.cgroups.get("").unwrap();
            let path = format!("/sys/fs/cgroup/{}/cpuset.cpus", group);
            if let Ok(mut f) = File::open(&path) {
                let mut buffer = String::new();
                f.read_to_string(&mut buffer).unwrap();
                return parse_cpu_cores(buffer.trim());
            }
        } else {
            let group = self.cgroups.get("cpuset").unwrap();
            let path = format!("/sys/fs/cgroup/cpuset/{}/cpuset.cpus", group);
            if let Ok(mut f) = File::open(&path) {
                let mut buffer = String::new();
                f.read_to_string(&mut buffer).unwrap();
                return parse_cpu_cores(buffer.trim());
            }
        }
        HashSet::new()
    }

    /// None means no limit.
    pub fn cpu_quota(&self) -> Option<f64> {
        if self.is_v2 {
            let group = self.cgroups.get("").unwrap();
            let path = format!("/sys/fs/cgroup/{}/cpu.max", group);
            if let Ok(mut f) = File::open(&path) {
                let mut buffer = String::new();
                f.read_to_string(&mut buffer).unwrap();
                return parse_cpu_quota_v2(buffer.trim());
            }
        } else {
            let group = self.cgroups.get("cpu").unwrap();
            let path1 = format!("/sys/fs/cgroup/cpu/{}/cpu.cfs_quota_us", group);
            let path2 = format!("/sys/fs/cgroup/cpu/{}/cpu.cfs_period_us", group);
            if let (Ok(mut f1), Ok(mut f2)) = (File::open(&path1), File::open(&path2)) {
                let (mut buffer1, mut buffer2) = (String::new(), String::new());
                f1.read_to_string(&mut buffer1).unwrap();
                f2.read_to_string(&mut buffer2).unwrap();
                return parse_cpu_quota_v1(buffer1.trim(), buffer2.trim());
            }
        }
        None
    }
}

fn is_cgroup2_unified_mode() -> bool {
    let path = "/sys/fs/cgroup\0";
    let mut buffer = MaybeUninit::<libc::statfs>::uninit();
    let f_type = unsafe {
        assert_eq!(libc::statfs(path.as_ptr() as _, buffer.as_mut_ptr()), 0);
        buffer.assume_init().f_type
    };
    f_type == libc::CGROUP2_SUPER_MAGIC
}

fn parse_proc_cgroup_v1(lines: &str) -> HashMap<String, String> {
    let mut subsystems = HashMap::new();
    for line in lines.lines().map(|s| s.trim()).filter(|s| !s.is_empty()) {
        let mut iter = line.split(':');
        let _id = iter.next().unwrap();
        let systems = iter.next().unwrap();
        let path = iter.next().unwrap();
        for system in systems.split(',') {
            subsystems.insert(system.to_owned(), path.to_owned());
        }
    }
    subsystems
}

fn parse_proc_cgroup_v2(lines: &str) -> HashMap<String, String> {
    let subsystems = parse_proc_cgroup_v1(lines);
    assert_eq!(subsystems.len(), 1);
    subsystems
}

fn parse_memory_max(line: &str) -> i64 {
    if line == "max" {
        return -1;
    }
    line.parse().unwrap()
}

fn parse_cpu_cores(value: &str) -> HashSet<usize> {
    let mut cores = HashSet::new();
    for v in value.split(',') {
        if v.contains('-') {
            let mut v = v.split('-');
            let s = v.next().unwrap().parse::<usize>().unwrap();
            let e = v.next().unwrap().parse::<usize>().unwrap();
            for x in s..=e {
                cores.insert(x);
            }
        } else {
            let s = v.parse::<usize>().unwrap();
            cores.insert(s);
        }
    }
    cores
}

fn parse_cpu_quota_v2(line: &str) -> Option<f64> {
    let mut iter = line.split(' ');
    let max = iter.next().unwrap();
    if max != "max" {
        let period = iter.next().unwrap();
        return Some(max.parse::<f64>().unwrap() / period.parse::<f64>().unwrap());
    }
    None
}

fn parse_cpu_quota_v1(line1: &str, line2: &str) -> Option<f64> {
    let max = line1.parse::<i64>().unwrap();
    if max >= 0 {
        let period = line2.parse::<i64>().unwrap();
        return Some(max as f64 / period as f64);
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_proc_cgroup_v1() {
        let content = r#"
            10:cpuset:/test-cpuset
            4:memory:/kubepods/burstable/poda2ebe2cd-64c7-11ea-8799-eeeeeeeeeeee/a026c487f1168b7f5442444ac8e35161dfcde87c175ef27d9a806270e267a575
            5:cpuacct,cpu:/kubepods/burstable/poda2ebe2cd-64c7-11ea-8799-eeeeeeeeeeee/a026c487f1168b7f5442444ac8e35161dfcde87c175ef27d9a806270e267a575
        "#;
        let cgroups = parse_proc_cgroup_v1(&content);
        assert_eq!(
            cgroups.get("memory").unwrap(),
            "/kubepods/burstable/poda2ebe2cd-64c7-11ea-8799-eeeeeeeeeeee/a026c487f1168b7f5442444ac8e35161dfcde87c175ef27d9a806270e267a575"
        );
        assert_eq!(
            cgroups.get("cpu").unwrap(),
            "/kubepods/burstable/poda2ebe2cd-64c7-11ea-8799-eeeeeeeeeeee/a026c487f1168b7f5442444ac8e35161dfcde87c175ef27d9a806270e267a575"
        );
        assert_eq!(cgroups.get("cpuset").unwrap(), "/test-cpuset");
    }

    #[test]
    fn test_parse_proc_cgroup_v2() {
        let content = "0::/test-all";
        let cgroups = parse_proc_cgroup_v2(&content);
        assert_eq!(cgroups.get("").unwrap(), "/test-all");
    }

    #[test]
    fn test_parse_memory_max() {
        let contents = vec!["max", "9223372036854771712", "21474836480"];
        let expects = vec![-1, 9223372036854771712, 21474836480];
        for (content, expect) in contents.into_iter().zip(expects) {
            let limit = parse_memory_max(content);
            assert_eq!(limit, expect);
        }
    }

    #[test]
    fn test_parse_cpu_cores() {
        let mut cpusets = Vec::new();
        cpusets.extend(parse_cpu_cores("1-2,5-8,10,12,4"));
        cpusets.sort_unstable();
        assert_eq!(cpusets, vec![1, 2, 4, 5, 6, 7, 8, 10, 12]);
    }

    #[test]
    fn test_parse_cpu_quota_v2() {
        let contents = vec!["max 100000", "10000 100000", "1000000 100000"];
        let expects = vec![None, Some(0.1), Some(10.0)];
        for (content, expect) in contents.into_iter().zip(expects) {
            let limit = parse_cpu_quota_v2(content);
            assert_eq!(limit, expect);
        }
    }

    #[test]
    fn test_parse_cpu_quota_v1() {
        let contents = vec![("-1", "100000"), ("10000", "100000"), ("1000000", "100000")];
        let expects = vec![None, Some(0.1), Some(10.0)];
        for (i, (quota, period)) in contents.into_iter().enumerate() {
            let limit = parse_cpu_quota_v1(quota, period);
            assert_eq!(limit, expects[i]);
        }
    }

    // Currently this case can only be run manually:
    // $ cargo test --package=tikv_util --features test-cgroup --tests test_cgroup
    // Please make sure that you have privilege to run `cgcreate` and `cgdelete`.
    #[cfg(feature = "test-cgroup")]
    #[test]
    fn test_cgroup() {
        use std::process::{self, Command};

        let pid = process::id();
        let group = format!("tikv-test-{}", pid);
        let ctls_group = format!("cpu,cpuset,memory:{}", group);

        // $ cgcreate -g cpu,cpuset,memory:tikv-test
        let mut child = Command::new("cgcreate")
            .args(&["-g", &ctls_group])
            .spawn()
            .unwrap();
        assert!(child.wait().unwrap().success());

        // $ cgclassify -g cpu,cpuset,memory:tikv-test <pid>
        let mut child = Command::new("cgclassify")
            .args(&["-g", &ctls_group, &format!("{}", pid)])
            .spawn()
            .unwrap();
        assert!(child.wait().unwrap().success());

        // cgroup-v2 $ cgset -r memory.max=1G tikv-test
        // cgroup-v1 $ cgset -r memory.limit_in_bytes=1G tikv-test
        let mut child = if is_cgroup2_unified_mode() {
            Command::new("cgset")
                .args(&["-r", "memory.max=1G", &group])
                .spawn()
                .unwrap()
        } else {
            Command::new("cgset")
                .args(&["-r", "memory.limit_in_bytes=1G", &group])
                .spawn()
                .unwrap()
        };
        assert!(child.wait().unwrap().success());

        // cgroup-v2 $ cgset -r cpu.max='1000000 1000000' tikv-test
        // cgroup-v1 $ cgset -r cpu.cfs_quota_us=1000000 tikv-test
        // cgroup-v1 $ cgset -r cpu.cfs_period_us=1000000 tikv-test
        if is_cgroup2_unified_mode() {
            let mut child = Command::new("cgset")
                .args(&["-r", "cpu.max=1000000 1000000", &group])
                .spawn()
                .unwrap();
            assert!(child.wait().unwrap().success());
        } else {
            let mut child = Command::new("cgset")
                .args(&["-r", "cpu.cfs_quota_us=1000000", &group])
                .spawn()
                .unwrap();
            assert!(child.wait().unwrap().success());
            let mut child = Command::new("cgset")
                .args(&["-r", "cpu.cfs_period_us=1000000", &group])
                .spawn()
                .unwrap();
            assert!(child.wait().unwrap().success());
        }

        // $ cgset -r cpuset.cpus=0 tikv-test
        let mut child = Command::new("cgset")
            .args(&["-r", "cpuset.cpus=0", &group])
            .spawn()
            .unwrap();
        assert!(child.wait().unwrap().success());

        let cg = CGroupSys::new();
        assert_eq!(cg.memory_limit_in_bytes(), 1 * 1024 * 1024 * 1024);
        assert_eq!(cg.cpu_quota(), Some(1.0));
        assert_eq!(cg.cpuset_cores().into_iter().collect::<Vec<_>>(), vec![0]);

        // $ cgdelete -g cpu,cpuset,memory:tikv-test
        let mut child = Command::new("cgdelete")
            .args(&["-g", &ctls_group])
            .spawn()
            .unwrap();
        assert!(child.wait().unwrap().success());
    }
}
