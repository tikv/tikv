// Copyright 2020 TiKV Project Authors. Licensed under Apache-2.0.

use procfs::process::{MountInfo, Process};
use std::collections::{HashMap, HashSet};
use std::fs::read_to_string;
use std::mem::MaybeUninit;
use std::path::{Component, Path, PathBuf};

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
//
// For more details about cgrop v2, PTAL
// https://www.kernel.org/doc/html/latest/admin-guide/cgroup-v2.html.
//
// The above examples are implicitly based on a premise that paths in `/proc/self/cgroup`
// can be appended to `/sys/fs/cgroup` directly to get the final paths. Generally it's
// correct for Linux hosts but maybe wrong for containers. For containers, cgroup file systems
// can be based on other mount points. For example:
//
// /proc/self/cgroup:
//   4:memory:/path/to/the/controller
// /proc/self/mountinfo:
//   34 25 0:30 /path/to/the/controller /sys/fs/cgroup/memory relatime - cgroup cgroup memory
// `path/to/the/controller` is possible to be not accessable in the container. However from the
// `mountinfo` file we can know the path is mounted on `sys/fs/cgroup/memory`, then we can build
// the absolute path based on the mountinfo file.
//
// For the format of the mountinfo file, PTAL https://man7.org/linux/man-pages/man5/proc.5.html.

const CONTROLLERS: &[&str] = &["memory", "cpuset", "cpu"];

#[derive(Default)]
pub struct CGroupSys {
    // map[controller] -> path.
    cgroups: HashMap<String, String>,
    // map[controller] -> (root, mount_point).
    mount_points: HashMap<String, (String, PathBuf)>,
    is_v2: bool,
}

impl CGroupSys {
    pub fn new() -> Result<Self, String> {
        let lines = read_to_string("/proc/self/cgroup").unwrap();
        let is_v2 = is_cgroup2_unified_mode()?;
        let (cgroups, mount_points) = if !is_v2 {
            (parse_proc_cgroup_v1(&lines), cgroup_mountinfos_v1())
        } else {
            (parse_proc_cgroup_v2(&lines), cgroup_mountinfos_v2())
        };

        Ok(CGroupSys {
            cgroups,
            mount_points,
            is_v2,
        })
    }

    /// -1 means no limit.
    pub fn memory_limit_in_bytes(&self) -> i64 {
        let component = if self.is_v2 { "" } else { "memory" };
        if let Some(group) = self.cgroups.get(component) {
            let (root, mount_point) = self.mount_points.get(component).unwrap();
            let path = build_path(group, root, mount_point);
            let path = if self.is_v2 {
                format!("{}/memory.max", path.to_str().unwrap())
            } else {
                format!("{}/memory.limit_in_bytes", path.to_str().unwrap())
            };
            return read_to_string(&path).map_or(-1, |x| parse_memory_max(x.trim()));
        }
        -1
    }

    pub fn cpuset_cores(&self) -> HashSet<usize> {
        let component = if self.is_v2 { "" } else { "cpuset" };
        if let Some(group) = self.cgroups.get(component) {
            let (root, mount_point) = self.mount_points.get(component).unwrap();
            let path = build_path(group, root, mount_point);
            let path = format!("{}/cpuset.cpus", path.to_str().unwrap());
            return read_to_string(&path)
                .map_or_else(|_| HashSet::new(), |x| parse_cpu_cores(x.trim()));
        }
        Default::default()
    }

    /// None means no limit.
    pub fn cpu_quota(&self) -> Option<f64> {
        let component = if self.is_v2 { "" } else { "cpu" };
        if let Some(group) = self.cgroups.get(component) {
            let (root, mount_point) = self.mount_points.get(component).unwrap();
            let path = build_path(group, root, mount_point);
            if self.is_v2 {
                let path = format!("{}/cpu.max", path.to_str().unwrap());
                if let Ok(buffer) = read_to_string(&path) {
                    return parse_cpu_quota_v2(buffer.trim());
                }
            } else {
                let path1 = format!("{}/cpu.cfs_quota_us", path.to_str().unwrap());
                let path2 = format!("{}/cpu.cfs_period_us", path.to_str().unwrap());
                if let (Ok(buffer1), Ok(buffer2)) = (read_to_string(&path1), read_to_string(&path2))
                {
                    return parse_cpu_quota_v1(buffer1.trim(), buffer2.trim());
                }
            }
        }
        None
    }
}

fn is_cgroup2_unified_mode() -> Result<bool, String> {
    let path = "/sys/fs/cgroup\0";
    let mut buffer = MaybeUninit::<libc::statfs>::uninit();
    let f_type = unsafe {
        if libc::statfs(path.as_ptr() as _, buffer.as_mut_ptr()) != 0 {
            let errno = *libc::__errno_location();
            let msg = format!("statfs(/sys/fs/cgroup) fail, errno: {}", errno);
            return Err(msg);
        }
        buffer.assume_init().f_type
    };
    Ok(f_type == libc::CGROUP2_SUPER_MAGIC)
}

// From cgroup spec:
// "/proc/$PID/cgroup" lists a process’s cgroup membership. If legacy cgroup is in use in
// the system, this file may contain multiple lines, one for each hierarchy.
//
// The format is "<id>:<hierarchy>:<path>". For example, "10:cpuset:/test-cpuset".
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

// From cgroup spec:
// The entry for cgroup v2 is always in the format "0::$PATH"
fn parse_proc_cgroup_v2(lines: &str) -> HashMap<String, String> {
    let subsystems = parse_proc_cgroup_v1(lines);
    assert_eq!(subsystems.len(), 1);
    assert_eq!(subsystems.keys().next().unwrap(), "");
    subsystems
}

fn cgroup_mountinfos_v1() -> HashMap<String, (String, PathBuf)> {
    let infos = Process::myself().and_then(|x| x.mountinfo()).unwrap();
    parse_mountinfos_v1(infos)
}

fn parse_mountinfos_v1(infos: Vec<MountInfo>) -> HashMap<String, (String, PathBuf)> {
    let mut ret = HashMap::new();
    for cg_info in infos.into_iter().filter(|x| x.fs_type == "cgroup") {
        for controller in CONTROLLERS {
            if cg_info.super_options.contains_key(*controller) {
                let key = controller.to_string();
                let value = (cg_info.root.clone(), cg_info.mount_point.clone());
                ret.insert(key, value);
            }
        }
    }
    ret
}

fn cgroup_mountinfos_v2() -> HashMap<String, (String, PathBuf)> {
    let infos = Process::myself().and_then(|x| x.mountinfo()).unwrap();
    parse_mountinfos_v2(infos)
}

fn parse_mountinfos_v2(infos: Vec<MountInfo>) -> HashMap<String, (String, PathBuf)> {
    let mut ret = HashMap::new();
    let mut cg_infos = infos.into_iter().filter(|x| x.fs_type == "cgroup2");
    if let Some(cg_info) = cg_infos.next() {
        assert!(cg_infos.next().is_none()); // Only one item for cgroup-2.
        ret.insert("".to_string(), (cg_info.root, cg_info.mount_point));
    }
    ret
}

// `root` is mounted on `mount_point`. `path` is a sub path of `root`.
// This is used to build an absolute path starts with `mount_point`.
fn build_path(path: &str, root: &str, mount_point: &Path) -> PathBuf {
    let abs_root = normalize_path(Path::new(root));
    let root = abs_root.to_str().unwrap();
    assert!(path.starts_with('/') && root.starts_with('/'));

    let relative = path.strip_prefix(root).unwrap();
    let mut absolute = mount_point.to_owned();
    absolute.push(relative);
    absolute
}

fn normalize_path(path: &Path) -> PathBuf {
    let mut components = path.components().peekable();
    let mut ret = if let Some(c @ Component::Prefix(..)) = components.peek().cloned() {
        components.next();
        PathBuf::from(c.as_os_str())
    } else {
        PathBuf::new()
    };

    for component in components {
        match component {
            Component::Prefix(..) => unreachable!(),
            Component::RootDir => {
                ret.push(component.as_os_str());
            }
            Component::CurDir => {}
            Component::ParentDir => {
                ret.pop();
            }
            Component::Normal(c) => {
                ret.push(c);
            }
        }
    }
    ret
}

fn parse_memory_max(line: &str) -> i64 {
    if line == "max" {
        return -1;
    }
    line.parse().unwrap()
}

fn parse_cpu_cores(value: &str) -> HashSet<usize> {
    let mut cores = HashSet::new();
    if value.is_empty() {
        return cores;
    }

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
    use std::fs::OpenOptions;
    use std::io::Write;

    #[test]
    fn test_defult_cgroup_sys() {
        let cgroup = CGroupSys::default();
        assert_eq!(cgroup.memory_limit_in_bytes(), -1);
        assert_eq!(cgroup.cpu_quota(), None);
        assert!(cgroup.cpuset_cores().is_empty());
    }

    #[test]
    fn test_parse_mountinfos_without_cgroup() {
        let temp = tempfile::TempDir::new().unwrap();
        let dir = temp.path().to_str().unwrap();
        std::fs::copy("/proc/self/stat", &format!("{}/stat", dir)).unwrap();
        let mut f = OpenOptions::new()
            .create(true)
            .write(true)
            .open(&format!("{}/mountinfo", dir))
            .unwrap();
        f.write_all(b"").unwrap();

        let p = Process::new_with_root(PathBuf::from(dir)).unwrap();
        assert!(parse_mountinfos_v1(p.mountinfo().unwrap()).is_empty());
        assert!(parse_mountinfos_v2(p.mountinfo().unwrap()).is_empty());
    }

    #[test]
    fn test_cpuset_cpu_cpuacct() {
        let temp = tempfile::TempDir::new().unwrap();
        let dir = temp.path().to_str().unwrap();
        std::fs::copy("/proc/self/stat", &format!("{}/stat", dir)).unwrap();

        let mut f = OpenOptions::new()
            .create(true)
            .write(true)
            .open(&format!("{}/mountinfo", dir))
            .unwrap();
        f.write_all(b"30 26 0:27 / /sys/fs/cgroup/cpuset,cpu,cpuacct rw,nosuid,nodev,noexec,relatime shared:11 - cgroup cgroup rw,cpuset,cpu,cpuacct\n").unwrap();

        let cgroups = parse_proc_cgroup_v1("3:cpuset,cpu,cpuacct:/\n");
        let mount_points = {
            let infos = Process::new_with_root(PathBuf::from(dir))
                .and_then(|x| x.mountinfo())
                .unwrap();
            parse_mountinfos_v1(infos)
        };

        let cgroup_sys = CGroupSys {
            cgroups,
            mount_points,
            is_v2: false,
        };
        assert_eq!(cgroup_sys.cpu_quota(), None);
        assert!(cgroup_sys.cpuset_cores().is_empty());
    }

    #[test]
    fn test_mountinfo_with_relative_path() {
        let temp = tempfile::TempDir::new().unwrap();
        let dir = temp.path().to_str().unwrap();
        std::fs::copy("/proc/self/stat", &format!("{}/stat", dir)).unwrap();

        let mut f = OpenOptions::new()
            .create(true)
            .write(true)
            .open(&format!("{}/mountinfo", dir))
            .unwrap();
        f.write_all(b"1663 1661 0:27 /../../../../../.. /sys/fs/cgroup rw,nosuid,nodev,noexec,relatime - cgroup2 cgroup2 rw\n").unwrap();

        let cgroups = parse_proc_cgroup_v2("0::/\n");
        let mount_points = {
            let infos = Process::new_with_root(PathBuf::from(dir))
                .and_then(|x| x.mountinfo())
                .unwrap();
            parse_mountinfos_v2(infos)
        };
        let cgroup_sys = CGroupSys {
            cgroups,
            mount_points,
            is_v2: true,
        };

        assert_eq!(cgroup_sys.memory_limit_in_bytes(), -1);
    }

    #[test]
    fn test_parse_proc_cgroup_v1() {
        let content = r#"
            10:cpuset:/test-cpuset
            4:memory:/kubepods/burstable/poda2ebe2cd-64c7-11ea-8799-eeeeeeeeeeee/a026c487f1168b7f5442444ac8e35161dfcde87c175ef27d9a806270e267a575
            5:cpuacct,cpu:/kubepods/burstable/poda2ebe2cd-64c7-11ea-8799-eeeeeeeeeeee/a026c487f1168b7f5442444ac8e35161dfcde87c175ef27d9a806270e267a575
        "#;
        let cgroups = parse_proc_cgroup_v1(content);
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
        let cgroups = parse_proc_cgroup_v2(content);
        assert_eq!(cgroups.get("").unwrap(), "/test-all");
    }

    #[test]
    fn test_parse_memory_max() {
        let contents = vec!["max", "-1", "9223372036854771712", "21474836480"];
        let expects = vec![-1, -1, 9223372036854771712, 21474836480];
        for (content, expect) in contents.into_iter().zip(expects) {
            let limit = parse_memory_max(content);
            assert_eq!(limit, expect);
        }
    }

    #[test]
    fn test_parse_cpu_cores() {
        let mut cpusets = Vec::new();
        cpusets.extend(parse_cpu_cores(""));
        assert!(cpusets.is_empty());

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

        let cg = CGroupSys::new().unwrap();
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
