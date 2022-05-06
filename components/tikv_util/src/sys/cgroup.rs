// Copyright 2020 TiKV Project Authors. Licensed under Apache-2.0.

use std::{
    collections::{HashMap, HashSet},
    fs::read_to_string,
    mem::MaybeUninit,
    num::IntErrorKind,
    path::{Path, PathBuf},
};

use num_traits::Bounded;
use procfs::process::{MountInfo, Process};

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
        let lines = read_to_string("/proc/self/cgroup")
            .map_err(|e| format!("fail to read /proc/self/cgroup: {}", e))?;
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
            if let Some((root, mount_point)) = self.mount_points.get(component) {
                let path = build_path(group, root, mount_point);
                let path = if self.is_v2 {
                    format!("{}/memory.max", path.to_str().unwrap())
                } else {
                    format!("{}/memory.limit_in_bytes", path.to_str().unwrap())
                };
                return read_to_string(&path).map_or(-1, |x| parse_memory_max(x.trim()));
            } else {
                warn!("cgroup memory controller found but not mounted.");
            }
        }
        -1
    }

    pub fn cpuset_cores(&self) -> HashSet<usize> {
        let component = if self.is_v2 { "" } else { "cpuset" };
        if let Some(group) = self.cgroups.get(component) {
            if let Some((root, mount_point)) = self.mount_points.get(component) {
                let path = build_path(group, root, mount_point);
                let path = format!("{}/cpuset.cpus", path.to_str().unwrap());
                return read_to_string(&path)
                    .map_or_else(|_| HashSet::new(), |x| parse_cpu_cores(x.trim()));
            } else {
                warn!("cgroup cpuset controller found but not mounted.");
            }
        }
        Default::default()
    }

    /// None means no limit.
    pub fn cpu_quota(&self) -> Option<f64> {
        let component = if self.is_v2 { "" } else { "cpu" };
        if let Some(group) = self.cgroups.get(component) {
            if let Some((root, mount_point)) = self.mount_points.get(component) {
                let path = build_path(group, root, mount_point);
                if self.is_v2 {
                    let path = format!("{}/cpu.max", path.to_str().unwrap());
                    if let Ok(buffer) = read_to_string(&path) {
                        return parse_cpu_quota_v2(buffer.trim());
                    }
                } else {
                    let path1 = format!("{}/cpu.cfs_quota_us", path.to_str().unwrap());
                    let path2 = format!("{}/cpu.cfs_period_us", path.to_str().unwrap());
                    if let (Ok(buffer1), Ok(buffer2)) =
                        (read_to_string(&path1), read_to_string(&path2))
                    {
                        return parse_cpu_quota_v1(buffer1.trim(), buffer2.trim());
                    }
                }
            } else {
                warn!("cgroup cpu controller found but not mounted.");
            }
        }
        None
    }
}

fn capping_parse_int<T: std::str::FromStr<Err = std::num::ParseIntError> + Bounded>(
    s: &str,
) -> Result<T, std::num::ParseIntError> {
    match s.parse::<T>() {
        Err(e) if matches!(e.kind(), IntErrorKind::PosOverflow) => Ok(T::max_value()),
        Err(e) if matches!(e.kind(), IntErrorKind::NegOverflow) => Ok(T::min_value()),
        x => x,
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
        if let Some(_id) = iter.next() {
            if let Some(systems) = iter.next() {
                if let Some(path) = iter.next() {
                    for system in systems.split(',') {
                        subsystems.insert(system.to_owned(), path.to_owned());
                    }
                    continue;
                }
            }
        }
        warn!("fail to parse cgroup v1: {}", line);
    }
    subsystems
}

// From cgroup spec:
// The entry for cgroup v2 is always in the format "0::$PATH"
fn parse_proc_cgroup_v2(lines: &str) -> HashMap<String, String> {
    let subsystems = parse_proc_cgroup_v1(lines);
    if subsystems.len() != 1 {
        warn!(
            "cgroup v2 should only have one subsystem, got {}",
            subsystems.len()
        );
    } else if subsystems.keys().next().unwrap() != "" {
        warn!(
            "unexpected cgroup v2 subsystem name: {}",
            subsystems.keys().next().unwrap()
        );
    }
    subsystems
}

fn cgroup_mountinfos_v1() -> HashMap<String, (String, PathBuf)> {
    match Process::myself().and_then(|x| x.mountinfo()) {
        Ok(info) => parse_mountinfos_v1(info),
        Err(e) => {
            warn!("fail to get mountinfo: {}", e);
            HashMap::new()
        }
    }
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
    match Process::myself().and_then(|x| x.mountinfo()) {
        Ok(info) => parse_mountinfos_v2(info),
        Err(e) => {
            warn!("fail to get mountinfo: {}", e);
            HashMap::new()
        }
    }
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
    let abs_root = super::super::config::normalize_path(Path::new(root));
    let root = abs_root.to_str().unwrap();
    assert!(path.starts_with('/') && root.starts_with('/'));

    let relative = path.strip_prefix(root).unwrap();
    let mut absolute = mount_point.to_owned();
    absolute.push(relative);
    absolute
}

fn parse_memory_max(line: &str) -> i64 {
    if line == "max" {
        return -1;
    }
    match capping_parse_int::<i64>(line) {
        Ok(x) => x,
        Err(e) => {
            warn!("fail to parse memory max: {}", e);
            -1
        }
    }
}

fn parse_cpu_cores(value: &str) -> HashSet<usize> {
    let mut cores = HashSet::new();
    if value.is_empty() {
        return cores;
    }

    for v in value.split(',') {
        if v.contains('-') {
            let mut v = v.split('-');
            if let Some(s) = v.next() {
                if let Ok(s) = capping_parse_int::<usize>(s) {
                    if let Some(e) = v.next() {
                        if let Ok(e) = capping_parse_int::<usize>(e) {
                            for x in s..=e {
                                cores.insert(x);
                            }
                            continue;
                        }
                    }
                }
            }
        } else if let Ok(s) = capping_parse_int::<usize>(v) {
            cores.insert(s);
            continue;
        }
        warn!("fail to parse cpu cores: {}", v);
    }
    cores
}

fn parse_cpu_quota_v2(line: &str) -> Option<f64> {
    let mut iter = line.split(' ');
    if let Some(max) = iter.next() {
        if max != "max" {
            if let Ok(max) = max.parse::<f64>() {
                if let Some(period) = iter.next() {
                    if let Ok(period) = period.parse::<f64>() {
                        if period > 0.0 {
                            return Some(max / period);
                        }
                    }
                }
            }
            warn!("fail to parse cpu quota v2: {}", line);
        }
    }
    None
}

fn parse_cpu_quota_v1(line1: &str, line2: &str) -> Option<f64> {
    if let Ok(max) = line1.parse::<f64>() {
        if max > 0.0 {
            if let Ok(period) = line2.parse::<f64>() {
                if period > 0.0 {
                    return Some(max as f64 / period as f64);
                }
            }
        } else {
            return None;
        }
    }
    warn!("fail to parse cpu quota v1: {}, {}", line1, line2);
    None
}

#[cfg(test)]
mod tests {
    use std::{fs::OpenOptions, io::Write};

    use super::*;

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
    fn test_cgroup_without_mountinfo() {
        let temp = tempfile::TempDir::new().unwrap();
        let dir = temp.path().to_str().unwrap();
        std::fs::copy("/proc/self/stat", &format!("{}/stat", dir)).unwrap();

        let mut f = OpenOptions::new()
            .create(true)
            .write(true)
            .open(&format!("{}/mountinfo", dir))
            .unwrap();
        f.write_all(b"1663 1661 0:27 /../../../../../.. /sys/fs/cgroup rw,nosuid,nodev,noexec,relatime - cgroup cgroup rw\n").unwrap();

        let cgroups = parse_proc_cgroup_v1("3:cpu:/\n");
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
        let cases = vec![
            ("0::/test-all", Some("/test-all".to_owned())),
            ("0::/test-all\n1::/test-2", Some("/test-2".to_owned())),
            ("0:name:/test-all", None),
        ];
        for (lines, expect) in cases.into_iter() {
            let cgroups = parse_proc_cgroup_v2(lines);
            assert_eq!(cgroups.get(""), expect.as_ref());
        }
    }

    #[test]
    fn test_parse_memory_max() {
        let cases = vec![
            ("max", -1),
            ("-1", -1),
            ("9223372036854771712", 9223372036854771712),
            ("21474836480", 21474836480),
            // Malformed.
            ("18446744073709551610", 9223372036854775807),
            ("-18446744073709551610", -9223372036854775808),
            ("0.1", -1),
        ];
        for (content, expect) in cases.into_iter() {
            let limit = parse_memory_max(content);
            assert_eq!(limit, expect);
        }
    }

    #[test]
    fn test_parse_cpu_cores() {
        assert!(parse_cpu_cores("").is_empty());

        let mut cpusets = Vec::new();
        cpusets.extend(parse_cpu_cores("1-2,5-8,10,12,4"));
        cpusets.sort_unstable();
        assert_eq!(cpusets, vec![1, 2, 4, 5, 6, 7, 8, 10, 12]);

        // Malformed info.
        let mut cpusets = Vec::new();
        cpusets.extend(parse_cpu_cores("0.9,8-,-9,7,18446744073709551616,1-4"));
        cpusets.sort_unstable();
        assert_eq!(cpusets, vec![1, 2, 3, 4, 7, 18446744073709551615]);
    }

    #[test]
    fn test_parse_cpu_quota_v2() {
        let cases = vec![
            ("max 100000", None),
            ("10000 100000", Some(0.1)),
            ("1000000 100000", Some(10.0)),
            // Malformed.
            ("1000", None),
        ];
        for (content, expect) in cases.into_iter() {
            let limit = parse_cpu_quota_v2(content);
            assert_eq!(limit, expect);
        }
    }

    #[test]
    fn test_parse_cpu_quota_v1() {
        let cases = vec![
            (("-1", "100000"), None),
            (("10000", "100000"), Some(0.1)),
            (("1000000", "100000"), Some(10.0)),
            // Malformed.
            (("18446744073709551616", "18446744073709551616"), Some(1.0)),
            ((",", ""), None),
            (("", "0.1"), None),
            (("100", "0"), None),
        ];
        for ((quota, period), expect) in cases.into_iter() {
            let limit = parse_cpu_quota_v1(quota, period);
            assert_eq!(limit, expect);
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
        let mut child = if is_cgroup2_unified_mode().unwrap() {
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
        if is_cgroup2_unified_mode().unwrap() {
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
