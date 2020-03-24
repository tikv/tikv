// Copyright 2020 TiKV Project Authors. Licensed under Apache-2.0.

use std::collections::HashMap;
use std::error;
use std::fs::File;
use std::io::{BufRead, BufReader};
use std::path::Path;

const CGROUP_PATH: &str = "/proc/self/cgroup";
const CGROUP_MOUNTINFO: &str = "/proc/self/mountinfo";
const CGROUP_FSTYPE: &str = "cgroup";
const MEM_SUBSYS: &str = "memory";
const MEM_LIMIT_IN_BYTES: &str = "memory.limit_in_bytes";
const CPU_SUBSYS: &str = "cpu";
const CPU_QUOTA: &str = "cpu.cfs_quota_us";
const CPU_PERIOD: &str = "cpu.cfs_period_us";

const MOUNTINFO_SEP: &str = " ";
const OPTIONS_SEP: &str = ",";
const OPTIONAL_FIELDS_SEP: &str = "-";

const CGROUP_SEP: &str = ":";
const SUBSYS_SEP: &str = ",";

quick_error! {
    #[derive(Debug)]
    pub enum Error {
        Other(err: Box<dyn error::Error + Sync + Send>) {
            from()
            cause(err.as_ref())
            description(err.description())
            display("{:?}", err)
        }
        Io(err: std::io::Error) {
            from()
            cause(err)
            description(err.description())
        }
        PathErr(err: std::path::StripPrefixError) {
            from()
            cause(err)
            description(err.description())
        }
    }
}

enum MountInfoFieldPart1 {
    MountID = 0,
    ParentID,
    DeviceID,
    Root,
    MountPoint,
    Options,
    OptionalFields,

    Count,
}

enum MountInfoFieldPart2 {
    FSType = 0,
    MountSource,
    SuperOptions,

    Count,
}

#[derive(Debug, PartialEq)]
pub struct MountPoint {
    pub mount_id: i32,
    pub parent_id: i32,
    pub device_id: String,
    pub root: String,
    pub mount_point: String,
    pub options: Vec<String>,
    pub optional_fields: Vec<String>,
    pub fs_type: String,
    pub mount_source: String,
    pub super_options: Vec<String>,
}

impl MountPoint {
    // Converts an absolute path inside the *MountPoint's file system to
    // the host file system path in the mount namespace the *MountPoint belongs to.
    pub fn translate(&self, abs_path: &str) -> Result<String, Error> {
        let abs = Path::new(abs_path);
        let rel = abs.strip_prefix(&self.root)?;
        let path = Path::new(&self.mount_point).join(rel);
        if let Some(path) = path.to_str() {
            Ok(String::from(path))
        } else {
            Err(box_err!("empty path"))
        }
    }
}

pub fn parse_mount_point_from_line(line: &str) -> Result<MountPoint, String> {
    let fields: Vec<String> = line.split(MOUNTINFO_SEP).map(|s| s.to_string()).collect();
    if fields.len() < MountInfoFieldPart1::Count as usize + MountInfoFieldPart2::Count as usize {
        return Err("mount point format invalid".to_string());
    }

    let mut sep_pos = MountInfoFieldPart1::OptionalFields as usize;
    let mut found_sep = false;
    for field in &fields[MountInfoFieldPart1::OptionalFields as usize..] {
        if field == OPTIONAL_FIELDS_SEP {
            found_sep = true;
            break;
        }
        sep_pos += 1;
    }
    if !found_sep {
        return Err(
            "mount point format invalid, doesn't found optional field separator".to_string(),
        );
    }

    let fs_start = sep_pos + 1;
    let mount_id = fields[MountInfoFieldPart1::MountID as usize]
        .parse::<i32>()
        .unwrap();
    let parent_id = fields[MountInfoFieldPart1::ParentID as usize]
        .parse::<i32>()
        .unwrap();
    let mount_point = MountPoint {
        mount_id,
        parent_id,
        device_id: fields[MountInfoFieldPart1::DeviceID as usize].clone(),
        root: fields[MountInfoFieldPart1::Root as usize].clone(),
        mount_point: fields[MountInfoFieldPart1::MountPoint as usize].clone(),
        options: fields[MountInfoFieldPart1::Options as usize]
            .split(OPTIONS_SEP)
            .map(|s| s.to_string())
            .collect(),
        optional_fields: fields[MountInfoFieldPart1::OptionalFields as usize..fs_start - 1]
            .to_vec(),
        fs_type: fields[fs_start + MountInfoFieldPart2::FSType as usize].clone(),
        mount_source: fields[fs_start + MountInfoFieldPart2::MountSource as usize].clone(),
        super_options: fields[fs_start + MountInfoFieldPart2::SuperOptions as usize]
            .split(OPTIONS_SEP)
            .map(|s| s.to_string())
            .collect(),
    };

    Ok(mount_point)
}

enum SubsysFields {
    ID = 0,
    Subsystems,
    Name,

    Count,
}

#[derive(Clone, Debug, PartialEq)]
pub struct CGroupSubsys {
    pub id: i32,
    pub sub_systems: Vec<String>,
    pub name: String,
}

pub fn parse_subsys_from_line(line: &str) -> Result<CGroupSubsys, Error> {
    let fields: Vec<String> = line.split(CGROUP_SEP).map(|s| s.to_string()).collect();
    if fields.len() != SubsysFields::Count as usize {
        return Err(box_err!("subsystem format invalid"));
    }

    let id = fields[SubsysFields::ID as usize].parse::<i32>().unwrap();
    Ok(CGroupSubsys {
        id,
        sub_systems: fields[SubsysFields::Subsystems as usize]
            .split(SUBSYS_SEP)
            .map(|s| s.to_string())
            .collect(),
        name: fields[SubsysFields::Name as usize].clone(),
    })
}

pub fn file_scanner<F>(path: &str, mut f: F) -> std::io::Result<()>
where
    F: FnMut(&str),
{
    let file = File::open(path)?;
    let reader = BufReader::new(file);
    for line in reader.lines() {
        if let Ok(l) = line {
            f(&l);
        }
    }
    Ok(())
}

#[derive(Debug, PartialEq)]
pub struct CGroup {
    // subsystem path
    path: String,
}

impl CGroup {
    pub fn new(path: String) -> Self {
        Self { path }
    }

    pub fn read_num(&self, param: &str) -> Result<i64, Box<dyn error::Error>> {
        let f = File::open(Path::new(&self.path).join(param))?;
        let mut reader = BufReader::new(f);

        let mut first_line = String::new();
        let _len = reader.read_line(&mut first_line)?;
        Ok(first_line.trim().parse::<i64>()?)
    }
}

#[derive(Debug)]
pub struct CGroupSys {
    // subsystem name -> CGroup
    pub cgroups: HashMap<String, CGroup>,
}

impl Default for CGroupSys {
    fn default() -> Self {
        Self::new(CGROUP_PATH, CGROUP_MOUNTINFO)
    }
}

impl CGroupSys {
    pub fn new(cgroup_path: &str, mountinfo_path: &str) -> CGroupSys {
        // Parse subsystem from cgroup_path.
        let mut subsystems: HashMap<String, CGroupSubsys> = HashMap::new();
        let _ = file_scanner(cgroup_path, |line| {
            if let Ok(s) = parse_subsys_from_line(line) {
                for subsys in &s.sub_systems {
                    subsystems.insert(subsys.to_string(), s.clone());
                }
            }
        });

        // Parse mount points from mountinfo_path and collect subsystems path.
        let mut cgroups: HashMap<String, CGroup> = HashMap::new();
        let _ = file_scanner(mountinfo_path, |line| {
            if let Ok(mp) = parse_mount_point_from_line(line) {
                if mp.fs_type == CGROUP_FSTYPE {
                    for op in &mp.super_options {
                        if let Some(sub) = subsystems.get(op) {
                            if let Ok(path) = mp.translate(&sub.name) {
                                cgroups.insert(op.to_string(), CGroup::new(path));
                            }
                        }
                    }
                }
            }
        });

        Self { cgroups }
    }

    pub fn cpu_cores_quota(&self) -> i64 {
        if let Some(sub_cpu) = self.cgroups.get(CPU_SUBSYS) {
            if let Ok(quota) = sub_cpu.read_num(CPU_QUOTA) {
                if quota < 0 {
                    return -1;
                }
                if let Ok(period) = sub_cpu.read_num(CPU_PERIOD) {
                    return quota / period;
                }
            }
        }

        // -1 means no limit.
        -1
    }

    pub fn memory_limit_in_bytes(&self) -> i64 {
        if let Some(sub_mem) = self.cgroups.get(MEM_SUBSYS) {
            if let Ok(limits_in_bytes) = sub_mem.read_num(MEM_LIMIT_IN_BYTES) {
                return limits_in_bytes;
            }
        }

        // -1 means no limit.
        -1
    }
}

#[cfg(test)]
mod tests {
    use super::{
        parse_mount_point_from_line, parse_subsys_from_line, CGroup, CGroupSubsys, CGroupSys,
        MountPoint,
    };
    use std::collections::HashMap;
    use std::fs::File;
    use std::io::Write;
    use tempfile::TempDir;

    #[test]
    fn test_parse_subsystem_from_line() {
        let lines = vec!["1:cpu:/", "8:cpu,cpuacct,cpuset:/docker/1234567890abcdef"];
        let expected_subsystems = vec![
            CGroupSubsys {
                id: 1,
                sub_systems: vec!["cpu".to_string()],
                name: "/".to_string(),
            },
            CGroupSubsys {
                id: 8,
                sub_systems: vec![
                    "cpu".to_string(),
                    "cpuacct".to_string(),
                    "cpuset".to_string(),
                ],
                name: "/docker/1234567890abcdef".to_string(),
            },
        ];
        for (line, expected_subsystem) in lines.iter().zip(expected_subsystems) {
            let subsystem = parse_subsys_from_line(line).unwrap();
            assert_eq!(subsystem, expected_subsystem);
        }
    }

    #[test]
    fn test_parse_mount_point_from_line() {
        let lines = vec!["1 0 252:0 / / rw,noatime - ext4 /dev/dm-0 rw,errors=remount-ro,data=ordered",
                         "31 23 0:24 /docker /sys/fs/cgroup/cpu rw,nosuid,nodev,noexec,relatime shared:1 - cgroup cgroup rw,cpu"];
        let expected_mps = vec![
            MountPoint {
                mount_id: 1,
                parent_id: 0,
                device_id: "252:0".to_string(),
                root: "/".to_string(),
                mount_point: "/".to_string(),
                options: vec!["rw".to_string(), "noatime".to_string()],
                optional_fields: vec![],
                fs_type: "ext4".to_string(),
                mount_source: "/dev/dm-0".to_string(),
                super_options: vec![
                    "rw".to_string(),
                    "errors=remount-ro".to_string(),
                    "data=ordered".to_string(),
                ],
            },
            MountPoint {
                mount_id: 31,
                parent_id: 23,
                device_id: "0:24".to_string(),
                root: "/docker".to_string(),
                mount_point: "/sys/fs/cgroup/cpu".to_string(),
                options: vec![
                    "rw".to_string(),
                    "nosuid".to_string(),
                    "nodev".to_string(),
                    "noexec".to_string(),
                    "relatime".to_string(),
                ],
                optional_fields: vec!["shared:1".to_string()],
                fs_type: "cgroup".to_string(),
                mount_source: "cgroup".to_string(),
                super_options: vec!["rw".to_string(), "cpu".to_string()],
            },
        ];
        for (line, expected_mp) in lines.iter().zip(expected_mps) {
            let mp = parse_mount_point_from_line(line).unwrap();
            assert_eq!(mp, expected_mp);
        }
    }

    #[test]
    fn test_mount_point_translate() {
        let mp = MountPoint {
            mount_id: 31,
            parent_id: 23,
            device_id: "0:24".to_string(),
            root: "/docker".to_string(),
            mount_point: "/sys/fs/cgroup/cpu".to_string(),
            options: vec![
                "rw".to_string(),
                "nosuid".to_string(),
                "nodev".to_string(),
                "noexec".to_string(),
                "relatime".to_string(),
            ],
            optional_fields: vec!["shared:1".to_string()],
            fs_type: "cgroup".to_string(),
            mount_source: "cgroup".to_string(),
            super_options: vec!["rw".to_string(), "cpu".to_string()],
        };

        let translated = mp.translate("/docker").unwrap();
        assert_eq!(translated, "/sys/fs/cgroup/cpu/");
    }

    #[test]
    fn test_read_num() {
        let tmp_dir = TempDir::new().unwrap();
        let cgroup = CGroup::new(tmp_dir.path().to_str().unwrap().to_string());

        // Read number from file `memory.limits_in_bytes`
        let path = tmp_dir.path().join("memory.limits_in_bytes");
        let mut f1 = File::create(path).unwrap();
        f1.write_all(b"123").unwrap();
        f1.sync_all().unwrap();
        assert_eq!(123, cgroup.read_num("memory.limits_in_bytes").unwrap());

        // Read -1 from file `cpu.cfs_quota_us`
        let path = tmp_dir.path().join("cpu.cfs_quota_us");
        let mut f2 = File::create(path).unwrap();
        f2.write_all(b"-1").unwrap();
        f2.sync_all().unwrap();
        assert_eq!(-1, cgroup.read_num("cpu.cfs_quota_us").unwrap());

        // Read invalid number
        let path = tmp_dir.path().join("cpu.cfs_period_us");
        let mut f3 = File::create(path).unwrap();
        f3.write_all(b"abc").unwrap();
        f3.sync_all().unwrap();
        assert!(cgroup.read_num("cpu.cfs_period_us").is_err());

        // Read number with \n from file `memory.max_usage`.
        let path = tmp_dir.path().join("memory.max_usage");
        let mut f1 = File::create(path).unwrap();
        f1.write_all(b"123\n").unwrap();
        f1.sync_all().unwrap();
        assert_eq!(123, cgroup.read_num("memory.max_usage").unwrap());
    }

    #[test]
    fn test_cgroup_sys() {
        let temp_dir = TempDir::new().unwrap();
        let path1 = temp_dir.path().join("cgroup");
        let mut f1 = File::create(path1.clone()).unwrap();
        f1.write_all(b"4:memory:/kubepods/burstable/poda2ebe2cd-64c7-11ea-8799-eeeeeeeeeeee/a026c487f1168b7f5442444ac8e35161dfcde87c175ef27d9a806270e267a575\n").unwrap();
        f1.write_all(b"5:cpuacct,cpu:/kubepods/burstable/poda2ebe2cd-64c7-11ea-8799-eeeeeeeeeeee/a026c487f1168b7f5442444ac8e35161dfcde87c175ef27d9a806270e267a575\n").unwrap();
        f1.sync_all().unwrap();

        let path2 = temp_dir.path().join("mountinfo");
        let mut f2 = File::create(path2.clone()).unwrap();
        f2.write_all(b"5871 5867 0:26 /kubepods/burstable/poda2ebe2cd-64c7-11ea-8799-eeeeeeeeeeee/a026c487f1168b7f5442444ac8e35161dfcde87c175ef27d9a806270e267a575 /sys/fs/cgroup/memory ro,nosuid,nodev,noexec,relatime master:12 - cgroup cgroup rw,memory\n").unwrap();
        f2.write_all(b"5872 5867 0:27 /kubepods/burstable/poda2ebe2cd-64c7-11ea-8799-eeeeeeeeeeee/a026c487f1168b7f5442444ac8e35161dfcde87c175ef27d9a806270e267a575 /sys/fs/cgroup/cpu,cpuacct ro,nosuid,nodev,noexec,relatime master:13 - cgroup cgroup rw,cpuacct,cpu\n").unwrap();
        f2.sync_all().unwrap();

        // Create CGroupSys by f1 and f2.
        let cgroup = CGroupSys::new(path1.to_str().unwrap(), path2.to_str().unwrap());

        let mut expected_cgroups: HashMap<String, CGroup> = HashMap::new();
        expected_cgroups.insert(
            "memory".to_string(),
            CGroup::new("/sys/fs/cgroup/memory/".to_string()),
        );
        expected_cgroups.insert(
            "cpu".to_string(),
            CGroup::new("/sys/fs/cgroup/cpu,cpuacct/".to_string()),
        );
        expected_cgroups.insert(
            "cpuacct".to_string(),
            CGroup::new("/sys/fs/cgroup/cpu,cpuacct/".to_string()),
        );
        for (key, value) in expected_cgroups.iter() {
            let v = cgroup
                .cgroups
                .get(key)
                .unwrap_or_else(|| panic!("key {} doesn't exist", key));
            assert_eq!(v, value);
        }
    }
}
