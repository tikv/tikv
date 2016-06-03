// Copyright 2016 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

use std::ffi::{CString, CStr};
use std::mem;
use libc;

pub struct DiskStat {
    pub capacity: u64,
    pub available: u64,
}

// Get the disk stats for path belongs.
// TODO: define own Error type instead of string.
pub fn get_disk_stat(path: &str) -> Result<DiskStat, String> {
    let cpath = CString::new(path).unwrap();
    unsafe {
        let mut stat: libc::statfs = mem::zeroed();
        let ret = libc::statfs(cpath.as_ptr(), &mut stat);
        if ret != 0 {
            return Err(format!("get stats for {} failed {}",
                               path,
                               CStr::from_ptr(libc::strerror(ret)).to_str().unwrap()));
        }

        Ok(DiskStat {
            capacity: (stat.f_bsize as u64 * stat.f_blocks) as u64,
            available: (stat.f_bsize as u64 * stat.f_bfree) as u64,
        })
    }
}

#[cfg(test)]
mod tests {
    use std::fs::File;
    use std::io::Write;

    use tempdir::TempDir;

    use super::*;

    fn create_size_file(dir: &str, name: &str, size: usize) {
        let mut f = File::create(&format!("{}/{}", dir, name)).unwrap();
        let data = vec![0;size];
        f.write_all(&data).unwrap();
    }

    #[test]
    fn test_get_disk_stat() {
        let temp_dir = TempDir::new("var").unwrap();
        let dir = temp_dir.path().to_str().unwrap();

        let s1 = get_disk_stat(dir).unwrap();

        create_size_file(dir, "1.log", 1000);
        let s2 = get_disk_stat(dir).unwrap();

        assert_eq!(s2.capacity, s1.capacity);
        assert!(s2.available < s1.available);
    }
}
