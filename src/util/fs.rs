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

use std::result;

use walkdir::{WalkDir, Error as WalkDirError};

quick_error! {
    #[derive(Debug)]
    pub enum Error {
        WalkError(err: WalkDirError) {
            from()
            cause(err)
            description(err.description())
        }
    }
}

pub type Result<T> = result::Result<T, Error>;

/// `total_size_in_path` walks the path and calculates total
/// file size. It won't follow link, only care regular file.
/// If the path is a directory, it will walk the directory recursively
/// and sum all files' size, if the path is a file, only returns the
/// file size.
pub fn total_size_in_path(path: &str) -> Result<u64> {
    let mut total: u64 = 0;
    for entry in WalkDir::new(path) {
        let entry = try!(entry);
        let meta = try!(entry.metadata());
        if meta.is_file() {
            total += meta.len();
        }
    }

    Ok(total)
}

#[cfg(test)]
mod tests {
    use std::fs::{self, File};
    use std::io::Write;

    use tempdir::TempDir;

    use super::*;

    fn create_size_file(dir: &str, name: &str, size: usize) {
        let mut f = File::create(&format!("{}/{}", dir, name)).unwrap();
        let data = vec![0;size];
        f.write_all(&data).unwrap();
    }

    #[test]
    fn test_total_size_in_path() {
        let temp_dir = TempDir::new("var").unwrap();
        let dir = temp_dir.path().to_str().unwrap();

        assert_eq!(total_size_in_path(dir).unwrap(), 0);

        let sub_dir = &format!("{}/a", dir);
        fs::create_dir(sub_dir).unwrap();

        assert_eq!(total_size_in_path(dir).unwrap(), 0);

        create_size_file(dir, "1.log", 100);
        assert_eq!(total_size_in_path(dir).unwrap(), 100);

        create_size_file(sub_dir, "2.log", 100);
        assert_eq!(total_size_in_path(dir).unwrap(), 200);
    }
}
