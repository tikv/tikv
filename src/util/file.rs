// Copyright 2017 PingCAP, Inc.
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

use std::io::{self, ErrorKind};
use std::fs;
use std::path::{Path, PathBuf};

pub fn get_file_size(path: &PathBuf) -> io::Result<u64> {
    let meta = try!(fs::metadata(path));
    Ok(meta.len())
}

pub fn file_exists(file: &PathBuf) -> bool {
    let path = Path::new(file);
    path.exists() && path.is_file()
}

pub fn delete_file_if_exist(file: &PathBuf) {
    match fs::remove_file(file) {
        Ok(_) => {}
        Err(ref e) if e.kind() == ErrorKind::NotFound => {}
        Err(e) => {
            warn!("failed to delete file {}: {:?}", file.display(), e);
        }
    }
}

#[cfg(test)]
mod test {
    use std::io::Write;
    use std::fs::OpenOptions;
    use tempdir::TempDir;

    use super::*;

    #[test]
    fn test_get_file_size() {
        let tmp_dir = TempDir::new("").unwrap();
        let dir_path = tmp_dir.path().to_path_buf();

        // Ensure it works to get the size of an empty file.
        let empty_file = dir_path.join("empty_file");
        {
            let _ = OpenOptions::new().write(true).create_new(true).open(&empty_file).unwrap();
        }
        assert_eq!(get_file_size(&empty_file).unwrap(), 0);

        // Ensure it works to get the size of an non-empty file.
        let non_empty_file = dir_path.join("non_empty_file");
        let size = 5;
        let v = vec![0; size];
        {
            let mut f =
                OpenOptions::new().write(true).create_new(true).open(&non_empty_file).unwrap();
            f.write_all(&v[..]).unwrap();
        }
        assert_eq!(get_file_size(&non_empty_file).unwrap(), size as u64);

        // Ensure it works for non-existent file.
        let non_existent_file = dir_path.join("non_existent_file");
        assert!(get_file_size(&non_existent_file).is_err());
    }

    #[test]
    fn test_file_exists() {
        let tmp_dir = TempDir::new("").unwrap();
        let dir_path = tmp_dir.path().to_path_buf();

        assert_eq!(file_exists(&dir_path), false);

        let existent_file = dir_path.join("empty_file");
        {
            let _ = OpenOptions::new().write(true).create_new(true).open(&existent_file).unwrap();
        }
        assert_eq!(file_exists(&existent_file), true);

        let non_existent_file = dir_path.join("non_existent_file");
        assert_eq!(file_exists(&non_existent_file), false);
    }

    #[test]
    fn test_delete_file_if_exist() {
        let tmp_dir = TempDir::new("").unwrap();
        let dir_path = tmp_dir.path().to_path_buf();

        let existent_file = dir_path.join("empty_file");
        {
            let _ = OpenOptions::new().write(true).create_new(true).open(&existent_file).unwrap();
        }
        assert_eq!(file_exists(&existent_file), true);
        delete_file_if_exist(&existent_file);
        assert_eq!(file_exists(&existent_file), false);

        let perm_file = dir_path.join("perm_file");
        {
            let f = OpenOptions::new().write(true).create_new(true).open(&perm_file).unwrap();
            let mut perm = f.metadata().unwrap().permissions();
            perm.set_readonly(true);
            f.set_permissions(perm).unwrap();
        }
        assert_eq!(file_exists(&perm_file), true);
        delete_file_if_exist(&perm_file);
        assert_eq!(file_exists(&perm_file), false);

        let non_existent_file = dir_path.join("non_existent_file");
        delete_file_if_exist(&non_existent_file);
    }
}
