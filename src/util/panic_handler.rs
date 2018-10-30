// Copyright 2018 PingCAP, Inc.
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

use std::fs::File;
use std::path::{Path, PathBuf};

use super::file;

pub const PANIC_MARK_FILE: &str = "panic_mark_file";

pub fn panic_mark_file_path<P: AsRef<Path>>(db_path: P) -> PathBuf {
    db_path.as_ref().parent().unwrap().join(PANIC_MARK_FILE)
}

pub fn exit_with_panic_mark<P: AsRef<Path>>(db_path: P, err_msg: String) {
    let file_path = panic_mark_file_path(db_path);
    File::create(&file_path)
        .unwrap_or_else(|e| panic!("failed to create panic mark file, error: {:?}", e));
    panic!("Occurs unexpected error: {}", err_msg);
}

pub fn panic_mark_file_exists<P: AsRef<Path>>(db_path: P) -> bool {
    let path = panic_mark_file_path(db_path);
    file::file_exists(path)
}

#[cfg(test)]
mod tests {
    use std::fs::File;
    use tempdir::TempDir;

    use super::*;

    #[test]
    fn test_panic_mark_file_path() {
        let dir = TempDir::new("test_panic_mark_file_path").unwrap();
        let db_path = dir.path().join("db");
        let panic_mark_file = panic_mark_file_path(&db_path);
        assert_eq!(
            panic_mark_file,
            db_path.parent().unwrap().join(PANIC_MARK_FILE)
        )
    }

    #[test]
    fn test_panic_mark_file_exists() {
        let dir = TempDir::new("test_panic_mark_file_exists").unwrap();
        let db_path = dir.path().join("db");
        let file_path = panic_mark_file_path(&db_path);
        let _f = File::create(file_path).unwrap();
        assert!(panic_mark_file_exists(&db_path));
    }
}
