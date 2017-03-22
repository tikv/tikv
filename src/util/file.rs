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

use std::io;
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

pub fn delete_file_if_exist(file: &PathBuf) -> bool {
    if !file_exists(file) {
        return true;
    }
    if let Err(e) = fs::remove_file(file) {
        warn!("failed to delete file {}: {:?}", file.display(), e);
        return false;
    }
    true
}
