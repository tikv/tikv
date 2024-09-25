// Copyright 2024 TiKV Project Authors. Licensed under Apache-2.0.

use std::{fs::File, io::Write, path::PathBuf};

use rand::Rng;
use tempfile::TempDir;
pub fn create_master_key_file_test_only(val: &str) -> (PathBuf, TempDir) {
    let tmp_dir = TempDir::new().unwrap();
    let path = tmp_dir.path().join("master_key");
    let mut file = File::create(path.clone()).unwrap();
    file.write_all(format!("{}\n", val).as_bytes()).unwrap();
    (path, tmp_dir)
}

pub fn generate_random_master_key() -> String {
    let master_key: [u8; 32] = rand::thread_rng().gen();
    hex::encode(master_key)
}
