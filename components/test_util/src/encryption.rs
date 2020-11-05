// Copyright 2020 TiKV Project Authors. Licensed under Apache-2.0.

use std::{fs::File, io::Write, time::Duration};

use encryption::{DataKeyManager, FileConfig, MasterKeyConfig, Result};
use kvproto::encryptionpb::EncryptionMethod;

pub fn create_test_key_file(path: &str) {
    let mut file = File::create(path).unwrap();
    file.write_all(b"603deb1015ca71be2b73aef0857d77811f352c073b6108d72d9810a30914dff4\n")
        .unwrap();
}

pub fn new_test_key_manager(
    temp: Option<tempfile::TempDir>,
    method: Option<EncryptionMethod>,
    master_key: Option<MasterKeyConfig>,
    previous_master_key: Option<MasterKeyConfig>,
) -> (tempfile::TempDir, Result<Option<DataKeyManager>>) {
    let tmp = temp.unwrap_or_else(|| tempfile::TempDir::new().unwrap());
    let key_path = tmp.path().join("test_key").to_str().unwrap().to_owned();
    create_test_key_file(&key_path);
    let default_config = MasterKeyConfig::File {
        config: FileConfig { path: key_path },
    };
    let master_key = master_key.unwrap_or_else(|| default_config.clone());
    let previous_master_key = previous_master_key.unwrap_or(default_config);
    let manager = DataKeyManager::new(
        &master_key,
        &previous_master_key,
        method.unwrap_or(EncryptionMethod::Aes256Ctr),
        Duration::from_secs(60),
        2,
        tmp.path().as_os_str().to_str().unwrap(),
    );
    (tmp, manager)
}
