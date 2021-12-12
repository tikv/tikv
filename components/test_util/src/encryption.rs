// Copyright 2020 TiKV Project Authors. Licensed under Apache-2.0.

use std::{fs::File, io::Write, time::Duration};

use encryption_export::{
    create_backend, DataKeyManager, DataKeyManagerArgs, EncryptionConfig, FileConfig,
    MasterKeyConfig, Result,
};
use kvproto::encryptionpb::EncryptionMethod;
use tikv_util::config::ReadableDuration;

pub fn create_test_key_file(path: &str) {
    let mut file = File::create(path).unwrap();
    file.write_all(b"603deb1015ca71be2b73aef0857d77811f352c073b6108d72d9810a30914dff4\n")
        .unwrap();
}

fn new_test_file_master_key(tmp: &tempfile::TempDir) -> MasterKeyConfig {
    let key_path = tmp.path().join("test_key").to_str().unwrap().to_owned();
    create_test_key_file(&key_path);
    MasterKeyConfig::File {
        config: FileConfig { path: key_path },
    }
}

pub fn new_file_security_config(dir: &tempfile::TempDir) -> EncryptionConfig {
    let master_key_cfg = new_test_file_master_key(dir);
    EncryptionConfig {
        data_encryption_method: EncryptionMethod::Aes256Ctr,
        data_key_rotation_period: ReadableDuration::days(7),
        enable_file_dictionary_log: true,
        file_dictionary_rewrite_threshold: 100000,
        master_key: master_key_cfg.clone(),
        previous_master_key: master_key_cfg,
    }
}

pub fn new_test_key_manager(
    tmp_dir: &tempfile::TempDir,
    method: Option<EncryptionMethod>,
    master_key: Option<MasterKeyConfig>,
    previous_master_key: Option<MasterKeyConfig>,
) -> Result<Option<DataKeyManager>> {
    let default_config = new_test_file_master_key(tmp_dir);
    let master_key = master_key.unwrap_or_else(|| default_config.clone());
    let previous_master_key = previous_master_key.unwrap_or(default_config);
    DataKeyManager::new(
        create_backend(&master_key)?,
        Box::new(move || create_backend(&previous_master_key)),
        DataKeyManagerArgs {
            method: method.unwrap_or(EncryptionMethod::Aes256Ctr),
            rotation_period: Duration::from_secs(60),
            enable_file_dictionary_log: true,
            file_dictionary_rewrite_threshold: 2,
            dict_path: tmp_dir.path().as_os_str().to_str().unwrap().to_string(),
        },
    )
}
