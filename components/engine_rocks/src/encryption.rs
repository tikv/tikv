// Copyright 2020 TiKV Project Authors. Licensed under Apache-2.0.

use std::{io::Result, sync::Arc};

<<<<<<< HEAD
use encryption::{self, DataKeyManager};
use engine_traits::{EncryptionKeyManager, EncryptionMethod, FileEncryptionInfo};
=======
use encryption::{DataKeyManager, FileEncryptionInfo};
use kvproto::encryptionpb::EncryptionMethod;
>>>>>>> d96284cb29 (encryption: remove useless `EncryptionKeyManager` trait (#16086))
use rocksdb::{
    DBEncryptionMethod, EncryptionKeyManager, FileEncryptionInfo as DBFileEncryptionInfo,
};

use crate::raw::Env;

// Use engine::Env directly since Env is not abstracted.
pub(crate) fn get_env(
    base_env: Option<Arc<Env>>,
    key_manager: Option<Arc<DataKeyManager>>,
<<<<<<< HEAD
) -> std::result::Result<Arc<Env>, String> {
    let base_env = base_env.unwrap_or_else(|| Arc::new(Env::default()));
=======
) -> engine_traits::Result<Option<Arc<Env>>> {
>>>>>>> d96284cb29 (encryption: remove useless `EncryptionKeyManager` trait (#16086))
    if let Some(manager) = key_manager {
        Ok(Arc::new(Env::new_key_managed_encrypted_env(
            base_env,
            WrappedEncryptionKeyManager { manager },
        )?))
    } else {
        Ok(base_env)
    }
}

pub struct WrappedEncryptionKeyManager {
    manager: Arc<DataKeyManager>,
}

impl WrappedEncryptionKeyManager {
    pub fn new(manager: Arc<DataKeyManager>) -> Self {
        Self { manager }
    }
}

impl EncryptionKeyManager for WrappedEncryptionKeyManager {
    fn get_file(&self, fname: &str) -> Result<DBFileEncryptionInfo> {
        self.manager
            .get_file(fname)
            .map(convert_file_encryption_info)
    }
    fn new_file(&self, fname: &str) -> Result<DBFileEncryptionInfo> {
        self.manager
            .new_file(fname)
            .map(convert_file_encryption_info)
    }
    fn delete_file(&self, fname: &str) -> Result<()> {
        self.manager.delete_file(fname)
    }
    fn link_file(&self, src_fname: &str, dst_fname: &str) -> Result<()> {
        self.manager.link_file(src_fname, dst_fname)
    }
}

fn convert_file_encryption_info(input: FileEncryptionInfo) -> DBFileEncryptionInfo {
    DBFileEncryptionInfo {
        method: convert_encryption_method(input.method),
        key: input.key,
        iv: input.iv,
    }
}

fn convert_encryption_method(input: EncryptionMethod) -> DBEncryptionMethod {
    match input {
        EncryptionMethod::Plaintext => DBEncryptionMethod::Plaintext,
        EncryptionMethod::Aes128Ctr => DBEncryptionMethod::Aes128Ctr,
        EncryptionMethod::Aes192Ctr => DBEncryptionMethod::Aes192Ctr,
        EncryptionMethod::Aes256Ctr => DBEncryptionMethod::Aes256Ctr,
        EncryptionMethod::Unknown => DBEncryptionMethod::Unknown,
    }
}
