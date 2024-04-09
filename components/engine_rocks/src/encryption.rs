// Copyright 2020 TiKV Project Authors. Licensed under Apache-2.0.

use std::{io::Result, sync::Arc};

use encryption::{DataKeyManager, FileEncryptionInfo};
use kvproto::encryptionpb::EncryptionMethod;
use rocksdb::{
    DBEncryptionMethod, EncryptionKeyManager, FileEncryptionInfo as DBFileEncryptionInfo,
};

use crate::{r2e, raw::Env};

// Use engine::Env directly since Env is not abstracted.
pub(crate) fn get_env(
    base_env: Option<Arc<Env>>,
    key_manager: Option<Arc<DataKeyManager>>,
) -> engine_traits::Result<Option<Arc<Env>>> {
    if let Some(manager) = key_manager {
        let base_env = base_env.unwrap_or_else(|| Arc::new(Env::default()));
        Ok(Some(Arc::new(
            Env::new_key_managed_encrypted_env(base_env, WrappedEncryptionKeyManager { manager })
                .map_err(r2e)?,
        )))
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
    fn delete_file(&self, fname: &str, physical_fname: Option<&str>) -> Result<()> {
        self.manager.delete_file(fname, physical_fname)
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
        EncryptionMethod::Sm4Ctr => DBEncryptionMethod::Sm4Ctr,
        EncryptionMethod::Unknown => DBEncryptionMethod::Unknown,
    }
}
