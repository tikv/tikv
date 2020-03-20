// Copyright 2020 TiKV Project Authors. Licensed under Apache-2.0.

use std::io::Result;

use engine_traits::{EncryptionKeyManager, EncryptionMethod, FileEncryptionInfo};
use rocksdb::{
    DBEncryptionMethod, EncryptionKeyManager as DBEncryptionKeyManager,
    FileEncryptionInfo as DBFileEncryptionInfo,
};

pub struct WrappedEncryptionKeyManager<T: EncryptionKeyManager> {
    manager: T,
}

impl<T: EncryptionKeyManager> DBEncryptionKeyManager for WrappedEncryptionKeyManager<T> {
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
    fn rename_file(&self, src_fname: &str, dst_fname: &str) -> Result<()> {
        self.manager.rename_file(src_fname, dst_fname)
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
