// Copyright 2024 TiKV Project Authors. Licensed under Apache-2.0.

use std::sync::Arc;

use kvproto::{
    brpb::CipherInfo,
    encryptionpb::{EncryptedContent, EncryptionMethod},
};

use crate::{DataKeyManager, Error, MultiMasterKeyBackend};

#[derive(Clone)]
pub struct BackupEncryptionManager {
    // Plaintext data key directly passed from user in stream back request,
    // only used to encrypt backup files uploaded to external storage,
    // useful when backup encryption is not configured. Not recommended in
    // production.
    pub plaintext_data_key: Option<CipherInfo>,
    // encryption method for backup data file encryption using master
    pub master_key_based_file_encryption_method: EncryptionMethod,
    // backend that contains zero or multiple master keys
    pub multi_master_key_backend: MultiMasterKeyBackend,
    // used to encrypt local temp files
    pub tikv_data_key_manager: Option<Arc<DataKeyManager>>,
}
impl BackupEncryptionManager {
    pub fn new(
        plaintext_data_key: Option<CipherInfo>,
        master_key_based_file_encryption_method: EncryptionMethod,
        multi_master_key_backend: MultiMasterKeyBackend,
        tikv_data_key_manager: Option<Arc<DataKeyManager>>,
    ) -> Self {
        BackupEncryptionManager {
            plaintext_data_key,
            master_key_based_file_encryption_method,
            multi_master_key_backend,
            tikv_data_key_manager,
        }
    }

    pub fn default() -> Self {
        BackupEncryptionManager {
            plaintext_data_key: None,
            master_key_based_file_encryption_method: EncryptionMethod::default(),
            multi_master_key_backend: MultiMasterKeyBackend::new(),
            tikv_data_key_manager: None,
        }
    }

    pub fn opt_data_key_manager(&self) -> Option<Arc<DataKeyManager>> {
        self.tikv_data_key_manager.clone()
    }

    pub async fn encrypt_data_key(
        &self,
        plaintext_data_key: &[u8],
    ) -> Result<EncryptedContent, Error> {
        self.multi_master_key_backend
            .encrypt(plaintext_data_key)
            .await
    }

    pub async fn decrypt_data_key(
        &self,
        encrypted_content: &EncryptedContent,
    ) -> Result<Vec<u8>, Error> {
        self.multi_master_key_backend
            .decrypt(encrypted_content)
            .await
    }

    pub async fn is_master_key_backend_initialized(&self) -> bool {
        self.multi_master_key_backend.is_initialized().await
    }

    pub fn generate_data_key(&self) -> Result<Vec<u8>, Error> {
        self.multi_master_key_backend
            .generate_data_key(self.master_key_based_file_encryption_method)
    }
}
