// Copyright 2024 TiKV Project Authors. Licensed under Apache-2.0.

use std::{io, sync::Arc};

use encryption::{BackupEncryptionManager, DataKeyManager, MultiMasterKeyBackend};
use kvproto::encryptionpb::EncryptionMethod;

pub fn build_backup_encryption_manager(
    opt_encryption_key_manager: Option<Arc<DataKeyManager>>,
) -> Result<BackupEncryptionManager, io::Error> {
    let multi_master_key_backend = MultiMasterKeyBackend::new();

    Ok(BackupEncryptionManager::new(
        None,
        EncryptionMethod::Plaintext,
        multi_master_key_backend,
        opt_encryption_key_manager,
    ))
}
