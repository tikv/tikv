// Copyright 2024 TiKV Project Authors. Licensed under Apache-2.0.

use std::{io, sync::Arc};

use encryption::{BackupEncryptionManager, DataKeyManager, MultiMasterKeyBackend};
use encryption_export::create_async_backend;
use tikv::config::TikvConfig;

pub async fn build_backup_encryption_manager(
    config: TikvConfig,
    opt_encryption_key_manager: Option<Arc<DataKeyManager>>,
) -> Result<BackupEncryptionManager, io::Error> {
    let multi_master_key_backend = MultiMasterKeyBackend::new();
    multi_master_key_backend
        .update_from_config_if_needed(
            config.backup_encryption_config.master_keys.clone(),
            create_async_backend,
        )
        .await?;

    Ok(BackupEncryptionManager::new(
        None,
        config.backup_encryption_config.data_encryption_method,
        multi_master_key_backend,
        opt_encryption_key_manager,
    ))
}
