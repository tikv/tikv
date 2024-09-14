// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.
use std::path::Path;

use aws::{AwsKms, STORAGE_VENDOR_NAME_AWS};
use azure::{AzureKms, STORAGE_VENDOR_NAME_AZURE};
use cloud::kms::Config as CloudConfig;
pub use encryption::{
    clean_up_dir, clean_up_trash, trash_dir_all, AsyncBackend, AzureConfig, Backend,
    BackupEncryptionConfig, DataKeyImporter, DataKeyManager, DataKeyManagerArgs, DecrypterReader,
    EncryptionConfig, Error, FileConfig, Iv, KmsBackend, KmsConfig, MasterKeyConfig, Result,
};
use encryption::{cloud_convert_error, FileBackend, PlaintextBackend};
use gcp::{GcpKms, STORAGE_VENDOR_NAME_GCP};
use tikv_util::{box_err, error, info};

pub fn data_key_manager_from_config(
    config: &EncryptionConfig,
    dict_path: &str,
) -> Result<Option<DataKeyManager>> {
    let master_key = create_backend(&config.master_key).map_err(|e| {
        error!("failed to access master key, {}", e);
        e
    })?;
    let args = DataKeyManagerArgs::from_encryption_config(dict_path, config);
    let previous_master_key_conf = config.previous_master_key.clone();
    let previous_master_key = Box::new(move || create_backend(&previous_master_key_conf));
    DataKeyManager::new(master_key, previous_master_key, args)
}

pub fn create_async_backend(config: &MasterKeyConfig) -> Result<Box<dyn AsyncBackend>> {
    let result = create_async_backend_inner(config);
    if let Err(e) = result {
        error!("failed to access master key, {}", e);
        return Err(e);
    };
    result
}

pub fn create_backend(config: &MasterKeyConfig) -> Result<Box<dyn Backend>> {
    let result = create_backend_inner(config);
    if let Err(e) = result {
        error!("failed to access master key, {}", e);
        return Err(e);
    };
    result
}

pub fn create_cloud_backend(config: &KmsConfig) -> Result<Box<KmsBackend>> {
    info!("Encryption init KMS backend";
        "region" => &config.region,
        "endpoint" => &config.endpoint,
        "key_id" => &config.key_id,
        "vendor" => &config.vendor,
    );
    match config.vendor.as_str() {
        STORAGE_VENDOR_NAME_AWS | "" => {
            let conf = CloudConfig::from_proto(config.clone().into_proto())
                .map_err(cloud_convert_error("aws from proto".to_owned()))?;
            let kms_provider =
                Box::new(AwsKms::new(conf).map_err(cloud_convert_error("new AWS KMS".to_owned()))?);
            Ok(Box::new(KmsBackend::new(kms_provider)?))
        }
        STORAGE_VENDOR_NAME_AZURE => {
            if config.azure.is_none() {
                return Err(Error::Other(box_err!(
                    "invalid configurations for Azure KMS"
                )));
            }
            let (mk, azure_kms_cfg) = config.clone().convert_to_azure_kms_config();
            let conf = CloudConfig::from_azure_kms_config(mk, azure_kms_cfg)
                .map_err(cloud_convert_error("azure from proto".to_owned()))?;
            let keyvault_provider = Box::new(
                AzureKms::new(conf).map_err(cloud_convert_error("new Azure KMS".to_owned()))?,
            );
            Ok(Box::new(KmsBackend::new(keyvault_provider)?))
        }
        STORAGE_VENDOR_NAME_GCP => {
            let (mk, gcp_cfg) = config.clone().convert_to_gcp_config();
            let conf = CloudConfig::from_gcp_kms_config(mk, gcp_cfg)
                .map_err(cloud_convert_error("gcp from proto".to_owned()))?;
            let kms_provider =
                GcpKms::new(conf).map_err(cloud_convert_error("new GCP KMS".to_owned()))?;
            Ok(Box::new(KmsBackend::new(Box::new(kms_provider))?))
        }
        provider => Err(Error::Other(box_err!("provider not found {}", provider))),
    }
}

fn create_backend_inner(config: &MasterKeyConfig) -> Result<Box<dyn Backend>> {
    Ok(match config {
        MasterKeyConfig::Plaintext => Box::new(PlaintextBackend {}) as _,
        MasterKeyConfig::File { config } => {
            Box::new(FileBackend::new(Path::new(&config.path))?) as _
        }
        MasterKeyConfig::Kms { config } => create_cloud_backend(config)? as Box<dyn Backend>,
    })
}

fn create_async_backend_inner(config: &MasterKeyConfig) -> Result<Box<dyn AsyncBackend>> {
    Ok(match config {
        MasterKeyConfig::Plaintext => Box::new(PlaintextBackend {}) as _,
        MasterKeyConfig::File { config } => {
            Box::new(FileBackend::new(Path::new(&config.path))?) as _
        }
        MasterKeyConfig::Kms { config } => create_cloud_backend(config)? as Box<dyn AsyncBackend>,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_kms_cloud_backend_azure() {
        let config = KmsConfig {
            key_id: "key_id".to_owned(),
            region: "region".to_owned(),
            endpoint: "endpoint".to_owned(),
            vendor: STORAGE_VENDOR_NAME_AZURE.to_owned(),
            azure: Some(AzureConfig {
                tenant_id: "tenant_id".to_owned(),
                client_id: "client_id".to_owned(),
                keyvault_url: "https://keyvault_url.vault.azure.net".to_owned(),
                hsm_name: "hsm_name".to_owned(),
                hsm_url: "https://hsm_url.managedhsm.azure.net/".to_owned(),
                client_secret: Some("client_secret".to_owned()),
                ..AzureConfig::default()
            }),
            gcp: None,
        };
        let invalid_config = KmsConfig {
            azure: None,
            ..config.clone()
        };
        create_cloud_backend(&invalid_config).unwrap_err();
        let backend = create_cloud_backend(&config).unwrap();
        assert!(backend.is_secure());
    }
}
