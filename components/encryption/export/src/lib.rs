// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.
use std::path::Path;

#[cfg(feature = "cloud-aws")]
use aws::{AwsKms, STORAGE_VENDOR_NAME_AWS};
#[cfg(feature = "cloud-azure")]
use azure::{AzureKms, STORAGE_VENDOR_NAME_AZURE};
use cloud::kms::Config as CloudConfig;
#[cfg(feature = "cloud-aws")]
pub use encryption::KmsBackend;
pub use encryption::{
    clean_up_dir, clean_up_trash, from_engine_encryption_method, trash_dir_all, AzureKmsConfig,
    Backend, DataKeyImporter, DataKeyManager, DataKeyManagerArgs, DecrypterReader,
    EncryptionConfig, Error, FileConfig, Iv, KmsConfig, MasterKeyConfig, Result,
};
use encryption::{cloud_convert_error, FileBackend, PlaintextBackend};
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

pub fn create_backend(config: &MasterKeyConfig) -> Result<Box<dyn Backend>> {
    let result = create_backend_inner(config);
    if let Err(e) = result {
        error!("failed to access master key, {}", e);
        return Err(e);
    };
    result
}

pub fn create_cloud_backend(config: &KmsConfig) -> Result<Box<dyn Backend>> {
    info!("Encryption init aws backend";
        "region" => &config.region,
        "endpoint" => &config.endpoint,
        "key_id" => &config.key_id,
        "vendor" => &config.vendor,
    );
    match config.vendor.as_str() {
        #[cfg(feature = "cloud-aws")]
        STORAGE_VENDOR_NAME_AWS | "" => {
            let conf = CloudConfig::from_proto(config.clone().into_proto())
                .map_err(cloud_convert_error("aws from proto".to_owned()))?;
            let kms_provider =
                Box::new(AwsKms::new(conf).map_err(cloud_convert_error("new AWS KMS".to_owned()))?);
            Ok(Box::new(KmsBackend::new(kms_provider)?) as Box<dyn Backend>)
        }
        #[cfg(feature = "cloud-azure")]
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
            Ok(Box::new(KmsBackend::new(keyvault_provider)?) as Box<dyn Backend>)
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
        MasterKeyConfig::Kms { config } => return create_cloud_backend(config),
    })
}
