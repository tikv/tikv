// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.
use std::path::Path;

#[cfg(feature = "cloud-aws")]
use aws::{AwsKms, STORAGE_VENDOR_NAME_AWS};
use cloud::kms::Config as CloudConfig;
#[cfg(feature = "cloud-aws")]
pub use encryption::KmsBackend;
pub use encryption::{
    clean_up_dir, clean_up_trash, from_engine_encryption_method, trash_dir_all, Backend,
    DataKeyImporter, DataKeyManager, DataKeyManagerArgs, DecrypterReader, EncryptionConfig, Error,
    FileConfig, Iv, KmsConfig, MasterKeyConfig, Result,
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
    info!("Encryption init cloud backend";
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
