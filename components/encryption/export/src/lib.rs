// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.
use std::{fmt::Debug, path::Path};

use async_trait::async_trait;
#[cfg(feature = "cloud-aws")]
use aws::{AwsKms, STORAGE_VENDOR_NAME_AWS};
#[cfg(feature = "cloud-aws")]
use cloud::kms::Config as CloudConfig;
use cloud::{
    kms::{EncryptedKey as CloudEncryptedKey, KmsProvider as CloudKmsProvider},
    Error as CloudError,
};
use derive_more::Deref;
#[cfg(feature = "cloud-aws")]
pub use encryption::KmsBackend;
pub use encryption::{
    encryption_method_from_db_encryption_method, Backend, DataKeyManager, DataKeyManagerArgs,
    DecrypterReader, EncryptionConfig, Error, FileConfig, Iv, KmsConfig, MasterKeyConfig, Result,
};
use encryption::{
    DataKeyPair, EncryptedKey, FileBackend, KmsProvider, PlainKey, PlaintextBackend,
    RetryCodedError,
};
use error_code::{self, ErrorCode, ErrorCodeExt};
use tikv_util::{box_err, error, info, stream::RetryError};

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

fn cloud_convert_error(msg: String) -> Box<dyn FnOnce(CloudError) -> CloudConvertError> {
    Box::new(|err: CloudError| CloudConvertError(err, msg))
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
            let kms_provider = CloudKms(Box::new(
                AwsKms::new(conf).map_err(cloud_convert_error("new AWS KMS".to_owned()))?,
            ));
            Ok(Box::new(KmsBackend::new(Box::new(kms_provider))?) as Box<dyn Backend>)
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

// CloudKMS adapts the KmsProvider definition from the cloud crate to that of the encryption crate
#[derive(Debug, Deref)]
struct CloudKms(Box<dyn CloudKmsProvider>);

#[async_trait]
impl KmsProvider for CloudKms {
    async fn generate_data_key(&self) -> Result<DataKeyPair> {
        let cdk = (**self)
            .generate_data_key()
            .await
            .map_err(cloud_convert_error(format!(
                "{} generate data key API",
                self.name()
            )))?;
        Ok(DataKeyPair {
            plaintext: PlainKey::new(cdk.plaintext.to_vec())?,
            encrypted: EncryptedKey::new(cdk.encrypted.to_vec())?,
        })
    }

    async fn decrypt_data_key(&self, data_key: &EncryptedKey) -> Result<Vec<u8>> {
        let key = CloudEncryptedKey::new((*data_key).to_vec()).map_err(cloud_convert_error(
            format!("{} data key init for decrypt", self.name()),
        ))?;
        Ok((**self)
            .decrypt_data_key(&key)
            .await
            .map_err(cloud_convert_error(format!(
                "{} decrypt data key API",
                self.name()
            )))?)
    }

    fn name(&self) -> &str {
        (**self).name()
    }
}

// CloudConverError adapts cloud errors to encryption errors
// As the abstract RetryCodedError
#[derive(Debug)]
pub struct CloudConvertError(CloudError, String);

impl RetryCodedError for CloudConvertError {}

impl std::fmt::Display for CloudConvertError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_fmt(format_args!("{} {}", &self.0, &self.1))
    }
}

impl std::convert::From<CloudConvertError> for Error {
    fn from(err: CloudConvertError) -> Error {
        Error::RetryCodedError(Box::new(err) as Box<dyn RetryCodedError>)
    }
}

impl RetryError for CloudConvertError {
    fn is_retryable(&self) -> bool {
        self.0.is_retryable()
    }
}

impl ErrorCodeExt for CloudConvertError {
    fn error_code(&self) -> ErrorCode {
        self.0.error_code()
    }
}
