#![feature(min_specialization)]
#[macro_use]
extern crate slog_global;

use async_trait::async_trait;
use derive_more::Deref;
use error_code::{self, ErrorCode, ErrorCodeExt};
use std::fmt::Debug;
use std::path::Path;
use tikv_util::impl_format_delegate_newtype;
use tikv_util::stream::RetryError;

#[cfg(feature = "aws")]
use aws::{AwsKms, AWS_VENDOR_NAME};
use cloud::kms::{
    Config as CloudConfig, EncryptedKey as CloudEncryptedKey, KmsProvider as CloudKmsProvider,
};
use cloud::Error as CloudError;
#[cfg(feature = "aws")]
pub use encryption::KmsBackend;
pub use encryption::{
    encryption_method_from_db_encryption_method, Backend, DataKeyManager, DataKeyManagerArgs,
    DecrypterReader, EncryptionConfig, Error, FileConfig, Iv, KmsConfig, MasterKeyConfig, Result,
};
use encryption::{
    DataKeyPair, EncryptedKey, FileBackend, KmsProvider, PlainKey, PlaintextBackend,
    RetryCodedError,
};
use tikv_util::box_err;

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
    Ok(match config.vendor.as_str() {
        #[cfg(feature = "aws")]
        AWS_VENDOR_NAME | "" => {
            let conf =
                CloudConfig::from_proto(config.clone().into_proto()).map_err(CloudConvertError)?;
            let kms_provider = CloudKms(Box::new(AwsKms::new(conf).map_err(CloudConvertError)?));
            Box::new(KmsBackend::new(Box::new(kms_provider))?) as Box<dyn Backend>
        }
        provider => return Err(Error::Other(box_err!("provider not found {}", provider))),
    })
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
            .map_err(CloudConvertError)?;
        Ok(DataKeyPair {
            plaintext: PlainKey::new(cdk.plaintext.to_vec())?,
            encrypted: EncryptedKey::new(cdk.encrypted.to_vec())?,
        })
    }

    async fn decrypt_data_key(&self, data_key: &EncryptedKey) -> Result<Vec<u8>> {
        let key = CloudEncryptedKey::new((*data_key).to_vec()).map_err(CloudConvertError)?;
        Ok((**self)
            .decrypt_data_key(&key)
            .await
            .map_err(CloudConvertError)?)
    }

    fn name(&self) -> &[u8] {
        (**self).name()
    }
}

// CloudConverError adapts cloud errors to encryption errors
// As the abstract RetryCodedError
#[derive(Debug, Deref)]
pub struct CloudConvertError(CloudError);

impl RetryCodedError for CloudConvertError {}

impl_format_delegate_newtype!(CloudConvertError);

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
