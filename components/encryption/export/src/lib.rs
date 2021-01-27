#[macro_use]
extern crate slog_global;

use std::path::Path;

#[cfg(feature = "cloud-aws")]
pub use aws::AwsKms;
#[cfg(feature = "cloud-aws")]
pub use encryption::KmsBackend;
pub use encryption::{
    encryption_method_from_db_encryption_method, Backend, DataKeyManager, DataKeyManagerArgs,
    DecrypterReader, EncryptionConfig, Error, FileConfig, Iv, KmsConfig, MasterKeyConfig, Result,
};
use encryption::{FileBackend, PlaintextBackend};
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

fn create_backend_inner(config: &MasterKeyConfig) -> Result<Box<dyn Backend>> {
    Ok(match config {
        MasterKeyConfig::Plaintext => Box::new(PlaintextBackend {}) as _,
        MasterKeyConfig::File { config } => {
            Box::new(FileBackend::new(Path::new(&config.path))?) as _
        }
        MasterKeyConfig::Kms { config } => match config.provider.as_str() {
            #[cfg(feature = "cloud-aws")]
            "aws" | "" => {
                let kms_provider = AwsKms::new(config.clone())?;
                Box::new(KmsBackend::new(Box::new(kms_provider))?) as _
            }
            provider => return Err(Error::Other(box_err!("provider not found {}", provider))),
        },
    })
}
