// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

mod azblob;
mod kms;
mod token_credentials;

pub use azblob::{AzureStorage, Config};
pub use kms::AzureKms;
pub use token_credentials::certificate_credentials::ClientCertificateCredentialExt;

pub const STORAGE_VENDOR_NAME_AZURE: &str = "azure";
