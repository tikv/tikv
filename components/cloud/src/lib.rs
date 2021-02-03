// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.
//
// The cloud crate defines the interaction between
// the cloud provider crates and other TiKV crates

#[macro_use]
extern crate failure;

pub mod external_storage {
    pub use external_storage::{
        block_on_external_io, empty_to_none, error_stream, none_to_empty, retry,
        AsyncReadAsSyncStreamOfBytes, BucketConf, ExternalStorage, RetryError,
    };
}

pub mod error;
pub use error::{Error, ErrorTrait, Result};

pub mod kms;
#[cfg(test)]
pub use kms::fake;
pub use kms::{Config, DataKeyPair, EncryptedKey, KeyId, KmsProvider};
