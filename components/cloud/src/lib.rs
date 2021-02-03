// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.
//
// The cloud crate defines the interaction between
// the cloud provider crates and other TiKV crates

#[macro_use]
extern crate failure;

#[macro_use]
extern crate slog_global;

pub mod external_storage {
    pub use external_storage::{
        block_on_external_io, error_stream, retry, AsyncReadAsSyncStreamOfBytes, ExternalStorage,
        RetryError,
    };
}

pub mod error;
pub use error::{Error, ErrorTrait, Result};

pub mod kms;
#[cfg(test)]
pub use kms::fake;
pub use kms::{Config, DataKeyPair, EncryptedKey, KeyId, KmsProvider, PlainKey};

pub mod blob;
pub use blob::{none_to_empty, BucketConf, StringNonEmpty};
