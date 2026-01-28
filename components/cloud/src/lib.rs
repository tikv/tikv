// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.
//
// The cloud crate defines the interaction between
// the cloud provider crates and other TiKV crates

#![feature(test)]
#![feature(min_specialization)]

pub mod error;
pub use error::{Error, ErrorTrait, Result};

pub const STORAGE_VENDOR_NAME_GCP: &str = "gcp";
pub const STORAGE_VENDOR_NAME_GCP_V2: &str = "gcp_v2";

pub mod kms;
pub use kms::{Config, DataKeyPair, EncryptedKey, KeyId, KmsProvider, PlainKey, SubConfigAzure};

pub mod blob;
pub use blob::{BucketConf, StringNonEmpty, none_to_empty};

pub mod metrics;
