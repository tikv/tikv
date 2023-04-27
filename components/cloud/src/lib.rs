// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.
//
// The cloud crate defines the interaction between
// the cloud provider crates and other TiKV crates

#![feature(min_specialization)]

pub mod error;
pub use error::{Error, ErrorTrait, Result};

pub mod crypter;
pub use crypter::{Config, CrypterProvider, DataKeyPair, EncryptedKey, KeyId, PlainKey};

pub mod blob;
pub use blob::{none_to_empty, BucketConf, StringNonEmpty};

pub mod metrics;
