// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.
//
// The cloud crate defines the interaction between
// the cloud provider crates and other TiKV crates

#![feature(min_specialization)]

#[macro_use]
extern crate failure;

pub mod error;
pub use error::{Error, ErrorTrait, Result};

pub mod kms;
#[cfg(test)]
pub use kms::{Config, DataKeyPair, EncryptedKey, KeyId, KmsProvider, PlainKey};
