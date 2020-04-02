// Copyright 2020 TiKV Project Authors. Licensed under Apache-2.0.

#[macro_use]
extern crate slog_global;
#[macro_use]
extern crate failure;
#[macro_use]
extern crate serde_derive;

mod config;
mod crypter;
mod encrypted_file;
mod errors;
mod manager;
mod master_key;
mod metrics;

pub use self::config::{EncryptionConfig, MasterKeyConfig};
pub use self::errors::{Error, Result};
pub use self::manager::DataKeyManager;
