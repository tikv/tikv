// Copyright 2020 TiKV Project Authors. Licensed under Apache-2.0.

#[macro_use(fail_point)]
extern crate fail;
#[macro_use]
extern crate failure;
#[macro_use]
extern crate serde_derive;
#[macro_use]
extern crate tikv_util;
#[macro_use]
extern crate configuration;

mod config;
mod crypter;
mod encrypted_file;
mod errors;
mod file_dict_file;
mod io;
mod manager;
mod master_key;
mod metrics;

pub use self::config::*;
pub use self::crypter::{
    encryption_method_from_db_encryption_method, verify_encryption_config, AesGcmCrypter, Iv,
    PlainKey,
};
pub use self::encrypted_file::EncryptedFile;
pub use self::errors::{Error, Result, RetryCodedError};
pub use self::io::{create_aes_ctr_crypter, DecrypterReader, EncrypterReader, EncrypterWriter};
pub use self::manager::{DataKeyManager, DataKeyManagerArgs};
pub use self::master_key::{
    Backend, DataKeyPair, EncryptedKey, FileBackend, KmsBackend, KmsProvider, PlaintextBackend,
};
