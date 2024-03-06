// Copyright 2020 TiKV Project Authors. Licensed under Apache-2.0.

mod config;
mod crypter;
mod encrypted_file;
mod errors;
mod file_dict_file;
mod io;
mod manager;
mod master_key;
mod metrics;

pub use self::{
    config::*,
    crypter::{
        from_engine_encryption_method, to_engine_encryption_method, verify_encryption_config,
        AesGcmCrypter, Iv, PlainKey,
    },
    encrypted_file::EncryptedFile,
    errors::{Error, Result, RetryCodedError},
    file_dict_file::FileDictionaryFile,
    io::{
        create_aes_ctr_crypter, DecrypterReader, DecrypterWriter, EncrypterReader, EncrypterWriter,
    },
    manager::{DataKeyManager, DataKeyManagerArgs},
    master_key::{
        Backend, DataKeyPair, EncryptedKey, FileBackend, KmsBackend, KmsProvider, PlaintextBackend,
    },
};
