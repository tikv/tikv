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
<<<<<<< HEAD
    crypter::{
        compat, encryption_method_from_db_encryption_method,
        encryption_method_to_db_encryption_method, verify_encryption_config, AesGcmCrypter, Iv,
        PlainKey,
    },
=======
    crypter::{verify_encryption_config, AesGcmCrypter, FileEncryptionInfo, Iv},
>>>>>>> d96284cb29 (encryption: remove useless `EncryptionKeyManager` trait (#16086))
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
