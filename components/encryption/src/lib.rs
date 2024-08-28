// Copyright 2020 TiKV Project Authors. Licensed under Apache-2.0.

mod config;
mod crypter;
mod encrypted_file;
mod errors;
mod file_dict_file;
mod io;
mod manager;
mod master_key;
#[cfg(any(test, feature = "testexport"))]
pub use master_key::fake;
mod metrics;

use std::{io::ErrorKind, path::Path};

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

const TRASH_PREFIX: &str = "TRASH-";

/// Remove a directory.
///
/// Rename it before actually removal.
#[inline]
pub fn trash_dir_all(
    path: impl AsRef<Path>,
    key_manager: Option<&DataKeyManager>,
) -> std::io::Result<()> {
    let path = path.as_ref();
    let name = match path.file_name() {
        Some(n) => n,
        None => {
            return Err(std::io::Error::new(
                ErrorKind::InvalidInput,
                "path is invalid",
            ));
        }
    };
    let trash_path = path.with_file_name(format!("{}{}", TRASH_PREFIX, name.to_string_lossy()));
    if let Err(e) = file_system::rename(path, &trash_path) {
        if e.kind() == ErrorKind::NotFound {
            return Ok(());
        }
        return Err(e);
    } else if let Some(m) = key_manager {
        m.remove_dir(path, Some(&trash_path))?;
    }
    file_system::remove_dir_all(trash_path)
}

/// When using `trash_dir_all`, it's possible the directory is marked as trash
/// but not being actually deleted after a restart. This function can be used
/// to resume all those removal in the given directory.
#[inline]
pub fn clean_up_trash(
    path: impl AsRef<Path>,
    key_manager: Option<&DataKeyManager>,
) -> std::io::Result<()> {
    for e in file_system::read_dir(path)? {
        let e = e?;
        let os_fname = e.file_name();
        let fname = os_fname.to_str().unwrap();
        if let Some(original) = fname.strip_prefix(TRASH_PREFIX) {
            let original = e.path().with_file_name(original);
            if let Some(m) = &key_manager {
                m.remove_dir(&original, Some(&e.path()))?;
            }
            file_system::remove_dir_all(e.path())?;
        }
    }
    Ok(())
}

/// Removes all directories with the given prefix.
#[inline]
pub fn clean_up_dir(
    path: impl AsRef<Path>,
    prefix: &str,
    key_manager: Option<&DataKeyManager>,
) -> std::io::Result<()> {
    for e in file_system::read_dir(path)? {
        let e = e?;
        let fname = e.file_name().to_str().unwrap().to_owned();
        if fname.starts_with(prefix) {
            if let Some(m) = &key_manager {
                m.remove_dir(&e.path(), None)?;
            }
            file_system::remove_dir_all(e.path())?;
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use tempfile::Builder;

    use super::*;

    #[test]
    fn test_trash_dir_all() {
        let tmp_dir = Builder::new()
            .prefix("test_reserve_space_for_recover")
            .tempdir()
            .unwrap();
        let data_path = tmp_dir.path();
        let sub_dir0 = data_path.join("sub_dir0");
        let trash_sub_dir0 = data_path.join(format!("{}sub_dir0", TRASH_PREFIX));
        file_system::create_dir_all(&sub_dir0).unwrap();
        assert!(sub_dir0.exists());

        trash_dir_all(&sub_dir0, None).unwrap();
        assert!(!sub_dir0.exists());
        assert!(!trash_sub_dir0.exists());

        file_system::create_dir_all(&sub_dir0).unwrap();
        file_system::create_dir_all(&trash_sub_dir0).unwrap();
        trash_dir_all(&sub_dir0, None).unwrap();
        assert!(!sub_dir0.exists());
        assert!(!trash_sub_dir0.exists());

        clean_up_trash(data_path, None).unwrap();

        file_system::create_dir_all(&trash_sub_dir0).unwrap();
        assert!(trash_sub_dir0.exists());
        clean_up_trash(data_path, None).unwrap();
        assert!(!trash_sub_dir0.exists());

        file_system::create_dir_all(&sub_dir0).unwrap();
        assert!(sub_dir0.exists());
        clean_up_dir(data_path, "sub", None).unwrap();
        assert!(!sub_dir0.exists());
    }
}
