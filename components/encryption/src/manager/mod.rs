use std::collections::hash_map::{Entry, VacantEntry};
use std::io::{Error as IoError, ErrorKind};
use std::sync::{Arc, RwLock};

use kvproto::encryptionpb::{DataKey, EncryptionMethod, FileDictionary, FileInfo, KeyDictionary};

use self::rockmock::*;
use crate::master_key::Backend;
use crate::{Error, Iv, Result};

struct Dicts {
    file_dict: FileDictionary,
    key_dict: KeyDictionary,
}

impl Dicts {
    fn get_key(&self, key_id: u64) -> Option<&DataKey> {
        self.key_dict.keys.get(&key_id)
    }

    fn current_data_key(&self) -> (u64, &DataKey) {
        (
            self.key_dict.current_key_id,
            self.key_dict
                .keys
                .get(&self.key_dict.current_key_id)
                .unwrap_or_else(|| {
                    panic!(
                        "current data key not found! Length of keys {}",
                        self.key_dict.keys.len()
                    );
                }),
        )
    }

    fn get_file(&self, file_path: &str) -> Result<&FileInfo> {
        let file = self.file_dict.files.get(file_path).ok_or_else(|| {
            Error::Io(IoError::new(
                ErrorKind::NotFound,
                format!("file is not found, {}", file_path),
            ))
        })?;
        Ok(file)
    }

    fn new_file(&mut self, file_path: &str, method: EncryptionMethod) -> Result<&FileInfo> {
        let mut file = FileInfo::default();
        file.iv = Iv::new().as_slice().to_vec();
        file.key_id = self.key_dict.current_key_id;
        file.method = method;
        let mut entry = self.file_dict.files.entry(file_path.to_owned());
        let file = match entry {
            Entry::Vacant(e) => Ok(e.insert(file)),
            Entry::Occupied(e) => Err(Error::Io(IoError::new(
                ErrorKind::AlreadyExists,
                format!("file already exists {}", file_path),
            ))),
        }?;
        // self.save_file_dict()?;
        Ok(file)
    }

    fn delete_file(&mut self, file_path: &str) -> Result<()> {
        self.file_dict.files.remove(file_path).ok_or_else(|| {
            Error::Io(IoError::new(
                ErrorKind::NotFound,
                format!("file is not found, {}", file_path),
            ))
        })?;
        self.save_file_dict()
        // TOOD GC unused data keys.
    }

    // TODO save key dict and file dict. key dict must be encrypted by master key.
    fn save_key_dict(&self) -> Result<()> {
        unimplemented!()
    }
    fn save_file_dict(&self) -> Result<()> {
        unimplemented!()
    }
}

#[derive(Clone)]
pub struct KeyManagerImpl {
    master_key_backend: Arc<dyn Backend>,
    dicts: Arc<RwLock<Dicts>>,
    method: EncryptionMethod,
}

impl KeyManager for KeyManagerImpl {
    // Get key to open existing file.
    fn get_file(&self, file_path: &str) -> Result<FileEncryptionInfo> {
        let dicts = self.dicts.read().unwrap();
        let file = dicts.get_file(file_path)?;
        let key_id = file.key_id;
        let data_key = dicts.get_key(key_id);
        let key = data_key.map(|k| k.key.clone()).unwrap_or_else(|| vec![]);
        let encrypted_file = FileEncryptionInfo {
            key_id,
            key,
            method: file.method,
            iv: file.get_iv().to_owned(),
        };
        Ok(encrypted_file)
    }

    fn new_file(&self, file_path: &str) -> Result<FileEncryptionInfo> {
        let mut dicts = self.dicts.write().unwrap();
        let (key_id, data_key) = dicts.current_data_key();
        let key = data_key.get_key().to_owned();
        let file = dicts.new_file(file_path, self.method)?;
        let encrypted_file = FileEncryptionInfo {
            key_id,
            key,
            method: self.method,
            iv: file.get_iv().to_owned(),
        };
        Ok(encrypted_file)
    }

    fn delete_file(&self, file_path: &str) -> Result<()> {
        self.dicts.write().unwrap().delete_file(file_path)
    }
}

// Will be moved into rust-rocksdb
mod rockmock {
    use crate::{Error, Result};
    use kvproto::encryptionpb::{DataKey, EncryptionMethod, FileDictionary, FileInfo};

    pub struct FileEncryptionInfo {
        pub key_id: u64,
        pub key: Vec<u8>,
        pub method: EncryptionMethod, // Can be PLAINTEXT if file not found in file dictionary.
        pub iv: Vec<u8>,
    }

    pub trait KeyManager {
        // Get key to open existing file.
        fn get_file(&self, file_path: &str) -> Result<FileEncryptionInfo>;
        fn new_file(&self, file_path: &str) -> Result<FileEncryptionInfo>;
        fn delete_file(&self, file_path: &str) -> Result<()>;
    }
}
