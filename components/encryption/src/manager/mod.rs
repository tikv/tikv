use std::collections::hash_map::{Entry, VacantEntry};
use std::io::{Error as IoError, ErrorKind};
use std::sync::{Arc, RwLock};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use kvproto::encryptionpb::{DataKey, EncryptionMethod, FileDictionary, FileInfo, KeyDictionary};

use self::rockmock::*;
use crate::master_key::*;
use crate::{Error, Iv, Result};

#[derive(Default)]
struct Dicts {
    file_dict: FileDictionary,
    key_dict: KeyDictionary,
}

impl Dicts {
    fn open(path: &str) -> Result<Dicts> {
        // TODO open dicts at path.
        let file_dict = FileDictionary::default();
        let key_dict = KeyDictionary::default();

        Ok(Dicts {
            file_dict,
            key_dict,
        })
    }

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
                        "current data key not found! Number of keys {}",
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
        Ok(())
        // self.save_file_dict()
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

fn generate_data_key(method: EncryptionMethod) -> (u64, Vec<u8>) {
    use rand::{rngs::OsRng, Rng, RngCore};

    let key_id = OsRng.next_u64();
    let key = match method {
        EncryptionMethod::Plaintext => vec![],
        EncryptionMethod::Aes128Ctr => OsRng.gen::<[u8; 16]>().to_vec(),
        EncryptionMethod::Aes192Ctr => OsRng.gen::<[u8; 24]>().to_vec(),
        EncryptionMethod::Aes256Ctr => OsRng.gen::<[u8; 32]>().to_vec(),
        unknown => panic!("bad EncryptionMethod {:?}", unknown),
    };
    (key_id, key)
}

#[derive(Clone)]
pub struct DataKeyManager {
    master_key: Arc<dyn Backend>,
    dicts: Arc<RwLock<Dicts>>,
    method: EncryptionMethod,
    rotation_period: Duration,
}

impl DataKeyManager {
    pub fn new(
        master_key: Arc<dyn Backend>,
        method: EncryptionMethod,
        rotation_period: Duration,
        dict_path: &str,
    ) -> Result<DataKeyManager> {
        let dicts = Arc::new(RwLock::new(Dicts::open(dict_path)?));
        let manager = DataKeyManager {
            dicts,
            master_key,
            method,
            rotation_period,
        };
        manager.maybe_rotate_data_key();
        Ok(manager)
    }

    fn maybe_rotate_data_key(&self) {
        let mut dicts = self.dicts.write().unwrap();

        let now = SystemTime::now();
        // 0 means it's a new key dict
        if dicts.key_dict.current_key_id != 0 {
            // It's not a new dict, then check current data key
            // cration time.
            let (_, key) = dicts.current_data_key();

            let creation_time = UNIX_EPOCH + Duration::from_secs(key.creation_time);
            match now.duration_since(creation_time) {
                Ok(duration) => {
                    if self.rotation_period > duration {
                        debug!("current data key creation time is within rotation period";
                            "now" => ?now, "creation_time" => ?creation_time);
                        return;
                    }
                }
                Err(e) => {
                    warn!("data key rotate duraion overflow, generate a new data key";
                        "now" => ?now, "creation_time" => ?creation_time, "error" => ?e);
                }
            }
        }

        let duration = now.duration_since(UNIX_EPOCH).unwrap();
        let creation_time = duration.as_secs();

        // Generate new data key.
        let generate_limit = 5;
        for _ in 0..generate_limit {
            let (key_id, key) = generate_data_key(self.method);
            let mut data_key = DataKey::default();
            data_key.key = key;
            data_key.method = self.method;
            data_key.creation_time = creation_time;
            data_key.was_exposed = false;
            if dicts.key_dict.keys.insert(key_id, data_key).is_none() {
                // Update current data key id.
                dicts.key_dict.current_key_id = key_id;
                // TODO re-encrypt key dict file.
                break;
            }
            // key id collides, retry
        }
    }
}

impl KeyManager for DataKeyManager {
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

#[cfg(test)]
mod tests {
    use super::*;

    use std::thread::sleep;

    #[test]
    fn test_key_manager_create_get_delete() {
        let mut manager = DataKeyManager {
            master_key: Arc::new(PlainTextBackend::default()),
            dicts: Arc::default(),
            method: EncryptionMethod::Plaintext,
            rotation_period: Duration::from_secs(60), // 1 min.
        };
        manager.maybe_rotate_data_key();

        let new_file = manager.new_file("foo").unwrap();
        let get_file = manager.get_file("foo").unwrap();
        assert_eq!(new_file, get_file);
        manager.delete_file("foo").unwrap();
        manager.get_file("foo").unwrap_err();
        manager.delete_file("foo1").unwrap_err();
    }

    #[test]
    fn test_key_manager_rotate() {
        let mut manager = DataKeyManager {
            master_key: Arc::new(PlainTextBackend::default()),
            dicts: Arc::default(),
            method: EncryptionMethod::Plaintext,
            rotation_period: Duration::from_secs(60), // 1 min.
        };
        manager.maybe_rotate_data_key();
        let (key_id, key) = {
            let dicts = manager.dicts.read().unwrap();
            let (id, k) = dicts.current_data_key();
            (id, k.clone())
        };

        // Do not rotate.
        manager.maybe_rotate_data_key();
        let (current_key_id1, current_key1) = {
            let dicts = manager.dicts.read().unwrap();
            let (id, k) = dicts.current_data_key();
            (id, k.clone())
        };
        assert_eq!(current_key_id1, key_id);
        assert_eq!(current_key1, key);

        // Change rotateion period to a smaller value, must roate.
        manager.rotation_period = Duration::from_millis(1);
        sleep(Duration::from_secs(1));
        manager.maybe_rotate_data_key();
        let (current_key_id2, current_key2) = {
            let dicts = manager.dicts.read().unwrap();
            let (id, k) = dicts.current_data_key();
            (id, k.clone())
        };
        assert_ne!(current_key_id2, key_id);
        assert_ne!(current_key2, key);
    }
}

// Will be moved into rust-rocksdb
mod rockmock {
    use crate::{Error, Result};
    use kvproto::encryptionpb::{DataKey, EncryptionMethod, FileDictionary, FileInfo};

    #[derive(Debug, PartialEq, Eq)]
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
