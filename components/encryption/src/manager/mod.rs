// Copyright 2020 TiKV Project Authors. Licensed under Apache-2.0.

use std::collections::hash_map::Entry;
use std::io::{Error as IoError, ErrorKind, Result as IoResult};
use std::path::{Path, PathBuf};
use std::sync::{Arc, RwLock};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use engine_traits::{EncryptionKeyManager, FileEncryptionInfo};
use kvproto::encryptionpb::{DataKey, EncryptionMethod, FileDictionary, FileInfo, KeyDictionary};
use protobuf::Message;

use crate::config::MasterKeyConfig;
use crate::crypter::{self, Iv};
use crate::encrypted_file::EncryptedFile;
use crate::master_key::{Backend, PlaintextBackend};
use crate::{Error, Result};

const KEY_DICT_NAME: &str = "key.dict";
const FILE_DICT_NAME: &str = "file.dict";

struct Dicts {
    file_dict: FileDictionary,
    key_dict: KeyDictionary,
    rotation_period: Duration,
    base: PathBuf,
}

impl Dicts {
    fn new(path: &str, rotation_period: Duration) -> Dicts {
        Dicts {
            file_dict: FileDictionary::default(),
            key_dict: KeyDictionary {
                current_key_id: 0,
                ..Default::default()
            },
            rotation_period,
            base: Path::new(path).to_owned(),
        }
    }

    fn open(
        path: &str,
        rotation_period: Duration,
        master_key: &dyn Backend,
    ) -> Result<Option<Dicts>> {
        let base = Path::new(path);

        // File dict is saved in plaintext.
        let file_file = EncryptedFile::new(base, FILE_DICT_NAME);
        let plaintext = MasterKeyConfig::Plaintext.create_backend()?;
        let file_bytes = file_file.read(plaintext.as_ref())?;

        let key_file = EncryptedFile::new(base, KEY_DICT_NAME);
        let key_bytes = key_file.read(master_key)?;

        match (file_bytes, key_bytes) {
            (None, None) => Ok(None),
            (None, _) => Err(Error::Other(
                format!("file dictionary missing, path = {}", path).into(),
            )),
            (_, None) => Err(Error::Other(
                format!("key dictionary missing, path = {}", path).into(),
            )),
            (Some(file_bytes), Some(key_bytes)) => {
                let mut file_dict = FileDictionary::default();
                file_dict.merge_from_bytes(&file_bytes)?;
                let mut key_dict = KeyDictionary::default();
                key_dict.merge_from_bytes(&key_bytes)?;
                Ok(Some(Dicts {
                    file_dict,
                    key_dict,
                    rotation_period,
                    base: base.to_owned(),
                }))
            }
        }
    }

    fn save_key_dict(&mut self, master_key: &dyn Backend) -> Result<()> {
        let file = EncryptedFile::new(&self.base, KEY_DICT_NAME);
        if !master_key.is_secure() {
            for value in self.key_dict.keys.values_mut() {
                value.was_exposed = true
            }
        }
        let key_bytes = self.key_dict.write_to_bytes()?;
        file.write(&key_bytes, master_key)?;
        Ok(())
    }

    fn save_file_dict(&self) -> Result<()> {
        let file = EncryptedFile::new(&self.base, FILE_DICT_NAME);
        let file_bytes = self.file_dict.write_to_bytes()?;
        // File dict is saved in plaintext.
        file.write(&file_bytes, &PlaintextBackend::default())?;
        Ok(())
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

    fn get_file(&mut self, fname: &str) -> &FileInfo {
        if self.file_dict.files.get(fname).is_none() {
            // Return Plaintext if file not found
            let mut file = FileInfo::default();
            file.method = EncryptionMethod::Plaintext;
            self.file_dict.files.insert(fname.to_owned(), file);
        }
        self.file_dict.files.get(fname).unwrap()
    }

    fn new_file(&mut self, fname: &str, method: EncryptionMethod) -> Result<&FileInfo> {
        let mut file = FileInfo::default();
        file.iv = Iv::new().as_slice().to_vec();
        file.key_id = self.key_dict.current_key_id;
        file.method = method;
        self.file_dict.files.insert(fname.to_owned(), file);
        self.save_file_dict()?;
        Ok(self.file_dict.files.get(fname).unwrap())
    }

    fn delete_file(&mut self, fname: &str) -> Result<()> {
        self.file_dict.files.remove(fname).ok_or_else(|| {
            Error::Io(IoError::new(
                ErrorKind::NotFound,
                format!("file not found, {}", fname),
            ))
        })?;

        // TOOD GC unused data keys.
        self.save_file_dict()
    }

    fn link_file(&mut self, src_fname: &str, dst_fname: &str) -> Result<()> {
        let file = self
            .file_dict
            .files
            .get(src_fname)
            .cloned()
            .ok_or_else(|| {
                Error::Io(IoError::new(
                    ErrorKind::NotFound,
                    format!("file not found, {}", src_fname),
                ))
            })?;
        if self.file_dict.files.get(dst_fname).is_some() {
            return Err(Error::Io(IoError::new(
                ErrorKind::AlreadyExists,
                format!("file already exists, {}", dst_fname),
            )));
        }
        self.file_dict.files.insert(dst_fname.to_owned(), file);
        self.save_file_dict()
    }

    fn rename_file(&mut self, src_fname: &str, dst_fname: &str) -> Result<()> {
        let file = self.file_dict.files.remove(src_fname).ok_or_else(|| {
            Error::Io(IoError::new(
                ErrorKind::NotFound,
                format!("file not found, {}", src_fname),
            ))
        })?;
        self.file_dict.files.insert(dst_fname.to_owned(), file);
        self.save_file_dict()
    }

    fn rotate_key(&mut self, key_id: u64, key: DataKey, master_key: &dyn Backend) -> Result<bool> {
        match self.key_dict.keys.entry(key_id) {
            // key id collides
            Entry::Occupied(_) => return Ok(false),
            Entry::Vacant(e) => e.insert(key),
        };

        // Update current data key id.
        self.key_dict.current_key_id = key_id;
        // re-encrypt key dict file.
        self.save_key_dict(master_key).map(|()| true)
    }

    fn maybe_rotate_data_key(
        &mut self,
        method: EncryptionMethod,
        master_key: &dyn Backend,
    ) -> Result<()> {
        let now = SystemTime::now();
        // 0 means it's a new key dict
        if self.key_dict.current_key_id != 0 {
            // It's not a new dict, then check current data key
            // creation time.
            let (_, key) = self.current_data_key();

            // Generate a new data key if the current data key was
            // exposed and the master key backend is secure.
            if !(key.was_exposed && master_key.is_secure()) {
                let creation_time = UNIX_EPOCH + Duration::from_secs(key.creation_time);
                match now.duration_since(creation_time) {
                    Ok(duration) => {
                        if self.rotation_period > duration {
                            debug!("current data key creation time is within rotation period";
                                "now" => ?now, "creation_time" => ?creation_time);
                            return Ok(());
                        }
                    }
                    Err(e) => {
                        warn!("data key rotate duraion overflow, generate a new data key";
                            "now" => ?now, "creation_time" => ?creation_time, "error" => ?e);
                    }
                }
            }
        }

        let duration = now.duration_since(UNIX_EPOCH).unwrap();
        let creation_time = duration.as_secs();

        // Generate new data key.
        let generate_limit = 10;
        for _ in 0..generate_limit {
            let (key_id, key) = generate_data_key(method);
            if key_id == 0 {
                // 0 is invalid
                continue;
            }
            let mut data_key = DataKey::default();
            data_key.key = key;
            data_key.method = method;
            data_key.creation_time = creation_time;
            data_key.was_exposed = false;

            let ok = self.rotate_key(key_id, data_key, master_key)?;
            if !ok {
                // key id collides, retry
                continue;
            }
            return Ok(());
        }
        Err(Error::Other(
            format!("key id collides {} times!", generate_limit).into(),
        ))
    }
}

fn generate_data_key(method: EncryptionMethod) -> (u64, Vec<u8>) {
    use rand::{rngs::OsRng, RngCore};

    let key_id = OsRng.next_u64();
    let key_length = crypter::get_method_key_length(method);
    let mut key = vec![0; key_length];
    OsRng.fill_bytes(&mut key);
    (key_id, key)
}

#[derive(Clone)]
pub struct DataKeyManager {
    master_key: Arc<dyn Backend>,
    dicts: Arc<RwLock<Dicts>>,
    method: EncryptionMethod,
}

impl DataKeyManager {
    pub fn new(
        master_key_config: &MasterKeyConfig,
        previous_master_key_config: &MasterKeyConfig,
        method: EncryptionMethod,
        rotation_period: Duration,
        dict_path: &str,
    ) -> Result<Option<DataKeyManager>> {
        let master_key = master_key_config.create_backend()?;
        let mut dicts = match (
            Dicts::open(dict_path, rotation_period, master_key.as_ref()),
            method,
        ) {
            // Encryption is disabled.
            (Ok(None), EncryptionMethod::Plaintext) => {
                info!("encryption is disabled.");
                return Ok(None);
            }
            // Encryption is being enabled.
            (Ok(None), _) => {
                info!("encryption is being enabled. method = {:?}", method);
                Dicts::new(dict_path, rotation_period)
            }
            // Encryption was enabled and master key didn't change.
            (Ok(Some(dicts)), _) => {
                info!("encryption is enabled. method = {:?}", method);
                dicts
            }
            // Failed to decrypt the dictionaries using master key. Could be master key being
            // rotated. Try the previous master key.
            (Err(Error::MasterKey(e_current)), _) => {
                warn!(
                    "failed to open encryption metadata using master key. \
                      could be master key being rotated. \
                      current master key: {:?}, previous master key: {:?}",
                    master_key_config, previous_master_key_config
                );
                let previous_master_key = previous_master_key_config.create_backend()?;
                Dicts::open(dict_path, rotation_period, previous_master_key.as_ref())
                    .map_err(|e| {
                        if let Error::MasterKey(e_previous) = e {
                            Error::DictDecrypt(e_current, e_previous)
                        } else {
                            e
                        }
                    })?
                    .ok_or_else(|| {
                        Error::Other(
                            "Fallback to previous master key but find dictionaries to be empty."
                                .to_owned()
                                .into(),
                        )
                    })?
            }
            // Error.
            (Err(e), _) => return Err(e),
        };
        dicts.maybe_rotate_data_key(method, master_key.as_ref())?;
        Ok(Some(DataKeyManager {
            dicts: Arc::new(RwLock::new(dicts)),
            master_key,
            method,
        }))
    }
}

impl EncryptionKeyManager for DataKeyManager {
    // Get key to open existing file.
    fn get_file(&self, fname: &str) -> IoResult<FileEncryptionInfo> {
        let mut dicts = self.dicts.write().unwrap();
        let (method, key_id, iv) = {
            let file = dicts.get_file(fname);
            (file.method, file.key_id, file.iv.to_owned())
        };
        // Fail if key is specified but not found.
        let key = if method as i32 == EncryptionMethod::Plaintext as i32 {
            vec![]
        } else {
            match dicts.get_key(key_id) {
                Some(k) => k.key.clone(),
                None => {
                    return Err(IoError::new(
                        ErrorKind::Other,
                        format!("key not found for id {}", key_id),
                    ));
                }
            }
        };
        let encrypted_file = FileEncryptionInfo {
            key,
            method: crypter::encryption_method_to_db_encryption_method(method),
            iv,
        };
        Ok(encrypted_file)
    }

    fn new_file(&self, fname: &str) -> IoResult<FileEncryptionInfo> {
        let mut dicts = self.dicts.write().unwrap();
        // Rotate data key if necessary.
        dicts.maybe_rotate_data_key(self.method, self.master_key.as_ref())?;
        let (_, data_key) = dicts.current_data_key();
        let key = data_key.get_key().to_owned();
        let file = dicts.new_file(fname, self.method)?;
        let encrypted_file = FileEncryptionInfo {
            key,
            method: crypter::encryption_method_to_db_encryption_method(file.method),
            iv: file.get_iv().to_owned(),
        };
        Ok(encrypted_file)
    }

    fn delete_file(&self, fname: &str) -> IoResult<()> {
        self.dicts.write().unwrap().delete_file(fname)?;
        Ok(())
    }

    fn link_file(&self, src_fname: &str, dst_fname: &str) -> IoResult<()> {
        self.dicts
            .write()
            .unwrap()
            .link_file(src_fname, dst_fname)?;
        Ok(())
    }

    fn rename_file(&self, src_fname: &str, dst_fname: &str) -> IoResult<()> {
        self.dicts
            .write()
            .unwrap()
            .rename_file(src_fname, dst_fname)?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use engine_traits::EncryptionMethod as DBEncryptionMethod;
    use std::{fs::File, io::Write, thread::sleep};
    use tempfile::TempDir;

    fn new_tmp_key_manager(
        temp: Option<tempfile::TempDir>,
        master_key: Option<MasterKeyConfig>,
        previous_master_key: Option<MasterKeyConfig>,
    ) -> (tempfile::TempDir, DataKeyManager) {
        let tmp = temp.unwrap_or_else(|| tempfile::TempDir::new().unwrap());
        let master_key = master_key.unwrap_or(MasterKeyConfig::Mock);
        let previous_master_key = previous_master_key.unwrap_or(MasterKeyConfig::Mock);
        let manager = DataKeyManager::new(
            &master_key,
            &previous_master_key,
            EncryptionMethod::Aes256Ctr,
            Duration::from_secs(60),
            tmp.path().as_os_str().to_str().unwrap(),
        )
        .unwrap()
        .unwrap();
        (tmp, manager)
    }

    fn create_key_file(name: &str) -> (PathBuf, TempDir) {
        let tmp_dir = TempDir::new().unwrap();
        let path = tmp_dir.path().join(name);
        let mut file = File::create(path.clone()).unwrap();
        file.write(b"603deb1015ca71be2b73aef0857d77811f352c073b6108d72d9810a30914dff4\n")
            .unwrap();
        (path, tmp_dir)
    }

    #[test]
    fn test_key_manager_create_get_delete() {
        let (_tmp, mut manager) = new_tmp_key_manager(None, None, None);

        let new_file = manager.new_file("foo").unwrap();
        let get_file = manager.get_file("foo").unwrap();
        assert_eq!(new_file, get_file);
        manager.delete_file("foo").unwrap();
        manager.delete_file("foo").unwrap_err();
        manager.delete_file("foo1").unwrap_err();

        // Must be plaintext if file not found.
        let file = manager.get_file("foo").unwrap();
        assert_eq!(file.method, DBEncryptionMethod::Plaintext);

        manager.method = EncryptionMethod::Aes192Ctr;
        let file1 = manager.new_file("foo").unwrap();
        assert_ne!(file, file1);

        // Must fail if key is specified but not found.
        let mut file = FileInfo::default();
        file.method = EncryptionMethod::Aes192Ctr as _;
        file.key_id = 7; // Not exists.
        manager
            .dicts
            .write()
            .unwrap()
            .file_dict
            .files
            .insert("foo".to_owned(), file);
        manager.get_file("foo").unwrap_err();
    }

    #[test]
    fn test_key_manager_link() {
        let (_tmp, manager) = new_tmp_key_manager(None, None, None);

        let file = manager.new_file("foo").unwrap();
        manager.link_file("foo", "foo1").unwrap();

        // Must be the same.
        let file1 = manager.get_file("foo1").unwrap();
        assert_eq!(file1, file);

        // Source file not exists.
        manager.link_file("not exists", "not exists1").unwrap_err();
        // Target file already exists.
        manager.new_file("foo2").unwrap();
        manager.link_file("foo2", "foo1").unwrap_err();
    }

    #[test]
    fn test_key_manager_rename() {
        let (_tmp, mut manager) = new_tmp_key_manager(None, None, None);

        manager.method = EncryptionMethod::Aes192Ctr;
        let file = manager.new_file("foo").unwrap();
        manager.rename_file("foo", "foo1").unwrap();

        // Must be the same.
        let file1 = manager.get_file("foo1").unwrap();
        assert_eq!(file1, file);

        // foo must not exist (should be plaintext)
        manager.rename_file("foo", "foo2").unwrap_err();
        let file2 = manager.get_file("foo").unwrap();
        assert_ne!(file2, file);
        assert_eq!(file2.method, DBEncryptionMethod::Plaintext);
    }

    #[test]
    fn test_key_manager_rotate() {
        let (_tmp, manager) = new_tmp_key_manager(None, None, None);

        let (key_id, key) = {
            let dicts = manager.dicts.read().unwrap();
            let (id, k) = dicts.current_data_key();
            (id, k.clone())
        };

        // Do not rotate.
        manager
            .dicts
            .write()
            .unwrap()
            .maybe_rotate_data_key(manager.method, manager.master_key.as_ref())
            .unwrap();
        let (current_key_id1, current_key1) = {
            let dicts = manager.dicts.read().unwrap();
            let (id, k) = dicts.current_data_key();
            (id, k.clone())
        };
        assert_eq!(current_key_id1, key_id);
        assert_eq!(current_key1, key);

        // Change rotateion period to a smaller value, must rotate.
        manager.dicts.write().unwrap().rotation_period = Duration::from_millis(1);
        sleep(Duration::from_secs(1));
        manager
            .dicts
            .write()
            .unwrap()
            .maybe_rotate_data_key(manager.method, manager.master_key.as_ref())
            .unwrap();
        let (current_key_id2, current_key2) = {
            let dicts = manager.dicts.read().unwrap();
            let (id, k) = dicts.current_data_key();
            (id, k.clone())
        };
        assert_ne!(current_key_id2, key_id);
        assert_ne!(current_key2, key);

        // Sleep and must rotate when new a file.
        sleep(Duration::from_secs(1));
        manager.new_file("foo").unwrap();
        let (current_key_id3, current_key3) = {
            let dicts = manager.dicts.read().unwrap();
            let (id, k) = dicts.current_data_key();
            (id, k.clone())
        };
        assert_ne!(current_key_id3, current_key_id2);
        assert_ne!(current_key3, current_key2);
    }

    #[test]
    fn test_key_manager_persistence() {
        let (tmp, manager) = new_tmp_key_manager(None, None, None);

        // Create a file and a datakey.
        manager.new_file("foo").unwrap();

        let (files, keys) = {
            let dicts = manager.dicts.read().unwrap();
            (dicts.file_dict.clone(), dicts.key_dict.clone())
        };

        // Close and re-open.
        drop(manager);
        let (_tmp, manager1) = new_tmp_key_manager(Some(tmp), None, None);

        let dicts = manager1.dicts.read().unwrap();
        assert_eq!(files, dicts.file_dict);
        assert_eq!(keys, dicts.key_dict);
    }

    #[test]
    fn test_dcit_rotate_key_collides() {
        let (_tmp, manager) = new_tmp_key_manager(None, None, None);
        let mut dict = manager.dicts.write().unwrap();

        let ok = dict
            .rotate_key(1, DataKey::default(), manager.master_key.as_ref())
            .unwrap();
        assert!(ok);

        let ok = dict
            .rotate_key(1, DataKey::default(), manager.master_key.as_ref())
            .unwrap();
        assert!(!ok);
    }

    #[test]
    fn test_key_manager_rotate_on_key_expose() {
        let (key_path, _tmp_key_dir) = create_key_file("key");
        let master_key = MasterKeyConfig::File {
            method: EncryptionMethod::Aes256Ctr,
            path: key_path.to_str().unwrap().to_owned(),
        };
        let (_tmp_data_dir, manager) = new_tmp_key_manager(None, Some(master_key), None);
        let mut dicts = manager.dicts.write().unwrap();

        let (key_id, key) = {
            let (id, k) = dicts.current_data_key();
            (id, k.clone())
        };

        // Do not rotate.
        dicts
            .maybe_rotate_data_key(manager.method, manager.master_key.as_ref())
            .unwrap();
        let (current_key_id1, current_key1) = {
            let (id, k) = dicts.current_data_key();
            (id, k.clone())
        };
        assert_eq!(current_key_id1, key_id);
        assert_eq!(current_key1, key);

        // Expose the current data key and must rotate.
        dicts
            .key_dict
            .keys
            .get_mut(&current_key_id1)
            .unwrap()
            .was_exposed = true;
        dicts
            .maybe_rotate_data_key(manager.method, manager.master_key.as_ref())
            .unwrap();
        let (current_key_id2, current_key2) = {
            let (id, k) = dicts.current_data_key();
            (id, k.clone())
        };
        assert_ne!(current_key_id2, key_id);
        assert_ne!(current_key2, key);
    }

    #[test]
    fn test_expose_keys_on_insecure_backend() {
        let (key_path, _tmp_key_dir) = create_key_file("key");
        let master_key = MasterKeyConfig::File {
            method: EncryptionMethod::Aes256Ctr,
            path: key_path.to_str().unwrap().to_owned(),
        };

        let (_tmp_data_dir, manager) = new_tmp_key_manager(None, Some(master_key), None);
        let mut dicts = manager.dicts.write().unwrap();

        for i in 0..100 {
            let ok = dicts
                .rotate_key(i, DataKey::default(), manager.master_key.as_ref())
                .unwrap();
            assert!(ok);
        }
        for value in dicts.key_dict.keys.values() {
            assert!(!value.was_exposed);
        }

        // Change it insecure backend and save dicts,
        // must set expose for all keys.
        let insecure = Arc::new(PlaintextBackend::default());
        let ok = dicts
            .rotate_key(100, DataKey::default(), insecure.as_ref())
            .unwrap();
        assert!(ok);

        let mut count = 0;
        for value in dicts.key_dict.keys.values() {
            count += 1;
            assert!(value.was_exposed);
        }
        assert!(count >= 101);
    }
}
