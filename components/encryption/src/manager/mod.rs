// Copyright 2020 TiKV Project Authors. Licensed under Apache-2.0.

use std::fs::File;
use std::io::{Error as IoError, ErrorKind, Result as IoResult};
use std::path::{Path, PathBuf};
use std::sync::{atomic::AtomicU64, atomic::Ordering, Arc, Mutex};
use std::thread::JoinHandle;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use crossbeam::channel::{self, select, tick};
use engine_traits::{EncryptionKeyManager, FileEncryptionInfo};
use kvproto::encryptionpb::{DataKey, EncryptionMethod, FileDictionary, FileInfo, KeyDictionary};
use protobuf::Message;

use crate::config::{EncryptionConfig, MasterKeyConfig};
use crate::crypter::{self, compat, Iv};
use crate::encrypted_file::EncryptedFile;
use crate::io::EncrypterWriter;
use crate::log_file::LogFile;
use crate::master_key::{create_backend, Backend};
use crate::metrics::*;
use crate::{Error, Result};

const KEY_DICT_NAME: &str = "key.dict";
const FILE_DICT_NAME: &str = "file.dict";
const ROTATE_CHECK_PERIOD: u64 = 600; // 10min

struct Dicts {
    // Maps data file paths to key id and metadata. This file is stored as plaintext.
    file_dict: Mutex<FileDictionary>,
    log_file: Mutex<LogFile>,
    // Maps data key id to data keys, together with metadata. Also stores the data
    // key id used to encrypt the encryption file dictionary. The content is encrypted
    // using master key.
    key_dict: Mutex<KeyDictionary>,
    // Thread-safe version of current_key_id. Only when writing back to key_dict,
    // write it back to `key_dict`. Reader should always use this atomic, instead of
    // key_dict.current_key_id, since the latter can reflect an update-in-progress key.
    current_key_id: AtomicU64,
    rotation_period: Duration,
    base: PathBuf,
}

impl Dicts {
    fn new(path: &str, rotation_period: Duration, file_rewrite_threshold: u64) -> Result<Dicts> {
        Ok(Dicts {
            file_dict: Mutex::new(FileDictionary::default()),
            log_file: Mutex::new(LogFile::new(
                Path::new(path).to_owned(),
                FILE_DICT_NAME,
                file_rewrite_threshold,
            )?),
            key_dict: Mutex::new(KeyDictionary {
                current_key_id: 0,
                ..Default::default()
            }),
            current_key_id: AtomicU64::new(0),
            rotation_period,
            base: Path::new(path).to_owned(),
        })
    }

    fn open(
        path: &str,
        rotation_period: Duration,
        master_key: &dyn Backend,
        file_rewrite_threshold: u64,
    ) -> Result<Option<Dicts>> {
        let base = Path::new(path);

        // File dict is saved in plaintext.
        let log_content = LogFile::open(base, FILE_DICT_NAME, file_rewrite_threshold, false);

        let key_file = EncryptedFile::new(base, KEY_DICT_NAME);
        let key_bytes = key_file.read(master_key);

        match (log_content, key_bytes) {
            // Both files are found.
            (Ok((log_file, file_dict)), Ok(key_bytes)) => {
                info!("encryption: found both of key dictionary and file dictionary.");
                let mut key_dict = KeyDictionary::default();
                key_dict.merge_from_bytes(&key_bytes)?;
                let current_key_id = AtomicU64::new(key_dict.current_key_id);

                ENCRYPTION_DATA_KEY_GAUGE.set(key_dict.keys.len() as _);
                ENCRYPTION_FILE_NUM_GAUGE.set(file_dict.files.len() as _);

                Ok(Some(Dicts {
                    file_dict: Mutex::new(file_dict),
                    log_file: Mutex::new(log_file),
                    key_dict: Mutex::new(key_dict),
                    current_key_id,
                    rotation_period,
                    base: base.to_owned(),
                }))
            }
            // If neither files are found, encryption was never enabled.
            (Err(Error::Io(file_err)), Err(Error::Io(key_err)))
                if file_err.kind() == ErrorKind::NotFound
                    && key_err.kind() == ErrorKind::NotFound =>
            {
                info!("encryption: none of key dictionary and file dictionary are found.");
                Ok(None)
            }
            // ...else, return either error.
            (log_file, key_bytes) => {
                if let Err(key_err) = key_bytes {
                    error!("encryption: failed to load key dictionary.");
                    Err(key_err)
                } else {
                    error!("encryption: failed to load file dictionary.");
                    Err(log_file.unwrap_err())
                }
            }
        }
    }

    fn save_key_dict(&self, master_key: &dyn Backend) -> Result<()> {
        let file = EncryptedFile::new(&self.base, KEY_DICT_NAME);
        let (keys_len, key_bytes) = {
            let mut key_dict = self.key_dict.lock().unwrap();
            if !master_key.is_secure() {
                for value in key_dict.keys.values_mut() {
                    value.was_exposed = true
                }
            }
            let keys_len = key_dict.keys.len() as _;
            let key_bytes = key_dict.write_to_bytes()?;
            (keys_len, key_bytes)
        };
        file.write(&key_bytes, master_key)?;

        ENCRYPTION_FILE_SIZE_GAUGE
            .with_label_values(&["key_dictionary"])
            .set(key_bytes.len() as _);
        ENCRYPTION_DATA_KEY_GAUGE.set(keys_len);

        Ok(())
    }

    fn current_data_key(&self) -> (u64, DataKey) {
        let key_dict = self.key_dict.lock().unwrap();
        let current_key_id = self.current_key_id.load(Ordering::SeqCst);
        (
            current_key_id,
            key_dict
                .keys
                .get(&current_key_id)
                .cloned()
                .unwrap_or_else(|| {
                    panic!(
                        "current data key not found! Number of keys {}",
                        key_dict.keys.len()
                    );
                }),
        )
    }

    fn get_file(&self, fname: &str) -> FileInfo {
        let dict = self.file_dict.lock().unwrap();
        let file_info = dict.files.get(fname);
        match file_info {
            None => {
                // Return Plaintext if file not found
                let mut file = FileInfo::default();
                file.method = compat(EncryptionMethod::Plaintext);
                file
            }
            Some(info) => info.clone(),
        }
    }

    fn new_file(&self, fname: &str, method: EncryptionMethod) -> Result<FileInfo> {
        let mut log_file = self.log_file.lock().unwrap();
        let iv = Iv::new_ctr();
        let mut file = FileInfo::default();
        file.iv = iv.as_slice().to_vec();
        file.key_id = self.current_key_id.load(Ordering::SeqCst);
        file.method = compat(method);
        let file_num = {
            let mut file_dict = self.file_dict.lock().unwrap();
            file_dict.files.insert(fname.to_owned(), file.clone());
            file_dict.files.len() as _
        };

        log_file.insert(fname, &file)?;
        ENCRYPTION_FILE_NUM_GAUGE.set(file_num);

        if method != EncryptionMethod::Plaintext {
            info!("new encrypted file"; 
                  "fname" => fname, 
                  "method" => format!("{:?}", method), 
                  "iv" => hex::encode(iv.as_slice()));
        } else {
            info!("new plaintext file"; "fname" => fname);
        }
        Ok(file)
    }

    fn delete_file(&self, fname: &str) -> Result<()> {
        let mut log_file = self.log_file.lock().unwrap();
        let (file, file_num) = {
            let mut file_dict = self.file_dict.lock().unwrap();

            match file_dict.files.remove(fname) {
                Some(file_info) => {
                    let file_num = file_dict.files.len() as _;
                    (file_info, file_num)
                }
                None => {
                    // Could be a plaintext file not tracked by file dictionary.
                    info!("delete untracked plaintext file"; "fname" => fname);
                    return Ok(());
                }
            }
        };

        log_file.remove(fname)?;
        ENCRYPTION_FILE_NUM_GAUGE.set(file_num);
        if file.method != compat(EncryptionMethod::Plaintext) {
            info!("delete encrypted file"; "fname" => fname);
        } else {
            info!("delete plaintext file"; "fname" => fname);
        }
        Ok(())
    }

    fn link_file(&self, src_fname: &str, dst_fname: &str) -> Result<()> {
        let mut log_file = self.log_file.lock().unwrap();
        let (method, file, file_num) = {
            let mut file_dict = self.file_dict.lock().unwrap();
            let file = match file_dict.files.get(src_fname) {
                Some(file_info) => file_info.clone(),
                None => {
                    // Could be a plaintext file not tracked by file dictionary.
                    info!("link untracked plaintext file"; "src" => src_fname, "dst" => dst_fname);
                    return Ok(());
                }
            };
            if file_dict.files.get(dst_fname).is_some() {
                return Err(Error::Io(IoError::new(
                    ErrorKind::AlreadyExists,
                    format!("file already exists, {}", dst_fname),
                )));
            }
            let method = file.method;
            file_dict.files.insert(dst_fname.to_owned(), file.clone());
            let file_num = file_dict.files.len() as _;
            (method, file, file_num)
        };
        log_file.insert(dst_fname, &file)?;
        ENCRYPTION_FILE_NUM_GAUGE.set(file_num);

        if method != compat(EncryptionMethod::Plaintext) {
            info!("link encrypted file"; "src" => src_fname, "dst" => dst_fname);
        } else {
            info!("link plaintext file"; "src" => src_fname, "dst" => dst_fname);
        }
        Ok(())
    }

    fn rename_file(&self, src_fname: &str, dst_fname: &str) -> Result<()> {
        let mut log_file = self.log_file.lock().unwrap();
        let (method, file, file_num) = {
            let mut file_dict = self.file_dict.lock().unwrap();
            let file = match file_dict.files.remove(src_fname) {
                Some(file_info) => file_info,
                None => {
                    // Could be a plaintext file not tracked by file dictionary.
                    info!("rename untracked plaintext file"; "src" => src_fname, "dst" => dst_fname);
                    return Ok(());
                }
            };
            let method = file.method;
            file_dict.files.insert(dst_fname.to_owned(), file.clone());
            let file_num = file_dict.files.len() as _;
            (method, file, file_num)
        };
        log_file.replace(src_fname, dst_fname, file)?;

        ENCRYPTION_FILE_NUM_GAUGE.set(file_num);

        if method != compat(EncryptionMethod::Plaintext) {
            info!("rename encrypted file"; "src" => src_fname, "dst" => dst_fname);
        } else {
            info!("rename plaintext file"; "src" => src_fname, "dst" => dst_fname);
        }
        Ok(())
    }

    fn rotate_key(&self, key_id: u64, key: DataKey, master_key: &dyn Backend) -> Result<()> {
        info!("encryption: rotate data key."; "key_id" => key_id);
        {
            let mut key_dict = self.key_dict.lock().unwrap();
            key_dict.keys.insert(key_id, key);
            key_dict.current_key_id = key_id;
        };

        // re-encrypt key dict file.
        self.save_key_dict(master_key)?;
        // Update current data key id.
        self.current_key_id.store(key_id, Ordering::SeqCst);
        Ok(())
    }

    fn maybe_rotate_data_key(
        &self,
        method: EncryptionMethod,
        master_key: &dyn Backend,
    ) -> Result<()> {
        let now = SystemTime::now();
        // 0 means it's a new key dict
        if self.current_key_id.load(Ordering::SeqCst) != 0 {
            // It's not a new dict, then check current data key
            // creation time.
            let (_, key) = self.current_data_key();
            // Generate a new data key if
            //   1. encryption method is not the same, or
            //   2. the current data key was exposed and the new master key is secure.
            if compat(method) == key.method && !(key.was_exposed && master_key.is_secure()) {
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

        let (key_id, key) = generate_data_key(method);
        let mut data_key = DataKey::default();
        data_key.key = key;
        data_key.method = compat(method);
        data_key.creation_time = creation_time;
        data_key.was_exposed = false;
        self.rotate_key(key_id, data_key, master_key)
    }
}

fn run_background_rotate_work(
    dict: Arc<Dicts>,
    method: EncryptionMethod,
    master_key: Arc<dyn Backend>,
    terminal_recv: channel::Receiver<()>,
) {
    let check_period = std::cmp::min(
        Duration::from_secs(ROTATE_CHECK_PERIOD),
        dict.rotation_period,
    );

    loop {
        select! {
            recv(tick(check_period)) -> _ => {
                info!("Try to rotate data key, current method:{:?}", method);
                dict.maybe_rotate_data_key(method, master_key.as_ref())
                    .expect("Rotating key operation encountered error in the background worker");
            },
            recv(terminal_recv) -> _ => {
                info!("Key rotate worker has been cancelled.");
                break
            },
        }
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

pub struct DataKeyManager {
    dicts: Arc<Dicts>,
    method: EncryptionMethod,
    rotate_terminal: channel::Sender<()>,
    background_worker: Option<JoinHandle<()>>,
}

impl DataKeyManager {
    pub fn from_config(
        config: &EncryptionConfig,
        dict_path: &str,
    ) -> Result<Option<DataKeyManager>> {
        Self::new(
            &config.master_key,
            &config.previous_master_key,
            config.data_encryption_method,
            config.data_key_rotation_period.into(),
            config.file_rewrite_threshold,
            dict_path,
        )
    }

    pub fn new(
        master_key_config: &MasterKeyConfig,
        previous_master_key_config: &MasterKeyConfig,
        method: EncryptionMethod,
        rotation_period: Duration,
        file_rewrite_threshold: u64,
        dict_path: &str,
    ) -> Result<Option<DataKeyManager>> {
        let master_key = create_backend(master_key_config).map_err(|e| {
            error!("failed to access master key, {}", e);
            e
        })?;
        if method != EncryptionMethod::Plaintext && !master_key.is_secure() {
            return Err(box_err!(
                "encryption is to enable but master key is either absent or insecure."
            ));
        }
        let dicts = match (
            Dicts::open(
                dict_path,
                rotation_period,
                master_key.as_ref(),
                file_rewrite_threshold,
            ),
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
                Dicts::new(dict_path, rotation_period, file_rewrite_threshold)?
            }
            // Encryption was enabled and master key didn't change.
            (Ok(Some(dicts)), _) => {
                info!("encryption is enabled. method = {:?}", method);
                dicts
            }
            // Failed to decrypt the dictionaries using master key. Could be master key being
            // rotated. Try the previous master key.
            (Err(Error::WrongMasterKey(e_current)), _) => {
                warn!(
                    "failed to open encryption metadata using master key. \
                      could be master key being rotated. \
                      current master key: {:?}, previous master key: {:?}",
                    master_key_config, previous_master_key_config
                );
                let previous_master_key = create_backend(previous_master_key_config)?;
                let dicts = Dicts::open(
                    dict_path,
                    rotation_period,
                    previous_master_key.as_ref(),
                    file_rewrite_threshold,
                )
                .map_err(|e| {
                    if let Error::WrongMasterKey(e_previous) = e {
                        Error::BothMasterKeyFail(e_current, e_previous)
                    } else {
                        e
                    }
                })?
                .ok_or_else(|| {
                    Error::Other(box_err!(
                        "Fallback to previous master key but find dictionaries to be empty."
                    ))
                })?;
                // Rewrite key_dict after replace master key.
                dicts.save_key_dict(master_key.as_ref())?;

                info!("encryption: persisted result after replace master key.");

                dicts
            }
            // Error.
            (Err(e), _) => return Err(e),
        };
        dicts.maybe_rotate_data_key(method, master_key.as_ref())?;

        let dicts = Arc::new(dicts);
        let dict_clone = dicts.clone();
        let (rotate_terminal, rx) = channel::bounded(1);
        let background_worker = std::thread::Builder::new()
            .name(thd_name!("enc:key"))
            .spawn(move || {
                run_background_rotate_work(dict_clone, method, master_key, rx);
            })?;

        ENCRYPTION_INITIALIZED_GAUGE.set(1);

        Ok(Some(DataKeyManager {
            dicts,
            method,
            rotate_terminal,
            background_worker: Some(background_worker),
        }))
    }

    pub fn create_file<P: AsRef<Path>>(&self, path: P) -> Result<EncrypterWriter<File>> {
        let fname = path.as_ref().to_str().ok_or_else(|| {
            Error::Other(box_err!(
                "failed to convert path to string {:?}",
                path.as_ref()
            ))
        })?;
        let file = self.new_file(fname)?;
        let file_writer = File::create(path)?;
        EncrypterWriter::new(
            file_writer,
            crypter::encryption_method_from_db_encryption_method(file.method),
            &file.key,
            Iv::from_slice(&file.iv)?,
        )
    }

    pub fn dump_key_dict(
        config: &EncryptionConfig,
        dict_path: &str,
        key_ids: Option<Vec<u64>>,
    ) -> Result<()> {
        let dict_file = EncryptedFile::new(Path::new(dict_path), KEY_DICT_NAME);
        // Here we don't trigger master key rotation and don't care about
        // config.previous_master_key.
        let backend = create_backend(&config.master_key)?;
        let dict_bytes = dict_file.read(backend.as_ref())?;
        let mut dict = KeyDictionary::default();
        dict.merge_from_bytes(&dict_bytes)?;
        if let Some(key_ids) = key_ids {
            for key_id in key_ids {
                if let Some(key) = dict.keys.get(&key_id) {
                    println!("{}: {:?}", key_id, key);
                }
            }
        } else {
            println!("current key id: {}", dict.current_key_id);
            for (key_id, key) in dict.keys.iter() {
                println!("{}: {:?}", key_id, key);
            }
        }
        Ok(())
    }

    pub fn dump_file_dict(dict_path: &str, file_path: Option<&str>) -> Result<()> {
        let (_, file_dict) = LogFile::open(dict_path, FILE_DICT_NAME, 1, true)?;
        if let Some(file_path) = file_path {
            if let Some(info) = file_dict.files.get(file_path) {
                println!("{}: {:?}", file_path, info);
            }
        } else {
            for (path, info) in file_dict.files.iter() {
                println!("{}: {:?}", path, info);
            }
        }
        Ok(())
    }
}

impl Drop for DataKeyManager {
    fn drop(&mut self) {
        self.rotate_terminal.send(()).unwrap();
        self.background_worker.take().unwrap().join().unwrap();
    }
}

impl EncryptionKeyManager for DataKeyManager {
    // Get key to open existing file.
    fn get_file(&self, fname: &str) -> IoResult<FileEncryptionInfo> {
        let (method, key_id, iv) = {
            let file = self.dicts.get_file(fname);
            (file.method, file.key_id, file.iv)
        };
        // Fail if key is specified but not found.
        let key = if method as i32 == EncryptionMethod::Plaintext as i32 {
            vec![]
        } else {
            match self.dicts.key_dict.lock().unwrap().keys.get(&key_id) {
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
        let (_, data_key) = self.dicts.current_data_key();
        let key = data_key.get_key().to_owned();
        let file = self.dicts.new_file(fname, self.method)?;
        let encrypted_file = FileEncryptionInfo {
            key,
            method: crypter::encryption_method_to_db_encryption_method(file.method),
            iv: file.get_iv().to_owned(),
        };
        Ok(encrypted_file)
    }

    fn delete_file(&self, fname: &str) -> IoResult<()> {
        self.dicts.delete_file(fname)?;
        Ok(())
    }

    fn link_file(&self, src_fname: &str, dst_fname: &str) -> IoResult<()> {
        self.dicts.link_file(src_fname, dst_fname)?;
        Ok(())
    }

    fn rename_file(&self, src_fname: &str, dst_fname: &str) -> IoResult<()> {
        self.dicts.rename_file(src_fname, dst_fname)?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::{FileConfig, Mock};
    use crate::master_key::tests::MockBackend;
    use crate::master_key::PlaintextBackend;

    use engine_traits::EncryptionMethod as DBEncryptionMethod;
    use matches::assert_matches;
    use std::{
        fs::{remove_file, File},
        io::Write,
        sync::{Arc, Mutex},
    };
    use tempfile::TempDir;

    // TODO(yiwu): use the similar method in test_util crate instead.
    fn new_tmp_key_manager(
        temp: Option<tempfile::TempDir>,
        method: Option<EncryptionMethod>,
        master_key: Option<MasterKeyConfig>,
        previous_master_key: Option<MasterKeyConfig>,
    ) -> (tempfile::TempDir, Result<Option<DataKeyManager>>) {
        let tmp = temp.unwrap_or_else(|| tempfile::TempDir::new().unwrap());
        let mock_config = MasterKeyConfig::Mock(Mock(Arc::new(Mutex::new(MockBackend::default()))));
        let master_key = master_key.unwrap_or_else(|| mock_config.clone());
        let previous_master_key = previous_master_key.unwrap_or(mock_config);
        let manager = DataKeyManager::new(
            &master_key,
            &previous_master_key,
            method.unwrap_or(EncryptionMethod::Aes256Ctr),
            Duration::from_secs(60),
            2,
            tmp.path().as_os_str().to_str().unwrap(),
        );
        (tmp, manager)
    }

    // TODO(yiwu): use the similar method in test_util crate instead.
    fn create_key_file(name: &str) -> (PathBuf, TempDir) {
        let tmp_dir = TempDir::new().unwrap();
        let path = tmp_dir.path().join(name);
        let mut file = File::create(path.clone()).unwrap();
        file.write_all(b"603deb1015ca71be2b73aef0857d77811f352c073b6108d72d9810a30914dff4\n")
            .unwrap();
        (path, tmp_dir)
    }

    #[test]
    fn test_key_manager_encryption_enable_disable() {
        // encryption not enabled.
        let (tmp, manager) =
            new_tmp_key_manager(None, Some(EncryptionMethod::Plaintext), None, None);
        assert!(manager.unwrap().is_none());

        // encryption being enabled.
        let (tmp, manager) =
            new_tmp_key_manager(Some(tmp), Some(EncryptionMethod::Aes256Ctr), None, None);
        let manager = manager.unwrap().unwrap();
        let foo1 = manager.new_file("foo").unwrap();
        drop(manager);

        // encryption was enabled. reopen.
        let (tmp, manager) =
            new_tmp_key_manager(Some(tmp), Some(EncryptionMethod::Aes256Ctr), None, None);
        let manager = manager.unwrap().unwrap();
        let foo2 = manager.get_file("foo").unwrap();
        assert_eq!(foo1, foo2);
        drop(manager);

        // disable encryption.
        let (_tmp, manager) =
            new_tmp_key_manager(Some(tmp), Some(EncryptionMethod::Plaintext), None, None);
        let manager = manager.unwrap().unwrap();
        let foo3 = manager.get_file("foo").unwrap();
        assert_eq!(foo1, foo3);
        let bar = manager.new_file("bar").unwrap();
        assert_eq!(bar.method, DBEncryptionMethod::Plaintext);
    }

    // When enabling encryption, using insecure master key is not allowed.
    #[test]
    fn test_key_manager_disallow_plaintext_metadata() {
        let (_tmp, manager) = new_tmp_key_manager(
            None,
            Some(EncryptionMethod::Aes256Ctr),
            Some(MasterKeyConfig::Plaintext),
            None,
        );
        manager.err().unwrap();
    }

    // If master_key is the wrong key, fallback to previous_master_key.
    #[test]
    fn test_key_manager_rotate_master_key() {
        // create initial dictionaries.
        let (tmp, manager) = new_tmp_key_manager(None, None, None, None);
        let manager = manager.unwrap().unwrap();
        let info1 = manager.new_file("foo").unwrap();
        drop(manager);

        let current_key = Arc::new(Mutex::new(MockBackend {
            is_wrong_master_key: true,
            ..Default::default()
        }));
        let previous_key = Arc::new(Mutex::new(MockBackend::default()));
        let (_tmp, manager) = new_tmp_key_manager(
            Some(tmp),
            None,
            Some(MasterKeyConfig::Mock(Mock(current_key.clone()))),
            Some(MasterKeyConfig::Mock(Mock(previous_key.clone()))),
        );
        let manager = manager.unwrap().unwrap();
        let info2 = manager.get_file("foo").unwrap();
        assert_eq!(info1, info2);
        assert_eq!(1, current_key.lock().unwrap().encrypt_called);
        assert_eq!(1, current_key.lock().unwrap().decrypt_called);
        assert_eq!(1, previous_key.lock().unwrap().decrypt_called);
    }

    #[test]
    fn test_key_manager_rotate_master_key_rewrite_failure() {
        // create initial dictionaries.
        let (tmp, manager) = new_tmp_key_manager(None, None, None, None);
        let manager = manager.unwrap().unwrap();
        manager.new_file("foo").unwrap();
        drop(manager);

        let current_key = Arc::new(Mutex::new(MockBackend {
            is_wrong_master_key: true,
            encrypt_fail: true,
            ..Default::default()
        }));
        let previous_key = Arc::new(Mutex::new(MockBackend::default()));
        let (_tmp, manager) = new_tmp_key_manager(
            Some(tmp),
            None,
            Some(MasterKeyConfig::Mock(Mock(current_key.clone()))),
            Some(MasterKeyConfig::Mock(Mock(previous_key.clone()))),
        );
        assert!(manager.is_err());
        assert_eq!(1, current_key.lock().unwrap().encrypt_called);
        assert_eq!(1, current_key.lock().unwrap().decrypt_called);
        assert_eq!(1, previous_key.lock().unwrap().decrypt_called);
    }

    #[test]
    fn test_key_manager_both_master_key_fail() {
        // create initial dictionaries.
        let (tmp, manager) = new_tmp_key_manager(None, None, None, None);
        let manager = manager.unwrap().unwrap();
        manager.new_file("foo").unwrap();
        drop(manager);

        let master_key = Arc::new(Mutex::new(MockBackend {
            is_wrong_master_key: true,
            ..Default::default()
        }));
        let (_tmp, manager) = new_tmp_key_manager(
            Some(tmp),
            None,
            Some(MasterKeyConfig::Mock(Mock(master_key.clone()))),
            Some(MasterKeyConfig::Mock(Mock(master_key))),
        );
        assert_matches!(manager.err(), Some(Error::BothMasterKeyFail(_, _)));
    }

    #[test]
    fn test_key_manager_key_dict_missing() {
        // create initial dictionaries.
        let (tmp, manager) = new_tmp_key_manager(None, None, None, None);
        let manager = manager.unwrap().unwrap();
        manager.new_file("foo").unwrap();
        drop(manager);

        remove_file(tmp.path().join(KEY_DICT_NAME)).unwrap();
        let (_tmp, manager) = new_tmp_key_manager(Some(tmp), None, None, None);
        assert!(manager.is_err());
    }

    #[test]
    fn test_key_manager_file_dict_missing() {
        // create initial dictionaries.
        let (tmp, manager) = new_tmp_key_manager(None, None, None, None);
        let manager = manager.unwrap().unwrap();
        manager.new_file("foo").unwrap();
        drop(manager);

        remove_file(tmp.path().join(FILE_DICT_NAME)).unwrap();
        let (_tmp, manager) = new_tmp_key_manager(Some(tmp), None, None, None);
        assert!(manager.is_err());
    }

    #[test]
    fn test_key_manager_create_get_delete() {
        let (_tmp, manager) = new_tmp_key_manager(None, None, None, None);
        let mut manager = manager.unwrap().unwrap();

        let new_file = manager.new_file("foo").unwrap();
        let get_file = manager.get_file("foo").unwrap();
        assert_eq!(new_file, get_file);
        manager.delete_file("foo").unwrap();
        manager.delete_file("foo").unwrap();
        manager.delete_file("foo1").unwrap();

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
            .file_dict
            .lock()
            .unwrap()
            .files
            .insert("foo".to_owned(), file);
        manager.get_file("foo").unwrap_err();
    }

    #[test]
    fn test_key_manager_link() {
        let (_tmp, manager) = new_tmp_key_manager(None, None, None, None);
        let manager = manager.unwrap().unwrap();

        let file = manager.new_file("foo").unwrap();
        manager.link_file("foo", "foo1").unwrap();

        // Must be the same.
        let file1 = manager.get_file("foo1").unwrap();
        assert_eq!(file1, file);

        // Source file not exists.
        manager.link_file("not exists", "not exists1").unwrap();
        // Target file already exists.
        manager.new_file("foo2").unwrap();
        manager.link_file("foo2", "foo1").unwrap_err();
    }

    #[test]
    fn test_key_manager_rename() {
        let (_tmp, manager) = new_tmp_key_manager(None, None, None, None);
        let mut manager = manager.unwrap().unwrap();

        manager.method = EncryptionMethod::Aes192Ctr;
        let file = manager.new_file("foo").unwrap();
        manager.rename_file("foo", "foo1").unwrap();

        // Must be the same.
        let file1 = manager.get_file("foo1").unwrap();
        assert_eq!(file1, file);

        // foo must not exist (should be plaintext)
        manager.rename_file("foo", "foo2").unwrap();
        let file_foo = manager.get_file("foo").unwrap();
        assert_ne!(file_foo, file);
        assert_eq!(file_foo.method, DBEncryptionMethod::Plaintext);
        let file_foo2 = manager.get_file("foo2").unwrap();
        assert_ne!(file_foo2, file);
        assert_eq!(file_foo2.method, DBEncryptionMethod::Plaintext);
    }

    #[test]
    fn test_key_manager_rotate() {
        let (_tmp, manager) = new_tmp_key_manager(None, None, None, None);
        let manager = manager.unwrap().unwrap();
        let (key_id, key) = {
            let (id, k) = manager.dicts.current_data_key();
            (id, k)
        };

        // Do not rotate.
        let mock_config = MasterKeyConfig::Mock(Mock(Arc::new(Mutex::new(MockBackend::default()))));
        let master_key = create_backend(&mock_config).unwrap();
        manager
            .dicts
            .maybe_rotate_data_key(manager.method, master_key.as_ref())
            .unwrap();
        let (current_key_id1, current_key1) = {
            let (id, k) = manager.dicts.current_data_key();
            (id, k)
        };
        assert_eq!(current_key_id1, key_id);
        assert_eq!(current_key1, key);

        // Change rotateion period to a smaller value, must rotate.
        unsafe {
            let ptr: *mut Dicts = manager.dicts.as_ref() as *const Dicts as *mut Dicts;
            let mut dict = Box::from_raw(ptr);
            dict.rotation_period = Duration::from_millis(1);
            Box::leak(dict);
        }
        std::thread::sleep(Duration::from_secs(1));
        manager
            .dicts
            .maybe_rotate_data_key(manager.method, master_key.as_ref())
            .unwrap();
        let (current_key_id2, current_key2) = {
            let (id, k) = manager.dicts.current_data_key();
            (id, k)
        };
        assert_ne!(current_key_id2, key_id);
        assert_ne!(current_key2, key);
    }

    #[test]
    fn test_key_manager_persistence() {
        let (tmp, manager) = new_tmp_key_manager(None, None, None, None);
        let manager = manager.unwrap().unwrap();

        // Create a file and a datakey.
        manager.new_file("foo").unwrap();

        let files = manager.dicts.file_dict.lock().unwrap().clone();
        let keys = manager.dicts.key_dict.lock().unwrap().clone();

        // Close and re-open.
        drop(manager);
        let (_tmp, manager1) = new_tmp_key_manager(Some(tmp), None, None, None);
        let manager1 = manager1.unwrap().unwrap();

        let files1 = manager1.dicts.file_dict.lock().unwrap().clone();
        let keys1 = manager1.dicts.key_dict.lock().unwrap().clone();
        assert_eq!(files, files1);
        assert_eq!(keys, keys1);
    }

    #[test]
    fn test_key_manager_rotate_on_key_expose() {
        let (key_path, _tmp_key_dir) = create_key_file("key");
        let master_key = MasterKeyConfig::File {
            config: FileConfig {
                path: key_path.to_str().unwrap().to_owned(),
            },
        };
        let master_key_backend = create_backend(&master_key).unwrap();
        let (_tmp_data_dir, manager) = new_tmp_key_manager(None, None, Some(master_key), None);
        let manager = manager.unwrap().unwrap();
        let (key_id, key) = {
            let (id, k) = manager.dicts.current_data_key();
            (id, k)
        };

        // Do not rotate.
        manager
            .dicts
            .maybe_rotate_data_key(manager.method, master_key_backend.as_ref())
            .unwrap();
        let (current_key_id1, current_key1) = {
            let (id, k) = manager.dicts.current_data_key();
            (id, k)
        };
        assert_eq!(current_key_id1, key_id);
        assert_eq!(current_key1, key);

        // Expose the current data key and must rotate.
        manager
            .dicts
            .key_dict
            .lock()
            .unwrap()
            .keys
            .get_mut(&current_key_id1)
            .unwrap()
            .was_exposed = true;
        manager
            .dicts
            .maybe_rotate_data_key(manager.method, master_key_backend.as_ref())
            .unwrap();
        let (current_key_id2, current_key2) = {
            let (id, k) = manager.dicts.current_data_key();
            (id, k)
        };
        assert_ne!(current_key_id2, key_id);
        assert_ne!(current_key2, key);
    }

    #[test]
    fn test_expose_keys_on_insecure_backend() {
        let (key_path, _tmp_key_dir) = create_key_file("key");
        let master_key = MasterKeyConfig::File {
            config: FileConfig {
                path: key_path.to_str().unwrap().to_owned(),
            },
        };

        let master_key_backend = create_backend(&master_key).unwrap();
        let (_tmp_data_dir, manager) = new_tmp_key_manager(None, None, Some(master_key), None);
        let manager = manager.unwrap().unwrap();

        for i in 0..100 {
            manager
                .dicts
                .rotate_key(i, DataKey::default(), master_key_backend.as_ref())
                .unwrap();
        }
        for value in manager.dicts.key_dict.lock().unwrap().keys.values() {
            assert!(!value.was_exposed);
        }

        // Change it insecure backend and save dicts,
        // must set expose for all keys.
        let insecure = Arc::new(PlaintextBackend::default());
        manager
            .dicts
            .rotate_key(100, DataKey::default(), insecure.as_ref())
            .unwrap();

        let mut count = 0;
        for value in manager.dicts.key_dict.lock().unwrap().keys.values() {
            count += 1;
            assert!(value.was_exposed);
        }
        assert!(count >= 101);
    }
}
