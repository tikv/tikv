// Copyright 2020 TiKV Project Authors. Licensed under Apache-2.0.

use std::io::{Error as IoError, ErrorKind, Result as IoResult};
use std::path::{Path, PathBuf};
use std::sync::{atomic::AtomicU64, atomic::Ordering, Arc, Mutex};
use std::thread::JoinHandle;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use crossbeam::channel::{self, select, tick};
use engine_traits::{EncryptionKeyManager, FileEncryptionInfo};
use file_system::File;
use kvproto::encryptionpb::{DataKey, EncryptionMethod, FileDictionary, FileInfo, KeyDictionary};
use protobuf::Message;

use crate::config::EncryptionConfig;

use crate::crypter::{self, compat, Iv};
use crate::encrypted_file::EncryptedFile;
use crate::file_dict_file::FileDictionaryFile;
use crate::io::EncrypterWriter;
use crate::master_key::Backend;
use crate::metrics::*;
use crate::{Error, Result};

const KEY_DICT_NAME: &str = "key.dict";
const FILE_DICT_NAME: &str = "file.dict";
const ROTATE_CHECK_PERIOD: u64 = 600; // 10min

struct Dicts {
    // Maps data file paths to key id and metadata. This file is stored as plaintext.
    file_dict: Mutex<FileDictionary>,
    file_dict_file: Mutex<FileDictionaryFile>,
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
    fn new(
        path: &str,
        rotation_period: Duration,
        enable_file_dictionary_log: bool,
        file_dictionary_rewrite_threshold: u64,
    ) -> Result<Dicts> {
        Ok(Dicts {
            file_dict: Mutex::new(FileDictionary::default()),
            file_dict_file: Mutex::new(FileDictionaryFile::new(
                Path::new(path).to_owned(),
                FILE_DICT_NAME,
                enable_file_dictionary_log,
                file_dictionary_rewrite_threshold,
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
        enable_file_dictionary_log: bool,
        file_dictionary_rewrite_threshold: u64,
    ) -> Result<Option<Dicts>> {
        let base = Path::new(path);

        // File dict is saved in plaintext.
        let log_content = FileDictionaryFile::open(
            base,
            FILE_DICT_NAME,
            enable_file_dictionary_log,
            file_dictionary_rewrite_threshold,
            false,
        );

        let key_file = EncryptedFile::new(base, KEY_DICT_NAME);
        let key_bytes = key_file.read(master_key);

        match (log_content, key_bytes) {
            // Both files are found.
            (Ok((file_dict_file, file_dict)), Ok(key_bytes)) => {
                info!("encryption: found both of key dictionary and file dictionary.");
                let mut key_dict = KeyDictionary::default();
                key_dict.merge_from_bytes(&key_bytes)?;
                let current_key_id = AtomicU64::new(key_dict.current_key_id);

                ENCRYPTION_DATA_KEY_GAUGE.set(key_dict.keys.len() as _);
                ENCRYPTION_FILE_NUM_GAUGE.set(file_dict.files.len() as _);

                Ok(Some(Dicts {
                    file_dict: Mutex::new(file_dict),
                    file_dict_file: Mutex::new(file_dict_file),
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
            (Ok((file_dict_file, file_dict)), Err(Error::Io(key_err)))
                if key_err.kind() == ErrorKind::NotFound && file_dict.files.is_empty() =>
            {
                std::fs::remove_file(file_dict_file.file_path())?;
                info!("encryption: file dict is empty and none of key dictionary are found.");
                Ok(None)
            }
            // ...else, return either error.
            (file_dict_file, key_bytes) => {
                if let Err(key_err) = key_bytes {
                    error!("encryption: failed to load key dictionary.");
                    Err(key_err)
                } else {
                    error!("encryption: failed to load file dictionary.");
                    Err(file_dict_file.unwrap_err())
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

    fn get_file(&self, fname: &str) -> Option<FileInfo> {
        let dict = self.file_dict.lock().unwrap();
        dict.files.get(fname).cloned()
    }

    fn new_file(&self, fname: &str, method: EncryptionMethod) -> Result<FileInfo> {
        let mut file_dict_file = self.file_dict_file.lock().unwrap();
        let iv = Iv::new_ctr();
        let file = FileInfo {
            iv: iv.as_slice().to_vec(),
            key_id: self.current_key_id.load(Ordering::SeqCst),
            method: compat(method),
            ..Default::default()
        };
        let file_num = {
            let mut file_dict = self.file_dict.lock().unwrap();
            file_dict.files.insert(fname.to_owned(), file.clone());
            file_dict.files.len() as _
        };

        file_dict_file.insert(fname, &file)?;
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

    // If the file does not exist, return Ok(())
    // In either case the intent that the file not exist is achieved.
    fn delete_file(&self, fname: &str) -> Result<()> {
        let mut file_dict_file = self.file_dict_file.lock().unwrap();
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

        file_dict_file.remove(fname)?;
        ENCRYPTION_FILE_NUM_GAUGE.set(file_num);
        if file.method != compat(EncryptionMethod::Plaintext) {
            info!("delete encrypted file"; "fname" => fname);
        } else {
            info!("delete plaintext file"; "fname" => fname);
        }
        Ok(())
    }

    fn link_file(&self, src_fname: &str, dst_fname: &str) -> Result<Option<()>> {
        let mut file_dict_file = self.file_dict_file.lock().unwrap();
        let (method, file, file_num) = {
            let mut file_dict = self.file_dict.lock().unwrap();
            let file = match file_dict.files.get(src_fname) {
                Some(file_info) => file_info.clone(),
                None => {
                    // Could be a plaintext file not tracked by file dictionary.
                    info!("link untracked plaintext file"; "src" => src_fname, "dst" => dst_fname);
                    return Ok(None);
                }
            };
            // When an encrypted file exists in the file system, the file_dict must have info about
            // this file. But the opposite is not true, this is because the actual file operation
            // and file_dict operation are not atomic.
            check_stale_file_exist(dst_fname, &mut file_dict, &mut file_dict_file)?;
            let method = file.method;
            file_dict.files.insert(dst_fname.to_owned(), file.clone());
            let file_num = file_dict.files.len() as _;
            (method, file, file_num)
        };
        file_dict_file.insert(dst_fname, &file)?;
        ENCRYPTION_FILE_NUM_GAUGE.set(file_num);

        if method != compat(EncryptionMethod::Plaintext) {
            info!("link encrypted file"; "src" => src_fname, "dst" => dst_fname);
        } else {
            info!("link plaintext file"; "src" => src_fname, "dst" => dst_fname);
        }
        Ok(Some(()))
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
        let data_key = DataKey {
            key,
            method: compat(method),
            creation_time,
            was_exposed: false,
            ..Default::default()
        };
        self.rotate_key(key_id, data_key, master_key)
    }
}

fn check_stale_file_exist(
    fname: &str,
    file_dict: &mut FileDictionary,
    file_dict_file: &mut FileDictionaryFile,
) -> Result<()> {
    if file_dict.files.get(fname).is_some() {
        if Path::new(fname).exists() {
            return Err(Error::Io(IoError::new(
                ErrorKind::AlreadyExists,
                format!("file already exists, {}", fname),
            )));
        }
        info!(
            "Clean stale file information in file dictionary: {:?}",
            fname
        );
        file_dict_file.remove(fname)?;
        let _ = file_dict.files.remove(fname);
    }
    Ok(())
}

fn run_background_rotate_work(
    dict: Arc<Dicts>,
    method: EncryptionMethod,
    master_key: &dyn Backend,
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
                dict.maybe_rotate_data_key(method, master_key)
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

#[derive(Debug)]
pub struct DataKeyManagerArgs {
    pub method: EncryptionMethod,
    pub rotation_period: Duration,
    pub enable_file_dictionary_log: bool,
    pub file_dictionary_rewrite_threshold: u64,
    pub dict_path: String,
}

impl DataKeyManagerArgs {
    pub fn from_encryption_config(
        dict_path: &str,
        config: &EncryptionConfig,
    ) -> DataKeyManagerArgs {
        DataKeyManagerArgs {
            dict_path: dict_path.to_string(),
            method: config.data_encryption_method,
            rotation_period: config.data_key_rotation_period.into(),
            enable_file_dictionary_log: config.enable_file_dictionary_log,
            file_dictionary_rewrite_threshold: config.file_dictionary_rewrite_threshold,
        }
    }
}

#[allow(clippy::large_enum_variant)]
enum LoadDicts {
    EncryptionDisabled,
    WrongMasterKey(Box<dyn std::error::Error + Send + Sync + 'static>),
    Loaded(Dicts),
}

impl DataKeyManager {
    #[cfg(test)]
    fn new_previous_loaded(
        master_key: Box<dyn Backend>,
        previous_master_key: Box<dyn Backend>,
        args: DataKeyManagerArgs,
    ) -> Result<Option<DataKeyManager>> {
        Self::new(master_key, Box::new(move || Ok(previous_master_key)), args)
    }

    pub fn new(
        master_key: Box<dyn Backend>,
        previous_master_key: Box<dyn FnOnce() -> Result<Box<dyn Backend>>>,
        args: DataKeyManagerArgs,
    ) -> Result<Option<DataKeyManager>> {
        let dicts = match Self::load_dicts(&*master_key, &args)? {
            LoadDicts::Loaded(dicts) => dicts,
            LoadDicts::EncryptionDisabled => return Ok(None),
            LoadDicts::WrongMasterKey(err) => {
                Self::load_previous_dicts(&*master_key, &*(previous_master_key()?), &args, err)?
            }
        };
        Ok(Some(Self::from_dicts(dicts, args.method, master_key)?))
    }

    fn load_dicts(master_key: &dyn Backend, args: &DataKeyManagerArgs) -> Result<LoadDicts> {
        if args.method != EncryptionMethod::Plaintext && !master_key.is_secure() {
            return Err(box_err!(
                "encryption is to enable but master key is either absent or insecure."
            ));
        }
        match (
            Dicts::open(
                &args.dict_path,
                args.rotation_period,
                &*master_key,
                args.enable_file_dictionary_log,
                args.file_dictionary_rewrite_threshold,
            ),
            args.method,
        ) {
            // Encryption is disabled.
            (Ok(None), EncryptionMethod::Plaintext) => {
                info!("encryption is disabled.");
                Ok(LoadDicts::EncryptionDisabled)
            }
            // Encryption is being enabled.
            (Ok(None), _) => {
                info!("encryption is being enabled. method = {:?}", args.method);
                Ok(LoadDicts::Loaded(Dicts::new(
                    &args.dict_path,
                    args.rotation_period,
                    args.enable_file_dictionary_log,
                    args.file_dictionary_rewrite_threshold,
                )?))
            }
            // Encryption was enabled and master key didn't change.
            (Ok(Some(dicts)), _) => {
                info!("encryption is enabled. method = {:?}", args.method);
                Ok(LoadDicts::Loaded(dicts))
            }
            // Failed to decrypt the dictionaries using master key. Could be master key being
            // rotated. Try the previous master key.
            (Err(Error::WrongMasterKey(e_current)), _) => Ok(LoadDicts::WrongMasterKey(e_current)),
            // Error.
            (Err(e), _) => Err(e),
        }
    }

    fn load_previous_dicts(
        master_key: &dyn Backend,
        previous_master_key: &dyn Backend,
        args: &DataKeyManagerArgs,
        e_current: Box<dyn std::error::Error + Send + Sync + 'static>,
    ) -> Result<Dicts> {
        warn!(
            "failed to open encryption metadata using master key. \
                could be master key being rotated. \
                current master key: {:?}, previous master key: {:?}",
            master_key, previous_master_key
        );
        let dicts = Dicts::open(
            &args.dict_path,
            args.rotation_period,
            previous_master_key,
            args.enable_file_dictionary_log,
            args.file_dictionary_rewrite_threshold,
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
        dicts.save_key_dict(&*master_key)?;

        info!("encryption: persisted result after replace master key.");
        Ok(dicts)
    }

    fn from_dicts(
        dicts: Dicts,
        method: EncryptionMethod,
        master_key: Box<dyn Backend>,
    ) -> Result<DataKeyManager> {
        dicts.maybe_rotate_data_key(method, &*master_key)?;
        let dicts = Arc::new(dicts);
        let dict_clone = dicts.clone();
        let (rotate_terminal, rx) = channel::bounded(1);
        let background_worker = std::thread::Builder::new()
            .name(thd_name!("enc:key"))
            .spawn(move || {
                run_background_rotate_work(dict_clone, method, &*master_key, rx);
            })?;

        ENCRYPTION_INITIALIZED_GAUGE.set(1);

        Ok(DataKeyManager {
            dicts,
            method,
            rotate_terminal,
            background_worker: Some(background_worker),
        })
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
        backend: Box<dyn Backend>,
        dict_path: &str,
        key_ids: Option<Vec<u64>>,
    ) -> Result<()> {
        let dict_file = EncryptedFile::new(Path::new(dict_path), KEY_DICT_NAME);
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
        let (_, file_dict) = FileDictionaryFile::open(
            dict_path,
            FILE_DICT_NAME,
            true, /*enable_file_dictionary_log*/
            1,
            true, /*skip_rewrite*/
        )?;
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

    fn get_file_exists(&self, fname: &str) -> IoResult<Option<FileEncryptionInfo>> {
        let (method, key_id, iv) = {
            match self.dicts.get_file(fname) {
                Some(file) => (file.method, file.key_id, file.iv),
                None => return Ok(None),
            }
        };
        // Fail if key is specified but not found.
        let key = if method as i32 == EncryptionMethod::Plaintext as i32 {
            vec![]
        } else {
            match self.dicts.key_dict.lock().unwrap().keys.get(&key_id) {
                Some(k) => k.key.clone(),
                None => {
                    return Err(IoError::new(
                        ErrorKind::NotFound,
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
        Ok(Some(encrypted_file))
    }
}

impl Drop for DataKeyManager {
    fn drop(&mut self) {
        self.rotate_terminal
            .send(())
            .expect("DataKeyManager drop send");
        self.background_worker
            .take()
            .expect("DataKeyManager worker take")
            .join()
            .expect("DataKeyManager worker join");
    }
}

impl EncryptionKeyManager for DataKeyManager {
    // Get key to open existing file.
    fn get_file(&self, fname: &str) -> IoResult<FileEncryptionInfo> {
        match self.get_file_exists(fname) {
            Ok(Some(result)) => Ok(result),
            Ok(None) => {
                // Return Plaintext if file is not found
                // RocksDB requires this
                let file = FileInfo::default();
                let method = compat(EncryptionMethod::Plaintext);
                Ok(FileEncryptionInfo {
                    key: vec![],
                    method: crypter::encryption_method_to_db_encryption_method(method),
                    iv: file.iv,
                })
            }
            Err(err) => Err(err),
        }
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
        fail_point!("key_manager_fails_before_delete_file", |_| IoResult::Err(
            std::io::ErrorKind::Other.into()
        ));
        self.dicts.delete_file(fname)?;
        Ok(())
    }

    fn link_file(&self, src_fname: &str, dst_fname: &str) -> IoResult<()> {
        let _ = self.dicts.link_file(src_fname, dst_fname)?;
        // For now we ignore file not found.
        // TODO: propagate it back up as an Option.
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::master_key::tests::{decrypt_called, encrypt_called, MockBackend};
    use crate::master_key::{FileBackend, PlaintextBackend};

    use engine_traits::EncryptionMethod as DBEncryptionMethod;
    use file_system::{remove_file, File};
    use matches::assert_matches;
    use std::io::Write;
    use tempfile::TempDir;

    lazy_static::lazy_static! {
        static ref LOCK_FOR_GAUGE: Mutex<i32> = Mutex::new(1);
    }

    fn new_mock_backend() -> Box<MockBackend> {
        Box::new(MockBackend::default())
    }

    fn new_key_manager_def(
        tmp_dir: &tempfile::TempDir,
        method: Option<EncryptionMethod>,
    ) -> Result<DataKeyManager> {
        let master_backend = new_mock_backend() as Box<dyn Backend>;
        let mut args = def_data_key_args(tmp_dir);
        if let Some(method) = method {
            args.method = method;
        }
        match DataKeyManager::new_previous_loaded(
            master_backend,
            Box::new(MockBackend::default()),
            args,
        ) {
            Ok(None) => panic!("expected encryption"),
            Ok(Some(dkm)) => Ok(dkm),
            Err(e) => Err(e),
        }
    }

    fn def_data_key_args(tmp_dir: &tempfile::TempDir) -> DataKeyManagerArgs {
        DataKeyManagerArgs {
            method: EncryptionMethod::Aes256Ctr,
            rotation_period: Duration::from_secs(60),
            enable_file_dictionary_log: true,
            file_dictionary_rewrite_threshold: 2,
            dict_path: tmp_dir.path().as_os_str().to_str().unwrap().to_string(),
        }
    }

    // TODO(yiwu): use the similar method in test_util crate instead.
    fn new_mock_key_manager(
        tmp_dir: &tempfile::TempDir,
        method: Option<EncryptionMethod>,
        master_backend: Box<MockBackend>,
        previous_key: Box<MockBackend>,
    ) -> Result<DataKeyManager> {
        let mut args = def_data_key_args(tmp_dir);
        if let Some(method) = method {
            args.method = method;
        }
        match DataKeyManager::new_previous_loaded(master_backend, previous_key, args) {
            Ok(None) => panic!("expected encryption"),
            Ok(Some(dkm)) => Ok(dkm),
            Err(e) => Err(e),
        }
    }

    fn new_key_manager(
        tmp_dir: &tempfile::TempDir,
        method: Option<EncryptionMethod>,
        master_backend: Box<dyn Backend>,
        previous_key: Box<dyn Backend>,
    ) -> Result<DataKeyManager> {
        let mut args = def_data_key_args(tmp_dir);
        if let Some(method) = method {
            args.method = method;
        }
        match DataKeyManager::new_previous_loaded(master_backend, previous_key, args) {
            Ok(None) => panic!("expected encryption"),
            Ok(Some(dkm)) => Ok(dkm),
            Err(e) => Err(e),
        }
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
        let tmp_dir = tempfile::TempDir::new().unwrap();
        let mut args = def_data_key_args(&tmp_dir);
        args.method = EncryptionMethod::Plaintext;
        let dkm = DataKeyManager::new(
            new_mock_backend(),
            Box::new(|| Ok(new_mock_backend())),
            args,
        )
        .unwrap();
        assert!(dkm.is_none());

        // encryption being enabled.
        let manager = new_key_manager_def(&tmp_dir, Some(EncryptionMethod::Aes256Ctr)).unwrap();
        let foo1 = manager.new_file("foo").unwrap();
        drop(manager);

        // encryption was enabled. reopen.
        let manager = new_key_manager_def(&tmp_dir, Some(EncryptionMethod::Aes256Ctr)).unwrap();
        let foo2 = manager.get_file("foo").unwrap();
        assert_eq!(foo1, foo2);
        drop(manager);

        // disable encryption.
        let manager = new_key_manager_def(&tmp_dir, Some(EncryptionMethod::Plaintext)).unwrap();
        let foo3 = manager.get_file("foo").unwrap();
        assert_eq!(foo1, foo3);
        let bar = manager.new_file("bar").unwrap();
        assert_eq!(bar.method, DBEncryptionMethod::Plaintext);
    }

    // When enabling encryption, using insecure master key is not allowed.
    #[test]
    fn test_key_manager_disallow_plaintext_metadata() {
        let tmp_dir = tempfile::TempDir::new().unwrap();
        let manager = new_key_manager(
            &tmp_dir,
            Some(EncryptionMethod::Aes256Ctr),
            Box::new(PlaintextBackend::default()),
            new_mock_backend() as Box<dyn Backend>,
        );
        manager.err().unwrap();
    }

    // If master_key is the wrong key, fallback to previous_master_key.
    #[test]
    fn test_key_manager_rotate_master_key() {
        let mut _guard = LOCK_FOR_GAUGE.lock().unwrap();

        // create initial dictionaries.
        let tmp_dir = tempfile::TempDir::new().unwrap();
        let manager = new_key_manager_def(&tmp_dir, None).unwrap();
        let info1 = manager.new_file("foo").unwrap();
        drop(manager);

        let mut current_key = Box::new(MockBackend {
            is_wrong_master_key: true,
            ..Default::default()
        });
        current_key.track("current".to_string());
        let mut previous_key = new_mock_backend();
        previous_key.track("previous".to_string());
        assert_eq!(encrypt_called("current"), 0);
        assert_eq!(encrypt_called("previous"), 0);
        assert_eq!(decrypt_called("current"), 0);
        assert_eq!(decrypt_called("previous"), 0);
        let manager = new_mock_key_manager(&tmp_dir, None, current_key, previous_key).unwrap();
        let info2 = manager.get_file("foo").expect("get file foo");
        assert_eq!(info1, info2);
        assert_eq!(encrypt_called("current"), 1);
        assert_eq!(encrypt_called("previous"), 0);
        assert_eq!(decrypt_called("current"), 1);
        assert_eq!(decrypt_called("previous"), 1);
        assert_eq!(ENCRYPTION_INITIALIZED_GAUGE.get(), 1);
        assert_eq!(ENCRYPTION_FILE_NUM_GAUGE.get(), 1);
    }

    #[test]
    fn test_key_manager_rotate_master_key_rewrite_failure() {
        let _guard = LOCK_FOR_GAUGE.lock().unwrap();
        // create initial dictionaries.
        let tmp_dir = tempfile::TempDir::new().unwrap();
        let manager = new_key_manager_def(&tmp_dir, None).unwrap();
        manager.new_file("foo").unwrap();
        drop(manager);

        let mut current_key = Box::new(MockBackend {
            is_wrong_master_key: true,
            encrypt_fail: true,
            ..MockBackend::default()
        });
        current_key.track("current".to_string());
        let mut previous_key = new_mock_backend();
        previous_key.track("previous".to_string());
        let manager = new_mock_key_manager(&tmp_dir, None, current_key, previous_key);
        assert!(manager.is_err());
        assert_eq!(encrypt_called("current"), 1);
        assert_eq!(encrypt_called("previous"), 0);
        assert_eq!(decrypt_called("current"), 1);
        assert_eq!(decrypt_called("previous"), 1);
        assert_eq!(ENCRYPTION_INITIALIZED_GAUGE.get(), 1);
        assert_eq!(ENCRYPTION_FILE_NUM_GAUGE.get(), 1);
    }

    #[test]
    fn test_key_manager_both_master_key_fail() {
        // create initial dictionaries.
        let tmp_dir = tempfile::TempDir::new().unwrap();
        let manager = new_key_manager_def(&tmp_dir, None).unwrap();
        manager.new_file("foo").unwrap();
        drop(manager);

        let master_key = Box::new(MockBackend {
            is_wrong_master_key: true,
            ..Default::default()
        });
        let previous_master_key = Box::new(MockBackend {
            is_wrong_master_key: true,
            ..Default::default()
        });
        let manager = new_mock_key_manager(&tmp_dir, None, master_key, previous_master_key);
        assert_matches!(manager.err(), Some(Error::BothMasterKeyFail(_, _)));
    }

    #[test]
    fn test_key_manager_key_dict_missing() {
        // create initial dictionaries.
        let tmp_dir = tempfile::TempDir::new().unwrap();
        let manager = new_key_manager_def(&tmp_dir, None).unwrap();
        manager.new_file("foo").unwrap();
        drop(manager);

        remove_file(tmp_dir.path().join(KEY_DICT_NAME)).unwrap();
        let manager = new_key_manager_def(&tmp_dir, None);
        assert!(manager.is_err());
    }

    #[test]
    fn test_key_manager_file_dict_missing() {
        // create initial dictionaries.
        let tmp_dir = tempfile::TempDir::new().unwrap();
        let manager = new_key_manager_def(&tmp_dir, None).unwrap();
        manager.new_file("foo").unwrap();
        drop(manager);

        remove_file(tmp_dir.path().join(FILE_DICT_NAME)).unwrap();
        let manager = new_key_manager_def(&tmp_dir, None);
        assert!(manager.is_err());
    }

    #[test]
    fn test_key_manager_create_get_delete() {
        let tmp_dir = tempfile::TempDir::new().unwrap();
        let manager = new_key_manager_def(&tmp_dir, None).unwrap();

        let new_file = manager.new_file("foo").unwrap();
        let get_file = manager.get_file("foo").unwrap();
        assert_eq!(new_file, get_file);
        manager.delete_file("foo").unwrap();
        manager.delete_file("foo").unwrap();
        manager.delete_file("foo1").unwrap();

        // Must be plaintext if file not found.
        assert_eq!(manager.get_file_exists("foo").unwrap(), None,);

        let manager = new_key_manager_def(&tmp_dir, Some(EncryptionMethod::Aes192Ctr)).unwrap();
        assert_eq!(manager.get_file_exists("foo").unwrap(), None,);

        // Must fail if key is specified but not found.
        let file = FileInfo {
            method: EncryptionMethod::Aes192Ctr as _,
            key_id: 7, // Not exists
            ..Default::default()
        };
        manager
            .dicts
            .file_dict
            .lock()
            .unwrap()
            .files
            .insert("foo".to_owned(), file);
        assert_eq!(
            manager.get_file_exists("foo").unwrap_err().kind(),
            ErrorKind::NotFound,
        );
    }

    #[test]
    fn test_key_manager_link() {
        let tmp_dir = tempfile::TempDir::new().unwrap();
        let file_foo1 = tmp_dir.path().join("foo1");
        let _ = File::create(&file_foo1).unwrap();
        let foo1_path = file_foo1.as_path().to_str().unwrap();

        let manager = new_key_manager_def(&tmp_dir, None).unwrap();

        let file = manager.new_file("foo").unwrap();
        manager.link_file("foo", foo1_path).unwrap();

        // Must be the same.
        let file1 = manager.get_file(foo1_path).unwrap();
        assert_eq!(file1, file);

        manager.link_file("not exists", "not exists1").unwrap();
        // Target file already exists.
        manager.new_file("foo2").unwrap();
        // Here we create a temp file "foo1" to make the `link_file` return an error.
        assert_eq!(
            manager.link_file("foo2", foo1_path).unwrap_err().kind(),
            ErrorKind::AlreadyExists,
        )
    }

    #[test]
    fn test_key_manager_rename() {
        let tmp_dir = tempfile::TempDir::new().unwrap();
        let manager = new_key_manager_def(&tmp_dir, Some(EncryptionMethod::Aes192Ctr)).unwrap();
        let file = manager.new_file("foo").unwrap();

        manager.link_file("foo", "foo1").unwrap();
        manager.delete_file("foo").unwrap();

        // Must be the same.
        let file1 = manager.get_file("foo1").unwrap();
        assert_eq!(file1, file);

        manager.link_file("foo", "foo2").unwrap();
        manager.delete_file("foo").unwrap();

        assert_eq!(manager.get_file_exists("foo").unwrap(), None);
        assert_eq!(manager.get_file_exists("foo2").unwrap(), None);
    }

    #[test]
    fn test_key_manager_rotate() {
        let tmp_dir = tempfile::TempDir::new().unwrap();
        let manager = new_key_manager_def(&tmp_dir, None).unwrap();
        let (key_id, key) = {
            let (id, k) = manager.dicts.current_data_key();
            (id, k)
        };

        // Do not rotate.
        let master_key = MockBackend::default();
        manager
            .dicts
            .maybe_rotate_data_key(manager.method, &master_key)
            .unwrap();
        let (current_key_id1, current_key1) = {
            let (id, k) = manager.dicts.current_data_key();
            (id, k)
        };
        assert_eq!(current_key_id1, key_id);
        assert_eq!(current_key1, key);

        // Change rotation period to a smaller value, must rotate.
        unsafe {
            let ptr: *mut Dicts = manager.dicts.as_ref() as *const Dicts as *mut Dicts;
            let mut dict = Box::from_raw(ptr);
            dict.rotation_period = Duration::from_millis(1);
            Box::leak(dict);
        }
        std::thread::sleep(Duration::from_secs(1));
        manager
            .dicts
            .maybe_rotate_data_key(manager.method, &master_key)
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
        let tmp_dir = tempfile::TempDir::new().unwrap();
        let manager = new_key_manager_def(&tmp_dir, None).unwrap();

        // Create a file and a datakey.
        manager.new_file("foo").unwrap();

        let files = manager.dicts.file_dict.lock().unwrap().clone();
        let keys = manager.dicts.key_dict.lock().unwrap().clone();

        // Close and re-open.
        drop(manager);
        let manager1 = new_key_manager_def(&tmp_dir, None).unwrap();

        let files1 = manager1.dicts.file_dict.lock().unwrap().clone();
        let keys1 = manager1.dicts.key_dict.lock().unwrap().clone();
        assert_eq!(files, files1);
        assert_eq!(keys, keys1);
    }

    #[test]
    fn test_key_manager_rotate_on_key_expose() {
        let (key_path, _tmp_key_dir) = create_key_file("key");
        let master_key_backend =
            Box::new(FileBackend::new(key_path.as_path()).unwrap()) as Box<dyn Backend>;
        let tmp_dir = tempfile::TempDir::new().unwrap();
        let previous = new_mock_backend() as Box<dyn Backend>;
        let manager = new_key_manager(&tmp_dir, None, master_key_backend, previous).unwrap();
        let (key_id, key) = {
            let (id, k) = manager.dicts.current_data_key();
            (id, k)
        };

        let master_key_backend =
            Box::new(FileBackend::new(key_path.as_path()).unwrap()) as Box<dyn Backend>;
        // Do not rotate.
        manager
            .dicts
            .maybe_rotate_data_key(manager.method, &*master_key_backend)
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
            .maybe_rotate_data_key(manager.method, &*master_key_backend)
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
        let file_backend = FileBackend::new(key_path.as_path()).unwrap();
        let master_key_backend = Box::new(file_backend);
        let tmp_dir = tempfile::TempDir::new().unwrap();
        let previous = new_mock_backend() as Box<dyn Backend>;
        let manager = new_key_manager(&tmp_dir, None, master_key_backend, previous).unwrap();

        let file_backend = FileBackend::new(key_path.as_path()).unwrap();
        let master_key_backend = Box::new(file_backend);
        for i in 0..100 {
            manager
                .dicts
                .rotate_key(i, DataKey::default(), &*master_key_backend)
                .unwrap();
        }
        for value in manager.dicts.key_dict.lock().unwrap().keys.values() {
            assert!(!value.was_exposed);
        }

        // Change it insecure backend and save dicts,
        // must set expose for all keys.
        let insecure = PlaintextBackend::default();
        manager
            .dicts
            .rotate_key(100, DataKey::default(), &insecure)
            .unwrap();

        let mut count = 0;
        for value in manager.dicts.key_dict.lock().unwrap().keys.values() {
            count += 1;
            assert!(value.was_exposed);
        }
        assert!(count >= 101);
    }

    #[test]
    fn test_master_key_failure_and_succeed() {
        let _guard = LOCK_FOR_GAUGE.lock().unwrap();
        let tmp_dir = tempfile::TempDir::new().unwrap();

        let wrong_key = Box::new(MockBackend {
            is_wrong_master_key: true,
            encrypt_fail: true,
            ..MockBackend::default()
        });
        let right_key = Box::new(MockBackend {
            is_wrong_master_key: true,
            encrypt_fail: false,
            ..MockBackend::default()
        });
        let previous = Box::new(PlaintextBackend::default()) as Box<dyn Backend>;

        let result = new_key_manager(&tmp_dir, None, wrong_key, previous);
        // When the master key is invalid, the key manager left a empty file dict and return errors.
        assert!(result.is_err());
        let previous = Box::new(PlaintextBackend::default()) as Box<dyn Backend>;
        let result = new_key_manager(&tmp_dir, None, right_key, previous);
        assert!(result.is_ok());
    }
}
