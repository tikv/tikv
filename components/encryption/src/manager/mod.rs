// Copyright 2020 TiKV Project Authors. Licensed under Apache-2.0.

use std::{
    collections::hash_map::Entry,
    io::{self, Error as IoError, ErrorKind, Result as IoResult},
    path::{Path, PathBuf},
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc, Mutex,
    },
    thread::JoinHandle,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use crossbeam::channel::{self, select, tick};
use crypto::rand;
use fail::fail_point;
use file_system::File;
use kvproto::encryptionpb::{DataKey, EncryptionMethod, FileDictionary, FileInfo, KeyDictionary};
use protobuf::Message;
use tikv_util::{box_err, debug, error, info, sys::thread::StdThreadBuildWrapper, thd_name, warn};

use crate::{
    config::EncryptionConfig,
    crypter::{self, FileEncryptionInfo, Iv},
    encrypted_file::EncryptedFile,
    file_dict_file::FileDictionaryFile,
    io::{DecrypterReader, EncrypterWriter},
    master_key::Backend,
    metrics::*,
    Error, Result,
};

const KEY_DICT_NAME: &str = "key.dict";
const FILE_DICT_NAME: &str = "file.dict";
const ROTATE_CHECK_PERIOD: u64 = 600; // 10min
const GENERATE_DATA_KEY_LIMIT: usize = 10;

struct Dicts {
    // Maps data file paths to key id and metadata. This file is stored as plaintext.
    file_dict: Mutex<FileDictionary>,
    file_dict_file: Mutex<FileDictionaryFile>,
    // Maps data key id to data keys, together with metadata. Also stores the data
    // key id used to encrypt the encryption file dictionary. The content is encrypted
    // using master key.
    key_dict: Mutex<KeyDictionary>,
    // A lock used to protect key_dict rotation.
    key_dict_file_lock: Mutex<()>,
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
                Path::new(path),
                FILE_DICT_NAME,
                enable_file_dictionary_log,
                file_dictionary_rewrite_threshold,
            )?),
            key_dict: Mutex::new(KeyDictionary {
                current_key_id: 0,
                ..Default::default()
            }),
            key_dict_file_lock: Mutex::new(()),
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
                    key_dict_file_lock: Mutex::default(),
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
        // In reality we only call this function inside `run_background_rotate_work`.
        let _lk = self.key_dict_file_lock.try_lock().unwrap();
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

    fn new_file(&self, fname: &str, method: EncryptionMethod, sync: bool) -> Result<FileInfo> {
        let mut file_dict_file = self.file_dict_file.lock().unwrap();
        let iv = if method != EncryptionMethod::Plaintext {
            Iv::new_ctr()?
        } else {
            Iv::Empty
        };
        let file = FileInfo {
            iv: iv.as_slice().to_vec(),
            key_id: self.current_key_id.load(Ordering::SeqCst),
            method: method.into(),
            ..Default::default()
        };
        let file_num = {
            let mut file_dict = self.file_dict.lock().unwrap();
            file_dict.files.insert(fname.to_owned(), file.clone());
            file_dict.files.len() as _
        };

        file_dict_file.insert(fname, &file, sync)?;
        ENCRYPTION_FILE_NUM_GAUGE.set(file_num);

        if method != EncryptionMethod::Plaintext {
            debug!("new encrypted file";
                  "fname" => fname,
                  "method" => format!("{:?}", method),
                  "iv" => hex::encode(iv.as_slice()));
        } else {
            debug!("new plaintext file"; "fname" => fname);
        }
        Ok(file)
    }

    // If the file does not exist, return Ok(())
    // In either case the intent that the file not exist is achieved.
    fn delete_file(&self, fname: &str, sync: bool) -> Result<()> {
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

        file_dict_file.remove(fname, sync)?;
        ENCRYPTION_FILE_NUM_GAUGE.set(file_num);
        if file.get_method() != EncryptionMethod::Plaintext {
            debug!("delete encrypted file"; "fname" => fname);
        } else {
            debug!("delete plaintext file"; "fname" => fname);
        }
        Ok(())
    }

    fn link_file(&self, src_fname: &str, dst_fname: &str, sync: bool) -> Result<Option<()>> {
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
            // When an encrypted file exists in the file system, the file_dict must have
            // info about this file. But the opposite is not true, this is because the
            // actual file operation and file_dict operation are not atomic.
            check_stale_file_exist(dst_fname, &mut file_dict, &mut file_dict_file)?;
            let method = file.get_method();
            file_dict.files.insert(dst_fname.to_owned(), file.clone());
            let file_num = file_dict.files.len() as _;
            (method, file, file_num)
        };
        file_dict_file.insert(dst_fname, &file, sync)?;
        ENCRYPTION_FILE_NUM_GAUGE.set(file_num);

        if method != EncryptionMethod::Plaintext {
            info!("link encrypted file"; "src" => src_fname, "dst" => dst_fname);
        } else {
            info!("link plaintext file"; "src" => src_fname, "dst" => dst_fname);
        }
        Ok(Some(()))
    }

    fn rotate_key(&self, key_id: u64, key: DataKey, master_key: &dyn Backend) -> Result<bool> {
        info!("encryption: rotate data key."; "key_id" => key_id);
        {
            let mut key_dict = self.key_dict.lock().unwrap();
            match key_dict.keys.entry(key_id) {
                // key id collides
                Entry::Occupied(_) => return Ok(false),
                Entry::Vacant(e) => e.insert(key),
            };
            key_dict.current_key_id = key_id;
        };

        // re-encrypt key dict file.
        self.save_key_dict(master_key)?;
        // Update current data key id.
        self.current_key_id.store(key_id, Ordering::SeqCst);
        Ok(true)
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
            if method == key.get_method() && !(key.was_exposed && master_key.is_secure()) {
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
        for _ in 0..GENERATE_DATA_KEY_LIMIT {
            let Ok((key_id, key)) = generate_data_key(method) else {
                continue;
            };
            if key_id == 0 {
                // 0 is invalid
                continue;
            }
            let data_key = DataKey {
                key,
                method: method.into(),
                creation_time,
                was_exposed: false,
                ..Default::default()
            };

            let ok = self.rotate_key(key_id, data_key, master_key)?;
            if !ok {
                // key id collides, retry
                continue;
            }
            return Ok(());
        }
        Err(box_err!(
            "key id collides {} times!",
            GENERATE_DATA_KEY_LIMIT
        ))
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
        file_dict_file.remove(fname, true)?;
        let _ = file_dict.files.remove(fname);
    }
    Ok(())
}

enum RotateTask {
    Terminate,
    Save(std::sync::mpsc::Sender<()>),
}

fn run_background_rotate_work(
    dict: Arc<Dicts>,
    method: EncryptionMethod,
    master_key: &dyn Backend,
    rx: channel::Receiver<RotateTask>,
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
            recv(rx) -> r => {
                match r {
                    Err(_) | Ok(RotateTask::Terminate) => {
                        info!("Key rotate worker has been cancelled.");
                        return;
                    }
                    Ok(RotateTask::Save(tx)) => {
                        dict.save_key_dict(master_key).expect("Saving key dict encountered error in the background worker");
                        tx.send(()).unwrap();
                    }
                }
            },
        }
    }
}

pub(crate) fn generate_data_key(method: EncryptionMethod) -> Result<(u64, Vec<u8>)> {
    let key_id = rand::rand_u64()?;
    let key_length = crypter::get_method_key_length(method);
    let mut key = vec![0; key_length];
    rand::rand_bytes(&mut key)?;
    Ok((key_id, key))
}

pub struct DataKeyManager {
    dicts: Arc<Dicts>,
    method: EncryptionMethod,
    rotate_tx: channel::Sender<RotateTask>,
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

    /// Will block file operation for a considerable amount of time. Only used
    /// for debugging purpose.
    pub fn retain_encrypted_files(&self, f: impl Fn(&str) -> bool) {
        let mut dict = self.dicts.file_dict.lock().unwrap();
        let mut file_dict_file = self.dicts.file_dict_file.lock().unwrap();
        dict.files.retain(|fname, info| {
            if info.get_method() != EncryptionMethod::Plaintext {
                let retain = f(fname);
                if !retain {
                    file_dict_file.remove(fname, true).unwrap();
                }
                retain
            } else {
                false
            }
        });
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
                master_key,
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
        dicts.save_key_dict(master_key)?;

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
        let (rotate_tx, rx) = channel::bounded(1);
        let background_worker = std::thread::Builder::new()
            .name(thd_name!("enc:key"))
            .spawn_wrapper(move || {
                run_background_rotate_work(dict_clone, method, &*master_key, rx);
            })?;

        ENCRYPTION_INITIALIZED_GAUGE.set(1);

        Ok(DataKeyManager {
            dicts,
            method,
            rotate_tx,
            background_worker: Some(background_worker),
        })
    }

    pub fn create_file_for_write<P: AsRef<Path>>(&self, path: P) -> Result<EncrypterWriter<File>> {
        let file_writer = File::create(&path)?;
        self.open_file_with_writer(path, file_writer, true /* create */)
    }

    pub fn open_file_with_writer<P: AsRef<Path>, W>(
        &self,
        path: P,
        writer: W,
        create: bool,
    ) -> Result<EncrypterWriter<W>> {
        let fname = path.as_ref().to_str().ok_or_else(|| {
            Error::Other(box_err!(
                "failed to convert path to string {:?}",
                path.as_ref()
            ))
        })?;
        let file = if create {
            self.new_file(fname)?
        } else {
            self.get_file(fname)?
        };
        EncrypterWriter::new(
            writer,
            file.method,
            &file.key,
            if file.method == EncryptionMethod::Plaintext {
                debug_assert!(file.iv.is_empty());
                Iv::Empty
            } else {
                Iv::from_slice(&file.iv)?
            },
        )
    }

    pub fn open_file_for_read<P: AsRef<Path>>(&self, path: P) -> Result<DecrypterReader<File>> {
        let file_reader = File::open(&path)?;
        self.open_file_with_reader(path, file_reader)
    }

    pub fn open_file_with_reader<P: AsRef<Path>, R>(
        &self,
        path: P,
        reader: R,
    ) -> Result<DecrypterReader<R>> {
        let fname = path.as_ref().to_str().ok_or_else(|| {
            Error::Other(box_err!(
                "failed to convert path to string {:?}",
                path.as_ref()
            ))
        })?;
        let file = self.get_file(fname)?;
        DecrypterReader::new(
            reader,
            file.method,
            &file.key,
            if file.method == EncryptionMethod::Plaintext {
                debug_assert!(file.iv.is_empty());
                Iv::Empty
            } else {
                Iv::from_slice(&file.iv)?
            },
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
            true, // enable_file_dictionary_log
            1,
            true, // skip_rewrite
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
                Some(file) => (file.get_method(), file.key_id, file.iv),
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
        let encrypted_file = FileEncryptionInfo { key, method, iv };
        Ok(Some(encrypted_file))
    }

    /// Returns initial vector and data key.
    pub fn get_file_internal(&self, fname: &str) -> IoResult<Option<(Vec<u8>, DataKey)>> {
        let (key_id, iv) = {
            match self.dicts.get_file(fname) {
                Some(file) if file.get_method() != EncryptionMethod::Plaintext => {
                    (file.key_id, file.iv)
                }
                _ => return Ok(None),
            }
        };
        // Fail if key is specified but not found.
        let k = match self.dicts.key_dict.lock().unwrap().keys.get(&key_id) {
            Some(k) => k.clone(),
            None => {
                return Err(IoError::new(
                    ErrorKind::NotFound,
                    format!("key not found for id {}", key_id),
                ));
            }
        };
        Ok(Some((iv, k)))
    }

    /// Removes data keys under the directory `logical`. If `physical` is
    /// present, if means the `logical` directory is already physically renamed
    /// to `physical`.
    /// There're two uses of this function:
    ///
    /// (1) without `physical`: `remove_dir` is called before
    /// `fs::remove_dir_all`. User must guarantee that this directory won't be
    /// read again even if the removal fails or panics.
    ///
    /// (2) with `physical`: Use `fs::rename` to rename the directory to trash.
    /// Then `remove_dir` with `physical` set to the trash directory name.
    /// Finally remove the trash directory. This is the safest way to delete a
    /// directory.
    pub fn remove_dir(&self, logical: &Path, physical: Option<&Path>) -> IoResult<()> {
        let scan = physical.unwrap_or(logical);
        debug_assert!(scan.is_dir());
        if !scan.exists() {
            return Ok(());
        }
        let mut iter = walkdir::WalkDir::new(scan)
            .into_iter()
            .filter(|e| e.as_ref().map_or(true, |e| !e.path().is_dir()))
            .peekable();
        while let Some(e) = iter.next() {
            let e = e?;
            if e.path().is_symlink() {
                return Err(io::Error::new(
                    io::ErrorKind::Other,
                    format!("unexpected symbolic link: {}", e.path().display()),
                ));
            }
            let fname = e.path().to_str().unwrap();
            let sync = iter.peek().is_none();
            if let Some(p) = physical {
                let sub = fname
                    .strip_prefix(p.to_str().unwrap())
                    .unwrap()
                    .trim_start_matches('/');
                self.dicts
                    .delete_file(logical.join(sub).to_str().unwrap(), sync)?;
            } else {
                self.dicts.delete_file(fname, sync)?;
            }
        }
        Ok(())
    }

    /// Return which method this manager is using.
    pub fn encryption_method(&self) -> EncryptionMethod {
        self.method
    }

    /// For tests.
    pub fn file_count(&self) -> usize {
        self.dicts.file_dict.lock().unwrap().files.len()
    }

    fn shutdown_background_worker(&mut self) {
        if let Err(e) = self.rotate_tx.send(RotateTask::Terminate) {
            info!("failed to terminate background rotation, are we shutting down?"; "err" => %e);
        }
        if let Some(Err(e)) = self.background_worker.take().map(|w| w.join()) {
            info!("failed to join background rotation, are we shutting down?"; "err" => ?e);
        }
    }
}

impl Drop for DataKeyManager {
    fn drop(&mut self) {
        self.shutdown_background_worker();
    }
}

impl DataKeyManager {
    // Get key to open existing file.
    pub fn get_file(&self, fname: &str) -> IoResult<FileEncryptionInfo> {
        match self.get_file_exists(fname) {
            Ok(Some(result)) => Ok(result),
            Ok(None) => {
                // Return Plaintext if file is not found
                // RocksDB requires this
                let file = FileInfo::default();
                let method = EncryptionMethod::Plaintext;
                Ok(FileEncryptionInfo {
                    key: vec![],
                    method,
                    iv: file.iv,
                })
            }
            Err(err) => Err(err),
        }
    }

    pub fn new_file(&self, fname: &str) -> IoResult<FileEncryptionInfo> {
        let (_, data_key) = self.dicts.current_data_key();
        let key = data_key.get_key().to_owned();
        let file = self.dicts.new_file(fname, self.method, true)?;
        let encrypted_file = FileEncryptionInfo {
            key,
            method: file.get_method(),
            iv: file.get_iv().to_owned(),
        };
        Ok(encrypted_file)
    }

    // Can be used with both file and directory. See comments of `remove_dir` for
    // more details when using this with a directory.
    //
    // `physical_fname` is a hint when `fname` was renamed physically.
    // Depending on the implementation, providing false negative or false
    // positive value may result in leaking encryption keys.
    pub fn delete_file(&self, fname: &str, physical_fname: Option<&str>) -> IoResult<()> {
        fail_point!("key_manager_fails_before_delete_file", |_| IoResult::Err(
            io::ErrorKind::Other.into()
        ));
        if let Some(physical) = physical_fname {
            let physical_path = Path::new(physical);
            if physical_path.is_dir() {
                self.remove_dir(Path::new(fname), Some(physical_path))?;
                return Ok(());
            }
        } else {
            let path = Path::new(fname);
            if path.is_dir() {
                self.remove_dir(path, None)?;
                return Ok(());
            }
        }
        self.dicts.delete_file(fname, true)?;
        Ok(())
    }

    pub fn link_file(&self, src_fname: &str, dst_fname: &str) -> IoResult<()> {
        let src_path = Path::new(src_fname);
        let dst_path = Path::new(dst_fname);
        if src_path.is_dir() {
            let mut iter = walkdir::WalkDir::new(src_path)
                .into_iter()
                .filter(|e| e.as_ref().map_or(true, |e| !e.path().is_dir()))
                .peekable();
            while let Some(e) = iter.next() {
                let e = e?;
                if e.path().is_symlink() {
                    return Err(io::Error::new(
                        io::ErrorKind::Other,
                        format!("unexpected symbolic link: {}", e.path().display()),
                    ));
                }
                let sub_path = e.path().strip_prefix(src_path).unwrap();
                let src = e.path().to_str().unwrap();
                let dst_path = dst_path.join(sub_path);
                let dst = dst_path.to_str().unwrap();
                self.dicts.link_file(src, dst, iter.peek().is_none())?;
            }
        } else {
            self.dicts.link_file(src_fname, dst_fname, true)?;
        }
        Ok(())
    }
}

/// An RAII-style importer of data keys. It automatically creates data key that
/// doesn't exist locally. It synchronizes log file in batch. It automatically
/// reverts changes if caller aborts.
pub struct DataKeyImporter<'a> {
    start_time: SystemTime,
    manager: &'a DataKeyManager,
    // Added file names.
    file_additions: Vec<String>,
    // Added key ids.
    key_additions: Vec<u64>,
    committed: bool,
}

#[allow(dead_code)]
impl<'a> DataKeyImporter<'a> {
    const EXPECTED_TIME_WINDOW_SECS: u64 = 120;

    pub fn new(manager: &'a DataKeyManager) -> Self {
        Self {
            start_time: SystemTime::now(),
            manager,
            file_additions: Vec::new(),
            key_additions: Vec::new(),
            committed: false,
        }
    }

    pub fn add(&mut self, fname: &str, iv: Vec<u8>, mut new_key: DataKey) -> Result<()> {
        // Needed for time window check.
        new_key.creation_time = self
            .start_time
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs();
        let method = new_key.method;
        let mut key_id = None;
        {
            let mut key_dict = self.manager.dicts.key_dict.lock().unwrap();
            for (id, data_key) in &key_dict.keys {
                if data_key.key == new_key.key {
                    // If this key is created within the window, there's a risk it is created by
                    // another importer, and can be rollback-ed.
                    if new_key.creation_time.saturating_sub(data_key.creation_time)
                        > Self::EXPECTED_TIME_WINDOW_SECS
                    {
                        key_id = Some(*id);
                    }
                    break;
                }
            }
            if key_id.is_none() {
                for _ in 0..GENERATE_DATA_KEY_LIMIT {
                    // Match `generate_data_key`.
                    let id = rand::rand_u64()?;
                    if let Entry::Vacant(e) = key_dict.keys.entry(id) {
                        key_id = Some(id);
                        e.insert(new_key);
                        self.key_additions.push(id);
                        info!("generate new ID for imported key"; "id" => id, "fname" => fname);
                        break;
                    }
                }
                if key_id.is_none() {
                    return Err(box_err!(
                        "key id collides {} times!",
                        GENERATE_DATA_KEY_LIMIT
                    ));
                }
            }
        }

        let file = FileInfo {
            iv,
            key_id: key_id.unwrap(),
            method,
            ..Default::default()
        };
        let mut file_dict_file = self.manager.dicts.file_dict_file.lock().unwrap();
        let file_num = {
            let mut file_dict = self.manager.dicts.file_dict.lock().unwrap();
            if let Entry::Vacant(e) = file_dict.files.entry(fname.to_owned()) {
                e.insert(file.clone());
            } else {
                // check for physical file.
                if Path::new(fname).exists() {
                    return Err(box_err!("file name collides with existing file: {}", fname));
                } else {
                    warn!("overwriting existing unused encryption key"; "fname" => fname);
                }
            }
            file_dict.files.len() as _
        };
        file_dict_file.insert(fname, &file, false)?;
        self.file_additions.push(fname.to_owned());
        ENCRYPTION_FILE_NUM_GAUGE.set(file_num);
        Ok(())
    }

    pub fn commit(mut self) -> Result<()> {
        if !self.key_additions.is_empty() {
            let (tx, rx) = std::sync::mpsc::channel();
            self.manager
                .rotate_tx
                .send(RotateTask::Save(tx))
                .map_err(|_| {
                    Error::Other(box_err!("Failed to request background key dict rotation"))
                })?;
            rx.recv().map_err(|_| {
                Error::Other(box_err!("Failed to wait for background key dict rotation"))
            })?;
        }
        if !self.file_additions.is_empty() {
            self.manager.dicts.file_dict_file.lock().unwrap().sync()?;
        }
        self.committed = true;
        Ok(())
    }

    pub fn rollback(&mut self) -> Result<()> {
        if let Some(fname) = self.file_additions.first() {
            info!("rollback imported file encryption info"; "sample_fname" => fname);
        }
        assert!(!self.committed);
        let mut iter = self.file_additions.drain(..).peekable();
        while let Some(f) = iter.next() {
            self.manager.dicts.delete_file(&f, iter.peek().is_none())?;
        }
        // If the duration is longer than the window, we cannot delete keys because they
        // may already be referenced by other files.
        // System time can drift, use 1s as safety padding.
        if !self.key_additions.is_empty()
            && let Ok(duration) = self.start_time.elapsed()
            && duration.as_secs() < Self::EXPECTED_TIME_WINDOW_SECS - 1
        {
            for key_id in self.key_additions.drain(..) {
                let mut key_dict = self.manager.dicts.key_dict.lock().unwrap();
                info!("rollback one imported data key"; "key_id" => key_id);
                key_dict.keys.remove(&key_id);
            }
            let (tx, rx) = std::sync::mpsc::channel();
            self.manager
                .rotate_tx
                .send(RotateTask::Save(tx))
                .map_err(|_| {
                    Error::Other(box_err!("Failed to request background key dict rotation"))
                })?;
            rx.recv().map_err(|_| {
                Error::Other(box_err!("Failed to wait for background key dict rotation"))
            })?;
        }
        Ok(())
    }
}

impl<'a> Drop for DataKeyImporter<'a> {
    fn drop(&mut self) {
        if !self.committed {
            if let Err(e) = self.rollback() {
                warn!("failed to rollback imported data keys"; "err" => ?e);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use file_system::{remove_file, File};
    use kvproto::encryptionpb::EncryptionMethod;
    use matches::assert_matches;
    use tempfile::TempDir;
    use test_util::create_test_key_file;

    use super::*;
    use crate::master_key::{
        tests::{decrypt_called, encrypt_called, MockBackend},
        FileBackend, PlaintextBackend,
    };

    lazy_static::lazy_static! {
        static ref LOCK_FOR_GAUGE: Mutex<()> = Mutex::new(());
    }

    fn new_mock_backend() -> Box<MockBackend> {
        Box::<MockBackend>::default()
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
            Box::<MockBackend>::default(),
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
            dict_path: tmp_dir.path().to_str().unwrap().to_string(),
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

    fn create_key_file(name: &str) -> (PathBuf, TempDir) {
        let tmp_dir = TempDir::new().unwrap();
        let path = tmp_dir.path().join(name);
        create_test_key_file(path.to_str().unwrap());
        (path, tmp_dir)
    }

    #[test]
    fn test_key_manager_encryption_enable_disable() {
        let _guard = LOCK_FOR_GAUGE.lock().unwrap();
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
        assert_eq!(bar.method, EncryptionMethod::Plaintext);
    }

    // When enabling encryption, using insecure master key is not allowed.
    #[test]
    fn test_key_manager_disallow_plaintext_metadata() {
        let _guard = LOCK_FOR_GAUGE.lock().unwrap();
        let tmp_dir = tempfile::TempDir::new().unwrap();
        let manager = new_key_manager(
            &tmp_dir,
            Some(EncryptionMethod::Aes256Ctr),
            Box::<PlaintextBackend>::default(),
            new_mock_backend() as Box<dyn Backend>,
        );
        manager.err().unwrap();
    }

    // If master_key is the wrong key, fallback to previous_master_key.
    #[test]
    fn test_key_manager_rotate_master_key() {
        let _guard = LOCK_FOR_GAUGE.lock().unwrap();

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
        let _guard = LOCK_FOR_GAUGE.lock().unwrap();
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
        let _guard = LOCK_FOR_GAUGE.lock().unwrap();
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
        let _guard = LOCK_FOR_GAUGE.lock().unwrap();
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
        let _guard = LOCK_FOR_GAUGE.lock().unwrap();
        let tmp_dir = tempfile::TempDir::new().unwrap();
        let manager = new_key_manager_def(&tmp_dir, None).unwrap();

        let new_file = manager.new_file("foo").unwrap();
        let get_file = manager.get_file("foo").unwrap();
        assert_eq!(new_file, get_file);
        manager.delete_file("foo", None).unwrap();
        manager.delete_file("foo", None).unwrap();
        manager.delete_file("foo1", None).unwrap();

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
        let _guard = LOCK_FOR_GAUGE.lock().unwrap();
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
        let _guard = LOCK_FOR_GAUGE.lock().unwrap();
        let tmp_dir = tempfile::TempDir::new().unwrap();
        let manager = new_key_manager_def(&tmp_dir, Some(EncryptionMethod::Aes192Ctr)).unwrap();
        let file = manager.new_file("foo").unwrap();

        manager.link_file("foo", "foo1").unwrap();
        manager.delete_file("foo", None).unwrap();

        // Must be the same.
        let file1 = manager.get_file("foo1").unwrap();
        assert_eq!(file1, file);

        manager.link_file("foo", "foo2").unwrap();
        manager.delete_file("foo", None).unwrap();

        assert_eq!(manager.get_file_exists("foo").unwrap(), None);
        assert_eq!(manager.get_file_exists("foo2").unwrap(), None);
    }

    #[test]
    fn test_key_manager_rotate() {
        let _guard = LOCK_FOR_GAUGE.lock().unwrap();
        let tmp_dir = tempfile::TempDir::new().unwrap();
        let mut manager = new_key_manager_def(&tmp_dir, None).unwrap();
        let (key_id, key) = {
            let (id, k) = manager.dicts.current_data_key();
            (id, k)
        };
        manager.shutdown_background_worker();

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
        let _guard = LOCK_FOR_GAUGE.lock().unwrap();
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
        let _guard = LOCK_FOR_GAUGE.lock().unwrap();
        let (key_path, _tmp_key_dir) = create_key_file("key");
        let master_key_backend =
            Box::new(FileBackend::new(key_path.as_path()).unwrap()) as Box<dyn Backend>;
        let tmp_dir = tempfile::TempDir::new().unwrap();
        let previous = new_mock_backend() as Box<dyn Backend>;
        let mut manager = new_key_manager(&tmp_dir, None, master_key_backend, previous).unwrap();
        let (key_id, key) = {
            let (id, k) = manager.dicts.current_data_key();
            (id, k)
        };
        manager.shutdown_background_worker();

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
        let _guard = LOCK_FOR_GAUGE.lock().unwrap();
        let (key_path, _tmp_key_dir) = create_key_file("key");
        let file_backend = FileBackend::new(key_path.as_path()).unwrap();
        let master_key_backend = Box::new(file_backend);
        let tmp_dir = tempfile::TempDir::new().unwrap();
        let previous = new_mock_backend() as Box<dyn Backend>;
        let mut manager = new_key_manager(&tmp_dir, None, master_key_backend, previous).unwrap();
        manager.shutdown_background_worker();

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
        let previous = Box::<PlaintextBackend>::default() as Box<dyn Backend>;

        let result = new_key_manager(&tmp_dir, None, wrong_key, previous);
        // When the master key is invalid, the key manager left a empty file dict and
        // return errors.
        assert!(result.is_err());
        let previous = Box::<PlaintextBackend>::default() as Box<dyn Backend>;
        new_key_manager(&tmp_dir, None, right_key, previous).unwrap();
    }

    #[test]
    fn test_plaintext_encrypter_writer() {
        use io::{Read, Write};

        let _guard = LOCK_FOR_GAUGE.lock().unwrap();
        let (key_path, _tmp_key_dir) = create_key_file("key");
        let master_key_backend =
            Box::new(FileBackend::new(key_path.as_path()).unwrap()) as Box<dyn Backend>;
        let tmp_dir = tempfile::TempDir::new().unwrap();
        let previous = new_mock_backend() as Box<dyn Backend>;
        let manager = new_key_manager(&tmp_dir, None, master_key_backend, previous).unwrap();
        let path = tmp_dir.path().join("nonencyrpted");
        let content = "I'm exposed.".to_string();
        {
            let raw = File::create(&path).unwrap();
            let mut f = manager
                .open_file_with_writer(&path, raw, false /* create */)
                .unwrap();
            f.write_all(content.as_bytes()).unwrap();
            f.sync_all().unwrap();
        }
        {
            let mut buffer = String::new();
            let mut f = File::open(&path).unwrap();
            assert_eq!(f.read_to_string(&mut buffer).unwrap(), content.len());
            assert_eq!(buffer, content);
        }
        {
            let mut buffer = String::new();
            let mut f = manager.open_file_for_read(&path).unwrap();
            assert_eq!(f.read_to_string(&mut buffer).unwrap(), content.len());
            assert_eq!(buffer, content);
        }
    }

    fn generate_mock_file<P: AsRef<Path>>(dkm: Option<&DataKeyManager>, path: P, content: &String) {
        use io::Write;
        match dkm {
            Some(manager) => {
                // Encryption enabled. Use DataKeyManager to manage file.
                let mut f = manager.create_file_for_write(&path).unwrap();
                f.write_all(content.as_bytes()).unwrap();
                f.sync_all().unwrap();
            }
            None => {
                // Encryption disabled. Write content in plaintext.
                let mut f = File::create(&path).unwrap();
                f.write_all(content.as_bytes()).unwrap();
                f.sync_all().unwrap();
            }
        }
    }

    fn check_mock_file_content<P: AsRef<Path>>(
        dkm: Option<&DataKeyManager>,
        path: P,
        expected: &String,
    ) {
        use io::Read;

        match dkm {
            Some(manager) => {
                let mut buffer = String::new();
                let mut f = manager.open_file_for_read(&path).unwrap();
                assert_eq!(f.read_to_string(&mut buffer).unwrap(), expected.len());
                assert_eq!(buffer, expected.to_string());
            }
            None => {
                let mut buffer = String::new();
                let mut f = File::open(&path).unwrap();
                assert_eq!(f.read_to_string(&mut buffer).unwrap(), expected.len());
                assert_eq!(buffer, expected.to_string());
            }
        }
    }

    fn test_change_method(from: EncryptionMethod, to: EncryptionMethod) {
        if from == to {
            return;
        }

        let generate_file_name = |method| format!("{:?}", method);
        let generate_file_content = |method| format!("Encrypted with {:?}", method);
        let tmp_dir = tempfile::TempDir::new().unwrap();
        let (key_path, _tmp_key_dir) = create_key_file("key");
        let master_key_backend =
            Box::new(FileBackend::new(key_path.as_path()).unwrap()) as Box<dyn Backend>;
        let previous = new_mock_backend() as Box<dyn Backend>;
        let path_to_file1 = tmp_dir.path().join(generate_file_name(from));
        let content1 = generate_file_content(from);

        if from == EncryptionMethod::Plaintext {
            // encryption not enabled.
            let mut args = def_data_key_args(&tmp_dir);
            args.method = EncryptionMethod::Plaintext;
            let manager =
                DataKeyManager::new(master_key_backend, Box::new(move || Ok(previous)), args)
                    .unwrap();
            assert!(manager.is_none());
            generate_mock_file(None, &path_to_file1, &content1);
            check_mock_file_content(None, &path_to_file1, &content1);
        } else {
            let manager =
                new_key_manager(&tmp_dir, Some(from), master_key_backend, previous).unwrap();

            generate_mock_file(Some(&manager), &path_to_file1, &content1);
            check_mock_file_content(Some(&manager), &path_to_file1, &content1);
            // Close old manager
            drop(manager);
        }

        // re-open with new encryption/plaintext algorithm.
        let master_key_backend =
            Box::new(FileBackend::new(key_path.as_path()).unwrap()) as Box<dyn Backend>;
        let previous = new_mock_backend() as Box<dyn Backend>;
        let manager = new_key_manager(&tmp_dir, Some(to), master_key_backend, previous).unwrap();
        let path_to_file2 = tmp_dir.path().join(generate_file_name(to));

        let content2 = generate_file_content(to);
        generate_mock_file(Some(&manager), &path_to_file2, &content2);
        check_mock_file_content(Some(&manager), &path_to_file2, &content2);
        // check old file content
        check_mock_file_content(Some(&manager), &path_to_file1, &content1);
    }

    #[test]
    fn test_encryption_algorithm_switch() {
        let _guard = LOCK_FOR_GAUGE.lock().unwrap();

        let method_list = [
            EncryptionMethod::Plaintext,
            EncryptionMethod::Aes128Ctr,
            EncryptionMethod::Aes192Ctr,
            EncryptionMethod::Aes256Ctr,
            EncryptionMethod::Sm4Ctr,
        ];
        for from in method_list {
            for to in method_list {
                test_change_method(from, to)
            }
        }
    }

    #[test]
    fn test_rename_dir() {
        let _guard = LOCK_FOR_GAUGE.lock().unwrap();
        let tmp_dir = tempfile::TempDir::new().unwrap();
        let manager = new_key_manager_def(&tmp_dir, Some(EncryptionMethod::Aes192Ctr)).unwrap();
        let subdir = tmp_dir.path().join("foo");
        std::fs::create_dir(&subdir).unwrap();
        let file_a = manager
            .new_file(subdir.join("a").to_str().unwrap())
            .unwrap();
        File::create(subdir.join("a")).unwrap();
        let file_b = manager
            .new_file(subdir.join("b").to_str().unwrap())
            .unwrap();
        File::create(subdir.join("b")).unwrap();

        let dstdir = tmp_dir.path().join("bar");
        manager
            .link_file(subdir.to_str().unwrap(), dstdir.to_str().unwrap())
            .unwrap();
        manager.delete_file(subdir.to_str().unwrap(), None).unwrap();

        assert_eq!(
            manager
                .get_file(dstdir.join("a").to_str().unwrap())
                .unwrap(),
            file_a
        );
        assert_eq!(
            manager
                .get_file_exists(subdir.join("a").to_str().unwrap())
                .unwrap(),
            None
        );

        assert_eq!(
            manager
                .get_file(dstdir.join("b").to_str().unwrap())
                .unwrap(),
            file_b
        );
        assert_eq!(
            manager
                .get_file_exists(subdir.join("b").to_str().unwrap())
                .unwrap(),
            None
        );
    }

    #[test]
    fn test_import_keys() {
        let _guard = LOCK_FOR_GAUGE.lock().unwrap();
        let tmp_dir = tempfile::TempDir::new().unwrap();
        let manager = new_key_manager_def(&tmp_dir, Some(EncryptionMethod::Aes192Ctr)).unwrap();

        let mut importer = DataKeyImporter::new(&manager);
        let file0 = manager.new_file("0").unwrap();

        // conflict with actual file.
        let f = tmp_dir.path().join("0").to_str().unwrap().to_owned();
        let _ = manager.new_file(&f).unwrap();
        File::create(&f).unwrap();
        importer
            .add(&f, file0.iv.clone(), DataKey::default())
            .unwrap_err();
        // conflict with only key.
        importer
            .add("0", file0.iv.clone(), DataKey::default())
            .unwrap();
        // same key
        importer
            .add(
                "1",
                file0.iv.clone(),
                DataKey {
                    key: file0.key.clone(),
                    method: EncryptionMethod::Aes192Ctr,
                    ..Default::default()
                },
            )
            .unwrap();
        // different key
        let (_, key2) = generate_data_key(EncryptionMethod::Aes192Ctr).unwrap();
        importer
            .add(
                "2",
                Iv::new_ctr().unwrap().as_slice().to_owned(),
                DataKey {
                    key: key2.clone(),
                    method: EncryptionMethod::Aes192Ctr,
                    ..Default::default()
                },
            )
            .unwrap();

        assert_eq!(manager.get_file("0").unwrap(), file0);
        assert_eq!(manager.get_file("1").unwrap(), file0);
        assert_eq!(manager.get_file("2").unwrap().key, key2);

        drop(importer);
        assert_eq!(manager.get_file_exists("1").unwrap(), None);
        assert_eq!(manager.get_file_exists("2").unwrap(), None);

        let mut importer = DataKeyImporter::new(&manager);
        // same key
        importer
            .add(
                "1",
                file0.iv.clone(),
                DataKey {
                    key: file0.key.clone(),
                    method: EncryptionMethod::Aes192Ctr,
                    ..Default::default()
                },
            )
            .unwrap();
        // different key
        importer
            .add(
                "2",
                Iv::new_ctr().unwrap().as_slice().to_owned(),
                DataKey {
                    key: key2.clone(),
                    method: EncryptionMethod::Aes192Ctr,
                    ..Default::default()
                },
            )
            .unwrap();
        // importer is dropped here.
        importer.commit().unwrap();
        assert_eq!(manager.get_file("1").unwrap(), file0);
        assert_eq!(manager.get_file("2").unwrap().key, key2);
    }

    // Test two importer importing duplicate files.
    // issue-15052
    #[test]
    fn test_import_keys_duplicate() {
        let _guard = LOCK_FOR_GAUGE.lock().unwrap();
        let tmp_dir = tempfile::TempDir::new().unwrap();
        let manager = new_key_manager_def(&tmp_dir, Some(EncryptionMethod::Aes192Ctr)).unwrap();

        let (_, key) = generate_data_key(EncryptionMethod::Aes192Ctr).unwrap();
        let file0 = manager.new_file("0").unwrap();
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs();
        let key = DataKey {
            key,
            method: EncryptionMethod::Aes192Ctr,
            creation_time: now,
            ..Default::default()
        };

        // Because of time window check, importer2 will create yet another key_id, so no
        // conflict.
        let mut importer1 = DataKeyImporter::new(&manager);
        importer1.add("1", file0.iv.clone(), key.clone()).unwrap();
        let mut importer2 = DataKeyImporter::new(&manager);
        importer2.add("2", file0.iv.clone(), key.clone()).unwrap();
        importer1.rollback().unwrap();
        importer2.commit().unwrap();
        assert_eq!(manager.get_file_exists("1").unwrap(), None);
        assert_eq!(manager.get_file("2").unwrap().key, key.key);

        let mut importer1 = DataKeyImporter::new(&manager);
        // Use a super old time.
        importer1.start_time = SystemTime::now() - std::time::Duration::from_secs(1000000);
        importer1.add("3", file0.iv.clone(), key.clone()).unwrap();
        let mut importer2 = DataKeyImporter::new(&manager);
        importer2.add("4", file0.iv, key.clone()).unwrap();
        // This time, even though importer2 will use the same key_id, importer1 rollback
        // cannot remove it.
        importer1.rollback().unwrap();
        importer2.commit().unwrap();
        assert_eq!(manager.get_file_exists("3").unwrap(), None);
        assert_eq!(manager.get_file("4").unwrap().key, key.key);
    }

    #[test]
    fn test_trash_encrypted_dir() {
        let tmp_dir = tempfile::Builder::new()
            .prefix("test_trash_encrypted_dir")
            .tempdir()
            .unwrap();
        let manager = new_key_manager_def(&tmp_dir, Some(EncryptionMethod::Aes192Ctr)).unwrap();
        let data_path = tmp_dir.path();
        let sub_dir = data_path.join("sub_dir");
        file_system::create_dir_all(&sub_dir).unwrap();
        let file_path = sub_dir.join("f");
        file_system::File::create(&file_path).unwrap();
        manager.new_file(file_path.to_str().unwrap()).unwrap();
        file_system::create_dir_all(sub_dir.join("deep_dir")).unwrap();
        assert_eq!(manager.file_count(), 1);

        crate::trash_dir_all(&sub_dir, Some(&manager)).unwrap();
        assert_eq!(manager.file_count(), 0);
    }
}
