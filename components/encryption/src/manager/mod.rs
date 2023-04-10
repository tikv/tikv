// Copyright 2020 TiKV Project Authors. Licensed under Apache-2.0.

use std::{
    io::{Error as IoError, ErrorKind, Result as IoResult},
    path::{Path, PathBuf},
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc, Mutex,
    },
    thread::JoinHandle,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use crossbeam::channel::{self, select, tick};
use engine_traits::{
    EncryptionKeyManager, EncryptionMethod as EtEncryptionMethod, FileEncryptionInfo,
};
use fail::fail_point;
use file_system::File;
use kvproto::encryptionpb::{
    DataKey, EncryptionMethod, FileDictionary, FileDictionaryV2, FileInfo, KeyDictionary,
};
use protobuf::{parse_from_bytes, Message};
use tikv_util::{
    box_err, debug, error, info, sys::thread::StdThreadBuildWrapper, thd_name, warn, Either,
};

use crate::{
    config::EncryptionConfig,
    crypter::{self, Iv},
    encrypted_file::EncryptedFile,
    file_dict_file::{DataKeyDictionaryItem, DataKeyDictionaryItemV2, DictionaryFile},
    io::{DecrypterReader, EncrypterWriter},
    master_key::Backend,
    metrics::*,
    Error, Result,
};

const KEY_DICT_NAME: &str = "key.dict";
const FILE_DICT_NAME: &str = "file.dict";
const FILE_DICT_V2_NAME: &str = "file.dict.v2";
const ROTATE_CHECK_PERIOD: u64 = 600; // 10min

struct Dicts {
    base: PathBuf,
    rotation_period: Duration,
    // Directories directly under these (`allowlist/<name>`) will be represented using Dir ID. This
    // makes it possible to rename directory without modifying records of every file.
    //
    // The reasons for having an allowlist instead of treating all directories this way:
    // (1) keep v1 data keys file backward compatible.
    // (2) make it easy to find the right prefix of a file path to locate its Dir ID.
    //
    // The string should not contain trailing slash.
    v2_directory_allowlist: Vec<String>,

    // File Full Path -> FileInfo.
    file_dict: Mutex<FileDictionary>,
    // `data_keys` stored as plaintext.
    file_dict_file: Mutex<DictionaryFile<FileDictionary>>,

    // Dir Path -> Dir ID; (Dir ID + File Relative Path) -> FileInfo.
    file_dict_v2: Mutex<FileDictionaryV2>,
    // `file_dict_v2` stored as plaintext.
    file_dict_file_v2: Mutex<DictionaryFile<FileDictionaryV2>>,
    // Used to allocate new Dir ID.
    next_dir_id: AtomicU64,

    // Maps data key id to data keys, together with metadata. Also stores the data
    // key id used to encrypt the encryption file dictionary. The content is encrypted
    // using master key.
    key_dict: Mutex<KeyDictionary>,
    // Thread-safe version of current_key_id. Only when writing back to key_dict,
    // write it back to `key_dict`. Reader should always use this atomic, instead of
    // key_dict.current_key_id, since the latter can reflect an update-in-progress key.
    current_key_id: AtomicU64,
}

impl Dicts {
    fn new(
        path: &str,
        rotation_period: Duration,
        enable_file_dictionary_log: bool,
        file_dictionary_rewrite_threshold: u64,
        v2_directory_allowlist: Vec<String>,
    ) -> Result<Dicts> {
        Ok(Dicts {
            base: Path::new(path).to_owned(),
            rotation_period,
            v2_directory_allowlist,
            file_dict: Mutex::new(FileDictionary::default()),
            file_dict_file: Mutex::new(DictionaryFile::<FileDictionary>::new(
                Path::new(path),
                FILE_DICT_NAME,
                enable_file_dictionary_log,
                file_dictionary_rewrite_threshold,
            )?),
            file_dict_v2: Mutex::new(FileDictionaryV2::default()),
            file_dict_file_v2: Mutex::new(DictionaryFile::<FileDictionaryV2>::new(
                Path::new(path),
                FILE_DICT_V2_NAME,
                enable_file_dictionary_log,
                file_dictionary_rewrite_threshold,
            )?),
            next_dir_id: AtomicU64::new(0),
            key_dict: Mutex::new(KeyDictionary {
                current_key_id: 0,
                ..Default::default()
            }),
            current_key_id: AtomicU64::new(0),
        })
    }

    fn open(
        path: &str,
        rotation_period: Duration,
        master_key: &dyn Backend,
        enable_file_dictionary_log: bool,
        file_dictionary_rewrite_threshold: u64,
        v2_directory_allowlist: Vec<String>,
    ) -> Result<Option<Dicts>> {
        let base = Path::new(path);

        let file_dict_file = DictionaryFile::<FileDictionary>::open(
            base,
            FILE_DICT_NAME,
            enable_file_dictionary_log,
            file_dictionary_rewrite_threshold,
            false,
        );
        let key_bytes = EncryptedFile::new(base, KEY_DICT_NAME).read(master_key);

        match (file_dict_file, key_bytes) {
            // Both files are found.
            (Ok(file_dict_file), Ok(key_bytes)) => {
                info!("encryption: found both of key dictionary and file dictionary.");
                let key_dict: KeyDictionary = parse_from_bytes(&key_bytes)?;
                let current_key_id = AtomicU64::new(key_dict.current_key_id);
                ENCRYPTION_DATA_KEY_GAUGE.set(key_dict.keys.len() as _);

                let file_dict = file_dict_file.dict().clone();
                let mut total_files = file_dict.files.len();

                let file_dict_file_v2 = match DictionaryFile::<FileDictionaryV2>::open(
                    base,
                    FILE_DICT_V2_NAME,
                    enable_file_dictionary_log,
                    file_dictionary_rewrite_threshold,
                    false,
                ) {
                    Ok(d) => d,
                    // Upgraded from an older version.
                    Err(Error::Io(e)) if e.kind() == ErrorKind::NotFound => {
                        DictionaryFile::<FileDictionaryV2>::new(
                            base,
                            FILE_DICT_V2_NAME,
                            enable_file_dictionary_log,
                            file_dictionary_rewrite_threshold,
                        )?
                    }
                    Err(e) => return Err(e),
                };
                let file_dict_v2 = file_dict_file_v2.dict().clone();
                let mut max_dir_id = 0;
                for (dir_id, files) in &file_dict_v2.dir_files {
                    max_dir_id = std::cmp::max(*dir_id, max_dir_id);
                    total_files += files.files.len();
                }
                ENCRYPTION_FILE_NUM_GAUGE.set(total_files as _);

                Ok(Some(Dicts {
                    base: base.to_owned(),
                    rotation_period,
                    v2_directory_allowlist,
                    file_dict: Mutex::new(file_dict),
                    file_dict_file: Mutex::new(file_dict_file),
                    file_dict_v2: Mutex::new(file_dict_v2),
                    file_dict_file_v2: Mutex::new(file_dict_file_v2),
                    next_dir_id: AtomicU64::new(max_dir_id + 1),
                    key_dict: Mutex::new(key_dict),
                    current_key_id,
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
            (Ok(file_dict_file), Err(Error::Io(key_err)))
                if key_err.kind() == ErrorKind::NotFound
                    && file_dict_file.dict().files.is_empty() =>
            {
                std::fs::remove_file(file_dict_file.file_path())?;
                info!("encryption: file dict is empty and none of key dictionary are found.");
                Ok(None)
            }
            // ...else, return either error.
            (Err(e), _) => {
                error!("encryption: failed to load file dictionary.");
                Err(e)
            }
            (_, Err(e)) => {
                error!("encryption: failed to load key dictionary.");
                Err(e)
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

    // Returns Dir, Relative Path.
    #[inline]
    fn parse_v2(&self, full_name: &str) -> Option<(String, String)> {
        for p in &self.v2_directory_allowlist {
            if let Some(name) = full_name.strip_prefix(p)
                && let len = name.len()
                && let name = name.trim_start_matches('/')
                && name.len() < len
            {
                if let Some(fname_start) = name.find('/') {
                    let dir = p.to_string() + "/" + &name[..fname_start];
                    let relative_path = name[fname_start + 1..].trim_start_matches('/');
                    return Some((dir, relative_path.to_string()))
                } else {
                    return Some((p.to_string() + "/" + name, String::new()))
                }
            }
        }
        None
    }

    fn get_file(&self, fname: &str) -> Option<FileInfo> {
        // Try v1 map first. It could contain files from an older verison.
        let info = {
            let dict = self.file_dict.lock().unwrap();
            dict.files.get(fname).cloned()
        };
        if info.is_none() {
            if let Some((dir, relative_path)) = self.parse_v2(fname) {
                let dict = self.file_dict_v2.lock().unwrap();
                return dict.dirs.get(&dir).and_then(|dir_id| {
                    dict.dir_files.get(dir_id).and_then(|files| {
                        let i = files.files.get(&relative_path).cloned();
                        i
                    })
                });
            }
        }
        info
    }

    // Returns number of files. The `FileInfo` can be either directly provided or
    // retrieved from dict after acquiring write lock. In the latter case, if dict
    // doesn't contains the file, returns `Ok(None)`.
    #[inline]
    fn new_file_imp(&self, fname: &str, file: Either<FileInfo, &str>) -> Result<Option<i64>> {
        if let Some((dir, relative_path)) = self.parse_v2(fname) {
            let mut dict_file = self.file_dict_file_v2.lock().unwrap();
            let file = match file {
                Either::Left(f) => f,
                Either::Right(s) => {
                    if let Some(f) = self.get_file(s) {
                        f
                    } else {
                        return Ok(None);
                    }
                }
            };

            let mut new_dir_id = None;
            let dir_id = {
                let mut dict = self.file_dict_v2.lock().unwrap();
                let dir_id = *dict.dirs.entry(dir.clone()).or_insert_with(|| {
                    new_dir_id = Some(self.next_dir_id.fetch_add(1, Ordering::Relaxed));
                    new_dir_id.unwrap()
                });
                dict.dir_files
                    .entry(dir_id)
                    .or_default()
                    .files
                    .insert(relative_path.clone(), file.clone());
                dir_id
            };
            if let Some(id) = new_dir_id {
                dict_file.add(DataKeyDictionaryItemV2::InsertDir(id, dir))?;
            }
            dict_file.add(DataKeyDictionaryItemV2::InsertFile(
                dir_id,
                relative_path,
                file,
            ))?;
            Ok(Some(0))
        } else {
            let mut file_dict_file = self.file_dict_file.lock().unwrap();
            let file = match file {
                Either::Left(f) => f,
                Either::Right(s) => {
                    if let Some(f) = self.get_file(s) {
                        f
                    } else {
                        return Ok(None);
                    }
                }
            };
            if self.get_file(fname).is_some() && Path::new(fname).exists() {
                return Err(Error::Io(IoError::new(
                    ErrorKind::AlreadyExists,
                    format!("file already exists, {}", fname),
                )));
            }

            let file_num = {
                let mut file_dict = self.file_dict.lock().unwrap();
                file_dict.files.insert(fname.to_owned(), file.clone());
                file_dict.files.len() as _
            };
            file_dict_file.add(DataKeyDictionaryItem::Insert(fname.to_owned(), file))?;
            Ok(Some(file_num))
        }
    }

    fn new_file(&self, fname: &str, method: EncryptionMethod) -> Result<FileInfo> {
        let iv = if method != EncryptionMethod::Plaintext {
            Iv::new_ctr()
        } else {
            Iv::Empty
        };
        let file = FileInfo {
            iv: iv.as_slice().to_vec(),
            key_id: self.current_key_id.load(Ordering::SeqCst),
            method,
            ..Default::default()
        };
        let file_num = self
            .new_file_imp(fname, Either::Left(file.clone()))?
            .unwrap();
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
    fn delete_file(&self, fname: &str) -> Result<()> {
        // This map only contains legacy and out-of-scope files. The overhead should be
        // small.
        let is_v1 = self.file_dict.lock().unwrap().files.contains_key(fname);

        let (file, file_num) = if !is_v1 && let Some((dir, relative_path)) = self.parse_v2(fname) {
            let mut file_dict_file = self.file_dict_file_v2.lock().unwrap();
            let (dir_id, info, file_num) = {
                let mut dict = self.file_dict_v2.lock().unwrap();
                if let Some(&dir_id) = dict.dirs.get(&dir)
                    && let Some(files) = dict.dir_files.get_mut(&dir_id)
                    && let Some(info) = files.files.remove(&relative_path)
                {
                    (dir_id, info, 0)
                } else {
                    // Could be a plaintext file not tracked by file dictionary.
                    // There's no need to check v1 again. Even if there's a race between `new_file` and getting `is_v1`, the created file should be a v2.
                    info!("delete untracked plaintext file"; "fname" => fname);
                    return Ok(());
                }
            };
            file_dict_file.add(DataKeyDictionaryItemV2::RemoveFile(dir_id, fname.to_owned()))?;
            (info, file_num)
        } else {
            // Still enter this branch if `is_v1`. There's could be a race between `new_file` and `delete_file`.
            let mut file_dict_file = self.file_dict_file.lock().unwrap();
            let r = {
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
            file_dict_file.add(DataKeyDictionaryItem::Remove(fname.to_owned()))?;
            r
        };

        ENCRYPTION_FILE_NUM_GAUGE.set(file_num);
        if file.method != EncryptionMethod::Plaintext {
            debug!("delete encrypted file"; "fname" => fname);
        } else {
            debug!("delete plaintext file"; "fname" => fname);
        }
        Ok(())
    }

    fn link_file(&self, src_fname: &str, dst_fname: &str) -> Result<Option<()>> {
        if let Some(file_num) = self.new_file_imp(dst_fname, Either::Right(src_fname))? {
            ENCRYPTION_FILE_NUM_GAUGE.set(file_num);
            info!("link file"; "src" => src_fname, "dst" => dst_fname);
            Ok(Some(()))
        } else {
            info!("link untracked plaintext file"; "src" => src_fname, "dst" => dst_fname);
            Ok(None)
        }
    }

    fn link_dir(&self, src_name: &str, dst_name: &str) -> Result<()> {
        if let Some((dir, relative_path)) = self.parse_v2(src_name) {
            if !relative_path.is_empty() {
                return Err(box_err!(format!(
                    "Linking nested directory {src_name} is not supported."
                )));
            }
            if let Some((dst_dir, dst_relative)) = self.parse_v2(dst_name) {
                if !dst_relative.is_empty() {
                    return Err(box_err!(format!(
                        "Linking nested directory {dst_name} is not supported."
                    )));
                }
                let mut dict_file = self.file_dict_file_v2.lock().unwrap();
                let dir_id = {
                    let mut dict = self.file_dict_v2.lock().unwrap();
                    if let Some(&dir_id) = dict.dirs.get(&dir) {
                        dict.dirs.insert(dst_dir.clone(), dir_id);
                        dir_id
                    } else {
                        // The directory doesn't contain any encrypted file.
                        return Ok(());
                    }
                };
                dict_file.add(DataKeyDictionaryItemV2::InsertDir(dir_id, dst_dir))?;
                Ok(())
            } else {
                Err(box_err!(format!(
                    "Linking directory {dst_name} not under allowlist is not supported."
                )))
            }
        } else {
            Err(box_err!(format!(
                "Linking directory {src_name} not under allowlist is not supported."
            )))
        }
    }

    fn delete_dir(&self, dname: &str, includes_files: bool) -> Result<()> {
        if let Some((dir, relative_path)) = self.parse_v2(dname) {
            if !relative_path.is_empty() {
                return Err(box_err!("Deleting nested directory is not supported."));
            }
            let mut dict_file = self.file_dict_file_v2.lock().unwrap();
            let dir_id = {
                let mut dict = self.file_dict_v2.lock().unwrap();
                if let Some(&dir_id) = dict.dirs.get(&dir) {
                    dict.dirs.remove(&dir);
                    if includes_files {
                        dict.dir_files.remove(&dir_id);
                    }
                    dir_id
                } else {
                    // The directory doesn't contain any encrypted file.
                    return Ok(());
                }
            };
            dict_file.add(DataKeyDictionaryItemV2::RemoveDirMapping(dir_id, dir))?;
            if includes_files {
                dict_file.add(DataKeyDictionaryItemV2::RemoveDirFiles(dir_id))?;
            }
            Ok(())
        } else {
            Err(box_err!(
                "Deleting directory {dname} not under allowlist is not supported."
            ))
        }
    }

    #[inline]
    fn is_v2_dir(&self, name: &str) -> bool {
        if let Some((_, relative_path)) = self.parse_v2(name) {
            relative_path.is_empty()
        } else {
            false
        }
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
            if method == key.method && !(key.was_exposed && master_key.is_secure()) {
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
            method,
            creation_time,
            was_exposed: false,
            ..Default::default()
        };
        self.rotate_key(key_id, data_key, master_key)
    }
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

#[derive(Debug, Clone)]
pub struct DataKeyManagerArgs {
    pub method: EncryptionMethod,
    pub rotation_period: Duration,
    pub enable_file_dictionary_log: bool,
    pub file_dictionary_rewrite_threshold: u64,
    pub dict_path: String,
    pub v2_directory_allowlist: Vec<String>,
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
            v2_directory_allowlist: config.v2_directory_allowlist.clone(),
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
            if info.method != EncryptionMethod::Plaintext {
                let retain = f(fname);
                if !retain {
                    file_dict_file
                        .add(DataKeyDictionaryItem::Remove(fname.to_owned()))
                        .unwrap();
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
                args.v2_directory_allowlist.clone(),
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
                    args.v2_directory_allowlist.clone(),
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
            args.v2_directory_allowlist.clone(),
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
        let (rotate_terminal, rx) = channel::bounded(1);
        let background_worker = std::thread::Builder::new()
            .name(thd_name!("enc:key"))
            .spawn_wrapper(move || {
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

    pub fn create_file_for_write<P: AsRef<Path>>(&self, path: P) -> Result<EncrypterWriter<File>> {
        let file_writer = File::create(&path)?;
        self.open_file_with_writer(path, file_writer, true /* create */)
    }

    pub fn open_file_with_writer<P: AsRef<Path>, W: std::io::Write>(
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
            crypter::from_engine_encryption_method(file.method),
            &file.key,
            if file.method == EtEncryptionMethod::Plaintext {
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
            crypter::from_engine_encryption_method(file.method),
            &file.key,
            if file.method == EtEncryptionMethod::Plaintext {
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
        let dict_file = DictionaryFile::<FileDictionary>::open(
            dict_path,
            FILE_DICT_NAME,
            true, // enable_file_dictionary_log
            1,
            true, // skip_rewrite
        )?;
        if let Some(file_path) = file_path {
            if let Some(info) = dict_file.dict().files.get(file_path) {
                println!("{}: {:?}", file_path, info);
            }
        } else {
            for (path, info) in dict_file.dict().files.iter() {
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
            method: crypter::to_engine_encryption_method(method),
            iv,
        };
        Ok(Some(encrypted_file))
    }

    // We didn't (yet) handle RocksDB's internal `DeleteDir` call. This should be
    // called manually to fully destroy a directory.
    pub fn delete_dir(&self, dname: &str, includes_files: bool) -> IoResult<()> {
        self.dicts.delete_dir(dname, includes_files)?;
        Ok(())
    }

    // #[cfg(test)]
    pub fn rename_dir(&self, src_dname: &str, dst_dname: &str) -> IoResult<()> {
        self.link_file(src_dname, dst_dname)?;
        self.delete_file(src_dname)
    }

    /// Return which method this manager is using.
    pub fn encryption_method(&self) -> engine_traits::EncryptionMethod {
        crypter::to_engine_encryption_method(self.method)
    }
}

impl Drop for DataKeyManager {
    fn drop(&mut self) {
        if let Err(e) = self.rotate_terminal.send(()) {
            info!("failed to terminate background rotation, are we shutting down?"; "err" => %e);
        }
        if let Some(Err(e)) = self.background_worker.take().map(|w| w.join()) {
            info!("failed to join background rotation, are we shutting down?"; "err" => ?e);
        }
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
                let method = EncryptionMethod::Plaintext;
                Ok(FileEncryptionInfo {
                    key: vec![],
                    method: crypter::to_engine_encryption_method(method),
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
            method: crypter::to_engine_encryption_method(file.method),
            iv: file.get_iv().to_owned(),
        };
        Ok(encrypted_file)
    }

    fn delete_file(&self, fname: &str) -> IoResult<()> {
        fail_point!("key_manager_fails_before_delete_file", |_| IoResult::Err(
            std::io::ErrorKind::Other.into()
        ));
        // The file (or dir) is already deleted from file system.
        if self.dicts.is_v2_dir(fname) {
            // This is only called during rename, we don't delete files.
            self.dicts.delete_dir(fname, false)?;
        } else {
            // Files under non-whitelisted directories will be leaked forever.
            self.dicts.delete_file(fname)?;
        }
        Ok(())
    }

    // Instead of directly renaming files. We first use this method to link `dst` to
    // `src`, then rename the directory on file system, finally remove `src`. This
    // makes sure the whole operation is atomic. Even if there's a panic, we'll only
    // get a phantom key mapping after recovering.
    //
    // RocksDB uses `RenameFile` to rename both file and directory. As a result,
    // both `link_file` and `delete_file` needs to detect if the path points to a
    // file or a directory.
    fn link_file(&self, src_fname: &str, dst_fname: &str) -> IoResult<()> {
        if Path::new(src_fname).is_dir() {
            self.dicts.link_dir(src_fname, dst_fname)?;
        } else {
            self.dicts.link_file(src_fname, dst_fname)?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use engine_traits::EncryptionMethod as EtEncryptionMethod;
    use file_system::{remove_file, File};
    use matches::assert_matches;
    use tempfile::TempDir;
    use test_util::create_test_key_file;

    use super::*;
    use crate::{
        master_key::{
            tests::{decrypt_called, encrypt_called, MockBackend},
            FileBackend, PlaintextBackend,
        },
        to_engine_encryption_method,
    };

    lazy_static::lazy_static! {
        static ref LOCK_FOR_GAUGE: Mutex<i32> = Mutex::new(1);
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
            dict_path: tmp_dir.path().as_os_str().to_str().unwrap().to_string(),
            v2_directory_allowlist: Vec::new(),
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
        assert_eq!(bar.method, EtEncryptionMethod::Plaintext);
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
        let _guard = LOCK_FOR_GAUGE.lock().unwrap();
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
        let _guard = LOCK_FOR_GAUGE.lock().unwrap();
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
        use std::io::{Read, Write};

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
        use std::io::Write;
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
        use std::io::Read;

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
    fn test_encrypted_directory_basic() {
        let _guard = LOCK_FOR_GAUGE.lock().unwrap();
        let tmp_dir = tempfile::TempDir::new().unwrap();
        let mut args = def_data_key_args(&tmp_dir);
        let path = tmp_dir.path();
        let (key_path, _tmp_key_dir) = create_key_file("key");
        args.v2_directory_allowlist = vec![path.to_str().unwrap().to_owned()];
        let method = to_engine_encryption_method(args.method);

        {
            let master_key_backend =
                Box::new(FileBackend::new(key_path.as_path()).unwrap()) as Box<dyn Backend>;
            let previous = new_mock_backend() as Box<dyn Backend>;
            let manager = DataKeyManager::new(
                master_key_backend,
                Box::new(move || Ok(previous)),
                args.clone(),
            )
            .unwrap()
            .unwrap();
            manager
                .new_file(path.join("dir0").join("foo").to_str().unwrap())
                .unwrap();
            manager
                .new_file(path.join("dir0").join("bar").to_str().unwrap())
                .unwrap();
            manager
                .new_file(path.join("dir2").join("foo").to_str().unwrap())
                .unwrap();
            // Manager needs to detect whether it's file or directory.
            std::fs::create_dir(path.join("dir0")).unwrap();
            manager
                .rename_dir(
                    path.join("dir0").to_str().unwrap(),
                    path.join("dir1").to_str().unwrap(),
                )
                .unwrap();
            manager
                .delete_dir(path.join("dir2").to_str().unwrap(), false)
                .unwrap();
            assert_eq!(
                manager
                    .get_file(path.join("dir1").join("foo").to_str().unwrap())
                    .unwrap()
                    .method,
                method
            );
            assert_eq!(
                manager
                    .get_file(path.join("dir1").join("bar").to_str().unwrap())
                    .unwrap()
                    .method,
                method
            );
            assert_eq!(
                manager
                    .get_file_exists(path.join("dir0").join("bar").to_str().unwrap())
                    .unwrap(),
                None
            );
            assert_eq!(
                manager
                    .get_file_exists(path.join("dir2").join("foo").to_str().unwrap())
                    .unwrap(),
                None
            );
        }
        {
            let master_key_backend =
                Box::new(FileBackend::new(key_path.as_path()).unwrap()) as Box<dyn Backend>;
            let previous = new_mock_backend() as Box<dyn Backend>;
            let manager =
                DataKeyManager::new(master_key_backend, Box::new(move || Ok(previous)), args)
                    .unwrap()
                    .unwrap();
            assert_eq!(
                manager
                    .get_file(path.join("dir1").join("foo").to_str().unwrap())
                    .unwrap()
                    .method,
                method
            );
            assert_eq!(
                manager
                    .get_file(path.join("dir1").join("bar").to_str().unwrap())
                    .unwrap()
                    .method,
                method
            );
            assert_eq!(
                manager
                    .get_file_exists(path.join("dir0").join("bar").to_str().unwrap())
                    .unwrap(),
                None
            );
            assert_eq!(
                manager
                    .get_file_exists(path.join("dir2").join("foo").to_str().unwrap())
                    .unwrap(),
                None
            );
        }
    }
}
