// Copyright 2018 TiKV Project Authors. Licensed under Apache-2.0.

use std::{
    borrow::Cow,
    collections::HashMap,
    fs::File,
    io::{self, BufReader, ErrorKind, Read},
    ops::Bound,
    path::{Path, PathBuf},
    sync::Arc,
    time::{Duration, SystemTime},
};

use collections::HashSet;
use dashmap::{mapref::entry::Entry, DashMap};
use encryption::{DataKeyManager, FileEncryptionInfo, MultiMasterKeyBackend};
use encryption_export::create_async_backend;
use engine_traits::{
    name_to_cf, util::check_key_in_range, CfName, IterOptions, Iterator, KvEngine, RefIterable,
    SstCompressionType, SstExt, SstMetaInfo, SstReader, SstWriter, SstWriterBuilder, CF_DEFAULT,
    CF_WRITE,
};
use external_storage::{
    compression_reader_dispatcher, encrypt_wrap_reader, wrap_with_checksum_reader_if_needed,
    ExternalStorage, RestoreConfig,
};
use file_system::{IoType, OpenOptions};
use kvproto::{
    brpb::{CipherInfo, StorageBackend},
    encryptionpb::{EncryptionMethod, FileEncryptionInfo_oneof_mode, MasterKey},
    import_sstpb::{Range, *},
    kvrpcpb::ApiVersion,
    metapb::Region,
};
use tikv_util::{
    codec::{
        bytes::{decode_bytes_in_place, encode_bytes},
        stream_event::{EventEncoder, EventIterator, Iterator as EIterator},
    },
    future::RescheduleChecker,
    memory::{MemoryQuota, OwnedAllocated},
    resizable_threadpool::ResizableRuntimeHandle,
    sys::{thread::ThreadBuildWrapper, SysQuota},
    time::{Instant, Limiter},
    Either, HandyRwLock,
};
use tokio::{runtime::Runtime, sync::OnceCell};
use txn_types::{Key, TimeStamp, WriteRef};

use crate::{
    caching::cache_map::{CacheMap, ShareOwned},
    import_file::{ImportDir, ImportFile},
    import_mode::{ImportModeSwitcher, RocksDbMetricsFn},
    import_mode2::{HashRange, ImportModeSwitcherV2},
    metrics::*,
    sst_writer::{RawSstWriter, TxnSstWriter},
    util, Config, ConfigManager as ImportConfigManager, Error, Result,
};

pub struct LoadedFile {
    _permit: OwnedAllocated,
    content: Arc<[u8]>,
}

impl std::fmt::Debug for LoadedFile {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("LoadedFileInner")
            .field("content.len()", &self.content.len())
            .finish()
    }
}

impl ShareOwned for LoadedFile {
    type Shared = Arc<[u8]>;

    fn share_owned(&self) -> Self::Shared {
        Arc::clone(&self.content)
    }
}

#[derive(Default, Debug, Clone)]
pub struct DownloadExt<'a> {
    cache_key: Option<&'a str>,
    req_type: DownloadRequestType,
}

impl<'a> DownloadExt<'a> {
    pub fn cache_key(mut self, key: &'a str) -> Self {
        self.cache_key = Some(key);
        self
    }

    pub fn req_type(mut self, req_type: DownloadRequestType) -> Self {
        self.req_type = req_type;
        self
    }
}

#[derive(Clone, Debug)]
pub enum CacheKvFile {
    Mem(Arc<OnceCell<LoadedFile>>),
    Fs(Arc<PathBuf>),
}

/// returns an error on an invalid internal state.
/// pass the error back to the client side for further debugging.
fn error(message: impl std::fmt::Display) -> Error {
    Error::Io(io::Error::new(
        ErrorKind::Other,
        format!("internal error in TiKV: {}", message),
    ))
}

impl CacheKvFile {
    // get the ref count of item.
    pub fn ref_count(&self) -> usize {
        match self {
            CacheKvFile::Mem(buff) => {
                if let Some(a) = buff.get() {
                    return Arc::strong_count(&a.content);
                }
                Arc::strong_count(buff)
            }
            CacheKvFile::Fs(path) => Arc::strong_count(path),
        }
    }

    // check the item is expired.
    pub fn is_expired(&self, start: &Instant) -> bool {
        match self {
            // The expired duration for memory is 60s.
            CacheKvFile::Mem(_) => start.saturating_elapsed() >= Duration::from_secs(60),
            // The expired duration for local file is 10min.
            CacheKvFile::Fs(_) => start.saturating_elapsed() >= Duration::from_secs(600),
        }
    }
}

/// SstImporter manages SST files that are waiting for ingesting.
pub struct SstImporter<E: KvEngine> {
    dir: ImportDir<E>,
    key_manager: Option<Arc<DataKeyManager>>,
    switcher: Either<ImportModeSwitcher, ImportModeSwitcherV2>,
    // TODO: lift api_version as a type parameter.
    api_version: ApiVersion,
    compression_types: HashMap<CfName, SstCompressionType>,

    cached_storage: CacheMap<StorageBackend>,
    // We need to keep reference to the runtime so background tasks won't be dropped.
    _download_rt: Runtime,
    file_locks: Arc<DashMap<String, (CacheKvFile, Instant)>>,
    memory_quota: Arc<MemoryQuota>,
    multi_master_keys_backend: MultiMasterKeyBackend,
}

impl<E: KvEngine> SstImporter<E> {
    pub fn new<P: AsRef<Path>>(
        cfg: &Config,
        import_dir: P,
        key_manager: Option<Arc<DataKeyManager>>,
        api_version: ApiVersion,
        raft_kv_v2: bool,
    ) -> Result<Self> {
        let switcher = if raft_kv_v2 {
            Either::Right(ImportModeSwitcherV2::new(cfg))
        } else {
            Either::Left(ImportModeSwitcher::new(cfg))
        };
        let cached_storage = CacheMap::default();
        // We are going to run some background tasks here, (hyper needs to maintain the
        // connection, the cache map needs gc intervally.) so we must create a
        // multi-thread runtime, given there isn't blocking, a single thread runtime is
        // enough.
        let download_rt = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(1)
            .thread_name("sst_import_misc")
            .with_sys_and_custom_hooks(
                || {
                    file_system::set_io_type(IoType::Import);
                },
                || {},
            )
            .enable_all()
            .build()?;
        download_rt.spawn(cached_storage.gc_loop());

        let memory_limit = Self::calcualte_usage_mem(cfg.memory_use_ratio);
        info!(
            "sst importer memory limit when apply";
            "ratio" => cfg.memory_use_ratio,
            "size" => ?memory_limit,
        );

        let dir = ImportDir::new(import_dir)?;

        Ok(SstImporter {
            dir,
            key_manager,
            switcher,
            api_version,
            compression_types: HashMap::with_capacity(2),
            file_locks: Arc::new(DashMap::default()),
            cached_storage,
            _download_rt: download_rt,
            memory_quota: Arc::new(MemoryQuota::new(memory_limit as _)),
            multi_master_keys_backend: MultiMasterKeyBackend::new(),
        })
    }

    pub fn ranges_enter_import_mode(&self, ranges: Vec<Range>) {
        if let Either::Right(ref switcher) = self.switcher {
            switcher.ranges_enter_import_mode(ranges)
        } else {
            unreachable!();
        }
    }

    pub fn clear_import_mode_regions(&self, ranges: Vec<Range>) {
        if let Either::Right(ref switcher) = self.switcher {
            switcher.clear_import_mode_range(ranges);
        } else {
            unreachable!();
        }
    }

    // it always returns false for v1
    pub fn region_in_import_mode(&self, region: &Region) -> bool {
        if let Either::Right(ref switcher) = self.switcher {
            switcher.region_in_import_mode(region)
        } else {
            false
        }
    }

    // it always returns false for v1
    pub fn range_in_import_mode(&self, range: &Range) -> bool {
        if let Either::Right(ref switcher) = self.switcher {
            switcher.range_in_import_mode(range)
        } else {
            false
        }
    }

    pub fn ranges_in_import(&self) -> HashSet<HashRange> {
        if let Either::Right(ref switcher) = self.switcher {
            switcher.ranges_in_import()
        } else {
            unreachable!()
        }
    }

    fn calcualte_usage_mem(mem_ratio: f64) -> u64 {
        ((SysQuota::memory_limit_in_bytes() as f64) * mem_ratio) as u64
    }

    pub fn set_compression_type(
        &mut self,
        cf_name: CfName,
        compression_type: Option<SstCompressionType>,
    ) {
        if let Some(ct) = compression_type {
            self.compression_types.insert(cf_name, ct);
        } else {
            self.compression_types.remove(cf_name);
        }
    }

    pub fn start_switch_mode_check(&self, executor: &ResizableRuntimeHandle, db: Option<E>) {
        match &self.switcher {
            Either::Left(switcher) => switcher.start_resizable_threads(executor, db.unwrap()),
            Either::Right(switcher) => switcher.start_resizable_threads(executor),
        }
    }

    pub fn get_path(&self, meta: &SstMeta) -> PathBuf {
        let path = self.dir.join_for_read(meta).unwrap();
        path.save
    }

    pub fn get_total_size(&self) -> Result<u64> {
        let mut total_size = 0;
        for entry in file_system::read_dir(self.dir.get_root_dir())? {
            match entry.and_then(|e| e.metadata().map(|m| (e, m))) {
                Ok((_, m)) => {
                    if !m.is_file() {
                        continue;
                    }
                    total_size += m.len();
                }
                Err(e) if e.kind() == ErrorKind::NotFound => continue,
                Err(e) => return Err(Error::from(e)),
            };
        }
        Ok(total_size)
    }

    pub fn create(&self, meta: &SstMeta) -> Result<ImportFile> {
        match self.dir.create(meta, self.key_manager.clone()) {
            Ok(f) => {
                info!("create"; "file" => ?f);
                Ok(f)
            }
            Err(e) => {
                error!(%e; "create failed"; "meta" => ?meta,);
                Err(e)
            }
        }
    }

    pub fn delete(&self, meta: &SstMeta) -> Result<()> {
        match self.dir.delete(meta, self.key_manager.as_deref()) {
            Ok(path) => {
                info!("delete"; "path" => ?path);
                Ok(())
            }
            Err(e) => {
                error!(%e; "delete failed"; "meta" => ?meta,);
                Err(e)
            }
        }
    }

    pub fn remove_dir(&self, prefix: &str) -> Result<()> {
        let path = self.dir.get_root_dir().join(prefix);
        if path.exists() {
            file_system::remove_dir_all(&path)?;
            info!("directory {:?} has been removed", path);
        }
        Ok(())
    }

    pub fn validate(&self, meta: &SstMeta) -> Result<SstMetaInfo> {
        self.dir.validate(meta, self.key_manager.clone())
    }

    /// check if api version of sst files are compatible
    pub fn check_api_version(&self, metas: &[SstMeta]) -> Result<bool> {
        self.dir
            .check_api_version(metas, self.key_manager.clone(), self.api_version)
    }

    pub fn ingest(&self, metas: &[SstMetaInfo], engine: &E) -> Result<()> {
        match self
            .dir
            .ingest(metas, engine, self.key_manager.clone(), self.api_version)
        {
            Ok(..) => {
                info!("ingest"; "metas" => ?metas);
                Ok(())
            }
            Err(e) => {
                error!(%e; "ingest failed"; "metas" => ?metas, );
                Err(e)
            }
        }
    }

    pub fn verify_checksum(&self, metas: &[SstMeta]) -> Result<()> {
        self.dir.verify_checksum(metas, self.key_manager.clone())
    }

    pub fn exist(&self, meta: &SstMeta) -> bool {
        self.dir.exist(meta).unwrap_or(false)
    }

    // Downloads an SST file from an external storage.
    //
    // This method is blocking. It performs the following transformations before
    // writing to disk:
    //
    //  1. only KV pairs in the *inclusive* range (`[start, end]`) are used. (set
    //     the range to `["", ""]` to import everything).
    //  2. keys are rewritten according to the given rewrite rule.
    //
    // Both the range and rewrite keys are specified using origin keys. However,
    // the SST itself should be data keys (contain the `z` prefix). The range
    // should be specified using keys after rewriting, to be consistent with the
    // region info in PD.
    //
    // This method returns the *inclusive* key range (`[start, end]`) of SST
    // file created, or returns None if the SST is empty.
    pub async fn download_ext(
        &self,
        meta: &SstMeta,
        backend: &StorageBackend,
        name: &str,
        rewrite_rule: &RewriteRule,
        crypter: Option<CipherInfo>,
        speed_limiter: Limiter,
        engine: E,
        ext: DownloadExt<'_>,
    ) -> Result<Option<Range>> {
        debug!("download start";
            "meta" => ?meta,
            "url" => ?backend,
            "name" => name,
            "rewrite_rule" => ?rewrite_rule,
            "speed_limit" => speed_limiter.speed_limit(),
        );
        let r = self.do_download_ext(
            meta,
            backend,
            name,
            rewrite_rule,
            crypter,
            &speed_limiter,
            engine,
            ext,
        );
        match r.await {
            Ok(r) => {
                info!("download"; "meta" => ?meta, "name" => name, "range" => ?r);
                Ok(r)
            }
            Err(e) => {
                error!(%e; "download failed"; "meta" => ?meta, "name" => name,);
                Err(e)
            }
        }
    }

    pub fn enter_normal_mode(&self, db: E, mf: RocksDbMetricsFn) -> Result<bool> {
        if let Either::Left(ref switcher) = self.switcher {
            switcher.enter_normal_mode(&db, mf)
        } else {
            unreachable!();
        }
    }

    pub fn enter_import_mode(&self, db: E, mf: RocksDbMetricsFn) -> Result<bool> {
        if let Either::Left(ref switcher) = self.switcher {
            switcher.enter_import_mode(&db, mf)
        } else {
            unreachable!();
        }
    }

    pub fn get_mode(&self) -> SwitchMode {
        if let Either::Left(ref switcher) = self.switcher {
            switcher.get_mode()
        } else {
            // v2 should use region_in_import_mode/range_in_import_mode to check regional
            // mode
            SwitchMode::Normal
        }
    }

    #[cfg(test)]
    fn download_file_from_external_storage(
        &self,
        file_length: u64,
        src_file_name: &str,
        dst_file: PathBuf,
        backend: &StorageBackend,
        speed_limiter: &Limiter,
        restore_config: RestoreConfig,
    ) -> Result<()> {
        self._download_rt
            .block_on(self.async_download_file_from_external_storage(
                file_length,
                src_file_name,
                dst_file,
                backend,
                speed_limiter,
                "",
                restore_config,
            ))
    }

    /// Create an external storage by the backend, and cache it with the key.
    /// If the cache exists, return it directly.
    pub fn external_storage_or_cache(
        &self,
        backend: &StorageBackend,
        cache_id: &str,
    ) -> Result<Arc<dyn ExternalStorage>> {
        // prepare to download the file from the external_storage
        // TODO: pass a config to support hdfs
        let ext_storage = if cache_id.is_empty() {
            EXT_STORAGE_CACHE_COUNT.with_label_values(&["skip"]).inc();
            let s = external_storage::create_storage(backend, Default::default())?;
            Arc::from(s)
        } else {
            self.cached_storage.cached_or_create(cache_id, backend)?
        };
        Ok(ext_storage)
    }

    async fn async_download_file_from_external_storage(
        &self,
        file_length: u64,
        src_file_name: &str,
        dst_file: PathBuf,
        backend: &StorageBackend,
        speed_limiter: &Limiter,
        cache_key: &str,
        restore_config: RestoreConfig,
    ) -> Result<()> {
        let start_read = Instant::now();
        if let Some(p) = dst_file.parent() {
            file_system::create_dir_all(p).or_else(|e| {
                if e.kind() == ErrorKind::AlreadyExists {
                    Ok(())
                } else {
                    Err(e)
                }
            })?;
        }

        let ext_storage = self.external_storage_or_cache(backend, cache_key)?;
        let ext_storage = self.auto_encrypt_local_file_if_needed(ext_storage);

        let result = ext_storage
            .restore(
                src_file_name,
                dst_file.clone(),
                file_length,
                speed_limiter,
                restore_config,
            )
            .await;
        IMPORTER_DOWNLOAD_BYTES.observe(file_length as _);
        result.map_err(|e| Error::CannotReadExternalStorage {
            url: util::url_for(&ext_storage),
            name: src_file_name.to_owned(),
            local_path: dst_file.clone(),
            err: e,
        })?;

        OpenOptions::new()
            .append(true)
            .open(dst_file)?
            .sync_data()?;

        IMPORTER_DOWNLOAD_DURATION
            .with_label_values(&["read"])
            .observe(start_read.saturating_elapsed().as_secs_f64());

        debug!("successfully download the file";
            "name" => src_file_name,
            "url"  => %util::url_for(&ext_storage),
        );
        Ok(())
    }

    pub fn update_config_memory_use_ratio(&self, cfg_mgr: &ImportConfigManager) {
        let mem_ratio = cfg_mgr.rl().memory_use_ratio;
        let memory_limit = Self::calcualte_usage_mem(mem_ratio) as usize;

        if self.memory_quota.capacity() != memory_limit {
            self.memory_quota.set_capacity(memory_limit);
            info!("update importer config";
                "memory_use_ratio" => mem_ratio,
                "size" => memory_limit,
            )
        }
    }

    pub fn shrink_by_tick(&self) -> usize {
        let mut shrink_buff_size: usize = 0;
        let mut retain_buff_size: usize = 0;
        let mut shrink_files: Vec<PathBuf> = Vec::default();
        let mut retain_file_count = 0_usize;

        self.file_locks.retain(|_, (c, start)| {
            let mut need_retain = true;
            match c {
                CacheKvFile::Mem(buff) => {
                    let buflen = buff.get().map(|v| v.content.len()).unwrap_or_default();
                    // The term of recycle memeory is 60s.
                    if c.ref_count() == 1 && c.is_expired(start) {
                        CACHE_EVENT.with_label_values(&["remove"]).inc();
                        need_retain = false;
                        shrink_buff_size += buflen;
                    } else {
                        retain_buff_size += buflen;
                    }
                }
                CacheKvFile::Fs(path) => {
                    let p = path.to_path_buf();
                    // The term of recycle file is 10min.
                    if c.ref_count() == 1 && c.is_expired(start) {
                        need_retain = false;
                        shrink_files.push(p);
                    } else {
                        retain_file_count += 1;
                    }
                }
            }

            need_retain
        });

        CACHED_FILE_IN_MEM.set(self.memory_quota.capacity() as _);

        if self.download_to_disk_only() {
            let shrink_file_count = shrink_files.len();
            if shrink_file_count > 0 || retain_file_count > 0 {
                info!("shrink space by tick"; "shrink_files_count" => shrink_file_count, "retain_files_count" => retain_file_count);
            }

            for f in shrink_files {
                self.remove_file_no_throw(&f);
            }
            shrink_file_count
        } else {
            if shrink_buff_size > 0 || retain_buff_size > 0 {
                info!("shrink cache by tick"; "shrink_size" => shrink_buff_size, "retain_size" => retain_buff_size);
            }
            shrink_buff_size
        }
    }

    pub fn download_to_disk_only(&self) -> bool {
        self.memory_quota.capacity() == 0
    }

    fn request_memory(&self, meta: &KvMeta) -> Option<OwnedAllocated> {
        let size = meta.get_length();
        let mut permit = OwnedAllocated::new(self.memory_quota.clone());
        // If the memory is limited, roll backup the memory_quota and return false.
        if permit.alloc(size as _).is_err() {
            CACHE_EVENT.with_label_values(&["out-of-quota"]).inc();
            None
        } else {
            CACHE_EVENT.with_label_values(&["add"]).inc();
            Some(permit)
        }
    }

    async fn download_kv_file_to_mem_buf(
        &self,
        meta: &KvMeta,
        ext_storage: Arc<dyn ExternalStorage>,
        speed_limiter: &Limiter,
        opt_file_encryption_info: Option<FileEncryptionInfo>,
        opt_encrypted_file_checksum: Option<Vec<u8>>,
    ) -> Result<LoadedFile> {
        let start = Instant::now();
        let permit = self
            .request_memory(meta)
            .ok_or_else(|| Error::ResourceNotEnough(String::from("memory is limited")))?;

        let expected_sha256 = {
            let sha256 = meta.get_sha256().to_vec();
            if !sha256.is_empty() {
                Some(sha256)
            } else {
                None
            }
        };
        let file_length = meta.get_length();
        let range = {
            let range_length = meta.get_range_length();
            if range_length == 0 {
                None
            } else {
                Some((meta.get_range_offset(), range_length))
            }
        };
        let restore_config = RestoreConfig {
            range,
            compression_type: Some(meta.get_compression_type()),
            expected_plaintext_file_checksum: expected_sha256,
            file_crypter: opt_file_encryption_info,
            opt_encrypted_file_checksum,
        };

        let buff = self
            .download_kv_files_from_external_storage_to_mem(
                file_length,
                meta.get_name(),
                ext_storage,
                speed_limiter,
                restore_config,
            )
            .await?;

        IMPORTER_DOWNLOAD_BYTES.observe(file_length as _);
        IMPORTER_APPLY_DURATION
            .with_label_values(&["exec_download"])
            .observe(start.saturating_elapsed().as_secs_f64());

        Ok(LoadedFile {
            content: Arc::from(buff.into_boxed_slice()),
            _permit: permit,
        })
    }

    pub async fn download_kv_file_to_mem_cache(
        &self,
        meta: &KvMeta,
        ext_storage: Arc<dyn ExternalStorage>,
        speed_limiter: &Limiter,
        opt_file_encryption_info: Option<FileEncryptionInfo>,
        opt_encrypted_file_checksum: Option<Vec<u8>>,
    ) -> Result<CacheKvFile> {
        let start = Instant::now();
        let dst_name = format!("{}_{}", meta.get_name(), meta.get_range_offset());

        let cache = {
            let lock = self.file_locks.entry(dst_name);
            IMPORTER_APPLY_DURATION
                .with_label_values(&["download-get-lock"])
                .observe(start.saturating_elapsed().as_secs_f64());

            match lock {
                Entry::Occupied(mut ent) => match ent.get_mut() {
                    (CacheKvFile::Mem(buff), last_used) => {
                        *last_used = Instant::now();
                        Arc::clone(buff)
                    }
                    _ => {
                        return Err(error(concat!(
                            "using both read-to-memory and download-to-file is unacceptable for now.",
                            "(If you think it is possible in the future you are reading this, ",
                            "please change this line to `return item.get.0.clone()`)",
                            "(Please also check the state transform is OK too.)",
                        )));
                    }
                },
                Entry::Vacant(ent) => {
                    let cache = Arc::new(OnceCell::new());
                    ent.insert((CacheKvFile::Mem(Arc::clone(&cache)), Instant::now()));
                    cache
                }
            }
        };

        if cache.initialized() {
            CACHE_EVENT.with_label_values(&["hit"]).inc();
        }

        cache
            .get_or_try_init(|| {
                self.download_kv_file_to_mem_buf(
                    meta,
                    ext_storage,
                    speed_limiter,
                    opt_file_encryption_info,
                    opt_encrypted_file_checksum,
                )
            })
            .await?;
        Ok(CacheKvFile::Mem(cache))
    }

    pub fn auto_encrypt_local_file_if_needed(
        &self,
        ext_storage: Arc<dyn ExternalStorage>,
    ) -> Arc<dyn ExternalStorage> {
        if let Some(key_manager) = self.key_manager.clone() {
            Arc::new(
                external_storage::AutoEncryptLocalRestoredFileExternalStorage {
                    key_manager,
                    storage: ext_storage,
                },
            )
        } else {
            ext_storage
        }
    }

    async fn download_kv_files_from_external_storage_to_mem(
        &self,
        file_length: u64,
        file_name: &str,
        ext_storage: Arc<dyn ExternalStorage>,
        speed_limiter: &Limiter,
        restore_config: RestoreConfig,
    ) -> Result<Vec<u8>> {
        let RestoreConfig {
            range,
            compression_type,
            expected_plaintext_file_checksum: expected_sha256,
            file_crypter,
            opt_encrypted_file_checksum,
        } = restore_config;

        let (mut reader, opt_hasher) = {
            let inner = if let Some((off, len)) = range {
                ext_storage.read_part(file_name, off, len)
            } else {
                ext_storage.read(file_name)
            };

            // wrap with checksum reader if needed
            //
            let (checksum_reader, opt_hasher) =
                wrap_with_checksum_reader_if_needed(opt_encrypted_file_checksum.is_some(), inner)?;

            // wrap with decrypter if needed
            //
            let encrypted_reader = encrypt_wrap_reader(file_crypter, checksum_reader)?;

            (
                compression_reader_dispatcher(compression_type, encrypted_reader)?,
                opt_hasher,
            )
        };

        let r = external_storage::read_external_storage_info_buff(
            &mut reader,
            speed_limiter,
            file_length,
            expected_sha256,
            external_storage::MIN_READ_SPEED,
            opt_encrypted_file_checksum,
            opt_hasher,
        )
        .await;
        let url = ext_storage.url()?.to_string();
        let buff = r.map_err(|e| Error::CannotReadExternalStorage {
            url: url.to_string(),
            name: file_name.to_string(),
            err: e,
            local_path: PathBuf::default(),
        })?;

        Ok(buff)
    }

    pub async fn download_kv_file(
        &self,
        meta: &KvMeta,
        ext_storage: Arc<dyn ExternalStorage>,
        backend: &StorageBackend,
        speed_limiter: &Limiter,
        opt_cipher_info: Option<CipherInfo>,
        master_keys_proto: Vec<MasterKey>,
    ) -> Result<Arc<[u8]>> {
        // update the master key backends if needed.
        //
        self.multi_master_keys_backend
            .update_from_proto_if_needed(master_keys_proto, create_async_backend)
            .await?;

        // extract backup file encryption info if configured
        //
        let opt_file_encryption_info = self
            .extract_file_encryption_info(meta, opt_cipher_info)
            .await?;
        let opt_checksum = extract_checksum_info(meta);

        let c = if self.download_to_disk_only() {
            self.download_kv_file_to_disk(
                meta,
                backend,
                speed_limiter,
                opt_file_encryption_info,
                opt_checksum,
            )
            .await?
        } else {
            self.download_kv_file_to_mem_cache(
                meta,
                ext_storage,
                speed_limiter,
                opt_file_encryption_info,
                opt_checksum,
            )
            .await?
        };
        match c {
            // If cache in memory, it has been rewrite, and content is plaintext,
            // return buffer directly.
            CacheKvFile::Mem(buff) => Ok(Arc::clone(
                &buff
                    .get()
                    .ok_or_else(|| error("invalid cache state"))?
                    .content,
            )),
            // If cache in a file, it needs to read and rewrite, and it is locally encrypted if
            // data key manager is configured
            CacheKvFile::Fs(path) => {
                let mut buffer = Vec::new();
                if let Some(key_manager) = self.key_manager.clone() {
                    let mut decrypter_reader = key_manager.open_file_for_read(path.as_ref())?;
                    decrypter_reader.read_to_end(&mut buffer)?;
                } else {
                    let file = File::open(path.as_ref())?;
                    let mut reader = BufReader::new(file);
                    reader.read_to_end(&mut buffer)?;
                }
                Ok(Arc::from(buffer.into_boxed_slice()))
            }
        }
    }

    pub async fn download_kv_file_to_disk(
        &self,
        meta: &KvMeta,
        backend: &StorageBackend,
        speed_limiter: &Limiter,
        opt_file_encryption_info: Option<FileEncryptionInfo>,
        opt_encrypted_file_checksum: Option<Vec<u8>>,
    ) -> Result<CacheKvFile> {
        let offset = meta.get_range_offset();
        let src_name = meta.get_name();
        let dst_name = format!("{}_{}", src_name, offset);
        let path = self.dir.get_import_path(&dst_name)?;
        let start = Instant::now();
        let plaintext_file_checksum = meta.get_sha256().to_vec();
        let expected_plaintext_checksum = if !plaintext_file_checksum.is_empty() {
            Some(plaintext_file_checksum)
        } else {
            None
        };

        let mut lock = self
            .file_locks
            .entry(dst_name)
            .or_insert((CacheKvFile::Fs(Arc::new(path.save.clone())), Instant::now()));

        if path.save.exists() {
            lock.1 = Instant::now();
            return Ok(lock.0.clone());
        }

        let range_length = meta.get_range_length();
        let range = if range_length == 0 {
            None
        } else {
            Some((offset, range_length))
        };

        let restore_config = RestoreConfig {
            range,
            compression_type: Some(meta.compression_type),
            expected_plaintext_file_checksum: expected_plaintext_checksum,
            file_crypter: opt_file_encryption_info,
            opt_encrypted_file_checksum,
        };

        self.async_download_file_from_external_storage(
            meta.get_length(),
            src_name,
            path.temp.clone(),
            backend,
            speed_limiter,
            "",
            restore_config,
        )
        .await?;
        info!(
            "download file finished {}, offset {}, length {}",
            src_name,
            offset,
            meta.get_length()
        );

        if let Some(p) = path.save.parent() {
            // we have v1 prefix in file name.
            file_system::create_dir_all(p).or_else(|e| {
                if e.kind() == ErrorKind::AlreadyExists {
                    Ok(())
                } else {
                    Err(e)
                }
            })?;
        }

        if let Some(manager) = self.key_manager.clone() {
            manager.rename_file(&path.temp, &path.save)?;
        } else {
            file_system::rename(path.temp.clone(), path.save.clone())?;
        }

        IMPORTER_APPLY_DURATION
            .with_label_values(&["download"])
            .observe(start.saturating_elapsed().as_secs_f64());

        lock.1 = Instant::now();
        Ok(lock.0.clone())
    }

    pub fn rewrite_kv_file(
        &self,
        file_buff: Vec<u8>,
        rewrite_rule: &RewriteRule,
    ) -> Result<Vec<u8>> {
        let old_prefix = rewrite_rule.get_old_key_prefix();
        let new_prefix = rewrite_rule.get_new_key_prefix();
        // if old_prefix equals new_prefix, do not need rewrite.
        if old_prefix == new_prefix {
            return Ok(file_buff);
        }

        // perform iteration and key rewrite.
        let mut new_buff = Vec::with_capacity(file_buff.len());
        let mut event_iter = EventIterator::with_rewriting(
            file_buff.as_slice(),
            rewrite_rule.get_old_key_prefix(),
            rewrite_rule.get_new_key_prefix(),
        );
        let mut key = new_prefix.to_vec();
        let new_prefix_data_key_len = key.len();

        let start = Instant::now();
        loop {
            if !event_iter.valid() {
                break;
            }
            event_iter.next()?;

            // perform rewrite
            let old_key = event_iter.key();
            if !old_key.starts_with(old_prefix) {
                return Err(Error::WrongKeyPrefix {
                    what: "Key in file",
                    key: old_key.to_vec(),
                    prefix: old_prefix.to_vec(),
                });
            }
            key.truncate(new_prefix_data_key_len);
            key.extend_from_slice(&old_key[old_prefix.len()..]);
            let value = event_iter.value();

            let encoded = EventEncoder::encode_event(&key, value);
            for slice in encoded {
                new_buff.append(&mut slice.as_ref().to_owned());
            }
        }

        IMPORTER_APPLY_DURATION
            .with_label_values(&["rewrite"])
            .observe(start.saturating_elapsed().as_secs_f64());
        Ok(new_buff)
    }

    pub fn do_apply_kv_file(
        &self,
        start_key: &[u8],
        end_key: &[u8],
        start_ts: u64,
        restore_ts: u64,
        file_buff: Arc<[u8]>,
        rewrite_rule: &RewriteRule,
        mut build_fn: impl FnMut(Vec<u8>, Vec<u8>),
    ) -> Result<Option<Range>> {
        let mut event_iter = EventIterator::with_rewriting(
            file_buff.as_ref(),
            rewrite_rule.get_old_key_prefix(),
            rewrite_rule.get_new_key_prefix(),
        );
        let mut smallest_key = None;
        let mut largest_key = None;
        let mut total_key = 0;
        let mut ts_not_expected = 0;
        let mut not_in_range = 0;
        let start = Instant::now();

        loop {
            if !event_iter.valid() {
                break;
            }
            total_key += 1;
            event_iter.next()?;
            INPORTER_APPLY_COUNT.with_label_values(&["key_meet"]).inc();

            if !event_iter
                .key()
                .starts_with(rewrite_rule.get_new_key_prefix())
            {
                return Err(Error::WrongKeyPrefix {
                    what: "do_apply_kv_file",
                    key: event_iter.key().to_vec(),
                    prefix: rewrite_rule.get_old_key_prefix().to_vec(),
                });
            }
            let key = event_iter.key().to_vec();
            let value = event_iter.value().to_vec();
            let ts = Key::decode_ts_from(&key)?;
            if ts < TimeStamp::new(start_ts) || ts > TimeStamp::new(restore_ts) {
                // we assume the keys in file are sorted by ts.
                // so if we met the key not satisfy the ts.
                // we can easily filter the remain keys.
                ts_not_expected += 1;
                continue;
            }
            if check_key_in_range(&key, 0, start_key, end_key).is_err() {
                // key not in range, we can simply skip this key here.
                // the client make sure the correct region will download and apply the same
                // file.
                INPORTER_APPLY_COUNT
                    .with_label_values(&["key_not_in_region"])
                    .inc();
                not_in_range += 1;
                continue;
            }

            build_fn(key.clone(), value);
            smallest_key = smallest_key
                .map_or_else(|| Some(key.clone()), |v: Vec<u8>| Some(v.min(key.clone())));
            largest_key = largest_key
                .map_or_else(|| Some(key.clone()), |v: Vec<u8>| Some(v.max(key.clone())));
        }
        if not_in_range != 0 || ts_not_expected != 0 {
            info!("build download request file done";
                "total_keys" => %total_key,
                "ts_filtered_keys" => %ts_not_expected,
                "range_filtered_keys" => %not_in_range);
        }

        IMPORTER_APPLY_DURATION
            .with_label_values(&["normal"])
            .observe(start.saturating_elapsed().as_secs_f64());

        match (smallest_key, largest_key) {
            (Some(sk), Some(lk)) => {
                let mut final_range = Range::default();
                final_range.set_start(sk);
                final_range.set_end(lk);
                Ok(Some(final_range))
            }
            _ => Ok(None),
        }
    }

    // raw download, without ext, compatibility to old tests.
    #[cfg(test)]
    fn download(
        &self,
        meta: &SstMeta,
        backend: &StorageBackend,
        name: &str,
        rewrite_rule: &RewriteRule,
        crypter: Option<CipherInfo>,
        speed_limiter: Limiter,
        engine: E,
    ) -> Result<Option<Range>> {
        self._download_rt.block_on(self.download_ext(
            meta,
            backend,
            name,
            rewrite_rule,
            crypter,
            speed_limiter,
            engine,
            DownloadExt::default(),
        ))
    }

    async fn do_download_ext(
        &self,
        meta: &SstMeta,
        backend: &StorageBackend,
        name: &str,
        rewrite_rule: &RewriteRule,
        crypter: Option<CipherInfo>,
        speed_limiter: &Limiter,
        engine: E,
        ext: DownloadExt<'_>,
    ) -> Result<Option<Range>> {
        let path = self.dir.join_for_write(meta)?;

        let file_crypter = crypter.map(|c| FileEncryptionInfo {
            method: c.cipher_type,
            key: c.cipher_key,
            iv: meta.cipher_iv.to_owned(),
        });

        let restore_config = RestoreConfig {
            file_crypter,
            ..Default::default()
        };

        self.async_download_file_from_external_storage(
            meta.length,
            name,
            path.temp.clone(),
            backend,
            speed_limiter,
            ext.cache_key.unwrap_or(""),
            restore_config,
        )
        .await?;

        // now validate the SST file.
        let dst_file_name = path.temp.to_str().unwrap();
        let sst_reader = E::SstReader::open(dst_file_name, self.key_manager.clone())?;
        sst_reader.verify_checksum()?;

        // undo key rewrite so we could compare with the keys inside SST
        let old_prefix = rewrite_rule.get_old_key_prefix();
        let new_prefix = rewrite_rule.get_new_key_prefix();
        let req_type = ext.req_type;

        debug!("downloaded file and verified";
            "meta" => ?meta,
            "name" => name,
            "path" => dst_file_name,
            "old_prefix" => log_wrappers::Value::key(old_prefix),
            "new_prefix" => log_wrappers::Value::key(new_prefix),
            "req_type" => ?req_type,
        );

        let range_start = meta.get_range().get_start();
        let range_end = meta.get_range().get_end();
        let range_start_bound = key_to_bound(range_start);
        let range_end_bound = if meta.get_end_key_exclusive() {
            key_to_exclusive_bound(range_end)
        } else {
            key_to_bound(range_end)
        };

        let mut range_start =
            keys::rewrite::rewrite_prefix_of_start_bound(new_prefix, old_prefix, range_start_bound)
                .map_err(|_| Error::WrongKeyPrefix {
                    what: "SST start range",
                    key: range_start.to_vec(),
                    prefix: new_prefix.to_vec(),
                })?;
        let mut range_end =
            keys::rewrite::rewrite_prefix_of_end_bound(new_prefix, old_prefix, range_end_bound)
                .map_err(|_| Error::WrongKeyPrefix {
                    what: "SST end range",
                    key: range_end.to_vec(),
                    prefix: new_prefix.to_vec(),
                })?;

        if req_type == DownloadRequestType::Keyspace {
            range_start = keys::rewrite::encode_bound(range_start);
            range_end = keys::rewrite::encode_bound(range_end);
        }

        let start_rename_rewrite = Instant::now();
        // read the first and last keys from the SST, determine if we could
        // simply move the entire SST instead of iterating and generate a new one.
        let mut iter = sst_reader.iter(IterOptions::default())?;
        let direct_retval = (|| -> Result<Option<_>> {
            if rewrite_rule.old_key_prefix != rewrite_rule.new_key_prefix
                || rewrite_rule.new_timestamp != 0
            {
                // must iterate if we perform key rewrite
                return Ok(None);
            }
            if !iter.seek_to_first()? {
                let mut range = meta.get_range().clone();
                if req_type == DownloadRequestType::Keyspace {
                    *range.mut_start() = encode_bytes(&range.take_start());
                    *range.mut_end() = encode_bytes(&range.take_end());
                }
                // the SST is empty, so no need to iterate at all (should be impossible?)
                return Ok(Some(range));
            }

            let start_key = keys::origin_key(iter.key());
            if is_before_start_bound(start_key, &range_start) {
                // SST's start is before the range to consume, so needs to iterate to skip over
                return Ok(None);
            }
            let start_key = start_key.to_vec();

            // seek to end and fetch the last (inclusive) key of the SST.
            iter.seek_to_last()?;
            let last_key = keys::origin_key(iter.key());
            if is_after_end_bound(last_key, &range_end) {
                // SST's end is after the range to consume
                return Ok(None);
            }

            // range contained the entire SST, no need to iterate, just moving the file is
            // ok
            let mut range = Range::default();
            range.set_start(start_key);
            range.set_end(last_key.to_vec());
            Ok(Some(range))
        })()?;

        if let Some(range) = direct_retval {
            if let Some(key_manager) = &self.key_manager {
                let temp_str = path
                    .temp
                    .to_str()
                    .ok_or_else(|| Error::InvalidSstPath(path.temp.clone()))?;
                let save_str = path
                    .save
                    .to_str()
                    .ok_or_else(|| Error::InvalidSstPath(path.save.clone()))?;
                key_manager.link_file(temp_str, save_str)?;
                let r = file_system::rename(&path.temp, &path.save);
                let del_file = if r.is_ok() { temp_str } else { save_str };
                if let Err(e) = key_manager.delete_file(del_file, None) {
                    warn!("fail to remove encryption metadata during 'do_download'"; "err" => ?e);
                }
                r?;
            } else {
                file_system::rename(&path.temp, &path.save)?;
            }
            IMPORTER_DOWNLOAD_DURATION
                .with_label_values(&["rename"])
                .observe(start_rename_rewrite.saturating_elapsed().as_secs_f64());
            return Ok(Some(range));
        }

        // perform iteration and key rewrite.
        let mut data_key = keys::DATA_PREFIX_KEY.to_vec();
        let data_key_prefix_len = keys::DATA_PREFIX_KEY.len();
        let mut user_key = new_prefix.to_vec();
        let user_key_prefix_len = new_prefix.len();
        let mut first_key = None;

        match range_start {
            Bound::Unbounded => iter.seek_to_first()?,
            Bound::Included(s) => iter.seek(&keys::data_key(&s))?,
            Bound::Excluded(_) => unreachable!(),
        };
        // SST writer must not be opened in gRPC threads, because it may be
        // blocked for a long time due to IO, especially, when encryption at rest
        // is enabled, and it leads to gRPC keepalive timeout.
        let cf_name = name_to_cf(meta.get_cf_name()).unwrap();
        let mut sst_writer = <E as SstExt>::SstWriterBuilder::new()
            .set_db(&engine)
            .set_cf(cf_name)
            .set_compression_type(self.compression_types.get(cf_name).copied())
            .build(path.save.to_str().unwrap())
            .unwrap();

        let mut yield_check =
            RescheduleChecker::new(tokio::task::yield_now, Duration::from_millis(10));
        let mut count = 0;
        while iter.valid()? {
            let mut old_key = Cow::Borrowed(keys::origin_key(iter.key()));
            let mut ts = None;

            if is_after_end_bound(old_key.as_ref(), &range_end) {
                break;
            }

            if req_type == DownloadRequestType::Keyspace {
                ts = Some(Key::decode_ts_bytes_from(old_key.as_ref())?.to_owned());
                old_key = {
                    let mut key = old_key.to_vec();
                    decode_bytes_in_place(&mut key, false)?;
                    Cow::Owned(key)
                };
            }

            if !old_key.starts_with(old_prefix) {
                return Err(Error::WrongKeyPrefix {
                    what: "Key in SST",
                    key: keys::origin_key(iter.key()).to_vec(),
                    prefix: old_prefix.to_vec(),
                });
            }

            data_key.truncate(data_key_prefix_len);
            user_key.truncate(user_key_prefix_len);
            user_key.extend_from_slice(&old_key[old_prefix.len()..]);
            if req_type == DownloadRequestType::Keyspace {
                data_key.extend(encode_bytes(&user_key));
                data_key.extend(ts.unwrap());
            } else {
                data_key.extend_from_slice(&user_key);
            }

            let mut value = Cow::Borrowed(iter.value());

            if rewrite_rule.new_timestamp != 0 {
                data_key = Key::from_encoded(data_key)
                    .truncate_ts()
                    .map_err(|e| {
                        Error::BadFormat(format!(
                            "key {}: {}",
                            log_wrappers::Value::key(keys::origin_key(iter.key())),
                            e
                        ))
                    })?
                    .append_ts(TimeStamp::new(rewrite_rule.new_timestamp))
                    .into_encoded();
                if meta.get_cf_name() == CF_WRITE {
                    let mut write = WriteRef::parse(iter.value()).map_err(|e| {
                        Error::BadFormat(format!(
                            "write {}: {}",
                            log_wrappers::Value::key(keys::origin_key(iter.key())),
                            e
                        ))
                    })?;
                    write.start_ts = TimeStamp::new(rewrite_rule.new_timestamp);
                    value = Cow::Owned(write.to_bytes());
                }
            }

            sst_writer.put(&data_key, &value)?;
            count += 1;
            if count >= 1024 {
                count = 0;
                yield_check.check().await;
            }
            iter.next()?;
            if first_key.is_none() {
                first_key = Some(keys::origin_key(&data_key).to_vec());
            }
        }

        self.remove_file_no_throw(&path.temp);

        IMPORTER_DOWNLOAD_DURATION
            .with_label_values(&["rewrite"])
            .observe(start_rename_rewrite.saturating_elapsed().as_secs_f64());

        if let Some(start_key) = first_key {
            let start_finish = Instant::now();
            sst_writer.finish()?;
            IMPORTER_DOWNLOAD_DURATION
                .with_label_values(&["finish"])
                .observe(start_finish.saturating_elapsed().as_secs_f64());

            let mut final_range = Range::default();
            final_range.set_start(start_key);
            final_range.set_end(keys::origin_key(&data_key).to_vec());
            Ok(Some(final_range))
        } else {
            // nothing is written: prevents finishing the SST at all.
            // also delete the empty sst file that is created when creating sst_writer
            drop(sst_writer);
            self.remove_file_no_throw(&path.save);
            Ok(None)
        }
    }

    /// List the basic information of the current SST files.
    /// The information contains UUID, region ID, region Epoch, api version,
    /// last modified time. Other fields may be left blank.
    pub fn list_ssts(&self) -> Result<Vec<(SstMeta, i32, SystemTime)>> {
        self.dir.list_ssts()
    }

    pub fn new_txn_writer(&self, db: &E, meta: SstMeta) -> Result<TxnSstWriter<E>> {
        let mut default_meta = meta.clone();
        default_meta.set_cf_name(CF_DEFAULT.to_owned());
        let default_path = self.dir.join_for_write(&default_meta)?;
        let default = E::SstWriterBuilder::new()
            .set_db(db)
            .set_cf(CF_DEFAULT)
            .set_compression_type(self.compression_types.get(CF_DEFAULT).copied())
            .build(default_path.temp.to_str().unwrap())
            .unwrap();

        let mut write_meta = meta;
        write_meta.set_cf_name(CF_WRITE.to_owned());
        let write_path = self.dir.join_for_write(&write_meta)?;
        let write = E::SstWriterBuilder::new()
            .set_db(db)
            .set_cf(CF_WRITE)
            .set_compression_type(self.compression_types.get(CF_WRITE).copied())
            .build(write_path.temp.to_str().unwrap())
            .unwrap();

        Ok(TxnSstWriter::new(
            default,
            write,
            default_path,
            write_path,
            default_meta,
            write_meta,
            self.key_manager.clone(),
            self.api_version,
        ))
    }

    pub fn new_raw_writer(&self, db: &E, mut meta: SstMeta) -> Result<RawSstWriter<E>> {
        meta.set_cf_name(CF_DEFAULT.to_owned());
        let default_path = self.dir.join_for_write(&meta)?;
        let default = E::SstWriterBuilder::new()
            .set_db(db)
            .set_cf(CF_DEFAULT)
            .build(default_path.temp.to_str().unwrap())
            .unwrap();
        Ok(RawSstWriter::new(
            default,
            default_path,
            meta,
            self.key_manager.clone(),
            self.api_version,
        ))
    }

    async fn extract_file_encryption_info(
        &self,
        kv_meta: &KvMeta,
        opt_cipher_info: Option<CipherInfo>,
    ) -> Result<Option<FileEncryptionInfo>> {
        if let Some(encryption_info) = kv_meta.file_encryption_info.as_ref() {
            if let Some(encryption_info_mode) = &encryption_info.mode {
                match encryption_info_mode {
                    FileEncryptionInfo_oneof_mode::PlainTextDataKey(_) => {
                        if let Some(cipher_info) = opt_cipher_info {
                            if cipher_info.cipher_type == EncryptionMethod::Unknown
                                || cipher_info.cipher_type == EncryptionMethod::Plaintext
                            {
                                return Err(error(
                                    "plaintext data key needed from client but plaintext or unknown provided",
                                ));
                            }
                            Ok(Some(FileEncryptionInfo {
                                method: cipher_info.cipher_type,
                                key: cipher_info.cipher_key,
                                iv: encryption_info.file_iv.clone(),
                            }))
                        } else {
                            Err(error(
                                "plaintext data key needed from client but not provided",
                            ))
                        }
                    }
                    FileEncryptionInfo_oneof_mode::MasterKeyBased(parsed_master_key_info) => {
                        // sanity check
                        if self.multi_master_keys_backend.is_initialized().await {
                            // decrypt encrypted data key
                            if parsed_master_key_info.data_key_encrypted_content.is_empty() {
                                return Err(error(
                                    "internal error: couldn't find any encrypted data key information for log backup file",
                                ));
                            }
                            // get the first key for the current impl
                            // the field is a list for future extension
                            // when multiple master key backends are provided for high availability.
                            let plaintext_data_key = self
                                .multi_master_keys_backend
                                .decrypt(
                                    parsed_master_key_info
                                        .data_key_encrypted_content
                                        .first()
                                        .unwrap(),
                                )
                                .await
                                .map_err(|e| {
                                    error(format!("failed to decrypt encrypted data key: {:?}", e))
                                })?;
                            Ok(Some(FileEncryptionInfo {
                                method: encryption_info.encryption_method,
                                key: plaintext_data_key,
                                iv: encryption_info.file_iv.clone(),
                            }))
                        } else {
                            Err(error(
                                "internal error: need to decrypt data key but multi master key backends is not initialized",
                            ))
                        }
                    }
                }
            } else {
                // encryption info set but empty, should never happen
                Err(error(
                    "internal error: encryption information is set in the kv file but empty, should never happen",
                ))
            }
        } else {
            // doesn't have encryption info, plaintext log backup files.
            Ok(None)
        }
    }

    fn remove_file_no_throw(&self, path_buf: &PathBuf) {
        // remove from file system
        if let Err(e) = file_system::remove_file(path_buf) {
            warn!("failed to remove file"; "filename" => ?path_buf, "error" => ?e);
        }
        // remove tracking from key manager if needed
        if let Some(key_manager) = self.key_manager.as_ref() {
            if let Err(e) = key_manager.delete_file(&path_buf.to_string_lossy(), None) {
                warn!("failed to remove file from key manager"; "filename" => ?path_buf, "error" => ?e);
            }
        }
    }
}

fn extract_checksum_info(kv_meta: &KvMeta) -> Option<Vec<u8>> {
    if let Some(encryption_info) = kv_meta.file_encryption_info.as_ref() {
        if encryption_info.checksum.is_empty() {
            None
        } else {
            Some(encryption_info.checksum.clone())
        }
    } else {
        None
    }
}

fn key_to_bound(key: &[u8]) -> Bound<&[u8]> {
    if key.is_empty() {
        Bound::Unbounded
    } else {
        Bound::Included(key)
    }
}

fn key_to_exclusive_bound(key: &[u8]) -> Bound<&[u8]> {
    if key.is_empty() {
        Bound::Unbounded
    } else {
        Bound::Excluded(key)
    }
}

fn is_before_start_bound<K: AsRef<[u8]>>(value: &[u8], bound: &Bound<K>) -> bool {
    match bound {
        Bound::Unbounded => false,
        Bound::Included(b) => *value < *b.as_ref(),
        Bound::Excluded(b) => *value <= *b.as_ref(),
    }
}

fn is_after_end_bound<K: AsRef<[u8]>>(value: &[u8], bound: &Bound<K>) -> bool {
    match bound {
        Bound::Unbounded => false,
        Bound::Included(b) => *value > *b.as_ref(),
        Bound::Excluded(b) => *value >= *b.as_ref(),
    }
}

#[cfg(test)]
mod tests {
    use std::{
        io::{self, Cursor},
        ops::Sub,
        sync::atomic::{AtomicUsize, Ordering},
        usize,
    };

    use async_compression::tokio::write::ZstdEncoder;
    use encryption::{EncrypterWriter, Iv};
    use engine_rocks::get_env;
    use engine_traits::{
        collect, Error as TraitError, ExternalSstFileInfo, Iterable, Iterator, RefIterable,
        SstCompressionType::Zstd, SstReader, SstWriter, CF_DEFAULT, DATA_CFS,
    };
    use external_storage::read_external_storage_info_buff;
    use file_system::Sha256Reader;
    use kvproto::{
        brpb::CompressionType,
        encryptionpb,
        encryptionpb::{EncryptionMethod, MasterKeyBased, MasterKeyFile, PlainTextDataKey},
    };
    use online_config::{ConfigManager, OnlineConfig};
    use openssl::hash::{Hasher, MessageDigest};
    use rand::Rng;
    use tempfile::{Builder, TempDir};
    use test_sst_importer::*;
    use test_util::new_test_key_manager;
    use tikv_util::{
        codec::stream_event::EventEncoder, resizable_threadpool::ResizableRuntime,
        stream::block_on_external_io,
    };
    use tokio::io::{AsyncWrite, AsyncWriteExt};
    use tokio_util::compat::{FuturesAsyncWriteCompatExt, TokioAsyncWriteCompatExt};
    use txn_types::{Value, WriteType};
    use uuid::Uuid;

    use super::*;
    use crate::{import_file::ImportPath, *};

    static COUNTER: AtomicUsize = AtomicUsize::new(0);
    type TokioResult<T> = std::io::Result<T>;

    fn create_tokio_runtime(_: usize, _: &str) -> TokioResult<Runtime> {
        tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
    }

    fn do_test_import_dir(key_manager: Option<Arc<DataKeyManager>>) {
        let temp_dir = Builder::new().prefix("test_import_dir").tempdir().unwrap();
        let dir = ImportDir::new(temp_dir.path()).unwrap();

        let mut meta = SstMeta::default();
        meta.set_uuid(Uuid::new_v4().as_bytes().to_vec());

        let path = dir.join_for_write(&meta).unwrap();

        // Test ImportDir::create()
        {
            let _file = dir.create(&meta, key_manager.clone()).unwrap();
            check_file_exists(&path.temp, key_manager.as_deref());
            check_file_not_exists(&path.save, key_manager.as_deref());
            check_file_not_exists(&path.clone, key_manager.as_deref());

            // Cannot create the same file again.
            dir.create(&meta, key_manager.clone()).unwrap_err();
        }

        // Test ImportDir::delete()
        {
            if let Some(ref manager) = key_manager {
                manager.create_file_for_write(&path.temp).unwrap();
                manager.create_file_for_write(&path.save).unwrap();
                manager.create_file_for_write(&path.clone).unwrap();
            } else {
                File::create(&path.temp).unwrap();
                File::create(&path.save).unwrap();
                File::create(&path.clone).unwrap();
            }

            dir.delete(&meta, key_manager.as_deref()).unwrap();
            check_file_not_exists(&path.temp, key_manager.as_deref());
            check_file_not_exists(&path.save, key_manager.as_deref());
            check_file_not_exists(&path.clone, key_manager.as_deref());
        }

        // Test ImportDir::ingest()

        let db_path = temp_dir.path().join("db");
        let env = get_env(key_manager.clone(), None /* io_rate_limiter */).unwrap();
        let db = new_test_engine_with_env(db_path.to_str().unwrap(), &[CF_DEFAULT], env);

        let cases = [(0, 10), (5, 15), (10, 20), (0, 100)];

        let mut ingested = Vec::new();

        for (i, &range) in cases.iter().enumerate() {
            let path = temp_dir.path().join(format!("{}.sst", i));
            let (meta, data) = gen_sst_file(path, range);

            let mut f = dir.create(&meta, key_manager.clone()).unwrap();
            f.append(&data).unwrap();
            f.finish().unwrap();
            let info = SstMetaInfo {
                total_bytes: 0,
                total_kvs: 0,
                meta: meta.to_owned(),
            };
            let api_version = info.meta.api_version;
            dir.ingest(&[info], &db, key_manager.clone(), api_version)
                .unwrap();
            check_db_range(&db, range);

            ingested.push(meta);
        }

        let ssts = dir.list_ssts().unwrap();
        assert_eq!(ssts.len(), ingested.len());
        for sst in &ssts {
            ingested
                .iter()
                .find(|s| s.get_uuid() == sst.0.get_uuid())
                .unwrap();
            dir.delete(&sst.0, key_manager.as_deref()).unwrap();
        }
        assert!(dir.list_ssts().unwrap().is_empty());
    }

    #[test]
    fn test_import_dir() {
        do_test_import_dir(None);

        let (_tmp_dir, key_manager) = new_key_manager_for_test();
        do_test_import_dir(Some(key_manager));
    }

    fn do_test_import_file(data_key_manager: Option<Arc<DataKeyManager>>) {
        let temp_dir = Builder::new().prefix("test_import_file").tempdir().unwrap();

        let path = ImportPath {
            save: temp_dir.path().join("save"),
            temp: temp_dir.path().join("temp"),
            clone: temp_dir.path().join("clone"),
        };

        let data = b"test_data";
        let crc32 = calc_data_crc32(data);

        let mut meta = SstMeta::default();

        {
            let mut f =
                ImportFile::create(meta.clone(), path.clone(), data_key_manager.clone()).unwrap();
            // Cannot create the same file again.
            ImportFile::create(meta.clone(), path.clone(), data_key_manager.clone()).unwrap_err();
            f.append(data).unwrap();
            // Invalid crc32 and length.
            f.finish().unwrap_err();
            check_file_exists(&path.temp, data_key_manager.as_deref());
            check_file_not_exists(&path.save, data_key_manager.as_deref());
        }

        meta.set_crc32(crc32);
        meta.set_length(data.len() as u64);

        {
            let mut f = ImportFile::create(meta, path.clone(), data_key_manager.clone()).unwrap();
            f.append(data).unwrap();
            f.finish().unwrap();
            check_file_not_exists(&path.temp, data_key_manager.as_deref());
            check_file_exists(&path.save, data_key_manager.as_deref());
        }
    }

    fn check_file_not_exists(path: &Path, key_manager: Option<&DataKeyManager>) {
        assert!(!path.exists());
        if let Some(manager) = key_manager {
            let info = manager.get_file(path.to_str().unwrap()).unwrap();
            assert!(info.is_empty());
            assert_eq!(info.method, EncryptionMethod::Plaintext);
        }
    }

    fn check_file_exists(path: &Path, key_manager: Option<&DataKeyManager>) {
        assert!(path.exists());
        if let Some(manager) = key_manager {
            let info = manager.get_file(path.to_str().unwrap()).unwrap();
            // the returned encryption info must not be default value
            assert_ne!(info.method, EncryptionMethod::Plaintext);
            assert!(!info.key.is_empty() && !info.iv.is_empty());
        }
    }

    fn check_file_is_same(path_a: &Path, path_b: &Path) -> bool {
        assert!(path_a.exists());
        assert!(path_b.exists());

        let content_a = file_system::read(path_a).unwrap();
        let content_b = file_system::read(path_b).unwrap();
        content_a == content_b
    }

    fn new_key_manager_for_test() -> (TempDir, Arc<DataKeyManager>) {
        // test with tde
        let tmp_dir = TempDir::new().unwrap();
        let key_manager = new_test_key_manager(&tmp_dir, None, None, None);
        (tmp_dir, Arc::new(key_manager.unwrap().unwrap()))
    }

    #[test]
    fn test_import_file() {
        // test without tde
        do_test_import_file(None);

        // test with tde
        let (_tmp_dir, key_manager) = new_key_manager_for_test();
        do_test_import_file(Some(key_manager));
    }

    fn create_external_sst_file_with_write_fn<F>(
        write_fn: F,
    ) -> Result<(TempDir, StorageBackend, SstMeta)>
    where
        F: FnOnce(&mut RocksSstWriter) -> Result<()>,
    {
        let ext_sst_dir = tempfile::tempdir()?;
        let mut sst_writer =
            new_sst_writer(ext_sst_dir.path().join("sample.sst").to_str().unwrap());
        write_fn(&mut sst_writer)?;
        let sst_info = sst_writer.finish()?;

        // make up the SST meta for downloading.
        let mut meta = SstMeta::default();
        let uuid = Uuid::new_v4();
        meta.set_uuid(uuid.as_bytes().to_vec());
        meta.set_cf_name(CF_DEFAULT.to_owned());
        meta.set_length(sst_info.file_size());
        meta.set_region_id(4);
        meta.mut_region_epoch().set_conf_ver(5);
        meta.mut_region_epoch().set_version(6);

        let backend = external_storage::make_local_backend(ext_sst_dir.path());
        Ok((ext_sst_dir, backend, meta))
    }

    fn create_sample_external_sst_file() -> Result<(TempDir, StorageBackend, SstMeta)> {
        create_external_sst_file_with_write_fn(|writer| {
            writer.put(b"zt123_r01", b"abc")?;
            writer.put(b"zt123_r04", b"xyz")?;
            writer.put(b"zt123_r07", b"pqrst")?;
            // writer.delete(b"t123_r10")?; // FIXME: can't handle DELETE ops yet.
            writer.put(b"zt123_r13", b"www")?;
            Ok(())
        })
    }

    fn create_sample_external_kv_file() -> Result<(TempDir, StorageBackend, KvMeta, Vec<u8>)> {
        create_sample_external_kv_file_with_optional_encryption(
            None,
            Vec::new(),
            EncryptionMethod::Plaintext,
            false,
        )
    }
    fn create_sample_external_kv_file_with_optional_encryption(
        opt_cipher_info: Option<CipherInfo>,
        master_key_configs: Vec<MasterKey>,
        master_key_based_data_encryption_method: EncryptionMethod,
        compression: bool,
    ) -> Result<(TempDir, StorageBackend, KvMeta, Vec<u8>)> {
        let ext_dir = tempfile::tempdir()?;
        let file_name = "v1/t000001/abc.log";
        let file_path = ext_dir.path().join(file_name);
        std::fs::create_dir_all(file_path.parent().unwrap())?;
        let file = block_on_external_io(tokio::fs::File::create(file_path.clone())).unwrap();

        // write to a buffer first, later flush to disk
        //
        let mut file_buffer = Vec::new();
        let cursor = Cursor::new(&mut file_buffer);
        let buf_writer = tokio::io::BufWriter::new(cursor);
        let mut kv_meta = KvMeta::default();

        // writer should compress the data first then encrypt,
        // wrapping zstdEncoder around Encrypter
        //
        let writer = if let Some(cipher_info) = opt_cipher_info {
            let iv = Iv::new_ctr().unwrap();
            // update meta
            //
            let mut encryption_info = encryptionpb::FileEncryptionInfo::new();
            encryption_info.set_file_iv(iv.as_slice().to_vec());
            encryption_info.set_encryption_method(cipher_info.cipher_type);
            encryption_info.set_plain_text_data_key(PlainTextDataKey::new());
            kv_meta.set_file_encryption_info(encryption_info);

            Box::new(
                EncrypterWriter::new(
                    buf_writer.compat_write(),
                    cipher_info.cipher_type,
                    &cipher_info.cipher_key,
                    iv,
                )
                .unwrap()
                .compat_write(),
            ) as Box<dyn AsyncWrite + Unpin>
        } else if !master_key_configs.is_empty() {
            let multi_master_key_backend = MultiMasterKeyBackend::new();
            block_on_external_io(
                multi_master_key_backend
                    .update_from_proto_if_needed(master_key_configs, create_async_backend),
            )
            .unwrap();

            let iv = Iv::new_ctr().unwrap();
            let plaintext_data_key = multi_master_key_backend
                .generate_data_key(master_key_based_data_encryption_method)
                .unwrap();

            let encryption_info =
                block_on_external_io(multi_master_key_backend.encrypt(&plaintext_data_key))
                    .unwrap();

            let mut encryption_info_proto = encryptionpb::FileEncryptionInfo::new();
            let mut master_key_proto = MasterKeyBased::new();
            encryption_info_proto.set_file_iv(iv.as_slice().to_vec());
            encryption_info_proto.set_encryption_method(master_key_based_data_encryption_method);
            master_key_proto.set_data_key_encrypted_content(protobuf::RepeatedField::from_vec(
                vec![encryption_info],
            ));
            encryption_info_proto.set_master_key_based(master_key_proto);
            kv_meta.set_file_encryption_info(encryption_info_proto);

            Box::new(
                EncrypterWriter::new(
                    buf_writer.compat_write(),
                    master_key_based_data_encryption_method,
                    &plaintext_data_key,
                    iv,
                )
                .unwrap()
                .compat_write(),
            ) as Box<dyn AsyncWrite + Unpin>
        } else {
            Box::new(buf_writer) as Box<dyn AsyncWrite + Unpin>
        };

        let mut writer = if compression {
            Box::new(ZstdEncoder::new(writer)) as Box<dyn AsyncWrite + Unpin>
        } else {
            writer
        };

        let kvs = vec![
            (b"t1_r01".to_vec(), b"tidb".to_vec()),
            (b"t1_r02".to_vec(), b"tikv".to_vec()),
            (b"t1_r03".to_vec(), b"pingcap".to_vec()),
            (b"t1_r04".to_vec(), b"test for PITR".to_vec()),
        ];

        let mut sha256 = Hasher::new(MessageDigest::sha256()).unwrap();
        let mut len = 0;
        let mut buf = vec![];
        for kv in kvs {
            let encoded = EventEncoder::encode_event(&kv.0, &kv.1);
            for slice in encoded {
                len += block_on_external_io(writer.write(slice.as_ref())).unwrap();
                sha256.update(slice.as_ref()).unwrap();
                buf.extend_from_slice(slice.as_ref());
            }
        }
        block_on_external_io(writer.flush()).unwrap();
        drop(writer);

        // calc checksum of the file buffer
        //
        if kv_meta.has_file_encryption_info() {
            let mut tmp_buf = Vec::new();
            let (mut checksum_reader, hasher) =
                Sha256Reader::new(Cursor::new(&mut file_buffer)).unwrap();
            checksum_reader.read_to_end(&mut tmp_buf).unwrap();
            let checksum = hasher.lock().unwrap().finish().unwrap().to_vec();
            kv_meta.mut_file_encryption_info().set_checksum(checksum);
        }

        // actually write to disk
        //
        let mut buf_writer = tokio::io::BufWriter::new(file);
        block_on_external_io(buf_writer.write_all(&file_buffer)).unwrap();
        block_on_external_io(buf_writer.flush()).unwrap();

        kv_meta.set_name(file_name.to_string());
        kv_meta.set_cf(String::from("default"));
        kv_meta.set_is_delete(false);
        kv_meta.set_length(len as _);
        kv_meta.set_sha256(sha256.finish().unwrap().to_vec());
        if compression {
            kv_meta.set_compression_type(CompressionType::Zstd);
        }
        let backend = external_storage::make_local_backend(ext_dir.path());
        Ok((ext_dir, backend, kv_meta, buf))
    }

    fn create_sample_external_rawkv_sst_file(
        start_key: &[u8],
        end_key: &[u8],
        end_key_exclusive: bool,
    ) -> Result<(tempfile::TempDir, StorageBackend, SstMeta)> {
        let (dir, backend, mut meta) = create_external_sst_file_with_write_fn(|writer| {
            writer.put(b"za", b"v1")?;
            writer.put(b"zb", b"v2")?;
            writer.put(b"zb\x00", b"v3")?;
            writer.put(b"zc", b"v4")?;
            writer.put(b"zc\x00", b"v5")?;
            writer.put(b"zc\x00\x00", b"v6")?;
            writer.put(b"zd", b"v7")?;
            Ok(())
        })?;
        meta.mut_range().set_start(start_key.to_vec());
        meta.mut_range().set_end(end_key.to_vec());
        meta.set_end_key_exclusive(end_key_exclusive);
        Ok((dir, backend, meta))
    }

    fn get_encoded_key(key: &[u8], ts: u64) -> Vec<u8> {
        keys::data_key(
            Key::from_raw(key)
                .append_ts(TimeStamp::new(ts))
                .as_encoded(),
        )
    }

    fn get_write_value(
        write_type: WriteType,
        start_ts: u64,
        short_value: Option<Value>,
    ) -> Vec<u8> {
        txn_types::Write::new(write_type, TimeStamp::new(start_ts), short_value)
            .as_ref()
            .to_bytes()
    }

    fn create_sample_external_sst_file_txn_default()
    -> Result<(tempfile::TempDir, StorageBackend, SstMeta)> {
        let ext_sst_dir = tempfile::tempdir()?;
        let mut sst_writer = new_sst_writer(
            ext_sst_dir
                .path()
                .join("sample_default.sst")
                .to_str()
                .unwrap(),
        );
        sst_writer.put(&get_encoded_key(b"t123_r01", 1), b"abc")?;
        sst_writer.put(&get_encoded_key(b"t123_r04", 3), b"xyz")?;
        sst_writer.put(&get_encoded_key(b"t123_r07", 7), b"pqrst")?;
        // sst_writer.delete(b"t123_r10")?; // FIXME: can't handle DELETE ops yet.
        let sst_info = sst_writer.finish()?;

        // make up the SST meta for downloading.
        let mut meta = SstMeta::default();
        let uuid = Uuid::new_v4();
        meta.set_uuid(uuid.as_bytes().to_vec());
        meta.set_cf_name(CF_DEFAULT.to_owned());
        meta.set_length(sst_info.file_size());
        meta.set_region_id(4);
        meta.mut_region_epoch().set_conf_ver(5);
        meta.mut_region_epoch().set_version(6);

        let backend = external_storage::make_local_backend(ext_sst_dir.path());
        Ok((ext_sst_dir, backend, meta))
    }

    fn create_sample_external_sst_file_txn_write()
    -> Result<(tempfile::TempDir, StorageBackend, SstMeta)> {
        let ext_sst_dir = tempfile::tempdir()?;
        let mut sst_writer = new_sst_writer(
            ext_sst_dir
                .path()
                .join("sample_write.sst")
                .to_str()
                .unwrap(),
        );
        sst_writer.put(
            &get_encoded_key(b"t123_r01", 5),
            &get_write_value(WriteType::Put, 1, None),
        )?;
        sst_writer.put(
            &get_encoded_key(b"t123_r02", 5),
            &get_write_value(WriteType::Delete, 1, None),
        )?;
        sst_writer.put(
            &get_encoded_key(b"t123_r04", 4),
            &get_write_value(WriteType::Put, 3, None),
        )?;
        sst_writer.put(
            &get_encoded_key(b"t123_r07", 8),
            &get_write_value(WriteType::Put, 7, None),
        )?;
        sst_writer.put(
            &get_encoded_key(b"t123_r13", 8),
            &get_write_value(WriteType::Put, 7, Some(b"www".to_vec())),
        )?;
        let sst_info = sst_writer.finish()?;

        // make up the SST meta for downloading.
        let mut meta = SstMeta::default();
        let uuid = Uuid::new_v4();
        meta.set_uuid(uuid.as_bytes().to_vec());
        meta.set_cf_name(CF_WRITE.to_owned());
        meta.set_length(sst_info.file_size());
        meta.set_region_id(4);
        meta.mut_region_epoch().set_conf_ver(5);
        meta.mut_region_epoch().set_version(6);

        let backend = external_storage::make_local_backend(ext_sst_dir.path());
        Ok((ext_sst_dir, backend, meta))
    }

    fn new_rewrite_rule(
        old_key_prefix: &[u8],
        new_key_prefix: &[u8],
        new_timestamp: u64,
    ) -> RewriteRule {
        let mut rule = RewriteRule::default();
        rule.set_old_key_prefix(old_key_prefix.to_vec());
        rule.set_new_key_prefix(new_key_prefix.to_vec());
        rule.set_new_timestamp(new_timestamp);
        rule
    }

    fn create_sst_test_engine() -> Result<TestEngine> {
        let temp_dir = Builder::new().prefix("test_import_dir").tempdir().unwrap();
        let db_path = temp_dir.path().join("db");
        let db = new_test_engine(db_path.to_str().unwrap(), DATA_CFS);
        Ok(db)
    }

    #[test]
    fn test_read_external_storage_into_file() {
        let data = &b"some input data"[..];
        let mut input = data;
        let mut output = Vec::new();
        let input_len = input.len() as u64;

        let mut hasher = Hasher::new(MessageDigest::sha256()).unwrap();
        hasher.update(data).unwrap();
        let hash256 = hasher.finish().unwrap().to_vec();

        block_on_external_io(external_storage::read_external_storage_into_file(
            &mut input,
            &mut output,
            &Limiter::new(f64::INFINITY),
            input_len,
            Some(hash256),
            8192,
            None,
            None,
        ))
        .unwrap();
        assert_eq!(&*output, data);
    }

    #[test]
    fn test_read_external_storage_into_file_timed_out() {
        use futures_util::stream::{pending, TryStreamExt};

        let mut input = pending::<io::Result<&[u8]>>().into_async_read();
        let mut output = Vec::new();
        let err = block_on_external_io(external_storage::read_external_storage_into_file(
            &mut input,
            &mut output,
            &Limiter::new(f64::INFINITY),
            0,
            None,
            usize::MAX,
            None,
            None,
        ))
        .unwrap_err();
        assert_eq!(err.kind(), ErrorKind::TimedOut);
    }

    #[test]
    fn test_read_external_storage_info_buff() {
        let data = &b"input some data, used to test read buff"[..];
        let mut reader = data;
        let len = reader.len() as _;
        let sha_256 = {
            let mut hasher = Hasher::new(MessageDigest::sha256()).unwrap();
            hasher.update(data).unwrap();
            hasher.finish().unwrap().to_vec()
        };

        // test successfully.
        let output = block_on_external_io(read_external_storage_info_buff(
            &mut reader,
            &Limiter::new(f64::INFINITY),
            len,
            Some(sha_256.clone()),
            0,
            None,
            None,
        ))
        .unwrap();
        assert_eq!(&output, data);

        // test without expected_sha256.
        reader = data;
        let output = block_on_external_io(read_external_storage_info_buff(
            &mut reader,
            &Limiter::new(f64::INFINITY),
            len,
            None,
            0,
            None,
            None,
        ))
        .unwrap();
        assert_eq!(&output, data);

        // test with wrong expected len.
        reader = data;
        let err = block_on_external_io(read_external_storage_info_buff(
            &mut reader,
            &Limiter::new(f64::INFINITY),
            len + 1,
            Some(sha_256.clone()),
            0,
            None,
            None,
        ))
        .unwrap_err();
        assert!(err.to_string().contains("length not match"));

        // test with wrong expected_sha256.
        reader = data;
        let err = block_on_external_io(read_external_storage_info_buff(
            &mut reader,
            &Limiter::new(f64::INFINITY),
            len,
            Some(sha_256[..sha_256.len() - 1].to_vec()),
            0,
            None,
            None,
        ))
        .unwrap_err();
        assert!(err.to_string().contains("sha256 not match"));
    }

    #[test]
    fn test_read_external_storage_info_buff_timed_out() {
        use futures_util::stream::{pending, TryStreamExt};

        let mut input = pending::<io::Result<&[u8]>>().into_async_read();
        let err = block_on_external_io(read_external_storage_info_buff(
            &mut input,
            &Limiter::new(f64::INFINITY),
            0,
            None,
            usize::MAX,
            None,
            None,
        ))
        .unwrap_err();
        assert_eq!(err.kind(), ErrorKind::TimedOut);
    }

    #[test]
    fn test_update_config_memory_use_ratio() {
        // create SstImporter with default.
        let cfg = Config {
            memory_use_ratio: 0.3,
            ..Default::default()
        };
        let import_dir = tempfile::tempdir().unwrap();
        let importer =
            SstImporter::<TestEngine>::new(&cfg, import_dir, None, ApiVersion::V1, false).unwrap();
        let mem_quota_old = importer.memory_quota.capacity();

        // create new config and get the diff config.
        let cfg_new = Config {
            memory_use_ratio: 0.1,
            ..Default::default()
        };
        let change = cfg.diff(&cfg_new);

        let threads = ResizableRuntime::new(
            cfg.num_threads,
            "test",
            Box::new(create_tokio_runtime),
            Box::new(|_| {}),
        );
        let handle = ResizableRuntimeHandle::new(threads);

        // create config manager and update config.
        let mut cfg_mgr = ImportConfigManager::new(cfg, handle);
        cfg_mgr.dispatch(change).unwrap();
        importer.update_config_memory_use_ratio(&cfg_mgr);

        let mem_quota_new = importer.memory_quota.capacity();
        assert!(mem_quota_old > mem_quota_new);
        assert_eq!(
            mem_quota_old / 3,
            mem_quota_new,
            "mem_quota_old / 3 = {} mem_quota_new = {}",
            mem_quota_old / 3,
            mem_quota_new
        );
    }

    #[test]
    fn test_update_config_with_invalid_conifg() {
        let cfg = Config::default();
        let cfg_new = Config {
            memory_use_ratio: -0.1,
            ..Default::default()
        };
        let change = cfg.diff(&cfg_new);

        let threads = ResizableRuntime::new(
            cfg.num_threads,
            "test",
            Box::new(create_tokio_runtime),
            Box::new(|_| {}),
        );
        let handle = ResizableRuntimeHandle::new(threads);

        let mut cfg_mgr = ImportConfigManager::new(cfg, handle);
        let r = cfg_mgr.dispatch(change);
        assert!(r.is_err());
    }

    #[test]
    fn test_update_import_num_threads() {
        let threads = ResizableRuntime::new(
            Config::default().num_threads,
            "test",
            Box::new(create_tokio_runtime),
            Box::new(|new_size: usize| {
                COUNTER.store(new_size, Ordering::SeqCst);
            }),
        );
        let handle = ResizableRuntimeHandle::new(threads);
        let mut cfg_mgr = ImportConfigManager::new(Config::default(), handle);

        assert_eq!(COUNTER.load(Ordering::SeqCst), cfg_mgr.rl().num_threads);
        assert_eq!(cfg_mgr.rl().num_threads, Config::default().num_threads);

        let cfg_new = Config {
            num_threads: 10,
            ..Default::default()
        };
        let change = Config::default().diff(&cfg_new);
        let r = cfg_mgr.dispatch(change);

        r.unwrap();
        assert_eq!(cfg_mgr.rl().num_threads, cfg_new.num_threads);
        assert_eq!(COUNTER.load(Ordering::SeqCst), cfg_mgr.rl().num_threads);
    }

    #[test]
    fn test_download_kv_file_to_mem_cache() {
        // create a sample kv file.
        let (_temp_dir, backend, kv_meta, buff) = create_sample_external_kv_file().unwrap();

        // create importer object.
        let import_dir = tempfile::tempdir().unwrap();
        let (_, key_manager) = new_key_manager_for_test();
        let importer = SstImporter::<TestEngine>::new(
            &Config::default(),
            import_dir,
            Some(key_manager),
            ApiVersion::V1,
            false,
        )
        .unwrap();
        let ext_storage = {
            importer.auto_encrypt_local_file_if_needed(
                importer.external_storage_or_cache(&backend, "").unwrap(),
            )
        };

        let output = block_on_external_io(importer.download_kv_file_to_mem_cache(
            &kv_meta,
            ext_storage,
            &Limiter::new(f64::INFINITY),
            None,
            None,
        ))
        .unwrap();

        assert!(
            matches!(output.clone(), CacheKvFile::Mem(rc) if &*rc.get().unwrap().content == buff.as_slice()),
            "{:?}",
            output
        );

        // Do not shrink nothing.
        let shrink_size = importer.shrink_by_tick();
        assert_eq!(shrink_size, 0);
        assert_eq!(importer.file_locks.len(), 1);

        // drop the refcnt
        drop(output);
        let shrink_size = importer.shrink_by_tick();
        assert_eq!(shrink_size, 0);
        assert_eq!(importer.file_locks.len(), 1);

        // set expired instance in Dashmap
        for mut kv in importer.file_locks.iter_mut() {
            kv.1 = Instant::now().sub(Duration::from_secs(61));
        }
        let shrink_size = importer.shrink_by_tick();
        assert_eq!(shrink_size, buff.len());
        assert!(importer.file_locks.is_empty());
    }

    #[test]
    fn test_download_kv_files_from_external_storage_to_mem() {
        // create a sample kv file.
        let (_temp_dir, backend, kv_meta, buff) = create_sample_external_kv_file().unwrap();

        // create importer object.
        let import_dir = tempfile::tempdir().unwrap();
        let (_, key_manager) = new_key_manager_for_test();
        let importer = SstImporter::<TestEngine>::new(
            &Config::default(),
            import_dir,
            Some(key_manager),
            ApiVersion::V1,
            false,
        )
        .unwrap();
        let ext_storage = {
            let inner = importer.auto_encrypt_local_file_if_needed(
                importer.external_storage_or_cache(&backend, "").unwrap(),
            );
            Arc::new(inner)
        };

        // test read all of the file.
        let restore_config = RestoreConfig {
            expected_plaintext_file_checksum: Some(kv_meta.get_sha256().to_vec()),
            ..Default::default()
        };

        let output = block_on_external_io(importer.download_kv_files_from_external_storage_to_mem(
            kv_meta.get_length(),
            kv_meta.get_name(),
            ext_storage.clone(),
            &Limiter::new(f64::INFINITY),
            restore_config,
        ))
        .unwrap();
        assert_eq!(
            buff,
            output,
            "we are testing addition with {} and {}",
            buff.len(),
            output.len()
        );

        // test read range of the file.
        let (offset, len) = (5, 16);
        let restore_config = RestoreConfig {
            range: Some((offset, len)),
            ..Default::default()
        };

        let output = block_on_external_io(importer.download_kv_files_from_external_storage_to_mem(
            len,
            kv_meta.get_name(),
            ext_storage,
            &Limiter::new(f64::INFINITY),
            restore_config,
        ))
        .unwrap();
        assert_eq!(&buff[offset as _..(offset + len) as _], &output[..]);
    }

    #[test]
    fn test_do_download_kv_file() {
        // create a sample kv file.
        let (_temp_dir, backend, kv_meta, buff) = create_sample_external_kv_file().unwrap();

        // create importer object.
        let import_dir = tempfile::tempdir().unwrap();
        let (_, key_manager) = new_key_manager_for_test();
        let cfg = Config {
            memory_use_ratio: 0.0,
            ..Default::default()
        };
        let importer = SstImporter::<TestEngine>::new(
            &cfg,
            import_dir,
            Some(key_manager.clone()),
            ApiVersion::V1,
            false,
        )
        .unwrap();
        let ext_storage = {
            importer.auto_encrypt_local_file_if_needed(
                importer.external_storage_or_cache(&backend, "").unwrap(),
            )
        };
        let path = importer
            .dir
            .get_import_path(
                format!("{}_{}", kv_meta.get_name(), kv_meta.get_range_offset()).as_str(),
            )
            .unwrap();

        // test do_download_kv_file().
        assert!(importer.download_to_disk_only());
        let output = block_on_external_io(importer.download_kv_file(
            &kv_meta,
            ext_storage,
            &backend,
            &Limiter::new(f64::INFINITY),
            None,
            Vec::new(),
        ))
        .unwrap();
        assert_eq!(*output, buff);
        check_file_exists(&path.save, Some(&*key_manager));

        // test shrink nothing.
        let shrint_files_cnt = importer.shrink_by_tick();
        assert_eq!(shrint_files_cnt, 0);

        // set expired instance in Dashmap.
        for mut kv in importer.file_locks.iter_mut() {
            kv.1 = Instant::now().sub(Duration::from_secs(601));
        }
        let shrint_files_cnt = importer.shrink_by_tick();
        assert_eq!(shrint_files_cnt, 1);
        check_file_not_exists(&path.save, Some(&*key_manager));
    }

    #[test]
    fn test_download_file_from_external_storage_for_sst() {
        // creates a sample SST file.
        let (_ext_sst_dir, backend, meta) = create_sample_external_sst_file().unwrap();

        // create importer object.
        let import_dir = tempfile::tempdir().unwrap();
        let (_, key_manager) = new_key_manager_for_test();
        let importer = SstImporter::<TestEngine>::new(
            &Config::default(),
            import_dir,
            Some(key_manager.clone()),
            ApiVersion::V1,
            false,
        )
        .unwrap();

        // perform download file into .temp dir.
        let file_name = "sample.sst";
        let path = importer.dir.get_import_path(file_name).unwrap();
        let restore_config = RestoreConfig::default();
        importer
            .download_file_from_external_storage(
                meta.get_length(),
                file_name,
                path.temp.clone(),
                &backend,
                &Limiter::new(f64::INFINITY),
                restore_config,
            )
            .unwrap();
        check_file_exists(&path.temp, Some(&key_manager));
        assert!(!check_file_is_same(
            &_ext_sst_dir.path().join(file_name),
            &path.temp,
        ));
    }

    #[test]
    fn test_download_file_from_external_storage_for_kv() {
        let (_temp_dir, backend, kv_meta, _) = create_sample_external_kv_file().unwrap();
        let (_, key_manager) = new_key_manager_for_test();

        let import_dir = tempfile::tempdir().unwrap();
        let importer = SstImporter::<TestEngine>::new(
            &Config::default(),
            import_dir,
            Some(key_manager.clone()),
            ApiVersion::V1,
            false,
        )
        .unwrap();

        let path = importer.dir.get_import_path(kv_meta.get_name()).unwrap();
        let restore_config = RestoreConfig {
            expected_plaintext_file_checksum: Some(kv_meta.get_sha256().to_vec()),
            ..Default::default()
        };
        importer
            .download_file_from_external_storage(
                kv_meta.get_length(),
                kv_meta.get_name(),
                path.temp.clone(),
                &backend,
                &Limiter::new(f64::INFINITY),
                restore_config,
            )
            .unwrap();

        check_file_exists(&path.temp, Some(&key_manager));
        assert!(!check_file_is_same(
            &_temp_dir.path().join(kv_meta.get_name()),
            &path.temp,
        ));
    }

    #[test]
    fn test_download_sst_no_key_rewrite() {
        // creates a sample SST file.
        let (_ext_sst_dir, backend, meta) = create_sample_external_sst_file().unwrap();

        // performs the download.
        let importer_dir = tempfile::tempdir().unwrap();
        let cfg = Config::default();
        let importer =
            SstImporter::<TestEngine>::new(&cfg, &importer_dir, None, ApiVersion::V1, false)
                .unwrap();
        let db = create_sst_test_engine().unwrap();

        let range = importer
            .download(
                &meta,
                &backend,
                "sample.sst",
                &RewriteRule::default(),
                None,
                Limiter::new(f64::INFINITY),
                db,
            )
            .unwrap()
            .unwrap();

        assert_eq!(range.get_start(), b"t123_r01");
        assert_eq!(range.get_end(), b"t123_r13");

        // verifies that the file is saved to the correct place.
        let sst_file_path = importer.dir.join_for_read(&meta).unwrap().save;
        let sst_file_metadata = sst_file_path.metadata().unwrap();
        assert!(sst_file_metadata.is_file());
        assert_eq!(sst_file_metadata.len(), meta.get_length());

        // verifies the SST content is correct.
        let sst_reader = new_sst_reader(sst_file_path.to_str().unwrap(), None);
        sst_reader.verify_checksum().unwrap();
        let mut iter = sst_reader.iter(IterOptions::default()).unwrap();
        iter.seek_to_first().unwrap();
        assert_eq!(
            collect(iter),
            vec![
                (b"zt123_r01".to_vec(), b"abc".to_vec()),
                (b"zt123_r04".to_vec(), b"xyz".to_vec()),
                (b"zt123_r07".to_vec(), b"pqrst".to_vec()),
                (b"zt123_r13".to_vec(), b"www".to_vec()),
            ]
        );
    }

    #[test]
    fn test_download_sst_no_key_rewrite_with_encrypted() {
        // creates a sample SST file.
        let (_ext_sst_dir, backend, meta) = create_sample_external_sst_file().unwrap();

        // performs the download.
        let importer_dir = tempfile::tempdir().unwrap();
        let cfg = Config::default();
        let (temp_dir, key_manager) = new_key_manager_for_test();
        let importer = SstImporter::<TestEngine>::new(
            &cfg,
            &importer_dir,
            Some(key_manager.clone()),
            ApiVersion::V1,
            false,
        )
        .unwrap();

        let db_path = temp_dir.path().join("db");
        let env = get_env(Some(key_manager.clone()), None /* io_rate_limiter */).unwrap();
        let db = new_test_engine_with_env(db_path.to_str().unwrap(), DATA_CFS, env.clone());

        let range = importer
            .download(
                &meta,
                &backend,
                "sample.sst",
                &RewriteRule::default(),
                None,
                Limiter::new(f64::INFINITY),
                db,
            )
            .unwrap()
            .unwrap();

        assert_eq!(range.get_start(), b"t123_r01");
        assert_eq!(range.get_end(), b"t123_r13");

        // verifies that the file is saved to the correct place.
        let sst_file_path = importer.dir.join_for_read(&meta).unwrap().save;
        let sst_file_metadata = sst_file_path.metadata().unwrap();
        assert!(sst_file_metadata.is_file());
        assert_eq!(sst_file_metadata.len(), meta.get_length());

        // verified the tmp files are correctly cleaned up
        check_file_not_exists(
            importer.dir.join_for_read(&meta).unwrap().temp.as_path(),
            Some(&*key_manager),
        );

        // verifies the SST content is correct.
        let sst_reader = new_sst_reader(sst_file_path.to_str().unwrap(), Some(env));
        sst_reader.verify_checksum().unwrap();
        let mut iter = sst_reader.iter(IterOptions::default()).unwrap();
        iter.seek_to_first().unwrap();
        assert_eq!(
            collect(iter),
            vec![
                (b"zt123_r01".to_vec(), b"abc".to_vec()),
                (b"zt123_r04".to_vec(), b"xyz".to_vec()),
                (b"zt123_r07".to_vec(), b"pqrst".to_vec()),
                (b"zt123_r13".to_vec(), b"www".to_vec()),
            ]
        );
    }

    #[test]
    fn test_download_sst_with_key_rewrite() {
        // creates a sample SST file.
        let (_ext_sst_dir, backend, meta) = create_sample_external_sst_file().unwrap();

        // performs the download.
        let importer_dir = tempfile::tempdir().unwrap();
        let cfg = Config::default();
        let importer =
            SstImporter::<TestEngine>::new(&cfg, &importer_dir, None, ApiVersion::V1, false)
                .unwrap();
        let db = create_sst_test_engine().unwrap();

        let range = importer
            .download(
                &meta,
                &backend,
                "sample.sst",
                &new_rewrite_rule(b"t123", b"t567", 0),
                None,
                Limiter::new(f64::INFINITY),
                db,
            )
            .unwrap()
            .unwrap();

        assert_eq!(range.get_start(), b"t567_r01");
        assert_eq!(range.get_end(), b"t567_r13");

        // verifies that the file is saved to the correct place.
        // (the file size may be changed, so not going to check the file size)
        let sst_file_path = importer.dir.join_for_read(&meta).unwrap().save;
        assert!(sst_file_path.is_file());

        // verifies the SST content is correct.
        let sst_reader = new_sst_reader(sst_file_path.to_str().unwrap(), None);
        sst_reader.verify_checksum().unwrap();
        let mut iter = sst_reader.iter(IterOptions::default()).unwrap();
        iter.seek_to_first().unwrap();
        assert_eq!(
            collect(iter),
            vec![
                (b"zt567_r01".to_vec(), b"abc".to_vec()),
                (b"zt567_r04".to_vec(), b"xyz".to_vec()),
                (b"zt567_r07".to_vec(), b"pqrst".to_vec()),
                (b"zt567_r13".to_vec(), b"www".to_vec()),
            ]
        );
    }

    #[test]
    fn test_download_sst_with_key_rewrite_ts_default() {
        // performs the download.
        let importer_dir = tempfile::tempdir().unwrap();
        let cfg = Config::default();
        let importer =
            SstImporter::<TestEngine>::new(&cfg, &importer_dir, None, ApiVersion::V1, false)
                .unwrap();

        // creates a sample SST file.
        let (_ext_sst_dir, backend, meta) = create_sample_external_sst_file_txn_default().unwrap();
        let db = create_sst_test_engine().unwrap();

        let _ = importer
            .download(
                &meta,
                &backend,
                "sample_default.sst",
                &new_rewrite_rule(b"", b"", 16),
                None,
                Limiter::new(f64::INFINITY),
                db,
            )
            .unwrap()
            .unwrap();

        // verifies that the file is saved to the correct place.
        // (the file size may be changed, so not going to check the file size)
        let sst_file_path = importer.dir.join_for_read(&meta).unwrap().save;
        assert!(sst_file_path.is_file());

        // verifies the SST content is correct.
        let sst_reader = new_sst_reader(sst_file_path.to_str().unwrap(), None);
        sst_reader.verify_checksum().unwrap();
        let mut iter = sst_reader.iter(IterOptions::default()).unwrap();
        iter.seek_to_first().unwrap();
        assert_eq!(
            collect(iter),
            vec![
                (get_encoded_key(b"t123_r01", 16), b"abc".to_vec()),
                (get_encoded_key(b"t123_r04", 16), b"xyz".to_vec()),
                (get_encoded_key(b"t123_r07", 16), b"pqrst".to_vec()),
            ]
        );
    }

    #[test]
    fn test_download_sst_with_key_rewrite_ts_write() {
        // performs the download.
        let importer_dir = tempfile::tempdir().unwrap();
        let cfg = Config::default();
        let importer =
            SstImporter::<TestEngine>::new(&cfg, &importer_dir, None, ApiVersion::V1, false)
                .unwrap();

        // creates a sample SST file.
        let (_ext_sst_dir, backend, meta) = create_sample_external_sst_file_txn_write().unwrap();
        let db = create_sst_test_engine().unwrap();

        let _ = importer
            .download(
                &meta,
                &backend,
                "sample_write.sst",
                &new_rewrite_rule(b"", b"", 16),
                None,
                Limiter::new(f64::INFINITY),
                db,
            )
            .unwrap()
            .unwrap();

        // verifies that the file is saved to the correct place.
        // (the file size may be changed, so not going to check the file size)
        let sst_file_path = importer.dir.join_for_read(&meta).unwrap().save;
        assert!(sst_file_path.is_file());

        // verifies the SST content is correct.
        let sst_reader = new_sst_reader(sst_file_path.to_str().unwrap(), None);
        sst_reader.verify_checksum().unwrap();
        let mut iter = sst_reader.iter(IterOptions::default()).unwrap();
        iter.seek_to_first().unwrap();
        assert_eq!(
            collect(iter),
            vec![
                (
                    get_encoded_key(b"t123_r01", 16),
                    get_write_value(WriteType::Put, 16, None)
                ),
                (
                    get_encoded_key(b"t123_r02", 16),
                    get_write_value(WriteType::Delete, 16, None)
                ),
                (
                    get_encoded_key(b"t123_r04", 16),
                    get_write_value(WriteType::Put, 16, None)
                ),
                (
                    get_encoded_key(b"t123_r07", 16),
                    get_write_value(WriteType::Put, 16, None)
                ),
                (
                    get_encoded_key(b"t123_r13", 16),
                    get_write_value(WriteType::Put, 16, Some(b"www".to_vec()))
                ),
            ]
        );
    }

    #[test]
    fn test_download_sst_then_ingest() {
        for cf in &[CF_DEFAULT, CF_WRITE] {
            // creates a sample SST file.
            let (_ext_sst_dir, backend, mut meta) = create_sample_external_sst_file().unwrap();
            meta.set_cf_name((*cf).to_string());

            // performs the download.
            let importer_dir = tempfile::tempdir().unwrap();
            let cfg = Config::default();
            let importer =
                SstImporter::<TestEngine>::new(&cfg, &importer_dir, None, ApiVersion::V1, false)
                    .unwrap();
            let db = create_sst_test_engine().unwrap();

            let range = importer
                .download(
                    &meta,
                    &backend,
                    "sample.sst",
                    &new_rewrite_rule(b"t123", b"t9102", 0),
                    None,
                    Limiter::new(f64::INFINITY),
                    db,
                )
                .unwrap()
                .unwrap();

            assert_eq!(range.get_start(), b"t9102_r01");
            assert_eq!(range.get_end(), b"t9102_r13");

            // performs the ingest
            let ingest_dir = tempfile::tempdir().unwrap();
            let db = new_test_engine(ingest_dir.path().to_str().unwrap(), DATA_CFS);

            meta.set_length(0); // disable validation.
            meta.set_crc32(0);
            let meta_info = importer.validate(&meta).unwrap();
            importer.ingest(&[meta_info.clone()], &db).unwrap();
            // key1 = "zt9102_r01", value1 = "abc", len = 13
            // key2 = "zt9102_r04", value2 = "xyz", len = 13
            // key3 = "zt9102_r07", value3 = "pqrst", len = 15
            // key4 = "zt9102_r13", value4 = "www", len = 13
            // total_bytes = (13 + 13 + 15 + 13) + 4 * 8 = 86
            // don't no why each key has extra 8 byte length in raw_key_size(), but it seems
            // tolerable. https://docs.rs/rocks/0.1.0/rocks/table_properties/struct.TableProperties.html#method.raw_key_size
            assert_eq!(meta_info.total_bytes, 86);
            assert_eq!(meta_info.total_kvs, 4);

            // verifies the DB content is correct.
            let mut iter = db.iterator(cf).unwrap();
            iter.seek_to_first().unwrap();
            assert_eq!(
                collect(iter),
                vec![
                    (b"zt9102_r01".to_vec(), b"abc".to_vec()),
                    (b"zt9102_r04".to_vec(), b"xyz".to_vec()),
                    (b"zt9102_r07".to_vec(), b"pqrst".to_vec()),
                    (b"zt9102_r13".to_vec(), b"www".to_vec()),
                ]
            );

            // check properties
            let start = keys::data_key(b"");
            let end = keys::data_end_key(b"");
            let collection = db.get_range_properties_cf(cf, &start, &end).unwrap();
            assert!(!collection.is_empty());
            for (_, v) in collection.iter() {
                assert!(!v.user_collected_properties().is_empty());
                assert_eq!(
                    v.user_collected_properties()
                        .get(PROP_TEST_MARKER_CF_NAME)
                        .unwrap(),
                    cf.as_bytes()
                );
            }
        }
    }

    #[test]
    fn test_download_sst_partial_range() {
        let (_ext_sst_dir, backend, mut meta) = create_sample_external_sst_file().unwrap();
        let importer_dir = tempfile::tempdir().unwrap();
        let cfg = Config::default();
        let importer =
            SstImporter::<TestEngine>::new(&cfg, &importer_dir, None, ApiVersion::V1, false)
                .unwrap();
        let db = create_sst_test_engine().unwrap();
        // note: the range doesn't contain the DATA_PREFIX 'z'.
        meta.mut_range().set_start(b"t123_r02".to_vec());
        meta.mut_range().set_end(b"t123_r12".to_vec());

        let range = importer
            .download(
                &meta,
                &backend,
                "sample.sst",
                &RewriteRule::default(),
                None,
                Limiter::new(f64::INFINITY),
                db,
            )
            .unwrap()
            .unwrap();

        assert_eq!(range.get_start(), b"t123_r04");
        assert_eq!(range.get_end(), b"t123_r07");

        // verifies that the file is saved to the correct place.
        // (the file size is changed, so not going to check the file size)
        let sst_file_path = importer.dir.join_for_read(&meta).unwrap().save;
        assert!(sst_file_path.is_file());

        // verifies the SST content is correct.
        let sst_reader = new_sst_reader(sst_file_path.to_str().unwrap(), None);
        sst_reader.verify_checksum().unwrap();
        let mut iter = sst_reader.iter(IterOptions::default()).unwrap();
        iter.seek_to_first().unwrap();
        assert_eq!(
            collect(iter),
            vec![
                (b"zt123_r04".to_vec(), b"xyz".to_vec()),
                (b"zt123_r07".to_vec(), b"pqrst".to_vec()),
            ]
        );
    }

    #[test]
    fn test_download_sst_partial_range_with_key_rewrite() {
        let (_ext_sst_dir, backend, mut meta) = create_sample_external_sst_file().unwrap();
        let importer_dir = tempfile::tempdir().unwrap();
        let cfg = Config::default();
        let importer =
            SstImporter::<TestEngine>::new(&cfg, &importer_dir, None, ApiVersion::V1, false)
                .unwrap();
        let db = create_sst_test_engine().unwrap();
        meta.mut_range().set_start(b"t5_r02".to_vec());
        meta.mut_range().set_end(b"t5_r12".to_vec());

        let range = importer
            .download(
                &meta,
                &backend,
                "sample.sst",
                &new_rewrite_rule(b"t123", b"t5", 0),
                None,
                Limiter::new(f64::INFINITY),
                db,
            )
            .unwrap()
            .unwrap();

        assert_eq!(range.get_start(), b"t5_r04");
        assert_eq!(range.get_end(), b"t5_r07");

        // verifies that the file is saved to the correct place.
        let sst_file_path = importer.dir.join_for_read(&meta).unwrap().save;
        assert!(sst_file_path.is_file());

        // verifies the SST content is correct.
        let sst_reader = new_sst_reader(sst_file_path.to_str().unwrap(), None);
        sst_reader.verify_checksum().unwrap();
        let mut iter = sst_reader.iter(IterOptions::default()).unwrap();
        iter.seek_to_first().unwrap();
        assert_eq!(
            collect(iter),
            vec![
                (b"zt5_r04".to_vec(), b"xyz".to_vec()),
                (b"zt5_r07".to_vec(), b"pqrst".to_vec()),
            ]
        );
    }

    #[test]
    fn test_download_sst_invalid() {
        let ext_sst_dir = tempfile::tempdir().unwrap();
        file_system::write(ext_sst_dir.path().join("sample.sst"), b"not an SST file").unwrap();
        let mut meta = SstMeta::default();
        meta.set_uuid(vec![0u8; 16]);
        let importer_dir = tempfile::tempdir().unwrap();
        let cfg = Config::default();
        let importer =
            SstImporter::<TestEngine>::new(&cfg, &importer_dir, None, ApiVersion::V1, false)
                .unwrap();
        let db = create_sst_test_engine().unwrap();
        let backend = external_storage::make_local_backend(ext_sst_dir.path());

        let result = importer.download(
            &meta,
            &backend,
            "sample.sst",
            &RewriteRule::default(),
            None,
            Limiter::new(f64::INFINITY),
            db,
        );
        match &result {
            Err(Error::EngineTraits(TraitError::Engine(s)))
                if s.state().starts_with("Corruption:") => {}
            _ => panic!("unexpected download result: {:?}", result),
        }
    }

    #[test]
    fn test_download_sst_empty() {
        let (_ext_sst_dir, backend, mut meta) = create_sample_external_sst_file().unwrap();
        let importer_dir = tempfile::tempdir().unwrap();
        let cfg = Config::default();
        let importer =
            SstImporter::<TestEngine>::new(&cfg, &importer_dir, None, ApiVersion::V1, false)
                .unwrap();
        let db = create_sst_test_engine().unwrap();
        meta.mut_range().set_start(vec![b'x']);
        meta.mut_range().set_end(vec![b'y']);

        let result = importer.download(
            &meta,
            &backend,
            "sample.sst",
            &RewriteRule::default(),
            None,
            Limiter::new(f64::INFINITY),
            db,
        );

        let path = importer.dir.join_for_write(&meta).unwrap();
        assert!(!file_system::file_exists(path.save));

        match result {
            Ok(None) => {}
            _ => panic!("unexpected download result: {:?}", result),
        }
    }

    #[test]
    fn test_download_sst_wrong_key_prefix() {
        let (_ext_sst_dir, backend, meta) = create_sample_external_sst_file().unwrap();
        let importer_dir = tempfile::tempdir().unwrap();
        let cfg = Config::default();
        let importer =
            SstImporter::<TestEngine>::new(&cfg, &importer_dir, None, ApiVersion::V1, false)
                .unwrap();
        let db = create_sst_test_engine().unwrap();

        let result = importer.download(
            &meta,
            &backend,
            "sample.sst",
            &new_rewrite_rule(b"xxx", b"yyy", 0),
            None,
            Limiter::new(f64::INFINITY),
            db,
        );

        match &result {
            Err(Error::WrongKeyPrefix { key, prefix, .. }) => {
                assert_eq!(key, b"t123_r01");
                assert_eq!(prefix, b"xxx");
            }
            _ => panic!("unexpected download result: {:?}", result),
        }
    }

    #[test]
    fn test_download_rawkv_sst() {
        test_download_rawkv_sst_impl(ApiVersion::V1);
        test_download_rawkv_sst_impl(ApiVersion::V1ttl);
        test_download_rawkv_sst_impl(ApiVersion::V2);
    }

    fn test_download_rawkv_sst_impl(api_version: ApiVersion) {
        // creates a sample SST file.
        let (_ext_sst_dir, backend, meta) =
            create_sample_external_rawkv_sst_file(b"0", b"z", false).unwrap();

        // performs the download.
        let importer_dir = tempfile::tempdir().unwrap();
        let cfg = Config::default();
        let importer =
            SstImporter::<TestEngine>::new(&cfg, &importer_dir, None, api_version, false).unwrap();
        let db = create_sst_test_engine().unwrap();

        let range = importer
            .download(
                &meta,
                &backend,
                "sample.sst",
                &RewriteRule::default(),
                None,
                Limiter::new(f64::INFINITY),
                db,
            )
            .unwrap()
            .unwrap();

        assert_eq!(range.get_start(), b"a");
        assert_eq!(range.get_end(), b"d");

        // verifies that the file is saved to the correct place.
        let sst_file_path = importer.dir.join_for_read(&meta).unwrap().save;
        let sst_file_metadata = sst_file_path.metadata().unwrap();
        assert!(sst_file_metadata.is_file());
        assert_eq!(sst_file_metadata.len(), meta.get_length());

        // verifies the SST content is correct.
        let sst_reader = new_sst_reader(sst_file_path.to_str().unwrap(), None);
        sst_reader.verify_checksum().unwrap();
        let mut iter = sst_reader.iter(IterOptions::default()).unwrap();
        iter.seek_to_first().unwrap();
        assert_eq!(
            collect(iter),
            vec![
                (b"za".to_vec(), b"v1".to_vec()),
                (b"zb".to_vec(), b"v2".to_vec()),
                (b"zb\x00".to_vec(), b"v3".to_vec()),
                (b"zc".to_vec(), b"v4".to_vec()),
                (b"zc\x00".to_vec(), b"v5".to_vec()),
                (b"zc\x00\x00".to_vec(), b"v6".to_vec()),
                (b"zd".to_vec(), b"v7".to_vec()),
            ]
        );
    }

    #[test]
    fn test_download_rawkv_sst_partial() {
        test_download_rawkv_sst_partial_impl(ApiVersion::V1);
        test_download_rawkv_sst_partial_impl(ApiVersion::V1ttl);
        test_download_rawkv_sst_partial_impl(ApiVersion::V2);
    }

    fn test_download_rawkv_sst_partial_impl(api_version: ApiVersion) {
        // creates a sample SST file.
        let (_ext_sst_dir, backend, meta) =
            create_sample_external_rawkv_sst_file(b"b", b"c\x00", false).unwrap();

        // performs the download.
        let importer_dir = tempfile::tempdir().unwrap();
        let cfg = Config::default();
        let importer =
            SstImporter::<TestEngine>::new(&cfg, &importer_dir, None, api_version, false).unwrap();
        let db = create_sst_test_engine().unwrap();

        let range = importer
            .download(
                &meta,
                &backend,
                "sample.sst",
                &RewriteRule::default(),
                None,
                Limiter::new(f64::INFINITY),
                db,
            )
            .unwrap()
            .unwrap();

        assert_eq!(range.get_start(), b"b");
        assert_eq!(range.get_end(), b"c\x00");

        // verifies that the file is saved to the correct place.
        let sst_file_path = importer.dir.join_for_read(&meta).unwrap().save;
        let sst_file_metadata = sst_file_path.metadata().unwrap();
        assert!(sst_file_metadata.is_file());

        // verifies the SST content is correct.
        let sst_reader = new_sst_reader(sst_file_path.to_str().unwrap(), None);
        sst_reader.verify_checksum().unwrap();
        let mut iter = sst_reader.iter(IterOptions::default()).unwrap();
        iter.seek_to_first().unwrap();
        assert_eq!(
            collect(iter),
            vec![
                (b"zb".to_vec(), b"v2".to_vec()),
                (b"zb\x00".to_vec(), b"v3".to_vec()),
                (b"zc".to_vec(), b"v4".to_vec()),
                (b"zc\x00".to_vec(), b"v5".to_vec()),
            ]
        );
    }

    #[test]
    fn test_download_rawkv_sst_partial_exclusive_end_key() {
        test_download_rawkv_sst_partial_exclusive_end_key_impl(ApiVersion::V1);
        test_download_rawkv_sst_partial_exclusive_end_key_impl(ApiVersion::V1ttl);
        test_download_rawkv_sst_partial_exclusive_end_key_impl(ApiVersion::V2);
    }

    fn test_download_rawkv_sst_partial_exclusive_end_key_impl(api_version: ApiVersion) {
        // creates a sample SST file.
        let (_ext_sst_dir, backend, meta) =
            create_sample_external_rawkv_sst_file(b"b", b"c\x00", true).unwrap();

        // performs the download.
        let importer_dir = tempfile::tempdir().unwrap();
        let cfg = Config::default();
        let importer =
            SstImporter::<TestEngine>::new(&cfg, &importer_dir, None, api_version, false).unwrap();
        let db = create_sst_test_engine().unwrap();

        let range = importer
            .download(
                &meta,
                &backend,
                "sample.sst",
                &RewriteRule::default(),
                None,
                Limiter::new(f64::INFINITY),
                db,
            )
            .unwrap()
            .unwrap();

        assert_eq!(range.get_start(), b"b");
        assert_eq!(range.get_end(), b"c");

        // verifies that the file is saved to the correct place.
        let sst_file_path = importer.dir.join_for_read(&meta).unwrap().save;
        let sst_file_metadata = sst_file_path.metadata().unwrap();
        assert!(sst_file_metadata.is_file());

        // verifies the SST content is correct.
        let sst_reader = new_sst_reader(sst_file_path.to_str().unwrap(), None);
        sst_reader.verify_checksum().unwrap();
        let mut iter = sst_reader.iter(IterOptions::default()).unwrap();
        iter.seek_to_first().unwrap();
        assert_eq!(
            collect(iter),
            vec![
                (b"zb".to_vec(), b"v2".to_vec()),
                (b"zb\x00".to_vec(), b"v3".to_vec()),
                (b"zc".to_vec(), b"v4".to_vec()),
            ]
        );
    }

    #[test]
    fn test_download_compression() {
        // creates a sample SST file.
        let (_ext_sst_dir, backend, meta) = create_sample_external_sst_file().unwrap();

        // performs the download.
        let importer_dir = tempfile::tempdir().unwrap();
        let cfg = Config::default();
        let mut importer =
            SstImporter::<TestEngine>::new(&cfg, &importer_dir, None, ApiVersion::V1, false)
                .unwrap();
        importer.set_compression_type(CF_DEFAULT, Some(SstCompressionType::Snappy));
        let db = create_sst_test_engine().unwrap();

        importer
            .download(
                &meta,
                &backend,
                "sample.sst",
                &new_rewrite_rule(b"t123", b"t789", 0),
                None,
                Limiter::new(f64::INFINITY),
                db,
            )
            .unwrap()
            .unwrap();

        // verifies the SST is compressed using Snappy.
        let sst_file_path = importer.dir.join_for_read(&meta).unwrap().save;
        assert!(sst_file_path.is_file());

        let sst_reader = new_sst_reader(sst_file_path.to_str().unwrap(), None);
        assert_eq!(sst_reader.compression_name(), "Snappy");
    }

    #[test]
    fn test_write_compression() {
        let mut meta = SstMeta::default();
        meta.set_uuid(Uuid::new_v4().as_bytes().to_vec());

        let importer_dir = tempfile::tempdir().unwrap();
        let cfg = Config::default();
        let mut importer =
            SstImporter::<TestEngine>::new(&cfg, &importer_dir, None, ApiVersion::V1, false)
                .unwrap();
        importer.set_compression_type(CF_DEFAULT, Some(Zstd));
        let db_path = importer_dir.path().join("db");
        let db = new_test_engine(db_path.to_str().unwrap(), DATA_CFS);

        let mut w = importer.new_txn_writer(&db, meta).unwrap();
        let mut batch = WriteBatch::default();
        let mut pairs = vec![];

        // put short value kv in write cf
        let mut pair = Pair::default();
        pair.set_key(b"k1".to_vec());
        pair.set_value(b"short_value".to_vec());
        pairs.push(pair);

        // put big value kv in default cf
        let big_value = vec![42; 256];
        let mut pair = Pair::default();
        pair.set_key(b"k2".to_vec());
        pair.set_value(big_value);
        pairs.push(pair);

        // generate two cf metas
        batch.set_commit_ts(10);
        batch.set_pairs(pairs.into());
        w.write(batch).unwrap();

        let metas = w.finish().unwrap();
        assert_eq!(metas.len(), 2);

        // verifies SST compression algorithm...
        for meta in metas {
            let sst_file_path = importer.dir.join_for_read(&meta).unwrap().save;
            assert!(sst_file_path.is_file());

            let sst_reader = new_sst_reader(sst_file_path.to_str().unwrap(), None);
            let expected_compression_name = match &*meta.cf_name {
                CF_DEFAULT => "ZSTD",
                CF_WRITE => "LZ4", // Lz4 is the default if unspecified.
                _ => unreachable!(),
            };
            assert_eq!(sst_reader.compression_name(), expected_compression_name);
        }
    }

    #[test]
    fn test_import_support_download() {
        let import_dir = tempfile::tempdir().unwrap();
        let importer = SstImporter::<TestEngine>::new(
            &Config::default(),
            import_dir,
            None,
            ApiVersion::V1,
            false,
        )
        .unwrap();
        assert_eq!(importer.download_to_disk_only(), false);

        let import_dir = tempfile::tempdir().unwrap();
        let importer = SstImporter::<TestEngine>::new(
            &Config {
                memory_use_ratio: 0.0,
                ..Default::default()
            },
            import_dir,
            None,
            ApiVersion::V1,
            false,
        )
        .unwrap();
        assert_eq!(importer.download_to_disk_only(), true);
    }

    #[test]
    fn test_inc_mem_and_check() {
        // create importer object.
        let import_dir = tempfile::tempdir().unwrap();
        let importer = SstImporter::<TestEngine>::new(
            &Config::default(),
            import_dir,
            None,
            ApiVersion::V1,
            false,
        )
        .unwrap();
        assert_eq!(importer.memory_quota.in_use(), 0);

        // test inc_mem_and_check() and dec_mem() successfully.
        let meta = KvMeta {
            length: 100,
            ..Default::default()
        };
        let check = importer.request_memory(&meta);
        assert!(check.is_some());
        assert_eq!(importer.memory_quota.in_use() as u64, meta.get_length());

        drop(check);
        assert_eq!(importer.memory_quota.in_use(), 0);

        // test inc_mem_and_check() failed.
        let meta = KvMeta {
            length: u64::MAX,
            ..Default::default()
        };
        let check = importer.request_memory(&meta);
        assert!(check.is_none());
    }

    #[test]
    fn test_dashmap_lock() {
        let import_dir = tempfile::tempdir().unwrap();
        let importer = SstImporter::<TestEngine>::new(
            &Config::default(),
            import_dir,
            None,
            ApiVersion::V1,
            false,
        )
        .unwrap();

        let key = "file1";
        let r = Arc::new(OnceCell::new());
        let value = (CacheKvFile::Mem(r), Instant::now());
        let lock = importer.file_locks.entry(key.to_string()).or_insert(value);

        // test locked by try_entry()
        let lock2 = importer.file_locks.try_entry(key.to_string());
        assert!(lock2.is_none());
        let lock2 = importer.file_locks.try_get(key);
        assert!(lock2.is_locked());

        // test unlocked by entry()
        drop(lock);
        let v = importer.file_locks.get(key).unwrap();
        assert_eq!(v.0.ref_count(), 1);

        let _buff = v.0.clone();
        assert_eq!(v.0.ref_count(), 2);
    }

    #[test]
    fn test_download_kv_with_no_encryption() {
        // test both on disk and in mem case
        //
        test_download_kv_with_optional_encryption(None, Vec::new(), true, true);
        test_download_kv_with_optional_encryption(None, Vec::new(), false, true);
        test_download_kv_with_optional_encryption(None, Vec::new(), true, false);
        test_download_kv_with_optional_encryption(None, Vec::new(), false, false);
    }

    #[test]
    fn test_download_kv_with_plaintext_data_key() {
        let data_key: [u8; 32] = rand::thread_rng().gen();
        let mut cipher = CipherInfo::new();
        cipher.set_cipher_key(data_key.to_vec());
        cipher.set_cipher_type(EncryptionMethod::Aes256Ctr);

        // test both on disk and in mem case
        //
        test_download_kv_with_optional_encryption(Some(cipher.clone()), Vec::new(), true, true);
        test_download_kv_with_optional_encryption(Some(cipher.clone()), Vec::new(), false, true);
        test_download_kv_with_optional_encryption(Some(cipher.clone()), Vec::new(), true, false);
        test_download_kv_with_optional_encryption(Some(cipher), Vec::new(), false, false);
    }

    #[test]
    fn test_download_kv_with_master_key_based() {
        // set up file backed master key
        //
        let hex_bytes = encryption::test_utils::generate_random_master_key();
        let (path, _dir) = encryption::test_utils::create_master_key_file_test_only(&hex_bytes);

        let mut master_key_file_proto = MasterKeyFile::new();
        master_key_file_proto.set_path(path.to_string_lossy().into_owned());

        let mut master_key_proto = MasterKey::new();
        master_key_proto.set_file(master_key_file_proto);

        let master_key_proto_vec = vec![master_key_proto];

        // test both on disk and in mem case
        //
        test_download_kv_with_optional_encryption(None, master_key_proto_vec.clone(), true, true);
        test_download_kv_with_optional_encryption(None, master_key_proto_vec.clone(), false, true);
        test_download_kv_with_optional_encryption(None, master_key_proto_vec.clone(), true, false);
        test_download_kv_with_optional_encryption(None, master_key_proto_vec.clone(), false, false);
    }

    fn test_download_kv_with_optional_encryption(
        opt_cipher_info: Option<CipherInfo>,
        master_key_configs: Vec<MasterKey>,
        in_mem: bool,
        with_local_file_encryption: bool,
    ) {
        // set up external kv file
        //
        let (_dir, storage_backend, kv_meta, file_content) =
            create_sample_external_kv_file_with_optional_encryption(
                opt_cipher_info.clone(),
                master_key_configs.clone(),
                EncryptionMethod::Aes256Ctr,
                true,
            )
            .unwrap();

        // set up importer
        //
        let import_dir = tempfile::tempdir().unwrap();
        let opt_key_manager = if with_local_file_encryption {
            let (_, key_manager) = new_key_manager_for_test();
            Some(key_manager)
        } else {
            None
        };
        let cfg = Config {
            memory_use_ratio: if in_mem { 0.5 } else { 0.0 },
            ..Default::default()
        };
        let importer = SstImporter::<TestEngine>::new(
            &cfg,
            import_dir,
            opt_key_manager.clone(),
            ApiVersion::V1,
            false,
        )
        .unwrap();
        let ext_storage = {
            importer.auto_encrypt_local_file_if_needed(
                importer
                    .external_storage_or_cache(&storage_backend, "")
                    .unwrap(),
            )
        };
        let path = importer
            .dir
            .get_import_path(
                format!("{}_{}", kv_meta.get_name(), kv_meta.get_range_offset()).as_str(),
            )
            .unwrap();

        let output = block_on_external_io(importer.download_kv_file(
            &kv_meta,
            ext_storage,
            &storage_backend,
            &Limiter::new(f64::INFINITY),
            opt_cipher_info,
            master_key_configs,
        ))
        .unwrap();
        assert_eq!(*output, file_content);
        if !in_mem {
            check_file_exists(&path.save, opt_key_manager.as_deref());
        }
    }
}
