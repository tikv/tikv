// Copyright 2018 TiKV Project Authors. Licensed under Apache-2.0.

use std::{
    borrow::Cow,
    collections::HashMap,
    fs::File,
    io::{prelude::*, BufReader},
    ops::Bound,
    path::{Path, PathBuf},
    sync::Arc,
};

use dashmap::DashMap;
use encryption::{encryption_method_to_db_encryption_method, DataKeyManager};
use engine_rocks::{get_env, RocksSstReader};
use engine_traits::{
    name_to_cf, util::check_key_in_range, CfName, EncryptionKeyManager, FileEncryptionInfo,
    Iterator, KvEngine, SeekKey, SstCompressionType, SstExt, SstMetaInfo, SstReader, SstWriter,
    SstWriterBuilder, CF_DEFAULT, CF_WRITE,
};
use file_system::{get_io_rate_limiter, OpenOptions};
use futures::executor::ThreadPool;
use kvproto::{
    brpb::{CipherInfo, StorageBackend},
    import_sstpb::*,
    kvrpcpb::ApiVersion,
};
use tikv_util::{
    codec::stream_event::{EventIterator, Iterator as EIterator},
    time::{Instant, Limiter},
};
use txn_types::{Key, TimeStamp, WriteRef};

use crate::{
    import_file::{ImportDir, ImportFile},
    import_mode::{ImportModeSwitcher, RocksDBMetricsFn},
    metrics::*,
    sst_writer::{RawSstWriter, TxnSstWriter},
    Config, Error, Result,
};

/// SstImporter manages SST files that are waiting for ingesting.
pub struct SstImporter {
    dir: ImportDir,
    key_manager: Option<Arc<DataKeyManager>>,
    switcher: ImportModeSwitcher,
    // TODO: lift api_version as a type parameter.
    api_version: ApiVersion,
    compression_types: HashMap<CfName, SstCompressionType>,
    file_locks: Arc<DashMap<String, ()>>,
}

impl SstImporter {
    pub fn new<P: AsRef<Path>>(
        cfg: &Config,
        root: P,
        key_manager: Option<Arc<DataKeyManager>>,
        api_version: ApiVersion,
    ) -> Result<SstImporter> {
        let switcher = ImportModeSwitcher::new(cfg);
        Ok(SstImporter {
            dir: ImportDir::new(root)?,
            key_manager,
            switcher,
            api_version,
            compression_types: HashMap::with_capacity(2),
            file_locks: Arc::new(DashMap::default()),
        })
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

    pub fn start_switch_mode_check<E: KvEngine>(&self, executor: &ThreadPool, db: E) {
        self.switcher.start(executor, db);
    }

    pub fn get_path(&self, meta: &SstMeta) -> PathBuf {
        let path = self.dir.join(meta).unwrap();
        path.save
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

    pub fn ingest<E: KvEngine>(&self, metas: &[SstMetaInfo], engine: &E) -> Result<()> {
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
    //  1. only KV pairs in the *inclusive* range (`[start, end]`) are used.
    //     (set the range to `["", ""]` to import everything).
    //  2. keys are rewritten according to the given rewrite rule.
    //
    // Both the range and rewrite keys are specified using origin keys. However,
    // the SST itself should be data keys (contain the `z` prefix). The range
    // should be specified using keys after rewriting, to be consistent with the
    // region info in PD.
    //
    // This method returns the *inclusive* key range (`[start, end]`) of SST
    // file created, or returns None if the SST is empty.
    pub fn download<E: KvEngine>(
        &self,
        meta: &SstMeta,
        backend: &StorageBackend,
        name: &str,
        rewrite_rule: &RewriteRule,
        crypter: Option<CipherInfo>,
        speed_limiter: Limiter,
        engine: E,
    ) -> Result<Option<Range>> {
        debug!("download start";
            "meta" => ?meta,
            "url" => ?backend,
            "name" => name,
            "rewrite_rule" => ?rewrite_rule,
            "speed_limit" => speed_limiter.speed_limit(),
        );
        match self.do_download::<E>(
            meta,
            backend,
            name,
            rewrite_rule,
            crypter,
            &speed_limiter,
            engine,
        ) {
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

    pub fn enter_normal_mode<E: KvEngine>(&self, db: E, mf: RocksDBMetricsFn) -> Result<bool> {
        self.switcher.enter_normal_mode(&db, mf)
    }

    pub fn enter_import_mode<E: KvEngine>(&self, db: E, mf: RocksDBMetricsFn) -> Result<bool> {
        self.switcher.enter_import_mode(&db, mf)
    }

    pub fn get_mode(&self) -> SwitchMode {
        self.switcher.get_mode()
    }

    fn download_file_from_external_storage(
        &self,
        file_length: u64,
        src_file_name: &str,
        dst_file: std::path::PathBuf,
        backend: &StorageBackend,
        expect_sha256: Option<Vec<u8>>,
        file_crypter: Option<FileEncryptionInfo>,
        speed_limiter: &Limiter,
    ) -> Result<()> {
        let start_read = Instant::now();
        // prepare to download the file from the external_storage
        // TODO: pass a config to support hdfs
        let ext_storage = external_storage_export::create_storage(backend, Default::default())?;
        let url = ext_storage.url()?.to_string();

        let ext_storage: Box<dyn external_storage_export::ExternalStorage> =
            if let Some(key_manager) = &self.key_manager {
                Box::new(external_storage_export::EncryptedExternalStorage {
                    key_manager: (*key_manager).clone(),
                    storage: ext_storage,
                }) as _
            } else {
                ext_storage as _
            };

        let result = ext_storage.restore(
            src_file_name,
            dst_file.clone(),
            file_length,
            expect_sha256,
            speed_limiter,
            file_crypter,
        );
        IMPORTER_DOWNLOAD_BYTES.observe(file_length as _);
        result.map_err(|e| Error::CannotReadExternalStorage {
            url: url.to_string(),
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

        debug!("downloaded file succeed";
            "name" => src_file_name,
            "url"  => %url,
        );
        Ok(())
    }

    pub fn do_download_kv_file(
        &self,
        meta: &KvMeta,
        backend: &StorageBackend,
        speed_limiter: &Limiter,
    ) -> Result<PathBuf> {
        let name = meta.get_name();
        let path = self.dir.get_import_path(name)?;
        let start = Instant::now();
        let sha256 = meta.get_sha256().to_vec();
        let expected_sha256 = if !sha256.is_empty() {
            Some(sha256)
        } else {
            None
        };
        if path.save.exists() {
            return Ok(path.save);
        }

        let lock = self.file_locks.entry(name.to_string()).or_default();

        if path.save.exists() {
            return Ok(path.save);
        }

        self.download_file_from_external_storage(
            // don't check file length after download file for now.
            meta.get_length(),
            name,
            path.temp.clone(),
            backend,
            expected_sha256,
            // don't support encrypt for now.
            None,
            speed_limiter,
        )?;
        info!("download file finished {}", name);

        if let Some(p) = path.save.parent() {
            // we have v1 prefix in file name.
            file_system::create_dir_all(p)?;
        }
        file_system::rename(path.temp, path.save.clone())?;

        drop(lock);
        self.file_locks.remove(name);

        IMPORTER_APPLY_DURATION
            .with_label_values(&["download"])
            .observe(start.saturating_elapsed().as_secs_f64());

        Ok(path.save)
    }

    pub fn do_apply_kv_file<P: AsRef<Path>>(
        &self,
        start_key: &[u8],
        end_key: &[u8],
        restore_ts: u64,
        file_path: P,
        rewrite_rule: &RewriteRule,
        build_fn: &mut dyn FnMut(Vec<u8>, Vec<u8>),
    ) -> Result<Option<Range>> {
        // iterator file and performs rewrites and apply.
        let file = File::open(&file_path)?;
        let mut reader = BufReader::new(file);
        let mut buffer = Vec::new();
        reader.read_to_end(&mut buffer)?;

        let mut event_iter = EventIterator::new(buffer);

        let old_prefix = rewrite_rule.get_old_key_prefix();
        let new_prefix = rewrite_rule.get_new_key_prefix();

        let perform_rewrite = old_prefix != new_prefix;

        // perform iteration and key rewrite.
        let mut key = new_prefix.to_vec();
        let new_prefix_data_key_len = key.len();
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
            let ts = Key::decode_ts_from(event_iter.key())?;
            if ts > TimeStamp::new(restore_ts) {
                // we assume the keys in file are sorted by ts.
                // so if we met the key not satisfy the ts.
                // we can easily filter the remain keys.
                ts_not_expected += 1;
                continue;
            }
            if perform_rewrite {
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

                debug!(
                    "perform rewrite new key: {:?}, new key prefix: {:?}, old key prefix: {:?}",
                    log_wrappers::Value::key(&key),
                    log_wrappers::Value::key(new_prefix),
                    log_wrappers::Value::key(old_prefix),
                );
            } else {
                key = event_iter.key().to_vec();
            }
            if check_key_in_range(&key, 0, start_key, end_key).is_err() {
                // key not in range, we can simply skip this key here.
                // the client make sure the correct region will download and apply the same file.
                INPORTER_APPLY_COUNT
                    .with_label_values(&["key_not_in_region"])
                    .inc();
                not_in_range += 1;
                continue;
            }
            let value = event_iter.value().to_vec();
            build_fn(key.clone(), value);

            let iter_key = key.clone();
            smallest_key = smallest_key.map_or_else(
                || Some(iter_key.clone()),
                |v: Vec<u8>| Some(v.min(iter_key.clone())),
            );

            largest_key = largest_key.map_or_else(
                || Some(iter_key.clone()),
                |v: Vec<u8>| Some(v.max(iter_key.clone())),
            );
        }
        info!("build download request file done"; "total keys" => %total_key,
            "ts filtered keys" => %ts_not_expected,
            "range filtered keys" => %not_in_range,
            "file" => %file_path.as_ref().display());

        let label = if perform_rewrite { "rewrite" } else { "normal" };
        IMPORTER_APPLY_DURATION
            .with_label_values(&[label])
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

    fn do_download<E: KvEngine>(
        &self,
        meta: &SstMeta,
        backend: &StorageBackend,
        name: &str,
        rewrite_rule: &RewriteRule,
        crypter: Option<CipherInfo>,
        speed_limiter: &Limiter,
        engine: E,
    ) -> Result<Option<Range>> {
        let path = self.dir.join(meta)?;

        let file_crypter = crypter.map(|c| FileEncryptionInfo {
            method: encryption_method_to_db_encryption_method(c.cipher_type),
            key: c.cipher_key,
            iv: meta.cipher_iv.to_owned(),
        });

        self.download_file_from_external_storage(
            meta.length,
            name,
            path.temp.clone(),
            backend,
            None,
            file_crypter,
            speed_limiter,
        )?;

        // now validate the SST file.
        let env = get_env(self.key_manager.clone(), get_io_rate_limiter())?;
        // Use abstracted SstReader after Env is abstracted.
        let dst_file_name = path.temp.to_str().unwrap();
        let sst_reader = RocksSstReader::open_with_env(dst_file_name, Some(env))?;
        sst_reader.verify_checksum()?;

        debug!("downloaded file and verified";
            "meta" => ?meta,
            "name" => name,
            "path" => dst_file_name,
        );

        // undo key rewrite so we could compare with the keys inside SST
        let old_prefix = rewrite_rule.get_old_key_prefix();
        let new_prefix = rewrite_rule.get_new_key_prefix();

        let range_start = meta.get_range().get_start();
        let range_end = meta.get_range().get_end();
        let range_start_bound = key_to_bound(range_start);
        let range_end_bound = if meta.get_end_key_exclusive() {
            key_to_exclusive_bound(range_end)
        } else {
            key_to_bound(range_end)
        };

        let range_start =
            keys::rewrite::rewrite_prefix_of_start_bound(new_prefix, old_prefix, range_start_bound)
                .map_err(|_| Error::WrongKeyPrefix {
                    what: "SST start range",
                    key: range_start.to_vec(),
                    prefix: new_prefix.to_vec(),
                })?;
        let range_end =
            keys::rewrite::rewrite_prefix_of_end_bound(new_prefix, old_prefix, range_end_bound)
                .map_err(|_| Error::WrongKeyPrefix {
                    what: "SST end range",
                    key: range_end.to_vec(),
                    prefix: new_prefix.to_vec(),
                })?;

        let start_rename_rewrite = Instant::now();
        // read the first and last keys from the SST, determine if we could
        // simply move the entire SST instead of iterating and generate a new one.
        let mut iter = sst_reader.iter();
        let direct_retval = (|| -> Result<Option<_>> {
            if rewrite_rule.old_key_prefix != rewrite_rule.new_key_prefix
                || rewrite_rule.new_timestamp != 0
            {
                // must iterate if we perform key rewrite
                return Ok(None);
            }
            if !iter.seek(SeekKey::Start)? {
                // the SST is empty, so no need to iterate at all (should be impossible?)
                return Ok(Some(meta.get_range().clone()));
            }
            let start_key = keys::origin_key(iter.key());
            if is_before_start_bound(start_key, &range_start) {
                // SST's start is before the range to consume, so needs to iterate to skip over
                return Ok(None);
            }
            let start_key = start_key.to_vec();

            // seek to end and fetch the last (inclusive) key of the SST.
            iter.seek(SeekKey::End)?;
            let last_key = keys::origin_key(iter.key());
            if is_after_end_bound(last_key, &range_end) {
                // SST's end is after the range to consume
                return Ok(None);
            }

            // range contained the entire SST, no need to iterate, just moving the file is ok
            let mut range = Range::default();
            range.set_start(start_key);
            range.set_end(last_key.to_vec());
            Ok(Some(range))
        })()?;

        if let Some(range) = direct_retval {
            file_system::rename(&path.temp, &path.save)?;
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
                key_manager.delete_file(temp_str)?;
            }
            IMPORTER_DOWNLOAD_DURATION
                .with_label_values(&["rename"])
                .observe(start_rename_rewrite.saturating_elapsed().as_secs_f64());
            return Ok(Some(range));
        }

        // perform iteration and key rewrite.
        let mut key = keys::data_key(new_prefix);
        let new_prefix_data_key_len = key.len();
        let mut first_key = None;

        match range_start {
            Bound::Unbounded => iter.seek(SeekKey::Start)?,
            Bound::Included(s) => iter.seek(SeekKey::Key(&keys::data_key(&s)))?,
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

        while iter.valid()? {
            let old_key = keys::origin_key(iter.key());
            if is_after_end_bound(old_key, &range_end) {
                break;
            }
            if !old_key.starts_with(old_prefix) {
                return Err(Error::WrongKeyPrefix {
                    what: "Key in SST",
                    key: keys::origin_key(iter.key()).to_vec(),
                    prefix: old_prefix.to_vec(),
                });
            }
            key.truncate(new_prefix_data_key_len);
            key.extend_from_slice(&old_key[old_prefix.len()..]);
            let mut value = Cow::Borrowed(iter.value());

            if rewrite_rule.new_timestamp != 0 {
                key = Key::from_encoded(key)
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

            sst_writer.put(&key, &value)?;
            iter.next()?;
            if first_key.is_none() {
                first_key = Some(keys::origin_key(&key).to_vec());
            }
        }

        let _ = file_system::remove_file(&path.temp);

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
            final_range.set_end(keys::origin_key(&key).to_vec());
            Ok(Some(final_range))
        } else {
            // nothing is written: prevents finishing the SST at all.
            Ok(None)
        }
    }

    pub fn list_ssts(&self) -> Result<Vec<SstMeta>> {
        self.dir.list_ssts()
    }

    pub fn new_txn_writer<E: KvEngine>(&self, db: &E, meta: SstMeta) -> Result<TxnSstWriter<E>> {
        let mut default_meta = meta.clone();
        default_meta.set_cf_name(CF_DEFAULT.to_owned());
        let default_path = self.dir.join(&default_meta)?;
        let default = E::SstWriterBuilder::new()
            .set_db(db)
            .set_cf(CF_DEFAULT)
            .set_compression_type(self.compression_types.get(CF_DEFAULT).copied())
            .build(default_path.temp.to_str().unwrap())
            .unwrap();

        let mut write_meta = meta;
        write_meta.set_cf_name(CF_WRITE.to_owned());
        let write_path = self.dir.join(&write_meta)?;
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

    pub fn new_raw_writer<E: KvEngine>(
        &self,
        db: &E,
        mut meta: SstMeta,
    ) -> Result<RawSstWriter<E>> {
        meta.set_cf_name(CF_DEFAULT.to_owned());
        let default_path = self.dir.join(&meta)?;
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
    use std::io;

    use engine_traits::{
        collect, EncryptionMethod, Error as TraitError, ExternalSstFileInfo, Iterable, Iterator,
        SeekKey, SstReader, SstWriter, CF_DEFAULT, DATA_CFS,
    };
    use file_system::File;
    use openssl::hash::{Hasher, MessageDigest};
    use tempfile::Builder;
    use test_sst_importer::*;
    use test_util::new_test_key_manager;
    use tikv_util::stream::block_on_external_io;
    use txn_types::{Value, WriteType};
    use uuid::Uuid;

    use super::*;
    use crate::{import_file::ImportPath, *};

    fn do_test_import_dir(key_manager: Option<Arc<DataKeyManager>>) {
        let temp_dir = Builder::new().prefix("test_import_dir").tempdir().unwrap();
        let dir = ImportDir::new(temp_dir.path()).unwrap();

        let mut meta = SstMeta::default();
        meta.set_uuid(Uuid::new_v4().as_bytes().to_vec());

        let path = dir.join(&meta).unwrap();

        // Test ImportDir::create()
        {
            let _file = dir.create(&meta, key_manager.clone()).unwrap();
            check_file_exists(&path.temp, key_manager.as_deref());
            check_file_not_exists(&path.save, key_manager.as_deref());
            check_file_not_exists(&path.clone, key_manager.as_deref());

            // Cannot create the same file again.
            assert!(dir.create(&meta, key_manager.clone()).is_err());
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
        let env = get_env(key_manager.clone(), None /*io_rate_limiter*/).unwrap();
        let db = new_test_engine_with_env(db_path.to_str().unwrap(), &[CF_DEFAULT], env);

        let cases = vec![(0, 10), (5, 15), (10, 20), (0, 100)];

        let mut ingested = Vec::new();

        for (i, &range) in cases.iter().enumerate() {
            let path = temp_dir.path().join(format!("{}.sst", i));
            let (meta, data) = gen_sst_file(&path, range);

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
                .find(|s| s.get_uuid() == sst.get_uuid())
                .unwrap();
            dir.delete(sst, key_manager.as_deref()).unwrap();
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
            assert!(
                ImportFile::create(meta.clone(), path.clone(), data_key_manager.clone()).is_err()
            );
            f.append(data).unwrap();
            // Invalid crc32 and length.
            assert!(f.finish().is_err());
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

    fn new_key_manager_for_test() -> (tempfile::TempDir, Arc<DataKeyManager>) {
        // test with tde
        let tmp_dir = tempfile::TempDir::new().unwrap();
        let key_manager = new_test_key_manager(&tmp_dir, None, None, None);
        assert!(key_manager.is_ok());
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
    ) -> Result<(tempfile::TempDir, StorageBackend, SstMeta)>
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

        let backend = external_storage_export::make_local_backend(ext_sst_dir.path());
        Ok((ext_sst_dir, backend, meta))
    }

    fn create_sample_external_sst_file() -> Result<(tempfile::TempDir, StorageBackend, SstMeta)> {
        create_external_sst_file_with_write_fn(|writer| {
            writer.put(b"zt123_r01", b"abc")?;
            writer.put(b"zt123_r04", b"xyz")?;
            writer.put(b"zt123_r07", b"pqrst")?;
            // writer.delete(b"t123_r10")?; // FIXME: can't handle DELETE ops yet.
            writer.put(b"zt123_r13", b"www")?;
            Ok(())
        })
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
            txn_types::Key::from_raw(key)
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

        let backend = external_storage_export::make_local_backend(ext_sst_dir.path());
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

        let backend = external_storage_export::make_local_backend(ext_sst_dir.path());
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

        block_on_external_io(external_storage_export::read_external_storage_into_file(
            &mut input,
            &mut output,
            &Limiter::new(f64::INFINITY),
            input_len,
            Some(hash256),
            8192,
        ))
        .unwrap();
        assert_eq!(&*output, data);
    }

    #[test]
    fn test_read_external_storage_into_file_timed_out() {
        use futures_util::stream::{pending, TryStreamExt};

        let mut input = pending::<io::Result<&[u8]>>().into_async_read();
        let mut output = Vec::new();
        let err = block_on_external_io(external_storage_export::read_external_storage_into_file(
            &mut input,
            &mut output,
            &Limiter::new(f64::INFINITY),
            0,
            None,
            usize::MAX,
        ))
        .unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::TimedOut);
    }

    #[test]
    fn test_download_sst_no_key_rewrite() {
        // creates a sample SST file.
        let (_ext_sst_dir, backend, meta) = create_sample_external_sst_file().unwrap();

        // performs the download.
        let importer_dir = tempfile::tempdir().unwrap();
        let cfg = Config::default();
        let importer = SstImporter::new(&cfg, &importer_dir, None, ApiVersion::V1).unwrap();
        let db = create_sst_test_engine().unwrap();

        let range = importer
            .download::<TestEngine>(
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
        let sst_file_path = importer.dir.join(&meta).unwrap().save;
        let sst_file_metadata = sst_file_path.metadata().unwrap();
        assert!(sst_file_metadata.is_file());
        assert_eq!(sst_file_metadata.len(), meta.get_length());

        // verifies the SST content is correct.
        let sst_reader = new_sst_reader(sst_file_path.to_str().unwrap(), None);
        sst_reader.verify_checksum().unwrap();
        let mut iter = sst_reader.iter();
        iter.seek(SeekKey::Start).unwrap();
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
        let importer = SstImporter::new(
            &cfg,
            &importer_dir,
            Some(key_manager.clone()),
            ApiVersion::V1,
        )
        .unwrap();

        let db_path = temp_dir.path().join("db");
        let env = get_env(Some(key_manager), None /*io_rate_limiter*/).unwrap();
        let db = new_test_engine_with_env(db_path.to_str().unwrap(), DATA_CFS, env.clone());

        let range = importer
            .download::<TestEngine>(
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
        let sst_file_path = importer.dir.join(&meta).unwrap().save;
        let sst_file_metadata = sst_file_path.metadata().unwrap();
        assert!(sst_file_metadata.is_file());
        assert_eq!(sst_file_metadata.len(), meta.get_length());

        // verifies the SST content is correct.
        let sst_reader = new_sst_reader(sst_file_path.to_str().unwrap(), Some(env));
        sst_reader.verify_checksum().unwrap();
        let mut iter = sst_reader.iter();
        iter.seek(SeekKey::Start).unwrap();
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
        let importer = SstImporter::new(&cfg, &importer_dir, None, ApiVersion::V1).unwrap();
        let db = create_sst_test_engine().unwrap();

        let range = importer
            .download::<TestEngine>(
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
        let sst_file_path = importer.dir.join(&meta).unwrap().save;
        assert!(sst_file_path.is_file());

        // verifies the SST content is correct.
        let sst_reader = new_sst_reader(sst_file_path.to_str().unwrap(), None);
        sst_reader.verify_checksum().unwrap();
        let mut iter = sst_reader.iter();
        iter.seek(SeekKey::Start).unwrap();
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
        let importer = SstImporter::new(&cfg, &importer_dir, None, ApiVersion::V1).unwrap();

        // creates a sample SST file.
        let (_ext_sst_dir, backend, meta) = create_sample_external_sst_file_txn_default().unwrap();
        let db = create_sst_test_engine().unwrap();

        let _ = importer
            .download::<TestEngine>(
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
        let sst_file_path = importer.dir.join(&meta).unwrap().save;
        assert!(sst_file_path.is_file());

        // verifies the SST content is correct.
        let sst_reader = new_sst_reader(sst_file_path.to_str().unwrap(), None);
        sst_reader.verify_checksum().unwrap();
        let mut iter = sst_reader.iter();
        iter.seek(SeekKey::Start).unwrap();
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
        let importer = SstImporter::new(&cfg, &importer_dir, None, ApiVersion::V1).unwrap();

        // creates a sample SST file.
        let (_ext_sst_dir, backend, meta) = create_sample_external_sst_file_txn_write().unwrap();
        let db = create_sst_test_engine().unwrap();

        let _ = importer
            .download::<TestEngine>(
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
        let sst_file_path = importer.dir.join(&meta).unwrap().save;
        assert!(sst_file_path.is_file());

        // verifies the SST content is correct.
        let sst_reader = new_sst_reader(sst_file_path.to_str().unwrap(), None);
        sst_reader.verify_checksum().unwrap();
        let mut iter = sst_reader.iter();
        iter.seek(SeekKey::Start).unwrap();
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
            let importer = SstImporter::new(&cfg, &importer_dir, None, ApiVersion::V1).unwrap();
            let db = create_sst_test_engine().unwrap();

            let range = importer
                .download::<TestEngine>(
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
            let _ = importer.ingest(&[meta_info.clone()], &db).unwrap();
            // key1 = "zt9102_r01", value1 = "abc", len = 13
            // key2 = "zt9102_r04", value2 = "xyz", len = 13
            // key3 = "zt9102_r07", value3 = "pqrst", len = 15
            // key4 = "zt9102_r13", value4 = "www", len = 13
            // total_bytes = (13 + 13 + 15 + 13) + 4 * 8 = 86
            // don't no why each key has extra 8 byte length in raw_key_size(), but it seems tolerable.
            // https://docs.rs/rocks/0.1.0/rocks/table_properties/struct.TableProperties.html#method.raw_key_size
            assert_eq!(meta_info.total_bytes, 86);
            assert_eq!(meta_info.total_kvs, 4);

            // verifies the DB content is correct.
            let mut iter = db.iterator_cf(cf).unwrap();
            iter.seek(SeekKey::Start).unwrap();
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
        let importer = SstImporter::new(&cfg, &importer_dir, None, ApiVersion::V1).unwrap();
        let db = create_sst_test_engine().unwrap();
        // note: the range doesn't contain the DATA_PREFIX 'z'.
        meta.mut_range().set_start(b"t123_r02".to_vec());
        meta.mut_range().set_end(b"t123_r12".to_vec());

        let range = importer
            .download::<TestEngine>(
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
        let sst_file_path = importer.dir.join(&meta).unwrap().save;
        assert!(sst_file_path.is_file());

        // verifies the SST content is correct.
        let sst_reader = new_sst_reader(sst_file_path.to_str().unwrap(), None);
        sst_reader.verify_checksum().unwrap();
        let mut iter = sst_reader.iter();
        iter.seek(SeekKey::Start).unwrap();
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
        let importer = SstImporter::new(&cfg, &importer_dir, None, ApiVersion::V1).unwrap();
        let db = create_sst_test_engine().unwrap();
        meta.mut_range().set_start(b"t5_r02".to_vec());
        meta.mut_range().set_end(b"t5_r12".to_vec());

        let range = importer
            .download::<TestEngine>(
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
        let sst_file_path = importer.dir.join(&meta).unwrap().save;
        assert!(sst_file_path.is_file());

        // verifies the SST content is correct.
        let sst_reader = new_sst_reader(sst_file_path.to_str().unwrap(), None);
        sst_reader.verify_checksum().unwrap();
        let mut iter = sst_reader.iter();
        iter.seek(SeekKey::Start).unwrap();
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
        let importer = SstImporter::new(&cfg, &importer_dir, None, ApiVersion::V1).unwrap();
        let db = create_sst_test_engine().unwrap();
        let backend = external_storage_export::make_local_backend(ext_sst_dir.path());

        let result = importer.download::<TestEngine>(
            &meta,
            &backend,
            "sample.sst",
            &RewriteRule::default(),
            None,
            Limiter::new(f64::INFINITY),
            db,
        );
        match &result {
            Err(Error::EngineTraits(TraitError::Engine(msg))) if msg.starts_with("Corruption:") => {
            }
            _ => panic!("unexpected download result: {:?}", result),
        }
    }

    #[test]
    fn test_download_sst_empty() {
        let (_ext_sst_dir, backend, mut meta) = create_sample_external_sst_file().unwrap();
        let importer_dir = tempfile::tempdir().unwrap();
        let cfg = Config::default();
        let importer = SstImporter::new(&cfg, &importer_dir, None, ApiVersion::V1).unwrap();
        let db = create_sst_test_engine().unwrap();
        meta.mut_range().set_start(vec![b'x']);
        meta.mut_range().set_end(vec![b'y']);

        let result = importer.download::<TestEngine>(
            &meta,
            &backend,
            "sample.sst",
            &RewriteRule::default(),
            None,
            Limiter::new(f64::INFINITY),
            db,
        );

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
        let importer = SstImporter::new(&cfg, &importer_dir, None, ApiVersion::V1).unwrap();
        let db = create_sst_test_engine().unwrap();

        let result = importer.download::<TestEngine>(
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
        let importer = SstImporter::new(&cfg, &importer_dir, None, api_version).unwrap();
        let db = create_sst_test_engine().unwrap();

        let range = importer
            .download::<TestEngine>(
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
        let sst_file_path = importer.dir.join(&meta).unwrap().save;
        let sst_file_metadata = sst_file_path.metadata().unwrap();
        assert!(sst_file_metadata.is_file());
        assert_eq!(sst_file_metadata.len(), meta.get_length());

        // verifies the SST content is correct.
        let sst_reader = new_sst_reader(sst_file_path.to_str().unwrap(), None);
        sst_reader.verify_checksum().unwrap();
        let mut iter = sst_reader.iter();
        iter.seek(SeekKey::Start).unwrap();
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
        let importer = SstImporter::new(&cfg, &importer_dir, None, api_version).unwrap();
        let db = create_sst_test_engine().unwrap();

        let range = importer
            .download::<TestEngine>(
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
        let sst_file_path = importer.dir.join(&meta).unwrap().save;
        let sst_file_metadata = sst_file_path.metadata().unwrap();
        assert!(sst_file_metadata.is_file());

        // verifies the SST content is correct.
        let sst_reader = new_sst_reader(sst_file_path.to_str().unwrap(), None);
        sst_reader.verify_checksum().unwrap();
        let mut iter = sst_reader.iter();
        iter.seek(SeekKey::Start).unwrap();
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
        let importer = SstImporter::new(&cfg, &importer_dir, None, api_version).unwrap();
        let db = create_sst_test_engine().unwrap();

        let range = importer
            .download::<TestEngine>(
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
        let sst_file_path = importer.dir.join(&meta).unwrap().save;
        let sst_file_metadata = sst_file_path.metadata().unwrap();
        assert!(sst_file_metadata.is_file());

        // verifies the SST content is correct.
        let sst_reader = new_sst_reader(sst_file_path.to_str().unwrap(), None);
        sst_reader.verify_checksum().unwrap();
        let mut iter = sst_reader.iter();
        iter.seek(SeekKey::Start).unwrap();
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
        let mut importer = SstImporter::new(&cfg, &importer_dir, None, ApiVersion::V1).unwrap();
        importer.set_compression_type(CF_DEFAULT, Some(SstCompressionType::Snappy));
        let db = create_sst_test_engine().unwrap();

        importer
            .download::<TestEngine>(
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
        let sst_file_path = importer.dir.join(&meta).unwrap().save;
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
        let mut importer = SstImporter::new(&cfg, &importer_dir, None, ApiVersion::V1).unwrap();
        importer.set_compression_type(CF_DEFAULT, Some(SstCompressionType::Zstd));
        let db_path = importer_dir.path().join("db");
        let db = new_test_engine(db_path.to_str().unwrap(), DATA_CFS);

        let mut w = importer.new_txn_writer::<TestEngine>(&db, meta).unwrap();
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
            let sst_file_path = importer.dir.join(&meta).unwrap().save;
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
}
