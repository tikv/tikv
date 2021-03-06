// Copyright 2018 TiKV Project Authors. Licensed under Apache-2.0.

use std::borrow::Cow;
use std::fmt;
use std::io::{self, Write};
use std::marker::Unpin;
use std::ops::Bound;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, Instant};

use futures_util::io::{AsyncRead, AsyncReadExt};
use kvproto::backup::StorageBackend;
#[cfg(feature = "prost-codec")]
use kvproto::import_sstpb::pair::Op as PairOp;
#[cfg(not(feature = "prost-codec"))]
use kvproto::import_sstpb::PairOp;
use kvproto::import_sstpb::*;
use tokio::time::timeout;
use uuid::{Builder as UuidBuilder, Uuid};

use encryption::{DataKeyManager, EncrypterWriter};
use engine_rocks::{
    encryption::get_env as get_encrypted_env, file_system::get_env as get_inspected_env,
    RocksSstReader,
};
use engine_traits::{
    EncryptionKeyManager, IngestExternalFileOptions, Iterator, KvEngine, SeekKey, SstReader,
    SstWriter, SstWriterBuilder, CF_DEFAULT, CF_WRITE,
};
use external_storage::{create_storage, url_of_backend};
use file_system::{sync_dir, File, OpenOptions};
use tikv_util::stream::{block_on_external_io, READ_BUF_SIZE};
use tikv_util::time::Limiter;
use txn_types::{is_short_value, Key, TimeStamp, Write as KvWrite, WriteRef, WriteType};

use super::{Error, Result};
use crate::metrics::*;

/// SSTImporter manages SST files that are waiting for ingesting.
pub struct SSTImporter {
    dir: ImportDir,
    key_manager: Option<Arc<DataKeyManager>>,
}

impl SSTImporter {
    pub fn new<P: AsRef<Path>>(
        root: P,
        key_manager: Option<Arc<DataKeyManager>>,
    ) -> Result<SSTImporter> {
        Ok(SSTImporter {
            dir: ImportDir::new(root)?,
            key_manager,
        })
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

    pub fn ingest<E: KvEngine>(&self, meta: &SstMeta, engine: &E) -> Result<()> {
        match self.dir.ingest(meta, engine, self.key_manager.as_deref()) {
            Ok(_) => {
                info!("ingest"; "meta" => ?meta);
                Ok(())
            }
            Err(e) => {
                error!(%e; "ingest failed"; "meta" => ?meta, );
                Err(e)
            }
        }
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
        speed_limiter: Limiter,
        sst_writer: E::SstWriter,
    ) -> Result<Option<Range>> {
        debug!("download start";
            "meta" => ?meta,
            "url" => ?backend,
            "name" => name,
            "rewrite_rule" => ?rewrite_rule,
            "speed_limit" => speed_limiter.speed_limit(),
        );
        match self.do_download::<E>(meta, backend, name, rewrite_rule, speed_limiter, sst_writer) {
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

    async fn read_external_storage_into_file(
        input: &mut (dyn AsyncRead + Unpin),
        output: &mut dyn Write,
        speed_limiter: &Limiter,
        expected_length: u64,
        min_read_speed: usize,
    ) -> io::Result<()> {
        let dur = Duration::from_secs((READ_BUF_SIZE / min_read_speed) as u64);

        // do the I/O copy from external_storage to the local file.
        let mut buffer = vec![0u8; READ_BUF_SIZE];
        let mut file_length = 0;

        loop {
            // separate the speed limiting from actual reading so it won't
            // affect the timeout calculation.
            let bytes_read = timeout(dur, input.read(&mut buffer))
                .await
                .map_err(|_| io::ErrorKind::TimedOut)??;
            if bytes_read == 0 {
                break;
            }
            speed_limiter.consume(bytes_read).await;
            output.write_all(&buffer[..bytes_read])?;
            file_length += bytes_read as u64;
        }

        if expected_length != 0 && expected_length != file_length {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!(
                    "downloaded size {}, expected {}",
                    file_length, expected_length
                ),
            ));
        }

        IMPORTER_DOWNLOAD_BYTES.observe(file_length as _);

        Ok(())
    }

    fn do_download<E: KvEngine>(
        &self,
        meta: &SstMeta,
        backend: &StorageBackend,
        name: &str,
        rewrite_rule: &RewriteRule,
        speed_limiter: Limiter,
        mut sst_writer: E::SstWriter,
    ) -> Result<Option<Range>> {
        let path = self.dir.join(meta)?;
        let url = url_of_backend(backend);

        {
            let start_read = Instant::now();

            // prepare to download the file from the external_storage
            let ext_storage = create_storage(backend)?;
            let mut ext_reader = ext_storage.read(name);

            let mut plain_file;
            let mut encrypted_file;
            let file_writer: &mut dyn Write = if let Some(key_manager) = &self.key_manager {
                encrypted_file = key_manager.create_file(&path.temp)?;
                &mut encrypted_file
            } else {
                plain_file = File::create(&path.temp)?;
                &mut plain_file
            };

            // the minimum speed of reading data, in bytes/second.
            // if reading speed is slower than this rate, we will stop with
            // a "TimedOut" error.
            // (at 8 KB/s for a 2 MB buffer, this means we timeout after 4m16s.)
            const MINIMUM_READ_SPEED: usize = 8192;

            block_on_external_io(Self::read_external_storage_into_file(
                &mut ext_reader,
                file_writer,
                &speed_limiter,
                meta.length,
                MINIMUM_READ_SPEED,
            ))
            .map_err(|e| {
                Error::CannotReadExternalStorage(
                    url.to_string(),
                    name.to_owned(),
                    path.temp.to_owned(),
                    e,
                )
            })?;

            OpenOptions::new()
                .append(true)
                .open(&path.temp)?
                .sync_data()?;

            IMPORTER_DOWNLOAD_DURATION
                .with_label_values(&["read"])
                .observe(start_read.elapsed().as_secs_f64());
        }

        // now validate the SST file.
        let path_str = path.temp.to_str().unwrap();
        let env = get_encrypted_env(self.key_manager.clone(), None /*base_env*/)?;
        let env = get_inspected_env(Some(env))?;
        // Use abstracted SstReader after Env is abstracted.
        let sst_reader = RocksSstReader::open_with_env(path_str, Some(env))?;
        sst_reader.verify_checksum()?;

        debug!("downloaded file and verified";
            "meta" => ?meta,
            "url" => %url,
            "name" => name,
            "path" => path_str,
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
                .map_err(|_| {
                    Error::WrongKeyPrefix(
                        "SST start range",
                        range_start.to_vec(),
                        new_prefix.to_vec(),
                    )
                })?;
        let range_end =
            keys::rewrite::rewrite_prefix_of_end_bound(new_prefix, old_prefix, range_end_bound)
                .map_err(|_| {
                    Error::WrongKeyPrefix("SST end range", range_end.to_vec(), new_prefix.to_vec())
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
                    .ok_or_else(|| Error::InvalidSSTPath(path.temp.clone()))?;
                let save_str = path
                    .save
                    .to_str()
                    .ok_or_else(|| Error::InvalidSSTPath(path.save.clone()))?;
                key_manager.link_file(temp_str, save_str)?;
                key_manager.delete_file(temp_str)?;
            }
            IMPORTER_DOWNLOAD_DURATION
                .with_label_values(&["rename"])
                .observe(start_rename_rewrite.elapsed().as_secs_f64());
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
        while iter.valid()? {
            let old_key = keys::origin_key(iter.key());
            if is_after_end_bound(&old_key, &range_end) {
                break;
            }
            if !old_key.starts_with(old_prefix) {
                return Err(Error::WrongKeyPrefix(
                    "Key in SST",
                    keys::origin_key(iter.key()).to_vec(),
                    old_prefix.to_vec(),
                ));
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
            .observe(start_rename_rewrite.elapsed().as_secs_f64());

        if let Some(start_key) = first_key {
            let start_finish = Instant::now();
            sst_writer.finish()?;
            IMPORTER_DOWNLOAD_DURATION
                .with_label_values(&["finish"])
                .observe(start_finish.elapsed().as_secs_f64());

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

    pub fn new_writer<E: KvEngine>(&self, db: &E, meta: SstMeta) -> Result<SSTWriter<E>> {
        let mut default_meta = meta.clone();
        default_meta.set_cf_name(CF_DEFAULT.to_owned());
        let default_path = self.dir.join(&default_meta)?;
        let default = E::SstWriterBuilder::new()
            .set_db(&db)
            .set_cf(CF_DEFAULT)
            .build(default_path.temp.to_str().unwrap())
            .unwrap();

        let mut write_meta = meta;
        write_meta.set_cf_name(CF_WRITE.to_owned());
        let write_path = self.dir.join(&write_meta)?;
        let write = E::SstWriterBuilder::new()
            .set_db(&db)
            .set_cf(CF_WRITE)
            .build(write_path.temp.to_str().unwrap())
            .unwrap();

        Ok(SSTWriter::new(
            default,
            write,
            default_path,
            write_path,
            default_meta,
            write_meta,
            self.key_manager.clone(),
        ))
    }
}

pub struct SSTWriter<E: KvEngine> {
    default: E::SstWriter,
    default_entries: u64,
    default_path: ImportPath,
    default_meta: SstMeta,
    write: E::SstWriter,
    write_entries: u64,
    write_path: ImportPath,
    write_meta: SstMeta,
    key_manager: Option<Arc<DataKeyManager>>,
}

impl<E: KvEngine> SSTWriter<E> {
    pub fn new(
        default: E::SstWriter,
        write: E::SstWriter,
        default_path: ImportPath,
        write_path: ImportPath,
        default_meta: SstMeta,
        write_meta: SstMeta,
        key_manager: Option<Arc<DataKeyManager>>,
    ) -> Self {
        SSTWriter {
            default,
            default_path,
            default_entries: 0,
            default_meta,
            write,
            write_path,
            write_entries: 0,
            write_meta,
            key_manager,
        }
    }

    pub fn write(&mut self, batch: WriteBatch) -> Result<()> {
        let commit_ts = TimeStamp::new(batch.get_commit_ts());
        for m in batch.get_pairs().iter() {
            let k = Key::from_raw(m.get_key()).append_ts(commit_ts);
            self.put(k.as_encoded(), m.get_value(), m.get_op())?;
        }
        Ok(())
    }

    fn put(&mut self, key: &[u8], value: &[u8], op: PairOp) -> Result<()> {
        let k = keys::data_key(key);
        let (_, commit_ts) = Key::split_on_ts_for(key)?;
        let w = match (op, is_short_value(value)) {
            (PairOp::Delete, _) => KvWrite::new(WriteType::Delete, commit_ts, None),
            (PairOp::Put, true) => KvWrite::new(WriteType::Put, commit_ts, Some(value.to_vec())),
            (PairOp::Put, false) => {
                self.default.put(&k, value)?;
                self.default_entries += 1;
                KvWrite::new(WriteType::Put, commit_ts, None)
            }
        };
        self.write.put(&k, &w.as_ref().to_bytes())?;
        self.write_entries += 1;
        Ok(())
    }

    pub fn finish(self) -> Result<Vec<SstMeta>> {
        let default_meta = self.default_meta.clone();
        let write_meta = self.write_meta.clone();
        let mut metas = Vec::with_capacity(2);
        let (default_entries, write_entries) = (self.default_entries, self.write_entries);
        let (p1, p2) = (self.default_path.clone(), self.write_path.clone());
        let (w1, w2, key_manager) = (self.default, self.write, self.key_manager);
        if default_entries > 0 {
            w1.finish()?;
            Self::save(p1, key_manager.as_deref())?;
            metas.push(default_meta);
        }
        if write_entries > 0 {
            w2.finish()?;
            Self::save(p2, key_manager.as_deref())?;
            metas.push(write_meta);
        }
        info!("finish write to sst";
            "default entries" => default_entries,
            "write entries" => write_entries,
        );
        Ok(metas)
    }

    // move file from temp to save.
    fn save(mut import_path: ImportPath, key_manager: Option<&DataKeyManager>) -> Result<()> {
        file_system::rename(&import_path.temp, &import_path.save)?;
        if let Some(key_manager) = key_manager {
            let temp_str = import_path
                .temp
                .to_str()
                .ok_or_else(|| Error::InvalidSSTPath(import_path.temp.clone()))?;
            let save_str = import_path
                .save
                .to_str()
                .ok_or_else(|| Error::InvalidSSTPath(import_path.save.clone()))?;
            key_manager.link_file(temp_str, save_str)?;
            key_manager.delete_file(temp_str)?;
        }
        // sync the directory after rename
        import_path.save.pop();
        sync_dir(&import_path.save)?;
        Ok(())
    }
}

/// ImportDir is responsible for operating SST files and related path
/// calculations.
///
/// The file being written is stored in `$root/.temp/$file_name`. After writing
/// is completed, the file is moved to `$root/$file_name`. The file generated
/// from the ingestion process will be placed in `$root/.clone/$file_name`.
///
/// TODO: Add size and rate limit.
pub struct ImportDir {
    root_dir: PathBuf,
    temp_dir: PathBuf,
    clone_dir: PathBuf,
}

impl ImportDir {
    const TEMP_DIR: &'static str = ".temp";
    const CLONE_DIR: &'static str = ".clone";

    fn new<P: AsRef<Path>>(root: P) -> Result<ImportDir> {
        let root_dir = root.as_ref().to_owned();
        let temp_dir = root_dir.join(Self::TEMP_DIR);
        let clone_dir = root_dir.join(Self::CLONE_DIR);
        if temp_dir.exists() {
            file_system::remove_dir_all(&temp_dir)?;
        }
        if clone_dir.exists() {
            file_system::remove_dir_all(&clone_dir)?;
        }
        file_system::create_dir_all(&temp_dir)?;
        file_system::create_dir_all(&clone_dir)?;
        Ok(ImportDir {
            root_dir,
            temp_dir,
            clone_dir,
        })
    }

    fn join(&self, meta: &SstMeta) -> Result<ImportPath> {
        let file_name = sst_meta_to_path(meta)?;
        let save_path = self.root_dir.join(&file_name);
        let temp_path = self.temp_dir.join(&file_name);
        let clone_path = self.clone_dir.join(&file_name);
        Ok(ImportPath {
            save: save_path,
            temp: temp_path,
            clone: clone_path,
        })
    }

    fn create(
        &self,
        meta: &SstMeta,
        key_manager: Option<Arc<DataKeyManager>>,
    ) -> Result<ImportFile> {
        let path = self.join(meta)?;
        if path.save.exists() {
            return Err(Error::FileExists(path.save, "create SST upload cache"));
        }
        ImportFile::create(meta.clone(), path, key_manager)
    }

    fn delete_file(&self, path: &Path, key_manager: Option<&DataKeyManager>) -> Result<()> {
        if path.exists() {
            file_system::remove_file(&path)?;
            if let Some(manager) = key_manager {
                manager.delete_file(path.to_str().unwrap())?;
            }
        }

        Ok(())
    }

    fn delete(&self, meta: &SstMeta, manager: Option<&DataKeyManager>) -> Result<ImportPath> {
        let path = self.join(meta)?;
        self.delete_file(&path.save, manager)?;
        self.delete_file(&path.temp, manager)?;
        self.delete_file(&path.clone, manager)?;
        Ok(path)
    }

    fn exist(&self, meta: &SstMeta) -> Result<bool> {
        let path = self.join(meta)?;
        Ok(path.save.exists())
    }

    fn ingest<E: KvEngine>(
        &self,
        meta: &SstMeta,
        engine: &E,
        key_manager: Option<&DataKeyManager>,
    ) -> Result<()> {
        let start = Instant::now();
        let path = self.join(meta)?;
        let cf = meta.get_cf_name();
        super::prepare_sst_for_ingestion(&path.save, &path.clone, key_manager)?;
        let length = meta.get_length();
        let crc32 = meta.get_crc32();
        // FIXME perform validate_sst_for_ingestion after we can handle sst file size correctly.
        // currently we can not handle sst file size after rewrite,
        // we need re-compute length & crc32 and fill back to sstMeta.
        if length != 0 {
            if crc32 != 0 {
                // we only validate if the length and CRC32 are explicitly provided.
                engine.validate_sst_for_ingestion(cf, &path.clone, length, crc32)?;
            }
            IMPORTER_INGEST_BYTES.observe(length as _)
        }

        let mut opts = E::IngestExternalFileOptions::new();
        opts.move_files(true);
        engine.ingest_external_file_cf(cf, &opts, &[path.clone.to_str().unwrap()])?;
        IMPORTER_INGEST_DURATION
            .with_label_values(&["ingest"])
            .observe(start.elapsed().as_secs_f64());
        Ok(())
    }

    fn list_ssts(&self) -> Result<Vec<SstMeta>> {
        let mut ssts = Vec::new();
        for e in file_system::read_dir(&self.root_dir)? {
            let e = e?;
            if !e.file_type()?.is_file() {
                continue;
            }
            let path = e.path();
            match path_to_sst_meta(&path) {
                Ok(sst) => ssts.push(sst),
                Err(e) => error!(%e; "path_to_sst_meta failed"; "path" => %path.to_str().unwrap(),),
            }
        }
        Ok(ssts)
    }
}

#[derive(Clone)]
pub struct ImportPath {
    // The path of the file that has been uploaded.
    save: PathBuf,
    // The path of the file that is being uploaded.
    temp: PathBuf,
    // The path of the file that is going to be ingested.
    clone: PathBuf,
}

impl fmt::Debug for ImportPath {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ImportPath")
            .field("save", &self.save)
            .field("temp", &self.temp)
            .field("clone", &self.clone)
            .finish()
    }
}
// `SyncableWrite` extends io::Write with sync
trait SyncableWrite: io::Write + Send {
    // sync all metadata to storage
    fn sync(&self) -> io::Result<()>;
}

impl SyncableWrite for File {
    fn sync(&self) -> io::Result<()> {
        self.sync_all()
    }
}

impl SyncableWrite for EncrypterWriter<File> {
    fn sync(&self) -> io::Result<()> {
        self.sync_all()
    }
}

/// ImportFile is used to handle the writing and verification of SST files.
pub struct ImportFile {
    meta: SstMeta,
    path: ImportPath,
    file: Option<Box<dyn SyncableWrite>>,
    digest: crc32fast::Hasher,
    key_manager: Option<Arc<DataKeyManager>>,
}

impl ImportFile {
    fn create(
        meta: SstMeta,
        path: ImportPath,
        key_manager: Option<Arc<DataKeyManager>>,
    ) -> Result<ImportFile> {
        let file: Box<dyn SyncableWrite> = if let Some(ref manager) = key_manager {
            // key manager will truncate existed file, so we should check exist manually.
            if path.temp.exists() {
                return Err(Error::Io(io::Error::new(
                    io::ErrorKind::AlreadyExists,
                    format!("file already exists, {}", path.temp.to_str().unwrap()),
                )));
            }
            Box::new(manager.create_file(&path.temp)?)
        } else {
            Box::new(
                OpenOptions::new()
                    .write(true)
                    .create_new(true)
                    .open(&path.temp)?,
            )
        };
        Ok(ImportFile {
            meta,
            path,
            file: Some(file),
            digest: crc32fast::Hasher::new(),
            key_manager,
        })
    }

    pub fn append(&mut self, data: &[u8]) -> Result<()> {
        self.file.as_mut().unwrap().write_all(data)?;
        self.digest.update(data);
        Ok(())
    }

    pub fn finish(&mut self) -> Result<()> {
        self.validate()?;
        // sync is a wrapping for File::sync_all
        self.file.take().unwrap().sync()?;
        if self.path.save.exists() {
            return Err(Error::FileExists(
                self.path.save.clone(),
                "finalize SST write cache",
            ));
        }
        file_system::rename(&self.path.temp, &self.path.save)?;
        if let Some(ref manager) = self.key_manager {
            let tmp_str = self.path.temp.to_str().unwrap();
            let save_str = self.path.save.to_str().unwrap();
            manager.link_file(tmp_str, save_str)?;
            manager.delete_file(self.path.temp.to_str().unwrap())?;
        }
        Ok(())
    }

    fn cleanup(&mut self) -> Result<()> {
        self.file.take();
        if self.path.temp.exists() {
            if let Some(ref manager) = self.key_manager {
                manager.delete_file(self.path.temp.to_str().unwrap())?;
            }
            file_system::remove_file(&self.path.temp)?;
        }
        Ok(())
    }

    fn validate(&self) -> Result<()> {
        let crc32 = self.digest.clone().finalize();
        let expect = self.meta.get_crc32();
        if crc32 != expect {
            let reason = format!("crc32 {}, expect {}", crc32, expect);
            return Err(Error::FileCorrupted(self.path.temp.clone(), reason));
        }
        Ok(())
    }
}

impl Drop for ImportFile {
    fn drop(&mut self) {
        if let Err(e) = self.cleanup() {
            warn!("cleanup failed"; "file" => ?self, "err" => %e);
        }
    }
}

impl fmt::Debug for ImportFile {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ImportFile")
            .field("meta", &self.meta)
            .field("path", &self.path)
            .finish()
    }
}

const SST_SUFFIX: &str = ".sst";

pub fn sst_meta_to_path(meta: &SstMeta) -> Result<PathBuf> {
    Ok(PathBuf::from(format!(
        "{}_{}_{}_{}_{}{}",
        UuidBuilder::from_slice(meta.get_uuid())?.build(),
        meta.get_region_id(),
        meta.get_region_epoch().get_conf_ver(),
        meta.get_region_epoch().get_version(),
        meta.get_cf_name(),
        SST_SUFFIX,
    )))
}

fn path_to_sst_meta<P: AsRef<Path>>(path: P) -> Result<SstMeta> {
    let path = path.as_ref();
    let file_name = match path.file_name().and_then(|n| n.to_str()) {
        Some(name) => name,
        None => return Err(Error::InvalidSSTPath(path.to_owned())),
    };

    // A valid file name should be in the format:
    // "{uuid}_{region_id}_{region_epoch.conf_ver}_{region_epoch.version}_{cf}.sst"
    if !file_name.ends_with(SST_SUFFIX) {
        return Err(Error::InvalidSSTPath(path.to_owned()));
    }
    let elems: Vec<_> = file_name.trim_end_matches(SST_SUFFIX).split('_').collect();
    if elems.len() != 5 {
        return Err(Error::InvalidSSTPath(path.to_owned()));
    }

    let mut meta = SstMeta::default();
    let uuid = Uuid::parse_str(elems[0])?;
    meta.set_uuid(uuid.as_bytes().to_vec());
    meta.set_region_id(elems[1].parse()?);
    meta.mut_region_epoch().set_conf_ver(elems[2].parse()?);
    meta.mut_region_epoch().set_version(elems[3].parse()?);
    meta.set_cf_name(elems[4].to_owned());
    Ok(meta)
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
    use super::*;
    use test_sst_importer::*;

    use std::f64::INFINITY;

    use engine_traits::{
        collect, name_to_cf, EncryptionMethod, Iterable, Iterator, SeekKey, CF_DEFAULT, DATA_CFS,
    };
    use engine_traits::{Error as TraitError, SstWriterBuilder, TablePropertiesExt};
    use engine_traits::{
        ExternalSstFileInfo, SstExt, TableProperties, TablePropertiesCollection,
        UserCollectedProperties,
    };
    use tempfile::Builder;
    use test_sst_importer::{
        new_sst_reader, new_sst_writer, new_test_engine, RocksSstWriter, PROP_TEST_MARKER_CF_NAME,
    };
    use txn_types::{Value, WriteType};

    use test_util::new_test_key_manager;

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
                manager.create_file(&path.temp).unwrap();
                manager.create_file(&path.save).unwrap();
                manager.create_file(&path.clone).unwrap();
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
        let env = get_encrypted_env(key_manager.clone(), None /*base_env*/).unwrap();
        let db = new_test_engine_with_env(db_path.to_str().unwrap(), &[CF_DEFAULT], env);

        let cases = vec![(0, 10), (5, 15), (10, 20), (0, 100)];

        let mut ingested = Vec::new();

        for (i, &range) in cases.iter().enumerate() {
            let path = temp_dir.path().join(format!("{}.sst", i));
            let (meta, data) = gen_sst_file(&path, range);

            let mut f = dir.create(&meta, key_manager.clone()).unwrap();
            f.append(&data).unwrap();
            f.finish().unwrap();

            dir.ingest(&meta, &db, key_manager.as_deref()).unwrap();
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

    #[test]
    fn test_sst_meta_to_path() {
        let mut meta = SstMeta::default();
        let uuid = Uuid::new_v4();
        meta.set_uuid(uuid.as_bytes().to_vec());
        meta.set_region_id(1);
        meta.set_cf_name(CF_DEFAULT.to_owned());
        meta.mut_region_epoch().set_conf_ver(2);
        meta.mut_region_epoch().set_version(3);

        let path = sst_meta_to_path(&meta).unwrap();
        let expected_path = format!("{}_1_2_3_default.sst", uuid);
        assert_eq!(path.to_str().unwrap(), &expected_path);

        let new_meta = path_to_sst_meta(path).unwrap();
        assert_eq!(meta, new_meta);
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

        let backend = external_storage::make_local_backend(ext_sst_dir.path());
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

    fn create_sample_external_sst_file_txn_default(
    ) -> Result<(tempfile::TempDir, StorageBackend, SstMeta)> {
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

    fn create_sample_external_sst_file_txn_write(
    ) -> Result<(tempfile::TempDir, StorageBackend, SstMeta)> {
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

    fn create_sst_writer_with_db(
        importer: &SSTImporter,
        meta: &SstMeta,
    ) -> Result<<TestEngine as SstExt>::SstWriter> {
        let temp_dir = Builder::new().prefix("test_import_dir").tempdir().unwrap();
        let db_path = temp_dir.path().join("db");
        let db = new_test_engine(db_path.to_str().unwrap(), DATA_CFS);
        let sst_writer = <TestEngine as SstExt>::SstWriterBuilder::new()
            .set_db(&db)
            .set_cf(name_to_cf(meta.get_cf_name()).unwrap())
            .build(importer.get_path(meta).to_str().unwrap())
            .unwrap();
        Ok(sst_writer)
    }

    #[test]
    fn test_read_external_storage_into_file() {
        let data = &b"some input data"[..];
        let mut input = data;
        let mut output = Vec::new();
        let input_len = input.len() as u64;
        block_on_external_io(SSTImporter::read_external_storage_into_file(
            &mut input,
            &mut output,
            &Limiter::new(INFINITY),
            input_len,
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
        let err = block_on_external_io(SSTImporter::read_external_storage_into_file(
            &mut input,
            &mut output,
            &Limiter::new(INFINITY),
            0,
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
        let importer = SSTImporter::new(&importer_dir, None).unwrap();
        let sst_writer = create_sst_writer_with_db(&importer, &meta).unwrap();

        let range = importer
            .download::<TestEngine>(
                &meta,
                &backend,
                "sample.sst",
                &RewriteRule::default(),
                Limiter::new(INFINITY),
                sst_writer,
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
        let sst_reader = new_sst_reader(sst_file_path.to_str().unwrap());
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
        let importer = SSTImporter::new(&importer_dir, None).unwrap();
        let sst_writer = create_sst_writer_with_db(&importer, &meta).unwrap();

        let range = importer
            .download::<TestEngine>(
                &meta,
                &backend,
                "sample.sst",
                &new_rewrite_rule(b"t123", b"t567", 0),
                Limiter::new(INFINITY),
                sst_writer,
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
        let sst_reader = new_sst_reader(sst_file_path.to_str().unwrap());
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
        let importer = SSTImporter::new(&importer_dir, None).unwrap();

        // creates a sample SST file.
        let (_ext_sst_dir, backend, meta) = create_sample_external_sst_file_txn_default().unwrap();
        let sst_writer = create_sst_writer_with_db(&importer, &meta).unwrap();

        let _ = importer
            .download::<TestEngine>(
                &meta,
                &backend,
                "sample_default.sst",
                &new_rewrite_rule(b"", b"", 16),
                Limiter::new(INFINITY),
                sst_writer,
            )
            .unwrap()
            .unwrap();

        // verifies that the file is saved to the correct place.
        // (the file size may be changed, so not going to check the file size)
        let sst_file_path = importer.dir.join(&meta).unwrap().save;
        assert!(sst_file_path.is_file());

        // verifies the SST content is correct.
        let sst_reader = new_sst_reader(sst_file_path.to_str().unwrap());
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
        let importer = SSTImporter::new(&importer_dir, None).unwrap();

        // creates a sample SST file.
        let (_ext_sst_dir, backend, meta) = create_sample_external_sst_file_txn_write().unwrap();
        let sst_writer = create_sst_writer_with_db(&importer, &meta).unwrap();

        let _ = importer
            .download::<TestEngine>(
                &meta,
                &backend,
                "sample_write.sst",
                &new_rewrite_rule(b"", b"", 16),
                Limiter::new(INFINITY),
                sst_writer,
            )
            .unwrap()
            .unwrap();

        // verifies that the file is saved to the correct place.
        // (the file size may be changed, so not going to check the file size)
        let sst_file_path = importer.dir.join(&meta).unwrap().save;
        assert!(sst_file_path.is_file());

        // verifies the SST content is correct.
        let sst_reader = new_sst_reader(sst_file_path.to_str().unwrap());
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
            let importer = SSTImporter::new(&importer_dir, None).unwrap();
            let sst_writer = create_sst_writer_with_db(&importer, &meta).unwrap();

            let range = importer
                .download::<TestEngine>(
                    &meta,
                    &backend,
                    "sample.sst",
                    &new_rewrite_rule(b"t123", b"t9102", 0),
                    Limiter::new(INFINITY),
                    sst_writer,
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
            importer.ingest(&meta, &db).unwrap();

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
        let importer = SSTImporter::new(&importer_dir, None).unwrap();
        let sst_writer = create_sst_writer_with_db(&importer, &meta).unwrap();
        // note: the range doesn't contain the DATA_PREFIX 'z'.
        meta.mut_range().set_start(b"t123_r02".to_vec());
        meta.mut_range().set_end(b"t123_r12".to_vec());

        let range = importer
            .download::<TestEngine>(
                &meta,
                &backend,
                "sample.sst",
                &RewriteRule::default(),
                Limiter::new(INFINITY),
                sst_writer,
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
        let sst_reader = new_sst_reader(sst_file_path.to_str().unwrap());
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
        let importer = SSTImporter::new(&importer_dir, None).unwrap();
        let sst_writer = create_sst_writer_with_db(&importer, &meta).unwrap();
        meta.mut_range().set_start(b"t5_r02".to_vec());
        meta.mut_range().set_end(b"t5_r12".to_vec());

        let range = importer
            .download::<TestEngine>(
                &meta,
                &backend,
                "sample.sst",
                &new_rewrite_rule(b"t123", b"t5", 0),
                Limiter::new(INFINITY),
                sst_writer,
            )
            .unwrap()
            .unwrap();

        assert_eq!(range.get_start(), b"t5_r04");
        assert_eq!(range.get_end(), b"t5_r07");

        // verifies that the file is saved to the correct place.
        let sst_file_path = importer.dir.join(&meta).unwrap().save;
        assert!(sst_file_path.is_file());

        // verifies the SST content is correct.
        let sst_reader = new_sst_reader(sst_file_path.to_str().unwrap());
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
        let importer = SSTImporter::new(&importer_dir, None).unwrap();
        let sst_writer = create_sst_writer_with_db(&importer, &meta).unwrap();
        let backend = external_storage::make_local_backend(ext_sst_dir.path());

        let result = importer.download::<TestEngine>(
            &meta,
            &backend,
            "sample.sst",
            &RewriteRule::default(),
            Limiter::new(INFINITY),
            sst_writer,
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
        let importer = SSTImporter::new(&importer_dir, None).unwrap();
        let sst_writer = create_sst_writer_with_db(&importer, &meta).unwrap();
        meta.mut_range().set_start(vec![b'x']);
        meta.mut_range().set_end(vec![b'y']);

        let result = importer.download::<TestEngine>(
            &meta,
            &backend,
            "sample.sst",
            &RewriteRule::default(),
            Limiter::new(INFINITY),
            sst_writer,
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
        let importer = SSTImporter::new(&importer_dir, None).unwrap();
        let sst_writer = create_sst_writer_with_db(&importer, &meta).unwrap();

        let result = importer.download::<TestEngine>(
            &meta,
            &backend,
            "sample.sst",
            &new_rewrite_rule(b"xxx", b"yyy", 0),
            Limiter::new(INFINITY),
            sst_writer,
        );

        match &result {
            Err(Error::WrongKeyPrefix(_, key, prefix)) => {
                assert_eq!(key, b"t123_r01");
                assert_eq!(prefix, b"xxx");
            }
            _ => panic!("unexpected download result: {:?}", result),
        }
    }

    #[test]
    fn test_write_sst() {
        let mut meta = SstMeta::default();
        meta.set_uuid(Uuid::new_v4().as_bytes().to_vec());

        let importer_dir = tempfile::tempdir().unwrap();
        let importer = SSTImporter::new(&importer_dir, None).unwrap();
        let db_path = importer_dir.path().join("db");
        let db = new_test_engine(db_path.to_str().unwrap(), DATA_CFS);

        let mut w = importer.new_writer::<TestEngine>(&db, meta).unwrap();
        let mut batch = WriteBatch::default();
        let mut pairs = vec![];

        // put short value kv in wirte cf
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

        // put delete type key in write cf
        let mut pair = Pair::default();
        pair.set_key(b"k3".to_vec());
        pair.set_op(PairOp::Delete);
        pairs.push(pair);

        // generate two cf metas
        batch.set_commit_ts(10);
        batch.set_pairs(pairs.into());
        w.write(batch).unwrap();
        assert_eq!(w.write_entries, 3);
        assert_eq!(w.default_entries, 1);

        let metas = w.finish().unwrap();
        assert_eq!(metas.len(), 2);
    }

    #[test]
    fn test_download_rawkv_sst() {
        // creates a sample SST file.
        let (_ext_sst_dir, backend, meta) =
            create_sample_external_rawkv_sst_file(b"0", b"z", false).unwrap();

        // performs the download.
        let importer_dir = tempfile::tempdir().unwrap();
        let importer = SSTImporter::new(&importer_dir, None).unwrap();
        let sst_writer = create_sst_writer_with_db(&importer, &meta).unwrap();

        let range = importer
            .download::<TestEngine>(
                &meta,
                &backend,
                "sample.sst",
                &RewriteRule::default(),
                Limiter::new(INFINITY),
                sst_writer,
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
        let sst_reader = new_sst_reader(sst_file_path.to_str().unwrap());
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
        // creates a sample SST file.
        let (_ext_sst_dir, backend, meta) =
            create_sample_external_rawkv_sst_file(b"b", b"c\x00", false).unwrap();

        // performs the download.
        let importer_dir = tempfile::tempdir().unwrap();
        let importer = SSTImporter::new(&importer_dir, None).unwrap();
        let sst_writer = create_sst_writer_with_db(&importer, &meta).unwrap();

        let range = importer
            .download::<TestEngine>(
                &meta,
                &backend,
                "sample.sst",
                &RewriteRule::default(),
                Limiter::new(INFINITY),
                sst_writer,
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
        let sst_reader = new_sst_reader(sst_file_path.to_str().unwrap());
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
        // creates a sample SST file.
        let (_ext_sst_dir, backend, meta) =
            create_sample_external_rawkv_sst_file(b"b", b"c\x00", true).unwrap();

        // performs the download.
        let importer_dir = tempfile::tempdir().unwrap();
        let importer = SSTImporter::new(&importer_dir, None).unwrap();
        let sst_writer = create_sst_writer_with_db(&importer, &meta).unwrap();

        let range = importer
            .download::<TestEngine>(
                &meta,
                &backend,
                "sample.sst",
                &RewriteRule::default(),
                Limiter::new(INFINITY),
                sst_writer,
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
        let sst_reader = new_sst_reader(sst_file_path.to_str().unwrap());
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
}
