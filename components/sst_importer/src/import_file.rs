// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

use std::collections::HashMap;
use std::fmt;
use std::io::{self, Write};
use std::path::{Path, PathBuf};
use std::sync::Arc;

use encryption::{DataKeyManager, EncrypterWriter};
use engine_rocks::{
    encryption::get_env as get_encrypted_env, file_system::get_env as get_inspected_env,
    RocksSstReader,
};
use engine_traits::{EncryptionKeyManager, KvEngine, SSTMetaInfo, SstReader};
use file_system::{get_io_rate_limiter, sync_dir, File, OpenOptions};
use kvproto::import_sstpb::*;
use tikv_util::time::Instant;
use uuid::{Builder as UuidBuilder, Uuid};

use crate::metrics::*;
use crate::{Error, Result};

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

#[derive(Clone)]
pub struct ImportPath {
    // The path of the file that has been uploaded.
    pub save: PathBuf,
    // The path of the file that is being uploaded.
    pub temp: PathBuf,
    // The path of the file that is going to be ingested.
    pub clone: PathBuf,
}

impl ImportPath {
    // move file from temp to save.
    pub fn save(mut self, key_manager: Option<&DataKeyManager>) -> Result<()> {
        file_system::rename(&self.temp, &self.save)?;
        if let Some(key_manager) = key_manager {
            let temp_str = self
                .temp
                .to_str()
                .ok_or_else(|| Error::InvalidSSTPath(self.temp.clone()))?;
            let save_str = self
                .save
                .to_str()
                .ok_or_else(|| Error::InvalidSSTPath(self.save.clone()))?;
            key_manager.link_file(temp_str, save_str)?;
            key_manager.delete_file(temp_str)?;
        }
        // sync the directory after rename
        self.save.pop();
        sync_dir(&self.save)?;
        Ok(())
    }
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

/// ImportFile is used to handle the writing and verification of SST files.
pub struct ImportFile {
    meta: SstMeta,
    path: ImportPath,
    file: Option<Box<dyn SyncableWrite>>,
    digest: crc32fast::Hasher,
    key_manager: Option<Arc<DataKeyManager>>,
}

impl ImportFile {
    pub fn create(
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

impl fmt::Debug for ImportFile {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ImportFile")
            .field("meta", &self.meta)
            .field("path", &self.path)
            .finish()
    }
}

impl Drop for ImportFile {
    fn drop(&mut self) {
        if let Err(e) = self.cleanup() {
            warn!("cleanup failed"; "file" => ?self, "err" => %e);
        }
    }
}

/// ImportDir is responsible for operating SST files and related path
/// calculations.
///
/// The file being written is stored in `$root/.temp/$file_name`. After writing
/// is completed, the file is moved to `$root/$file_name`. The file generated
/// from the ingestion process will be placed in `$root/.clone/$file_name`.
pub struct ImportDir {
    root_dir: PathBuf,
    temp_dir: PathBuf,
    clone_dir: PathBuf,
}

impl ImportDir {
    const TEMP_DIR: &'static str = ".temp";
    const CLONE_DIR: &'static str = ".clone";

    pub fn new<P: AsRef<Path>>(root: P) -> Result<ImportDir> {
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

    pub fn join(&self, meta: &SstMeta) -> Result<ImportPath> {
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

    pub fn create(
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

    pub fn delete_file(&self, path: &Path, key_manager: Option<&DataKeyManager>) -> Result<()> {
        if path.exists() {
            file_system::remove_file(&path)?;
            if let Some(manager) = key_manager {
                manager.delete_file(path.to_str().unwrap())?;
            }
        }

        Ok(())
    }

    pub fn delete(&self, meta: &SstMeta, manager: Option<&DataKeyManager>) -> Result<ImportPath> {
        let path = self.join(meta)?;
        self.delete_file(&path.save, manager)?;
        self.delete_file(&path.temp, manager)?;
        self.delete_file(&path.clone, manager)?;
        Ok(path)
    }

    pub fn exist(&self, meta: &SstMeta) -> Result<bool> {
        let path = self.join(meta)?;
        Ok(path.save.exists())
    }

    pub fn validate(
        &self,
        meta: &SstMeta,
        key_manager: Option<Arc<DataKeyManager>>,
    ) -> Result<SSTMetaInfo> {
        let path = self.join(meta)?;
        let path_str = path.save.to_str().unwrap();
        let env = get_encrypted_env(key_manager, None /*base_env*/)?;
        let env = get_inspected_env(Some(env), get_io_rate_limiter())?;
        let sst_reader = RocksSstReader::open_with_env(&path_str, Some(env))?;
        sst_reader.verify_checksum()?;
        // TODO: check the length and crc32 of ingested file.
        let meta_info = sst_reader.sst_meta_info(meta.to_owned());
        Ok(meta_info)
    }

    pub fn ingest<E: KvEngine>(
        &self,
        metas: &[SstMeta],
        engine: &E,
        key_manager: Option<Arc<DataKeyManager>>,
    ) -> Result<()> {
        let start = Instant::now();

        let mut paths = HashMap::new();
        let mut ingest_bytes = 0;
        for meta in metas {
            let path = self.join(meta)?;
            let cf = meta.get_cf_name();
            super::prepare_sst_for_ingestion(&path.save, &path.clone, key_manager.as_deref())?;
            ingest_bytes += meta.get_length();
            paths.entry(cf).or_insert_with(Vec::new).push(path);
        }

        for (cf, cf_paths) in paths {
            let files: Vec<&str> = cf_paths.iter().map(|p| p.clone.to_str().unwrap()).collect();
            engine.ingest_external_file_cf(cf, &files)?;
        }
        INPORTER_INGEST_COUNT.observe(metas.len() as _);
        IMPORTER_INGEST_BYTES.observe(ingest_bytes as _);
        IMPORTER_INGEST_DURATION
            .with_label_values(&["ingest"])
            .observe(start.saturating_elapsed().as_secs_f64());
        Ok(())
    }

    pub fn list_ssts(&self) -> Result<Vec<SstMeta>> {
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

pub fn path_to_sst_meta<P: AsRef<Path>>(path: P) -> Result<SstMeta> {
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

#[cfg(test)]
mod test {
    use super::*;
    use engine_traits::CF_DEFAULT;

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
}
