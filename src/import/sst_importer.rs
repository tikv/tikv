// Copyright 2018 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

use std::fmt;
use std::fs::{self, File, OpenOptions};
use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::Mutex;
use std::sync::atomic::{AtomicUsize, Ordering};

use crc::crc32::{self, Hasher32};
use uuid::Uuid;
use kvproto::importpb::*;

use util::collections::HashMap;

use super::{Error, Result};

pub type Token = usize;

pub struct SSTImporter {
    dir: ImportDir,
    token: AtomicUsize,
    files: Mutex<HashMap<Token, ImportFile>>,
}

impl SSTImporter {
    pub fn new<P: AsRef<Path>>(root: P) -> Result<SSTImporter> {
        Ok(SSTImporter {
            dir: ImportDir::new(root)?,
            token: AtomicUsize::new(1),
            files: Mutex::new(HashMap::default()),
        })
    }

    pub fn token(&self) -> Token {
        self.token.fetch_add(1, Ordering::SeqCst)
    }

    fn insert(&self, token: Token, file: ImportFile) {
        let mut files = self.files.lock().unwrap();
        assert!(files.insert(token, file).is_none());
    }

    pub fn remove(&self, token: Token) -> Option<ImportFile> {
        let mut files = self.files.lock().unwrap();
        files.remove(&token)
    }

    pub fn create(&self, token: Token, meta: &SSTMeta) -> Result<()> {
        let mut files = self.files.lock().unwrap();
        if files.contains_key(&token) {
            return Err(Error::TokenExists(token));
        }

        match self.dir.create(meta) {
            Ok(f) => {
                info!("create {:?}", f);
                files.insert(token, f);
                Ok(())
            }
            Err(e) => {
                error!("create {:?}: {:?}", meta, e);
                Err(e)
            }
        }
    }

    pub fn append(&self, token: Token, data: &[u8]) -> Result<()> {
        match self.remove(token) {
            Some(mut f) => match f.append(data) {
                Ok(_) => {
                    self.insert(token, f);
                    Ok(())
                }
                Err(e) => {
                    error!("append {:?}: {:?}", f, e);
                    Err(e)
                }
            },
            None => Err(Error::TokenNotFound(token)),
        }
    }

    pub fn finish(&self, token: Token) -> Result<()> {
        match self.remove(token) {
            Some(mut f) => match f.finish() {
                Ok(_) => {
                    info!("finish {:?}", f);
                    Ok(())
                }
                Err(e) => {
                    error!("finish {:?}: {:?}", f, e);
                    Err(e)
                }
            },
            None => Err(Error::TokenNotFound(token)),
        }
    }

    pub fn delete(&self, meta: &SSTMeta) -> Result<()> {
        match self.dir.delete(meta) {
            Ok(path) => {
                info!("delete {:?}", path);
                Ok(())
            }
            Err(e) => {
                error!("delete {:?}: {:?}", meta, e);
                Err(e)
            }
        }
    }
}

// TODO: Add size and rate limit.
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
            fs::remove_dir_all(&temp_dir)?;
        }
        if clone_dir.exists() {
            fs::remove_dir_all(&clone_dir)?;
        }
        fs::create_dir_all(&temp_dir)?;
        fs::create_dir_all(&clone_dir)?;
        Ok(ImportDir {
            root_dir: root_dir,
            temp_dir: temp_dir,
            clone_dir: clone_dir,
        })
    }

    fn join(&self, meta: &SSTMeta) -> Result<ImportPath> {
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

    fn create(&self, meta: &SSTMeta) -> Result<ImportFile> {
        let path = self.join(meta)?;
        if path.save.exists() {
            return Err(Error::FileExists(path.save));
        }
        ImportFile::create(meta.clone(), path)
    }

    fn delete(&self, meta: &SSTMeta) -> Result<ImportPath> {
        let path = self.join(meta)?;
        if path.save.exists() {
            fs::remove_file(&path.save)?;
        }
        if path.temp.exists() {
            fs::remove_file(&path.temp)?;
        }
        if path.clone.exists() {
            fs::remove_file(&path.clone)?;
        }
        Ok(path)
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
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.debug_struct("ImportPath")
            .field("save", &self.save)
            .field("temp", &self.temp)
            .field("clone", &self.clone)
            .finish()
    }
}

pub struct ImportFile {
    meta: SSTMeta,
    path: ImportPath,
    file: Option<File>,
    digest: crc32::Digest,
}

impl ImportFile {
    fn create(meta: SSTMeta, path: ImportPath) -> Result<ImportFile> {
        let file = OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&path.temp)?;
        Ok(ImportFile {
            meta: meta,
            path: path,
            file: Some(file),
            digest: crc32::Digest::new(crc32::IEEE),
        })
    }

    fn append(&mut self, data: &[u8]) -> Result<()> {
        self.file.as_mut().unwrap().write_all(data)?;
        self.digest.write(data);
        Ok(())
    }

    fn finish(&mut self) -> Result<()> {
        self.validate()?;
        self.file.take().unwrap().sync_all()?;
        if self.path.save.exists() {
            return Err(Error::FileExists(self.path.save.clone()));
        }
        fs::rename(&self.path.temp, &self.path.save)?;
        Ok(())
    }

    fn cleanup(&mut self) -> Result<()> {
        self.file.take();
        if self.path.temp.exists() {
            fs::remove_file(&self.path.temp)?;
        }
        Ok(())
    }

    fn validate(&self) -> Result<()> {
        let crc32 = self.digest.sum32();
        let expect = self.meta.get_crc32();
        if crc32 != expect {
            let reason = format!("crc32 {}, expect {}", crc32, expect);
            return Err(Error::FileCorrupted(self.path.temp.clone(), reason));
        }

        let f = self.file.as_ref().unwrap();
        let length = f.metadata()?.len();
        let expect = self.meta.get_length();
        if length != expect {
            let reason = format!("length {}, expect {}", length, expect);
            return Err(Error::FileCorrupted(self.path.temp.clone(), reason));
        }
        Ok(())
    }
}

impl Drop for ImportFile {
    fn drop(&mut self) {
        if let Err(e) = self.cleanup() {
            warn!("cleanup {:?}: {:?}", self, e);
        }
    }
}

impl fmt::Debug for ImportFile {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.debug_struct("ImportFile")
            .field("meta", &self.meta)
            .field("path", &self.path)
            .finish()
    }
}

const SST_SUFFIX: &str = ".sst";

fn sst_meta_to_path(meta: &SSTMeta) -> Result<PathBuf> {
    Ok(PathBuf::from(format!(
        "{}_{}_{}_{}{}",
        Uuid::from_bytes(meta.get_uuid())?,
        meta.get_region_id(),
        meta.get_region_epoch().get_conf_ver(),
        meta.get_region_epoch().get_version(),
        SST_SUFFIX,
    )))
}

#[cfg(test)]
mod tests {
    use super::*;
    use import::test_helpers::*;

    use tempdir::TempDir;

    #[test]
    fn test_import_dir() {
        let temp_dir = TempDir::new("test_import_dir").unwrap();
        let dir = ImportDir::new(temp_dir.path()).unwrap();

        let mut meta = SSTMeta::new();
        meta.set_uuid(Uuid::new_v4().as_bytes().to_vec());

        let path = dir.join(&meta).unwrap();

        // Test ImportDir::create()
        {
            let _file = dir.create(&meta).unwrap();
            assert!(path.temp.exists());
            assert!(!path.save.exists());
            assert!(!path.clone.exists());
            // Cannot create the same file again.
            assert!(dir.create(&meta).is_err());
        }

        // Test ImportDir::delete()
        {
            File::create(&path.temp).unwrap();
            File::create(&path.save).unwrap();
            File::create(&path.clone).unwrap();
            dir.delete(&meta).unwrap();
            assert!(!path.temp.exists());
            assert!(!path.save.exists());
            assert!(!path.clone.exists());
        }
    }

    #[test]
    fn test_import_file() {
        let temp_dir = TempDir::new("test_import_file").unwrap();

        let path = ImportPath {
            save: temp_dir.path().join("save"),
            temp: temp_dir.path().join("temp"),
            clone: temp_dir.path().join("clone"),
        };

        let data = b"test_data";
        let crc32 = calc_data_crc32(data);

        let mut meta = SSTMeta::new();

        {
            let mut f = ImportFile::create(meta.clone(), path.clone()).unwrap();
            // Cannot create the same again.
            assert!(ImportFile::create(meta.clone(), path.clone()).is_err());
            f.append(data).unwrap();
            // Invalid crc32 and length.
            assert!(f.finish().is_err());
            assert!(path.temp.exists());
            assert!(!path.save.exists());
        }

        meta.set_crc32(crc32);

        {
            let mut f = ImportFile::create(meta.clone(), path.clone()).unwrap();
            f.append(data).unwrap();
            // Invalid length.
            assert!(f.finish().is_err());
        }

        meta.set_length(data.len() as u64);

        {
            let mut f = ImportFile::create(meta.clone(), path.clone()).unwrap();
            f.append(data).unwrap();
            f.finish().unwrap();
            assert!(!path.temp.exists());
            assert!(path.save.exists());
        }
    }

    #[test]
    fn test_sst_meta_to_path() {
        let mut meta = SSTMeta::new();
        let uuid = Uuid::new_v4();
        meta.set_uuid(uuid.as_bytes().to_vec());
        meta.set_region_id(1);
        meta.mut_region_epoch().set_conf_ver(2);
        meta.mut_region_epoch().set_version(3);

        let path = sst_meta_to_path(&meta).unwrap();
        let expected_path = format!("{}_1_2_3.sst", uuid);
        assert_eq!(path.to_str().unwrap(), &expected_path);
    }
}
