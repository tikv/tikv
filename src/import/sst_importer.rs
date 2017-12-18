// Copyright 2017 PingCAP, Inc.
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

use std::collections::HashMap;
use std::fmt;
use std::fs::{self, File};
use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::Mutex;
use std::sync::atomic::{AtomicUsize, Ordering};

use crc::crc32::{self, Hasher32};
use uuid::Uuid;

use kvproto::importpb::*;

use super::{Error, Result};

pub type Token = usize;

struct Inner {
    dir: ImportDir,
    files: HashMap<Token, ImportFile>,
}

pub struct SSTImporter {
    token: AtomicUsize,
    inner: Mutex<Inner>,
}

impl SSTImporter {
    pub fn new<P: AsRef<Path>>(root: P) -> Result<SSTImporter> {
        Ok(SSTImporter {
            token: AtomicUsize::new(1),
            inner: Mutex::new(Inner {
                dir: ImportDir::new(root)?,
                files: HashMap::new(),
            }),
        })
    }

    pub fn token(&self) -> Token {
        self.token.fetch_add(1, Ordering::SeqCst)
    }

    fn insert(&self, token: Token, file: ImportFile) {
        let mut inner = self.inner.lock().unwrap();
        assert!(inner.files.insert(token, file).is_none());
    }

    pub fn remove(&self, token: Token) -> Option<ImportFile> {
        let mut inner = self.inner.lock().unwrap();
        inner.files.remove(&token)
    }

    pub fn create(&self, token: Token, meta: &SSTMeta) -> Result<()> {
        let mut inner = self.inner.lock().unwrap();
        match inner.dir.create(meta) {
            Ok(f) => {
                info!("create {}", f);
                assert!(inner.files.insert(token, f).is_none());
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
                    error!("append {}: {:?}", f, e);
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
                    info!("finish {}", f);
                    Ok(())
                }
                Err(e) => {
                    error!("finish {}: {:?}", f, e);
                    Err(e)
                }
            },
            None => Err(Error::TokenNotFound(token)),
        }
    }

    pub fn locate(&self, meta: &SSTMeta) -> Result<PathBuf> {
        let inner = self.inner.lock().unwrap();
        inner.dir.locate(meta)
    }
}

// TODO: Add size and rate limit.
pub struct ImportDir {
    root: PathBuf,
}

impl ImportDir {
    const TEMP_DIR: &'static str = ".temp";

    pub fn new<P: AsRef<Path>>(root: P) -> Result<ImportDir> {
        let root_dir = root.as_ref().to_owned();
        let temp_dir = root_dir.join(Self::TEMP_DIR);
        if temp_dir.exists() {
            fs::remove_dir_all(&temp_dir)?;
        }
        fs::create_dir_all(&temp_dir)?;
        Ok(ImportDir { root: root_dir })
    }

    pub fn create(&self, meta: &SSTMeta) -> Result<ImportFile> {
        let file_name = sst_meta_to_path(meta)?;
        let save_path = self.root.join(&file_name);
        let temp_path = self.root.join(Self::TEMP_DIR).join(&file_name);
        if save_path.exists() {
            return Err(Error::FileExists(save_path));
        }
        if temp_path.exists() {
            return Err(Error::FileExists(temp_path));
        }
        ImportFile::create(meta.clone(), save_path, temp_path)
    }

    pub fn locate(&self, meta: &SSTMeta) -> Result<PathBuf> {
        let file_name = sst_meta_to_path(meta)?;
        let save_path = self.root.join(&file_name);
        if !save_path.exists() {
            return Err(Error::FileNotExists(save_path));
        }
        Ok(save_path)
    }
}

pub struct ImportFile {
    meta: SSTMeta,
    file: Option<File>,
    digest: crc32::Digest,
    save_path: PathBuf,
    temp_path: PathBuf,
}

impl ImportFile {
    fn create(meta: SSTMeta, save_path: PathBuf, temp_path: PathBuf) -> Result<ImportFile> {
        let file = File::create(&temp_path)?;
        Ok(ImportFile {
            meta: meta,
            file: Some(file),
            digest: crc32::Digest::new(crc32::IEEE),
            save_path: save_path,
            temp_path: temp_path,
        })
    }

    fn append(&mut self, data: &[u8]) -> Result<()> {
        self.file.as_mut().unwrap().write_all(data)?;
        self.digest.write(data);
        Ok(())
    }

    fn finish(&mut self) -> Result<()> {
        self.validate()?;
        self.file.take();
        assert!(self.temp_path.exists());
        assert!(!self.save_path.exists());
        fs::rename(&self.temp_path, &self.save_path)?;
        Ok(())
    }

    fn remove(&mut self) -> Result<()> {
        self.file.take();
        if self.temp_path.exists() {
            fs::remove_file(&self.temp_path)?;
        }
        Ok(())
    }

    fn validate(&self) -> Result<()> {
        if self.digest.sum32() != self.meta.get_crc32() {
            return Err(Error::FileCorrupted(self.temp_path.clone()));
        }
        let f = self.file.as_ref().unwrap();
        if f.metadata()?.len() != self.meta.get_length() {
            return Err(Error::FileCorrupted(self.temp_path.clone()));
        }
        Ok(())
    }
}

impl Drop for ImportFile {
    fn drop(&mut self) {
        if let Err(e) = self.remove() {
            warn!("remove {}: {:?}", self, e);
        }
    }
}

impl fmt::Display for ImportFile {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "ImportFile {{meta: {{{:?}}}, save_path: {}, temp_path: {}}}",
            self.meta,
            self.save_path.to_str().unwrap(),
            self.temp_path.to_str().unwrap(),
        )
    }
}

fn sst_meta_to_path(meta: &SSTMeta) -> Result<PathBuf> {
    Ok(PathBuf::from(format!(
        "{}_{}_{}_{}.sst",
        Uuid::from_bytes(meta.get_uuid())?,
        meta.get_region_id(),
        meta.get_region_epoch().get_conf_ver(),
        meta.get_region_epoch().get_version(),
    )))
}
