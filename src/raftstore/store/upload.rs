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

use std::io::{Error, ErrorKind, Result, Write};
use std::fmt::{self, Display, Formatter};
use std::fs::{self, File};
use std::path::{Path, PathBuf};
use std::sync::Mutex;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::collections::HashMap;

use crc::crc32::{self, Hasher32};
use kvproto::importpb::*;

pub type Token = usize;

pub struct Uploader {
    dir: UploadDir,
    token: AtomicUsize,
    files: Mutex<HashMap<Token, UploadFile>>,
}

impl Uploader {
    pub fn new<P: AsRef<Path>>(root: P) -> Result<Uploader> {
        Ok(Uploader {
            dir: UploadDir::new(root)?,
            token: AtomicUsize::new(1),
            files: Mutex::new(HashMap::new()),
        })
    }

    pub fn token(&self) -> Token {
        self.token.fetch_add(1, Ordering::SeqCst) as Token
    }

    pub fn create(&self, token: Token, meta: &SSTMeta) -> Result<()> {
        match self.dir.create(meta.clone()) {
            Ok(f) => {
                info!("create {}", f);
                self.insert(token, f);
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
            None => Err(Self::token_not_found(token)),
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
            None => Err(Self::token_not_found(token)),
        }
    }

    fn insert(&self, token: Token, file: UploadFile) {
        let mut files = self.files.lock().unwrap();
        assert!(files.insert(token, file).is_none());
    }

    pub fn remove(&self, token: Token) -> Option<UploadFile> {
        let mut files = self.files.lock().unwrap();
        files.remove(&token)
    }

    fn token_not_found(token: usize) -> Error {
        let error = format!("token {} not found", token);
        Error::new(ErrorKind::NotFound, error)
    }
}

// TODO: Add size limit and rate limit.
pub struct UploadDir {
    root: Mutex<PathBuf>,
}

impl UploadDir {
    const TEMP_DIR: &'static str = ".temp";

    pub fn new<P: AsRef<Path>>(root: P) -> Result<UploadDir> {
        let root_dir = root.as_ref().to_owned();
        let temp_dir = root_dir.join(Self::TEMP_DIR);
        if temp_dir.exists() {
            fs::remove_dir_all(&temp_dir)?;
        }
        fs::create_dir_all(&temp_dir)?;
        Ok(UploadDir {
            root: Mutex::new(root_dir),
        })
    }

    pub fn create(&self, meta: SSTMeta) -> Result<UploadFile> {
        let path = sst_handle_to_path(meta.get_handle());
        let root = self.root.lock().unwrap();
        let save_path = root.join(&path);
        let temp_path = root.join(Self::TEMP_DIR).join(&path);
        if save_path.exists() {
            return Err(file_exists(&save_path));
        }
        if temp_path.exists() {
            return Err(file_exists(&temp_path));
        }
        UploadFile::create(meta, save_path, temp_path)
    }
}

pub struct UploadFile {
    meta: SSTMeta,
    save_path: PathBuf,
    temp_path: PathBuf,
    temp_file: Option<File>,
    temp_digest: crc32::Digest,
}

impl UploadFile {
    fn create(meta: SSTMeta, save_path: PathBuf, temp_path: PathBuf) -> Result<UploadFile> {
        let temp_file = File::create(&temp_path)?;
        Ok(UploadFile {
            meta: meta,
            save_path: save_path,
            temp_path: temp_path,
            temp_file: Some(temp_file),
            temp_digest: crc32::Digest::new(crc32::IEEE),
        })
    }

    fn append(&mut self, data: &[u8]) -> Result<()> {
        self.temp_file.as_mut().unwrap().write_all(data)?;
        self.temp_digest.write(data);
        Ok(())
    }

    fn finish(&mut self) -> Result<()> {
        self.validate()?;
        self.temp_file.take();
        assert!(self.temp_path.exists());
        assert!(!self.save_path.exists());
        fs::rename(&self.temp_path, &self.save_path)
    }

    fn remove(&mut self) -> Result<()> {
        self.temp_file.take();
        if self.temp_path.exists() {
            fs::remove_file(&self.temp_path)
        } else {
            Ok(())
        }
    }

    fn validate(&self) -> Result<()> {
        let f = self.temp_file.as_ref().unwrap();
        if f.metadata()?.len() != self.meta.get_len() {
            return Err(file_corrupted(&self.temp_path));
        }
        if self.temp_digest.sum32() != self.meta.get_crc32() {
            return Err(file_corrupted(&self.temp_path));
        }
        Ok(())
    }
}

impl Drop for UploadFile {
    fn drop(&mut self) {
        if let Err(e) = self.remove() {
            warn!("failed to remove {}: {:?}", self, e);
        }
    }
}

impl Display for UploadFile {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        write!(
            f,
            "UploadFile {{meta: {{{:?}}}, save_path: {:?}, temp_path: {:?}}}",
            self.meta,
            self.save_path,
            self.temp_path
        )
    }
}

fn sst_handle_to_path(h: &SSTHandle) -> PathBuf {
    PathBuf::from(format!(
        "{}_{}_{}_{}_{}.sst",
        h.get_uuid(),
        h.get_cfname(),
        h.get_region_id(),
        h.get_region_epoch().get_conf_ver(),
        h.get_region_epoch().get_version(),
    ))
}

fn file_exists<P: AsRef<Path>>(path: P) -> Error {
    let path = path.as_ref().as_os_str();
    let error = format!("file {:?} exists", path);
    Error::new(ErrorKind::AlreadyExists, error)
}

fn file_corrupted<P: AsRef<Path>>(path: P) -> Error {
    let path = path.as_ref().as_os_str();
    let error = format!("file {:?} corrupted", path);
    Error::new(ErrorKind::InvalidData, error)
}
