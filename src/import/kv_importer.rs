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

use std::fs;
use std::fmt;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Instant;

use uuid::Uuid;
use kvproto::importpb::*;

use config::DbConfig;
use storage::{CF_DEFAULT, CF_WRITE};
use util::collections::HashMap;

use super::client::*;
use super::engine::*;
use super::import::*;
use super::{Config, Error, Result};

pub type Token = usize;

struct Inner {
    jobs: HashMap<Uuid, Arc<ImportJob<Client>>>,
    engines: HashMap<Uuid, Arc<EngineFile>>,
    clients: HashMap<Token, Arc<EngineFile>>,
}

pub struct KVImporter {
    cfg: Config,
    dir: EngineDir,
    token: AtomicUsize,
    inner: Mutex<Inner>,
}

impl KVImporter {
    pub fn new(cfg: Config, opts: DbConfig) -> Result<KVImporter> {
        let dir = EngineDir::new(&cfg.import_dir, opts)?;
        Ok(KVImporter {
            cfg: cfg,
            dir: dir,
            token: AtomicUsize::new(1),
            inner: Mutex::new(Inner {
                jobs: HashMap::default(),
                engines: HashMap::default(),
                clients: HashMap::default(),
            }),
        })
    }

    pub fn token(&self) -> Token {
        self.token.fetch_add(1, Ordering::SeqCst)
    }

    fn insert(&self, token: Token, engine: Arc<EngineFile>) {
        let mut inner = self.inner.lock().unwrap();
        assert!(inner.clients.insert(token, engine).is_none());
    }

    pub fn remove(&self, token: Token) -> Option<Arc<EngineFile>> {
        let mut inner = self.inner.lock().unwrap();
        inner.clients.remove(&token)
    }

    pub fn open(&self, uuid: Uuid) -> Result<()> {
        let mut inner = self.inner.lock().unwrap();
        if inner.engines.contains_key(&uuid) {
            return Ok(());
        }

        match self.dir.open(uuid) {
            Ok(engine) => {
                info!("open {:?}", engine);
                inner.engines.insert(uuid, Arc::new(engine));
                Ok(())
            }
            Err(e) => {
                error!("open {}: {:?}", uuid, e);
                Err(e)
            }
        }
    }

    /// Attach to the engine.
    /// Clients with the same uuid will share the same engine.
    pub fn attach(&self, token: Token, uuid: Uuid) -> Result<()> {
        let mut inner = self.inner.lock().unwrap();
        if inner.clients.contains_key(&token) {
            return Err(Error::TokenExists(token));
        }

        let engine = match inner.engines.get(&uuid) {
            Some(engine) => Arc::clone(engine),
            None => return Err(Error::EngineNotFound(uuid)),
        };

        info!("attach {:?}", engine);
        inner.clients.insert(token, engine);
        Ok(())
    }

    pub fn detach(&self, token: Token) -> Result<()> {
        match self.remove(token) {
            Some(engine) => {
                info!("detach {:?}", engine);
                Ok(())
            }
            None => Err(Error::TokenNotFound(token)),
        }
    }

    pub fn write(&self, token: Token, batch: WriteBatch, options: WriteOptions) -> Result<usize> {
        match self.remove(token) {
            Some(engine) => match engine.write(batch, options) {
                Ok(v) => {
                    self.insert(token, engine);
                    Ok(v)
                }
                Err(e) => {
                    error!("write {:?}: {:?}", engine, e);
                    Err(e)
                }
            },
            None => Err(Error::TokenNotFound(token)),
        }
    }

    /// Close the engine.
    /// Engine can not be closed when it is writing.
    pub fn close(&self, uuid: Uuid) -> Result<()> {
        let mut engine = {
            let mut inner = self.inner.lock().unwrap();
            let engine = match inner.engines.remove(&uuid) {
                Some(engine) => engine,
                None => return Err(Error::EngineNotFound(uuid)),
            };
            match Arc::try_unwrap(engine) {
                Ok(engine) => engine,
                Err(engine) => {
                    inner.engines.insert(uuid, engine);
                    return Err(Error::EngineInUse(uuid));
                }
            }
        };

        match engine.close() {
            Ok(_) => {
                info!("close {:?}", engine);
                Ok(())
            }
            Err(e) => {
                error!("close {:?}: {:?}", engine, e);
                Err(e)
            }
        }
    }

    /// Import the engine to TiKV stores.
    /// Engine can not be imported before it is closed.
    pub fn import(&self, uuid: Uuid, client: Client) -> Result<()> {
        let job = {
            let mut inner = self.inner.lock().unwrap();
            if inner.jobs.contains_key(&uuid) || inner.engines.contains_key(&uuid) {
                return Err(Error::EngineInUse(uuid));
            }
            let engine = self.dir.import(uuid)?;
            let job = Arc::new(ImportJob::new(self.cfg.clone(), client, engine));
            inner.jobs.insert(uuid, Arc::clone(&job));
            job
        };

        let res = job.run();
        self.inner.lock().unwrap().jobs.remove(&uuid);

        match res {
            Ok(_) => {
                info!("import {}", uuid);
                Ok(())
            }
            Err(e) => {
                error!("import {}: {:?}", uuid, e);
                Err(e)
            }
        }
    }

    /// Clean up the engine.
    /// Engine can not be cleaned up when it is writing or importing.
    pub fn cleanup(&self, uuid: Uuid) -> Result<()> {
        // Drop the engine outside of the lock.
        let _ = {
            let mut inner = self.inner.lock().unwrap();
            if inner.jobs.contains_key(&uuid) {
                return Err(Error::EngineInUse(uuid));
            }
            if let Some(engine) = inner.engines.remove(&uuid) {
                match Arc::try_unwrap(engine) {
                    Ok(engine) => Some(engine),
                    Err(engine) => {
                        inner.engines.insert(uuid, engine);
                        return Err(Error::EngineInUse(uuid));
                    }
                }
            } else {
                None
            }
        };

        match self.dir.cleanup(uuid) {
            Ok(_) => {
                info!("cleanup {}", uuid);
                Ok(())
            }
            Err(e) => {
                error!("cleanup {}: {:?}", uuid, e);
                Err(e)
            }
        }
    }

    pub fn compact(&self, client: Client) -> Result<()> {
        let start = Instant::now();
        match client.compact_range(&[CF_DEFAULT, CF_WRITE]) {
            Ok(v) => {
                info!("compact takes {:?}", start.elapsed());
                Ok(v)
            }
            Err(e) => {
                error!("compact: {:?}", e);
                Err(e)
            }
        }
    }
}

pub struct EngineDir {
    opts: DbConfig,
    root_dir: PathBuf,
    temp_dir: PathBuf,
}

impl EngineDir {
    const TEMP_DIR: &'static str = ".temp";

    fn new<P: AsRef<Path>>(root: P, opts: DbConfig) -> Result<EngineDir> {
        let root_dir = root.as_ref().to_owned();
        let temp_dir = root_dir.join(Self::TEMP_DIR);
        if !temp_dir.exists() {
            fs::create_dir_all(&temp_dir)?;
        }
        Ok(EngineDir {
            opts: opts,
            root_dir: root_dir,
            temp_dir: temp_dir,
        })
    }

    fn join(&self, uuid: Uuid) -> EnginePath {
        let file_name = format!("{}", uuid);
        let save_path = self.root_dir.join(&file_name);
        let temp_path = self.temp_dir.join(&file_name);
        EnginePath {
            save: save_path,
            temp: temp_path,
        }
    }

    fn open(&self, uuid: Uuid) -> Result<EngineFile> {
        let path = self.join(uuid);
        if path.save.exists() {
            return Err(Error::FileExists(path.save));
        }
        EngineFile::new(uuid, path, self.opts.clone())
    }

    fn import(&self, uuid: Uuid) -> Result<Engine> {
        let path = self.join(uuid);
        Engine::new(&path.save, uuid, self.opts.clone())
    }

    fn cleanup(&self, uuid: Uuid) -> Result<EnginePath> {
        let path = self.join(uuid);
        if path.save.exists() {
            fs::remove_dir_all(&path.save)?;
        }
        if path.temp.exists() {
            fs::remove_dir_all(&path.temp)?;
        }
        Ok(path)
    }
}

pub struct EnginePath {
    // The path of the engine that has been closed.
    save: PathBuf,
    // The path of the engine that is being wrote.
    temp: PathBuf,
}

impl fmt::Debug for EnginePath {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.debug_struct("EnginePath")
            .field("save", &self.save)
            .field("temp", &self.temp)
            .finish()
    }
}

pub struct EngineFile {
    uuid: Uuid,
    path: EnginePath,
    engine: Option<Engine>,
}

impl EngineFile {
    fn new(uuid: Uuid, path: EnginePath, opts: DbConfig) -> Result<EngineFile> {
        let engine = Engine::new(&path.temp, uuid, opts)?;
        Ok(EngineFile {
            uuid: uuid,
            path: path,
            engine: Some(engine),
        })
    }

    fn write(&self, batch: WriteBatch, options: WriteOptions) -> Result<usize> {
        self.engine.as_ref().unwrap().write(batch, options)
    }

    fn close(&mut self) -> Result<()> {
        self.engine.take().unwrap().flush(true)?;
        if self.path.save.exists() {
            return Err(Error::FileExists(self.path.save.clone()));
        }
        fs::rename(&self.path.temp, &self.path.save)?;
        Ok(())
    }

    fn cleanup(&mut self) -> Result<()> {
        self.engine.take();
        if self.path.temp.exists() {
            fs::remove_dir_all(&self.path.temp)?;
        }
        Ok(())
    }
}

impl Drop for EngineFile {
    fn drop(&mut self) {
        if let Err(e) = self.cleanup() {
            warn!("cleanup {:?}: {:?}", self, e);
        }
    }
}

impl fmt::Debug for EngineFile {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.debug_struct("EngineFile")
            .field("uuid", &self.uuid)
            .field("path", &self.path)
            .finish()
    }
}
