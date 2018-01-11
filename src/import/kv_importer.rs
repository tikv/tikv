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
use std::collections::hash_map::Entry;
use std::fs;
use std::fmt;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::sync::atomic::{AtomicUsize, Ordering};

use uuid::Uuid;

use config::DbConfig;
use kvproto::importpb::*;

use super::{Client, Config, Engine, Error, ImportJob, Result};

pub type Token = usize;

struct Inner {
    jobs: HashMap<Uuid, Arc<ImportJob>>,
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
    pub fn new(cfg: &Config, opts: &DbConfig) -> Result<KVImporter> {
        Ok(KVImporter {
            cfg: cfg.clone(),
            dir: EngineDir::new(&cfg.import_dir, opts)?,
            token: AtomicUsize::new(1),
            inner: Mutex::new(Inner {
                jobs: HashMap::new(),
                engines: HashMap::new(),
                clients: HashMap::new(),
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

    /// Attach to the engine.
    /// Clients with the same uuid will share the same engine.
    pub fn attach(&self, token: Token, uuid: Uuid) -> Result<()> {
        let mut inner = self.inner.lock().unwrap();
        let engine = match inner.engines.entry(uuid) {
            Entry::Occupied(e) => e.get().clone(),
            Entry::Vacant(e) => {
                let engine = match self.dir.create(uuid) {
                    Ok(engine) => {
                        info!("create {}", engine);
                        Arc::new(engine)
                    }
                    Err(e) => {
                        error!("create {}: {:?}", uuid, e);
                        return Err(e);
                    }
                };
                e.insert(engine.clone());
                engine
            }
        };
        info!("attach {}", engine);
        assert!(inner.clients.insert(token, engine).is_none());
        Ok(())
    }

    pub fn detach(&self, token: Token) -> Result<()> {
        match self.remove(token) {
            Some(engine) => {
                info!("detach {}", engine);
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
                    error!("write {}: {:?}", engine, e);
                    Err(e)
                }
            },
            None => Err(Error::TokenNotFound(token)),
        }
    }

    /// Close the engine.
    /// Engine can not be closed when it is being wrote.
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
                info!("close {}", engine);
                Ok(())
            }
            Err(e) => {
                error!("close {}: {:?}", engine, e);
                Err(e)
            }
        }
    }

    /// Import the engine to the cluster.
    /// Engine can not be imported if it hasn't been closed.
    pub fn import(&self, uuid: Uuid, client: Arc<Client>) -> Result<()> {
        let job = {
            let mut inner = self.inner.lock().unwrap();
            if inner.jobs.contains_key(&uuid) || inner.engines.contains_key(&uuid) {
                return Err(Error::EngineInUse(uuid));
            }
            let engine = self.dir.open(uuid)?;
            let job = Arc::new(ImportJob::new(self.cfg.clone(), engine, client)?);
            inner.jobs.insert(uuid, job.clone());
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

    /// Delete the engine.
    /// Engine can not be deleted when it is being wrote or imported.
    pub fn delete(&self, uuid: Uuid) -> Result<()> {
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

        match self.dir.delete(uuid) {
            Ok(_) => {
                info!("delete {}", uuid);
                Ok(())
            }
            Err(e) => {
                error!("delete {}: {:?}", uuid, e);
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

    fn new<P: AsRef<Path>>(root: P, opts: &DbConfig) -> Result<EngineDir> {
        let root_dir = root.as_ref().to_owned();
        let temp_dir = root_dir.join(Self::TEMP_DIR);
        fs::create_dir_all(&temp_dir)?;
        Ok(EngineDir {
            opts: opts.clone(),
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

    fn open(&self, uuid: Uuid) -> Result<Engine> {
        let path = self.join(uuid);
        Engine::new(uuid, path.save, self.opts.clone())
    }

    fn create(&self, uuid: Uuid) -> Result<EngineFile> {
        let path = self.join(uuid);
        if path.save.exists() {
            return Err(Error::FileExists(path.save));
        }
        EngineFile::new(uuid, path, self.opts.clone())
    }

    fn delete(&self, uuid: Uuid) -> Result<EnginePath> {
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

impl fmt::Display for EnginePath {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "EnginePath: {{save: {}, temp: {}}}",
            self.save.to_str().unwrap(),
            self.temp.to_str().unwrap(),
        )
    }
}

pub struct EngineFile {
    uuid: Uuid,
    path: EnginePath,
    engine: Option<Engine>,
}

impl EngineFile {
    fn new(uuid: Uuid, path: EnginePath, opts: DbConfig) -> Result<EngineFile> {
        let engine = Engine::new(uuid, &path.temp, opts)?;
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
        self.engine.take().unwrap().flush()?;
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
            warn!("cleanup {}: {:?}", self, e);
        }
    }
}

impl fmt::Display for EngineFile {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "EngineFile {{uuid: {}, path: {}}}", self.uuid, self.path)
    }
}
