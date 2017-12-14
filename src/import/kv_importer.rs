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
    dir: EngineDir,
    jobs: HashMap<Uuid, Arc<ImportJob>>,
    engines: HashMap<Uuid, Arc<EngineFile>>,
    clients: HashMap<Token, Arc<EngineFile>>,
}

pub struct KVImporter {
    cfg: Config,
    token: AtomicUsize,
    inner: Mutex<Inner>,
}

impl KVImporter {
    pub fn new(cfg: &Config, opts: &DbConfig) -> Result<KVImporter> {
        Ok(KVImporter {
            cfg: cfg.clone(),
            token: AtomicUsize::new(1),
            inner: Mutex::new(Inner {
                dir: EngineDir::new(&cfg.import_dir, opts)?,
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
        let engine = match inner.engines.get(&uuid).cloned() {
            Some(engine) => engine,
            None => {
                let engine = match inner.dir.create(uuid) {
                    Ok(engine) => {
                        info!("create {}", engine);
                        Arc::new(engine)
                    }
                    Err(e) => {
                        error!("create {}: {:?}", uuid, e);
                        return Err(e);
                    }
                };
                inner.engines.insert(uuid, engine.clone());
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

    pub fn write(&self, token: Token, batch: WriteBatch) -> Result<()> {
        match self.remove(token) {
            Some(engine) => match engine.write(batch) {
                Ok(_) => {
                    self.insert(token, engine);
                    Ok(())
                }
                Err(e) => {
                    error!("write {}: {:?}", engine, e);
                    Err(e)
                }
            },
            None => Err(Error::TokenNotFound(token)),
        }
    }

    /// Flush the engine.
    /// Engine can not be flushed when it is being written.
    pub fn flush(&self, uuid: Uuid) -> Result<()> {
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

        match engine.finish() {
            Ok(_) => {
                info!("finish {}", engine);
                Ok(())
            }
            Err(e) => {
                error!("finish {}: {:?}", engine, e);
                Err(e)
            }
        }
    }

    /// Import the engine to the cluster.
    /// Engine can not be imported if it hasn't been flushed.
    pub fn import(&self, uuid: Uuid, client: Arc<Client>) -> Result<()> {
        let job = {
            let mut inner = self.inner.lock().unwrap();
            if inner.jobs.contains_key(&uuid) || inner.engines.contains_key(&uuid) {
                return Err(Error::EngineInUse(uuid));
            }
            let engine = inner.dir.open(uuid)?;
            let job = Arc::new(ImportJob::new(self.cfg.clone(), engine, client)?);
            inner.jobs.insert(uuid, job.clone());
            job
        };

        let res = job.run();
        {
            let mut inner = self.inner.lock().unwrap();
            inner.jobs.remove(&uuid);
        }

        match res {
            Ok(_) => {
                info!("import {} done", uuid);
                Ok(())
            }
            Err(e) => {
                error!("import {}: {:?}", uuid, e);
                Err(e)
            }
        }
    }

    /// Cleanup the engine.
    /// Engine can not be cleanuped when it is being written or imported.
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

        let inner = self.inner.lock().unwrap();
        match inner.dir.remove(uuid) {
            Ok(_) => {
                info!("cleanup {} done", uuid);
                Ok(())
            }
            Err(e) => {
                error!("cleanup {}: {:?}", uuid, e);
                Err(e)
            }
        }
    }
}

pub struct EngineDir {
    root: PathBuf,
    opts: DbConfig,
}

impl EngineDir {
    const TEMP_DIR: &'static str = ".temp";

    pub fn new<P: AsRef<Path>>(root: P, opts: &DbConfig) -> Result<EngineDir> {
        let root_dir = root.as_ref().to_owned();
        let temp_dir = root_dir.join(Self::TEMP_DIR);
        if temp_dir.exists() {
            fs::remove_dir_all(&temp_dir)?;
        }
        fs::create_dir_all(&temp_dir)?;
        Ok(EngineDir {
            root: root_dir,
            opts: opts.clone(),
        })
    }

    pub fn open(&self, uuid: Uuid) -> Result<Engine> {
        let file_name = format!("{}", uuid);
        let save_path = self.root.join(&file_name);
        if !save_path.exists() {
            return Err(Error::FileNotExists(save_path));
        }
        Engine::new(self.opts.clone(), uuid, save_path)
    }

    pub fn create(&self, uuid: Uuid) -> Result<EngineFile> {
        let file_name = format!("{}", uuid);
        let save_path = self.root.join(&file_name);
        let temp_path = self.root.join(Self::TEMP_DIR).join(&file_name);
        if save_path.exists() {
            return Err(Error::FileExists(save_path));
        }
        if temp_path.exists() {
            return Err(Error::FileExists(temp_path));
        }
        EngineFile::new(self.opts.clone(), uuid, save_path, temp_path)
    }

    pub fn remove(&self, uuid: Uuid) -> Result<()> {
        let file_name = format!("{}", uuid);
        let save_path = self.root.join(&file_name);
        let temp_path = self.root.join(Self::TEMP_DIR).join(&file_name);
        if save_path.exists() {
            fs::remove_dir_all(save_path)?;
        }
        if temp_path.exists() {
            fs::remove_dir_all(temp_path)?;
        }
        Ok(())
    }
}

pub struct EngineFile {
    uuid: Uuid,
    engine: Option<Engine>,
    save_path: PathBuf,
    temp_path: PathBuf,
}

impl EngineFile {
    fn new(
        cfg: DbConfig,
        uuid: Uuid,
        save_path: PathBuf,
        temp_path: PathBuf,
    ) -> Result<EngineFile> {
        let engine = Engine::new(cfg, uuid, &temp_path)?;
        Ok(EngineFile {
            uuid: uuid,
            engine: Some(engine),
            save_path: save_path,
            temp_path: temp_path,
        })
    }

    fn write(&self, batch: WriteBatch) -> Result<()> {
        self.engine.as_ref().unwrap().write(batch)
    }

    fn finish(&mut self) -> Result<()> {
        self.engine.take();
        assert!(self.temp_path.exists());
        assert!(!self.save_path.exists());
        fs::rename(&self.temp_path, &self.save_path)?;
        Ok(())
    }

    fn remove(&mut self) -> Result<()> {
        self.engine.take();
        if self.temp_path.exists() {
            fs::remove_file(&self.temp_path)?;
        }
        Ok(())
    }
}

impl Drop for EngineFile {
    fn drop(&mut self) {
        if let Err(e) = self.remove() {
            warn!("remove {}: {:?}", self, e);
        }
    }
}

impl fmt::Display for EngineFile {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "EngineFile {{uuid: {}, save_path: {}, temp_path: {}}}",
            self.uuid,
            self.save_path.to_str().unwrap(),
            self.temp_path.to_str().unwrap(),
        )
    }
}
