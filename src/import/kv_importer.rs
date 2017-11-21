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
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::sync::atomic::{AtomicUsize, Ordering};

use uuid::Uuid;
use tempdir::TempDir;

use config::DbConfig;
use kvproto::importpb::*;

use super::{Engine, Error, Result};

pub type Token = usize;

pub struct KVImporter {
    dir: EngineDir,
    token: AtomicUsize,
    engines: Mutex<HashMap<Uuid, Arc<Engine>>>,
    clients: Mutex<HashMap<Token, Arc<Engine>>>,
}

impl KVImporter {
    pub fn new<P: AsRef<Path>>(root: P, opts: &DbConfig) -> Result<KVImporter> {
        let dir = EngineDir::new(root, opts)?;
        Ok(KVImporter {
            dir: dir,
            token: AtomicUsize::new(1),
            engines: Mutex::new(HashMap::new()),
            clients: Mutex::new(HashMap::new()),
        })
    }

    pub fn token(&self) -> Token {
        self.token.fetch_add(1, Ordering::SeqCst)
    }

    /// Open an engine identified by the uuid.
    /// Clients with the same uuid will share the same engine.
    pub fn open(&self, token: Token, uuid: Uuid) -> Result<()> {
        let mut engines = self.engines.lock().unwrap();
        let engine = match engines.entry(uuid) {
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
        info!("open {}", engine);
        self.insert(token, engine);
        Ok(())
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

    pub fn close(&self, token: Token) -> Result<()> {
        match self.remove(token) {
            Some(engine) => {
                info!("close {}", engine);
                Ok(())
            }
            None => Err(Error::TokenNotFound(token)),
        }
    }

    /// Finish the engine identified by the uuid.
    /// Engine can not be finished if it is still in use.
    /// TODO: Cleanup long time unfinished engines.
    pub fn finish(&self, uuid: Uuid) -> Result<Engine> {
        let mut engines = self.engines.lock().unwrap();
        let engine = match engines.remove(&uuid) {
            Some(engine) => engine,
            None => return Err(Error::EngineNotFound(uuid)),
        };
        match Arc::try_unwrap(engine) {
            Ok(engine) => {
                info!("finish {}", engine);
                Ok(engine)
            }
            Err(engine) => {
                engines.insert(uuid, engine);
                Err(Error::EngineInUse(uuid))
            }
        }
    }

    fn insert(&self, token: Token, engine: Arc<Engine>) {
        self.clients.lock().unwrap().insert(token, engine);
    }

    pub fn remove(&self, token: Token) -> Option<Arc<Engine>> {
        self.clients.lock().unwrap().remove(&token)
    }
}

pub struct EngineDir {
    root: PathBuf,
    opts: DbConfig,
}

impl EngineDir {
    pub fn new<P: AsRef<Path>>(root: P, opts: &DbConfig) -> Result<EngineDir> {
        let root_dir = root.as_ref().to_owned();
        // The whole root will be emptied since old data can't be used anymore.
        if root_dir.exists() {
            fs::remove_dir_all(&root_dir)?;
        }
        fs::create_dir_all(&root_dir)?;
        Ok(EngineDir {
            root: root_dir,
            opts: opts.clone(),
        })
    }

    pub fn create(&self, uuid: Uuid) -> Result<Engine> {
        let path = format!("{}", uuid);
        let temp_dir = TempDir::new_in(&self.root, &path)?;
        Engine::new(&self.opts, uuid, temp_dir)
    }
}
