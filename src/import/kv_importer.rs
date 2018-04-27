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
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

use kvproto::importpb::*;
use uuid::Uuid;

use config::DbConfig;
use util::collections::HashMap;

use super::engine::*;
use super::{Config, Error, Result};

pub struct KVImporter {
    dir: EngineDir,
    engines: Mutex<HashMap<Uuid, Arc<EngineFile>>>,
}

impl KVImporter {
    pub fn new(cfg: Config, opts: DbConfig) -> Result<KVImporter> {
        let dir = EngineDir::new(&cfg.import_dir, opts)?;
        Ok(KVImporter {
            dir,
            engines: Mutex::new(HashMap::default()),
        })
    }

    /// Open the engine.
    pub fn open_engine(&self, uuid: Uuid) -> Result<()> {
        let mut engines = self.engines.lock().unwrap();
        if engines.contains_key(&uuid) {
            return Ok(());
        }

        match self.dir.open(uuid) {
            Ok(engine) => {
                info!("open {:?}", engine);
                engines.insert(uuid, Arc::new(engine));
                Ok(())
            }
            Err(e) => {
                error!("open {}: {:?}", uuid, e);
                Err(e)
            }
        }
    }

    pub fn bind_engine(&self, uuid: Uuid) -> Result<Arc<EngineFile>> {
        let engines = self.engines.lock().unwrap();
        match engines.get(&uuid) {
            Some(engine) => Ok(Arc::clone(engine)),
            None => Err(Error::EngineNotFound(uuid)),
        }
    }

    /// Close the engine.
    /// Engine can not be closed when it is writing.
    pub fn close_engine(&self, uuid: Uuid) -> Result<()> {
        let mut engine = {
            let mut engines = self.engines.lock().unwrap();
            let engine = match engines.remove(&uuid) {
                Some(engine) => engine,
                None => return Err(Error::EngineNotFound(uuid)),
            };
            match Arc::try_unwrap(engine) {
                Ok(engine) => engine,
                Err(engine) => {
                    engines.insert(uuid, engine);
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
            opts,
            root_dir,
            temp_dir,
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
}

#[derive(Clone)]
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
            uuid,
            path,
            engine: Some(engine),
        })
    }

    pub fn write(&self, batch: WriteBatch) -> Result<usize> {
        self.engine.as_ref().unwrap().write(batch)
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

#[cfg(test)]
mod tests {
    use super::*;

    use tempdir::TempDir;

    #[test]
    fn test_kv_importer() {
        let temp_dir = TempDir::new("test_kv_importer").unwrap();

        let mut cfg = Config::default();
        cfg.import_dir = temp_dir.path().to_str().unwrap().to_owned();
        let importer = KVImporter::new(cfg, DbConfig::default()).unwrap();

        let uuid = Uuid::new_v4();
        // Can not bind to an unopened engine.
        assert!(importer.bind_engine(uuid).is_err());
        importer.open_engine(uuid).unwrap();
        let engine = importer.bind_engine(uuid).unwrap();
        engine.write(WriteBatch::new()).unwrap();
        // Can not close an in use engine.
        assert!(importer.close_engine(uuid).is_err());
        drop(engine);
        importer.close_engine(uuid).unwrap();
    }

    #[test]
    fn test_engine_file() {
        let temp_dir = TempDir::new("test_engine_file").unwrap();

        let uuid = Uuid::new_v4();
        let opts = DbConfig::default();
        let path = EnginePath {
            save: temp_dir.path().join("save"),
            temp: temp_dir.path().join("temp"),
        };

        // Test close.
        {
            let mut f = EngineFile::new(uuid, path.clone(), opts.clone()).unwrap();
            // Cannot create the same file again.
            assert!(EngineFile::new(uuid, path.clone(), opts.clone()).is_err());
            assert!(path.temp.exists());
            assert!(!path.save.exists());
            f.close().unwrap();
            assert!(!path.temp.exists());
            assert!(path.save.exists());
            fs::remove_dir_all(&path.save).unwrap();
        }

        // Test cleanup.
        {
            let f = EngineFile::new(uuid, path.clone(), opts.clone()).unwrap();
            assert!(path.temp.exists());
            assert!(!path.save.exists());
            drop(f);
            assert!(!path.temp.exists());
            assert!(!path.save.exists());
        }
    }
}
