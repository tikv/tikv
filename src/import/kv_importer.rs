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

use kvproto::import_kvpb::*;
use uuid::Uuid;

use crate::config::DbConfig;
use crate::util::collections::HashMap;

use super::client::*;
use super::engine::*;
use super::import::*;
use super::{Config, Error, Result};
use crate::util::security::SecurityConfig;

pub struct Inner {
    engines: HashMap<Uuid, Arc<EngineFile>>,
    import_jobs: HashMap<Uuid, Arc<ImportJob<Client>>>,
}

/// KVImporter manages all engines according to UUID.
pub struct KVImporter {
    cfg: Config,
    dir: EngineDir,
    inner: Mutex<Inner>,
}

impl KVImporter {
    pub fn new(cfg: Config, db_cfg: DbConfig, security_cfg: SecurityConfig) -> Result<KVImporter> {
        let dir = EngineDir::new(&cfg.import_dir, db_cfg, security_cfg)?;
        Ok(KVImporter {
            cfg,
            dir,
            inner: Mutex::new(Inner {
                engines: HashMap::default(),
                import_jobs: HashMap::default(),
            }),
        })
    }

    /// Open the engine.
    pub fn open_engine(&self, uuid: Uuid) -> Result<()> {
        // Checks if we have already opened an engine related to UUID
        let mut inner = self.inner.lock().unwrap();
        if inner.engines.contains_key(&uuid) {
            return Ok(());
        }

        // Restrict max open engines
        if inner.engines.len() >= self.cfg.max_open_engines {
            error!("Too many open engines "; "uuid" => %uuid, "opened_engine_count" => %inner.engines.len());
            return Err(Error::ResourceTemporarilyUnavailable(
                "Too many open engines".to_string(),
            ));
        }

        match self.dir.open(uuid) {
            Ok(engine) => {
                info!("open engine"; "engine" => ?engine);
                inner.engines.insert(uuid, Arc::new(engine));
                Ok(())
            }
            Err(e) => {
                error!("open engine failed"; "uuid" => %uuid, "err" => %e);
                Err(e)
            }
        }
    }

    /// Returns an opened engine reference for write.
    pub fn bind_engine(&self, uuid: Uuid) -> Result<Arc<EngineFile>> {
        let inner = self.inner.lock().unwrap();
        match inner.engines.get(&uuid) {
            Some(engine) => Ok(Arc::clone(engine)),
            None => Err(Error::EngineNotFound(uuid)),
        }
    }

    /// Close the engine.
    /// Engine can not be closed when it is writing.
    pub fn close_engine(&self, uuid: Uuid) -> Result<()> {
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
                info!("close engine"; "engine" => ?engine);
                Ok(())
            }
            Err(e) => {
                error!("close engine failed"; "engine" => ?engine, "err" => %e);
                Err(e)
            }
        }
    }

    /// Import the engine to TiKV stores.
    /// Engine can not be imported before it is closed.
    pub fn import_engine(&self, uuid: Uuid, pd_addr: &str) -> Result<()> {
        let client = Client::new(pd_addr, self.cfg.num_import_jobs)?;
        let job = {
            let mut inner = self.inner.lock().unwrap();
            // One engine only related to one ImportJob
            if inner.engines.contains_key(&uuid) || inner.import_jobs.contains_key(&uuid) {
                return Err(Error::EngineInUse(uuid));
            }
            let engine = self.dir.import(uuid)?;
            let job = Arc::new(ImportJob::new(self.cfg.clone(), client, engine));
            inner.import_jobs.insert(uuid, Arc::clone(&job));
            job
        };

        let res = job.run();
        self.inner.lock().unwrap().import_jobs.remove(&uuid);

        match res {
            Ok(_) => {
                info!("import"; "uuid" => %uuid);
                Ok(())
            }
            Err(e) => {
                error!("import failed"; "uuid" => %uuid, "err" => %e);
                Err(e)
            }
        }
    }

    /// Clean up the engine.
    /// Engine can not be cleaned up when it is writing or importing.
    pub fn cleanup_engine(&self, uuid: Uuid) -> Result<()> {
        // Drop the engine outside of the lock.
        let _ = {
            let mut inner = self.inner.lock().unwrap();
            if inner.import_jobs.contains_key(&uuid) {
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
                info!("cleanup"; "uuid" => %uuid);
                Ok(())
            }
            Err(e) => {
                error!("cleanup failed"; "uuid" => %uuid, "err" => %e);
                Err(e)
            }
        }
    }
}

/// EngineDir is responsible for managing engine directories.
///
/// The temporary RocksDB engine is placed in `$root/.temp/$uuid`. After writing
/// is completed, the files are stored in `$root/$uuid`.
pub struct EngineDir {
    db_cfg: DbConfig,
    security_cfg: SecurityConfig,
    root_dir: PathBuf,
    temp_dir: PathBuf,
}

impl EngineDir {
    const TEMP_DIR: &'static str = ".temp";

    fn new<P: AsRef<Path>>(
        root: P,
        db_cfg: DbConfig,
        security_cfg: SecurityConfig,
    ) -> Result<EngineDir> {
        let root_dir = root.as_ref().to_owned();
        let temp_dir = root_dir.join(Self::TEMP_DIR);
        if temp_dir.exists() {
            fs::remove_dir_all(&temp_dir)?;
        }
        fs::create_dir_all(&temp_dir)?;
        Ok(EngineDir {
            db_cfg,
            security_cfg,
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

    /// Creates an engine from `$root/.temp/$uuid` for storing and sorting KV pairs temporarily.
    fn open(&self, uuid: Uuid) -> Result<EngineFile> {
        let path = self.join(uuid);
        if path.save.exists() {
            return Err(Error::FileExists(path.save));
        }
        EngineFile::new(uuid, path, self.db_cfg.clone(), self.security_cfg.clone())
    }

    /// Creates an engine from `$root/$uuid` for importing data.
    fn import(&self, uuid: Uuid) -> Result<Engine> {
        let path = self.join(uuid);
        Engine::new(
            &path.save,
            uuid,
            self.db_cfg.clone(),
            self.security_cfg.clone(),
        )
    }

    /// Cleans up directories for both `$root/.temp/$uuid` and `$root/$uuid`
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

/// EngineFile creates an engine in the temp directory for writing, and when
/// writing is completed, it moves files to the save directory.
pub struct EngineFile {
    uuid: Uuid,
    path: EnginePath,
    engine: Option<Engine>,
}

impl EngineFile {
    /// Creates an engine in the temp directory for writing.
    fn new(
        uuid: Uuid,
        path: EnginePath,
        db_cfg: DbConfig,
        security_cfg: SecurityConfig,
    ) -> Result<EngineFile> {
        let engine = Engine::new(&path.temp, uuid, db_cfg, security_cfg)?;
        Ok(EngineFile {
            uuid,
            path,
            engine: Some(engine),
        })
    }

    /// Writes KV pairs to the engine.
    pub fn write(&self, batch: WriteBatch) -> Result<usize> {
        self.engine.as_ref().unwrap().write(batch)
    }

    /// Finish writing and move files from temp directory to save directory.
    fn close(&mut self) -> Result<()> {
        self.engine.take().unwrap().flush(true)?;
        if self.path.save.exists() {
            return Err(Error::FileExists(self.path.save.clone()));
        }
        fs::rename(&self.path.temp, &self.path.save)?;
        Ok(())
    }

    /// Close the engine and remove all temp files.
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
            warn!("cleanup"; "engine file" => ?self, "err" => %e);
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
        let importer =
            KVImporter::new(cfg, DbConfig::default(), SecurityConfig::default()).unwrap();

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
        let db_cfg = DbConfig::default();
        let security_cfg = SecurityConfig::default();
        let path = EnginePath {
            save: temp_dir.path().join("save"),
            temp: temp_dir.path().join("temp"),
        };

        // Test close.
        {
            let mut f =
                EngineFile::new(uuid, path.clone(), db_cfg.clone(), security_cfg.clone()).unwrap();
            // Cannot create the same file again.
            assert!(
                EngineFile::new(uuid, path.clone(), db_cfg.clone(), security_cfg.clone()).is_err()
            );
            assert!(path.temp.exists());
            assert!(!path.save.exists());
            f.close().unwrap();
            assert!(!path.temp.exists());
            assert!(path.save.exists());
            fs::remove_dir_all(&path.save).unwrap();
        }

        // Test cleanup.
        {
            let f =
                EngineFile::new(uuid, path.clone(), db_cfg.clone(), security_cfg.clone()).unwrap();
            assert!(path.temp.exists());
            assert!(!path.save.exists());
            drop(f);
            assert!(!path.temp.exists());
            assert!(!path.save.exists());
        }
    }
}
