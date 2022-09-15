// Copyright 2022 TiKV Project Authors. Licensed under Apache-2.0.

use std::{fs, path::Path, sync::Arc};

use engine_traits::{Code, Error, Result, Status};
use tirocks::{db::RawCfHandle, Db};

#[derive(Debug)]
#[repr(transparent)]
pub struct RocksEngine(Arc<Db>);

impl RocksEngine {
    #[inline]
    pub(crate) fn new(db: Arc<Db>) -> Self {
        RocksEngine(db)
    }

    #[inline]
    pub fn exists(path: &str) -> Result<bool> {
        let path = Path::new(path);
        if !path.exists() || !path.is_dir() {
            return Ok(false);
        }
        let current_file_path = path.join("CURRENT");
        if current_file_path.exists() && current_file_path.is_file() {
            return Ok(true);
        }

        // If path is not an empty directory, we say db exists. If path is not an empty
        // directory but db has not been created, `DB::list_column_families` fails and
        // we can clean up the directory by this indication.
        if fs::read_dir(&path).unwrap().next().is_some() {
            Err(Error::Engine(Status::with_code(Code::Corruption)))
        } else {
            Ok(false)
        }
    }

    #[cfg(test)]
    #[inline]
    pub(crate) fn as_inner(&self) -> &Arc<Db> {
        &self.0
    }

    #[inline]
    pub fn cf(&self, name: &str) -> Option<&RawCfHandle> {
        self.0.cf(name)
    }
}
