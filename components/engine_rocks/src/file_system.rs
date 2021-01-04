// Copyright 2020 TiKV Project Authors. Licensed under Apache-2.0.

use crate::raw::Env;
use engine_traits::{EngineFileSystemInspector, FileSystemInspector};
use rocksdb::FileSystemInspector as DBFileSystemInspector;
use std::sync::Arc;

// Use engine::Env directly since Env is not abstracted.
pub fn get_env(
    inspector: Option<Arc<EngineFileSystemInspector>>,
    base_env: Option<Arc<Env>>,
) -> Result<Arc<Env>, String> {
    let base_env = base_env.unwrap_or_else(|| Arc::new(Env::default()));
    if let Some(inspector) = inspector {
        Ok(Arc::new(Env::new_file_system_inspected_env(
            base_env,
            Box::new(WrappedFileSystemInspector { inspector }),
        )?))
    } else {
        Ok(base_env)
    }
}

pub struct WrappedFileSystemInspector<T: FileSystemInspector> {
    inspector: Arc<T>,
}

impl<T: FileSystemInspector> DBFileSystemInspector for WrappedFileSystemInspector<T> {
    fn read(&self, len: usize) -> Result<usize, String> {
        self.inspector.read(len)
    }

    fn write(&self, len: usize) -> Result<usize, String> {
        self.inspector.write(len)
    }
}
