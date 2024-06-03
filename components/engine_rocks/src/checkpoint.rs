// Copyright 2022 TiKV Project Authors. Licensed under Apache-2.0.

use std::path::Path;

use engine_traits::{Checkpointable, Checkpointer, Result};

use crate::{r2e, RocksEngine};

impl Checkpointable for RocksEngine {
    type Checkpointer = RocksEngineCheckpointer;

    fn new_checkpointer(&self) -> Result<Self::Checkpointer> {
        match self.as_inner().new_checkpointer() {
            Ok(pointer) => Ok(RocksEngineCheckpointer(pointer)),
            Err(e) => Err(r2e(e)),
        }
    }

    fn merge(&self, dbs: &[&Self]) -> Result<()> {
        let mut mopts = rocksdb::MergeInstanceOptions::default();
        mopts.merge_memtable = false;
        mopts.allow_source_write = true;
        let inner: Vec<_> = dbs.iter().map(|e| e.as_inner().as_ref()).collect();
        self.as_inner().merge_instances(&mopts, &inner).map_err(r2e)
    }
}

pub struct RocksEngineCheckpointer(rocksdb::Checkpointer);

impl Checkpointer for RocksEngineCheckpointer {
    fn create_at(
        &mut self,
        db_out_dir: &Path,
        titan_out_dir: Option<&Path>,
        log_size_for_flush: u64,
    ) -> Result<()> {
        #[cfg(any(test, feature = "testexport"))]
        file_system::delete_dir_if_exist(db_out_dir).unwrap();
        self.0
            .create_at(db_out_dir, titan_out_dir, log_size_for_flush)
            .map_err(|e| r2e(e))
    }
}

#[cfg(test)]
mod tests {
    use engine_traits::{Checkpointable, Checkpointer, Peekable, SyncMutable, ALL_CFS};
    use tempfile::tempdir;

    use crate::util::new_engine;

    #[test]
    fn test_checkpoint() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("origin");
        let engine = new_engine(path.as_path().to_str().unwrap(), ALL_CFS).unwrap();
        engine.put(b"key", b"value").unwrap();

        let mut check_pointer = engine.new_checkpointer().unwrap();

        // engine.pause_background_work().unwrap();
        // let path2 = dir.path().join("checkpoint");
        // check_pointer
        //     .create_at(path2.as_path(), None, 0)
        //     .unwrap_err();
        // engine.continue_background_work().unwrap();

        let path2 = dir.path().join("checkpoint");
        check_pointer.create_at(path2.as_path(), None, 0).unwrap();
        let engine2 = new_engine(path2.as_path().to_str().unwrap(), ALL_CFS).unwrap();
        assert_eq!(engine2.get_value(b"key").unwrap().unwrap(), b"value");
    }
}
