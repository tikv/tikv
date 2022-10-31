// Copyright 2022 TiKV Project Authors. Licensed under Apache-2.0.

use engine_traits::{Checkpoint, Code, Error, Result, Status};
use rocksdb::Checkpointer;

use crate::RocksEngine;

impl Checkpoint for RocksEngine {
    type Checkpointer = Checkpointer;

    fn new_checkpointer(&self) -> Result<Self::Checkpointer> {
        self.as_inner()
            .new_checkpointer()
            .map_err(|e| Error::Engine(Status::with_error(Code::IoError, e)))
    }
}

#[cfg(test)]
mod tests {
    use engine_traits::{Checkpoint, Peekable, SyncMutable, ALL_CFS};
    use tempfile::tempdir;

    use crate::util::new_engine;

    #[test]
    fn test_checkpoint() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("origin");
        let engine = new_engine(path.as_path().to_str().unwrap(), ALL_CFS).unwrap();
        engine.put(b"key", b"value").unwrap();

        let mut check_pointer = engine.new_checkpointer().unwrap();
        let path2 = dir.path().join("checkpoint");
        check_pointer.create_at(path2.as_path(), None, 0).unwrap();
        let engine2 = new_engine(path2.as_path().to_str().unwrap(), ALL_CFS).unwrap();
        assert_eq!(engine2.get_value(b"key").unwrap().unwrap(), b"value");
    }
}
