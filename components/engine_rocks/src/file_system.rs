// Copyright 2020 TiKV Project Authors. Licensed under Apache-2.0.

use crate::raw::Env;
use engine_traits::{EngineFileSystemInspector, FileSystemInspector};
use rocksdb::FileSystemInspector as DBFileSystemInspector;
use std::sync::Arc;

// Use engine::Env directly since Env is not abstracted.
pub fn get_env(base_env: Option<Arc<Env>>) -> Result<Arc<Env>, String> {
    let base_env = base_env.unwrap_or_else(|| Arc::new(Env::default()));
    Ok(Arc::new(Env::new_file_system_inspected_env(
        base_env,
        WrappedFileSystemInspector {
            inspector: EngineFileSystemInspector::new(),
        },
    )?))
}

pub struct WrappedFileSystemInspector<T: FileSystemInspector> {
    inspector: T,
}

impl<T: FileSystemInspector> DBFileSystemInspector for WrappedFileSystemInspector<T> {
    fn read(&self, len: usize) -> Result<usize, String> {
        self.inspector.read(len)
    }

    fn write(&self, len: usize) -> Result<usize, String> {
        self.inspector.write(len)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::compat::Compat;
    use crate::event_listener::RocksEventListener;
    use crate::raw::{ColumnFamilyOptions, DBCompressionType};
    use crate::raw_util::{new_engine_opt, CFOptions};
    use engine_traits::{CompactExt, CF_DEFAULT};
    use file_system::{IOOp, IORateLimiterStatistics, IOType, WithIORateLimit};
    use keys::data_key;
    use rocksdb::Writable;
    use rocksdb::{DBOptions, DB};
    use std::sync::Arc;
    use tempfile::Builder;

    fn new_test_db(dir: &str) -> (Arc<DB>, Arc<IORateLimiterStatistics>, WithIORateLimit) {
        let (guard, stats) = WithIORateLimit::new(0);
        let mut db_opts = DBOptions::new();
        db_opts.add_event_listener(RocksEventListener::new("test_db"));
        let env = get_env(None).unwrap();
        db_opts.set_env(env);
        let mut cf_opts = ColumnFamilyOptions::new();
        cf_opts.set_disable_auto_compactions(true);
        cf_opts.compression_per_level(&[DBCompressionType::No; 7]);
        let db = Arc::new(
            new_engine_opt(dir, db_opts, vec![CFOptions::new(CF_DEFAULT, cf_opts)]).unwrap(),
        );
        (db, stats, guard)
    }

    #[test]
    fn test_inspected_compact() {
        let value_size = 1024;
        let temp_dir = Builder::new()
            .prefix("test_inspected_compact")
            .tempdir()
            .unwrap();

        let (db, stats, _guard) = new_test_db(temp_dir.path().to_str().unwrap());
        let value = vec![b'v'; value_size];

        db.put(&data_key(b"a1"), &value).unwrap();
        db.put(&data_key(b"a2"), &value).unwrap();
        db.flush(true /*sync*/).unwrap();
        assert!(stats.fetch(IOType::Flush, IOOp::Write) > value_size * 2);
        assert!(stats.fetch(IOType::Flush, IOOp::Write) < value_size * 3);
        stats.reset();
        db.put(&data_key(b"a2"), &value).unwrap();
        db.put(&data_key(b"a3"), &value).unwrap();
        db.flush(true /*sync*/).unwrap();
        assert!(stats.fetch(IOType::Flush, IOOp::Write) > value_size * 2);
        assert!(stats.fetch(IOType::Flush, IOOp::Write) < value_size * 3);
        stats.reset();
        db.c()
            .compact_range(
                CF_DEFAULT, None,  /*start_key*/
                None,  /*end_key*/
                false, /*exclusive_manual*/
                1,     /*max_subcompactions*/
            )
            .unwrap();
        assert!(stats.fetch(IOType::Compaction, IOOp::Read) > value_size * 4);
        assert!(stats.fetch(IOType::Compaction, IOOp::Read) < value_size * 5);
        assert!(stats.fetch(IOType::Compaction, IOOp::Write) > value_size * 3);
        assert!(stats.fetch(IOType::Compaction, IOOp::Write) < value_size * 4);
    }
}
