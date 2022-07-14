// Copyright 2020 TiKV Project Authors. Licensed under Apache-2.0.

use std::sync::Arc;

use engine_traits::{EngineFileSystemInspector, FileSystemInspector};
use rocksdb::FileSystemInspector as DBFileSystemInspector;

use crate::raw::Env;

// Use engine::Env directly since Env is not abstracted.
pub(crate) fn get_env(
    base_env: Option<Arc<Env>>,
    limiter: Option<Arc<file_system::IORateLimiter>>,
) -> Result<Arc<Env>, String> {
    let base_env = base_env.unwrap_or_else(|| Arc::new(Env::default()));
    Ok(Arc::new(Env::new_file_system_inspected_env(
        base_env,
        WrappedFileSystemInspector {
            inspector: EngineFileSystemInspector::from_limiter(limiter),
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
    use std::sync::Arc;

    use engine_traits::{CompactExt, CF_DEFAULT};
    use file_system::{IOOp, IORateLimiter, IORateLimiterStatistics, IOType};
    use keys::data_key;
    use rocksdb::{DBOptions, Writable, DB};
    use tempfile::Builder;

    use super::*;
    use crate::{
        compat::Compat,
        event_listener::RocksEventListener,
        raw::{ColumnFamilyOptions, DBCompressionType},
        raw_util::{new_engine_opt, CFOptions},
    };

    fn new_test_db(dir: &str) -> (Arc<DB>, Arc<IORateLimiterStatistics>) {
        let limiter = Arc::new(IORateLimiter::new_for_test());
        let mut db_opts = DBOptions::new();
        db_opts.add_event_listener(RocksEventListener::new("test_db", None));
        let env = get_env(None, Some(limiter.clone())).unwrap();
        db_opts.set_env(env);
        let mut cf_opts = ColumnFamilyOptions::new();
        cf_opts.set_disable_auto_compactions(true);
        cf_opts.compression_per_level(&[DBCompressionType::No; 7]);
        let db = Arc::new(
            new_engine_opt(dir, db_opts, vec![CFOptions::new(CF_DEFAULT, cf_opts)]).unwrap(),
        );
        (db, limiter.statistics().unwrap())
    }

    #[test]
    fn test_inspected_compact() {
        let value_size = 1024;
        let temp_dir = Builder::new()
            .prefix("test_inspected_compact")
            .tempdir()
            .unwrap();

        let (db, stats) = new_test_db(temp_dir.path().to_str().unwrap());
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
        assert!(stats.fetch(IOType::LevelZeroCompaction, IOOp::Read) > value_size * 4);
        assert!(stats.fetch(IOType::LevelZeroCompaction, IOOp::Read) < value_size * 5);
        assert!(stats.fetch(IOType::LevelZeroCompaction, IOOp::Write) > value_size * 3);
        assert!(stats.fetch(IOType::LevelZeroCompaction, IOOp::Write) < value_size * 4);
    }
}
