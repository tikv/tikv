// Copyright 2020 TiKV Project Authors. Licensed under Apache-2.0.

use std::sync::Arc;

use engine_traits::{EngineFileSystemInspector, FileSystemInspector};
use rocksdb::FileSystemInspector as DBFileSystemInspector;

use crate::{e2r, r2e, raw::Env};

// Use engine::Env directly since Env is not abstracted.
pub(crate) fn get_env(
    base_env: Option<Arc<Env>>,
    limiter: Option<Arc<file_system::IoRateLimiter>>,
) -> engine_traits::Result<Arc<Env>> {
    let base_env = base_env.unwrap_or_else(|| Arc::new(Env::default()));
    Ok(Arc::new(
        Env::new_file_system_inspected_env(
            base_env,
            WrappedFileSystemInspector {
                inspector: EngineFileSystemInspector::from_limiter(limiter),
            },
        )
        .map_err(r2e)?,
    ))
}

pub struct WrappedFileSystemInspector<T: FileSystemInspector> {
    inspector: T,
}

impl<T: FileSystemInspector> DBFileSystemInspector for WrappedFileSystemInspector<T> {
    fn read(&self, len: usize) -> Result<usize, String> {
        self.inspector.read(len).map_err(e2r)
    }

    fn write(&self, len: usize) -> Result<usize, String> {
        self.inspector.write(len).map_err(e2r)
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use engine_traits::{CompactExt, ManualCompactionOptions, MiscExt, SyncMutable, CF_DEFAULT};
    use file_system::{IoOp, IoRateLimiter, IoRateLimiterStatistics, IoType};
    use keys::data_key;
    use tempfile::Builder;

    use super::*;
    use crate::{
        event_listener::RocksEventListener, raw::DBCompressionType, util::new_engine_opt,
        RocksCfOptions, RocksDbOptions, RocksEngine,
    };

    fn new_test_db(dir: &str) -> (RocksEngine, Arc<IoRateLimiterStatistics>) {
        let limiter = Arc::new(IoRateLimiter::new_for_test());
        let mut db_opts = RocksDbOptions::default();
        db_opts.add_event_listener(RocksEventListener::new("test_db", None));
        let env = get_env(None, Some(limiter.clone())).unwrap();
        db_opts.set_env(env);
        let mut cf_opts = RocksCfOptions::default();
        cf_opts.set_disable_auto_compactions(true);
        cf_opts.compression_per_level(&[DBCompressionType::No; 7]);
        let db = new_engine_opt(dir, db_opts, vec![(CF_DEFAULT, cf_opts)]).unwrap();
        (db, limiter.statistics().unwrap())
    }

    #[test]
    fn test_inspected_compact() {
        // NOTICE: Specific to RocksDB version.
        let amplification_bytes = 2560;
        let value_size = amplification_bytes * 2;
        let temp_dir = Builder::new()
            .prefix("test_inspected_compact")
            .tempdir()
            .unwrap();

        let (db, stats) = new_test_db(temp_dir.path().to_str().unwrap());
        let value = vec![b'v'; value_size];

        db.put(&data_key(b"a1"), &value).unwrap();
        db.put(&data_key(b"a2"), &value).unwrap();
        assert_eq!(stats.fetch(IoType::Flush, IoOp::Write), 0);
        db.flush_cfs(&[], true /* wait */).unwrap();
        assert!(stats.fetch(IoType::Flush, IoOp::Write) > value_size * 2);
        assert!(stats.fetch(IoType::Flush, IoOp::Write) < value_size * 2 + amplification_bytes);
        stats.reset();
        db.put(&data_key(b"a2"), &value).unwrap();
        db.put(&data_key(b"a3"), &value).unwrap();
        db.flush_cfs(&[], true /* wait */).unwrap();
        assert!(stats.fetch(IoType::Flush, IoOp::Write) > value_size * 2);
        assert!(stats.fetch(IoType::Flush, IoOp::Write) < value_size * 2 + amplification_bytes);
        stats.reset();
        db.compact_range_cf(
            CF_DEFAULT,
            None, // start_key
            None, // end_key
            ManualCompactionOptions::new(false, 1, false),
        )
        .unwrap();
        assert!(stats.fetch(IoType::LevelZeroCompaction, IoOp::Read) > value_size * 4);
        assert!(
            stats.fetch(IoType::LevelZeroCompaction, IoOp::Read)
                < value_size * 4 + amplification_bytes
        );
        assert!(stats.fetch(IoType::LevelZeroCompaction, IoOp::Write) > value_size * 3);
        assert!(
            stats.fetch(IoType::LevelZeroCompaction, IoOp::Write)
                < value_size * 3 + amplification_bytes
        );
    }
}
