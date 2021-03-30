// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

use crate::storage::config::BlockCacheConfig;
use crate::storage::kv::{Result, RocksEngine};
use engine_rocks::raw::ColumnFamilyOptions;
use engine_rocks::raw_util::CFOptions;
use engine_traits::{CfName, ALL_CFS, CF_DEFAULT, CF_LOCK, CF_RAFT, CF_WRITE};
use std::path::{Path, PathBuf};

// Duplicated from rocksdb_engine
const TEMP_DIR: &str = "";

/// A builder to build a temporary `RocksEngine`.
///
/// Only used for test purpose.
#[must_use]
pub struct TestEngineBuilder {
    path: Option<PathBuf>,
    cfs: Option<Vec<CfName>>,
    enable_ttl: bool,
}

impl TestEngineBuilder {
    pub fn new() -> Self {
        Self {
            path: None,
            cfs: None,
            enable_ttl: false,
        }
    }

    /// Customize the data directory of the temporary engine.
    ///
    /// By default, TEMP_DIR will be used.
    pub fn path(mut self, path: impl AsRef<Path>) -> Self {
        self.path = Some(path.as_ref().to_path_buf());
        self
    }

    /// Customize the CFs that engine will have.
    ///
    /// By default, engine will have all CFs.
    pub fn cfs(mut self, cfs: impl AsRef<[CfName]>) -> Self {
        self.cfs = Some(cfs.as_ref().to_vec());
        self
    }

    pub fn ttl(mut self, b: bool) -> Self {
        self.enable_ttl = b;
        self
    }

    /// Build a `RocksEngine`.
    pub fn build(self) -> Result<RocksEngine> {
        let cfg_rocksdb = crate::config::DbConfig::default();
        self.build_with_cfg(&cfg_rocksdb)
    }

    pub fn build_with_cfg(self, cfg_rocksdb: &crate::config::DbConfig) -> Result<RocksEngine> {
        let path = match self.path {
            None => TEMP_DIR.to_owned(),
            Some(p) => p.to_str().unwrap().to_owned(),
        };
        let enable_ttl = self.enable_ttl;
        let cfs = self.cfs.unwrap_or_else(|| ALL_CFS.to_vec());
        let cache = BlockCacheConfig::default().build_shared_cache();
        let cfs_opts = cfs
            .iter()
            .map(|cf| match *cf {
                CF_DEFAULT => CFOptions::new(
                    CF_DEFAULT,
                    cfg_rocksdb.defaultcf.build_opt(&cache, None, enable_ttl),
                ),
                CF_LOCK => CFOptions::new(CF_LOCK, cfg_rocksdb.lockcf.build_opt(&cache)),
                CF_WRITE => CFOptions::new(CF_WRITE, cfg_rocksdb.writecf.build_opt(&cache, None)),
                CF_RAFT => CFOptions::new(CF_RAFT, cfg_rocksdb.raftcf.build_opt(&cache)),
                _ => CFOptions::new(*cf, ColumnFamilyOptions::new()),
            })
            .collect();
        RocksEngine::new(&path, &cfs, Some(cfs_opts), cache.is_some())
    }
}

#[cfg(test)]
mod tests {
    use super::super::CfStatistics;
    use super::super::PerfStatisticsInstant;
    use super::super::{Engine, Snapshot};
    use super::*;
    use crate::storage::{Cursor, CursorBuilder, ScanMode};
    use engine_traits::IterOptions;
    use kvproto::kvrpcpb::Context;
    use tikv_kv::tests::*;
    use txn_types::Key;
    use txn_types::TimeStamp;

    #[test]
    fn test_rocksdb() {
        let engine = TestEngineBuilder::new()
            .cfs(TEST_ENGINE_CFS)
            .build()
            .unwrap();
        test_base_curd_options(&engine)
    }

    #[test]
    fn test_rocksdb_linear() {
        let engine = TestEngineBuilder::new()
            .cfs(TEST_ENGINE_CFS)
            .build()
            .unwrap();
        test_linear(&engine);
    }

    #[test]
    fn test_rocksdb_statistic() {
        let engine = TestEngineBuilder::new()
            .cfs(TEST_ENGINE_CFS)
            .build()
            .unwrap();
        test_cfs_statistics(&engine);
    }

    #[test]
    fn rocksdb_reopen() {
        let dir = tempfile::Builder::new()
            .prefix("rocksdb_test")
            .tempdir()
            .unwrap();
        {
            let engine = TestEngineBuilder::new()
                .path(dir.path())
                .cfs(TEST_ENGINE_CFS)
                .build()
                .unwrap();
            must_put_cf(&engine, "cf", b"k", b"v1");
        }
        {
            let engine = TestEngineBuilder::new()
                .path(dir.path())
                .cfs(TEST_ENGINE_CFS)
                .build()
                .unwrap();
            assert_has_cf(&engine, "cf", b"k", b"v1");
        }
    }

    #[test]
    fn test_rocksdb_perf_statistics() {
        let engine = TestEngineBuilder::new()
            .cfs(TEST_ENGINE_CFS)
            .build()
            .unwrap();
        test_perf_statistics(&engine);
    }

    #[test]
    fn test_max_skippable_internal_keys_error() {
        let engine = TestEngineBuilder::new().build().unwrap();
        must_put(&engine, b"foo", b"bar");
        must_delete(&engine, b"foo");
        must_put(&engine, b"foo1", b"bar1");
        must_delete(&engine, b"foo1");
        must_put(&engine, b"foo2", b"bar2");

        let snapshot = engine.snapshot(Default::default()).unwrap();
        let mut iter_opt = IterOptions::default();
        iter_opt.set_max_skippable_internal_keys(1);
        let mut iter = Cursor::new(snapshot.iter(iter_opt).unwrap(), ScanMode::Forward, false);

        let mut statistics = CfStatistics::default();
        let res = iter.seek(&Key::from_raw(b"foo"), &mut statistics);
        assert!(res.is_err());
        assert!(res
            .unwrap_err()
            .to_string()
            .contains("Result incomplete: Too many internal keys skipped"));
    }

    fn test_perf_statistics<E: Engine>(engine: &E) {
        must_put(engine, b"foo", b"bar1");
        must_put(engine, b"foo2", b"bar2");
        must_put(engine, b"foo3", b"bar3"); // deleted
        must_put(engine, b"foo4", b"bar4");
        must_put(engine, b"foo42", b"bar42"); // deleted
        must_put(engine, b"foo5", b"bar5"); // deleted
        must_put(engine, b"foo6", b"bar6");
        must_delete(engine, b"foo3");
        must_delete(engine, b"foo42");
        must_delete(engine, b"foo5");

        let snapshot = engine.snapshot(Default::default()).unwrap();
        let mut iter = Cursor::new(
            snapshot.iter(IterOptions::default()).unwrap(),
            ScanMode::Forward,
            false,
        );

        let mut statistics = CfStatistics::default();

        let perf_statistics = PerfStatisticsInstant::new();
        iter.seek(&Key::from_raw(b"foo30"), &mut statistics)
            .unwrap();
        assert_eq!(perf_statistics.delta().0.internal_delete_skipped_count, 0);

        let perf_statistics = PerfStatisticsInstant::new();
        iter.near_seek(&Key::from_raw(b"foo55"), &mut statistics)
            .unwrap();
        assert_eq!(perf_statistics.delta().0.internal_delete_skipped_count, 2);

        let perf_statistics = PerfStatisticsInstant::new();
        iter.prev(&mut statistics);
        assert_eq!(perf_statistics.delta().0.internal_delete_skipped_count, 2);

        iter.prev(&mut statistics);
        assert_eq!(perf_statistics.delta().0.internal_delete_skipped_count, 3);

        iter.prev(&mut statistics);
        assert_eq!(perf_statistics.delta().0.internal_delete_skipped_count, 3);
    }

    #[test]
    fn test_prefix_seek_skip_tombstone() {
        let engine = TestEngineBuilder::new().build().unwrap();
        engine
            .put_cf(
                &Context::default(),
                "write",
                Key::from_raw(b"aoo").append_ts(TimeStamp::zero()),
                b"ba".to_vec(),
            )
            .unwrap();
        for key in &[
            b"foo".to_vec(),
            b"foo1".to_vec(),
            b"foo2".to_vec(),
            b"foo3".to_vec(),
        ] {
            engine
                .put_cf(
                    &Context::default(),
                    "write",
                    Key::from_raw(key).append_ts(TimeStamp::zero()),
                    b"bar".to_vec(),
                )
                .unwrap();
            engine
                .delete_cf(
                    &Context::default(),
                    "write",
                    Key::from_raw(key).append_ts(TimeStamp::zero()),
                )
                .unwrap();
        }

        engine
            .put_cf(
                &Context::default(),
                "write",
                Key::from_raw(b"foo4").append_ts(TimeStamp::zero()),
                b"bar4".to_vec(),
            )
            .unwrap();

        let snapshot = engine.snapshot(Default::default()).unwrap();
        let mut iter = CursorBuilder::new(&snapshot, CF_WRITE)
            .prefix_seek(true)
            .scan_mode(ScanMode::Forward)
            .build()
            .unwrap();

        let mut statistics = CfStatistics::default();
        let perf_statistics = PerfStatisticsInstant::new();
        iter.seek(
            &Key::from_raw(b"aoo").append_ts(TimeStamp::zero()),
            &mut statistics,
        )
        .unwrap();
        assert_eq!(iter.valid().unwrap(), true);
        assert_eq!(perf_statistics.delta().0.internal_delete_skipped_count, 0);

        let perf_statistics = PerfStatisticsInstant::new();
        iter.seek(
            &Key::from_raw(b"foo").append_ts(TimeStamp::zero()),
            &mut statistics,
        )
        .unwrap();
        assert_eq!(iter.valid().unwrap(), false);
        assert_eq!(perf_statistics.delta().0.internal_delete_skipped_count, 1);
        let perf_statistics = PerfStatisticsInstant::new();
        iter.seek(
            &Key::from_raw(b"foo1").append_ts(TimeStamp::zero()),
            &mut statistics,
        )
        .unwrap();
        assert_eq!(iter.valid().unwrap(), false);
        assert_eq!(perf_statistics.delta().0.internal_delete_skipped_count, 1);
        let perf_statistics = PerfStatisticsInstant::new();
        iter.seek(
            &Key::from_raw(b"foo2").append_ts(TimeStamp::zero()),
            &mut statistics,
        )
        .unwrap();
        assert_eq!(iter.valid().unwrap(), false);
        assert_eq!(perf_statistics.delta().0.internal_delete_skipped_count, 1);
        let perf_statistics = PerfStatisticsInstant::new();
        assert_eq!(
            iter.seek(
                &Key::from_raw(b"foo4").append_ts(TimeStamp::zero()),
                &mut statistics
            )
            .unwrap(),
            true
        );
        assert_eq!(iter.valid().unwrap(), true);
        assert_eq!(
            iter.key(&mut statistics),
            Key::from_raw(b"foo4")
                .append_ts(TimeStamp::zero())
                .as_encoded()
                .as_slice()
        );
        assert_eq!(perf_statistics.delta().0.internal_delete_skipped_count, 0);
    }
}
