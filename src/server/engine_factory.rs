// Copyright 2022 TiKV Project Authors. Licensed under Apache-2.0.

use std::{path::Path, sync::Arc};

use encryption_export::DataKeyManager;
use engine_rocks::{
    raw::{Cache, Env},
    util::RangeCompactionFilterFactory,
    CompactedEventSender, CompactionListener, FlowListener, RocksCfOptions, RocksCompactionJobInfo,
    RocksDbOptions, RocksEngine, RocksEventListener, RocksPersistenceListener, RocksStatistics,
    TabletLogger,
};
use engine_traits::{
    CompactionJobInfo, MiscExt, PersistenceListener, Result, StateStorage, TabletContext,
    TabletFactory, CF_DEFAULT, CF_WRITE,
};
use kvproto::kvrpcpb::ApiVersion;
use raftstore::RegionInfoAccessor;
use tikv_util::worker::Scheduler;

use crate::{
    config::{CfResources, DbConfig, DbResources, TikvConfig, DEFAULT_ROCKSDB_SUB_DIR},
    storage::config::EngineType,
};

struct FactoryInner {
    region_info_accessor: Option<RegionInfoAccessor>,
    rocksdb_config: Arc<DbConfig>,
    api_version: ApiVersion,
    flow_listener: Option<engine_rocks::FlowListener>,
    sst_recovery_sender: Option<Scheduler<String>>,
    encryption_key_manager: Option<Arc<DataKeyManager>>,
    db_resources: DbResources,
    cf_resources: CfResources,
    state_storage: Option<Arc<dyn StateStorage>>,
    lite: bool,
}

pub struct KvEngineFactoryBuilder {
    inner: FactoryInner,
    compact_event_sender: Option<Arc<dyn CompactedEventSender + Send + Sync>>,
}

impl KvEngineFactoryBuilder {
    pub fn new(
        env: Arc<Env>,
        config: &TikvConfig,
        cache: Cache,
        key_manager: Option<Arc<DataKeyManager>>,
    ) -> Self {
        Self {
            inner: FactoryInner {
                region_info_accessor: None,
                rocksdb_config: Arc::new(config.rocksdb.clone()),
                api_version: config.storage.api_version(),
                flow_listener: None,
                sst_recovery_sender: None,
                encryption_key_manager: key_manager,
                db_resources: config.rocksdb.build_resources(env, config.storage.engine),
                cf_resources: config.rocksdb.build_cf_resources(cache),
                state_storage: None,
                lite: false,
            },
            compact_event_sender: None,
        }
    }

    pub fn region_info_accessor(mut self, accessor: RegionInfoAccessor) -> Self {
        self.inner.region_info_accessor = Some(accessor);
        self
    }

    pub fn flow_listener(mut self, listener: FlowListener) -> Self {
        self.inner.flow_listener = Some(listener);
        self
    }

    pub fn sst_recovery_sender(mut self, sender: Option<Scheduler<String>>) -> Self {
        self.inner.sst_recovery_sender = sender;
        self
    }

    pub fn compaction_event_sender(
        mut self,
        sender: Arc<dyn CompactedEventSender + Send + Sync>,
    ) -> Self {
        self.compact_event_sender = Some(sender);
        self
    }

    /// Set whether enable lite mode.
    ///
    /// In lite mode, most listener/filters will not be installed.
    pub fn lite(mut self, lite: bool) -> Self {
        self.inner.lite = lite;
        self
    }

    /// A storage for persisting flush states, which is used for recovering when
    /// disable WAL. Only work for v2.
    pub fn state_storage(mut self, storage: Arc<dyn StateStorage>) -> Self {
        self.inner.state_storage = Some(storage);
        self
    }

    pub fn build(self) -> KvEngineFactory {
        KvEngineFactory {
            inner: Arc::new(self.inner),
            compact_event_sender: self.compact_event_sender.clone(),
        }
    }
}

#[derive(Clone)]
pub struct KvEngineFactory {
    inner: Arc<FactoryInner>,
    compact_event_sender: Option<Arc<dyn CompactedEventSender + Send + Sync>>,
}

impl KvEngineFactory {
    pub fn create_raftstore_compaction_listener(&self) -> Option<CompactionListener> {
        self.compact_event_sender.as_ref()?;
        fn size_change_filter(info: &RocksCompactionJobInfo<'_>) -> bool {
            // When calculating region size, we only consider write and default
            // column families.
            let cf = info.cf_name();
            if cf != CF_WRITE && cf != CF_DEFAULT {
                return false;
            }
            // Compactions in level 0 and level 1 are very frequently.
            if info.output_level() < 2 {
                return false;
            }

            true
        }
        Some(CompactionListener::new(
            self.compact_event_sender.as_ref().unwrap().clone(),
            Some(size_change_filter),
        ))
    }

    pub fn rocks_statistics(&self) -> Arc<RocksStatistics> {
        self.inner.db_resources.statistics.clone()
    }

    fn db_opts(&self, for_engine: EngineType) -> RocksDbOptions {
        // Create kv engine.
        let mut db_opts = self
            .inner
            .rocksdb_config
            .build_opt(&self.inner.db_resources, for_engine);
        if !self.inner.lite {
            db_opts.add_event_listener(RocksEventListener::new(
                "kv",
                self.inner.sst_recovery_sender.clone(),
            ));
            if let Some(filter) = self.create_raftstore_compaction_listener() {
                db_opts.add_event_listener(filter);
            }
        }
        db_opts
    }

    fn cf_opts(
        &self,
        filter_factory: Option<&RangeCompactionFilterFactory>,
        for_engine: EngineType,
    ) -> Vec<(&str, RocksCfOptions)> {
        self.inner.rocksdb_config.build_cf_opts(
            &self.inner.cf_resources,
            self.inner.region_info_accessor.as_ref(),
            self.inner.api_version,
            filter_factory,
            for_engine,
        )
    }

    pub fn block_cache(&self) -> &Cache {
        &self.inner.cf_resources.cache
    }

    /// Create a shared db.
    ///
    /// It will always create in path/DEFAULT_DB_SUB_DIR.
    pub fn create_shared_db(&self, path: impl AsRef<Path>) -> Result<RocksEngine> {
        let path = path.as_ref();
        let mut db_opts = self.db_opts(EngineType::RaftKv);
        let cf_opts = self.cf_opts(None, EngineType::RaftKv);
        if let Some(listener) = &self.inner.flow_listener {
            db_opts.add_event_listener(listener.clone());
        }
        let target_path = path.join(DEFAULT_ROCKSDB_SUB_DIR);
        let kv_engine =
            engine_rocks::util::new_engine_opt(target_path.to_str().unwrap(), db_opts, cf_opts);
        if let Err(e) = &kv_engine {
            error!("failed to create kv engine"; "path" => %path.display(), "err" => ?e);
        }
        kv_engine
    }
}

impl TabletFactory<RocksEngine> for KvEngineFactory {
    fn open_tablet(&self, ctx: TabletContext, path: &Path) -> Result<RocksEngine> {
        let mut db_opts = self.db_opts(EngineType::RaftKv2);
        let tablet_name = path.file_name().unwrap().to_str().unwrap().to_string();
        db_opts.set_info_log(TabletLogger::new(tablet_name));
        let factory = RangeCompactionFilterFactory::new(ctx.start_key.clone(), ctx.end_key.clone());
        let cf_opts = self.cf_opts(Some(&factory), EngineType::RaftKv2);
        if let Some(listener) = &self.inner.flow_listener {
            db_opts.add_event_listener(listener.clone_with(ctx.id));
        }
        if let Some(storage) = &self.inner.state_storage
            && let Some(flush_state) = ctx.flush_state {
            let listener = PersistenceListener::new(
                ctx.id,
                ctx.suffix.unwrap(),
                flush_state,
                storage.clone(),
            );
            db_opts.add_event_listener(RocksPersistenceListener::new(listener));
        }
        let kv_engine =
            engine_rocks::util::new_engine_opt(path.to_str().unwrap(), db_opts, cf_opts);
        if let Err(e) = &kv_engine {
            error!("failed to create tablet"; "id" => ctx.id, "suffix" => ?ctx.suffix, "path" => %path.display(), "err" => ?e);
        } else if let Some(listener) = &self.inner.flow_listener {
            listener.clone_with(ctx.id).on_created();
        }
        kv_engine
    }

    fn destroy_tablet(&self, ctx: TabletContext, path: &Path) -> Result<()> {
        info!("destroy tablet"; "path" => %path.display(), "region_id" => ctx.id, "suffix" => ?ctx.suffix);
        // Create kv engine.
        let _db_opts = self.db_opts(EngineType::RaftKv2);
        let _cf_opts = self.cf_opts(None, EngineType::RaftKv2);
        // TODOTODO: call rust-rocks or tirocks to destroy_engine;
        // engine_rocks::util::destroy_engine(
        //   path.to_str().unwrap(),
        //   kv_db_opts,
        //   kv_cfs_opts,
        // )?;
        // TODO: use RocksDB::DestroyDB.
        let _ =
            encryption_export::trash_dir_all(path, self.inner.encryption_key_manager.as_deref());
        if let Some(listener) = &self.inner.flow_listener {
            listener.clone_with(ctx.id).on_destroyed();
        }
        Ok(())
    }

    fn exists(&self, path: &Path) -> bool {
        RocksEngine::exists(path.to_str().unwrap())
    }

    #[cfg(feature = "testexport")]
    fn set_state_storage(&self, state_storage: Arc<dyn StateStorage>) {
        let inner = Arc::as_ptr(&self.inner) as *mut FactoryInner;
        unsafe {
            (*inner).state_storage = Some(state_storage);
        }
    }
}

#[cfg(test)]
mod tests {
    use std::path::Path;

    use engine_traits::{Peekable, SyncMutable, TabletRegistry};
    use kvproto::metapb::Region;

    use super::*;
    use crate::config::TikvConfig;

    fn build_test(name: &'static str) -> (tempfile::TempDir, TabletRegistry<RocksEngine>) {
        let manifest_dir = Path::new(env!("CARGO_MANIFEST_DIR"));
        let common_test_cfg = manifest_dir.join("components/test_raftstore/src/common-test.toml");
        let cfg = TikvConfig::from_file(&common_test_cfg, None).unwrap_or_else(|e| {
            panic!(
                "invalid auto generated configuration file {}, err {}",
                manifest_dir.display(),
                e
            );
        });
        let cache = cfg.storage.block_cache.build_shared_cache();
        let dir = test_util::temp_dir(name, false);
        let env = cfg.build_shared_rocks_env(None, None).unwrap();

        let factory = KvEngineFactoryBuilder::new(env, &cfg, cache, None).build();
        let reg = TabletRegistry::new(Box::new(factory), dir.path()).unwrap();
        (dir, reg)
    }

    #[test]
    fn test_engine_factory() {
        let (_dir, reg) = build_test("test_engine_factory");

        let path = reg.tablet_path(1, 3);
        assert!(!reg.tablet_factory().exists(&path));
        let mut tablet_ctx = TabletContext::with_infinite_region(1, Some(3));
        let engine = reg
            .tablet_factory()
            .open_tablet(tablet_ctx.clone(), &path)
            .unwrap();
        assert!(reg.tablet_factory().exists(&path));
        // Second attempt should fail with lock.
        reg.tablet_factory()
            .open_tablet(tablet_ctx.clone(), &path)
            .unwrap_err();
        drop(engine);
        tablet_ctx.suffix = Some(3);
        reg.tablet_factory()
            .destroy_tablet(tablet_ctx, &path)
            .unwrap();
        assert!(!reg.tablet_factory().exists(&path));
    }

    #[test]
    fn test_engine_factory_compaction_filter() {
        let (_dir, reg) = build_test("test_engine_factory_compaction_filter");

        let region = Region {
            id: 1,
            start_key: b"k1".to_vec(),
            end_key: b"k3".to_vec(),
            ..Default::default()
        };
        let tablet_ctx = TabletContext::new(&region, Some(3));
        let path = reg.tablet_path(1, 3);
        let engine = reg.tablet_factory().open_tablet(tablet_ctx, &path).unwrap();
        engine.put(&keys::data_key(b"k0"), b"v0").unwrap();
        engine.put(&keys::data_key(b"k1"), b"v1").unwrap();
        engine.put(&keys::data_key(b"k2"), b"v2").unwrap();
        engine.put(&keys::data_key(b"k3"), b"v3").unwrap();
        engine.put(&keys::data_key(b"k4"), b"v4").unwrap();
        engine.flush_cfs(&[], true).unwrap();
        assert!(engine.get_value(&keys::data_key(b"k0")).unwrap().is_none());
        assert_eq!(
            engine.get_value(&keys::data_key(b"k1")).unwrap().unwrap(),
            b"v1"
        );
        assert_eq!(
            engine.get_value(&keys::data_key(b"k2")).unwrap().unwrap(),
            b"v2"
        );
        assert!(engine.get_value(&keys::data_key(b"k3")).unwrap().is_none());
        assert!(engine.get_value(&keys::data_key(b"k4")).unwrap().is_none());
    }
}
