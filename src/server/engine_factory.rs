// Copyright 2022 TiKV Project Authors. Licensed under Apache-2.0.

use std::{
    path::{Path, PathBuf},
    sync::{Arc, Mutex},
};

use engine_rocks::{
    raw::{Cache, Env},
    CompactedEventSender, CompactionListener, FlowListener, RocksCompactionJobInfo, RocksEngine,
    RocksEventListener,
};
use engine_traits::{
    CfOptions, CfOptionsExt, CompactionJobInfo, OpenOptions, Result, TabletAccessor, TabletFactory,
    CF_DEFAULT, CF_WRITE,
};
use kvproto::kvrpcpb::ApiVersion;
use raftstore::RegionInfoAccessor;
use tikv_util::worker::Scheduler;

use super::engine_factory_v2::KvEngineFactoryV2;
use crate::config::{DbConfig, TikvConfig, DEFAULT_ROCKSDB_SUB_DIR};

struct FactoryInner {
    env: Arc<Env>,
    region_info_accessor: Option<RegionInfoAccessor>,
    block_cache: Option<Cache>,
    rocksdb_config: Arc<DbConfig>,
    store_path: PathBuf,
    api_version: ApiVersion,
    flow_listener: Option<engine_rocks::FlowListener>,
    sst_recovery_sender: Option<Scheduler<String>>,
    root_db: Mutex<Option<RocksEngine>>,
}

pub struct KvEngineFactoryBuilder {
    inner: FactoryInner,
    compact_event_sender: Option<Arc<dyn CompactedEventSender + Send + Sync>>,
}

impl KvEngineFactoryBuilder {
    pub fn new(env: Arc<Env>, config: &TikvConfig, store_path: impl Into<PathBuf>) -> Self {
        Self {
            inner: FactoryInner {
                env,
                region_info_accessor: None,
                block_cache: None,
                rocksdb_config: Arc::new(config.rocksdb.clone()),
                store_path: store_path.into(),
                api_version: config.storage.api_version(),
                flow_listener: None,
                sst_recovery_sender: None,
                root_db: Mutex::default(),
            },
            compact_event_sender: None,
        }
    }

    pub fn region_info_accessor(mut self, accessor: RegionInfoAccessor) -> Self {
        self.inner.region_info_accessor = Some(accessor);
        self
    }

    pub fn block_cache(mut self, cache: Cache) -> Self {
        self.inner.block_cache = Some(cache);
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

    pub fn build(self) -> KvEngineFactory {
        KvEngineFactory {
            inner: Arc::new(self.inner),
            compact_event_sender: self.compact_event_sender.clone(),
        }
    }

    pub fn build_v2(self) -> KvEngineFactoryV2 {
        let factory = KvEngineFactory {
            inner: Arc::new(self.inner),
            compact_event_sender: self.compact_event_sender.clone(),
        };
        KvEngineFactoryV2::new(factory)
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

    pub fn create_tablet(
        &self,
        tablet_path: &Path,
        region_id: u64,
        suffix: u64,
    ) -> Result<RocksEngine> {
        // Create kv engine.
        let mut kv_db_opts = self.inner.rocksdb_config.build_opt();
        kv_db_opts.set_env(self.inner.env.clone());
        kv_db_opts.add_event_listener(RocksEventListener::new(
            "kv",
            self.inner.sst_recovery_sender.clone(),
        ));
        if let Some(filter) = self.create_raftstore_compaction_listener() {
            kv_db_opts.add_event_listener(filter);
        }
        if let Some(listener) = &self.inner.flow_listener {
            kv_db_opts.add_event_listener(listener.clone_with(region_id, suffix));
        }
        let kv_cfs_opts = self.inner.rocksdb_config.build_cf_opts(
            &self.inner.block_cache,
            self.inner.region_info_accessor.as_ref(),
            self.inner.api_version,
        );
        let kv_engine = engine_rocks::util::new_engine_opt(
            tablet_path.to_str().unwrap(),
            kv_db_opts,
            kv_cfs_opts,
        );
        let mut kv_engine = match kv_engine {
            Ok(e) => e,
            Err(e) => {
                error!("failed to create kv engine"; "path" => %tablet_path.display(), "err" => ?e);
                return Err(e);
            }
        };
        let shared_block_cache = self.inner.block_cache.is_some();
        kv_engine.set_shared_block_cache(shared_block_cache);
        Ok(kv_engine)
    }

    pub fn on_tablet_created(&self, region_id: u64, suffix: u64) {
        if let Some(listener) = &self.inner.flow_listener {
            let listener = listener.clone_with(region_id, suffix);
            listener.on_created();
        }
    }

    pub fn destroy_tablet(&self, tablet_path: &Path) -> engine_traits::Result<()> {
        info!("destroy tablet"; "path" => %tablet_path.display());
        // Create kv engine.
        let mut kv_db_opts = self.inner.rocksdb_config.build_opt();
        kv_db_opts.set_env(self.inner.env.clone());
        if let Some(filter) = self.create_raftstore_compaction_listener() {
            kv_db_opts.add_event_listener(filter);
        }
        let _kv_cfs_opts = self.inner.rocksdb_config.build_cf_opts(
            &self.inner.block_cache,
            self.inner.region_info_accessor.as_ref(),
            self.inner.api_version,
        );
        // TODOTODO: call rust-rocks or tirocks to destroy_engine;
        // engine_rocks::util::destroy_engine(
        //   tablet_path.to_str().unwrap(),
        //   kv_db_opts,
        //   kv_cfs_opts,
        // )?;
        let _ = std::fs::remove_dir_all(tablet_path);
        Ok(())
    }

    pub fn on_tablet_destroy(&self, region_id: u64, suffix: u64) {
        if let Some(listener) = &self.inner.flow_listener {
            let listener = listener.clone_with(region_id, suffix);
            listener.on_destroyed();
        }
    }

    pub fn store_path(&self) -> PathBuf {
        self.inner.store_path.clone()
    }

    #[inline]
    fn kv_engine_path(&self) -> PathBuf {
        self.inner.store_path.join(DEFAULT_ROCKSDB_SUB_DIR)
    }
}

impl TabletFactory<RocksEngine> for KvEngineFactory {
    #[inline]
    fn create_shared_db(&self) -> Result<RocksEngine> {
        let root_path = self.kv_engine_path();
        let tablet = self.create_tablet(&root_path, 0, 0)?;
        let mut root_db = self.inner.root_db.lock().unwrap();
        root_db.replace(tablet.clone());
        Ok(tablet)
    }

    /// Open the root tablet according to the OpenOptions.
    ///
    /// If options.create_new is true, create the root tablet. If the tablet
    /// exists, it will fail.
    ///
    /// If options.create is true, open the the root tablet if it exists or
    /// create it otherwise.
    fn open_tablet(
        &self,
        _id: u64,
        _suffix: Option<u64>,
        options: OpenOptions,
    ) -> Result<RocksEngine> {
        if let Some(db) = self.inner.root_db.lock().unwrap().as_ref() {
            if options.create_new() {
                return Err(box_err!(
                    "root tablet {} already exists",
                    db.as_inner().path()
                ));
            }
            return Ok(db.clone());
        }
        // No need for mutex protection here since root_db creation only occurs at
        // tikv bootstrap time when there is no racing issue.
        if options.create_new() || options.create() {
            return self.create_shared_db();
        }

        Err(box_err!("root tablet has not been initialized"))
    }

    fn open_tablet_raw(
        &self,
        _path: &Path,
        _id: u64,
        _suffix: u64,
        _options: OpenOptions,
    ) -> Result<RocksEngine> {
        self.create_shared_db()
    }

    fn exists_raw(&self, _path: &Path) -> bool {
        false
    }

    fn tablet_path(&self, _id: u64, _suffix: u64) -> PathBuf {
        self.kv_engine_path()
    }

    fn tablets_path(&self) -> PathBuf {
        self.kv_engine_path()
    }

    #[inline]
    fn destroy_tablet(&self, _id: u64, _suffix: u64) -> engine_traits::Result<()> {
        Ok(())
    }

    fn set_shared_block_cache_capacity(&self, capacity: u64) -> Result<()> {
        let db = self.inner.root_db.lock().unwrap();
        let opt = db.as_ref().unwrap().get_options_cf(CF_DEFAULT).unwrap(); // FIXME unwrap
        opt.set_block_cache_capacity(capacity)?;
        Ok(())
    }
}

impl TabletAccessor<RocksEngine> for KvEngineFactory {
    fn for_each_opened_tablet(&self, f: &mut dyn FnMut(u64, u64, &RocksEngine)) {
        let db = self.inner.root_db.lock().unwrap();
        let db = db.as_ref().unwrap();
        f(0, 0, db);
    }

    fn is_single_engine(&self) -> bool {
        true
    }
}
