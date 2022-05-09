// Copyright 2022 TiKV Project Authors. Licensed under Apache-2.0.

use std::{
    path::{Path, PathBuf},
    sync::{Arc, Mutex},
};

use engine_rocks::{
    raw::{Cache, Env},
    CompactionListener, FlowListener, RocksCompactedEvent, RocksCompactionJobInfo, RocksEngine,
    RocksEventListener,
};
use engine_traits::{CompactionJobInfo, RaftEngine, Result, TabletFactory, CF_DEFAULT, CF_WRITE};
use kvproto::kvrpcpb::ApiVersion;
use raftstore::{
    store::{RaftRouter, StoreMsg},
    RegionInfoAccessor,
};
use tikv_util::worker::Scheduler;

use crate::config::{DbConfig, TiKvConfig, DEFAULT_ROCKSDB_SUB_DIR};

struct FactoryInner {
    env: Arc<Env>,
    region_info_accessor: Option<RegionInfoAccessor>,
    block_cache: Option<Cache>,
    rocksdb_config: Arc<DbConfig>,
    store_path: PathBuf,
    api_version: ApiVersion,
    flow_listener: Option<engine_rocks::FlowListener>,
    sst_recovery_sender: Option<Scheduler<String>>,
}

pub struct KvEngineFactoryBuilder<ER: RaftEngine> {
    inner: FactoryInner,
    router: Option<RaftRouter<RocksEngine, ER>>,
}

impl<ER: RaftEngine> KvEngineFactoryBuilder<ER> {
    pub fn new(env: Arc<Env>, config: &TiKvConfig, store_path: impl Into<PathBuf>) -> Self {
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
            },
            router: None,
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

    pub fn compaction_filter_router(mut self, router: RaftRouter<RocksEngine, ER>) -> Self {
        self.router = Some(router);
        self
    }

    pub fn build(self) -> KvEngineFactory<ER> {
        KvEngineFactory {
            inner: Arc::new(self.inner),
            router: self.router,
        }
    }
}

#[derive(Clone)]
pub struct KvEngineFactory<ER: RaftEngine> {
    inner: Arc<FactoryInner>,
    router: Option<RaftRouter<RocksEngine, ER>>,
}

impl<ER: RaftEngine> KvEngineFactory<ER> {
    fn create_raftstore_compaction_listener(&self) -> Option<CompactionListener> {
        let ch = match &self.router {
            Some(r) => Mutex::new(r.clone()),
            None => return None,
        };
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

        let compacted_handler = Box::new(move |compacted_event: RocksCompactedEvent| {
            let ch = ch.lock().unwrap();
            let event = StoreMsg::CompactedEvent(compacted_event);
            if let Err(e) = ch.send_control(event) {
                error_unknown!(?e; "send compaction finished event to raftstore failed");
            }
        });
        Some(CompactionListener::new(
            compacted_handler,
            Some(size_change_filter),
        ))
    }

    fn create_tablet(&self, tablet_path: &Path) -> Result<RocksEngine> {
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
            kv_db_opts.add_event_listener(listener.clone());
        }
        let kv_cfs_opts = self.inner.rocksdb_config.build_cf_opts(
            &self.inner.block_cache,
            self.inner.region_info_accessor.as_ref(),
            self.inner.api_version,
        );
        let kv_engine = engine_rocks::raw_util::new_engine_opt(
            tablet_path.to_str().unwrap(),
            kv_db_opts,
            kv_cfs_opts,
        );
        let kv_engine = match kv_engine {
            Ok(e) => e,
            Err(e) => {
                error!("failed to create kv engine"; "path" => %tablet_path.display(), "err" => ?e);
                return Err(e);
            }
        };
        let mut kv_engine = RocksEngine::from_db(Arc::new(kv_engine));
        let shared_block_cache = self.inner.block_cache.is_some();
        kv_engine.set_shared_block_cache(shared_block_cache);
        Ok(kv_engine)
    }

    #[inline]
    fn kv_engine_path(&self) -> PathBuf {
        self.inner.store_path.join(DEFAULT_ROCKSDB_SUB_DIR)
    }
}

impl<ER: RaftEngine> TabletFactory<RocksEngine> for KvEngineFactory<ER> {
    #[inline]
    fn create_tablet(&self) -> Result<RocksEngine> {
        let root_path = self.kv_engine_path();
        self.create_tablet(&root_path)
    }
}
