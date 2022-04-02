// Copyright 2022 TiKV Project Authors. Licensed under Apache-2.0.

use crate::config::{DbConfig, TiKvConfig, DEFAULT_ROCKSDB_SUB_DIR};
use engine_rocks::raw::{Cache, Env};
use engine_rocks::{CompactionListener, RocksCompactedEvent, RocksCompactionJobInfo, RocksEngine};
use engine_traits::{CompactionJobInfo, RaftEngine, Result, TabletFactory, CF_DEFAULT, CF_WRITE};
use kvproto::kvrpcpb::ApiVersion;
use raftstore::store::{RaftRouter, StoreMsg};
use raftstore::RegionInfoAccessor;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::Mutex;

struct FactoryInner {
    env: Option<Arc<Env>>,
    region_info_accessor: Option<RegionInfoAccessor>,
    block_cache: Option<Cache>,
    rocksdb_config: Arc<DbConfig>,
    store_path: PathBuf,
    api_version: ApiVersion,
    flow_listener: Option<engine_rocks::FlowListener>,
}

#[derive(Clone)]
pub struct KvEngineFactory<ER: RaftEngine> {
    inner: Arc<FactoryInner>,
    router: Option<RaftRouter<RocksEngine, ER>>,
}

impl<ER: RaftEngine> KvEngineFactory<ER> {
    #[inline]
    pub fn new(
        env: Option<Arc<Env>>,
        config: &TiKvConfig,
        region_info_accessor: Option<RegionInfoAccessor>,
        block_cache: Option<Cache>,
        store_path: PathBuf,
        router: Option<RaftRouter<RocksEngine, ER>>,
        flow_listener: Option<engine_rocks::FlowListener>,
    ) -> KvEngineFactory<ER> {
        KvEngineFactory {
            inner: Arc::new(FactoryInner {
                env,
                region_info_accessor,
                block_cache,
                rocksdb_config: Arc::new(config.rocksdb.clone()),
                store_path,
                api_version: config.storage.api_version(),
                flow_listener,
            }),
            router,
        }
    }

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
        if let Some(env) = &self.inner.env {
            kv_db_opts.set_env(env.clone());
        }
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
