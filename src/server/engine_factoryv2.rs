// Copyright 2022 TiKV Project Authors. Licensed under Apache-2.0.

use std::{
    path::{Path, PathBuf},
    sync::{Arc, Mutex},
};

use collections::HashMap;
use engine_rocks::{
    raw::{Cache, Env},
    CompactionListener, RocksCompactedEvent, RocksCompactionJobInfo, RocksEngine,
    RocksEventListener,
};
use engine_traits::{CompactionJobInfo, RaftEngine, Result, TabletFactory, CF_DEFAULT, CF_WRITE};
use kvproto::kvrpcpb::ApiVersion;
use raftstore::{
    store::{RaftRouter, StoreMsg},
    RegionInfoAccessor,
};
use tikv_util::worker::Scheduler;

use crate::config::DbConfig;

const TOMBSTONE_MARK: &str = "TOMBSTONE_TABLET";

pub struct FactoryInnerV2 {
    env: Arc<Env>,
    region_info_accessor: Option<RegionInfoAccessor>,
    block_cache: Option<Cache>,
    rocksdb_config: Arc<DbConfig>,
    store_path: PathBuf,
    api_version: ApiVersion,
    flow_listener: Option<engine_rocks::FlowListener>,
    sst_recovery_sender: Option<Scheduler<String>>,
    registry: Mutex<HashMap<(u64, u64), RocksEngine>>,
}

#[derive(Clone)]
pub struct KvEngineFactoryV2<ER: RaftEngine> {
    inner: Arc<FactoryInnerV2>,
    router: Option<RaftRouter<RocksEngine, ER>>,
}

impl<ER: RaftEngine> KvEngineFactoryV2<ER> {
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

    fn create_tablet(&self, tablet_id: u64, tablet_suffix: u64) -> Result<RocksEngine> {
        // Create kv engine.
        let tablet_path = self.tablet_path(tablet_id, tablet_suffix);
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

    fn destroy_tablet(&self, tablet_path: &Path) -> engine_traits::Result<()> {
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
        /*
        engine_rocks::raw_util::destroy_engine(
            tablet_path.to_str().unwrap(),
            kv_db_opts,
            kv_cfs_opts,
        )?;*/
        let _ = std::fs::remove_dir_all(tablet_path);
        Ok(())
    }
}

impl<ER: RaftEngine> TabletFactory<RocksEngine> for KvEngineFactoryV2<ER> {
    fn create_tablet(&self, id: u64, suffix: u64) -> Result<RocksEngine> {
        let mut reg = self.inner.registry.lock().unwrap();
        if let Some(db) = reg.get(&(id, suffix)) {
            panic!("region {} {} already exists", id, db.as_inner().path());
        }

        let kv_engine = self.create_tablet(id, suffix)?;
        debug!("inserting tablet"; "key" => ?(id, suffix));
        reg.insert((id, suffix), kv_engine.clone());
        Ok(kv_engine)
    }

    fn open_tablet(&self, id: u64, suffix: u64) -> Result<RocksEngine> {
        let mut reg = self.inner.registry.lock().unwrap();
        if let Some(db) = reg.get(&(id, suffix)) {
            return Ok(db.clone());
        }

        let db_path = self.tablet_path(id, suffix);
        let db = self.open_tablet_raw(db_path.as_path(), false)?;
        debug!("open tablet"; "key" => ?(id, suffix));
        reg.insert((id, suffix), db.clone());
        Ok(db)
    }

    fn open_tablet_cache(&self, id: u64, suffix: u64) -> Option<RocksEngine> {
        let reg = self.inner.registry.lock().unwrap();
        if let Some(db) = reg.get(&(id, suffix)) {
            return Some(db.clone());
        }
        None
    }

    fn open_tablet_cache_any(&self, id: u64) -> Option<RocksEngine> {
        let reg = self.inner.registry.lock().unwrap();
        if let Some(k) = reg.keys().find(|k| k.0 == id) {
            debug!("choose a random tablet"; "key" => ?k);
            return Some(reg.get(k).unwrap().clone());
        }
        None
    }

    fn open_tablet_raw(&self, path: &Path, _readonly: bool) -> Result<RocksEngine> {
        if !RocksEngine::exists(path.to_str().unwrap_or_default()) {
            return Err(box_err!(
                "path {} does not have db",
                path.to_str().unwrap_or_default()
            ));
        }
        let (mut tablet_id, mut tablet_suffix) = (0, 1);
        if let Some(s) = path.file_name().map(|s| s.to_string_lossy()) {
            let mut split = s.split('_');
            tablet_id = split.next().and_then(|s| s.parse().ok()).unwrap_or(0);
            tablet_suffix = split.next().and_then(|s| s.parse().ok()).unwrap_or(1);
        }
        self.create_tablet(tablet_id, tablet_suffix)
    }

    #[inline]
    fn create_root_db(&self) -> Result<RocksEngine> {
        self.create_tablet(0, 0)
    }

    #[inline]
    fn exists_raw(&self, path: &Path) -> bool {
        RocksEngine::exists(path.to_str().unwrap_or_default())
    }

    #[inline]
    fn tablets_path(&self) -> PathBuf {
        self.inner.store_path.join("tablets")
    }

    #[inline]
    fn tablet_path(&self, id: u64, suffix: u64) -> PathBuf {
        self.inner
            .store_path
            .join(format!("tablets/{}_{}", id, suffix))
    }

    #[inline]
    fn mark_tombstone(&self, region_id: u64, suffix: u64) {
        let path = self.tablet_path(region_id, suffix).join(TOMBSTONE_MARK);
        std::fs::File::create(&path).unwrap();
        debug!("tombstone tablet"; "region_id" => region_id, "suffix" => suffix);
        self.inner
            .registry
            .lock()
            .unwrap()
            .remove(&(region_id, suffix));
    }

    #[inline]
    fn is_tombstoned(&self, region_id: u64, suffix: u64) -> bool {
        self.tablet_path(region_id, suffix)
            .join(TOMBSTONE_MARK)
            .exists()
    }

    #[inline]
    fn destroy_tablet(&self, id: u64, suffix: u64) -> engine_traits::Result<()> {
        let path = self.tablet_path(id, suffix);
        self.destroy_tablet(&path)
    }

    #[inline]
    fn loop_tablet_cache(&self, mut f: Box<dyn FnMut(u64, u64, &RocksEngine) + '_>) {
        let reg = self.inner.registry.lock().unwrap();
        for ((id, suffix), tablet) in &*reg {
            f(*id, *suffix, tablet)
        }
    }

    #[inline]
    fn load_tablet(&self, path: &Path, id: u64, suffix: u64) -> Result<RocksEngine> {
        let reg = self.inner.registry.lock().unwrap();
        if let Some(db) = reg.get(&(id, suffix)) {
            return Err(box_err!(
                "region {} {} already exists",
                id,
                db.as_inner().path()
            ));
        }

        let db_path = self.tablet_path(id, suffix);
        if !path.exists() {}
        std::fs::rename(path, &db_path)?;
        self.open_tablet_raw(db_path.as_path(), false)
    }

    fn clone(&self) -> Box<dyn TabletFactory<RocksEngine> + Send> {
        Box::new(std::clone::Clone::clone(self))
    }
}
