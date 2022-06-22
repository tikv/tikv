// Copyright 2022 TiKV Project Authors. Licensed under Apache-2.0.

use std::{
    path::{Path, PathBuf},
    sync::{Arc, Mutex},
};

use collections::HashMap;
use engine_rocks::RocksEngine;
use engine_traits::{RaftEngine, Result, TabletFactory};

use crate::server::engine_factory::KvEngineFactory;

const TOMBSTONE_MARK: &str = "TOMBSTONE_TABLET";

#[derive(Clone)]
pub struct KvEngineFactoryV2<ER: RaftEngine> {
    inner: KvEngineFactory<ER>,
    registry: Arc<Mutex<HashMap<(u64, u64), RocksEngine>>>,
}

impl<ER: RaftEngine> TabletFactory<RocksEngine> for KvEngineFactoryV2<ER> {
    fn create_tablet(&self, id: u64, suffix: u64) -> Result<RocksEngine> {
        let mut reg = self.registry.lock().unwrap();
        if let Some(db) = reg.get(&(id, suffix)) {
            return Err(box_err!(
                "region {} {} already exists",
                id,
                db.as_inner().path()
            ));
        }
        let tablet_path = self.tablet_path(id, suffix);
        let kv_engine = self.inner.create_tablet(&tablet_path, id, suffix)?;
        debug!("inserting tablet"; "key" => ?(id, suffix));
        reg.insert((id, suffix), kv_engine.clone());
        self.inner.on_tablet_created(id, suffix);
        Ok(kv_engine)
    }

    fn open_tablet(&self, id: u64, suffix: u64) -> Result<RocksEngine> {
        let mut reg = self.registry.lock().unwrap();
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
        let reg = self.registry.lock().unwrap();
        if let Some(db) = reg.get(&(id, suffix)) {
            return Some(db.clone());
        }
        None
    }

    fn open_tablet_cache_any(&self, id: u64) -> Option<RocksEngine> {
        let reg = self.registry.lock().unwrap();
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
    fn create_shared_db(&self) -> Result<RocksEngine> {
        self.create_tablet(0, 0)
    }

    #[inline]
    fn exists_raw(&self, path: &Path) -> bool {
        RocksEngine::exists(path.to_str().unwrap_or_default())
    }

    #[inline]
    fn tablets_path(&self) -> PathBuf {
        self.inner.store_path().join("tablets")
    }

    #[inline]
    fn tablet_path(&self, id: u64, suffix: u64) -> PathBuf {
        self.inner
            .store_path()
            .join(format!("tablets/{}_{}", id, suffix))
    }

    #[inline]
    fn mark_tombstone(&self, region_id: u64, suffix: u64) {
        let path = self.tablet_path(region_id, suffix).join(TOMBSTONE_MARK);
        std::fs::File::create(&path).unwrap();
        debug!("tombstone tablet"; "region_id" => region_id, "suffix" => suffix);
        self.registry.lock().unwrap().remove(&(region_id, suffix));
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
        self.registry.lock().unwrap().remove(&(id, suffix));
        self.inner.destroy_tablet(&path)?;
        self.inner.on_tablet_destroy(id, suffix);
        Ok(())
    }

    #[inline]
    fn loop_tablet_cache(&self, mut f: Box<dyn FnMut(u64, u64, &RocksEngine) + '_>) {
        let reg = self.registry.lock().unwrap();
        for ((id, suffix), tablet) in &*reg {
            f(*id, *suffix, tablet)
        }
    }

    #[inline]
    fn load_tablet(&self, path: &Path, id: u64, suffix: u64) -> Result<RocksEngine> {
        {
            let reg = self.registry.lock().unwrap();
            if let Some(db) = reg.get(&(id, suffix)) {
                return Err(box_err!(
                    "region {} {} already exists",
                    id,
                    db.as_inner().path()
                ));
            }
        }

        let db_path = self.tablet_path(id, suffix);
        std::fs::rename(path, &db_path)?;
        self.open_tablet_raw(db_path.as_path(), false)
    }

    fn clone(&self) -> Box<dyn TabletFactory<RocksEngine> + Send> {
        Box::new(std::clone::Clone::clone(self))
    }
}

#[cfg(test)]
mod tests {
    use engine_traits::TabletFactory;

    use super::*;
    use crate::{config::TiKvConfig, server::KvEngineFactoryBuilder};

    lazy_static! {
        static ref TEST_CONFIG: TiKvConfig = {
            let manifest_dir = Path::new(env!("CARGO_MANIFEST_DIR"));
            let common_test_cfg =
                manifest_dir.join("components/test_raftstore/src/common-test.toml");
            TiKvConfig::from_file(&common_test_cfg, None).unwrap_or_else(|e| {
                panic!(
                    "invalid auto generated configuration file {}, err {}",
                    manifest_dir.display(),
                    e
                );
            })
        };
    }

    impl<ER: RaftEngine> KvEngineFactoryV2<ER> {
        pub fn new(inner: KvEngineFactory<ER>) -> Self {
            KvEngineFactoryV2 {
                inner,
                registry: Arc::new(Mutex::new(HashMap::default())),
            }
        }
    }

    #[test]
    fn test_kvengine_factory() {
        let cfg = TEST_CONFIG.clone();
        let dir = test_util::temp_dir("test_kvengine_factory", false);
        let env = cfg.build_shared_rocks_env(None, None).unwrap();

        let builder = KvEngineFactoryBuilder::<RocksEngine>::new(env, &cfg, dir.path());
        let factory = builder.build();
        let shared_db = factory.create_shared_db().unwrap();
        let tablet = TabletFactory::create_tablet(&factory, 1, 10);
        assert!(tablet.is_ok());
        let tablet = tablet.unwrap();
        let tablet2 = factory.open_tablet(1, 10).unwrap();
        assert_eq!(tablet.as_inner().path(), shared_db.as_inner().path());
        assert_eq!(tablet.as_inner().path(), tablet2.as_inner().path());
        let tablet2 = factory.open_tablet_cache(1, 10).unwrap();
        assert_eq!(tablet.as_inner().path(), tablet2.as_inner().path());
        let tablet2 = factory.open_tablet_cache_any(1).unwrap();
        assert_eq!(tablet.as_inner().path(), tablet2.as_inner().path());
        let tablet_path = factory.tablet_path(1, 10);
        let tablet2 = factory.open_tablet_raw(&tablet_path, false).unwrap();
        assert_eq!(tablet.as_inner().path(), tablet2.as_inner().path());
    }

    #[test]
    fn test_kvengine_factory_v2() {
        let cfg = TEST_CONFIG.clone();
        let dir = test_util::temp_dir("test_kvengine_factory_v2", false);
        let env = cfg.build_shared_rocks_env(None, None).unwrap();

        let builder = KvEngineFactoryBuilder::<RocksEngine>::new(env, &cfg, dir.path());
        let inner_factory = builder.build();
        let factory = KvEngineFactoryV2::new(inner_factory);
        let tablet = factory.create_tablet(1, 10);
        assert!(tablet.is_ok());
        let tablet = tablet.unwrap();
        let tablet2 = factory.open_tablet(1, 10).unwrap();
        assert_eq!(tablet.as_inner().path(), tablet2.as_inner().path());
        let tablet2 = factory.open_tablet_cache(1, 10).unwrap();
        assert_eq!(tablet.as_inner().path(), tablet2.as_inner().path());
        let tablet2 = factory.open_tablet_cache_any(1).unwrap();
        assert_eq!(tablet.as_inner().path(), tablet2.as_inner().path());
        let tablet_path = factory.tablet_path(1, 10);
        let result = factory.open_tablet_raw(&tablet_path, false);
        assert!(result.is_err());

        assert!(factory.exists(1, 10));
        assert!(!factory.exists(1, 11));
        assert!(!factory.exists(2, 10));
        assert!(!factory.exists(2, 11));
        assert!(factory.exists_raw(&tablet_path));
        assert!(!factory.is_tombstoned(1, 10));
        assert!(factory.load_tablet(&tablet_path, 1, 10).is_err());
        assert!(factory.load_tablet(&tablet_path, 1, 20).is_ok());
        factory.mark_tombstone(1, 20);
        assert!(factory.is_tombstoned(1, 20));
        factory.destroy_tablet(1, 20).unwrap();
        let result = factory.open_tablet(1, 20);
        assert!(result.is_err());
    }

    #[test]
    fn test_get_live_tablets() {
        let cfg = TEST_CONFIG.clone();
        let dir = test_util::temp_dir("test_get_live_tablets", false);
        let env = cfg.build_shared_rocks_env(None, None).unwrap();

        let builder = KvEngineFactoryBuilder::<RocksEngine>::new(env, &cfg, dir.path());
        let inner_factory = builder.build();
        let factory = KvEngineFactoryV2::new(inner_factory);
        factory.create_tablet(1, 10).unwrap();
        factory.create_tablet(2, 10).unwrap();
        let mut count = 0;
        factory.loop_tablet_cache(Box::new(|id, suffix, _tablet| {
            assert!(id == 1 || id == 2);
            assert!(suffix == 10);
            count += 1;
        }));
        assert_eq!(count, 2);
    }
}
