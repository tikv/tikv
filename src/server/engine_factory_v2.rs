// Copyright 2022 TiKV Project Authors. Licensed under Apache-2.0.

use std::{
    path::{Path, PathBuf},
    sync::{Arc, Mutex},
};

use collections::HashMap;
use engine_rocks::RocksEngine;
use engine_traits::{
    CFOptionsExt, ColumnFamilyOptions, Result, TabletAccessor, TabletFactory, CF_DEFAULT,
};

use crate::server::engine_factory::KvEngineFactory;

const TOMBSTONE_MARK: &str = "TOMBSTONE_TABLET";

#[derive(Clone)]
pub struct KvEngineFactoryV2 {
    inner: KvEngineFactory,
    registry: Arc<Mutex<HashMap<(u64, u64), RocksEngine>>>,
}

// Extract tablet id and tablet suffix from the path.
fn get_id_and_suffix_from_path(path: &Path) -> (u64, u64) {
    let (mut tablet_id, mut tablet_suffix) = (0, 1);
    if let Some(s) = path.file_name().map(|s| s.to_string_lossy()) {
        let mut split = s.split('_');
        tablet_id = split.next().and_then(|s| s.parse().ok()).unwrap_or(0);
        tablet_suffix = split.next().and_then(|s| s.parse().ok()).unwrap_or(1);
    }
    (tablet_id, tablet_suffix)
}

impl TabletFactory<RocksEngine> for KvEngineFactoryV2 {
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
        let (tablet_id, tablet_suffix) = get_id_and_suffix_from_path(path);
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
        let new_engine = self.open_tablet_raw(db_path.as_path(), false);
        if new_engine.is_ok() {
            let (old_id, old_suffix) = get_id_and_suffix_from_path(path);
            self.registry.lock().unwrap().remove(&(old_id, old_suffix));
        }
        new_engine
    }

    fn set_shared_block_cache_capacity(&self, capacity: u64) -> Result<()> {
        let reg = self.registry.lock().unwrap();
        // pick up any tablet and set the shared block cache capacity
        if let Some(((_id, _suffix), tablet)) = (*reg).iter().next() {
            let opt = tablet.get_options_cf(CF_DEFAULT).unwrap(); // FIXME unwrap
            opt.set_block_cache_capacity(capacity)?;
        }
        Ok(())
    }
}

impl TabletAccessor<RocksEngine> for KvEngineFactoryV2 {
    #[inline]
    fn for_each_opened_tablet(&self, f: &mut dyn FnMut(u64, u64, &RocksEngine)) {
        let reg = self.registry.lock().unwrap();
        for ((id, suffix), tablet) in &*reg {
            f(*id, *suffix, tablet)
        }
    }

    // it have multi tablets.
    fn is_single_engine(&self) -> bool {
        false
    }
}

#[cfg(test)]
mod tests {
    use engine_traits::{TabletFactory, CF_WRITE};

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

    impl KvEngineFactoryV2 {
        pub fn new(inner: KvEngineFactory) -> Self {
            KvEngineFactoryV2 {
                inner,
                registry: Arc::new(Mutex::new(HashMap::default())),
            }
        }
    }

    #[test]
    fn test_kvengine_factory() {
        let cfg = TEST_CONFIG.clone();
        assert!(cfg.storage.block_cache.shared);
        let cache = cfg.storage.block_cache.build_shared_cache();
        let dir = test_util::temp_dir("test_kvengine_factory", false);
        let env = cfg.build_shared_rocks_env(None, None).unwrap();

        let mut builder = KvEngineFactoryBuilder::new(env, &cfg, dir.path());
        if let Some(cache) = cache {
            builder = builder.block_cache(cache);
        }
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
        let mut count = 0;
        factory.for_each_opened_tablet(&mut |id, suffix, _tablet| {
            assert!(id == 0);
            assert!(suffix == 0);
            count += 1;
        });
        assert_eq!(count, 1);
        assert!(factory.is_single_engine());
        assert!(shared_db.is_single_engine());
        factory
            .set_shared_block_cache_capacity(1024 * 1024)
            .unwrap();
        let opt = shared_db.get_options_cf(CF_DEFAULT).unwrap();
        assert_eq!(opt.get_block_cache_capacity(), 1024 * 1024);
    }

    #[test]
    fn test_kvengine_factory_v2() {
        let cfg = TEST_CONFIG.clone();
        assert!(cfg.storage.block_cache.shared);
        let cache = cfg.storage.block_cache.build_shared_cache();
        let dir = test_util::temp_dir("test_kvengine_factory_v2", false);
        let env = cfg.build_shared_rocks_env(None, None).unwrap();

        let mut builder = KvEngineFactoryBuilder::new(env, &cfg, dir.path());
        if let Some(cache) = cache {
            builder = builder.block_cache(cache);
        }
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
        factory
            .set_shared_block_cache_capacity(1024 * 1024)
            .unwrap();
        let opt = tablet.get_options_cf(CF_WRITE).unwrap();
        assert_eq!(opt.get_block_cache_capacity(), 1024 * 1024);

        assert!(factory.exists(1, 10));
        assert!(!factory.exists(1, 11));
        assert!(!factory.exists(2, 10));
        assert!(!factory.exists(2, 11));
        assert!(factory.exists_raw(&tablet_path));
        assert!(!factory.is_tombstoned(1, 10));
        assert!(factory.load_tablet(&tablet_path, 1, 10).is_err());
        assert!(factory.load_tablet(&tablet_path, 1, 20).is_ok());
        // After we load it as with the new id or suffix, we should be unable to get it with
        // the old id and suffix in the cache.
        assert!(factory.open_tablet_cache(1, 10).is_none());
        assert!(factory.open_tablet_cache(1, 20).is_some());

        factory.mark_tombstone(1, 20);
        assert!(factory.is_tombstoned(1, 20));
        factory.destroy_tablet(1, 20).unwrap();
        let result = factory.open_tablet(1, 20);
        assert!(result.is_err());
        assert!(!factory.is_single_engine());
    }

    #[test]
    fn test_get_live_tablets() {
        let cfg = TEST_CONFIG.clone();
        let dir = test_util::temp_dir("test_get_live_tablets", false);
        let env = cfg.build_shared_rocks_env(None, None).unwrap();

        let builder = KvEngineFactoryBuilder::new(env, &cfg, dir.path());
        let inner_factory = builder.build();
        let factory = KvEngineFactoryV2::new(inner_factory);
        factory.create_tablet(1, 10).unwrap();
        factory.create_tablet(2, 10).unwrap();
        let mut count = 0;
        factory.for_each_opened_tablet(&mut |id, suffix, _tablet| {
            assert!(id == 1 || id == 2);
            assert!(suffix == 10);
            count += 1;
        });
        assert_eq!(count, 2);
    }
}
