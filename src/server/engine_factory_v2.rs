// Copyright 2022 TiKV Project Authors. Licensed under Apache-2.0.

use std::{
    path::{Path, PathBuf},
    sync::{Arc, Mutex},
};

use collections::HashMap;
use engine_rocks::RocksEngine;
use engine_traits::{
    CfOptions, CfOptionsExt, MiscExt, OpenOptions, Result, TabletAccessor, TabletFactory,
    CF_DEFAULT,
};

use crate::server::engine_factory::KvEngineFactory;

const TOMBSTONE_MARK: &str = "TOMBSTONE_TABLET";

#[derive(Clone)]
pub struct KvEngineFactoryV2 {
    inner: KvEngineFactory,
    registry: Arc<Mutex<HashMap<(u64, u64), RocksEngine>>>,
}

impl KvEngineFactoryV2 {
    pub fn new(inner: KvEngineFactory) -> Self {
        KvEngineFactoryV2 {
            inner,
            registry: Arc::new(Mutex::new(HashMap::default())),
        }
    }
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
    /// open a tablet according to the OpenOptions.
    ///
    /// If options.cache_only is true, only open the relevant tablet from
    /// `registry`, and if suffix is None, return an arbitrary tablet with the
    /// target region id if there are any.
    ///
    /// If options.create_new is true, create a tablet by id and suffix. If the
    /// tablet exists, it will fail.
    ///
    /// If options.create is true, open the tablet with id and suffix if it
    /// exists or create it otherwise.
    ///
    /// Note: options.cache_only and options.create and/or options.create_new
    /// cannot be true simultaneously
    fn open_tablet(
        &self,
        id: u64,
        suffix: Option<u64>,
        mut options: OpenOptions,
    ) -> Result<RocksEngine> {
        if options.create() || options.create_new() {
            options = options.set_cache_only(false);
        }

        let mut reg = self.registry.lock().unwrap();
        if let Some(suffix) = suffix {
            if let Some(tablet) = reg.get(&(id, suffix)) {
                // Target tablet exist in the cache

                if options.create_new() {
                    return Err(box_err!(
                        "region {} {} already exists",
                        id,
                        tablet.as_inner().path()
                    ));
                }
                return Ok(tablet.clone());
            } else if !options.cache_only() {
                let tablet_path = self.tablet_path(id, suffix);
                let tablet = self.open_tablet_raw(&tablet_path, id, suffix, options.clone())?;
                if !options.skip_cache() {
                    debug!("Insert a tablet"; "key" => ?(id, suffix));
                    reg.insert((id, suffix), tablet.clone());
                }
                return Ok(tablet);
            }
        } else if options.cache_only() {
            // This branch reads an arbitrary tablet with region id `id`

            if let Some(k) = reg.keys().find(|k| k.0 == id) {
                debug!("choose a random tablet"; "key" => ?k);
                return Ok(reg.get(k).unwrap().clone());
            }
        }

        Err(box_err!(
            "tablet with region id {} suffix {:?} does not exist",
            id,
            suffix
        ))
    }

    fn open_tablet_raw(
        &self,
        path: &Path,
        id: u64,
        suffix: u64,
        options: OpenOptions,
    ) -> Result<RocksEngine> {
        let engine_exist = RocksEngine::exists(path.to_str().unwrap_or_default());
        // Even though neither options.create nor options.create_new are true, if the
        // tablet files already exists, we will open it by calling
        // inner.create_tablet. In this case, the tablet exists but not in the cache
        // (registry).
        if !options.create() && !options.create_new() && !engine_exist {
            return Err(box_err!(
                "path {} does not have db",
                path.to_str().unwrap_or_default()
            ));
        };

        if options.create_new() && engine_exist {
            return Err(box_err!(
                "region {} {} already exists",
                id,
                path.to_str().unwrap()
            ));
        }

        let tablet = self.inner.create_tablet(path, id, suffix)?;
        debug!("open tablet"; "key" => ?(id, suffix));
        self.inner.on_tablet_created(id, suffix);
        Ok(tablet)
    }

    #[inline]
    fn create_shared_db(&self) -> Result<RocksEngine> {
        self.open_tablet(0, Some(0), OpenOptions::default().set_create_new(true))
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
        let new_engine =
            self.open_tablet(id, Some(suffix), OpenOptions::default().set_create(true));
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
    use engine_traits::{OpenOptions, TabletFactory, CF_WRITE};

    use super::*;
    use crate::{config::TikvConfig, server::KvEngineFactoryBuilder};

    lazy_static! {
        static ref TEST_CONFIG: TikvConfig = {
            let manifest_dir = Path::new(env!("CARGO_MANIFEST_DIR"));
            let common_test_cfg =
                manifest_dir.join("components/test_raftstore/src/common-test.toml");
            TikvConfig::from_file(&common_test_cfg, None).unwrap_or_else(|e| {
                panic!(
                    "invalid auto generated configuration file {}, err {}",
                    manifest_dir.display(),
                    e
                );
            })
        };
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

        // V1 can only create tablet once
        factory
            .open_tablet(1, Some(10), OpenOptions::default().set_create_new(true))
            .unwrap_err();

        let tablet = factory
            .open_tablet(1, Some(10), OpenOptions::default().set_create(true))
            .unwrap();
        assert_eq!(tablet.as_inner().path(), shared_db.as_inner().path());
        let tablet = factory
            .open_tablet(1, Some(10), OpenOptions::default().set_cache_only(true))
            .unwrap();
        assert_eq!(tablet.as_inner().path(), shared_db.as_inner().path());
        let tablet = factory
            .open_tablet(1, None, OpenOptions::default().set_cache_only(true))
            .unwrap();
        assert_eq!(tablet.as_inner().path(), shared_db.as_inner().path());
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
    fn test_kvengine_factory_root_db_implicit_creation() {
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

        // root_db should be created implicitly here
        let tablet = factory
            .open_tablet(1, Some(10), OpenOptions::default().set_create(true))
            .unwrap();

        // error is expected since root_db is created already
        factory
            .open_tablet(1, Some(10), OpenOptions::default().set_create_new(true))
            .unwrap_err();

        let mut count = 0;
        factory.for_each_opened_tablet(&mut |id, suffix, _tablet| {
            assert!(id == 0);
            assert!(suffix == 0);
            count += 1;
        });
        assert_eq!(count, 1);
        assert!(factory.is_single_engine());
        factory
            .set_shared_block_cache_capacity(1024 * 1024)
            .unwrap();
        let opt = tablet.get_options_cf(CF_DEFAULT).unwrap();
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

        let factory = builder.build_v2();
        let tablet = factory
            .open_tablet(1, Some(10), OpenOptions::default().set_create_new(true))
            .unwrap();
        let tablet2 = factory
            .open_tablet(1, Some(10), OpenOptions::default().set_create(true))
            .unwrap();
        assert_eq!(tablet.as_inner().path(), tablet2.as_inner().path());
        let tablet2 = factory
            .open_tablet(1, Some(10), OpenOptions::default().set_cache_only(true))
            .unwrap();
        assert_eq!(tablet.as_inner().path(), tablet2.as_inner().path());
        let tablet2 = factory
            .open_tablet(1, None, OpenOptions::default().set_cache_only(true))
            .unwrap();
        assert_eq!(tablet.as_inner().path(), tablet2.as_inner().path());

        let tablet_path = factory.tablet_path(1, 10);
        let result = factory.open_tablet(1, Some(10), OpenOptions::default().set_create_new(true));
        result.unwrap_err();

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
        factory.load_tablet(&tablet_path, 1, 10).unwrap_err();
        factory.load_tablet(&tablet_path, 1, 20).unwrap();
        // After we load it as with the new id or suffix, we should be unable to get it
        // with the old id and suffix in the cache.
        factory
            .open_tablet(1, Some(10), OpenOptions::default().set_cache_only(true))
            .unwrap_err();
        factory
            .open_tablet(1, Some(20), OpenOptions::default().set_cache_only(true))
            .unwrap();

        factory.mark_tombstone(1, 20);
        assert!(factory.is_tombstoned(1, 20));
        factory.destroy_tablet(1, 20).unwrap();

        let result = factory.open_tablet(1, Some(20), OpenOptions::default());
        result.unwrap_err();

        assert!(!factory.is_single_engine());
    }

    #[test]
    fn test_existed_db_not_in_registry() {
        let cfg = TEST_CONFIG.clone();
        assert!(cfg.storage.block_cache.shared);
        let cache = cfg.storage.block_cache.build_shared_cache();
        let dir = test_util::temp_dir("test_kvengine_factory_v2", false);
        let env = cfg.build_shared_rocks_env(None, None).unwrap();

        let mut builder = KvEngineFactoryBuilder::new(env, &cfg, dir.path());
        if let Some(cache) = cache {
            builder = builder.block_cache(cache);
        }

        let factory = builder.build_v2();
        let tablet = factory
            .open_tablet(1, Some(10), OpenOptions::default().set_create_new(true))
            .unwrap();
        drop(tablet);
        let tablet = factory.registry.lock().unwrap().remove(&(1, 10)).unwrap();
        drop(tablet);
        factory
            .open_tablet(1, Some(10), OpenOptions::default().set_cache_only(true))
            .unwrap_err();

        let tablet_path = factory.tablet_path(1, 10);
        let tablet = factory
            .open_tablet_raw(&tablet_path, 1, 10, OpenOptions::default())
            .unwrap();
        // the tablet will not inserted in the cache
        factory
            .open_tablet(1, Some(10), OpenOptions::default().set_cache_only(true))
            .unwrap_err();
        drop(tablet);

        let tablet_path = factory.tablet_path(1, 20);
        // No such tablet, so error will be returned.
        factory
            .open_tablet_raw(&tablet_path, 1, 10, OpenOptions::default())
            .unwrap_err();

        let _ = factory
            .open_tablet(1, Some(10), OpenOptions::default().set_create(true))
            .unwrap();

        // Now, it should be in the cache.
        factory
            .open_tablet(1, Some(10), OpenOptions::default().set_cache_only(true))
            .unwrap();
    }

    #[test]
    fn test_get_live_tablets() {
        let cfg = TEST_CONFIG.clone();
        let dir = test_util::temp_dir("test_get_live_tablets", false);
        let env = cfg.build_shared_rocks_env(None, None).unwrap();

        let builder = KvEngineFactoryBuilder::new(env, &cfg, dir.path());
        let factory = builder.build_v2();
        factory
            .open_tablet(1, Some(10), OpenOptions::default().set_create_new(true))
            .unwrap();
        factory
            .open_tablet(2, Some(10), OpenOptions::default().set_create_new(true))
            .unwrap();
        let mut count = 0;
        factory.for_each_opened_tablet(&mut |id, suffix, _tablet| {
            assert!(id == 1 || id == 2);
            assert!(suffix == 10);
            count += 1;
        });
        assert_eq!(count, 2);
    }
}
