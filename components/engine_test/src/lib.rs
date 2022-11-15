// Copyright 2020 TiKV Project Authors. Licensed under Apache-2.0.

//! Engines for use in the test suite, implementing both the KvEngine
//! and RaftEngine traits.
//!
//! These engines link to all other engines, providing concrete single storage
//! engine type to run tests against.
//!
//! This provides a simple way to integrate non-RocksDB engines into the
//! existing test suite without too much disruption.
//!
//! Engines presently supported by this crate are
//!
//! - RocksEngine from engine_rocks
//! - PanicEngine from engine_panic
//! - RaftLogEngine from raft_log_engine
//!
//! TiKV uses two different storage engine instances,
//! the "raft" engine, for storing consensus data,
//! and the "kv" engine, for storing user data.
//!
//! The types and constructors for these two engines are located in the `raft`
//! and `kv` modules respectively.
//!
//! The engine for each module is chosen at compile time with feature flags:
//!
//! - `--features test-engine-kv-rocksdb`
//! - `--features test-engine-kv-panic`
//! - `--features test-engine-raft-rocksdb`
//! - `--features test-engine-raft-panic`
//! - `--features test-engine-raft-raft-engine`
//!
//! By default, the `tikv` crate turns on `test-engine-kv-rocksdb`,
//! and `test-engine-raft-raft-engine`. This behavior can be disabled
//! with `--disable-default-features`.
//!
//! The `tikv` crate additionally provides some feature flags that
//! contral both the `kv` and `raft` engines at the same time:
//!
//! - `--features test-engines-rocksdb`
//! - `--features test-engines-panic`
//!
//! So, e.g., to run the test suite with the panic engine:
//!
//! ```
//! cargo test --all --disable-default-features --features=protobuf_codec,test-engines-panic
//! ```
//!
//! We'll probably revisit the engine-testing strategy in the future,
//! e.g. by using engine-parameterized tests instead.
//!
//! This create also contains a `ctor` module that contains constructor methods
//! appropriate for constructing storage engines of any type. It is intended
//! that this module is _the only_ module within TiKV that knows about concrete
//! storage engines, and that it be extracted into its own crate for use in
//! TiKV, once the full requirements are better understood.

#![feature(let_chains)]

/// Types and constructors for the "raft" engine
pub mod raft {
    #[cfg(feature = "test-engine-raft-panic")]
    pub use engine_panic::PanicEngine as RaftTestEngine;
    #[cfg(feature = "test-engine-raft-rocksdb")]
    pub use engine_rocks::RocksEngine as RaftTestEngine;
    use engine_traits::Result;
    #[cfg(feature = "test-engine-raft-raft-engine")]
    pub use raft_log_engine::RaftLogEngine as RaftTestEngine;

    use crate::ctor::{RaftDbOptions, RaftEngineConstructorExt};

    pub fn new_engine(path: &str, db_opt: Option<RaftDbOptions>) -> Result<RaftTestEngine> {
        RaftTestEngine::new_raft_engine(path, db_opt)
    }
}

/// Types and constructors for the "kv" engine
pub mod kv {
    use std::{
        path::{Path, PathBuf},
        sync::{Arc, Mutex},
    };

    use collections::HashMap;
    #[cfg(feature = "test-engine-kv-panic")]
    pub use engine_panic::{
        PanicEngine as KvTestEngine, PanicEngineIterator as KvTestEngineIterator,
        PanicSnapshot as KvTestSnapshot, PanicWriteBatch as KvTestWriteBatch,
    };
    #[cfg(feature = "test-engine-kv-rocksdb")]
    pub use engine_rocks::{
        RocksEngine as KvTestEngine, RocksEngineIterator as KvTestEngineIterator,
        RocksSnapshot as KvTestSnapshot, RocksWriteBatchVec as KvTestWriteBatch,
    };
    use engine_traits::{
        CfOptions, CfOptionsExt, MiscExt, OpenOptions, Result, TabletAccessor, TabletFactory,
        CF_DEFAULT,
    };
    use tikv_util::box_err;

    use crate::ctor::{CfOptions as KvTestCfOptions, DbOptions, KvEngineConstructorExt};

    pub fn new_engine(path: &str, cfs: &[&str]) -> Result<KvTestEngine> {
        KvTestEngine::new_kv_engine(path, cfs)
    }

    pub fn new_engine_opt(
        path: &str,
        db_opt: DbOptions,
        cfs_opts: Vec<(&str, KvTestCfOptions)>,
    ) -> Result<KvTestEngine> {
        KvTestEngine::new_kv_engine_opt(path, db_opt, cfs_opts)
    }

    const TOMBSTONE_MARK: &str = "TOMBSTONE_TABLET";

    #[derive(Clone)]
    pub struct TestTabletFactory {
        root_path: PathBuf,
        db_opt: DbOptions,
        cf_opts: Vec<(&'static str, KvTestCfOptions)>,
        root_db: Arc<Mutex<Option<KvTestEngine>>>,
    }

    impl TestTabletFactory {
        pub fn new(
            root_path: &Path,
            db_opt: DbOptions,
            cf_opts: Vec<(&'static str, KvTestCfOptions)>,
        ) -> Self {
            Self {
                root_path: root_path.to_path_buf(),
                db_opt,
                cf_opts,
                root_db: Arc::new(Mutex::default()),
            }
        }

        fn create_tablet(&self, tablet_path: &Path) -> Result<KvTestEngine> {
            KvTestEngine::new_kv_engine_opt(
                tablet_path.to_str().unwrap(),
                self.db_opt.clone(),
                self.cf_opts.clone(),
            )
        }
    }

    impl TabletFactory<KvTestEngine> for TestTabletFactory {
        fn create_shared_db(&self) -> Result<KvTestEngine> {
            let tablet_path = self.tablet_path(0, 0);
            let tablet = self.create_tablet(&tablet_path)?;
            let mut root_db = self.root_db.lock().unwrap();
            root_db.replace(tablet.clone());
            Ok(tablet)
        }

        /// See the comment above the same name method in KvEngineFactory
        fn open_tablet(
            &self,
            _id: u64,
            _suffix: Option<u64>,
            options: OpenOptions,
        ) -> Result<KvTestEngine> {
            if let Some(db) = self.root_db.lock().unwrap().as_ref() {
                if options.create_new() {
                    return Err(box_err!("root tablet {} already exists", db.path()));
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
        ) -> Result<KvTestEngine> {
            self.create_shared_db()
        }

        fn exists_raw(&self, _path: &Path) -> bool {
            false
        }

        #[inline]
        fn tablet_path_with_prefix(&self, _prefix: &str, _id: u64, _suffix: u64) -> PathBuf {
            self.root_path.join("db")
        }

        #[inline]
        fn tablets_path(&self) -> PathBuf {
            Path::new(&self.root_path).join("tablets")
        }

        #[inline]
        fn destroy_tablet(&self, _id: u64, _suffix: u64) -> engine_traits::Result<()> {
            Ok(())
        }

        fn set_shared_block_cache_capacity(&self, capacity: u64) -> Result<()> {
            let db = self.root_db.lock().unwrap();
            let opt = db.as_ref().unwrap().get_options_cf(CF_DEFAULT).unwrap(); // FIXME unwrap
            opt.set_block_cache_capacity(capacity)?;
            Ok(())
        }
    }

    impl TabletAccessor<KvTestEngine> for TestTabletFactory {
        fn for_each_opened_tablet(&self, f: &mut dyn FnMut(u64, u64, &KvTestEngine)) {
            let db = self.root_db.lock().unwrap();
            let db = db.as_ref().unwrap();
            f(0, 0, db);
        }

        fn is_single_engine(&self) -> bool {
            true
        }
    }

    #[derive(Clone)]
    pub struct TestTabletFactoryV2 {
        inner: TestTabletFactory,
        // region_id -> (tablet, tablet_suffix)
        registry: Arc<Mutex<HashMap<u64, (KvTestEngine, u64)>>>,
    }

    impl TestTabletFactoryV2 {
        pub fn new(
            root_path: &Path,
            db_opt: DbOptions,
            cf_opts: Vec<(&'static str, KvTestCfOptions)>,
        ) -> Self {
            Self {
                inner: TestTabletFactory::new(root_path, db_opt, cf_opts),
                registry: Arc::default(),
            }
        }
    }

    impl TabletFactory<KvTestEngine> for TestTabletFactoryV2 {
        /// See the comment above the same name method in KvEngineFactoryV2
        fn open_tablet(
            &self,
            id: u64,
            suffix: Option<u64>,
            mut options: OpenOptions,
        ) -> Result<KvTestEngine> {
            if options.create_new() && suffix.is_none() {
                return Err(box_err!(
                    "suffix should be provided when creating new tablet"
                ));
            }

            if options.create_new() || options.create() {
                options = options.set_cache_only(false);
            }

            let mut reg = self.registry.lock().unwrap();
            if let Some(suffix) = suffix {
                if let Some((cached_tablet, cached_suffix)) = reg.get(&id) && *cached_suffix == suffix {
                    // Target tablet exist in the cache
                    if options.create_new() {
                        return Err(box_err!("region {} {} already exists", id, cached_tablet.path()));
                    }
                    return Ok(cached_tablet.clone());
                } else if !options.cache_only() {
                    let tablet_path = self.tablet_path(id, suffix);
                    let tablet = self.open_tablet_raw(&tablet_path, id, suffix, options.clone())?;
                    if !options.skip_cache() {
                        reg.insert(id, (tablet.clone(), suffix));
                    }
                    return Ok(tablet);
                }
            } else if let Some((tablet, _)) = reg.get(&id) {
                return Ok(tablet.clone());
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
            _suffix: u64,
            options: OpenOptions,
        ) -> Result<KvTestEngine> {
            let engine_exist = KvTestEngine::exists(path.to_str().unwrap_or_default());
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

            self.inner.create_tablet(path)
        }

        #[inline]
        fn create_shared_db(&self) -> Result<KvTestEngine> {
            self.open_tablet(0, Some(0), OpenOptions::default().set_create_new(true))
        }

        #[inline]
        fn exists_raw(&self, path: &Path) -> bool {
            KvTestEngine::exists(path.to_str().unwrap_or_default())
        }

        #[inline]
        fn tablets_path(&self) -> PathBuf {
            self.inner.root_path.join("tablets")
        }

        #[inline]
        fn tablet_path_with_prefix(&self, prefix: &str, id: u64, suffix: u64) -> PathBuf {
            self.inner
                .root_path
                .join(format!("tablets/{}{}_{}", prefix, id, suffix))
        }

        #[inline]
        fn mark_tombstone(&self, region_id: u64, suffix: u64) {
            let path = self.tablet_path(region_id, suffix).join(TOMBSTONE_MARK);
            // When the full directory path does not exsit, create will return error and in
            // this case, we just ignore it.
            let _ = std::fs::File::create(path);
            {
                let mut reg = self.registry.lock().unwrap();
                if let Some((cached_tablet, cached_suffix)) = reg.remove(&region_id) && cached_suffix != suffix {
                    reg.insert(region_id, (cached_tablet, cached_suffix));
                }
            }
        }

        #[inline]
        fn is_tombstoned(&self, region_id: u64, suffix: u64) -> bool {
            self.tablet_path(region_id, suffix)
                .join(TOMBSTONE_MARK)
                .exists()
        }

        #[inline]
        fn destroy_tablet(&self, region_id: u64, suffix: u64) -> engine_traits::Result<()> {
            let path = self.tablet_path(region_id, suffix);
            {
                let mut reg = self.registry.lock().unwrap();
                if let Some((cached_tablet, cached_suffix)) = reg.remove(&region_id) && cached_suffix != suffix {
                    reg.insert(region_id, (cached_tablet, cached_suffix));
                }
            }
            let _ = std::fs::remove_dir_all(path);
            Ok(())
        }

        #[inline]
        fn load_tablet(&self, path: &Path, region_id: u64, suffix: u64) -> Result<KvTestEngine> {
            {
                let reg = self.registry.lock().unwrap();
                if let Some((db, db_suffix)) = reg.get(&region_id) && *db_suffix == suffix {
                    return Err(box_err!("region {} {} already exists", region_id, db.path()));
                }
            }

            let db_path = self.tablet_path(region_id, suffix);
            std::fs::rename(path, db_path)?;
            self.open_tablet(
                region_id,
                Some(suffix),
                OpenOptions::default().set_create(true),
            )
        }

        fn set_shared_block_cache_capacity(&self, capacity: u64) -> Result<()> {
            let reg = self.registry.lock().unwrap();
            // pick up any tablet and set the shared block cache capacity
            if let Some((_id, (tablet, _suffix))) = (*reg).iter().next() {
                let opt = tablet.get_options_cf(CF_DEFAULT).unwrap(); // FIXME unwrap
                opt.set_block_cache_capacity(capacity)?;
            }
            Ok(())
        }
    }

    impl TabletAccessor<KvTestEngine> for TestTabletFactoryV2 {
        #[inline]
        fn for_each_opened_tablet(&self, f: &mut dyn FnMut(u64, u64, &KvTestEngine)) {
            let reg = self.registry.lock().unwrap();
            for (id, (tablet, suffix)) in &*reg {
                f(*id, *suffix, tablet)
            }
        }

        // it have multi tablets.
        fn is_single_engine(&self) -> bool {
            false
        }
    }
}

/// Create a storage engine with a concrete type. This should ultimately be the
/// only module within TiKV that needs to know about concrete engines. Other
/// code only uses the `engine_traits` abstractions.
///
/// At the moment this has a lot of open-coding of engine-specific
/// initialization, but in the future more constructor abstractions should be
/// pushed down into engine_traits.
///
/// This module itself is intended to be extracted from this crate into its own
/// crate, once the requirements for engine construction are better understood.
pub mod ctor {
    use std::sync::Arc;

    use encryption::DataKeyManager;
    use engine_traits::Result;
    use file_system::IoRateLimiter;

    /// Kv engine construction
    ///
    /// For simplicity, all engine constructors are expected to configure every
    /// engine such that all of TiKV and its tests work correctly, for the
    /// constructed column families.
    ///
    /// Specifically, this means that RocksDB constructors should set up
    /// all properties collectors, always.
    pub trait KvEngineConstructorExt: Sized {
        /// Create a new kv engine with either:
        ///
        /// - The column families specified as `cfs`, with default options, or
        /// - The column families specified as `opts`, with options.
        ///
        /// Note that if `opts` is not `None` then the `cfs` argument is
        /// completely ignored.
        ///
        /// The engine stores its data in the `path` directory.
        /// If that directory does not exist, then it is created.
        fn new_kv_engine(path: &str, cfs: &[&str]) -> Result<Self>;

        /// Create a new engine with specified column families and options
        ///
        /// The engine stores its data in the `path` directory.
        /// If that directory does not exist, then it is created.
        fn new_kv_engine_opt(
            path: &str,
            db_opt: DbOptions,
            cf_opts: Vec<(&str, CfOptions)>,
        ) -> Result<Self>;
    }

    /// Raft engine construction
    pub trait RaftEngineConstructorExt: Sized {
        /// Create a new raft engine.
        fn new_raft_engine(path: &str, db_opt: Option<RaftDbOptions>) -> Result<Self>;
    }

    #[derive(Clone, Default)]
    pub struct DbOptions {
        key_manager: Option<Arc<DataKeyManager>>,
        rate_limiter: Option<Arc<IoRateLimiter>>,
        enable_multi_batch_write: bool,
    }

    impl DbOptions {
        pub fn set_key_manager(&mut self, key_manager: Option<Arc<DataKeyManager>>) {
            self.key_manager = key_manager;
        }

        pub fn set_rate_limiter(&mut self, rate_limiter: Option<Arc<IoRateLimiter>>) {
            self.rate_limiter = rate_limiter;
        }

        pub fn set_enable_multi_batch_write(&mut self, enable: bool) {
            self.enable_multi_batch_write = enable;
        }
    }

    pub type RaftDbOptions = DbOptions;

    /// Properties for a single column family
    ///
    /// All engines must emulate column families, but at present it is not clear
    /// how non-RocksDB engines should deal with the wide variety of options for
    /// column families.
    ///
    /// At present this very closely mirrors the column family options
    /// for RocksDB, with the exception that it provides no capacity for
    /// installing table property collectors, which have little hope of being
    /// emulated on arbitrary engines.
    ///
    /// Instead, the RocksDB constructors need to always install the table
    /// property collectors that TiKV needs, and other engines need to
    /// accomplish the same high-level ends those table properties are used for
    /// by their own means.
    ///
    /// At present, they should probably emulate, reinterpret, or ignore them as
    /// suitable to get tikv functioning.
    ///
    /// In the future TiKV will probably have engine-specific configuration
    /// options.
    #[derive(Clone)]
    pub struct CfOptions {
        disable_auto_compactions: bool,
        level_zero_file_num_compaction_trigger: Option<i32>,
        level_zero_slowdown_writes_trigger: Option<i32>,
        /// On RocksDB, turns off the range properties collector. Only used in
        /// tests. Unclear how other engines should deal with this.
        no_range_properties: bool,
        /// On RocksDB, turns off the table properties collector. Only used in
        /// tests. Unclear how other engines should deal with this.
        no_table_properties: bool,
    }

    impl CfOptions {
        pub fn new() -> CfOptions {
            CfOptions {
                disable_auto_compactions: false,
                level_zero_file_num_compaction_trigger: None,
                level_zero_slowdown_writes_trigger: None,
                no_range_properties: false,
                no_table_properties: false,
            }
        }

        pub fn set_disable_auto_compactions(&mut self, v: bool) {
            self.disable_auto_compactions = v;
        }

        pub fn get_disable_auto_compactions(&self) -> bool {
            self.disable_auto_compactions
        }

        pub fn set_level_zero_file_num_compaction_trigger(&mut self, n: i32) {
            self.level_zero_file_num_compaction_trigger = Some(n);
        }

        pub fn get_level_zero_file_num_compaction_trigger(&self) -> Option<i32> {
            self.level_zero_file_num_compaction_trigger
        }

        pub fn set_level_zero_slowdown_writes_trigger(&mut self, n: i32) {
            self.level_zero_slowdown_writes_trigger = Some(n);
        }

        pub fn get_level_zero_slowdown_writes_trigger(&self) -> Option<i32> {
            self.level_zero_slowdown_writes_trigger
        }

        pub fn set_no_range_properties(&mut self, v: bool) {
            self.no_range_properties = v;
        }

        pub fn get_no_range_properties(&self) -> bool {
            self.no_range_properties
        }

        pub fn set_no_table_properties(&mut self, v: bool) {
            self.no_table_properties = v;
        }

        pub fn get_no_table_properties(&self) -> bool {
            self.no_table_properties
        }
    }

    impl Default for CfOptions {
        fn default() -> Self {
            Self::new()
        }
    }

    mod panic {
        use engine_panic::PanicEngine;
        use engine_traits::Result;

        use super::{CfOptions, DbOptions, KvEngineConstructorExt, RaftEngineConstructorExt};

        impl KvEngineConstructorExt for engine_panic::PanicEngine {
            fn new_kv_engine(_path: &str, _cfs: &[&str]) -> Result<Self> {
                Ok(PanicEngine)
            }

            fn new_kv_engine_opt(
                _path: &str,
                _db_opt: DbOptions,
                _cfs_opts: Vec<(&str, CfOptions)>,
            ) -> Result<Self> {
                Ok(PanicEngine)
            }
        }

        impl RaftEngineConstructorExt for engine_panic::PanicEngine {
            fn new_raft_engine(_path: &str, _db_opt: Option<DbOptions>) -> Result<Self> {
                Ok(PanicEngine)
            }
        }
    }

    mod rocks {
        use engine_rocks::{
            get_env,
            properties::{MvccPropertiesCollectorFactory, RangePropertiesCollectorFactory},
            util::new_engine_opt as rocks_new_engine_opt,
            RocksCfOptions, RocksDbOptions,
        };
        use engine_traits::{CfOptions as _, Result, CF_DEFAULT};

        use super::{
            CfOptions, DbOptions, KvEngineConstructorExt, RaftDbOptions, RaftEngineConstructorExt,
        };

        impl KvEngineConstructorExt for engine_rocks::RocksEngine {
            // FIXME this is duplicating behavior from engine_rocks::util in order to
            // call set_standard_cf_opts.
            fn new_kv_engine(path: &str, cfs: &[&str]) -> Result<Self> {
                let rocks_db_opt = RocksDbOptions::default();
                let default_cf_opt = CfOptions::new();
                let rocks_cfs_opts = cfs
                    .iter()
                    .map(|cf_name| (*cf_name, get_rocks_cf_opts(&default_cf_opt)))
                    .collect();
                rocks_new_engine_opt(path, rocks_db_opt, rocks_cfs_opts)
            }

            fn new_kv_engine_opt(
                path: &str,
                db_opt: DbOptions,
                cfs_opts: Vec<(&str, CfOptions)>,
            ) -> Result<Self> {
                let rocks_db_opts = get_rocks_db_opts(db_opt)?;
                let rocks_cfs_opts = cfs_opts
                    .iter()
                    .map(|(name, opt)| (*name, get_rocks_cf_opts(opt)))
                    .collect();
                rocks_new_engine_opt(path, rocks_db_opts, rocks_cfs_opts)
            }
        }

        impl RaftEngineConstructorExt for engine_rocks::RocksEngine {
            fn new_raft_engine(path: &str, db_opt: Option<RaftDbOptions>) -> Result<Self> {
                let rocks_db_opts = match db_opt {
                    Some(db_opt) => get_rocks_db_opts(db_opt)?,
                    None => RocksDbOptions::default(),
                };
                let rocks_cf_opts = get_rocks_cf_opts(&CfOptions::new());
                let default_cfs_opts = vec![(CF_DEFAULT, rocks_cf_opts)];
                rocks_new_engine_opt(path, rocks_db_opts, default_cfs_opts)
            }
        }

        fn get_rocks_cf_opts(cf_opts: &CfOptions) -> RocksCfOptions {
            let mut rocks_cf_opts = RocksCfOptions::new();
            if !cf_opts.get_no_range_properties() {
                rocks_cf_opts.add_table_properties_collector_factory(
                    "tikv.range-properties-collector",
                    RangePropertiesCollectorFactory::default(),
                );
            }
            if !cf_opts.get_no_table_properties() {
                rocks_cf_opts.add_table_properties_collector_factory(
                    "tikv.mvcc-properties-collector",
                    MvccPropertiesCollectorFactory::default(),
                );
            }

            if let Some(trigger) = cf_opts.get_level_zero_file_num_compaction_trigger() {
                rocks_cf_opts.set_level_zero_file_num_compaction_trigger(trigger);
            }
            if let Some(trigger) = cf_opts.get_level_zero_slowdown_writes_trigger() {
                rocks_cf_opts.set_level_zero_slowdown_writes_trigger(trigger);
            }
            if cf_opts.get_disable_auto_compactions() {
                rocks_cf_opts.set_disable_auto_compactions(true);
            }
            rocks_cf_opts
        }

        fn get_rocks_db_opts(db_opts: DbOptions) -> Result<RocksDbOptions> {
            let mut rocks_db_opts = RocksDbOptions::default();
            let env = get_env(db_opts.key_manager.clone(), db_opts.rate_limiter)?;
            rocks_db_opts.set_env(env);
            if db_opts.enable_multi_batch_write {
                rocks_db_opts.enable_unordered_write(false);
                rocks_db_opts.enable_pipelined_write(false);
                rocks_db_opts.enable_multi_batch_write(true);
            }
            Ok(rocks_db_opts)
        }
    }

    mod raft_engine {
        use engine_traits::Result;
        use raft_log_engine::{RaftEngineConfig, RaftLogEngine};

        use super::{RaftDbOptions, RaftEngineConstructorExt};

        impl RaftEngineConstructorExt for raft_log_engine::RaftLogEngine {
            fn new_raft_engine(path: &str, db_opts: Option<RaftDbOptions>) -> Result<Self> {
                let mut config = RaftEngineConfig::default();
                config.dir = path.to_owned();
                RaftLogEngine::new(
                    config,
                    db_opts.as_ref().and_then(|opts| opts.key_manager.clone()),
                    db_opts.and_then(|opts| opts.rate_limiter),
                )
            }
        }
    }
}

/// Create a new set of engines in a temporary directory
///
/// This is little-used and probably shouldn't exist.
pub fn new_temp_engine(
    path: &tempfile::TempDir,
) -> engine_traits::Engines<crate::kv::KvTestEngine, crate::raft::RaftTestEngine> {
    let raft_path = path.path().join(std::path::Path::new("raft"));
    engine_traits::Engines::new(
        crate::kv::new_engine(path.path().to_str().unwrap(), engine_traits::ALL_CFS).unwrap(),
        crate::raft::new_engine(raft_path.to_str().unwrap(), None).unwrap(),
    )
}
