// Copyright 2020 TiKV Project Authors. Licensed under Apache-2.0.

//! An engine for use in the test suite
//!
//! This storage engine links to all other engines, providing a single storage
//! engine to run tests against.
//!
//! This provides a simple way to integrate non-RocksDB engines into the
//! existing test suite without too much disruption.
//!
//! The engine is chosen at compile time with feature flags (TODO document me).
//!
//! We'll probably revisit the engine-testing strategy in the future,
//! e.g. by using engine-parameterized tests instead.
//!
//! TiKV uses two different storage engine instances,
//! the "raft" engine, for storing consensus data,
//! and the "kv" engine, for storing user data.
//!
//! The types and constructors for these two engines are located in the `raft`
//! and `kv` modules respectively.
//!
//! This create also contains a `ctor` module that contains constructor methods
//! appropriate for constructing storage engines of any type. It is intended
//! that this module is _the only_ module within TiKV that knows about concrete
//! storage engines, and that it be extracted into its own crate for use in
//! TiKV.

/// Types and constructors for the "raft" engine
pub mod raft {
    use engine_traits::Result;
    use crate::ctor::{EngineConstructorExt, EngineOpts};

    #[cfg(feature = "test-engine-raft-panic")]
    pub use engine_panic::{
        PanicEngine as RaftTestEngine,
    };

    #[cfg(feature = "test-engine-raft-rocksdb")]
    pub use engine_rocks::{
        RocksEngine as RaftTestEngine,
    };

    pub fn new_all_cfs_engine(path: &str, opts: Option<EngineOpts>) -> Result<RaftTestEngine> {
        RaftTestEngine::new_all_cfs_engine(path, opts)
    }
}

/// Types and constructors for the "kv" engine
pub mod kv {
    use engine_traits::Result;
    use crate::ctor::{EngineConstructorExt, EngineOpts};

    #[cfg(feature = "test-engine-kv-panic")]
    pub use engine_panic::{
        PanicEngine as KvTestEngine,
    };

    #[cfg(feature = "test-engine-kv-rocksdb")]
    pub use engine_rocks::{
        RocksEngine as KvTestEngine,
    };

    pub fn new_all_cfs_engine(path: &str, opts: Option<EngineOpts>) -> Result<KvTestEngine> {
        KvTestEngine::new_all_cfs_engine(path, opts)
    }
}

/// Create a storage engine with a concrete type. This should ultimately be the
/// only module within TiKv that needs to know about concrete engines. Other
/// code only uses the `engine_traits` abstractions.
///
/// At the moment this has a lot of open-coding of engine-specific
/// initialization, but in the future more constructor abstractions should be
/// pushed down into engine_traits.
///
/// This module itself is intended to be extracted from this crate into its own
/// crate, once the requirements for engine construction are better understood.
pub mod ctor {
    use engine_traits::Result;

    /// Engine construction
    ///
    /// For simplicity, all engine constructors are expected to configure every
    /// engine such that all of TiKV and its tests work correctly, for the
    /// constructed column families.
    ///
    /// Specifically, this means that RocksDB constructors should set up
    /// all properties collectors, always.
    pub trait EngineConstructorExt: Sized {
        /// Create an `EngineOpts` suitable for the engine
        fn default_opts() -> EngineOpts;

        /// Create a new engine with all column families, as defined
        /// by `ALL_CFS`.
        fn new_all_cfs_engine(path: &str, opts: Option<EngineOpts>) -> Result<Self>;

        /// Create a new engine with all "large" column families, as defined
        /// by `LARGE_CFS`.
        fn new_large_cfs_engine(path: &str, opts: Option<EngineOpts>) -> Result<Self>;

        /// Create a new engine with specified column families
        fn new_engine_cfs(path: &str, cfs: &[&str], opts: Option<EngineOpts>) -> Result<Self>;
    }

    /// A limited set of engine construction options that can be supported
    /// in some form across multiple engines.
    pub struct EngineOpts {
        /// Applies to all column families
        pub level_zero_file_num_compaction_trigger: Option<i32>,
    }

    mod panic {
        use engine_traits::Result;
        use super::{EngineConstructorExt, EngineOpts};

        impl EngineConstructorExt for engine_panic::PanicEngine {
            fn default_opts() -> EngineOpts {
                EngineOpts {
                    level_zero_file_num_compaction_trigger: None,
                }
            }

            fn new_all_cfs_engine(_path: &str, _opts: Option<EngineOpts>) -> Result<Self> {
                Ok(engine_panic::PanicEngine)
            }

            fn new_large_cfs_engine(_path: &str, _opts: Option<EngineOpts>) -> Result<Self> {
                Ok(engine_panic::PanicEngine)
            }

            fn new_engine_cfs(_path: &str, _cfs: &[&str], _opts: Option<EngineOpts>) -> Result<Self> {
                Ok(engine_panic::PanicEngine)
            }
        }
    }

    mod rocks {
        use super::{EngineConstructorExt, EngineOpts};

        use engine_traits::Result;
        use engine_traits::{ALL_CFS, LARGE_CFS};

        use engine_rocks::RocksEngine;
        // FIXME: Don't use "raw" module here
        use engine_rocks::raw::{ColumnFamilyOptions, DBOptions};
        use engine_rocks::raw_util::{new_engine_opt, CFOptions};
        use engine_rocks::properties::{
            MvccPropertiesCollectorFactory, RangePropertiesCollectorFactory,
        };
        use std::sync::Arc;

        impl EngineConstructorExt for engine_rocks::RocksEngine {
            fn default_opts() -> EngineOpts {
                EngineOpts {
                    level_zero_file_num_compaction_trigger: None,
                }
            }

            fn new_all_cfs_engine(path: &str, opts: Option<EngineOpts>) -> Result<Self> {
                Self::new_engine_cfs(path, ALL_CFS, opts)
            }

            fn new_large_cfs_engine(path: &str, opts: Option<EngineOpts>) -> Result<Self> {
                Self::new_engine_cfs(path, LARGE_CFS, opts)
            }

            fn new_engine_cfs(path: &str, cfs: &[&str], opts: Option<EngineOpts>) -> Result<Self> {
                let db_opts = DBOptions::new();
                let cfs_opts = cfs
                    .iter()
                    .map(|cf| {
                        let mut cf_opts = ColumnFamilyOptions::new();
                        set_standard_cf_opts(&mut cf_opts);
                        set_cf_opts(&mut cf_opts, &opts);
                        CFOptions::new(cf, cf_opts)
                    })
                    .collect();
                let engine = Arc::new(new_engine_opt(path, db_opts, cfs_opts).unwrap());

                Ok(RocksEngine::from_db(engine))
            }
        }

        fn set_standard_cf_opts(cf_opts: &mut ColumnFamilyOptions) {
            let f = Box::new(RangePropertiesCollectorFactory::default());
            cf_opts.add_table_properties_collector_factory("tikv.range-properties-collector", f);
            let f = Box::new(MvccPropertiesCollectorFactory::default());
            cf_opts.add_table_properties_collector_factory("tikv.mvcc-properties-collector", f);
        }

        fn set_cf_opts(cf_opts: &mut ColumnFamilyOptions, opts: &Option<EngineOpts>) {
            if let Some(ref opts) = opts {
                if let Some(trigger) = opts.level_zero_file_num_compaction_trigger {
                    cf_opts.set_level_zero_file_num_compaction_trigger(trigger);
                }
            }
        }
    }
}

