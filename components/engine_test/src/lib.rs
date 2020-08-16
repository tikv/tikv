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
    use crate::ctor::{EngineConstructorExt};

    #[cfg(feature = "test-engine-raft-panic")]
    pub use engine_panic::{
        PanicEngine as RaftTestEngine,
    };

    #[cfg(feature = "test-engine-raft-rocksdb")]
    pub use engine_rocks::{
        RocksEngine as RaftTestEngine,
    };

    pub fn new_default_engine(path: &str) -> Result<RaftTestEngine> {
        RaftTestEngine::new_default_engine(path)
    }
}

/// Types and constructors for the "kv" engine
pub mod kv {
    use engine_traits::Result;
    use crate::ctor::{EngineConstructorExt};

    #[cfg(feature = "test-engine-kv-panic")]
    pub use engine_panic::{
        PanicEngine as KvTestEngine,
    };

    #[cfg(feature = "test-engine-kv-rocksdb")]
    pub use engine_rocks::{
        RocksEngine as KvTestEngine,
    };

    pub fn new_default_engine(path: &str) -> Result<KvTestEngine> {
        KvTestEngine::new_default_engine(path)
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

    pub trait EngineConstructorExt: Sized {
        fn new_default_engine(path: &str) -> Result<Self>;
    }

    mod panic {
        use engine_traits::Result;
        use super::EngineConstructorExt;

        impl EngineConstructorExt for engine_panic::PanicEngine {
            fn new_default_engine(_path: &str) -> Result<Self> {
                Ok(engine_panic::PanicEngine)
            }
        }
    }

    mod rocks {
        use engine_traits::Result;
        use super::EngineConstructorExt;

        impl EngineConstructorExt for engine_rocks::RocksEngine {
            fn new_default_engine(path: &str) -> Result<Self> {
                engine_rocks::util::new_default_engine(path)
            }
        }
    }
}

