// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

//! Tests for the `engine_traits` crate
//!
//! These are basic tests that can be used to verify the conformance of
//! engines that implement the traits in the `engine_traits` crate.
//!
//! All engine instances are constructed through the `engine_test` crate,
//! so individual engines can be tested by setting that crate's feature flags.
//!
//! e.g. to test the `engine_panic` crate
//!
//! ```no_test
//! cargo test -p engine_traits_tests --no-default-features --features=protobuf-codec,test-engines-panic
//! ```
//!
//! As of now this mostly tests the essential features of
//!
//! - point get / delete
//! - range deletes
//! - snapshots
//! - write batches
//! - iterators
//!
//! It is intended though to cover essentially all features of storage engines.
//!
//! It is poor at testing data consistency and error recovery,
//! as such tests require hooks into the engine which are not
//! presented by the engine traits.
//!
//! Note that in some cases there are multiple ways to accomplish the same
//! thing. In particular, engines have methods that operate on the default
//! column family, and these are effectively the same as using the `_cf` methods
//! with `CF_DEFAULT`; and writing can be performed directly through the engine,
//! or through write batches. As such, some tests are parameterized over
//! multiple APIs. This is particularly true of the `scenario_writes` module,
//! which contains much of the basic writing tests, and the `iterator` module.

#![cfg(test)]

mod basic_read_write;
mod cf_names;
mod ctor;
mod delete_range;
mod iterator;
mod misc;
mod read_consistency;
mod scenario_writes;
mod snapshot_basic;
mod write_batch;

/// The engine / tempdir pair used in all tests
struct TempDirEnginePair {
    // NB engine must drop before tempdir
    engine: engine_test::kv::KvTestEngine,
    tempdir: tempfile::TempDir,
}

/// Create an engine with only CF_DEFAULT
fn default_engine() -> TempDirEnginePair {
    use engine_test::ctor::EngineConstructorExt;
    use engine_test::kv::KvTestEngine;
    use engine_traits::CF_DEFAULT;

    let dir = tempdir();
    let path = dir.path().to_str().unwrap();
    let engine = KvTestEngine::new_engine(path, None, &[CF_DEFAULT], None).unwrap();
    TempDirEnginePair {
        engine,
        tempdir: dir,
    }
}

/// Create an engine with the specified column families
fn engine_cfs(cfs: &[&str]) -> TempDirEnginePair {
    use engine_test::ctor::EngineConstructorExt;
    use engine_test::kv::KvTestEngine;

    let dir = tempdir();
    let path = dir.path().to_str().unwrap();
    let engine = KvTestEngine::new_engine(path, None, cfs, None).unwrap();
    TempDirEnginePair {
        engine,
        tempdir: dir,
    }
}

fn tempdir() -> tempfile::TempDir {
    tempfile::Builder::new()
        .prefix("tikv-engine-traits-tests")
        .tempdir()
        .unwrap()
}

fn assert_engine_error<T>(r: engine_traits::Result<T>) {
    match r {
        Err(engine_traits::Error::Engine(_)) => {}
        Err(e) => panic!("expected Error::Engine, got {:?}", e),
        Ok(_) => panic!("expected Error::Engine, got Ok"),
    }
}
