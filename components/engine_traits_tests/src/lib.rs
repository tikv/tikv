// Copyright 2020 TiKV Project Authors. Licensed under Apache-2.0.

//! Tests for the `engine_traits` crate
//!
//! These are basic tests that can be used to verify the conformance of
//! engines that implement the traits in the `engine_traits` crate.
//!
//! All engine instances are constructed through the `engine_test` crate,
//! so individual engines can be tested by setting that crate's feature flags.
//!
//! e.g. to test the `engine_sled` crate
//!
//! ```no_test
//! cargo test -p engine_traits_tests --no-default-features --features=protobuf-codec,test-engines-sled
//! ```

#![cfg(test)]

mod ctor;
mod basic_read_write;
mod scenario_writes;
mod delete_range;
mod cf_names;
mod iterator;
mod snapshot_basic;
mod read_consistency;
mod write_batch;
mod misc;

fn tempdir() -> tempfile::TempDir {
    tempfile::Builder::new()
        .prefix("tikv-engine-traits-tests")
        .tempdir()
        .unwrap()
}

struct TempDirEnginePair {
    // NB engine must drop before tempdir
    engine: engine_test::kv::KvTestEngine,
    tempdir: tempfile::TempDir,
}

fn default_engine() -> TempDirEnginePair {
    use engine_traits::CF_DEFAULT;
    use engine_test::kv::KvTestEngine;
    use engine_test::ctor::EngineConstructorExt;

    let dir = tempdir();
    let path = dir.path().to_str().unwrap();
    let engine = KvTestEngine::new_engine(path, None, &[CF_DEFAULT], None).unwrap();
    TempDirEnginePair {
        engine, tempdir: dir,
    }
}

fn engine_cfs(cfs: &[&str]) -> TempDirEnginePair {
    use engine_test::kv::KvTestEngine;
    use engine_test::ctor::EngineConstructorExt;

    let dir = tempdir();
    let path = dir.path().to_str().unwrap();
    let engine = KvTestEngine::new_engine(path, None, cfs, None).unwrap();
    TempDirEnginePair {
        engine, tempdir: dir,
    }
}

fn assert_engine_error<T>(r: engine_traits::Result<T>) {
    match r {
        Err(engine_traits::Error::Engine(_)) => { },
        _ => panic!("expected Error::Engine"),
    }
}
