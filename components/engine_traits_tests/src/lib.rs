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


fn tempdir() -> tempfile::TempDir {
    tempfile::Builder::new()
        .prefix("tikv-engine-traits-tests")
        .tempdir()
        .unwrap()
}

struct TempDirEnginePair {
    // NB engine must drop before tempdir
    engine: engine_test::kv::KvTestEngine,
    #[allow(unused)]
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


mod ctor {
    //! Constructor tests

    use super::tempdir;

    use engine_traits::ALL_CFS;
    use engine_test::kv::KvTestEngine;
    use engine_test::ctor::{EngineConstructorExt, DBOptions, CFOptions, ColumnFamilyOptions};

    #[test]
    fn new_engine_basic() {
        let dir = tempdir();
        let path = dir.path().to_str().unwrap();
        let _db = KvTestEngine::new_engine(path, None, ALL_CFS, None).unwrap();
    }

    #[test]
    fn new_engine_opt_basic() {
        let dir = tempdir();
        let path = dir.path().to_str().unwrap();
        let db_opts = DBOptions::new();
        let cf_opts = ALL_CFS.iter().map(|cf| {
            CFOptions::new(cf, ColumnFamilyOptions::new())
        }).collect();
        let _db = KvTestEngine::new_engine_opt(path, db_opts, cf_opts).unwrap();
    }
}

mod basic_read_write {
    //! Reading and writing

    use super::default_engine;
    use engine_traits::{Peekable, SyncMutable};

    #[test]
    fn get_value_none() {
        let db = default_engine();
        let value = db.engine.get_value(b"foo").unwrap();
        assert!(value.is_none());
    }

    #[test]
    fn put_value_get_value() {
        let db = default_engine();
        let expected = b"bar";
        db.engine.put(b"foo", expected).unwrap();
        let actual = db.engine.get_value(b"foo").unwrap();
        let actual = actual.expect("value");
        assert_eq!(expected, &*actual);
    }
}
