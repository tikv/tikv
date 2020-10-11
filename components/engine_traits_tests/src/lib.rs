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

    use super::{default_engine, engine_cfs};
    use engine_traits::{Peekable, SyncMutable};
    use engine_traits::{ALL_CFS, CF_DEFAULT};

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

    #[test]
    fn non_cf_methods_are_default_cf() {
        let db = engine_cfs(ALL_CFS);
        // Use the non-cf put function
        db.engine.put(b"foo", b"bar").unwrap();
        // Retreive with the cf get function
        let value = db.engine.get_value_cf(CF_DEFAULT, b"foo").unwrap();
        let value = value.expect("value");
        assert_eq!(b"bar", &*value);
    }
}

mod cf_names {
    use super::{default_engine, engine_cfs};
    use engine_traits::CFNamesExt;
    use engine_traits::{CF_DEFAULT, ALL_CFS};

    #[test]
    fn default_names() {
        let db = default_engine();
        let names = db.engine.cf_names();
        assert_eq!(names.len(), 1);
        assert_eq!(names[0], CF_DEFAULT);
    }

    #[test]
    fn cf_names() {
        let db = engine_cfs(ALL_CFS);
        let names = db.engine.cf_names();
        assert_eq!(names.len(), ALL_CFS.len());
        for cf in ALL_CFS {
            assert!(names.contains(cf));
        }
    }
}

mod engine_iter {
    use super::{default_engine};
    use engine_traits::{Iterable, Iterator};
    use engine_traits::SeekKey;
    use engine_traits::SyncMutable;
    use std::panic::{self, AssertUnwindSafe};

    #[test]
    fn iter_empty() {
        let db = default_engine();
        let mut iter = db.engine.iterator().unwrap();

        assert_eq!(iter.valid().unwrap(), false);

        assert!(panic::catch_unwind(AssertUnwindSafe(|| {
            let _ = iter.prev();
        })).is_err());
        assert!(panic::catch_unwind(AssertUnwindSafe(|| {
            let _ = iter.next();
        })).is_err());
        assert!(panic::catch_unwind(|| {
            iter.key();
        }).is_err());
        assert!(panic::catch_unwind(|| {
            iter.value();
        }).is_err());

        assert_eq!(iter.seek(SeekKey::Start).unwrap(), false);
        assert_eq!(iter.seek(SeekKey::End).unwrap(), false);
        assert_eq!(iter.seek(SeekKey::Key(b"foo")).unwrap(), false);
        assert_eq!(iter.seek_for_prev(SeekKey::Start).unwrap(), false);
        assert_eq!(iter.seek_for_prev(SeekKey::End).unwrap(), false);
        assert_eq!(iter.seek_for_prev(SeekKey::Key(b"foo")).unwrap(), false);
    }

    #[test]
    fn iter_forward() {
        let db = default_engine();

        db.engine.put(b"a", b"a").unwrap();
        db.engine.put(b"b", b"b").unwrap();
        db.engine.put(b"c", b"c").unwrap();

        let mut iter = db.engine.iterator().unwrap();

        assert!(!iter.valid().unwrap());

        assert!(iter.seek(SeekKey::Start).unwrap());

        assert!(iter.valid().unwrap());
        assert_eq!(iter.key(), b"a");
        assert_eq!(iter.value(), b"a");

        assert_eq!(iter.next().unwrap(), true);

        assert!(iter.valid().unwrap());
        assert_eq!(iter.key(), b"b");
        assert_eq!(iter.value(), b"b");

        assert_eq!(iter.next().unwrap(), true);

        assert!(iter.valid().unwrap());
        assert_eq!(iter.key(), b"c");
        assert_eq!(iter.value(), b"c");

        assert_eq!(iter.next().unwrap(), false);

        assert!(!iter.valid().unwrap());

        assert!(panic::catch_unwind(AssertUnwindSafe(|| {
            iter.key();
        })).is_err());
        assert!(panic::catch_unwind(AssertUnwindSafe(|| {
            iter.value();
        })).is_err());
    }

    #[test]
    fn iter_reverse() {
        let db = default_engine();

        db.engine.put(b"a", b"a").unwrap();
        db.engine.put(b"b", b"b").unwrap();
        db.engine.put(b"c", b"c").unwrap();

        let mut iter = db.engine.iterator().unwrap();

        assert!(!iter.valid().unwrap());

        assert!(iter.seek(SeekKey::End).unwrap());

        assert!(iter.valid().unwrap());
        assert_eq!(iter.key(), b"c");
        assert_eq!(iter.value(), b"c");

        assert_eq!(iter.prev().unwrap(), true);

        assert!(iter.valid().unwrap());
        assert_eq!(iter.key(), b"b");
        assert_eq!(iter.value(), b"b");

        assert_eq!(iter.prev().unwrap(), true);

        assert!(iter.valid().unwrap());
        assert_eq!(iter.key(), b"a");
        assert_eq!(iter.value(), b"a");

        assert_eq!(iter.prev().unwrap(), false);

        assert!(!iter.valid().unwrap());

        assert!(panic::catch_unwind(AssertUnwindSafe(|| {
            iter.key();
        })).is_err());
        assert!(panic::catch_unwind(AssertUnwindSafe(|| {
            iter.value();
        })).is_err());
    }

}

