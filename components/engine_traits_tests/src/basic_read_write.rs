// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

//! Reading and writing

use engine_traits::{Peekable, SyncMutable, ALL_CFS, CF_DEFAULT, CF_WRITE};

use super::engine_cfs;

// Store using put; load using get_cf(CF_DEFAULT)
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

// CF_DEFAULT always exists
#[test]
fn non_cf_methods_implicit_default_cf() {
    let db = engine_cfs(&[CF_WRITE]);
    db.engine.put(b"foo", b"bar").unwrap();
    let value = db.engine.get_value(b"foo").unwrap();
    let value = value.expect("value");
    assert_eq!(b"bar", &*value);
    let value = db.engine.get_value_cf(CF_DEFAULT, b"foo").unwrap();
    let value = value.expect("value");
    assert_eq!(b"bar", &*value);
}
