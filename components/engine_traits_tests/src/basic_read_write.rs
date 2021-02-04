// Copyright 2020 TiKV Project Authors. Licensed under Apache-2.0.

//! Reading and writing

use super::{default_engine, engine_cfs};
use engine_traits::{Peekable, SyncMutable};
use engine_traits::{CF_WRITE, ALL_CFS, CF_DEFAULT};

#[test]
fn get_value_none() {
    let db = default_engine();
    let value = db.engine.get_value(b"foo").unwrap();
    assert!(value.is_none());
}

#[test]
fn put_get() {
    let db = default_engine();
    db.engine.put(b"foo", b"bar").unwrap();
    let value = db.engine.get_value(b"foo").unwrap();
    let value = value.expect("value");
    assert_eq!(b"bar", &*value);
}

#[test]
fn get_value_cf_none() {
    let db = engine_cfs(&[CF_WRITE]);
    let value = db.engine.get_value_cf(CF_WRITE, b"foo").unwrap();
    assert!(value.is_none());
}

#[test]
fn put_get_cf() {
    let db = engine_cfs(&[CF_WRITE]);
    db.engine.put_cf(CF_WRITE, b"foo", b"bar").unwrap();
    let value = db.engine.get_value_cf(CF_WRITE, b"foo").unwrap();
    let value = value.expect("value");
    assert_eq!(b"bar", &*value);
}

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

#[test]
fn non_cf_methods_implicit_default_cf() {
    let db = engine_cfs(&[CF_WRITE]);
    db.engine.put(b"foo", b"bar").unwrap();
    let value = db.engine.get_value(b"foo").unwrap();
    let value = value.expect("value");
    assert_eq!(b"bar", &*value);
    // CF_DEFAULT always exists
    let value = db.engine.get_value_cf(CF_DEFAULT, b"foo").unwrap();
    let value = value.expect("value");
    assert_eq!(b"bar", &*value);
}

#[test]
fn delete_none() {
    let db = default_engine();
    let res = db.engine.delete(b"foo");
    assert!(res.is_ok());
}

#[test]
fn delete_cf_none() {
    let db = engine_cfs(ALL_CFS);
    let res = db.engine.delete_cf(CF_WRITE, b"foo");
    assert!(res.is_ok());
}

#[test]
fn delete() {
    let db = default_engine();
    db.engine.put(b"foo", b"bar").unwrap();
    let value = db.engine.get_value(b"foo").unwrap();
    assert!(value.is_some());
    db.engine.delete(b"foo").unwrap();
    let value = db.engine.get_value(b"foo").unwrap();
    assert!(value.is_none());
}

#[test]
fn delete_cf() {
    let db = engine_cfs(ALL_CFS);
    db.engine.put_cf(CF_WRITE, b"foo", b"bar").unwrap();
    let value = db.engine.get_value_cf(CF_WRITE, b"foo").unwrap();
    assert!(value.is_some());
    db.engine.delete_cf(CF_WRITE, b"foo").unwrap();
    let value = db.engine.get_value_cf(CF_WRITE, b"foo").unwrap();
    assert!(value.is_none());
}
