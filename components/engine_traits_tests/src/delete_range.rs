// Copyright 2020 TiKV Project Authors. Licensed under Apache-2.0.

use super::{default_engine};
use engine_traits::{SyncMutable, Peekable};
use engine_traits::{CF_DEFAULT};
use std::panic::{self, AssertUnwindSafe};

#[test]
fn delete_range_cf_inclusive_exclusive() {
    let db = default_engine();

    db.engine.put(b"a", b"").unwrap();
    db.engine.put(b"b", b"").unwrap();
    db.engine.put(b"c", b"").unwrap();
    db.engine.put(b"d", b"").unwrap();
    db.engine.put(b"e", b"").unwrap();

    db.engine.delete_range_cf(CF_DEFAULT, b"b", b"e").unwrap();

    assert!(db.engine.get_value(b"a").unwrap().is_some());
    assert!(db.engine.get_value(b"b").unwrap().is_none());
    assert!(db.engine.get_value(b"c").unwrap().is_none());
    assert!(db.engine.get_value(b"d").unwrap().is_none());
    assert!(db.engine.get_value(b"e").unwrap().is_some());
}

#[test]
fn delete_range_cf_all_in_range() {
    let db = default_engine();

    db.engine.put(b"b", b"").unwrap();
    db.engine.put(b"c", b"").unwrap();
    db.engine.put(b"d", b"").unwrap();

    db.engine.delete_range_cf(CF_DEFAULT, b"a", b"e").unwrap();

    assert!(db.engine.get_value(b"b").unwrap().is_none());
    assert!(db.engine.get_value(b"c").unwrap().is_none());
    assert!(db.engine.get_value(b"d").unwrap().is_none());
}

#[test]
fn delete_range_cf_equal_begin_and_end() {
    let db = default_engine();

    db.engine.put(b"b", b"").unwrap();
    db.engine.put(b"c", b"").unwrap();
    db.engine.put(b"d", b"").unwrap();

    db.engine.delete_range_cf(CF_DEFAULT, b"c", b"c").unwrap();

    assert!(db.engine.get_value(b"b").unwrap().is_some());
    assert!(db.engine.get_value(b"c").unwrap().is_some());
    assert!(db.engine.get_value(b"d").unwrap().is_some());
}

#[test]
fn delete_range_cf_reverse_range() {
    let db = default_engine();

    db.engine.put(b"b", b"").unwrap();
    db.engine.put(b"c", b"").unwrap();
    db.engine.put(b"d", b"").unwrap();

    assert!(panic::catch_unwind(AssertUnwindSafe(|| {
        db.engine.delete_range_cf(CF_DEFAULT, b"d", b"b").unwrap();
    })).is_err());

    assert!(db.engine.get_value(b"b").unwrap().is_some());
    assert!(db.engine.get_value(b"c").unwrap().is_some());
    assert!(db.engine.get_value(b"d").unwrap().is_some());
}

#[test]
fn delete_range_cf_bad_cf() {
    let db = default_engine();
    assert!(panic::catch_unwind(AssertUnwindSafe(|| {
        db.engine.delete_range_cf("bogus", b"a", b"b").unwrap();
    })).is_err());
}
