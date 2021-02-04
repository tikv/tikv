// Copyright 2020 TiKV Project Authors. Licensed under Apache-2.0.

use super::{default_engine, engine_cfs};
use engine_traits::{KvEngine, SyncMutable, Peekable};
use engine_traits::{ALL_CFS, CF_WRITE};

#[test]
fn snapshot_get_value() {
    let db = default_engine();

    db.engine.put(b"a", b"aa").unwrap();

    let snap = db.engine.snapshot();

    let value = snap.get_value(b"a").unwrap();
    let value = value.unwrap();
    assert_eq!(value, b"aa");

    let value = snap.get_value(b"b").unwrap();
    assert!(value.is_none());
}

#[test]
fn snapshot_get_value_after_put() {
    let db = default_engine();

    db.engine.put(b"a", b"aa").unwrap();

    let snap = db.engine.snapshot();

    db.engine.put(b"a", b"aaa").unwrap();

    let value = snap.get_value(b"a").unwrap();
    let value = value.unwrap();
    assert_eq!(value, b"aa");
}

#[test]
fn snapshot_get_value_cf() {
    let db = engine_cfs(ALL_CFS);

    db.engine.put_cf(CF_WRITE, b"a", b"aa").unwrap();

    let snap = db.engine.snapshot();

    let value = snap.get_value_cf(CF_WRITE, b"a").unwrap();
    let value = value.unwrap();
    assert_eq!(value, b"aa");

    let value = snap.get_value_cf(CF_WRITE, b"b").unwrap();
    assert!(value.is_none());
}

#[test]
fn snapshot_get_value_cf_after_put() {
    let db = engine_cfs(ALL_CFS);

    db.engine.put_cf(CF_WRITE, b"a", b"aa").unwrap();

    let snap = db.engine.snapshot();

    db.engine.put_cf(CF_WRITE, b"a", b"aaa").unwrap();

    let value = snap.get_value_cf(CF_WRITE, b"a").unwrap();
    let value = value.unwrap();
    assert_eq!(value, b"aa");
}
