// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

use engine_traits::{KvEngine, MiscExt, Peekable, SyncMutable};

use super::default_engine;

#[test]
fn sync_basic() {
    let db = default_engine();
    db.engine.put(b"foo", b"bar").unwrap();
    db.engine.sync().unwrap();
    let value = db.engine.get_value(b"foo").unwrap();
    let value = value.expect("value");
    assert_eq!(b"bar", &*value);
}

#[test]
fn path() {
    let db = default_engine();
    let path = db.tempdir.path().to_str().unwrap();
    assert_eq!(db.engine.path(), path);
}
