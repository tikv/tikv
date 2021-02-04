// Copyright 2020 TiKV Project Authors. Licensed under Apache-2.0.

use super::{default_engine};
use engine_traits::{KvEngine, SyncMutable, Peekable, MiscExt};

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
