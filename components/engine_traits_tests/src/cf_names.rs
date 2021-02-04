// Copyright 2020 TiKV Project Authors. Licensed under Apache-2.0.

use super::{default_engine, engine_cfs};
use engine_traits::{KvEngine, CFNamesExt, Snapshot};
use engine_traits::{CF_DEFAULT, ALL_CFS, CF_WRITE};

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

#[test]
fn implicit_default_cf() {
    let db = engine_cfs(&[CF_WRITE]);
    let names = db.engine.cf_names();
    assert_eq!(names.len(), 2);
    assert!(names.contains(&CF_DEFAULT));
}

#[test]
fn default_names_snapshot() {
    let db = default_engine();
    let snapshot = db.engine.snapshot();
    let names = snapshot.cf_names();
    assert_eq!(names.len(), 1);
    assert_eq!(names[0], CF_DEFAULT);
}

#[test]
fn cf_names_snapshot() {
    let db = engine_cfs(ALL_CFS);
    let snapshot = db.engine.snapshot();
    let names = snapshot.cf_names();
    assert_eq!(names.len(), ALL_CFS.len());
    for cf in ALL_CFS {
        assert!(names.contains(cf));
    }
}

#[test]
fn implicit_default_cf_snapshot() {
    let db = engine_cfs(&[CF_WRITE]);
    let snapshot = db.engine.snapshot();
    let names = snapshot.cf_names();
    assert_eq!(names.len(), 2);
    assert!(names.contains(&CF_DEFAULT));
}
