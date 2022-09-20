// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

use engine_traits::{CfNamesExt, KvEngine, Snapshot, ALL_CFS, CF_DEFAULT};

use super::{default_engine, engine_cfs};

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
