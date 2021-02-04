// Copyright 2020 TiKV Project Authors. Licensed under Apache-2.0.

//! Testing iterator and snapshot behavior in the presence of intermixed writes

use super::{default_engine};
use engine_traits::{KvEngine, SyncMutable, Peekable};
use engine_traits::{Iterable, Iterator};

#[test]
fn snapshot_with_writes() {
    let db = default_engine();

    db.engine.put(b"a", b"aa").unwrap();

    let snapshot = db.engine.snapshot();

    assert_eq!(snapshot.get_value(b"a").unwrap().unwrap(), b"aa");

    db.engine.put(b"b", b"bb").unwrap();

    assert!(snapshot.get_value(b"b").unwrap().is_none());
    assert_eq!(db.engine.get_value(b"b").unwrap().unwrap(), b"bb");

    db.engine.delete(b"a").unwrap();

    assert_eq!(snapshot.get_value(b"a").unwrap().unwrap(), b"aa");
    assert!(db.engine.get_value(b"a").unwrap().is_none());
}

// Both the snapshot and engine iterators maintain read consistency at a
// single point in time. It seems the engine iterator is essentially just a
// snapshot iterator.
fn iterator_with_writes<E, I, IF>(e: &E, i: IF)
where E: KvEngine,
      I: Iterator,
      IF: Fn(&E) -> I,
{
    e.put(b"a", b"").unwrap();
    e.put(b"c", b"").unwrap();

    let mut iter = i(e);

    assert!(iter.seek_to_first().unwrap());
    assert_eq!(iter.key(), b"a");

    e.put(b"b", b"").unwrap();

    assert!(iter.next().unwrap());
    assert_eq!(iter.key(), b"c");
    assert!(e.get_value(b"b").unwrap().is_some());

    e.put(b"d", b"").unwrap();

    assert!(!iter.next().unwrap());
    assert!(e.get_value(b"d").unwrap().is_some());

    e.delete(b"a").unwrap();
    e.delete(b"c").unwrap();

    iter.seek_to_first().unwrap();
    assert_eq!(iter.key(), b"a");
    assert!(iter.next().unwrap());
    assert_eq!(iter.key(), b"c");
    assert!(!iter.next().unwrap());

    assert!(e.get_value(b"a").unwrap().is_none());
    assert!(e.get_value(b"c").unwrap().is_none());
}

#[test]
fn iterator_with_writes_engine() {
    let db = default_engine();
    iterator_with_writes(&db.engine, |e| e.iterator().unwrap());
}

#[test]
fn iterator_with_writes_snapshot() {
    let db = default_engine();
    iterator_with_writes(&db.engine, |e| e.snapshot().iterator().unwrap());
}
