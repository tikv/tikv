// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

use engine_test::kv::KvTestEngine;
use engine_traits::{Mutable, Peekable, SyncMutable, WriteBatch, WriteBatchExt};
use panic_hook::recover_safe;

use super::{assert_engine_error, default_engine, multi_batch_write_engine};

#[test]
fn write_batch_none_no_commit() {
    let db = default_engine();
    let wb = db.engine.write_batch();
    drop(wb);

    let db = multi_batch_write_engine();
    let wb = db.engine.write_batch_with_cap(1024);
    drop(wb);
}

#[test]
fn write_batch_none() {
    let db = default_engine();
    let wb = db.engine.write_batch();
    wb.write().unwrap();

    let db = multi_batch_write_engine();
    let wb = db.engine.write_batch_with_cap(1024);
    wb.write().unwrap();
}

#[test]
fn write_batch_put() {
    let db = default_engine();

    let mut wb = db.engine.write_batch();

    wb.put(b"a", b"aa").unwrap();

    wb.write().unwrap();

    assert_eq!(db.engine.get_value(b"a").unwrap().unwrap(), b"aa");

    let db = multi_batch_write_engine();

    let mut wb = db.engine.write_batch_with_cap(1024);

    for i in 0..128_usize {
        let x = i.to_be_bytes();
        wb.put(&x, &x).unwrap();
    }
    wb.put(b"a", b"aa").unwrap();
    for i in 128..256_usize {
        let x = i.to_be_bytes();
        wb.put(&x, &x).unwrap();
    }

    wb.write().unwrap();

    assert_eq!(db.engine.get_value(b"a").unwrap().unwrap(), b"aa");
    for i in 0..256_usize {
        let x = i.to_be_bytes();
        assert_eq!(db.engine.get_value(&x).unwrap().unwrap(), &x);
    }
}

#[test]
fn write_batch_delete() {
    let db = default_engine();

    db.engine.put(b"a", b"aa").unwrap();

    let mut wb = db.engine.write_batch();

    wb.delete(b"a").unwrap();

    wb.write().unwrap();

    assert!(db.engine.get_value(b"a").unwrap().is_none());

    let db = multi_batch_write_engine();

    for i in 0..127_usize {
        let x = i.to_be_bytes();
        db.engine.put(&x, &x).unwrap();
    }
    db.engine.put(b"a", b"aa").unwrap();
    for i in 127..255_usize {
        let x = i.to_be_bytes();
        db.engine.put(&x, &x).unwrap();
    }

    let mut wb = db.engine.write_batch_with_cap(1024);

    for i in 0..255_usize {
        let k = i.to_be_bytes();
        wb.delete(&k).unwrap();
    }
    wb.delete(b"a").unwrap();

    wb.write().unwrap();

    assert!(db.engine.get_value(b"a").unwrap().is_none());
    for i in 0..255_usize {
        assert!(db.engine.get_value(&i.to_be_bytes()).unwrap().is_none());
    }
}

#[test]
fn write_batch_write_twice_1() {
    let db = default_engine();

    let mut wb = db.engine.write_batch();

    wb.put(b"a", b"aa").unwrap();

    wb.write().unwrap();
    wb.write().unwrap();

    assert_eq!(db.engine.get_value(b"a").unwrap().unwrap(), b"aa");

    let db = multi_batch_write_engine();

    let mut wb = db.engine.write_batch_with_cap(1024);

    for i in 0..123_usize {
        let x = i.to_be_bytes();
        wb.put(&x, &x).unwrap();
    }
    wb.put(b"a", b"aa").unwrap();

    wb.write().unwrap();
    wb.write().unwrap();

    assert_eq!(db.engine.get_value(b"a").unwrap().unwrap(), b"aa");
    for i in 0..123_usize {
        let x = i.to_be_bytes();
        assert_eq!(db.engine.get_value(&x).unwrap().unwrap(), &x);
    }
}

#[test]
fn write_batch_write_twice_2() {
    let db = default_engine();

    let mut wb = db.engine.write_batch();

    wb.put(b"a", b"aa").unwrap();

    wb.write().unwrap();

    db.engine.put(b"a", b"b").unwrap();
    assert_eq!(db.engine.get_value(b"a").unwrap().unwrap(), b"b");

    wb.write().unwrap();

    assert_eq!(db.engine.get_value(b"a").unwrap().unwrap(), b"aa");

    let db = multi_batch_write_engine();

    let mut wb = db.engine.write_batch_with_cap(1024);

    for i in 0..128_usize {
        let x = i.to_be_bytes();
        wb.put(&x, &x).unwrap();
    }
    wb.put(b"a", b"aa").unwrap();

    wb.write().unwrap();

    db.engine.put(b"a", b"b").unwrap();
    assert_eq!(db.engine.get_value(b"a").unwrap().unwrap(), b"b");

    for i in 0..128_usize {
        let k = i.to_be_bytes();
        let v = (2 * i + 1).to_be_bytes();
        db.engine.put(&k, &v).unwrap();
    }
    for i in 0..128_usize {
        let k = i.to_be_bytes();
        let v = (2 * i + 1).to_be_bytes();
        assert_eq!(db.engine.get_value(&k).unwrap().unwrap(), &v);
    }

    wb.write().unwrap();

    assert_eq!(db.engine.get_value(b"a").unwrap().unwrap(), b"aa");
    for i in 0..128_usize {
        let x = i.to_be_bytes();
        assert_eq!(db.engine.get_value(&x).unwrap().unwrap(), &x);
    }
}

#[test]
fn write_batch_write_twice_3() {
    let db = default_engine();

    let mut wb = db.engine.write_batch();

    wb.put(b"a", b"aa").unwrap();

    wb.write().unwrap();
    db.engine.put(b"a", b"b").unwrap();
    wb.put(b"b", b"bb").unwrap();
    wb.write().unwrap();

    assert_eq!(db.engine.get_value(b"a").unwrap().unwrap(), b"aa");
    assert_eq!(db.engine.get_value(b"b").unwrap().unwrap(), b"bb");

    let db = multi_batch_write_engine();

    let mut wb = db.engine.write_batch_with_cap(1024);

    for i in 0..128_usize {
        let x = i.to_be_bytes();
        wb.put(&x, &x).unwrap();
    }
    wb.put(b"a", b"aa").unwrap();

    wb.write().unwrap();
    for i in 0..128_usize {
        let k = i.to_be_bytes();
        let v = (2 * i + 1).to_be_bytes();
        db.engine.put(&k, &v).unwrap();
    }
    db.engine.put(b"a", b"b").unwrap();
    for i in 128..256_usize {
        let x = i.to_be_bytes();
        wb.put(&x, &x).unwrap();
    }
    wb.put(b"b", b"bb").unwrap();
    wb.write().unwrap();

    assert_eq!(db.engine.get_value(b"a").unwrap().unwrap(), b"aa");
    assert_eq!(db.engine.get_value(b"b").unwrap().unwrap(), b"bb");
    for i in 0..256_usize {
        let x = i.to_be_bytes();
        assert_eq!(db.engine.get_value(&x).unwrap().unwrap(), &x);
    }
}

#[test]
fn write_batch_delete_range_basic() {
    let db = default_engine();

    db.engine.put(b"a", b"").unwrap();
    db.engine.put(b"b", b"").unwrap();
    db.engine.put(b"c", b"").unwrap();
    db.engine.put(b"d", b"").unwrap();
    db.engine.put(b"e", b"").unwrap();

    let mut wb = db.engine.write_batch();

    wb.delete_range(b"b", b"e").unwrap();
    wb.write().unwrap();

    assert!(db.engine.get_value(b"a").unwrap().is_some());
    assert!(db.engine.get_value(b"b").unwrap().is_none());
    assert!(db.engine.get_value(b"c").unwrap().is_none());
    assert!(db.engine.get_value(b"d").unwrap().is_none());
    assert!(db.engine.get_value(b"e").unwrap().is_some());

    let db = multi_batch_write_engine();

    db.engine.put(b"a", b"").unwrap();
    db.engine.put(b"b", b"").unwrap();
    db.engine.put(b"c", b"").unwrap();
    db.engine.put(b"d", b"").unwrap();
    db.engine.put(b"e", b"").unwrap();

    let mut wb = db.engine.write_batch_with_cap(1024);
    for i in 0..256_usize {
        let x = i.to_be_bytes();
        wb.put(&x, &x).unwrap();
    }

    wb.delete_range(b"b", b"e").unwrap();
    wb.delete_range(&32_usize.to_be_bytes(), &128_usize.to_be_bytes())
        .unwrap();
    wb.write().unwrap();

    assert!(db.engine.get_value(b"a").unwrap().is_some());
    assert!(db.engine.get_value(b"b").unwrap().is_none());
    assert!(db.engine.get_value(b"c").unwrap().is_none());
    assert!(db.engine.get_value(b"d").unwrap().is_none());
    assert!(db.engine.get_value(b"e").unwrap().is_some());
    for i in 0..32_usize {
        let x = i.to_be_bytes();
        assert!(db.engine.get_value(&x).unwrap().is_some());
    }
    for i in 32..128_usize {
        let x = i.to_be_bytes();
        assert!(db.engine.get_value(&x).unwrap().is_none());
    }
    for i in 128..256_usize {
        let x = i.to_be_bytes();
        assert!(db.engine.get_value(&x).unwrap().is_some());
    }
}

#[test]
fn write_batch_delete_range_inexact() {
    let db = default_engine();

    db.engine.put(b"a", b"").unwrap();
    db.engine.put(b"c", b"").unwrap();
    db.engine.put(b"d", b"").unwrap();
    db.engine.put(b"e", b"").unwrap();
    db.engine.put(b"g", b"").unwrap();

    let mut wb = db.engine.write_batch();

    wb.delete_range(b"b", b"f").unwrap();
    wb.write().unwrap();

    assert!(db.engine.get_value(b"a").unwrap().is_some());
    assert!(db.engine.get_value(b"b").unwrap().is_none());
    assert!(db.engine.get_value(b"c").unwrap().is_none());
    assert!(db.engine.get_value(b"d").unwrap().is_none());
    assert!(db.engine.get_value(b"e").unwrap().is_none());
    assert!(db.engine.get_value(b"f").unwrap().is_none());
    assert!(db.engine.get_value(b"g").unwrap().is_some());

    let db = multi_batch_write_engine();

    db.engine.put(b"a", b"").unwrap();
    db.engine.put(b"c", b"").unwrap();
    db.engine.put(b"d", b"").unwrap();
    db.engine.put(b"e", b"").unwrap();
    db.engine.put(b"g", b"").unwrap();

    let mut wb = db.engine.write_batch_with_cap(1024);
    for i in (0..256_usize).step_by(2) {
        let x = i.to_be_bytes();
        wb.put(&x, &x).unwrap();
    }

    wb.delete_range(b"b", b"f").unwrap();
    wb.delete_range(&0_usize.to_be_bytes(), &252_usize.to_be_bytes())
        .unwrap();
    wb.write().unwrap();

    assert!(db.engine.get_value(b"a").unwrap().is_some());
    assert!(db.engine.get_value(b"b").unwrap().is_none());
    assert!(db.engine.get_value(b"c").unwrap().is_none());
    assert!(db.engine.get_value(b"d").unwrap().is_none());
    assert!(db.engine.get_value(b"e").unwrap().is_none());
    assert!(db.engine.get_value(b"f").unwrap().is_none());
    assert!(db.engine.get_value(b"g").unwrap().is_some());
    for i in 0..252_usize {
        assert!(db.engine.get_value(&i.to_be_bytes()).unwrap().is_none());
    }
    assert!(
        db.engine
            .get_value(&252_usize.to_be_bytes())
            .unwrap()
            .is_some()
    );
    assert!(
        db.engine
            .get_value(&253_usize.to_be_bytes())
            .unwrap()
            .is_none()
    );
    assert!(
        db.engine
            .get_value(&254_usize.to_be_bytes())
            .unwrap()
            .is_some()
    );
}

#[test]
fn write_batch_delete_range_after_put() {
    let db = default_engine();
    let mut wb = db.engine.write_batch();

    wb.put(b"a", b"").unwrap();
    wb.put(b"b", b"").unwrap();
    wb.put(b"c", b"").unwrap();
    wb.put(b"d", b"").unwrap();
    wb.put(b"e", b"").unwrap();
    wb.delete_range(b"b", b"e").unwrap();
    wb.write().unwrap();

    assert!(db.engine.get_value(b"a").unwrap().is_some());
    assert!(db.engine.get_value(b"b").unwrap().is_none());
    assert!(db.engine.get_value(b"c").unwrap().is_none());
    assert!(db.engine.get_value(b"d").unwrap().is_none());
    assert!(db.engine.get_value(b"e").unwrap().is_some());

    let db = multi_batch_write_engine();
    let mut wb = db.engine.write_batch_with_cap(1024);
    for i in 0..256_usize {
        let x = i.to_be_bytes();
        wb.put(&x, &x).unwrap();
    }
    wb.put(b"a", b"").unwrap();
    wb.put(b"b", b"").unwrap();
    wb.put(b"c", b"").unwrap();
    wb.put(b"d", b"").unwrap();
    wb.put(b"e", b"").unwrap();
    wb.delete_range(&1_usize.to_be_bytes(), &255_usize.to_be_bytes())
        .unwrap();
    wb.delete_range(b"b", b"e").unwrap();
    wb.write().unwrap();

    assert!(
        db.engine
            .get_value(&0_usize.to_be_bytes())
            .unwrap()
            .is_some()
    );
    for i in 1..255_usize {
        assert!(db.engine.get_value(&i.to_be_bytes()).unwrap().is_none());
    }
    assert!(
        db.engine
            .get_value(&255_usize.to_be_bytes())
            .unwrap()
            .is_some()
    );
    assert!(db.engine.get_value(b"a").unwrap().is_some());
    assert!(db.engine.get_value(b"b").unwrap().is_none());
    assert!(db.engine.get_value(b"c").unwrap().is_none());
    assert!(db.engine.get_value(b"d").unwrap().is_none());
    assert!(db.engine.get_value(b"e").unwrap().is_some());
}

#[test]
fn write_batch_delete_range_none() {
    let db = default_engine();

    db.engine.put(b"a", b"").unwrap();
    db.engine.put(b"e", b"").unwrap();

    let mut wb = db.engine.write_batch();

    wb.delete_range(b"b", b"e").unwrap();
    wb.write().unwrap();

    assert!(db.engine.get_value(b"a").unwrap().is_some());
    assert!(db.engine.get_value(b"b").unwrap().is_none());
    assert!(db.engine.get_value(b"c").unwrap().is_none());
    assert!(db.engine.get_value(b"d").unwrap().is_none());
    assert!(db.engine.get_value(b"e").unwrap().is_some());

    let db = multi_batch_write_engine();

    db.engine.put(b"a", b"").unwrap();
    db.engine.put(b"e", b"").unwrap();
    for i in 0..256_usize {
        let x = i.to_be_bytes();
        db.engine.put(&x, &x).unwrap();
    }

    let mut wb = db.engine.write_batch_with_cap(1024);

    wb.delete_range(b"b", b"e").unwrap();
    wb.delete_range(&1_usize.to_be_bytes(), &256_usize.to_be_bytes())
        .unwrap();
    wb.write().unwrap();

    assert!(db.engine.get_value(b"a").unwrap().is_some());
    assert!(db.engine.get_value(b"b").unwrap().is_none());
    assert!(db.engine.get_value(b"c").unwrap().is_none());
    assert!(db.engine.get_value(b"d").unwrap().is_none());
    assert!(db.engine.get_value(b"e").unwrap().is_some());
    assert!(
        db.engine
            .get_value(&0_usize.to_be_bytes())
            .unwrap()
            .is_some()
    );
    for i in 1..256_usize {
        assert!(db.engine.get_value(&i.to_be_bytes()).unwrap().is_none());
    }
}

#[test]
fn write_batch_delete_range_twice() {
    let db = default_engine();

    db.engine.put(b"a", b"").unwrap();
    db.engine.put(b"b", b"").unwrap();
    db.engine.put(b"c", b"").unwrap();
    db.engine.put(b"d", b"").unwrap();
    db.engine.put(b"e", b"").unwrap();

    let mut wb = db.engine.write_batch();

    wb.delete_range(b"b", b"e").unwrap();
    wb.delete_range(b"b", b"e").unwrap();
    wb.write().unwrap();

    assert!(db.engine.get_value(b"a").unwrap().is_some());
    assert!(db.engine.get_value(b"b").unwrap().is_none());
    assert!(db.engine.get_value(b"c").unwrap().is_none());
    assert!(db.engine.get_value(b"d").unwrap().is_none());
    assert!(db.engine.get_value(b"e").unwrap().is_some());

    let db = multi_batch_write_engine();

    db.engine.put(b"a", b"").unwrap();
    db.engine.put(b"b", b"").unwrap();
    db.engine.put(b"c", b"").unwrap();
    db.engine.put(b"d", b"").unwrap();
    db.engine.put(b"e", b"").unwrap();

    let mut wb = db.engine.write_batch_with_cap(1024);
    for i in 0..256_usize {
        let x = i.to_be_bytes();
        wb.put(&x, &x).unwrap();
    }

    wb.delete_range(b"b", b"e").unwrap();
    wb.delete_range(b"b", b"e").unwrap();
    wb.delete_range(&1_usize.to_be_bytes(), &256_usize.to_be_bytes())
        .unwrap();
    wb.delete_range(&1_usize.to_be_bytes(), &256_usize.to_be_bytes())
        .unwrap();
    wb.write().unwrap();

    assert!(db.engine.get_value(b"a").unwrap().is_some());
    assert!(db.engine.get_value(b"b").unwrap().is_none());
    assert!(db.engine.get_value(b"c").unwrap().is_none());
    assert!(db.engine.get_value(b"d").unwrap().is_none());
    assert!(db.engine.get_value(b"e").unwrap().is_some());
    assert!(
        db.engine
            .get_value(&0_usize.to_be_bytes())
            .unwrap()
            .is_some()
    );
    for i in 1..256_usize {
        assert!(db.engine.get_value(&i.to_be_bytes()).unwrap().is_none());
    }
}

#[test]
fn write_batch_delete_range_twice_1() {
    let db = default_engine();

    db.engine.put(b"a", b"").unwrap();
    db.engine.put(b"b", b"").unwrap();
    db.engine.put(b"c", b"").unwrap();
    db.engine.put(b"d", b"").unwrap();
    db.engine.put(b"e", b"").unwrap();

    let mut wb = db.engine.write_batch();

    wb.delete_range(b"b", b"e").unwrap();
    wb.delete_range(b"b", b"e").unwrap();
    wb.write().unwrap();

    assert!(db.engine.get_value(b"a").unwrap().is_some());
    assert!(db.engine.get_value(b"b").unwrap().is_none());
    assert!(db.engine.get_value(b"c").unwrap().is_none());
    assert!(db.engine.get_value(b"d").unwrap().is_none());
    assert!(db.engine.get_value(b"e").unwrap().is_some());

    let db = multi_batch_write_engine();

    db.engine.put(b"a", b"").unwrap();
    db.engine.put(b"b", b"").unwrap();
    db.engine.put(b"c", b"").unwrap();
    db.engine.put(b"d", b"").unwrap();
    db.engine.put(b"e", b"").unwrap();
    for i in 0..256_usize {
        let x = i.to_be_bytes();
        db.engine.put(&x, &x).unwrap();
    }

    let mut wb = db.engine.write_batch_with_cap(1024);

    wb.delete_range(b"b", b"e").unwrap();
    wb.delete_range(b"b", b"e").unwrap();
    wb.delete_range(&1_usize.to_be_bytes(), &256_usize.to_be_bytes())
        .unwrap();
    wb.delete_range(&1_usize.to_be_bytes(), &256_usize.to_be_bytes())
        .unwrap();
    wb.write().unwrap();

    assert!(db.engine.get_value(b"a").unwrap().is_some());
    assert!(db.engine.get_value(b"b").unwrap().is_none());
    assert!(db.engine.get_value(b"c").unwrap().is_none());
    assert!(db.engine.get_value(b"d").unwrap().is_none());
    assert!(db.engine.get_value(b"e").unwrap().is_some());
    assert!(
        db.engine
            .get_value(&0_usize.to_be_bytes())
            .unwrap()
            .is_some()
    );
    for i in 1..256_usize {
        assert!(db.engine.get_value(&i.to_be_bytes()).unwrap().is_none());
    }
}

#[test]
fn write_batch_delete_range_twice_2() {
    let db = default_engine();

    db.engine.put(b"a", b"").unwrap();
    db.engine.put(b"b", b"").unwrap();
    db.engine.put(b"c", b"").unwrap();
    db.engine.put(b"d", b"").unwrap();
    db.engine.put(b"e", b"").unwrap();

    let mut wb = db.engine.write_batch();

    wb.delete_range(b"b", b"e").unwrap();
    wb.write().unwrap();
    db.engine.put(b"c", b"").unwrap();
    wb.delete_range(b"b", b"e").unwrap();
    wb.write().unwrap();

    assert!(db.engine.get_value(b"a").unwrap().is_some());
    assert!(db.engine.get_value(b"b").unwrap().is_none());
    assert!(db.engine.get_value(b"c").unwrap().is_none());
    assert!(db.engine.get_value(b"d").unwrap().is_none());
    assert!(db.engine.get_value(b"e").unwrap().is_some());

    let db = multi_batch_write_engine();

    db.engine.put(b"a", b"").unwrap();
    db.engine.put(b"b", b"").unwrap();
    db.engine.put(b"c", b"").unwrap();
    db.engine.put(b"d", b"").unwrap();
    db.engine.put(b"e", b"").unwrap();
    for i in 0..256_usize {
        let x = i.to_be_bytes();
        db.engine.put(&x, &x).unwrap();
    }

    let mut wb = db.engine.write_batch_with_cap(1024);

    wb.delete_range(b"b", b"e").unwrap();
    wb.delete_range(&1_usize.to_be_bytes(), &256_usize.to_be_bytes())
        .unwrap();
    wb.write().unwrap();
    db.engine.put(b"c", b"").unwrap();
    for i in 64..128_usize {
        let x = i.to_be_bytes();
        db.engine.put(&x, &x).unwrap();
    }
    wb.delete_range(b"b", b"e").unwrap();
    wb.delete_range(&1_usize.to_be_bytes(), &256_usize.to_be_bytes())
        .unwrap();
    wb.write().unwrap();

    assert!(db.engine.get_value(b"a").unwrap().is_some());
    assert!(db.engine.get_value(b"b").unwrap().is_none());
    assert!(db.engine.get_value(b"c").unwrap().is_none());
    assert!(db.engine.get_value(b"d").unwrap().is_none());
    assert!(db.engine.get_value(b"e").unwrap().is_some());
    assert!(
        db.engine
            .get_value(&0_usize.to_be_bytes())
            .unwrap()
            .is_some()
    );
    for i in 1..256_usize {
        assert!(db.engine.get_value(&i.to_be_bytes()).unwrap().is_none());
    }
}

#[test]
fn write_batch_delete_range_empty_range() {
    let db = default_engine();

    db.engine.put(b"a", b"").unwrap();
    db.engine.put(b"b", b"").unwrap();
    db.engine.put(b"c", b"").unwrap();

    let mut wb = db.engine.write_batch();

    wb.delete_range(b"b", b"b").unwrap();
    wb.write().unwrap();

    assert!(db.engine.get_value(b"a").unwrap().is_some());
    assert!(db.engine.get_value(b"b").unwrap().is_some());
    assert!(db.engine.get_value(b"c").unwrap().is_some());

    let db = multi_batch_write_engine();

    db.engine.put(b"a", b"").unwrap();
    db.engine.put(b"b", b"").unwrap();
    db.engine.put(b"c", b"").unwrap();
    for i in 0..256_usize {
        let x = i.to_be_bytes();
        db.engine.put(&x, &x).unwrap();
    }

    let mut wb = db.engine.write_batch_with_cap(1024);

    wb.delete_range(b"b", b"b").unwrap();
    wb.delete_range(&1_usize.to_be_bytes(), &1_usize.to_be_bytes())
        .unwrap();
    wb.write().unwrap();

    assert!(db.engine.get_value(b"a").unwrap().is_some());
    assert!(db.engine.get_value(b"b").unwrap().is_some());
    assert!(db.engine.get_value(b"c").unwrap().is_some());
    for i in 0..256_usize {
        assert!(db.engine.get_value(&i.to_be_bytes()).unwrap().is_some());
    }
}

#[test]
fn write_batch_delete_range_backward_range() {
    let db = default_engine();

    db.engine.put(b"a", b"").unwrap();
    db.engine.put(b"b", b"").unwrap();
    db.engine.put(b"c", b"").unwrap();

    let mut wb = db.engine.write_batch();

    wb.delete_range(b"c", b"a").unwrap();
    recover_safe(|| {
        wb.write().unwrap();
    })
    .unwrap_err();

    assert!(db.engine.get_value(b"a").unwrap().is_some());
    assert!(db.engine.get_value(b"b").unwrap().is_some());
    assert!(db.engine.get_value(b"c").unwrap().is_some());

    let db = multi_batch_write_engine();

    db.engine.put(b"a", b"").unwrap();
    db.engine.put(b"b", b"").unwrap();
    db.engine.put(b"c", b"").unwrap();

    for i in 0..256_usize {
        let x = i.to_be_bytes();
        db.engine.put(&x, &x).unwrap();
    }

    let mut wb = db.engine.write_batch_with_cap(1024);

    wb.delete_range(b"c", b"a").unwrap();
    wb.delete_range(&256_usize.to_be_bytes(), &0_usize.to_be_bytes())
        .unwrap();

    recover_safe(|| {
        wb.write().unwrap();
    })
    .unwrap_err();

    assert!(db.engine.get_value(b"a").unwrap().is_some());
    assert!(db.engine.get_value(b"b").unwrap().is_some());
    assert!(db.engine.get_value(b"c").unwrap().is_some());
    for i in 0..256_usize {
        assert!(db.engine.get_value(&i.to_be_bytes()).unwrap().is_some());
    }
}

#[test]
#[ignore] // see comment in test
fn write_batch_delete_range_backward_range_partial_commit() {
    let db = default_engine();

    db.engine.put(b"a", b"").unwrap();
    db.engine.put(b"b", b"").unwrap();
    db.engine.put(b"c", b"").unwrap();
    db.engine.put(b"d", b"").unwrap();

    let mut wb = db.engine.write_batch();

    // Everything in the write batch before the panic
    // due to bad range is going to end up committed.
    //
    // NB: This behavior seems pretty questionable and
    // should probably be re-evaluated before other engines
    // try to emulate it.
    //
    // A more reasonable solution might be to have a bogus
    // delete_range request immediately panic.
    wb.put(b"e", b"").unwrap();
    wb.delete(b"d").unwrap();
    wb.delete_range(b"c", b"a").unwrap();
    wb.put(b"f", b"").unwrap();
    wb.delete(b"a").unwrap();

    recover_safe(|| {
        wb.write().unwrap();
    })
    .unwrap_err();

    assert!(db.engine.get_value(b"a").unwrap().is_some());
    assert!(db.engine.get_value(b"b").unwrap().is_some());
    assert!(db.engine.get_value(b"c").unwrap().is_some());
    assert!(db.engine.get_value(b"d").unwrap().is_none());
    assert!(db.engine.get_value(b"e").unwrap().is_some());
    assert!(db.engine.get_value(b"f").unwrap().is_none());

    let db = multi_batch_write_engine();

    db.engine.put(b"a", b"").unwrap();
    db.engine.put(b"b", b"").unwrap();
    db.engine.put(b"c", b"").unwrap();
    db.engine.put(b"d", b"").unwrap();
    for i in 0..256_usize {
        let x = i.to_be_bytes();
        db.engine.put(&x, &x).unwrap();
    }

    let mut wb = db.engine.write_batch_with_cap(1024);

    // Everything in the write batch before the panic
    // due to bad range is going to end up committed.
    //
    // NB: This behavior seems pretty questionable and
    // should probably be re-evaluated before other engines
    // try to emulate it.
    //
    // A more reasonable solution might be to have a bogus
    // delete_range request immediately panic.
    wb.put(b"e", b"").unwrap();
    wb.delete(b"d").unwrap();
    wb.delete_range(b"c", b"a").unwrap();
    wb.put(b"f", b"").unwrap();
    wb.delete(b"a").unwrap();
    wb.delete_range(&128_usize.to_be_bytes(), &64_usize.to_be_bytes())
        .unwrap();
    wb.put(&256_usize.to_be_bytes(), b"").unwrap();
    for i in 0..64_usize {
        wb.delete(&i.to_be_bytes()).unwrap();
    }

    recover_safe(|| {
        wb.write().unwrap();
    })
    .unwrap_err();

    assert!(db.engine.get_value(b"a").unwrap().is_some());
    assert!(db.engine.get_value(b"b").unwrap().is_some());
    assert!(db.engine.get_value(b"c").unwrap().is_some());
    assert!(db.engine.get_value(b"d").unwrap().is_none());
    assert!(db.engine.get_value(b"e").unwrap().is_some());
    assert!(db.engine.get_value(b"f").unwrap().is_none());
}

#[test]
fn write_batch_is_empty() {
    let db = default_engine();
    let mut wb = db.engine.write_batch();

    assert!(wb.is_empty());
    wb.put(b"a", b"").unwrap();
    assert!(!wb.is_empty());
    wb.write().unwrap();
    assert!(!wb.is_empty());

    let db = multi_batch_write_engine();
    let mut wb = db.engine.write_batch_with_cap(1024);

    assert!(wb.is_empty());
    for i in 0..256_usize {
        let x = i.to_be_bytes();
        wb.put(&x, &x).unwrap();
    }
    assert!(!wb.is_empty());
    wb.write().unwrap();
    assert!(!wb.is_empty());
}

#[test]
fn write_batch_count() {
    let db = default_engine();
    let mut wb = db.engine.write_batch();

    assert_eq!(wb.count(), 0);
    wb.put(b"a", b"").unwrap();
    assert_eq!(wb.count(), 1);
    wb.write().unwrap();
    assert_eq!(wb.count(), 1);

    let db = multi_batch_write_engine();
    let mut wb = db.engine.write_batch_with_cap(1024);
    assert_eq!(wb.count(), 0);
    for i in 0..256_usize {
        let x = i.to_be_bytes();
        wb.put(&x, &x).unwrap();
    }
    assert_eq!(wb.count(), 256);
    wb.write().unwrap();
    assert_eq!(wb.count(), 256);
}

#[test]
fn write_batch_count_2() {
    let db = default_engine();
    let mut wb = db.engine.write_batch();

    assert_eq!(wb.count(), 0);
    wb.put(b"a", b"").unwrap();
    assert_eq!(wb.count(), 1);
    wb.delete(b"a").unwrap();
    assert_eq!(wb.count(), 2);
    wb.delete_range(b"a", b"b").unwrap();
    assert_eq!(wb.count(), 3);
    wb.write().unwrap();
    assert_eq!(wb.count(), 3);

    let db = multi_batch_write_engine();
    let mut wb = db.engine.write_batch_with_cap(1024);

    assert_eq!(wb.count(), 0);
    for i in 0..256_usize {
        let x = i.to_be_bytes();
        wb.put(&x, &x).unwrap();
    }
    wb.put(b"a", b"").unwrap();
    assert_eq!(wb.count(), 257);
    wb.delete(b"a").unwrap();
    assert_eq!(wb.count(), 258);
    wb.delete_range(b"a", b"b").unwrap();
    assert_eq!(wb.count(), 259);
    wb.write().unwrap();
    assert_eq!(wb.count(), 259);
}

#[test]
fn write_batch_clear() {
    let db = default_engine();
    let mut wb = db.engine.write_batch();

    wb.put(b"a", b"").unwrap();
    wb.put(b"b", b"").unwrap();
    wb.clear();
    assert!(wb.is_empty());
    assert_eq!(wb.count(), 0);
    wb.write().unwrap();
    assert!(db.engine.get_value(b"a").unwrap().is_none());

    let db = multi_batch_write_engine();
    let mut wb = db.engine.write_batch_with_cap(1024);

    for i in 0..256_usize {
        let x = i.to_be_bytes();
        wb.put(&x, &x).unwrap();
    }
    wb.clear();
    assert!(wb.is_empty());
    assert_eq!(wb.count(), 0);
    wb.write().unwrap();
    for i in 0..256_usize {
        assert!(db.engine.get_value(&i.to_be_bytes()).unwrap().is_none());
    }
}

#[test]
fn cap_zero() {
    let db = default_engine();
    let mut wb = db.engine.write_batch_with_cap(0);
    wb.put(b"a", b"").unwrap();
    wb.put(b"b", b"").unwrap();
    wb.put(b"c", b"").unwrap();
    wb.put(b"d", b"").unwrap();
    wb.put(b"e", b"").unwrap();
    wb.put(b"f", b"").unwrap();
    wb.write().unwrap();
    assert!(db.engine.get_value(b"a").unwrap().is_some());
    assert!(db.engine.get_value(b"f").unwrap().is_some());

    let db = multi_batch_write_engine();
    let mut wb = db.engine.write_batch_with_cap(0);
    for i in 0..256_usize {
        let x = i.to_be_bytes();
        wb.put(&x, &x).unwrap();
    }
    wb.put(b"a", b"").unwrap();
    wb.put(b"b", b"").unwrap();
    wb.put(b"c", b"").unwrap();
    wb.put(b"d", b"").unwrap();
    wb.put(b"e", b"").unwrap();
    wb.put(b"f", b"").unwrap();
    wb.write().unwrap();
    assert!(
        db.engine
            .get_value(&0_usize.to_be_bytes())
            .unwrap()
            .is_some()
    );
    assert!(
        db.engine
            .get_value(&123_usize.to_be_bytes())
            .unwrap()
            .is_some()
    );
    assert!(
        db.engine
            .get_value(&255_usize.to_be_bytes())
            .unwrap()
            .is_some()
    );
    assert!(db.engine.get_value(b"a").unwrap().is_some());
    assert!(db.engine.get_value(b"f").unwrap().is_some());
}

/// Write batch capacity seems to just be a suggestions
#[test]
fn cap_two() {
    let db = default_engine();
    let mut wb = db.engine.write_batch_with_cap(2);
    wb.put(b"a", b"").unwrap();
    wb.put(b"b", b"").unwrap();
    wb.put(b"c", b"").unwrap();
    wb.put(b"d", b"").unwrap();
    wb.put(b"e", b"").unwrap();
    wb.put(b"f", b"").unwrap();
    wb.write().unwrap();
    assert!(db.engine.get_value(b"a").unwrap().is_some());
    assert!(db.engine.get_value(b"f").unwrap().is_some());

    let db = multi_batch_write_engine();
    let mut wb = db.engine.write_batch_with_cap(2);

    for i in 0..256_usize {
        let x = i.to_be_bytes();
        wb.put(&x, &x).unwrap();
    }
    wb.put(b"a", b"").unwrap();
    wb.put(b"b", b"").unwrap();
    wb.put(b"c", b"").unwrap();
    wb.put(b"d", b"").unwrap();
    wb.put(b"e", b"").unwrap();
    wb.put(b"f", b"").unwrap();
    wb.write().unwrap();
    assert!(
        db.engine
            .get_value(&0_usize.to_be_bytes())
            .unwrap()
            .is_some()
    );
    assert!(
        db.engine
            .get_value(&123_usize.to_be_bytes())
            .unwrap()
            .is_some()
    );
    assert!(
        db.engine
            .get_value(&255_usize.to_be_bytes())
            .unwrap()
            .is_some()
    );
    assert!(db.engine.get_value(b"a").unwrap().is_some());
    assert!(db.engine.get_value(b"f").unwrap().is_some());
}

// We should write when count is greater than WRITE_BATCH_MAX_KEYS
#[test]
fn should_write_to_engine() {
    let db = default_engine();
    let mut wb = db.engine.write_batch();
    let max_keys = KvTestEngine::WRITE_BATCH_MAX_KEYS;

    let mut key = vec![];
    loop {
        key.push(b'a');
        wb.put(&key, b"").unwrap();
        if key.len() <= max_keys {
            assert!(!wb.should_write_to_engine());
        }
        if key.len() == max_keys + 1 {
            assert!(wb.should_write_to_engine());
            wb.write().unwrap();
            break;
        }
    }

    let db = multi_batch_write_engine();
    let mut wb = db.engine.write_batch_with_cap(1024);
    let max_keys = KvTestEngine::WRITE_BATCH_MAX_KEYS;

    let mut key = vec![];
    loop {
        key.push(b'a');
        wb.put(&key, b"").unwrap();
        if key.len() <= max_keys {
            assert!(!wb.should_write_to_engine());
        }
        if key.len() == max_keys + 1 {
            assert!(wb.should_write_to_engine());
            wb.write().unwrap();
            break;
        }
    }
}

// But there kind of aren't consequences for making huge write batches
#[test]
fn should_write_to_engine_but_whatever() {
    let db = default_engine();
    let mut wb = db.engine.write_batch();
    let max_keys = KvTestEngine::WRITE_BATCH_MAX_KEYS;

    let mut key = vec![];
    loop {
        key.push(b'a');
        wb.put(&key, b"").unwrap();
        if key.len() <= max_keys {
            assert!(!wb.should_write_to_engine());
        }
        if key.len() > max_keys {
            assert!(wb.should_write_to_engine());
        }
        if key.len() == max_keys * 2 {
            assert!(wb.should_write_to_engine());
            wb.write().unwrap();
            break;
        }
    }

    let mut key = vec![];
    loop {
        key.push(b'a');
        assert!(db.engine.get_value(&key).unwrap().is_some());
        if key.len() == max_keys * 2 {
            break;
        }
    }

    let db = multi_batch_write_engine();
    let mut wb = db.engine.write_batch_with_cap(1024);
    let max_keys = KvTestEngine::WRITE_BATCH_MAX_KEYS;

    let mut key = vec![];

    loop {
        key.push(b'a');
        wb.put(&key, b"").unwrap();
        if key.len() <= max_keys {
            assert!(!wb.should_write_to_engine());
        }
        if key.len() > max_keys {
            assert!(wb.should_write_to_engine());
        }
        if key.len() == max_keys * 2 {
            assert!(wb.should_write_to_engine());
            wb.write().unwrap();
            break;
        }
    }

    let mut key = vec![];
    loop {
        key.push(b'a');
        assert!(db.engine.get_value(&key).unwrap().is_some());
        if key.len() == max_keys * 2 {
            break;
        }
    }
}

#[test]
fn data_size() {
    let db = default_engine();
    let mut wb = db.engine.write_batch();

    let size1 = wb.data_size();
    wb.put(b"a", b"").unwrap();
    let size2 = wb.data_size();
    assert!(size1 < size2);
    wb.write().unwrap();
    let size3 = wb.data_size();
    assert_eq!(size2, size3);
    wb.clear();
    let size4 = wb.data_size();
    assert_eq!(size4, size1);
    wb.put(b"a", b"").unwrap();
    let size5 = wb.data_size();
    assert!(size4 < size5);
    wb.delete(b"a").unwrap();
    let size6 = wb.data_size();
    assert!(size5 < size6);
    wb.delete_range(b"a", b"b").unwrap();
    let size7 = wb.data_size();
    assert!(size6 < size7);
    wb.clear();
    let size8 = wb.data_size();
    assert_eq!(size8, size1);

    let db = multi_batch_write_engine();
    let mut wb = db.engine.write_batch_with_cap(1024);
    let max_keys = 256_usize;

    let size1 = wb.data_size();
    for i in 0..max_keys {
        let x = i.to_be_bytes();
        wb.put(&x, &x).unwrap();
    }
    let size2 = wb.data_size();
    assert!(size1 < size2);
    wb.write().unwrap();
    let size3 = wb.data_size();
    assert_eq!(size2, size3);
    wb.clear();
    let size4 = wb.data_size();
    assert_eq!(size4, size1);
    for i in 0..max_keys {
        let x = i.to_be_bytes();
        wb.put(&x, &x).unwrap();
    }
    let size5 = wb.data_size();
    assert!(size4 < size5);
    for i in 0..max_keys {
        let x = i.to_be_bytes();
        wb.delete(&x).unwrap();
    }
    let size6 = wb.data_size();
    assert!(size5 < size6);
    wb.delete_range(&0_usize.to_be_bytes(), &(max_keys * 2).to_be_bytes())
        .unwrap();
    let size7 = wb.data_size();
    assert!(size6 < size7);
    wb.clear();
    let size8 = wb.data_size();
    assert_eq!(size8, size1);
}

#[test]
fn save_point_rollback_none() {
    let db = default_engine();
    let mut wb = db.engine.write_batch();

    let err = wb.rollback_to_save_point();
    assert_engine_error(err);

    let db = multi_batch_write_engine();
    let mut wb = db.engine.write_batch_with_cap(1024);

    let err = wb.rollback_to_save_point();
    assert_engine_error(err);
}

#[test]
fn save_point_pop_none() {
    let db = default_engine();
    let mut wb = db.engine.write_batch();

    let err = wb.rollback_to_save_point();
    assert_engine_error(err);

    let db = multi_batch_write_engine();
    let mut wb = db.engine.write_batch_with_cap(1024);

    let err = wb.rollback_to_save_point();
    assert_engine_error(err);
}

#[test]
fn save_point_rollback_one() {
    let db = default_engine();
    let mut wb = db.engine.write_batch();

    wb.set_save_point();
    wb.put(b"a", b"").unwrap();

    wb.rollback_to_save_point().unwrap();

    let err = wb.rollback_to_save_point();
    assert_engine_error(err);
    let err = wb.pop_save_point();
    assert_engine_error(err);
    wb.write().unwrap();
    let val = db.engine.get_value(b"a").unwrap();
    assert!(val.is_none());

    let db = multi_batch_write_engine();
    let mut wb = db.engine.write_batch_with_cap(1024);

    wb.set_save_point();
    for i in 0..256_usize {
        let x = i.to_be_bytes();
        wb.put(&x, &x).unwrap();
    }
    wb.put(b"a", b"").unwrap();

    wb.rollback_to_save_point().unwrap();

    let err = wb.rollback_to_save_point();
    assert_engine_error(err);
    let err = wb.pop_save_point();
    assert_engine_error(err);
    wb.write().unwrap();
    for i in 0..256_usize {
        assert!(db.engine.get_value(&i.to_be_bytes()).unwrap().is_none());
    }
    let val = db.engine.get_value(b"a").unwrap();
    assert!(val.is_none());
}

#[test]
fn save_point_rollback_two() {
    let db = default_engine();
    let mut wb = db.engine.write_batch();

    wb.set_save_point();
    wb.put(b"a", b"").unwrap();
    wb.set_save_point();
    wb.put(b"b", b"").unwrap();

    wb.rollback_to_save_point().unwrap();
    wb.rollback_to_save_point().unwrap();

    let err = wb.rollback_to_save_point();
    assert_engine_error(err);
    let err = wb.pop_save_point();
    assert_engine_error(err);
    wb.write().unwrap();
    let a = db.engine.get_value(b"a").unwrap();
    assert!(a.is_none());
    let b = db.engine.get_value(b"b").unwrap();
    assert!(b.is_none());

    let db = multi_batch_write_engine();
    let mut wb = db.engine.write_batch_with_cap(1024);
    let max_keys = 256_usize;

    wb.set_save_point();
    for i in 0..max_keys {
        let x = i.to_be_bytes();
        wb.put(&x, &x).unwrap();
    }
    wb.put(b"a", b"").unwrap();
    wb.set_save_point();
    for i in max_keys..2 * max_keys {
        let x = i.to_be_bytes();
        wb.put(&x, &x).unwrap();
    }
    wb.put(b"b", b"").unwrap();

    wb.rollback_to_save_point().unwrap();
    wb.rollback_to_save_point().unwrap();

    let err = wb.rollback_to_save_point();
    assert_engine_error(err);
    let err = wb.pop_save_point();
    assert_engine_error(err);
    wb.write().unwrap();
    let a = db.engine.get_value(b"a").unwrap();
    assert!(a.is_none());
    let b = db.engine.get_value(b"b").unwrap();
    assert!(b.is_none());
    for i in 0..2 * max_keys {
        assert!(db.engine.get_value(&i.to_be_bytes()).unwrap().is_none());
    }
}

#[test]
fn save_point_rollback_partial() {
    let db = default_engine();
    let mut wb = db.engine.write_batch();

    wb.put(b"a", b"").unwrap();
    wb.set_save_point();
    wb.put(b"b", b"").unwrap();

    wb.rollback_to_save_point().unwrap();
    wb.write().unwrap();
    let a = db.engine.get_value(b"a").unwrap();
    assert!(a.is_some());
    let b = db.engine.get_value(b"b").unwrap();
    assert!(b.is_none());

    let db = multi_batch_write_engine();
    let mut wb = db.engine.write_batch_with_cap(1024);
    let max_keys = 256_usize;

    for i in 0..max_keys {
        let x = i.to_be_bytes();
        wb.put(&x, &x).unwrap();
    }
    wb.put(b"a", b"").unwrap();
    wb.set_save_point();
    wb.put(b"b", b"").unwrap();
    for i in max_keys..2 * max_keys {
        let x = i.to_be_bytes();
        wb.put(&x, &x).unwrap();
    }

    wb.rollback_to_save_point().unwrap();
    wb.write().unwrap();
    let a = db.engine.get_value(b"a").unwrap();
    assert!(a.is_some());
    for i in 0..max_keys {
        assert!(db.engine.get_value(&i.to_be_bytes()).unwrap().is_some());
    }
    let b = db.engine.get_value(b"b").unwrap();
    assert!(b.is_none());
    for i in max_keys..2 * max_keys {
        assert!(db.engine.get_value(&i.to_be_bytes()).unwrap().is_none());
    }
}

#[test]
fn save_point_pop_rollback() {
    let db = default_engine();
    let mut wb = db.engine.write_batch();

    wb.set_save_point();
    wb.put(b"a", b"").unwrap();
    wb.set_save_point();
    wb.put(b"a", b"").unwrap();

    wb.pop_save_point().unwrap();
    wb.rollback_to_save_point().unwrap();

    let err = wb.rollback_to_save_point();
    assert_engine_error(err);
    let err = wb.pop_save_point();
    assert_engine_error(err);
    wb.write().unwrap();
    let val = db.engine.get_value(b"a").unwrap();
    assert!(val.is_none());
    let val = db.engine.get_value(b"b").unwrap();
    assert!(val.is_none());

    let db = multi_batch_write_engine();
    let mut wb = db.engine.write_batch_with_cap(1024);

    wb.set_save_point();
    for i in 0..256_usize {
        let x = i.to_be_bytes();
        wb.put(&x, &x).unwrap();
    }
    wb.put(b"a", b"").unwrap();
    wb.set_save_point();
    for i in 0..256_usize {
        let x = i.to_be_bytes();
        wb.put(&x, &x).unwrap();
    }
    wb.put(b"a", b"").unwrap();

    wb.pop_save_point().unwrap();
    wb.rollback_to_save_point().unwrap();

    let err = wb.rollback_to_save_point();
    assert_engine_error(err);
    let err = wb.pop_save_point();
    assert_engine_error(err);
    wb.write().unwrap();
    let val = db.engine.get_value(b"a").unwrap();
    assert!(val.is_none());
    let val = db.engine.get_value(b"b").unwrap();
    assert!(val.is_none());
    for i in 0..512_usize {
        assert!(db.engine.get_value(&i.to_be_bytes()).unwrap().is_none());
    }
}

#[test]
fn save_point_rollback_after_write() {
    let db = default_engine();
    let mut wb = db.engine.write_batch();

    wb.set_save_point();
    wb.put(b"a", b"").unwrap();

    wb.write().unwrap();

    let val = db.engine.get_value(b"a").unwrap();
    assert!(val.is_some());

    db.engine.delete(b"a").unwrap();

    let val = db.engine.get_value(b"a").unwrap();
    assert!(val.is_none());

    wb.rollback_to_save_point().unwrap();
    wb.write().unwrap();

    let val = db.engine.get_value(b"a").unwrap();
    assert!(val.is_none());

    let db = multi_batch_write_engine();
    let mut wb = db.engine.write_batch_with_cap(1024);
    let max_keys = 256_usize;

    wb.set_save_point();
    for i in 0..max_keys {
        wb.put(&i.to_be_bytes(), b"").unwrap();
    }
    wb.put(b"a", b"").unwrap();

    wb.write().unwrap();

    assert!(db.engine.get_value(b"a").unwrap().is_some());
    for i in 0..max_keys {
        assert!(db.engine.get_value(&i.to_be_bytes()).unwrap().is_some());
    }

    db.engine.delete(b"a").unwrap();
    for i in 0..max_keys {
        db.engine.delete(&i.to_be_bytes()).unwrap();
    }

    assert!(db.engine.get_value(b"a").unwrap().is_none());
    for i in 0..max_keys {
        assert!(db.engine.get_value(&i.to_be_bytes()).unwrap().is_none());
    }

    wb.rollback_to_save_point().unwrap();
    wb.write().unwrap();

    assert!(db.engine.get_value(b"a").unwrap().is_none());
    for i in 0..max_keys {
        assert!(db.engine.get_value(&i.to_be_bytes()).unwrap().is_none());
    }
}

#[test]
fn save_point_same_rollback_one() {
    let db = default_engine();
    let mut wb = db.engine.write_batch();

    wb.put(b"a", b"").unwrap();

    wb.set_save_point();
    wb.set_save_point();
    wb.set_save_point();

    wb.put(b"b", b"").unwrap();

    wb.rollback_to_save_point().unwrap();

    wb.write().unwrap();

    let a = db.engine.get_value(b"a").unwrap();
    let b = db.engine.get_value(b"b").unwrap();

    assert!(a.is_some());
    assert!(b.is_none());

    let db = multi_batch_write_engine();
    let mut wb = db.engine.write_batch_with_cap(1024);
    let max_keys = 256_usize;

    for i in 0..max_keys {
        wb.put(&i.to_be_bytes(), b"").unwrap();
    }
    wb.put(b"a", b"").unwrap();

    wb.set_save_point();
    wb.set_save_point();
    wb.set_save_point();

    wb.put(b"b", b"").unwrap();
    for i in max_keys..2 * max_keys {
        wb.put(&i.to_be_bytes(), b"").unwrap();
    }

    wb.rollback_to_save_point().unwrap();

    wb.write().unwrap();

    assert!(db.engine.get_value(b"a").unwrap().is_some());
    for i in 0..max_keys {
        assert!(db.engine.get_value(&i.to_be_bytes()).unwrap().is_some());
    }

    assert!(db.engine.get_value(b"b").unwrap().is_none());
    for i in max_keys..2 * max_keys {
        assert!(db.engine.get_value(&i.to_be_bytes()).unwrap().is_none());
    }
}

#[test]
fn save_point_same_rollback_all() {
    let db = default_engine();
    let mut wb = db.engine.write_batch();

    wb.put(b"a", b"").unwrap();

    wb.set_save_point();
    wb.set_save_point();
    wb.set_save_point();

    wb.put(b"b", b"").unwrap();

    wb.rollback_to_save_point().unwrap();
    wb.rollback_to_save_point().unwrap();
    wb.rollback_to_save_point().unwrap();

    assert_engine_error(wb.pop_save_point());
    assert_engine_error(wb.rollback_to_save_point());

    wb.write().unwrap();

    let a = db.engine.get_value(b"a").unwrap();
    let b = db.engine.get_value(b"b").unwrap();

    assert!(a.is_some());
    assert!(b.is_none());

    let db = multi_batch_write_engine();
    let mut wb = db.engine.write_batch_with_cap(1024);
    let max_keys = 256_usize;

    for i in 0..max_keys {
        wb.put(&i.to_be_bytes(), b"").unwrap();
    }
    wb.put(b"a", b"").unwrap();

    wb.set_save_point();
    wb.set_save_point();
    wb.set_save_point();

    wb.put(b"b", b"").unwrap();
    for i in 0..max_keys {
        wb.put(&i.to_be_bytes(), b"").unwrap();
    }

    wb.rollback_to_save_point().unwrap();
    wb.rollback_to_save_point().unwrap();
    wb.rollback_to_save_point().unwrap();

    assert_engine_error(wb.pop_save_point());
    assert_engine_error(wb.rollback_to_save_point());

    wb.write().unwrap();

    assert!(db.engine.get_value(b"a").unwrap().is_some());
    for i in 0..max_keys {
        assert!(db.engine.get_value(&i.to_be_bytes()).unwrap().is_some());
    }

    assert!(db.engine.get_value(b"b").unwrap().is_none());
    for i in max_keys..2 * max_keys {
        assert!(db.engine.get_value(&i.to_be_bytes()).unwrap().is_none());
    }
}

#[test]
fn save_point_pop_after_write() {
    let db = default_engine();
    let mut wb = db.engine.write_batch();

    wb.set_save_point();
    wb.put(b"a", b"").unwrap();

    wb.write().unwrap();

    let val = db.engine.get_value(b"a").unwrap();
    assert!(val.is_some());

    db.engine.delete(b"a").unwrap();

    let val = db.engine.get_value(b"a").unwrap();
    assert!(val.is_none());

    wb.pop_save_point().unwrap();
    wb.write().unwrap();

    let val = db.engine.get_value(b"a").unwrap();
    assert!(val.is_some());

    let db = multi_batch_write_engine();
    let mut wb = db.engine.write_batch_with_cap(1024);
    let max_keys = 256_usize;

    wb.set_save_point();
    wb.put(b"a", b"").unwrap();
    for i in 0..max_keys {
        wb.put(&i.to_be_bytes(), b"").unwrap();
    }

    wb.write().unwrap();

    assert!(db.engine.get_value(b"a").unwrap().is_some());
    for i in 0..max_keys {
        assert!(db.engine.get_value(&i.to_be_bytes()).unwrap().is_some());
    }

    db.engine.delete(b"a").unwrap();
    for i in 0..max_keys {
        db.engine.delete(&i.to_be_bytes()).unwrap();
    }

    assert!(db.engine.get_value(b"a").unwrap().is_none());
    for i in 0..max_keys {
        assert!(db.engine.get_value(&i.to_be_bytes()).unwrap().is_none());
    }

    wb.pop_save_point().unwrap();
    wb.write().unwrap();

    assert!(db.engine.get_value(b"a").unwrap().is_some());
    for i in 0..max_keys {
        assert!(db.engine.get_value(&i.to_be_bytes()).unwrap().is_some());
    }
}

#[test]
fn save_point_all_commands() {
    let db = default_engine();
    let mut wb = db.engine.write_batch();

    db.engine.put(b"a", b"").unwrap();
    db.engine.put(b"d", b"").unwrap();

    wb.set_save_point();
    wb.delete(b"a").unwrap();
    wb.put(b"b", b"").unwrap();
    wb.delete_range(b"c", b"e").unwrap();

    wb.rollback_to_save_point().unwrap();
    wb.write().unwrap();

    let a = db.engine.get_value(b"a").unwrap();
    let b = db.engine.get_value(b"b").unwrap();
    let d = db.engine.get_value(b"d").unwrap();
    assert!(a.is_some());
    assert!(b.is_none());
    assert!(d.is_some());

    let db = multi_batch_write_engine();
    let mut wb = db.engine.write_batch_with_cap(1024);
    let max_keys = 256_usize;

    for i in 0..max_keys / 2 {
        db.engine.put(&i.to_be_bytes(), b"").unwrap();
    }
    db.engine.put(b"a", b"").unwrap();
    for i in max_keys / 2..max_keys {
        db.engine.put(&i.to_be_bytes(), b"").unwrap();
    }
    db.engine.put(b"d", b"").unwrap();

    wb.set_save_point();
    for i in 0..max_keys / 2 {
        wb.delete(&i.to_be_bytes()).unwrap();
    }
    wb.delete(b"a").unwrap();
    wb.put(b"b", b"").unwrap();
    wb.delete_range(b"c", b"e").unwrap();
    wb.delete_range(&(max_keys / 3).to_be_bytes(), &(2 * max_keys).to_be_bytes())
        .unwrap();

    wb.rollback_to_save_point().unwrap();
    wb.write().unwrap();

    let a = db.engine.get_value(b"a").unwrap();
    let b = db.engine.get_value(b"b").unwrap();
    let d = db.engine.get_value(b"d").unwrap();
    for i in 0..max_keys {
        assert!(db.engine.get_value(&i.to_be_bytes()).unwrap().is_some());
    }
    assert!(a.is_some());
    assert!(b.is_none());
    assert!(d.is_some());
}

// What happens to the count() and is_empty() methods
// as we use push and pop save points?
#[test]
fn save_points_and_counts() {
    let db = default_engine();
    let mut wb = db.engine.write_batch();

    assert_eq!(wb.is_empty(), true);
    assert_eq!(wb.count(), 0);

    wb.set_save_point();

    assert_eq!(wb.is_empty(), true);
    assert_eq!(wb.count(), 0);

    wb.put(b"a", b"").unwrap();

    assert_eq!(wb.is_empty(), false);
    assert_eq!(wb.count(), 1);

    wb.rollback_to_save_point().unwrap();

    assert_eq!(wb.is_empty(), true);
    assert_eq!(wb.count(), 0);

    wb.set_save_point();

    assert_eq!(wb.is_empty(), true);
    assert_eq!(wb.count(), 0);

    wb.put(b"a", b"").unwrap();

    assert_eq!(wb.is_empty(), false);
    assert_eq!(wb.count(), 1);

    wb.pop_save_point().unwrap();

    assert_eq!(wb.is_empty(), false);
    assert_eq!(wb.count(), 1);

    wb.clear();

    assert_eq!(wb.is_empty(), true);
    assert_eq!(wb.count(), 0);

    wb.set_save_point();

    assert_eq!(wb.is_empty(), true);
    assert_eq!(wb.count(), 0);

    wb.put(b"a", b"").unwrap();

    assert_eq!(wb.is_empty(), false);
    assert_eq!(wb.count(), 1);

    wb.write().unwrap();

    assert_eq!(wb.is_empty(), false);
    assert_eq!(wb.count(), 1);

    wb.rollback_to_save_point().unwrap();

    assert_eq!(wb.is_empty(), true);
    assert_eq!(wb.count(), 0);

    wb.set_save_point();

    assert_eq!(wb.is_empty(), true);
    assert_eq!(wb.count(), 0);

    wb.put(b"a", b"").unwrap();

    assert_eq!(wb.is_empty(), false);
    assert_eq!(wb.count(), 1);

    wb.write().unwrap();

    assert_eq!(wb.is_empty(), false);
    assert_eq!(wb.count(), 1);

    wb.pop_save_point().unwrap();

    assert_eq!(wb.is_empty(), false);
    assert_eq!(wb.count(), 1);

    wb.clear();

    assert_eq!(wb.is_empty(), true);
    assert_eq!(wb.count(), 0);

    let db = multi_batch_write_engine();
    let mut wb = db.engine.write_batch_with_cap(1024);
    let max_keys = 256_usize;

    assert_eq!(wb.is_empty(), true);
    assert_eq!(wb.count(), 0);

    wb.set_save_point();

    assert_eq!(wb.is_empty(), true);
    assert_eq!(wb.count(), 0);

    for i in 0..max_keys {
        wb.put(&i.to_be_bytes(), b"").unwrap();
    }

    assert_eq!(wb.is_empty(), false);
    assert_eq!(wb.count(), max_keys);

    wb.rollback_to_save_point().unwrap();

    assert_eq!(wb.is_empty(), true);
    assert_eq!(wb.count(), 0);

    wb.set_save_point();

    assert_eq!(wb.is_empty(), true);
    assert_eq!(wb.count(), 0);

    for i in 0..max_keys {
        wb.put(&i.to_be_bytes(), b"").unwrap();
    }

    assert_eq!(wb.is_empty(), false);
    assert_eq!(wb.count(), max_keys);

    wb.pop_save_point().unwrap();

    assert_eq!(wb.is_empty(), false);
    assert_eq!(wb.count(), max_keys);

    wb.clear();

    assert_eq!(wb.is_empty(), true);
    assert_eq!(wb.count(), 0);

    wb.set_save_point();

    assert_eq!(wb.is_empty(), true);
    assert_eq!(wb.count(), 0);

    for i in 0..max_keys {
        wb.put(&i.to_be_bytes(), b"").unwrap();
    }

    assert_eq!(wb.is_empty(), false);
    assert_eq!(wb.count(), max_keys);

    wb.write().unwrap();

    assert_eq!(wb.is_empty(), false);
    assert_eq!(wb.count(), max_keys);

    wb.rollback_to_save_point().unwrap();

    assert_eq!(wb.is_empty(), true);
    assert_eq!(wb.count(), 0);

    wb.set_save_point();

    assert_eq!(wb.is_empty(), true);
    assert_eq!(wb.count(), 0);

    for i in 0..max_keys {
        wb.put(&i.to_be_bytes(), b"").unwrap();
    }

    assert_eq!(wb.is_empty(), false);
    assert_eq!(wb.count(), max_keys);

    wb.write().unwrap();

    assert_eq!(wb.is_empty(), false);
    assert_eq!(wb.count(), max_keys);

    wb.pop_save_point().unwrap();

    assert_eq!(wb.is_empty(), false);
    assert_eq!(wb.count(), max_keys);

    wb.clear();

    assert_eq!(wb.is_empty(), true);
    assert_eq!(wb.count(), 0);
}
