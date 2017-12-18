// Copyright 2017 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

use tempdir::TempDir;
use tikv::storage::ALL_CFS;
use tikv::util::rocksdb;
use rocksdb::Writable;

pub const NUM: u64 = 1000000;

pub fn make_key(key: u64) -> String {
    let key = format!("k{}", key);
    key
}

pub fn make_val(val: u64) -> String {
    let val = format!("v{}", val);
    val
}

#[test]
fn test_rocksdb_insert() {
    let path = TempDir::new("_rocksdb_insert").unwrap();
    let db = rocksdb::new_engine(path.path().to_str().unwrap(), ALL_CFS, None).unwrap();
    for cf in ALL_CFS {
        let cf_handle = db.cf_handle(cf).unwrap();
        for i in 0..NUM {
            db.put_cf(cf_handle, make_key(i).as_bytes(), make_val(i).as_bytes())
                .unwrap();
        }
    }
    for cf in ALL_CFS {
        let cf_handle = db.cf_handle(cf).unwrap();
        for i in 0..NUM {
            assert_eq!(
                db.get_cf(cf_handle, make_key(i).as_bytes())
                    .unwrap()
                    .unwrap(),
                make_val(i).as_bytes()
            );
        }
    }
}

#[test]
fn test_rocksdb_update() {
    let path = TempDir::new("_rocksdb_update").unwrap();
    let db = rocksdb::new_engine(path.path().to_str().unwrap(), ALL_CFS, None).unwrap();
    for cf in ALL_CFS {
        let cf_handle = rocksdb::get_cf_handle(&db, cf).unwrap();
        for i in 0..NUM {
            db.put_cf(cf_handle, make_key(i).as_bytes(), make_val(i).as_bytes())
                .unwrap();
        }
    }
    for cf in ALL_CFS {
        let cf_handle = rocksdb::get_cf_handle(&db, cf).unwrap();
        for i in 0..NUM {
            db.put_cf(
                cf_handle,
                make_key(i).as_bytes(),
                make_val(i + NUM).as_bytes(),
            ).unwrap();
        }
    }
    for cf in ALL_CFS {
        let cf_handle = db.cf_handle(cf).unwrap();
        for i in 0..NUM {
            assert_eq!(
                db.get_cf(cf_handle, make_key(i).as_bytes())
                    .unwrap()
                    .unwrap(),
                make_val(i + NUM).as_bytes()
            );
        }
    }
}

#[test]
fn test_rocksdb_delete() {
    let path = TempDir::new("_rocksdb_delete").unwrap();
    let db = rocksdb::new_engine(path.path().to_str().unwrap(), ALL_CFS, None).unwrap();
    for cf in ALL_CFS {
        let cf_handle = rocksdb::get_cf_handle(&db, cf).unwrap();
        for i in 0..NUM {
            db.put_cf(cf_handle, make_key(i).as_bytes(), make_val(i).as_bytes())
                .unwrap();
        }
    }
    for cf in ALL_CFS {
        let cf_handle = rocksdb::get_cf_handle(&db, cf).unwrap();
        for i in 0..(NUM / 2) {
            db.delete_cf(cf_handle, make_key(i).as_bytes()).unwrap();
        }
    }
    for cf in ALL_CFS {
        let cf_handle = db.cf_handle(cf).unwrap();
        for i in 0..(NUM / 2) {
            assert!(
                db.get_cf(cf_handle, make_key(i).as_bytes())
                    .unwrap()
                    .is_none()
            );
        }
        for i in (NUM / 2)..NUM {
            assert_eq!(
                db.get_cf(cf_handle, make_key(i).as_bytes())
                    .unwrap()
                    .unwrap(),
                make_val(i).as_bytes()
            );
        }
    }
}
