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

fn make_key(key: u64) -> String {
    let key = format!("k{}", key);
    key
}

fn make_val(val: u64) -> String {
    let val = format!("v{}", val);
    val
}

#[test]
fn test_rocksdb_insert() {
    let path = TempDir::new("_open_rocksdb").unwrap();
    let db = rocksdb::new_engine(path.path().to_str().unwrap(), ALL_CFS, None).unwrap();
    let num = 100;
    for cf in ALL_CFS {
        let cf_handle = db.cf_handle(cf).unwrap();
        for i in 0..num {
            db.put_cf(cf_handle, make_key(i).as_bytes(), make_val(i).as_bytes())
                .unwrap();
        }
    }
    for cf in ALL_CFS {
        let cf_handle = db.cf_handle(cf).unwrap();
        for i in 0..num {
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
    let path = TempDir::new("_open_rocksdb").unwrap();
    let db = rocksdb::new_engine(path.path().to_str().unwrap(), ALL_CFS, None).unwrap();
    let num = 100;
    for cf in ALL_CFS {
        let cf_handle = rocksdb::get_cf_handle(&db, cf).unwrap();
        for i in 0..num {
            db.put_cf(cf_handle, make_key(i).as_bytes(), make_val(i).as_bytes())
                .unwrap();
        }
    }
    for cf in ALL_CFS {
        let cf_handle = rocksdb::get_cf_handle(&db, cf).unwrap();
        for i in 0..num {
            db.put_cf(
                cf_handle,
                make_key(i).as_bytes(),
                make_val(i + num).as_bytes(),
            ).unwrap();
        }
    }
    for cf in ALL_CFS {
        let cf_handle = db.cf_handle(cf).unwrap();
        for i in 0..num {
            assert_eq!(
                db.get_cf(cf_handle, make_key(i).as_bytes())
                    .unwrap()
                    .unwrap(),
                make_val(i + num).as_bytes()
            );
        }
    }
}

#[test]
fn test_rocksdb_delete() {
    let path = TempDir::new("_open_rocksdb").unwrap();
    let db = rocksdb::new_engine(path.path().to_str().unwrap(), ALL_CFS, None).unwrap();
    let num = 100;
    for cf in ALL_CFS {
        let cf_handle = rocksdb::get_cf_handle(&db, cf).unwrap();
        for i in 0..num {
            db.put_cf(cf_handle, make_key(i).as_bytes(), make_val(i).as_bytes())
                .unwrap();
        }
    }
    for cf in ALL_CFS {
        let cf_handle = rocksdb::get_cf_handle(&db, cf).unwrap();
        for i in 0..(num / 2) {
            db.delete_cf(cf_handle, make_key(i).as_bytes()).unwrap();
        }
    }
    for cf in ALL_CFS {
        let cf_handle = db.cf_handle(cf).unwrap();
        for i in 0..(num / 2) {
            assert!(
                db.get_cf(cf_handle, make_key(i).as_bytes())
                    .unwrap()
                    .is_none()
            );
        }
        for i in (num / 2)..num {
            assert_eq!(
                db.get_cf(cf_handle, make_key(i).as_bytes())
                    .unwrap()
                    .unwrap(),
                make_val(i).as_bytes()
            );
        }
    }
}
