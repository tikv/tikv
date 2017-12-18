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

use std::fs;
use tempdir::TempDir;
use tikv::storage::ALL_CFS;
use tikv::util::rocksdb;
use rocksdb::{CFHandle, ColumnFamilyOptions, EnvOptions, SstFileWriter, Writable, DB};

pub const NUM: u64 = 100000;

fn make_key(key: u64) -> String {
    let key = format!("k{}", key);
    key
}

fn make_val(val: u64) -> String {
    let val = format!("v{}", val);
    val
}

pub fn do_insert(db: &DB, k_start: u64, k_end: u64, v_start: u64) {
    let mut v = v_start;
    for cf in ALL_CFS {
        let cf_handle = db.cf_handle(cf).unwrap();
        for i in k_start..k_end {
            db.put_cf(cf_handle, make_key(i).as_bytes(), make_val(v).as_bytes())
                .unwrap();
        }
        v += 1;
    }
}

pub fn do_delete(db: &DB, k_start: u64, k_end: u64) {
    for cf in ALL_CFS {
        let cf_handle = db.cf_handle(cf).unwrap();
        for i in k_start..k_end {
            db.delete_cf(cf_handle, make_key(i).as_bytes()).unwrap();
        }
    }
}

pub fn do_delete_range(db: &DB, k_start: u64, k_end: u64) {
    for cf in ALL_CFS {
        let cf_handle = db.cf_handle(cf).unwrap();
        db.delete_range_cf(
            cf_handle,
            make_key(k_start).as_bytes(),
            make_key(k_end).as_bytes(),
        ).unwrap();
    }
}

pub fn do_check_exist(db: &DB, k_start: u64, k_end: u64, v_start: u64) {
    let mut v = v_start;
    for cf in ALL_CFS {
        let cf_handle = db.cf_handle(cf).unwrap();
        for i in k_start..k_end {
            assert_eq!(
                db.get_cf(cf_handle, make_key(i).as_bytes())
                    .unwrap()
                    .unwrap(),
                make_val(v).as_bytes()
            );
        }
        v += 1;
    }
}

pub fn do_check_non_exist(db: &DB, k_start: u64, k_end: u64) {
    for cf in ALL_CFS {
        let cf_handle = db.cf_handle(cf).unwrap();
        for i in k_start..k_end {
            assert!(
                db.get_cf(cf_handle, make_key(i).as_bytes())
                    .unwrap()
                    .is_none()
            );
        }
    }
}

pub fn do_gen_sst(opt: ColumnFamilyOptions, cf: &CFHandle, path: &str, start: u64, end: u64) {
    let _ = fs::remove_file(path);
    let env_opt = EnvOptions::new();
    let mut writer = SstFileWriter::new_cf(env_opt, opt, cf);
    writer.open(path).unwrap();
    for i in start..end {
        writer
            .put(make_key(i).as_bytes(), make_val(i).as_bytes())
            .unwrap();
    }

    writer.finish().unwrap();
}

#[test]
fn test_rocksdb_insert() {
    let path = TempDir::new("_rocksdb_insert").unwrap();
    let db = rocksdb::new_engine(path.path().to_str().unwrap(), ALL_CFS, None).unwrap();
    do_insert(&db, 0, NUM, 0);
    do_check_exist(&db, 0, NUM, 0);
}

#[test]
fn test_rocksdb_update() {
    let path = TempDir::new("_rocksdb_update").unwrap();
    let db = rocksdb::new_engine(path.path().to_str().unwrap(), ALL_CFS, None).unwrap();
    do_insert(&db, 0, NUM, 0);
    do_insert(&db, 0, NUM, NUM);
    do_check_exist(&db, 0, NUM, NUM);
}

#[test]
fn test_rocksdb_delete() {
    let path = TempDir::new("_rocksdb_delete").unwrap();
    let db = rocksdb::new_engine(path.path().to_str().unwrap(), ALL_CFS, None).unwrap();
    do_insert(&db, 0, NUM, 0);
    do_delete(&db, 0, NUM / 2);
    do_check_non_exist(&db, 0, NUM / 2);
    do_check_exist(&db, NUM / 2, NUM, 0);
}
