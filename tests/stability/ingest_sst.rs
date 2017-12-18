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
use rocksdb::IngestExternalFileOptions;

use super::basic::{do_check_exist, do_gen_sst};

#[test]
fn test_rocksdb_ingest_sst() {
    let path = TempDir::new("_rocksdb_ingest_sst").unwrap();
    let db = rocksdb::new_engine(path.path().to_str().unwrap(), ALL_CFS, None).unwrap();

    let sst_dir = TempDir::new("_rocksdb_sst_file").unwrap();
    let start = 11111;
    let end = 99999;
    for cf in ALL_CFS {
        let cf_handle = db.cf_handle(cf).unwrap();
        let file_path = sst_dir.path().join(cf);
        do_gen_sst(
            db.get_options(),
            cf_handle,
            file_path.to_str().unwrap(),
            start,
            end,
        );
        db.ingest_external_file_cf(
            cf_handle,
            &IngestExternalFileOptions::new(),
            &[file_path.to_str().unwrap()],
        ).unwrap();
    }
    do_check_exist(&db, start, end, start);
}
