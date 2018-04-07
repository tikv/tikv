// Copyright 2018 PingCAP, Inc.
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

use std::fs::File;
use std::io::Read;
use std::path::Path;

use crc::crc32::{self, Hasher32};
use kvproto::importpb::*;
use rocksdb::{ColumnFamilyOptions, EnvOptions, SstFileWriter, DB};
use uuid::Uuid;

use raftstore::store::keys;

pub fn calc_data_crc32(data: &[u8]) -> u32 {
    let mut digest = crc32::Digest::new(crc32::IEEE);
    digest.write(data);
    digest.sum32()
}

pub fn check_db_range(db: &DB, range: (u8, u8)) {
    for i in range.0..range.1 {
        let k = keys::data_key(&[i]);
        assert_eq!(db.get(&k).unwrap().unwrap(), &[i]);
    }
}

pub fn gen_sst_file<P: AsRef<Path>>(path: P, range: (u8, u8)) -> (SSTMeta, Vec<u8>) {
    let env_opt = EnvOptions::new();
    let cf_opt = ColumnFamilyOptions::new();
    let mut w = SstFileWriter::new(env_opt, cf_opt);

    w.open(path.as_ref().to_str().unwrap()).unwrap();
    for i in range.0..range.1 {
        let k = keys::data_key(&[i]);
        w.put(&k, &[i]).unwrap();
    }
    w.finish().unwrap();

    read_sst_file(path, range)
}

pub fn read_sst_file<P: AsRef<Path>>(path: P, range: (u8, u8)) -> (SSTMeta, Vec<u8>) {
    let mut data = Vec::new();
    File::open(path).unwrap().read_to_end(&mut data).unwrap();
    let crc32 = calc_data_crc32(&data);

    let mut meta = SSTMeta::new();
    meta.set_uuid(Uuid::new_v4().as_bytes().to_vec());
    meta.mut_range().set_start(vec![range.0]);
    meta.mut_range().set_end(vec![range.1]);
    meta.set_crc32(crc32);
    meta.set_length(data.len() as u64);
    meta.set_cf_name("default".to_owned());

    (meta, data)
}
