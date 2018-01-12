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

use std::io::Read;
use std::fs::File;
use std::path::Path;

use crc::crc32::{self, Hasher32};
use uuid::Uuid;
use rocksdb::{ColumnFamilyOptions, EnvOptions, SstFileWriter, DB};
use kvproto::importpb::*;

use raftstore::store::keys;

pub fn get_data_crc32(data: &[u8]) -> u32 {
    let mut digest = crc32::Digest::new(crc32::IEEE);
    digest.write(data);
    digest.sum32()
}

pub fn check_db_range(db: &DB, start: u8, end: u8) {
    for i in start..end {
        let k = keys::data_key(&[i]);
        assert_eq!(db.get(&k).unwrap().unwrap(), &[i]);
    }
}

pub fn gen_sst_meta<P: AsRef<Path>>(path: P, start: u8, end: u8) -> SSTMeta {
    let mut data = Vec::new();
    File::open(path).unwrap().read_to_end(&mut data).unwrap();
    let crc32 = get_data_crc32(&data);
    let length = data.len() as u64;

    let mut range = Range::new();
    range.set_start(vec![start]);
    range.set_end(vec![end]);

    let mut m = SSTMeta::new();
    m.set_uuid(Uuid::new_v4().as_bytes().to_vec());
    m.set_range(range);
    m.set_crc32(crc32);
    m.set_length(length);
    m.set_cf_name("default".to_owned());
    m
}

pub fn gen_sst_file<P: AsRef<Path>>(path: P, start: u8, end: u8) -> SSTMeta {
    let env_opt = EnvOptions::new();
    let cf_opt = ColumnFamilyOptions::new();
    let mut w = SstFileWriter::new(env_opt, cf_opt);

    w.open(path.as_ref().to_str().unwrap()).unwrap();
    for i in start..end {
        let k = keys::data_key(&[i]);
        w.put(&k, &[i]).unwrap();
    }
    w.finish().unwrap();

    gen_sst_meta(path, start, end)
}
