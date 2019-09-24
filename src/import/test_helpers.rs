// Copyright 2018 TiKV Project Authors. Licensed under Apache-2.0.

use std::fs;
use std::path::Path;

use crc::crc32::{self, Hasher32};
use kvproto::import_sstpb::*;
use uuid::Uuid;

use crate::raftstore::store::keys;
use engine::rocks::{SstWriterBuilder, DB};

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

pub fn gen_sst_file<P: AsRef<Path>>(path: P, range: (u8, u8)) -> (SstMeta, Vec<u8>) {
    let mut w = SstWriterBuilder::new()
        .build(path.as_ref().to_str().unwrap())
        .unwrap();
    for i in range.0..range.1 {
        let k = keys::data_key(&[i]);
        w.put(&k, &[i]).unwrap();
    }
    w.finish().unwrap();

    read_sst_file(path, range)
}

pub fn read_sst_file<P: AsRef<Path>>(path: P, range: (u8, u8)) -> (SstMeta, Vec<u8>) {
    let data = fs::read(path).unwrap();
    let crc32 = calc_data_crc32(&data);

    let mut meta = SstMeta::default();
    meta.set_uuid(Uuid::new_v4().as_bytes().to_vec());
    meta.mut_range().set_start(vec![range.0]);
    meta.mut_range().set_end(vec![range.1]);
    meta.set_crc32(crc32);
    meta.set_length(data.len() as u64);
    meta.set_cf_name("default".to_owned());

    (meta, data)
}
