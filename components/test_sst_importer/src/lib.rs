// Copyright 2018 TiKV Project Authors. Licensed under Apache-2.0.

use std::fs;
use std::path::Path;

use engine_rocks::RocksEngine;
use engine_rocks::RocksSstReader;
use engine_rocks::RocksSstWriter;
use engine_rocks::RocksSstWriterBuilder;
use engine_traits::KvEngine;
use engine_traits::SstReader;
use engine_traits::SstWriter;
use engine_traits::SstWriterBuilder;
use kvproto::import_sstpb::*;
use uuid::Uuid;

use engine::rocks::util::new_engine;
use std::sync::Arc;

pub use engine_rocks::RocksEngine as TestEngine;

pub fn new_test_engine(path: &str, cfs: &[&str]) -> RocksEngine {
    let db = new_engine(path, None, cfs, None).expect("rocks test engine");
    RocksEngine::from_db(Arc::new(db))
}

pub fn new_sst_reader(path: &str) -> RocksSstReader {
    RocksSstReader::open(path).expect("test sst reader")
}

pub fn new_sst_writer(path: &str) -> RocksSstWriter {
    RocksSstWriterBuilder::new()
        .build(path)
        .expect("test writer builder")
}

pub fn calc_data_crc32(data: &[u8]) -> u32 {
    let mut digest = crc32fast::Hasher::new();
    digest.update(data);
    digest.finalize()
}

pub fn check_db_range<E>(db: &E, range: (u8, u8))
where
    E: KvEngine,
{
    for i in range.0..range.1 {
        let k = keys::data_key(&[i]);
        assert_eq!(db.get_value(&k).unwrap().unwrap(), &[i]);
    }
}

pub fn gen_sst_file<P: AsRef<Path>>(path: P, range: (u8, u8)) -> (SstMeta, Vec<u8>) {
    let mut w = RocksSstWriterBuilder::new()
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
