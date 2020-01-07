// Copyright 2018 TiKV Project Authors. Licensed under Apache-2.0.

use std::collections::HashMap;
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

use engine::rocks::util::{new_engine, CFOptions};
use engine::rocks::{
    ColumnFamilyOptions, DBEntryType, TablePropertiesCollector, TablePropertiesCollectorFactory,
};
use std::sync::Arc;

pub use engine_rocks::RocksEngine as TestEngine;

pub const PROP_TEST_MARKER_CF_NAME: &[u8] = b"tikv.test_marker_cf_name";

pub fn new_test_engine(path: &str, cfs: &[&str]) -> RocksEngine {
    let cf_opts = cfs
        .iter()
        .map(|cf| {
            let mut opt = ColumnFamilyOptions::new();
            opt.add_table_properties_collector_factory(
                "tikv.test_properties",
                Box::new(TestPropertiesCollectorFactory::new(*cf)),
            );
            CFOptions::new(*cf, opt)
        })
        .collect();
    let db = new_engine(path, None, cfs, Some(cf_opts)).expect("rocks test engine");
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

#[derive(Default)]
struct TestPropertiesCollectorFactory {
    cf: String,
}

impl TestPropertiesCollectorFactory {
    pub fn new(cf: impl Into<String>) -> Self {
        Self { cf: cf.into() }
    }
}

impl TablePropertiesCollectorFactory for TestPropertiesCollectorFactory {
    fn create_table_properties_collector(&mut self, _: u32) -> Box<dyn TablePropertiesCollector> {
        Box::new(TestPropertiesCollector::new(self.cf.clone()))
    }
}

struct TestPropertiesCollector {
    cf: String,
}

impl TestPropertiesCollector {
    pub fn new(cf: String) -> Self {
        Self { cf }
    }
}

impl TablePropertiesCollector for TestPropertiesCollector {
    fn add(&mut self, _: &[u8], _: &[u8], _: DBEntryType, _: u64, _: u64) {}

    fn finish(&mut self) -> HashMap<Vec<u8>, Vec<u8>> {
        std::iter::once((
            PROP_TEST_MARKER_CF_NAME.to_owned(),
            self.cf.as_bytes().to_owned(),
        ))
        .collect()
    }
}
