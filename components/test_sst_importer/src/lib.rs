// Copyright 2018 TiKV Project Authors. Licensed under Apache-2.0.

use std::collections::HashMap;
use std::fs;
use std::path::Path;

use engine_rocks::RocksEngine;
use engine_rocks::RocksSstReader;
pub use engine_rocks::RocksSstWriter;
use engine_rocks::RocksSstWriterBuilder;
use engine_traits::KvEngine;
use engine_traits::SstReader;
use engine_traits::SstWriter;
use engine_traits::SstWriterBuilder;
use kvproto::import_sstpb::*;
use uuid::Uuid;

use engine_rocks::raw::{
    ColumnFamilyOptions, DBEntryType, DBOptions, Env, TablePropertiesCollector,
    TablePropertiesCollectorFactory,
};
use engine_rocks::raw_util::{new_engine, CFOptions};
use std::sync::Arc;

pub use engine_rocks::RocksEngine as TestEngine;

pub const PROP_TEST_MARKER_CF_NAME: &[u8] = b"tikv.test_marker_cf_name";

pub fn new_test_engine(path: &str, cfs: &[&str]) -> RocksEngine {
    new_test_engine_with_options(path, cfs, |_, _| {})
}

pub fn new_test_engine_with_env(path: &str, cfs: &[&str], env: Arc<Env>) -> RocksEngine {
    new_test_engine_with_options_and_env(path, cfs, |_, _| {}, Some(env))
}

pub fn new_test_engine_with_options_and_env<F>(
    path: &str,
    cfs: &[&str],
    mut apply: F,
    env: Option<Arc<Env>>,
) -> RocksEngine
where
    F: FnMut(&str, &mut ColumnFamilyOptions),
{
    let cf_opts = cfs
        .iter()
        .map(|cf| {
            let mut opt = ColumnFamilyOptions::new();
            if let Some(ref env) = env {
                opt.set_env(env.clone());
            }
            apply(*cf, &mut opt);
            opt.add_table_properties_collector_factory(
                "tikv.test_properties",
                Box::new(TestPropertiesCollectorFactory::new(*cf)),
            );
            CFOptions::new(*cf, opt)
        })
        .collect();

    let db_opts = env.map(|e| {
        let mut opts = DBOptions::default();
        opts.set_env(e);
        opts
    });
    let db = new_engine(path, db_opts, cfs, Some(cf_opts)).expect("rocks test engine");
    RocksEngine::from_db(Arc::new(db))
}

pub fn new_test_engine_with_options<F>(path: &str, cfs: &[&str], apply: F) -> RocksEngine
where
    F: FnMut(&str, &mut ColumnFamilyOptions),
{
    new_test_engine_with_options_and_env(path, cfs, apply, None)
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

pub fn gen_sst_file_by_db<P: AsRef<Path>>(
    path: P,
    range: &[Vec<u8>],
    db: Option<&RocksEngine>,
) -> (SstMeta, Vec<u8>) {
    let mut builder = RocksSstWriterBuilder::new();
    if let Some(db) = db {
        builder = builder.set_db(db);
    }
    let mut w = builder.build(path.as_ref().to_str().unwrap()).unwrap();
    for i in range {
        let k = keys::data_key(i);
        w.put(&k, i).unwrap();
    }
    w.finish().unwrap();
    let mut keys = range.to_vec();
    let mut end_key = keys.last().unwrap().clone();
    end_key.push(0);
    keys.push(end_key);
    read_sst_file(path, &keys)
}

pub fn gen_sst_file<P: AsRef<Path>>(path: P, range: (u8, u8)) -> (SstMeta, Vec<u8>) {
    let mut ranges = vec![];
    for i in range.0..range.1 {
        ranges.push(vec![i]);
    }
    gen_sst_file_by_db(path, &ranges, None)
}

pub fn read_sst_file<P: AsRef<Path>>(path: P, range: &[Vec<u8>]) -> (SstMeta, Vec<u8>) {
    assert!(range.len() > 1);
    let data = fs::read(path).unwrap();
    let crc32 = calc_data_crc32(&data);

    let mut meta = SstMeta::default();
    meta.set_uuid(Uuid::new_v4().as_bytes().to_vec());
    meta.mut_range().set_start(range[0].clone());
    let l = range.len();
    meta.mut_range().set_end(range[l - 1].clone());
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
