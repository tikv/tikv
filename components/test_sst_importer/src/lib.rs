// Copyright 2018 TiKV Project Authors. Licensed under Apache-2.0.

use std::{collections::HashMap, fs, path::Path, sync::Arc};

use engine_rocks::{
    raw::{
        ColumnFamilyOptions, DBEntryType, DBOptions, Env, TablePropertiesCollector,
        TablePropertiesCollectorFactory,
    },
    raw_util::{new_engine, CFOptions},
    RocksEngine, RocksSstReader, RocksSstWriterBuilder,
};
pub use engine_rocks::{RocksEngine as TestEngine, RocksSstWriter};
use engine_traits::{KvEngine, SstWriter, SstWriterBuilder};
use kvproto::import_sstpb::*;
use uuid::Uuid;

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
                TestPropertiesCollectorFactory::new(*cf),
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

pub fn new_sst_reader(path: &str, e: Option<Arc<Env>>) -> RocksSstReader {
    RocksSstReader::open_with_env(path, e).expect("test sst reader")
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
    range: (u8, u8),
    db: Option<&RocksEngine>,
) -> (SstMeta, Vec<u8>) {
    let mut builder = RocksSstWriterBuilder::new();
    if let Some(db) = db {
        builder = builder.set_db(db);
    }
    let mut w = builder.build(path.as_ref().to_str().unwrap()).unwrap();
    for i in range.0..range.1 {
        let k = keys::data_key(&[i]);
        w.put(&k, &[i]).unwrap();
    }
    w.finish().unwrap();

    read_sst_file(path, (&[range.0], &[range.1]))
}

pub fn gen_sst_file<P: AsRef<Path>>(path: P, range: (u8, u8)) -> (SstMeta, Vec<u8>) {
    gen_sst_file_by_db(path, range, None)
}

pub fn gen_sst_file_with_kvs<P: AsRef<Path>>(
    path: P,
    kvs: &[(&[u8], &[u8])],
) -> (SstMeta, Vec<u8>) {
    let builder = RocksSstWriterBuilder::new();
    let mut w = builder.build(path.as_ref().to_str().unwrap()).unwrap();
    for (k, v) in kvs {
        let dk = keys::data_key(k);
        w.put(&dk, v).unwrap();
    }
    w.finish().unwrap();

    let start_key = kvs[0].0;
    let end_key = keys::next_key(kvs.last().cloned().unwrap().0);
    read_sst_file(path, (start_key, &end_key))
}

pub fn read_sst_file<P: AsRef<Path>>(path: P, range: (&[u8], &[u8])) -> (SstMeta, Vec<u8>) {
    let data = fs::read(path).unwrap();
    let crc32 = calc_data_crc32(&data);

    let mut meta = SstMeta::default();
    meta.set_uuid(Uuid::new_v4().as_bytes().to_vec());
    meta.mut_range().set_start(range.0.to_vec());
    meta.mut_range().set_end(range.1.to_vec());
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

impl TablePropertiesCollectorFactory<TestPropertiesCollector> for TestPropertiesCollectorFactory {
    fn create_table_properties_collector(&mut self, _: u32) -> TestPropertiesCollector {
        TestPropertiesCollector::new(self.cf.clone())
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
