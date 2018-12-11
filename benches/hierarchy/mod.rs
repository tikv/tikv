#[macro_use]
extern crate criterion;
extern crate kvproto;
extern crate test_storage;
extern crate test_util;
extern crate tikv;

mod engines;
mod mvcc;
mod storage;
mod txn;

use criterion::Criterion;
use tikv::storage::{TestEngineBuilder,Engine, engine::BTreeEngine};
use self::engines::bench_engines;
use self::mvcc::bench_mvcc;
use self::storage::bench_storage;
use self::txn::bench_txn;

const DEFAULT_ITERATIONS: usize = 1;
const DEFAULT_KEY_LENGTHS: [usize; 1] = [64];
const DEFAULT_KEY_LENGTH: usize = 64;
const DEFAULT_VALUE_LENGTHS: [usize; 2] = [64, 65];
const DEFAULT_KV_GENERATOR_SEED: u64 = 0;

#[derive(Clone, Debug)]
pub struct KvConfig {
    pub key_length: usize,
    pub value_length: usize,
}

pub fn generate_kv_configs() -> Vec<KvConfig> {
    let key_lengths = DEFAULT_KEY_LENGTHS;
    let value_lengths = DEFAULT_VALUE_LENGTHS;
    let mut configs = vec![];

    for &kl in &key_lengths {
        for &vl in &value_lengths {
            configs.push(KvConfig {
                key_length: kl,
                value_length: vl,
            })
        }
    }
    configs
}

pub fn make_engine() -> impl Engine {
//    TestEngineBuilder::new().build().unwrap()
            BTreeEngine::default()
}

criterion_group!(benches, bench_engines, bench_mvcc, bench_txn, bench_storage);
criterion_main!(benches);
