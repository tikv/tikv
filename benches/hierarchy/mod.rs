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

use std::fmt;

use self::engines::bench_engine;
use self::mvcc::bench_mvcc;
use self::storage::bench_storage;
use self::txn::bench_txn;
use criterion::Criterion;
use tikv::storage::{
    engine::{BTreeEngine, RocksEngine},
    Engine, TestEngineBuilder,
};

const DEFAULT_ITERATIONS: usize = 1;
const DEFAULT_KEY_LENGTHS: [usize; 1] = [64];
const DEFAULT_VALUE_LENGTHS: [usize; 2] = [64, 65];
const DEFAULT_KV_GENERATOR_SEED: u64 = 0;

#[derive(Clone, Debug)]
pub struct KvConfig<F> {
    pub key_length: usize,
    pub value_length: usize,
    pub engine_factory: F,
}

pub fn generate_kv_configs<E: Engine, F: EngineFactory<E>>(engine_factory: F) -> Vec<KvConfig<F>> {
    let key_lengths = DEFAULT_KEY_LENGTHS;
    let value_lengths = DEFAULT_VALUE_LENGTHS;
    let mut configs = vec![];

    for &kl in &key_lengths {
        for &vl in &value_lengths {
            configs.push(KvConfig {
                key_length: kl,
                value_length: vl,
                engine_factory,
            })
        }
    }
    configs
}

pub trait EngineFactory<E: Engine>: Clone + Copy + fmt::Debug + 'static {
    fn build(&self) -> E;
}

#[derive(Clone, Copy)]
struct BTreeEngineFactory {}

impl EngineFactory<BTreeEngine> for BTreeEngineFactory {
    fn build(&self) -> BTreeEngine {
        BTreeEngine::default()
    }
}

impl fmt::Debug for BTreeEngineFactory {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "BTreeEngine")
    }
}

#[derive(Clone, Copy)]
struct RocksEngineFactory {}

impl EngineFactory<RocksEngine> for RocksEngineFactory {
    fn build(&self) -> RocksEngine {
        TestEngineBuilder::new().build().unwrap()
    }
}

impl fmt::Debug for RocksEngineFactory {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "RocksEngine")
    }
}

pub fn make_engine() -> impl Engine {
    //    TestEngineBuilder::new().build().unwrap()
    BTreeEngine::default()
}

fn main() {
    let mut c = Criterion::default();
    let btree_engine_configs = generate_kv_configs(BTreeEngineFactory {});
    let rocks_engine_configs = generate_kv_configs(RocksEngineFactory {});

    bench_engine(&mut c, &btree_engine_configs);
    bench_engine(&mut c, &rocks_engine_configs);

    bench_mvcc(&mut c, &btree_engine_configs);
    bench_mvcc(&mut c, &rocks_engine_configs);

    bench_txn(&mut c, &btree_engine_configs);
    bench_txn(&mut c, &rocks_engine_configs);

    bench_storage(&mut c, &btree_engine_configs);
    bench_storage(&mut c, &rocks_engine_configs);

    c.final_summary();
}
