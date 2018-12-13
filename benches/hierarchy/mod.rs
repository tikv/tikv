extern crate criterion;
extern crate kvproto;
extern crate test_storage;
extern crate test_util;
extern crate tikv;

mod engines;
mod mvcc;
mod storage;
mod txn;
mod engine_factory;

use self::engines::bench_engine;
use self::mvcc::bench_mvcc;
use self::storage::bench_storage;
use self::txn::bench_txn;
use self::engine_factory::{EngineFactory,BTreeEngineFactory,RocksEngineFactory};
use criterion::Criterion;
use tikv::storage::Engine;

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
