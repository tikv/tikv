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

use criterion::{black_box, Bencher, Criterion};
use kvproto::kvrpcpb::Context;
use test_util::KvGenerator;
use tikv::storage::engine::{Engine, TestEngineBuilder};
use tikv::storage::mvcc::{MvccReader, MvccTxn};
use tikv::storage::{Key, Mutation, Options};

use super::*;

fn mvcc_prewrite<E: Engine, F: EngineFactory<E>>(b: &mut Bencher, config: &KvConfig<F>) {
    let engine = make_engine();
    let ctx = Context::new();
    let option = Options::default();
    let snapshot = engine.snapshot(&ctx).unwrap();
    b.iter_with_setup(
        || {
            let mutations: Vec<(Mutation, Vec<u8>)> = KvGenerator::with_seed(
                config.key_length,
                config.value_length,
                DEFAULT_KV_GENERATOR_SEED,
            ).generate(DEFAULT_ITERATIONS)
                .iter()
                .map(|(k, v)| (Mutation::Put((Key::from_raw(&k), v.clone())), k.clone()))
                .collect();
            let txn = MvccTxn::new(snapshot.clone(), 1, false).unwrap();
            (mutations, txn, &option)
        },
        |(mutations, mut txn, option)| {
            for (mutation, primary) in mutations {
                black_box(txn.prewrite(mutation, &primary, option).unwrap());
            }
        },
    )
}

fn mvcc_commit<E: Engine, F: EngineFactory<E>>(b: &mut Bencher, config: &KvConfig<F>) {
    let engine = make_engine();
    let ctx = Context::new();
    let snapshot = engine.snapshot(&ctx).unwrap();
    let option = Options::default();
    b.iter_with_setup(
        || {
            let mut txn = MvccTxn::new(snapshot.clone(), 1, false).unwrap();

            let kvs = KvGenerator::with_seed(
                config.key_length,
                config.value_length,
                DEFAULT_KV_GENERATOR_SEED,
            ).generate(DEFAULT_ITERATIONS);
            for (k, v) in &kvs {
                txn.prewrite(
                    Mutation::Put((Key::from_raw(&k), v.clone())),
                    &k.clone(),
                    &option,
                ).unwrap();
            }
            let modifies = txn.into_modifies();

            let _ = engine.async_write(&ctx, modifies, Box::new(move |(_, _)| {}));

            let keys: Vec<Key> = kvs.iter().map(|(k, _)| Key::from_raw(&k)).collect();
            let snapshot = engine.snapshot(&ctx).unwrap();
            let txn = MvccTxn::new(snapshot, 1, false).unwrap();
            (txn, keys)
        },
        |(mut txn, keys)| {
            for key in keys {
                txn.commit(key, 1).unwrap();
            }
        },
    );
}

fn mvcc_reader_load_lock<E: Engine, F: EngineFactory<E>>(b: &mut Bencher, config: &KvConfig<F>) {
    let engine = make_engine();
    let ctx = Context::default();
    let test_keys: Vec<Key> = KvGenerator::with_seed(
        config.key_length,
        config.value_length,
        DEFAULT_KV_GENERATOR_SEED,
    ).generate(DEFAULT_ITERATIONS)
        .iter()
        .map(|(k, _)| Key::from_raw(&k))
        .collect();

    b.iter_with_setup(
        || {
            let snapshot = engine.snapshot(&ctx).unwrap();
            let reader = MvccReader::new(snapshot, None, false, None, None, ctx.isolation_level);
            (reader, &test_keys)
        },
        |(mut reader, test_kvs)| {
            for key in test_kvs {
                black_box(reader.load_lock(&key).unwrap());
            }
        },
    );
}

fn mvcc_reader_seek_write<E: Engine, F: EngineFactory<E>>(b: &mut Bencher, config: &KvConfig<F>) {
    let engine = make_engine();
    let ctx = Context::default();
    b.iter_with_setup(
        || {
            let snapshot = engine.snapshot(&ctx).unwrap();
            let reader = MvccReader::new(snapshot, None, false, None, None, ctx.isolation_level);
            let test_keys: Vec<Key> = KvGenerator::with_seed(
                config.key_length,
                config.value_length,
                DEFAULT_KV_GENERATOR_SEED,
            ).generate(DEFAULT_ITERATIONS)
                .iter()
                .map(|(k, _)| Key::from_raw(&k))
                .collect();
            (reader, test_keys)
        },
        |(mut reader, test_keys)| {
            for key in &test_keys {
                black_box(reader.seek_write(&key, u64::max_value()).unwrap());
            }
        },
    );
}

pub fn bench_mvcc<E: Engine, F: EngineFactory<E>>(c: &mut Criterion, configs: &Vec<KvConfig<F>>) {
    c.bench_function_over_inputs("mvcc_prewrite", mvcc_prewrite, configs.clone());
    c.bench_function_over_inputs("mvcc_commit", mvcc_commit, configs.clone());
    c.bench_function_over_inputs("mvcc_load_lock", mvcc_reader_load_lock, configs.clone());
    c.bench_function_over_inputs("mvcc_seek_write", mvcc_reader_seek_write, configs.clone());
}
