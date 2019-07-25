// Copyright 2018 TiKV Project Authors. Licensed under Apache-2.0.

use criterion::{black_box, BatchSize, Bencher, Criterion};
use engine::CF_DEFAULT;
use kvproto::kvrpcpb::Context;
use test_storage::SyncTestStorageBuilder;
use test_util::KvGenerator;
use tikv::storage::kv::Engine;
use tikv::storage::{Key, Mutation};

use super::{BenchConfig, EngineFactory, DEFAULT_ITERATIONS};

fn storage_raw_get<E: Engine, F: EngineFactory<E>>(b: &mut Bencher, config: &BenchConfig<F>) {
    let engine = config.engine_factory.build();
    let store = SyncTestStorageBuilder::from_engine(engine).build().unwrap();
    b.iter_batched(
        || {
            let kvs = KvGenerator::new(config.key_length, config.value_length)
                .generate(DEFAULT_ITERATIONS);
            let data: Vec<(Context, Vec<u8>)> = kvs
                .iter()
                .map(|(k, _)| (Context::default(), k.clone()))
                .collect();
            (data, &store)
        },
        |(data, store)| {
            for (context, key) in data {
                black_box(store.raw_get(context, CF_DEFAULT.to_owned(), key).unwrap());
            }
        },
        BatchSize::SmallInput,
    );
}

fn storage_prewrite<E: Engine, F: EngineFactory<E>>(b: &mut Bencher, config: &BenchConfig<F>) {
    let engine = config.engine_factory.build();
    let store = SyncTestStorageBuilder::from_engine(engine).build().unwrap();
    b.iter_batched(
        || {
            let kvs = KvGenerator::new(config.key_length, config.value_length)
                .generate(DEFAULT_ITERATIONS);

            let data: Vec<(Context, Vec<Mutation>, Vec<u8>)> = kvs
                .iter()
                .map(|(k, v)| {
                    (
                        Context::default(),
                        vec![Mutation::Put((Key::from_raw(&k), v.clone()))],
                        k.clone(),
                    )
                })
                .collect();
            (data, &store)
        },
        |(data, store)| {
            for (context, mutations, primary) in data {
                black_box(store.prewrite(context, mutations, primary, 1).unwrap());
            }
        },
        BatchSize::SmallInput,
    );
}

fn storage_commit<E: Engine, F: EngineFactory<E>>(b: &mut Bencher, config: &BenchConfig<F>) {
    let engine = config.engine_factory.build();
    let store = SyncTestStorageBuilder::from_engine(engine).build().unwrap();
    b.iter_batched(
        || {
            let kvs = KvGenerator::new(config.key_length, config.value_length)
                .generate(DEFAULT_ITERATIONS);

            for (k, v) in &kvs {
                store
                    .prewrite(
                        Context::default(),
                        vec![Mutation::Put((Key::from_raw(&k), v.clone()))],
                        k.clone(),
                        1,
                    )
                    .unwrap();
            }

            (kvs, &store)
        },
        |(kvs, store)| {
            for (k, _) in &kvs {
                black_box(store.commit(Context::default(), vec![Key::from_raw(k)], 1, 2)).unwrap();
            }
        },
        BatchSize::SmallInput,
    );
}

pub fn bench_storage<E: Engine, F: EngineFactory<E>>(
    c: &mut Criterion,
    configs: &[BenchConfig<F>],
) {
    c.bench_function_over_inputs(
        "storage_async_prewrite",
        storage_prewrite,
        configs.to_owned(),
    );
    c.bench_function_over_inputs("storage_async_commit", storage_commit, configs.to_owned());
    c.bench_function_over_inputs("storage_async_raw_get", storage_raw_get, configs.to_owned());
}
