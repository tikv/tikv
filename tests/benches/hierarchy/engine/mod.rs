// Copyright 2018 TiKV Project Authors. Licensed under Apache-2.0.

use criterion::{black_box, BatchSize, Bencher, Criterion};
use kvproto::kvrpcpb::Context;
use test_util::KvGenerator;
use tikv::storage::kv::{Engine, Snapshot};
use tikv::storage::{Key, Value};

use super::{BenchConfig, EngineFactory, DEFAULT_ITERATIONS, DEFAULT_KV_GENERATOR_SEED};

fn bench_engine_put<E: Engine, F: EngineFactory<E>>(
    bencher: &mut Bencher,
    config: &BenchConfig<F>,
) {
    let engine = config.engine_factory.build();
    let ctx = Context::default();
    bencher.iter_batched(
        || {
            let test_kvs: Vec<(Key, Value)> = KvGenerator::with_seed(
                config.key_length,
                config.value_length,
                DEFAULT_KV_GENERATOR_SEED,
            )
            .generate(DEFAULT_ITERATIONS)
            .iter()
            .map(|(key, value)| (Key::from_raw(&key), value.clone()))
            .collect();
            (test_kvs, &ctx)
        },
        |(test_kvs, ctx)| {
            for (key, value) in test_kvs {
                black_box(engine.put(ctx, key, value)).unwrap();
            }
        },
        BatchSize::SmallInput,
    );
}

fn bench_engine_snapshot<E: Engine, F: EngineFactory<E>>(
    bencher: &mut Bencher,
    config: &BenchConfig<F>,
) {
    let engine = config.engine_factory.build();
    let ctx = Context::default();
    bencher.iter(|| black_box(&engine).snapshot(black_box(&ctx)).unwrap());
}

//exclude snapshot
fn bench_engine_get<E: Engine, F: EngineFactory<E>>(
    bencher: &mut Bencher,
    config: &BenchConfig<F>,
) {
    let engine = config.engine_factory.build();
    let ctx = Context::default();
    let test_kvs: Vec<Key> = KvGenerator::with_seed(
        config.key_length,
        config.value_length,
        DEFAULT_KV_GENERATOR_SEED,
    )
    .generate(DEFAULT_ITERATIONS)
    .iter()
    .map(|(key, _)| Key::from_raw(&key))
    .collect();

    bencher.iter_batched(
        || {
            let snap = engine.snapshot(&ctx).unwrap();
            (snap, &test_kvs)
        },
        |(snap, test_kvs)| {
            for key in test_kvs {
                black_box(snap.get(key).unwrap());
            }
        },
        BatchSize::SmallInput,
    );
}

pub fn bench_engine<E: Engine, F: EngineFactory<E>>(c: &mut Criterion, configs: &[BenchConfig<F>]) {
    c.bench_function_over_inputs(
        "engine_get(exclude snapshot)",
        bench_engine_get,
        configs.to_vec(),
    );
    c.bench_function_over_inputs("engine_put", bench_engine_put, configs.to_owned());
    c.bench_function_over_inputs("engine_snapshot", bench_engine_snapshot, configs.to_owned());
}
