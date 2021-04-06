// Copyright 2018 TiKV Project Authors. Licensed under Apache-2.0.

use criterion::{black_box, BatchSize, Bencher, Criterion};
use kvproto::kvrpcpb::Context;
use test_util::KvGenerator;
use tikv::storage::kv::{Engine, Snapshot};
use txn_types::{Key, Value};

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
    bencher.iter(|| {
        black_box(&engine)
            .snapshot(black_box(Default::default()))
            .unwrap()
    });
}

//exclude snapshot
fn bench_engine_get<E: Engine, F: EngineFactory<E>>(
    bencher: &mut Bencher,
    config: &BenchConfig<F>,
) {
    let engine = config.engine_factory.build();
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
            let snap = engine.snapshot(Default::default()).unwrap();
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
    let mut group = c.benchmark_group("engine");
    for config in configs {
        group.bench_with_input(
            format!("get(exclude snapshot)/{:?}", config),
            config,
            bench_engine_get,
        );
        group.bench_with_input(format!("put/{:?}", config), config, bench_engine_put);
        group.bench_with_input(
            format!("snapshot/{:?}", config),
            config,
            bench_engine_snapshot,
        );
    }
    group.finish();
}
