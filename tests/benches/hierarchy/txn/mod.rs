// Copyright 2018 TiKV Project Authors. Licensed under Apache-2.0.

use criterion::{black_box, BatchSize, Bencher, Criterion};
use keys::{Key, TimeStamp};
use kvproto::kvrpcpb::Context;
use test_util::KvGenerator;
use tikv::storage::kv::Engine;
use tikv::storage::mvcc::{self, MvccTxn};
use tikv::storage::{Mutation, Options};

use super::{BenchConfig, EngineFactory, DEFAULT_ITERATIONS};

fn setup_prewrite<E, F>(
    engine: &E,
    config: &BenchConfig<F>,
    start_ts: impl Into<TimeStamp>,
) -> Vec<Key>
where
    E: Engine,
    F: EngineFactory<E>,
{
    let ctx = Context::default();
    let option = Options::default();

    let snapshot = engine.snapshot(&ctx).unwrap();
    let mut txn = MvccTxn::new(snapshot, start_ts.into(), true).unwrap();
    let kvs = KvGenerator::new(config.key_length, config.value_length).generate(DEFAULT_ITERATIONS);
    for (k, v) in &kvs {
        txn.prewrite(
            Mutation::Put((Key::from_raw(&k), v.clone())),
            &k.clone(),
            &option,
        )
        .unwrap();
    }
    let modifies = txn.into_modifies();
    let _ = engine.write(&ctx, modifies);
    let keys: Vec<Key> = kvs.iter().map(|(k, _)| Key::from_raw(&k)).collect();
    keys
}

fn txn_prewrite<E: Engine, F: EngineFactory<E>>(b: &mut Bencher, config: &BenchConfig<F>) {
    let engine = config.engine_factory.build();
    let ctx = Context::default();
    let option = Options::default();
    b.iter_batched(
        || {
            let mutations: Vec<(Mutation, Vec<u8>)> =
                KvGenerator::new(config.key_length, config.value_length)
                    .generate(DEFAULT_ITERATIONS)
                    .iter()
                    .map(|(k, v)| (Mutation::Put((Key::from_raw(&k), v.clone())), k.clone()))
                    .collect();
            (mutations, &option)
        },
        |(mutations, option)| {
            for (mutation, primary) in mutations {
                let snapshot = engine.snapshot(&ctx).unwrap();
                let mut txn = mvcc::new_txn!(snapshot, 1, true);
                txn.prewrite(mutation, &primary, option).unwrap();
                let modifies = txn.into_modifies();
                black_box(engine.write(&ctx, modifies)).unwrap();
            }
        },
        BatchSize::SmallInput,
    )
}

fn txn_commit<E: Engine, F: EngineFactory<E>>(b: &mut Bencher, config: &BenchConfig<F>) {
    let engine = config.engine_factory.build();
    let ctx = Context::default();
    b.iter_batched(
        || setup_prewrite(&engine, &config, 1),
        |keys| {
            for key in keys {
                let snapshot = engine.snapshot(&ctx).unwrap();
                let mut txn = mvcc::new_txn!(snapshot, 1, true);
                txn.commit(key, 2.into()).unwrap();
                let modifies = txn.into_modifies();
                black_box(engine.write(&ctx, modifies)).unwrap();
            }
        },
        BatchSize::SmallInput,
    );
}

fn txn_rollback_prewrote<E: Engine, F: EngineFactory<E>>(b: &mut Bencher, config: &BenchConfig<F>) {
    let engine = config.engine_factory.build();
    let ctx = Context::default();
    b.iter_batched(
        || setup_prewrite(&engine, &config, 1),
        |keys| {
            for key in keys {
                let snapshot = engine.snapshot(&ctx).unwrap();
                let mut txn = mvcc::new_txn!(snapshot, 1, true);
                txn.rollback(key).unwrap();
                let modifies = txn.into_modifies();
                black_box(engine.write(&ctx, modifies)).unwrap();
            }
        },
        BatchSize::SmallInput,
    )
}

fn txn_rollback_conflict<E: Engine, F: EngineFactory<E>>(b: &mut Bencher, config: &BenchConfig<F>) {
    let engine = config.engine_factory.build();
    let ctx = Context::default();
    b.iter_batched(
        || setup_prewrite(&engine, &config, 2),
        |keys| {
            for key in keys {
                let snapshot = engine.snapshot(&ctx).unwrap();
                let mut txn = mvcc::new_txn!(snapshot, 1, true);
                txn.rollback(key).unwrap();
                let modifies = txn.into_modifies();
                black_box(engine.write(&ctx, modifies)).unwrap();
            }
        },
        BatchSize::SmallInput,
    )
}

fn txn_rollback_non_prewrote<E: Engine, F: EngineFactory<E>>(
    b: &mut Bencher,
    config: &BenchConfig<F>,
) {
    let engine = config.engine_factory.build();
    let ctx = Context::default();
    b.iter_batched(
        || {
            let kvs = KvGenerator::new(config.key_length, config.value_length)
                .generate(DEFAULT_ITERATIONS);
            let keys: Vec<Key> = kvs.iter().map(|(k, _)| Key::from_raw(&k)).collect();
            keys
        },
        |keys| {
            for key in keys {
                let snapshot = engine.snapshot(&ctx).unwrap();
                let mut txn = mvcc::new_txn!(snapshot, 1, true);
                txn.rollback(key).unwrap();
                let modifies = txn.into_modifies();
                black_box(engine.write(&ctx, modifies)).unwrap();
            }
        },
        BatchSize::SmallInput,
    )
}

pub fn bench_txn<E: Engine, F: EngineFactory<E>>(c: &mut Criterion, configs: &[BenchConfig<F>]) {
    c.bench_function_over_inputs("txn_prewrite", txn_prewrite, configs.to_owned());
    c.bench_function_over_inputs("txn_commit", txn_commit, configs.to_owned());
    c.bench_function_over_inputs(
        "txn_rollback_prewrote",
        txn_rollback_prewrote,
        configs.to_owned(),
    );
    c.bench_function_over_inputs(
        "txn_rollback_conflict",
        txn_rollback_conflict,
        configs.to_owned(),
    );
    c.bench_function_over_inputs(
        "txn_rollback_non_prewrote",
        txn_rollback_non_prewrote,
        configs.to_owned(),
    );
}
