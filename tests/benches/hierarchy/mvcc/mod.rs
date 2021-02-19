// Copyright 2018 TiKV Project Authors. Licensed under Apache-2.0.

use concurrency_manager::ConcurrencyManager;
use criterion::{black_box, BatchSize, Bencher, Criterion};
use kvproto::kvrpcpb::Context;
use test_util::KvGenerator;
use tikv::storage::kv::{Engine, WriteData};
use tikv::storage::mvcc::{self, MvccReader, MvccTxn};
use tikv::storage::txn::{
    cleanup, commit, prewrite, CommitKind, TransactionKind, TransactionProperties,
};
use txn_types::{Key, Mutation, TimeStamp};

use super::{BenchConfig, EngineFactory, DEFAULT_ITERATIONS, DEFAULT_KV_GENERATOR_SEED};

fn setup_prewrite<E, F>(
    engine: &E,
    config: &BenchConfig<F>,
    start_ts: impl Into<TimeStamp>,
) -> (E::Snap, Vec<Key>)
where
    E: Engine,
    F: EngineFactory<E>,
{
    let ctx = Context::default();
    let snapshot = engine.snapshot(Default::default()).unwrap();
    let start_ts = start_ts.into();
    let cm = ConcurrencyManager::new(start_ts);
    let mut txn = MvccTxn::new(snapshot, start_ts, true, cm);

    let kvs = KvGenerator::with_seed(
        config.key_length,
        config.value_length,
        DEFAULT_KV_GENERATOR_SEED,
    )
    .generate(DEFAULT_ITERATIONS);
    for (k, v) in &kvs {
        let txn_props = TransactionProperties {
            start_ts,
            kind: TransactionKind::Optimistic(false),
            commit_kind: CommitKind::TwoPc,
            primary: &k.clone(),
            txn_size: 0,
            lock_ttl: 0,
            min_commit_ts: TimeStamp::default(),
            need_old_value: false,
        };
        prewrite(
            &mut txn,
            &txn_props,
            Mutation::Put((Key::from_raw(&k), v.clone())),
            &None,
            false,
        )
        .unwrap();
    }
    let write_data = WriteData::from_modifies(txn.into_modifies());
    let _ = engine.async_write(&ctx, write_data, Box::new(move |(_, _)| {}));
    let keys: Vec<Key> = kvs.iter().map(|(k, _)| Key::from_raw(&k)).collect();
    let snapshot = engine.snapshot(Default::default()).unwrap();
    (snapshot, keys)
}

fn mvcc_prewrite<E: Engine, F: EngineFactory<E>>(b: &mut Bencher, config: &BenchConfig<F>) {
    let engine = config.engine_factory.build();
    let cm = ConcurrencyManager::new(1.into());
    b.iter_batched(
        || {
            let mutations: Vec<(Mutation, Vec<u8>)> = KvGenerator::with_seed(
                config.key_length,
                config.value_length,
                DEFAULT_KV_GENERATOR_SEED,
            )
            .generate(DEFAULT_ITERATIONS)
            .iter()
            .map(|(k, v)| (Mutation::Put((Key::from_raw(&k), v.clone())), k.clone()))
            .collect();
            let snapshot = engine.snapshot(Default::default()).unwrap();
            (mutations, snapshot)
        },
        |(mutations, snapshot)| {
            for (mutation, primary) in mutations {
                let mut txn = mvcc::MvccTxn::new(snapshot.clone(), 1.into(), true, cm.clone());
                let txn_props = TransactionProperties {
                    start_ts: TimeStamp::default(),
                    kind: TransactionKind::Optimistic(false),
                    commit_kind: CommitKind::TwoPc,
                    primary: &primary,
                    txn_size: 0,
                    lock_ttl: 0,
                    min_commit_ts: TimeStamp::default(),
                    need_old_value: false,
                };
                prewrite(&mut txn, &txn_props, mutation, &None, false).unwrap();
            }
        },
        BatchSize::SmallInput,
    )
}

fn mvcc_commit<E: Engine, F: EngineFactory<E>>(b: &mut Bencher, config: &BenchConfig<F>) {
    let engine = config.engine_factory.build();
    let cm = ConcurrencyManager::new(1.into());
    b.iter_batched(
        || setup_prewrite(&engine, &config, 1),
        |(snapshot, keys)| {
            for key in keys {
                let mut txn = mvcc::MvccTxn::new(snapshot.clone(), 1.into(), true, cm.clone());
                black_box(commit(&mut txn, key, 1.into())).unwrap();
            }
        },
        BatchSize::SmallInput,
    );
}

fn mvcc_rollback_prewrote<E: Engine, F: EngineFactory<E>>(
    b: &mut Bencher,
    config: &BenchConfig<F>,
) {
    let engine = config.engine_factory.build();
    let cm = ConcurrencyManager::new(1.into());
    b.iter_batched(
        || setup_prewrite(&engine, &config, 1),
        |(snapshot, keys)| {
            for key in keys {
                let mut txn = mvcc::MvccTxn::new(snapshot.clone(), 1.into(), true, cm.clone());
                black_box(cleanup(&mut txn, key, TimeStamp::zero(), false)).unwrap();
            }
        },
        BatchSize::SmallInput,
    )
}

fn mvcc_rollback_conflict<E: Engine, F: EngineFactory<E>>(
    b: &mut Bencher,
    config: &BenchConfig<F>,
) {
    let engine = config.engine_factory.build();
    let cm = ConcurrencyManager::new(1.into());
    b.iter_batched(
        || setup_prewrite(&engine, &config, 2),
        |(snapshot, keys)| {
            for key in keys {
                let mut txn = mvcc::MvccTxn::new(snapshot.clone(), 1.into(), true, cm.clone());
                black_box(cleanup(&mut txn, key, TimeStamp::zero(), false)).unwrap();
            }
        },
        BatchSize::SmallInput,
    )
}

fn mvcc_rollback_non_prewrote<E: Engine, F: EngineFactory<E>>(
    b: &mut Bencher,
    config: &BenchConfig<F>,
) {
    let engine = config.engine_factory.build();
    let cm = ConcurrencyManager::new(1.into());
    b.iter_batched(
        || {
            let kvs = KvGenerator::with_seed(
                config.key_length,
                config.value_length,
                DEFAULT_KV_GENERATOR_SEED,
            )
            .generate(DEFAULT_ITERATIONS);
            let keys: Vec<Key> = kvs.iter().map(|(k, _)| Key::from_raw(&k)).collect();
            let snapshot = engine.snapshot(Default::default()).unwrap();
            (snapshot, keys)
        },
        |(snapshot, keys)| {
            for key in keys {
                let mut txn = mvcc::MvccTxn::new(snapshot.clone(), 1.into(), true, cm.clone());
                black_box(cleanup(&mut txn, key, TimeStamp::zero(), false)).unwrap();
            }
        },
        BatchSize::SmallInput,
    )
}

fn mvcc_reader_load_lock<E: Engine, F: EngineFactory<E>>(b: &mut Bencher, config: &BenchConfig<F>) {
    let engine = config.engine_factory.build();
    let ctx = Context::default();
    let test_keys: Vec<Key> = KvGenerator::with_seed(
        config.key_length,
        config.value_length,
        DEFAULT_KV_GENERATOR_SEED,
    )
    .generate(DEFAULT_ITERATIONS)
    .iter()
    .map(|(k, _)| Key::from_raw(&k))
    .collect();

    b.iter_batched(
        || {
            let snapshot = engine.snapshot(Default::default()).unwrap();
            (snapshot, &test_keys)
        },
        |(snapshot, test_kvs)| {
            for key in test_kvs {
                let mut reader =
                    MvccReader::new(snapshot.clone(), None, true, ctx.get_isolation_level());
                black_box(reader.load_lock(&key).unwrap());
            }
        },
        BatchSize::SmallInput,
    );
}

fn mvcc_reader_seek_write<E: Engine, F: EngineFactory<E>>(
    b: &mut Bencher,
    config: &BenchConfig<F>,
) {
    let engine = config.engine_factory.build();
    let ctx = Context::default();
    b.iter_batched(
        || {
            let snapshot = engine.snapshot(Default::default()).unwrap();
            let test_keys: Vec<Key> = KvGenerator::with_seed(
                config.key_length,
                config.value_length,
                DEFAULT_KV_GENERATOR_SEED,
            )
            .generate(DEFAULT_ITERATIONS)
            .iter()
            .map(|(k, _)| Key::from_raw(&k))
            .collect();
            (snapshot, test_keys)
        },
        |(snapshot, test_keys)| {
            for key in &test_keys {
                let mut reader =
                    MvccReader::new(snapshot.clone(), None, true, ctx.get_isolation_level());
                black_box(reader.seek_write(&key, TimeStamp::max()).unwrap());
            }
        },
        BatchSize::SmallInput,
    );
}

pub fn bench_mvcc<E: Engine, F: EngineFactory<E>>(c: &mut Criterion, configs: &[BenchConfig<F>]) {
    let mut group = c.benchmark_group("mvcc");
    for config in configs {
        group.bench_with_input(format!("prewrite/{:?}", config), config, mvcc_prewrite);
        group.bench_with_input(format!("commit/{:?}", config), config, mvcc_commit);
        group.bench_with_input(
            format!("rollback_prewrote/{:?}", config),
            config,
            mvcc_rollback_prewrote,
        );
        group.bench_with_input(
            format!("rollback_conflict/{:?}", config),
            config,
            mvcc_rollback_conflict,
        );
        group.bench_with_input(
            format!("rollback_non_prewrote/{:?}", config),
            config,
            mvcc_rollback_non_prewrote,
        );
        group.bench_with_input(
            format!("load_lock/{:?}", config),
            config,
            mvcc_reader_load_lock,
        );
        group.bench_with_input(
            format!("seek_write/{:?}", config),
            config,
            mvcc_reader_seek_write,
        );
    }
    group.finish();
}
