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

extern crate criterion;
extern crate kvproto;
extern crate test_util;
extern crate tikv;

use criterion::{black_box, Bencher, Criterion};
use kvproto::kvrpcpb::Context;
use std::fmt;
use test_util::generate_random_kvs;
use tikv::storage::engine::{
    BTreeEngine, Engine, Modify, RocksEngine, Snapshot, TestEngineBuilder,
};
use tikv::storage::{Key, CF_DEFAULT};

const DEFAULT_KEY_LENGTH: usize = 64;
const DEFAULT_ITERATIONS: usize = 1000;

trait EngineFactory<E: Engine>: Clone + fmt::Debug + 'static {
    fn build(&self) -> E;
}

#[derive(Clone)]
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

#[derive(Clone)]
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

fn fill_engine_with<E: Engine>(engine: &E, expect_engine_keys_count: usize, value_length: usize) {
    let mut modifies: Vec<Modify> = vec![];
    if expect_engine_keys_count > 0 {
        let kvs = generate_random_kvs(expect_engine_keys_count, DEFAULT_KEY_LENGTH, value_length);
        for (key, value) in kvs {
            modifies.push(Modify::Put(CF_DEFAULT, Key::from_raw(&key), value))
        }
    }
    let ctx = Context::new();
    let _ = engine.async_write(&ctx, modifies, Box::new(move |(_, _)| {}));
}

/// Measuring the performance of Engine::snapshot()
fn engine_snapshot_bench<E: Engine>(
    bencher: &mut Bencher,
    engine: &E,
    engine_keys_count: usize,
    value_length: usize,
) {
    fill_engine_with(engine, engine_keys_count, value_length);
    let ctx = Context::new();

    bencher.iter(|| engine.snapshot(&ctx).is_ok())
}

/// Measuring the performance of Snapshot::get(), skipping the Engine::snapshot();
fn engine_get_bench<E: Engine>(
    bencher: &mut Bencher,
    engine: &E,
    iterations: usize,
    engine_keys_count: usize,
    value_length: usize,
) {
    fill_engine_with(engine, engine_keys_count, value_length);
    let ctx = Context::new();
    let test_kvs: Vec<Key> = generate_random_kvs(iterations, DEFAULT_KEY_LENGTH, value_length)
        .iter()
        .map(|(key, _)| Key::from_raw(&key))
        .collect();

    bencher.iter_with_setup(
        || engine.snapshot(&ctx).unwrap(),
        |snap| {
            for key in &test_kvs {
                black_box(snap.get(key).is_ok());
            }
        },
    );
}

/// Measure the performance of Engine::put()
fn engine_put_bench<E: Engine>(
    bencher: &mut Bencher,
    engine: &E,
    write_count: usize,
    value_length: usize,
) {
    let ctx = Context::new();
    bencher.iter_with_setup(
        || {
            let test_kvs: Vec<(Key, Vec<u8>)> =
                generate_random_kvs(write_count, DEFAULT_KEY_LENGTH, value_length)
                    .iter()
                    .map(|(key, value)| (Key::from_raw(&key), value.clone()))
                    .collect();
            test_kvs
        },
        |test_kvs| {
            for (key, value) in test_kvs {
                black_box(engine.put(black_box(&ctx), key, value).is_ok());
            }
        },
    );
}

#[derive(Debug)]
struct PutConfig<F> {
    factory: F,
    put_count: usize,
    value_length: usize,
}

fn bench_engine_put<E: Engine, F: EngineFactory<E>>(bencher: &mut Bencher, config: &PutConfig<F>) {
    let engine = config.factory.build();
    engine_put_bench(bencher, &engine, config.put_count, config.value_length)
}

#[derive(Debug)]
struct SnapshotConfig<F> {
    factory: F,
    engine_keys_count: usize,
    value_length: usize,
}

fn bench_engine_snapshot<E: Engine, F: EngineFactory<E>>(
    bencher: &mut Bencher,
    config: &SnapshotConfig<F>,
) {
    let engine = config.factory.build();
    engine_snapshot_bench(
        bencher,
        &engine,
        config.engine_keys_count,
        config.value_length,
    )
}

#[derive(Debug)]
struct GetConfig<F> {
    factory: F,
    iterations: usize,
    value_length: usize,
    engine_keys_count: usize,
}

fn bench_engine_get<E: Engine, F: EngineFactory<E>>(bencher: &mut Bencher, config: &GetConfig<F>) {
    let engine = config.factory.build();
    engine_get_bench(
        bencher,
        &engine,
        config.iterations,
        config.engine_keys_count,
        config.value_length,
    )
}

fn bench_engines<E: Engine, F: EngineFactory<E>>(c: &mut Criterion, factory: F) {
    let value_lengths = vec![128, 1024];
    let engine_keys_counts = vec![0, 1000, 10_000];
    let engine_put_keys_counts = vec![1000];

    let mut get_configs = vec![];
    let mut put_configs = vec![];
    let mut snapshot_configs = vec![];

    for &value_length in &value_lengths {
        for &engine_keys_count in &engine_keys_counts {
            get_configs.push(GetConfig {
                factory: factory.clone(),
                iterations: DEFAULT_ITERATIONS,
                value_length,
                engine_keys_count,
            });
            snapshot_configs.push(SnapshotConfig {
                factory: factory.clone(),
                value_length,
                engine_keys_count,
            });
        }

        for &engine_put_keys_count in &engine_put_keys_counts {
            put_configs.push(PutConfig {
                factory: factory.clone(),
                put_count: engine_put_keys_count,
                value_length,
            });
        }
    }

    c.bench_function_over_inputs("bench_engine_get", bench_engine_get, get_configs);
    c.bench_function_over_inputs("bench_engine_put", bench_engine_put, put_configs);
    c.bench_function_over_inputs(
        "bench_engine_snapshot",
        bench_engine_snapshot,
        snapshot_configs,
    );
}

fn main() {
    let mut criterion = Criterion::default().sample_size(10);
    bench_engines(&mut criterion, RocksEngineFactory {});
    bench_engines(&mut criterion, BTreeEngineFactory {});
    criterion.final_summary();
}
