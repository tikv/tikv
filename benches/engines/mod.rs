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

use criterion::{Bencher, Criterion};
use kvproto::kvrpcpb::Context;

use test_util::generate_random_kvs;
use tikv::storage::engine::{BTreeEngine, Engine, Modify, Snapshot, TestEngineBuilder};
use tikv::storage::{Key, CF_DEFAULT};

const DEFAULT_KEY_LENGTH: usize = 64;
const DEFAULT_ITERATIONS: usize = 1000;

#[derive(Debug, Clone)]
enum EngineType {
    BTreeEngine,
    RocksDB,
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

    bencher.iter(|| {
        engine.snapshot(&ctx).is_ok();
    })
}

/// Actually, it measures the performance of Snapshot::get(), skipping the Engine::snapshot();
fn engine_get_bench<E: Engine>(
    bencher: &mut Bencher,
    engine: &E,
    iterations: usize,
    engine_keys_count: usize,
    value_length: usize,
) {
    fill_engine_with(engine, engine_keys_count, value_length);
    let ctx = Context::new();
    let test_kvs = generate_random_kvs(iterations, DEFAULT_KEY_LENGTH, value_length);

    bencher.iter_with_setup(
        || engine.snapshot(&ctx).unwrap(),
        |snap| {
            for (key, _) in &test_kvs {
                snap.get(&Key::from_raw(key)).is_ok();
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
    let test_kvs = generate_random_kvs(write_count, DEFAULT_KEY_LENGTH, value_length);
    let ctx = Context::new();
    bencher.iter(|| {
        for (key, value) in &test_kvs {
            let _ = engine.put(&ctx, Key::from_raw(&key), value.clone());
        }
    });
}

#[derive(Debug)]
struct PutConfig {
    engine_type: EngineType,
    put_count: usize,
    value_length: usize,
}

fn bench_engine_put(bencher: &mut Bencher, config: &PutConfig) {
    match config.engine_type {
        EngineType::BTreeEngine => {
            let engine = BTreeEngine::default();
            engine_put_bench(bencher, &engine, config.put_count, config.value_length)
        }
        EngineType::RocksDB => {
            let engine = TestEngineBuilder::new().build().unwrap();
            engine_put_bench(bencher, &engine, config.put_count, config.value_length)
        }
    }
}

#[derive(Debug)]
struct SnapshotConfig {
    engine_type: EngineType,
    engine_keys_count: usize,
    value_length: usize,
}

fn bench_engine_snapshot(bencher: &mut Bencher, config: &SnapshotConfig) {
    match config.engine_type {
        EngineType::BTreeEngine => {
            let engine = BTreeEngine::default();
            engine_snapshot_bench(
                bencher,
                &engine,
                config.engine_keys_count,
                config.value_length,
            )
        }
        EngineType::RocksDB => {
            let engine = TestEngineBuilder::new().build().unwrap();
            engine_snapshot_bench(
                bencher,
                &engine,
                config.engine_keys_count,
                config.value_length,
            )
        }
    }
}

#[derive(Debug)]
struct GetConfig {
    engine_type: EngineType,
    iterations: usize,
    value_length: usize,
    engine_keys_count: usize,
}

fn bench_engine_get(bencher: &mut Bencher, config: &GetConfig) {
    match config.engine_type {
        EngineType::BTreeEngine => {
            let engine = BTreeEngine::default();
            engine_get_bench(
                bencher,
                &engine,
                config.iterations,
                config.engine_keys_count,
                config.value_length,
            )
        }
        EngineType::RocksDB => {
            let engine = TestEngineBuilder::new().build().unwrap();
            engine_get_bench(
                bencher,
                &engine,
                config.iterations,
                config.engine_keys_count,
                config.value_length,
            )
        }
    }
}

fn bench_engines(c: &mut Criterion) {
    let engine_types = vec![EngineType::BTreeEngine, EngineType::RocksDB];
    let value_lengths = vec![128, 1024];
    let engine_keys_counts = vec![0, 1000, 10_000];
    let engine_put_keys_counts = vec![0, 1000];

    let mut get_configs = vec![];
    let mut put_configs = vec![];
    let mut snapshot_configs = vec![];

    for engine_type in engine_types {
        for &value_length in &value_lengths {
            for &engine_keys_count in &engine_keys_counts {
                get_configs.push(GetConfig {
                    engine_type: engine_type.clone(),
                    iterations: DEFAULT_ITERATIONS,
                    value_length,
                    engine_keys_count,
                });
                snapshot_configs.push(SnapshotConfig {
                    engine_type: engine_type.clone(),
                    value_length,
                    engine_keys_count,
                });
            }

            for &engine_put_keys_count in &engine_put_keys_counts {
                put_configs.push(PutConfig {
                    engine_type: engine_type.clone(),
                    put_count: engine_put_keys_count,
                    value_length,
                });
            }
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
    bench_engines(&mut criterion);
    criterion.final_summary();
}
