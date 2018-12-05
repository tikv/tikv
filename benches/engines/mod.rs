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
use test_util::KvGenerator;
use tikv::storage::engine::{
    BTreeEngine, Engine, Modify, RocksEngine, Snapshot, TestEngineBuilder,
};
use tikv::storage::{Key, Value, CF_DEFAULT};

const DEFAULT_KEY_LENGTH: usize = 64;
const DEFAULT_GET_KEYS_COUNT: usize = 1;
const DEFAULT_PUT_KVS_COUNT: usize = 1;
const DEFAULT_KV_GENERATOR_SEED: u64 = 0;

trait EngineFactory<E: Engine>: Clone + Copy + fmt::Debug + 'static {
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

fn fill_engine_with<E: Engine>(engine: &E, expect_engine_keys_count: usize, value_length: usize) {
    if expect_engine_keys_count > 0 {
        let mut modifies: Vec<Modify> = vec![];
        let kvs =
            KvGenerator::with_seed(DEFAULT_KEY_LENGTH, value_length, DEFAULT_KV_GENERATOR_SEED)
                .generate(expect_engine_keys_count);
        for (key, value) in kvs {
            modifies.push(Modify::Put(CF_DEFAULT, Key::from_raw(&key), value))
        }
        let ctx = Context::new();
        let _ = engine.async_write(&ctx, modifies, Box::new(move |(_, _)| {}));
    }
}

#[derive(Debug)]
struct PutConfig<F> {
    factory: F,
    put_count: usize,
    value_length: usize,
}

fn bench_engine_put<E: Engine, F: EngineFactory<E>>(bencher: &mut Bencher, config: &PutConfig<F>) {
    let engine = config.factory.build();
    let ctx = Context::new();
    bencher.iter_with_setup(
        || {
            let test_kvs: Vec<(Key, Value)> = KvGenerator::with_seed(
                DEFAULT_KEY_LENGTH,
                config.value_length,
                DEFAULT_KV_GENERATOR_SEED,
            ).generate(config.put_count)
                .iter()
                .map(|(key, value)| (Key::from_raw(&key), value.clone()))
                .collect();
            (test_kvs, &ctx)
        },
        |(test_kvs, ctx)| {
            for (key, value) in test_kvs {
                black_box(engine.put(ctx, key, value).is_ok());
            }
        },
    );
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
    let ctx = Context::new();
    fill_engine_with(&engine, config.engine_keys_count, config.engine_keys_count);
    bencher.iter(|| black_box(&engine).snapshot(black_box(&ctx)).unwrap());
}

#[derive(Debug)]
struct GetConfig<F> {
    factory: F,
    get_count: usize,
    value_length: usize,
    engine_keys_count: usize,
}

fn bench_engine_get<E: Engine, F: EngineFactory<E>>(bencher: &mut Bencher, config: &GetConfig<F>) {
    let engine = config.factory.build();
    let ctx = Context::new();
    fill_engine_with(&engine, config.engine_keys_count, config.value_length);
    let test_kvs: Vec<Key> = KvGenerator::with_seed(
        DEFAULT_KEY_LENGTH,
        config.value_length,
        DEFAULT_KV_GENERATOR_SEED,
    ).generate(config.get_count)
        .iter()
        .map(|(key, _)| Key::from_raw(&key))
        .collect();

    bencher.iter_with_setup(
        || {
            let snap = engine.snapshot(&ctx).unwrap();
            (snap, &test_kvs)
        },
        |(snap, test_kvs)| {
            for key in test_kvs {
                black_box(snap.get(key).unwrap());
            }
        },
    );
}

fn bench_engines<E: Engine, F: EngineFactory<E>>(c: &mut Criterion, factory: F) {
    let value_lengths = vec![64];
    let engine_entries_counts = vec![0];
    let engine_put_kv_counts = vec![DEFAULT_PUT_KVS_COUNT];
    let engine_get_key_counts = vec![DEFAULT_GET_KEYS_COUNT];

    let mut get_configs = vec![];
    let mut put_configs = vec![];
    let mut snapshot_configs = vec![];

    for &value_length in &value_lengths {
        for &engine_keys_count in &engine_entries_counts {
            for &get_count in &engine_get_key_counts {
                get_configs.push(GetConfig {
                    factory,
                    get_count,
                    value_length,
                    engine_keys_count,
                });
            }
            snapshot_configs.push(SnapshotConfig {
                factory,
                value_length,
                engine_keys_count,
            });
        }

        for &put_count in &engine_put_kv_counts {
            put_configs.push(PutConfig {
                factory,
                put_count,
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
    let mut criterion = Criterion::default();
    bench_engines(&mut criterion, RocksEngineFactory {});
    bench_engines(&mut criterion, BTreeEngineFactory {});
    criterion.final_summary();
}
