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
use test_storage::SyncTestStorageBuilder;
use test_util::KvGenerator;
use tikv::storage::kv::Engine;

use tikv::engine::CF_DEFAULT;
use tikv::storage::{Key, Mutation};

use super::{BenchConfig, EngineFactory, DEFAULT_ITERATIONS};

fn storage_raw_get<E: Engine, F: EngineFactory<E>>(b: &mut Bencher, config: &BenchConfig<F>) {
    let engine = config.engine_factory.build();
    let store = SyncTestStorageBuilder::from_engine(engine).build().unwrap();
    b.iter_with_setup(
        || {
            let kvs = KvGenerator::new(config.key_length, config.value_length)
                .generate(DEFAULT_ITERATIONS);
            let data: Vec<(Context, Vec<u8>)> = kvs
                .iter()
                .map(|(k, _)| (Context::new(), k.clone()))
                .collect();
            (data, &store)
        },
        |(data, store)| {
            for (context, key) in data {
                black_box(store.raw_get(context, CF_DEFAULT.to_owned(), key).unwrap());
            }
        },
    );
}

fn storage_prewrite<E: Engine, F: EngineFactory<E>>(b: &mut Bencher, config: &BenchConfig<F>) {
    let engine = config.engine_factory.build();
    let store = SyncTestStorageBuilder::from_engine(engine).build().unwrap();
    b.iter_with_setup(
        || {
            let kvs = KvGenerator::new(config.key_length, config.value_length)
                .generate(DEFAULT_ITERATIONS);

            let data: Vec<(Context, Vec<Mutation>, Vec<u8>)> = kvs
                .iter()
                .map(|(k, v)| {
                    (
                        Context::new(),
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
    );
}

fn storage_commit<E: Engine, F: EngineFactory<E>>(b: &mut Bencher, config: &BenchConfig<F>) {
    let engine = config.engine_factory.build();
    let store = SyncTestStorageBuilder::from_engine(engine).build().unwrap();
    b.iter_with_setup(
        || {
            let kvs = KvGenerator::new(config.key_length, config.value_length)
                .generate(DEFAULT_ITERATIONS);

            for (k, v) in &kvs {
                store
                    .prewrite(
                        Context::new(),
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
                black_box(store.commit(Context::new(), vec![Key::from_raw(k)], 1, 2)).unwrap();
            }
        },
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
