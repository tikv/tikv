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

mod bench_btree_engine;
mod bench_rocksdb;

use test::Bencher;

use kvproto::kvrpcpb::Context;
use tikv::storage::engine::{Engine, Modify, Snapshot};

use test_util::generate_random_kvs;
use tikv::storage::{Key, CF_DEFAULT};

const KEY_LENGTH: usize = 64;
const VALUE_LENGTH: usize = 128;
const ITERATIONS: usize = 1000;

fn prepare_engine<E: Engine>(
    engine: &E,
    target_kvs: &[(Vec<u8>, Vec<u8>)],
    add_target_kvs_to_engine: bool,
    expect_engine_size: usize,
) {
    let mut gap_size = expect_engine_size;
    let mut modifies: Vec<Modify> = vec![];
    if add_target_kvs_to_engine {
        for (key, value) in target_kvs {
            if gap_size == 0 {
                break;
            }
            modifies.push(Modify::Put(CF_DEFAULT, Key::from_raw(&key), value.clone()));
            gap_size -= 1;
        }
    }
    if gap_size > 0 {
        let kvs = generate_random_kvs(gap_size, KEY_LENGTH, VALUE_LENGTH);
        for (key, value) in kvs {
            modifies.push(Modify::Put(CF_DEFAULT, Key::from_raw(&key), value))
        }
    }
    let ctx = Context::new();
    let _ = engine.async_write(&ctx, modifies, Box::new(move |(_, _)| {}));
}

/// Measuring the performance of Engine::snapshot()
fn engine_snapshot_bench<E: Engine>(
    engine: &E,
    iterations: usize,
    engine_size: usize,
    bencher: &mut Bencher,
) {
    prepare_engine(engine, &[], false, engine_size);
    let ctx = Context::new();

    bencher.iter(|| {
        for _ in 0..iterations {
            engine.snapshot(&ctx).is_ok();
        }
    })
}

/// Actually, it measures the performance of Snapshot::get(), skipping the Engine::snapshot();
fn engine_get_bench<E: Engine>(
    engine: &E,
    iterations: usize,
    engine_size: usize,
    add_target_kvs_to_engine: bool,
    bencher: &mut Bencher,
) {
    let target_kvs = generate_random_kvs(iterations, KEY_LENGTH, VALUE_LENGTH);
    prepare_engine(engine, &target_kvs, add_target_kvs_to_engine, engine_size);
    let ctx = Context::new();

    bencher.iter(|| {
        let snap = engine.snapshot(&ctx).unwrap(); // engine.snapshot() is not the point.
        for (key, _) in &target_kvs {
            snap.get(&Key::from_raw(key)).is_ok();
        }
    });
}

/// Measure the performance of Engine::put()
fn engine_put_bench<E: Engine>(engine: &E, write_size: usize, bencher: &mut Bencher) {
    let target_kvs = generate_random_kvs(write_size, KEY_LENGTH, VALUE_LENGTH);
    let ctx = Context::new();
    bencher.iter(|| {
        for (key, value) in &target_kvs {
            let _ = engine.put(&ctx, Key::from_raw(&key), value.clone());
        }
    })
}
