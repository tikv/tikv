// Copyright 2025 TiKV Project Authors. Licensed under Apache-2.0.
//
// Proper criterion benchmark for PeerPessimisticLocks shrink_to_fit mechanism

use std::{collections::HashMap, hint::black_box, sync::Mutex};

use criterion::{criterion_group, criterion_main, BatchSize, Criterion};
use raftstore::store::{txn_ext::GLOBAL_MEM_SIZE, PeerPessimisticLocks};
use rand::{Rng, SeedableRng};
use txn_types::{Key, LastChange, PessimisticLock};

lazy_static::lazy_static! {
    static ref TEST_MUTEX: Mutex<()> = Mutex::new(());
}

// Benchmark configuration: (threshold, factor, defer_ops, min_ops_between,
// name)
type BenchConfig = (usize, usize, u32, u32, &'static str);

// Shared configurations across all benchmarks
const BENCHMARK_CONFIGS: &[BenchConfig] = &[
    (usize::MAX, 1, 0, 0, "no_shrinking"),  // Disable shrinking
    (16, 4, 0, 0, "immediate"),             // Immediate shrinking (defer_ops=0)
    (16, 4, 10, 20, "deferred_aggressive"), // More aggressive threshold
    // (16, 4, 20, 200, "deferred_balanced"),      // Balanced deferred shrinking
    (32, 8, 100, 400, "deferred_conservative"), // Conservative approach
];

fn create_test_lock(primary: &[u8]) -> PessimisticLock {
    PessimisticLock {
        primary: primary.to_vec().into_boxed_slice(),
        start_ts: 100.into(),
        ttl: 3000,
        for_update_ts: 110.into(),
        min_commit_ts: 110.into(),
        last_change: LastChange::make_exist(105.into(), 2),
        is_locked_with_conflict: false,
    }
}

/// Fast key generation using integer encoding for benchmarks
fn create_key_fast(id: u32) -> Key {
    // Most efficient approach - encode the ID as bytes directly without string
    // formatting
    let mut key_bytes = Vec::with_capacity(5); // 'k' + 4 bytes for u32
    key_bytes.push(b'k');
    key_bytes.extend_from_slice(&id.to_be_bytes());
    Key::from_encoded(key_bytes)
}

fn bench_random_workload(c: &mut Criterion) {
    let _guard = TEST_MUTEX.lock().unwrap();
    GLOBAL_MEM_SIZE.set(0);

    let mut group = c.benchmark_group("random_workload");

    for (threshold, factor, defer_ops, min_ops_between, name) in BENCHMARK_CONFIGS.iter() {
        group.bench_function(*name, |b| {
            b.iter_batched(
                || {
                    // Reset global memory counter for each iteration
                    GLOBAL_MEM_SIZE.set(0);

                    let mut locks = PeerPessimisticLocks::with_shrink_config(
                        *threshold,
                        *factor,
                        *defer_ops,
                        *min_ops_between,
                    );
                    let rng = rand::rngs::StdRng::seed_from_u64(12345);
                    let mut active_keys = Vec::new();
                    let mut next_key_id = 0;

                    // Pre-fill with some initial data using fast key creation
                    for _ in 0..100 {
                        let key = create_key_fast(next_key_id);
                        next_key_id += 1;
                        locks
                            .insert(vec![(key.clone(), create_test_lock(b"primary"))])
                            .unwrap();
                        active_keys.push(key);
                    }

                    (locks, rng, active_keys, next_key_id)
                },
                |(mut locks, mut rng, mut active_keys, mut next_key_id)| {
                    // Ensure clean state for this iteration
                    const LIMIT: usize = 500;
                    const OPERATIONS: usize = 1000;

                    for _ in 0..OPERATIONS {
                        if locks.len() >= LIMIT {
                            // Must remove when at limit
                            if !active_keys.is_empty() {
                                let idx = rng.gen_range(0..active_keys.len());
                                let key = active_keys.swap_remove(idx);
                                locks.remove(&key);
                            }
                        } else {
                            // Random insert or remove
                            if active_keys.is_empty() || rng.gen_bool(0.5) {
                                // Insert with fast key creation
                                let key = create_key_fast(next_key_id);
                                next_key_id += 1;
                                if locks
                                    .insert(vec![(key.clone(), create_test_lock(b"primary"))])
                                    .is_ok()
                                {
                                    active_keys.push(key);
                                }
                            } else {
                                // Remove
                                let idx = rng.gen_range(0..active_keys.len());
                                let key = active_keys.swap_remove(idx);
                                locks.remove(&key);
                            }
                        }
                    }

                    black_box((locks.len(), locks.capacity()));
                },
                BatchSize::SmallInput,
            );
        });
    }

    group.finish();
}

fn bench_normal_remove(c: &mut Criterion) {
    let _guard = TEST_MUTEX.lock().unwrap();
    GLOBAL_MEM_SIZE.set(0);

    let mut group = c.benchmark_group("normal_remove");

    group.bench_function("remove_all_1000_no_shrink", |b| {
        b.iter_batched(
            || {
                // Reset global memory counter for each iteration
                GLOBAL_MEM_SIZE.set(0);

                // Setup: Normal map with 1000 items, no shrinking enabled
                let mut locks = PeerPessimisticLocks::with_shrink_config(usize::MAX, 1, 0, 0);
                let mut keys = Vec::new();

                for i in 0..1000 {
                    let key = create_key_fast(i);
                    locks
                        .insert(vec![(key.clone(), create_test_lock(b"primary"))])
                        .unwrap();
                    keys.push(key);
                }

                (locks, keys)
            },
            |(mut locks, keys)| {
                // Measure: Remove all keys (no shrinking possible)
                for key in keys {
                    locks.remove(&key);
                }
                black_box((locks.len(), locks.capacity()));
            },
            BatchSize::SmallInput,
        );
    });

    group.bench_function("remove_all_1000_with_shrink", |b| {
        b.iter_batched(
            || {
                // Reset global memory counter for each iteration
                GLOBAL_MEM_SIZE.set(0);

                // Setup: Map with 1000 items, aggressive shrinking enabled
                let mut locks = PeerPessimisticLocks::with_shrink_config(16, 4, 0, 0);
                let mut keys = Vec::new();

                for i in 0..1000 {
                    let key = create_key_fast(i);
                    locks
                        .insert(vec![(key.clone(), create_test_lock(b"primary"))])
                        .unwrap();
                    keys.push(key);
                }

                (locks, keys)
            },
            |(mut locks, keys)| {
                // Measure: Remove all keys (with shrinking triggered)
                for key in keys {
                    locks.remove(&key);
                }
                black_box((locks.len(), locks.capacity()));
            },
            BatchSize::SmallInput,
        );
    });

    group.finish();
}

fn bench_std_hashmap_remove(c: &mut Criterion) {
    let mut group = c.benchmark_group("std_hashmap_remove");

    group.bench_function("remove_all_1000", |b| {
        b.iter_batched(
            || {
                // Setup: Standard HashMap with 1000 items
                let mut map = HashMap::new();
                let mut keys = Vec::new();
                for i in 0..1000 {
                    map.insert(i, i * 2);
                    keys.push(i);
                }
                (map, keys)
            },
            |(mut map, keys)| {
                // Measure: Remove all keys from std HashMap
                for key in keys {
                    map.remove(&key);
                }
                black_box((map.len(), map.capacity()));
            },
            BatchSize::SmallInput,
        );
    });

    group.finish();
}

fn bench_std_hashmap_shrink_to(c: &mut Criterion) {
    let mut group = c.benchmark_group("std_hashmap_shrink_to");

    group.bench_function("shrink_from_1000", |b| {
        b.iter_batched(
            || {
                // Setup: HashMap that needs shrinking
                let mut map = HashMap::new();

                // Fill with 1000 items
                for i in 0..1000 {
                    map.insert(i, i * 2);
                }

                // Remove 95% to create a heavily underutilized map
                for i in 0..950 {
                    map.remove(&i);
                }

                // Now we have 50 items in a map with capacity for ~1000
                map
            },
            |mut map| {
                // Measure: Direct shrink_to operation
                let target_capacity = map.len() * 3 / 2 + 8; // Same formula as our implementation
                map.shrink_to(target_capacity);
                black_box(map.capacity());
            },
            BatchSize::SmallInput,
        );
    });

    group.finish();
}

criterion_group!(
    benches,
    bench_random_workload,
    bench_normal_remove,
    bench_std_hashmap_remove,
    bench_std_hashmap_shrink_to,
);
criterion_main!(benches);
