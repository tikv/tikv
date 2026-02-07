// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

use std::{
    mem::{ManuallyDrop, forget},
    thread,
};

use concurrency_manager::ConcurrencyManager;
use futures::executor::block_on;
use rand::prelude::*;
use txn_types::{Key, Lock, LockType};

// This test is heavy so we shouldn't run it daily.
// Run it with the following command (recommending release mode) and see the
// printed stats:
//
// ```
// cargo test --package concurrency_manager --test memory_usage --features jemalloc --release -- test_memory_usage --exact --ignored --nocapture
// ```
#[test]
#[ignore]
fn test_memory_usage() {
    // Make key as small as possible, so concurrent writes in the scheduler
    // can be as many as possible. Then, we can maximize the amplication
    // caused by the data structure.
    const KEY_LEN: usize = 16;
    // We allow 100MB pending writes in the scheduler.
    const LOCK_COUNT: usize = 100 * 1024 * 1024 / KEY_LEN;

    // Let system collect the memory to avoid drop time at the end of the test.
    let cm = ManuallyDrop::new(ConcurrencyManager::new_for_test(1.into()));

    const THR_NUM: usize = 8;
    let mut ths = Vec::with_capacity(THR_NUM);
    for _ in 0..THR_NUM {
        let cm = cm.clone();
        let th = thread::spawn(move || {
            for _ in 0..(LOCK_COUNT / THR_NUM) {
                let mut raw = vec![0; KEY_LEN];
                thread_rng().fill_bytes(&mut raw[..]);
                let key = Key::from_raw(&raw);
                let lock = Lock::new(
                    LockType::Put,
                    raw,
                    10.into(),
                    1000,
                    None,
                    10.into(),
                    1,
                    20.into(),
                    false,
                );

                // Key already exists
                if cm.read_key_check(&key, |_| Err(())).is_err() {
                    continue;
                }

                let guard = block_on(cm.lock_key(&key));
                guard.with_lock(|l| {
                    *l = Some(lock);
                });
                forget(guard);
            }
        });
        ths.push(th);
    }

    ths.into_iter().for_each(|th| th.join().unwrap());

    println!("{:?}", tikv_alloc::fetch_stats());
}

// Stress test for crossbeam-skiplist RefRange memory leak.
// Run with:
// cargo test -p concurrency_manager --test memory_usage --release --
// stress_skipmap_range_iter --ignored --nocapture
#[test]
#[ignore]
fn stress_skipmap_range_iter() {
    use std::{
        sync::{
            Arc,
            atomic::{AtomicBool, AtomicU64, Ordering},
        },
        time::{Duration, Instant},
    };

    use crossbeam_skiplist::SkipMap;

    let map = Arc::new(SkipMap::<u64, u64>::new());
    let ops = Arc::new(AtomicU64::new(0));
    let stop = Arc::new(AtomicBool::new(false));
    let key_space = 10000_u64;
    let duration_secs = 10_u64;
    let idle_secs = 5_u64;
    let threads = thread::available_parallelism()
        .map(|n| n.get())
        .unwrap_or(4);

    println!(
        "config: threads={} key_space={} duration={}s idle={}s",
        threads, key_space, duration_secs, idle_secs
    );

    let start = Instant::now();
    let monitor = {
        let map = map.clone();
        let ops = ops.clone();
        let stop = stop.clone();
        thread::spawn(move || {
            while !stop.load(Ordering::Relaxed) {
                thread::sleep(Duration::from_secs(1));
                let allocated = tikv_alloc::fetch_stats()
                    .ok()
                    .and_then(|stats| stats)
                    .and_then(|stats| {
                        stats
                            .iter()
                            .find(|(k, _)| *k == "allocated")
                            .map(|(_, v)| *v)
                    })
                    .unwrap_or(0);
                println!(
                    "t={:.0}s ops={} len={} alloc={}MB",
                    start.elapsed().as_secs_f64(),
                    ops.load(Ordering::Relaxed),
                    map.len(),
                    allocated / 1024 / 1024
                );
            }
        })
    };

    let handles: Vec<_> = (0..threads)
        .map(|_| {
            let map = map.clone();
            let ops = ops.clone();
            let stop = stop.clone();
            thread::spawn(move || {
                while !stop.load(Ordering::Relaxed) {
                    for i in 0..key_space {
                        map.insert(i, i);
                    }
                    for entry in map.range(0..key_space) {
                        std::hint::black_box(entry.value());
                    }
                    for i in 0..key_space {
                        map.remove(&i);
                    }
                    ops.fetch_add(1, Ordering::Relaxed);
                }
            })
        })
        .collect();

    thread::sleep(Duration::from_secs(duration_secs));
    stop.store(true, Ordering::Relaxed);

    for h in handles {
        h.join().unwrap();
    }

    println!("workload done, waiting {}s for GC...", idle_secs);
    for _ in 0..idle_secs {
        thread::sleep(Duration::from_secs(1));
        let _ = map.get(&0);
        let allocated = tikv_alloc::fetch_stats()
            .ok()
            .and_then(|stats| stats)
            .and_then(|stats| {
                stats
                    .iter()
                    .find(|(k, _)| *k == "allocated")
                    .map(|(_, v)| *v)
            })
            .unwrap_or(0);
        println!(
            "idle: len={} alloc={}MB",
            map.len(),
            allocated / 1024 / 1024
        );
    }

    monitor.join().unwrap();
    let allocated = tikv_alloc::fetch_stats()
        .ok()
        .and_then(|stats| stats)
        .and_then(|stats| {
            stats
                .iter()
                .find(|(k, _)| *k == "allocated")
                .map(|(_, v)| *v)
        })
        .unwrap_or(0);
    println!(
        "final: ops={} len={} alloc={}MB",
        ops.load(Ordering::Relaxed),
        map.len(),
        allocated / 1024 / 1024
    );
}
