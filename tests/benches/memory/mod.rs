// Copyright 2024 TiKV Project Authors. Licensed under Apache-2.0.

use std::{
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
    thread,
    time::Duration,
};

use criterion::{black_box, criterion_group, criterion_main, BenchmarkId, Criterion};
use tikv_util::memory::{MemoryQuota, OwnedAllocated};

fn bench_memory_quota_alloc(c: &mut Criterion) {
    let mut group = c.benchmark_group("Alloc Only");

    let bytes = 0b1010100;
    let quota = Arc::new(MemoryQuota::new(bytes - 1));
    let max_quota = Arc::new(MemoryQuota::new(usize::MAX));

    group.bench_function(BenchmarkId::new("Alloc", "ok"), |b| {
        b.iter(|| {
            let _ = black_box(max_quota.alloc(bytes));
        })
    });
    group.bench_function(BenchmarkId::new("Alloc", "fail"), |b| {
        b.iter(|| {
            let _ = black_box(quota.alloc(bytes));
        })
    });

    group.finish();
}

fn bench_memory_quota_alloc_free(c: &mut Criterion) {
    let mut group = c.benchmark_group("Alloc Free");

    let bytes = 0b1010100;
    let quota = Arc::new(MemoryQuota::new(10 * bytes));
    let quota_ = quota.clone();

    group.bench_function(BenchmarkId::new("MemoryQuota", "alloc free"), |b| {
        b.iter(|| {
            let _ = black_box(quota.alloc(bytes));
            quota.free(bytes);
        })
    });
    group.bench_function(BenchmarkId::new("OwnedAllocated", "alloc free"), |b| {
        b.iter(|| {
            let mut owned_quota = OwnedAllocated::new(quota_.clone());
            let _ = black_box(owned_quota.alloc(bytes));
            drop(owned_quota);
        })
    });

    group.finish();
}

fn bench_memory_quota_multi_threads(c: &mut Criterion) {
    memory_quota_multi_threads(c, 32);
    memory_quota_multi_threads(c, 64);
}

fn memory_quota_multi_threads(c: &mut Criterion, total_threads: usize) {
    let threads = total_threads - 1;
    let mut group = c.benchmark_group(format!("{} Threads", total_threads));

    let bytes = 0b1010100;
    let quota = Arc::new(MemoryQuota::new(2 * threads * bytes));

    // Alloc and free by multiple thread.
    let mut handles = Vec::with_capacity(threads);
    let done = Arc::new(AtomicBool::default());
    // Alloc and free take about 20ns on Intel(R) Xeon(R) Gold 6240 CPU @ 2.60GHz.
    let duration = Duration::from_nanos(20);
    let check_interval = Duration::from_millis(500);
    let batch_work_count = check_interval.as_nanos() / duration.as_nanos();
    for _ in 0..threads {
        let quota_ = quota.clone();
        let done_ = done.clone();
        handles.push(thread::spawn(move || {
            loop {
                if done_.load(Ordering::Relaxed) {
                    return;
                }
                for _ in 0..batch_work_count {
                    let _ = black_box(quota_.alloc(bytes));
                    quota_.free(bytes);
                }
            }
        }));
    }

    let quota_ = quota.clone();
    group.bench_function(BenchmarkId::new("MemoryQuota", "alloc free"), |b| {
        b.iter(|| {
            let _ = black_box(quota.alloc(bytes));
            quota.free(bytes);
        })
    });
    group.bench_function(BenchmarkId::new("OwnedAllocated", "alloc free"), |b| {
        b.iter(|| {
            let mut owned_quota = OwnedAllocated::new(quota_.clone());
            let _ = black_box(owned_quota.alloc(bytes));
            drop(owned_quota);
        })
    });

    done.store(true, Ordering::Relaxed);
    let _ = handles.into_iter().map(|h| h.join().unwrap());
    group.finish();
}

criterion_group!(
    benches,
    bench_memory_quota_alloc,
    bench_memory_quota_alloc_free,
    bench_memory_quota_multi_threads,
);

criterion_main!(benches);
