// Copyright 2022 TiKV Project Authors. Licensed under Apache-2.0.

use std::{sync::Arc, time::Duration};

use causal_ts::{BatchTsoProvider, CausalTsProvider, TsoBatchList};
use criterion::*;
use futures::executor::block_on;
use test_pd_client::TestPdClient;
use txn_types::TimeStamp;

fn bench_batch_tso_list_pop(c: &mut Criterion) {
    const CAPACITY: u64 = 10_000;
    let cases = vec![("100", 100), ("10k", 10_000)]; // (id, batch_size)

    let bench_func = |b: &mut Bencher<'_>, batch_size: u64| {
        let batch_list = TsoBatchList::new(CAPACITY as u32);
        b.iter_batched(
            || {
                batch_list.flush();
                for i in 0..CAPACITY {
                    batch_list
                        .push(
                            batch_size as u32,
                            TimeStamp::compose(i as u64, batch_size),
                            false,
                        )
                        .unwrap();
                }
            },
            |_| {
                black_box(batch_list.pop(None).unwrap());
            },
            BatchSize::NumIterations(CAPACITY * batch_size),
        )
    };

    let mut group = c.benchmark_group("batch_tso_list_pop");
    for (id, batch_size) in cases {
        group.bench_function(id, |b| {
            bench_func(b, batch_size);
        });
    }
}

fn bench_batch_tso_list_push(c: &mut Criterion) {
    const BATCH_SIZE: u64 = 8192;
    let cases = vec![("50", 50), ("1024", 1024)]; // (id, capacity)

    let bench_func = |b: &mut Bencher<'_>, capacity: u64| {
        let batch_list = TsoBatchList::new(capacity as u32);
        let mut i = 0;
        b.iter(|| {
            i += 1;
            black_box(
                batch_list
                    .push(
                        BATCH_SIZE as u32,
                        TimeStamp::compose(i as u64, BATCH_SIZE),
                        false,
                    )
                    .unwrap(),
            );
        })
    };

    let mut group = c.benchmark_group("batch_tso_list_push");
    for (id, capacity) in cases {
        group.bench_function(id, |b| {
            bench_func(b, capacity);
        });
    }
}

fn bench_batch_tso_provider_get_ts(c: &mut Criterion) {
    let pd_cli = Arc::new(TestPdClient::new(1, false));

    // Disable background renew by setting `renew_interval` to 0 to make test result
    // stable.
    let provider = block_on(BatchTsoProvider::new_opt(
        pd_cli,
        Duration::ZERO,
        Duration::from_secs(1), // cache_multiplier = 10
        100,
        80000,
    ))
    .unwrap();

    c.bench_function("bench_batch_tso_provider_get_ts", |b| {
        b.iter(|| {
            black_box(provider.get_ts().unwrap());
        })
    });
}

fn bench_batch_tso_provider_flush(c: &mut Criterion) {
    let pd_cli = Arc::new(TestPdClient::new(1, false));

    // Disable background renew by setting `renew_interval` to 0 to make test result
    // stable.
    let provider = block_on(BatchTsoProvider::new_opt(
        pd_cli,
        Duration::ZERO,
        Duration::from_secs(1), // cache_multiplier = 10
        100,
        80000,
    ))
    .unwrap();

    c.bench_function("bench_batch_tso_provider_flush", |b| {
        b.iter(|| {
            black_box(provider.flush()).unwrap();
        })
    });
}

criterion_group!(
    benches,
    bench_batch_tso_list_pop,
    bench_batch_tso_list_push,
    bench_batch_tso_provider_get_ts,
    bench_batch_tso_provider_flush,
);
criterion_main!(benches);
