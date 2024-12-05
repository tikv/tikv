// Copyright 2024 TiKV Project Authors. Licensed under Apache-2.0.

use std::time::Duration;

use concurrency_manager::ConcurrencyManager;
use criterion::{black_box, criterion_group, criterion_main, Criterion};
use txn_types::TimeStamp;

fn benchmark_update_max_ts(c: &mut Criterion) {
    let latest_ts = TimeStamp::new(1000);
    let limit_valid_time = Duration::from_secs(20);
    let cm = ConcurrencyManager::new_with_config(latest_ts, limit_valid_time, false);

    cm.set_max_ts_limit(TimeStamp::new(2000));

    let new_ts = TimeStamp::new(3000);

    c.bench_function("update_max_ts", |b| {
        b.iter(|| {
            let _ = cm.update_max_ts(black_box(new_ts), Some("benchmark".to_string()));
        })
    });
}

criterion_group!(benches, benchmark_update_max_ts);
criterion_main!(benches);
