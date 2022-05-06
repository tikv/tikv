// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

#![feature(test)]
#![feature(bench_black_box)]

use std::{borrow::Cow, hint::black_box, mem::forget};

use concurrency_manager::ConcurrencyManager;
use criterion::*;
use futures::executor::block_on;
use kvproto::kvrpcpb::IsolationLevel;
use rand::prelude::*;
use txn_types::{Key, Lock, LockType, TsSet};

const KEY_LEN: usize = 64;
const LOCK_COUNT: usize = 10_000;

fn prepare_cm() -> ConcurrencyManager {
    let cm = ConcurrencyManager::new(1.into());
    let mut buf = [0; KEY_LEN];
    for _ in 0..LOCK_COUNT {
        thread_rng().fill_bytes(&mut buf[..]);
        let key = Key::from_raw(&buf);
        // Really rare to generate duplicate key. Do not consider it for simplicity.
        let guard = block_on(cm.lock_key(&key));
        guard.with_lock(|l| {
            *l = Some(Lock::new(
                LockType::Put,
                buf.to_vec(),
                10.into(),
                1000,
                None,
                10.into(),
                1,
                20.into(),
            ));
        });
        // Leak the guard so the lock won't be removed.
        // It doesn't matter because leaked memory is not too much.
        forget(guard);
    }
    cm
}

fn point_check_baseline(c: &mut Criterion) {
    let mut buf = [0; KEY_LEN];
    c.bench_function("point_check_baseline", |b| {
        b.iter(|| {
            thread_rng().fill_bytes(&mut buf[..]);
            black_box(Key::from_raw(&buf));
        })
    });
}

fn bench_point_check(c: &mut Criterion) {
    let cm = prepare_cm();
    let mut buf = [0; 64];
    let ts_set = TsSet::Empty;
    c.bench_function("point_check_10k_locks", |b| {
        b.iter(|| {
            thread_rng().fill_bytes(&mut buf[..]);
            let key = Key::from_raw(&buf);
            let _ = cm.read_key_check(&key, |l| {
                Lock::check_ts_conflict(
                    Cow::Borrowed(l),
                    &key,
                    1.into(),
                    &ts_set,
                    IsolationLevel::Si,
                )
            });
        })
    });
}

fn range_check_baseline(c: &mut Criterion) {
    c.bench_function("range_check_baseline", |b| {
        b.iter(|| {
            let start = thread_rng().gen_range(0u8..245);
            black_box(Key::from_raw(&[start]));
            black_box(Key::from_raw(&[start + 25]));
        })
    });
}

fn bench_range_check(c: &mut Criterion) {
    let cm = prepare_cm();
    let ts_set = TsSet::Empty;
    c.bench_function("range_check_1k_in_10k_locks", |b| {
        b.iter(|| {
            let start = thread_rng().gen_range(0u8..230);
            let start_key = Key::from_raw(&[start]);
            let end_key = Key::from_raw(&[start + 25]);
            // The key range is roughly 1/10 the key space.
            let _ = cm.read_range_check(Some(&start_key), Some(&end_key), |key, l| {
                Lock::check_ts_conflict(
                    Cow::Borrowed(l),
                    key,
                    1.into(),
                    &ts_set,
                    IsolationLevel::Si,
                )
            });
        })
    });
}

criterion_group!(
    benches,
    point_check_baseline,
    bench_point_check,
    range_check_baseline,
    bench_range_check
);
criterion_main!(benches);
