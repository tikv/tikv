// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

use std::{
    mem::{forget, ManuallyDrop},
    thread,
};

use concurrency_manager::ConcurrencyManager;
use futures::executor::block_on;
use rand::prelude::*;
use txn_types::{Key, Lock, LockType};

// This test is heavy so we shouldn't run it daily.
// Run it with the following command (recommending release mode) and see the printed stats:
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
    let cm = ManuallyDrop::new(ConcurrencyManager::new(1.into()));

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
