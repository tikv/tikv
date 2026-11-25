// Copyright 2020 TiKV Project Authors. Licensed under Apache-2.0.

use std::sync::Mutex;

/// Every test creates `store` pollers and some read the process-wide poller
/// metrics, so they run one at a time.
pub static TEST_LOCK: Mutex<()> = Mutex::new(());

pub fn serialize_tests() -> std::sync::MutexGuard<'static, ()> {
    TEST_LOCK.lock().unwrap_or_else(|e| e.into_inner())
}

mod batch;
mod router;
