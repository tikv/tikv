// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.
use fail::fail_point;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
static DISK_FULL: AtomicBool = AtomicBool::new(false);
static DISK_RESERVED: AtomicU64 = AtomicU64::new(0);

pub fn set_disk_full() {
    DISK_FULL.store(true, Ordering::Release);
}

pub fn clear_disk_full() {
    DISK_FULL.store(false, Ordering::Release);
}

pub fn is_disk_full() -> bool {
    DISK_FULL.load(Ordering::Acquire)
}

pub fn disk_full_precheck(_store_id: u64) -> bool {
    if cfg!(feature = "failpoints") {
        fail_point!("disk_full_peer_1", _store_id == 1, |_| true);
        fail_point!("disk_full_peer_2", _store_id == 2, |_| true);
        false
    } else {
        false
    }
}

pub fn set_disk_reserved(v: u64) {
    DISK_RESERVED.store(v, Ordering::Release);
}

pub fn get_disk_reserved() -> u64 {
    DISK_RESERVED.load(Ordering::Acquire)
}
