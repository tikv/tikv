// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.
use fail::fail_point;
use std::sync::atomic::{AtomicI32, AtomicU64, Ordering};

static DISK_RESERVED_SPACE: AtomicU64 = AtomicU64::new(0);

#[derive(Copy, Clone)]
pub enum DiskUsageStatus {
    Normal = 0,
    AlmostFull = 1,
    AlreadyFull = 2,
}

static DISK_STATUS: AtomicI32 = AtomicI32::new(0);

pub fn set_disk_reserved_space(v: u64) {
    DISK_RESERVED_SPACE.store(v, Ordering::Release)
}

pub fn get_disk_reserved_space() -> u64 {
    DISK_RESERVED_SPACE.load(Ordering::Acquire)
}

pub fn set_disk_status(status: DiskUsageStatus) {
    let s = match status {
        DiskUsageStatus::Normal => 0,
        DiskUsageStatus::AlmostFull => 1,
        DiskUsageStatus::AlreadyFull => 2,
    };
    DISK_STATUS.store(s, Ordering::Release);
}
pub fn get_disk_status() -> DiskUsageStatus {
    let s = DISK_STATUS.load(Ordering::Acquire);
    match s {
        0 => DiskUsageStatus::Normal,
        1 => DiskUsageStatus::AlmostFull,
        2 => DiskUsageStatus::AlreadyFull,
        _ => panic!("Disk Status Value not meet expectations"),
    }
}

// store id only used in ut, 0 means no need to set fail point.
pub fn is_disk_almost_full(status: DiskUsageStatus, _store_id: u64) -> bool {
    fail_point!("disk_almost_full_peer_1", _store_id == 1, |_| true);
    fail_point!("disk_almost_full_peer_2", _store_id == 2, |_| true);
    matches!(status, DiskUsageStatus::AlmostFull)
}

pub fn is_disk_already_full(status: DiskUsageStatus, _store_id: u64) -> bool {
    fail_point!("disk_already_full_peer_1", _store_id == 1, |_| true);
    fail_point!("disk_already_full_peer_2", _store_id == 2, |_| true);
    fail_point!("disk_already_full_peer_3", _store_id == 3, |_| true);
    matches!(status, DiskUsageStatus::AlreadyFull)
}

pub fn is_disk_full() -> bool {
    let s = DISK_STATUS.load(Ordering::Acquire);
    !matches!(s, 0)
}
