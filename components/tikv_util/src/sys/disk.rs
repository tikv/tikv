// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.
use fail::fail_point;
use std::sync::atomic::{AtomicI32, AtomicU64, Ordering};

static DISK_RESERVED_SPACE: AtomicU64 = AtomicU64::new(0);

#[derive(Copy, Clone)]
pub enum DiskStatus {
    DiskNormal = 0,
    DiskThd1 = 1,
    DiskThd2 = 2,
}

static DISK_STATUS: AtomicI32 = AtomicI32::new(0);

pub fn set_disk_reserved_space(v: u64) {
    DISK_RESERVED_SPACE.store(v, Ordering::Release)
}

pub fn get_disk_reserved_space() -> u64 {
    DISK_RESERVED_SPACE.load(Ordering::Acquire)
}

pub fn set_disk_status(status: DiskStatus) {
    let s = match status {
        DiskStatus::DiskNormal => 0,
        DiskStatus::DiskThd1 => 1,
        DiskStatus::DiskThd2 => 2,
    };
    DISK_STATUS.store(s, Ordering::Release);
}
pub fn get_disk_status() -> DiskStatus {
    let s = DISK_STATUS.load(Ordering::Acquire);
    match s {
        0 => DiskStatus::DiskNormal,
        1 => DiskStatus::DiskThd1,
        2 => DiskStatus::DiskThd2,
        _ => panic!("Disk Status Value not meet expectations"),
    }
}

// store id only used in ut, 0 means no need to set fail point.
pub fn is_disk_threshold_1(status: DiskStatus, _store_id: u64) -> bool {
    fail_point!("disk_almost_full_peer_1", _store_id == 1, |_| true);
    fail_point!("disk_almost_full_peer_2", _store_id == 2, |_| true);
    matches!(status, DiskStatus::DiskThd1)
}

pub fn is_disk_threshold_2(status: DiskStatus, _store_id: u64) -> bool {
    fail_point!("disk_already_full_peer_1", _store_id == 1, |_| true);
    fail_point!("disk_already_full_peer_2", _store_id == 2, |_| true);
    fail_point!("disk_already_full_peer_3", _store_id == 3, |_| true);
    matches!(status, DiskStatus::DiskThd2)
}

pub fn is_disk_full() -> bool {
    let s = DISK_STATUS.load(Ordering::Acquire);
    !matches!(s, 0)
}
