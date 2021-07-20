// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.
//use fail::fail_point;
use std::sync::atomic::{AtomicI32, Ordering};
#[derive(Copy, Clone)]
pub enum DiskStatus {
    DiskNormal = 0,
    DiskThd1 = 1,
    DiskThd2 = 2,
}

static DISK_STATUS: AtomicI32 = AtomicI32::new(0);

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
pub fn is_disk_threshold_1(status: DiskStatus) -> bool {
    matches!(status, DiskStatus::DiskThd1)
}

pub fn is_disk_threshold_2(status: DiskStatus) -> bool {
    matches!(status, DiskStatus::DiskThd2)
}

pub fn is_disk_full() -> bool {
    let s = DISK_STATUS.load(Ordering::Acquire);
    !matches!(s, 0)
}

// pub fn disk_full_precheck(_store_id: u64) -> bool {
//     fail_point!("disk_full_peer_1", _store_id == 1, |_| true);
//     fail_point!("disk_full_peer_2", _store_id == 2, |_| true);
//     false
// }
