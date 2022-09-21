// Copyright 2016 TiKV Project Authors. Licensed under Apache-2.0.

use kvproto::metapb::Region;

/// Check if key in region range (`start_key`, `end_key`).
pub fn check_key_in_region_exclusive(key: &[u8], region: &Region) -> bool {
    let end_key = region.get_end_key();
    let start_key = region.get_start_key();
    start_key < key && (key < end_key || end_key.is_empty())
}

/// Check if key in region range [`start_key`, `end_key`].
pub fn check_key_in_region_inclusive(key: &[u8], region: &Region) -> bool {
    let end_key = region.get_end_key();
    let start_key = region.get_start_key();
    key >= start_key && (end_key.is_empty() || key <= end_key)
}

/// Check if key in region range [`start_key`, `end_key`).
pub fn check_key_in_region(key: &[u8], region: &Region) -> bool {
    let end_key = region.get_end_key();
    let start_key = region.get_start_key();
    key >= start_key && (end_key.is_empty() || key < end_key)
}

/// Check if replicas of two regions are on the same stores.
pub fn region_on_same_stores(lhs: &Region, rhs: &Region) -> bool {
    if lhs.get_peers().len() != rhs.get_peers().len() {
        return false;
    }

    // Because every store can only have one replica for the same region,
    // so just one round check is enough.
    lhs.get_peers().iter().all(|lp| {
        rhs.get_peers()
            .iter()
            .any(|rp| rp.get_store_id() == lp.get_store_id() && rp.get_role() == lp.get_role())
    })
}
