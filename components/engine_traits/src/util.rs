// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

use super::{Error, Result};

/// Check if key in range [`start_key`, `end_key`).
#[allow(dead_code)]
pub fn check_key_in_range(
    key: &[u8],
    region_id: u64,
    start_key: &[u8],
    end_key: &[u8],
) -> Result<()> {
    if key >= start_key && (end_key.is_empty() || key < end_key) {
        Ok(())
    } else {
        Err(Error::NotInRange(
            key.to_vec(),
            region_id,
            start_key.to_vec(),
            end_key.to_vec(),
        ))
    }
}
