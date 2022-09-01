// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

use std::collections::HashMap;

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
        Err(Error::NotInRange {
            key: key.to_vec(),
            region_id,
            start: start_key.to_vec(),
            end: end_key.to_vec(),
        })
    }
}

#[derive(Serialize, Deserialize)]
pub struct FlushedSeqno {
    seqno: HashMap<String, u64>,
    min_seqno: u64,
}

impl FlushedSeqno {
    pub fn new(cfs: &[&str], min_seqno: u64) -> Self {
        let mut seqno = HashMap::new();
        for cf in cfs {
            seqno.insert(cf.to_string(), 0);
        }
        Self { seqno, min_seqno }
    }

    pub fn update(&mut self, cf: &str, seqno: u64) -> Option<u64> {
        self.seqno
            .entry(cf.to_string())
            .and_modify(|v| *v = u64::max(*v, seqno))
            .or_insert(seqno);
        let min = self.seqno.values().min().copied();
        match min {
            Some(min) if min > self.min_seqno => {
                self.min_seqno = min;
                Some(min)
            }
            _ => None,
        }
    }

    pub fn min_seqno(&self) -> u64 {
        self.min_seqno
    }
}
