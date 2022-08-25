// Copyright 2022 TiKV Project Authors. Licensed under Apache-2.0.

use crate::errors::Result;

#[derive(Debug)]
pub struct SeqnoProperties {
    pub largest_seqno: u64,
    pub smallest_seqno: u64,
}

impl Default for SeqnoProperties {
    fn default() -> Self {
        Self {
            largest_seqno: u64::MIN,
            smallest_seqno: u64::MAX,
        }
    }
}

pub trait SeqnoPropertiesExt {
    fn get_range_seqno_properties_cf(
        &self,
        cf: &str,
        start_key: &[u8],
        end_key: &[u8],
    ) -> Result<Option<SeqnoProperties>>;
}
