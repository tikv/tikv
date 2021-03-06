// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

use crate::errors::Result;

#[derive(Debug, Default)]
pub struct TTLProperties {
    pub max_expire_ts: u64,
    pub min_expire_ts: u64,
}

pub trait TTLPropertiesExt {
    fn get_range_ttl_properties_cf(
        &self,
        cf: &str,
        start_key: &[u8],
        end_key: &[u8],
    ) -> Result<Vec<(String, TTLProperties)>>;
}
