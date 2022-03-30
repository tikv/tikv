// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.
use crate::errors::Result;

pub trait FlowControlFactorsExt {
    fn get_cf_num_files_at_level(&self, cf: &str, level: usize) -> Result<Option<u64>>;

    fn get_cf_num_immutable_mem_table(&self, cf: &str) -> Result<Option<u64>>;

    fn get_cf_pending_compaction_bytes(&self, cf: &str) -> Result<Option<u64>>;
}
