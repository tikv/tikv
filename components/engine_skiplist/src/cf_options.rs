// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

use crate::db_options::SkiplistTitanDBOptions;
use engine_traits::ColumnFamilyOptions;

pub struct SkiplistColumnFamilyOptions;

impl ColumnFamilyOptions for SkiplistColumnFamilyOptions {
    type TitanDBOptions = SkiplistTitanDBOptions;

    fn new() -> Self {
        SkiplistColumnFamilyOptions
    }
    fn get_level_zero_slowdown_writes_trigger(&self) -> u32 {
        std::u32::MAX
    }
    fn get_level_zero_stop_writes_trigger(&self) -> u32 {
        std::u32::MAX
    }
    fn get_soft_pending_compaction_bytes_limit(&self) -> u64 {
        std::u64::MAX
    }
    fn get_hard_pending_compaction_bytes_limit(&self) -> u64 {
        std::u64::MAX
    }
    fn get_block_cache_capacity(&self) -> u64 {
        std::u64::MAX
    }
    fn set_block_cache_capacity(&self, capacity: u64) -> Result<(), String> {
        Ok(())
    }
    fn set_titandb_options(&mut self, opts: &Self::TitanDBOptions) {}
    fn get_target_file_size_base(&self) -> u64 {
        std::u64::MAX
    }
    fn get_disable_auto_compactions(&self) -> bool {
        false
    }
}
