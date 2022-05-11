// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

use crate::{db_options::TitanDBOptions, sst_partitioner::SstPartitionerFactory, Result};

/// Trait for engines with column family options
pub trait CFOptionsExt {
    type ColumnFamilyOptions: ColumnFamilyOptions;

    fn get_options_cf(&self, cf: &str) -> Result<Self::ColumnFamilyOptions>;
    fn set_options_cf(&self, cf: &str, options: &[(&str, &str)]) -> Result<()>;
}

pub trait ColumnFamilyOptions {
    type TitanDBOptions: TitanDBOptions;

    fn new() -> Self;
    fn get_max_write_buffer_number(&self) -> u32;
    fn get_level_zero_slowdown_writes_trigger(&self) -> u32;
    fn get_level_zero_stop_writes_trigger(&self) -> u32;
    fn set_level_zero_file_num_compaction_trigger(&mut self, v: i32);
    fn get_soft_pending_compaction_bytes_limit(&self) -> u64;
    fn get_hard_pending_compaction_bytes_limit(&self) -> u64;
    fn get_block_cache_capacity(&self) -> u64;
    fn set_block_cache_capacity(&self, capacity: u64) -> std::result::Result<(), String>;
    fn set_titandb_options(&mut self, opts: &Self::TitanDBOptions);
    fn get_target_file_size_base(&self) -> u64;
    fn set_disable_auto_compactions(&mut self, v: bool);
    fn get_disable_auto_compactions(&self) -> bool;
    fn get_disable_write_stall(&self) -> bool;
    fn set_sst_partitioner_factory<F: SstPartitionerFactory>(&mut self, factory: F);
}
