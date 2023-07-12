// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

use engine_traits::{CfOptions, CfOptionsExt, Result, SstPartitionerFactory};

use crate::{db_options::PanicTitanDbOptions, engine::PanicEngine};

impl CfOptionsExt for PanicEngine {
    type CfOptions = PanicCfOptions;

    fn get_options_cf(&self, cf: &str) -> Result<Self::CfOptions> {
        panic!()
    }
    fn set_options_cf(&self, cf: &str, options: &[(&str, &str)]) -> Result<()> {
        panic!()
    }
}

pub struct PanicCfOptions;

impl CfOptions for PanicCfOptions {
    type TitanCfOptions = PanicTitanDbOptions;

    fn new() -> Self {
        panic!()
    }
    fn get_max_write_buffer_number(&self) -> u32 {
        panic!()
    }
    fn get_level_zero_slowdown_writes_trigger(&self) -> i32 {
        panic!()
    }
    fn get_level_zero_stop_writes_trigger(&self) -> i32 {
        panic!()
    }
    fn set_level_zero_file_num_compaction_trigger(&mut self, v: i32) {
        panic!()
    }
    fn get_soft_pending_compaction_bytes_limit(&self) -> u64 {
        panic!()
    }
    fn get_hard_pending_compaction_bytes_limit(&self) -> u64 {
        panic!()
    }
    fn get_block_cache_capacity(&self) -> u64 {
        panic!()
    }
    fn set_block_cache_capacity(&self, capacity: u64) -> Result<()> {
        panic!()
    }
    fn set_titan_cf_options(&mut self, opts: &Self::TitanCfOptions) {
        panic!()
    }
    fn get_target_file_size_base(&self) -> u64 {
        panic!()
    }
    fn set_disable_auto_compactions(&mut self, v: bool) {
        panic!()
    }
    fn get_disable_auto_compactions(&self) -> bool {
        panic!()
    }
    fn get_disable_write_stall(&self) -> bool {
        panic!()
    }
    fn set_sst_partitioner_factory<F: SstPartitionerFactory>(&mut self, factory: F) {
        panic!()
    }
    fn set_max_compactions(&self, n: u32) -> Result<()> {
        panic!()
    }
}
