// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

use std::ops::{Deref, DerefMut};

use engine_traits::{CfOptions, CfOptionsExt, Result, SstPartitionerFactory};
use rocksdb::ColumnFamilyOptions as RawCfOptions;
use tikv_util::box_err;

use crate::{
    db_options::RocksTitanDbOptions, engine::RocksEngine, r2e,
    sst_partitioner::RocksSstPartitionerFactory, util,
};

impl CfOptionsExt for RocksEngine {
    type CfOptions = RocksCfOptions;

    fn get_options_cf(&self, cf: &str) -> Result<Self::CfOptions> {
        let handle = util::get_cf_handle(self.as_inner(), cf)?;
        Ok(RocksCfOptions::from_raw(
            self.as_inner().get_options_cf(handle),
        ))
    }

    fn set_options_cf(&self, cf: &str, options: &[(&str, &str)]) -> Result<()> {
        let handle = util::get_cf_handle(self.as_inner(), cf)?;
        self.as_inner()
            .set_options_cf(handle, options)
            .map_err(|e| box_err!(e))
    }
}

#[derive(Default, Clone)]
pub struct RocksCfOptions(RawCfOptions);

impl RocksCfOptions {
    pub fn from_raw(raw: RawCfOptions) -> RocksCfOptions {
        RocksCfOptions(raw)
    }

    pub fn into_raw(self) -> RawCfOptions {
        self.0
    }
}

impl Deref for RocksCfOptions {
    type Target = RawCfOptions;

    #[inline]
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl DerefMut for RocksCfOptions {
    #[inline]
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

impl CfOptions for RocksCfOptions {
    type TitanDbOptions = RocksTitanDbOptions;

    fn new() -> Self {
        RocksCfOptions::from_raw(RawCfOptions::default())
    }

    fn get_max_write_buffer_number(&self) -> u32 {
        self.0.get_max_write_buffer_number()
    }

    fn get_level_zero_slowdown_writes_trigger(&self) -> u32 {
        self.0.get_level_zero_slowdown_writes_trigger()
    }

    fn get_level_zero_stop_writes_trigger(&self) -> u32 {
        self.0.get_level_zero_stop_writes_trigger()
    }

    fn set_level_zero_file_num_compaction_trigger(&mut self, v: i32) {
        self.0.set_level_zero_file_num_compaction_trigger(v)
    }

    fn get_soft_pending_compaction_bytes_limit(&self) -> u64 {
        self.0.get_soft_pending_compaction_bytes_limit()
    }

    fn get_hard_pending_compaction_bytes_limit(&self) -> u64 {
        self.0.get_hard_pending_compaction_bytes_limit()
    }

    fn get_block_cache_capacity(&self) -> u64 {
        self.0.get_block_cache_capacity()
    }

    fn set_block_cache_capacity(&self, capacity: u64) -> Result<()> {
        self.0.set_block_cache_capacity(capacity).map_err(r2e)
    }

    fn set_titandb_options(&mut self, opts: &Self::TitanDbOptions) {
        self.0.set_titandb_options(opts.as_raw())
    }

    fn get_target_file_size_base(&self) -> u64 {
        self.0.get_target_file_size_base()
    }

    fn set_disable_auto_compactions(&mut self, v: bool) {
        self.0.set_disable_auto_compactions(v)
    }

    fn get_disable_auto_compactions(&self) -> bool {
        self.0.get_disable_auto_compactions()
    }

    fn get_disable_write_stall(&self) -> bool {
        self.0.get_disable_write_stall()
    }

    fn set_sst_partitioner_factory<F: SstPartitionerFactory>(&mut self, factory: F) {
        self.0
            .set_sst_partitioner_factory(RocksSstPartitionerFactory(factory));
    }
}
