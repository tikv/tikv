// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

use crate::{db_options::RocksTitanDBOptions, sst_partitioner::RocksSstPartitionerFactory, level_region_accessor::RocksLevelRegionAccessor};
use engine_traits::{ColumnFamilyOptions, SstPartitionerFactory, LevelRegionAccessor};
use rocksdb::ColumnFamilyOptions as RawCFOptions;

#[derive(Clone)]
pub struct RocksColumnFamilyOptions(RawCFOptions);

impl RocksColumnFamilyOptions {
    pub fn from_raw(raw: RawCFOptions) -> RocksColumnFamilyOptions {
        RocksColumnFamilyOptions(raw)
    }

    pub fn into_raw(self) -> RawCFOptions {
        self.0
    }
}

impl ColumnFamilyOptions for RocksColumnFamilyOptions {
    type TitanDBOptions = RocksTitanDBOptions;

    fn new() -> Self {
        RocksColumnFamilyOptions::from_raw(RawCFOptions::new())
    }

    fn get_level_zero_slowdown_writes_trigger(&self) -> u32 {
        self.0.get_level_zero_slowdown_writes_trigger()
    }

    fn get_level_zero_stop_writes_trigger(&self) -> u32 {
        self.0.get_level_zero_stop_writes_trigger()
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

    fn set_block_cache_capacity(&self, capacity: u64) -> Result<(), String> {
        self.0.set_block_cache_capacity(capacity)
    }

    fn set_titandb_options(&mut self, opts: &Self::TitanDBOptions) {
        self.0.set_titandb_options(opts.as_raw())
    }

    fn get_target_file_size_base(&self) -> u64 {
        self.0.get_target_file_size_base()
    }

    fn get_disable_auto_compactions(&self) -> bool {
        self.0.get_disable_auto_compactions()
    }

    fn set_sst_partitioner_factory<F: SstPartitionerFactory>(&mut self, factory: F) {
        self.0
            .set_sst_partitioner_factory(RocksSstPartitionerFactory(factory));
    }
    fn set_level_region_accessor<A: LevelRegionAccessor>(&mut self, accessor: A) {
        self.0
            .set_level_region_accessor(RocksLevelRegionAccessor(accessor));
    }
}
