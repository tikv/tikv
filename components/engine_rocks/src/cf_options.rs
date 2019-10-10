// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

use engine_traits::CFOptions;
use rocksdb::ColumnFamilyOptions as RawCFOptions;

pub struct RocksCFOptions(RawCFOptions);

impl RocksCFOptions {
    pub fn from_raw(raw: RawCFOptions) -> RocksCFOptions {
        RocksCFOptions(raw)
    }
}

impl CFOptions for RocksCFOptions {
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
}
