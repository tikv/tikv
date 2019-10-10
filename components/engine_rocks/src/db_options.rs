// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

use engine_traits::DBOptions;
use rocksdb::DBOptions as RawDBOptions;

pub struct RocksDBOptions(RawDBOptions);

impl RocksDBOptions {
    pub fn from_raw(raw: RawDBOptions) -> RocksDBOptions {
        RocksDBOptions(raw)
    }
}

impl DBOptions for RocksDBOptions {
    fn get_max_background_jobs(&self) -> i32 {
        self.0.get_max_background_jobs()
    }
}
