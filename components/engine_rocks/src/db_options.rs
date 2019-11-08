// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

use crate::engine::RocksEngine;
use engine_traits::DBOptions;
use engine_traits::DBOptionsExt;
use engine_traits::Result;
use engine_traits::TitanDBOptions;
use rocksdb::DBOptions as RawDBOptions;
use rocksdb::TitanDBOptions as RawTitanDBOptions;

impl DBOptionsExt for RocksEngine {
    type DBOptions = RocksDBOptions;

    fn get_db_options(&self) -> Self::DBOptions {
        RocksDBOptions::from_raw(self.as_inner().get_db_options())
    }
    fn set_db_options(&self, options: &[(&str, &str)]) -> Result<()> {
        self.as_inner()
            .set_db_options(options)
            .map_err(|e| box_err!(e))
    }
}

pub struct RocksDBOptions(RawDBOptions);

impl RocksDBOptions {
    pub fn from_raw(raw: RawDBOptions) -> RocksDBOptions {
        RocksDBOptions(raw)
    }

    pub fn into_raw(self) -> RawDBOptions {
        self.0
    }
}

impl DBOptions for RocksDBOptions {
    type TitanDBOptions = RocksTitanDBOptions;

    fn new() -> Self {
        RocksDBOptions::from_raw(RawDBOptions::new())
    }

    fn get_max_background_jobs(&self) -> i32 {
        self.0.get_max_background_jobs()
    }

    fn set_titandb_options(&mut self, opts: &Self::TitanDBOptions) {
        self.0.set_titandb_options(opts.as_raw())
    }
}

pub struct RocksTitanDBOptions(RawTitanDBOptions);

impl RocksTitanDBOptions {
    pub fn from_raw(raw: RawTitanDBOptions) -> RocksTitanDBOptions {
        RocksTitanDBOptions(raw)
    }

    pub fn as_raw(&self) -> &RawTitanDBOptions {
        &self.0
    }
}

impl TitanDBOptions for RocksTitanDBOptions {
    fn new() -> Self {
        RocksTitanDBOptions::from_raw(RawTitanDBOptions::new())
    }

    fn set_min_blob_size(&mut self, size: u64) {
        self.0.set_min_blob_size(size)
    }
}
