// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

use engine::IterOptionsExt;
use rocksdb::{ReadOptions as RawReadOptions, WriteOptions as RawWriteOptions};

pub struct RocksReadOptions(RawReadOptions);

impl RocksReadOptions {
    pub fn into_raw(self) -> RawReadOptions {
        self.0
    }
}

impl From<engine_traits::ReadOptions> for RocksReadOptions {
    fn from(_opts: engine_traits::ReadOptions) -> Self {
        RocksReadOptions(RawReadOptions::default())
    }
}

impl From<&engine_traits::ReadOptions> for RocksReadOptions {
    fn from(opts: &engine_traits::ReadOptions) -> Self {
        opts.clone().into()
    }
}

pub struct RocksWriteOptions(RawWriteOptions);

impl RocksWriteOptions {
    pub fn into_raw(self) -> RawWriteOptions {
        self.0
    }
}

impl From<engine_traits::WriteOptions> for RocksWriteOptions {
    fn from(opts: engine_traits::WriteOptions) -> Self {
        let mut r = RawWriteOptions::default();
        r.set_sync(opts.sync());
        RocksWriteOptions(r)
    }
}

impl From<&engine_traits::WriteOptions> for RocksWriteOptions {
    fn from(opts: &engine_traits::WriteOptions) -> Self {
        opts.clone().into()
    }
}

impl From<engine_traits::IterOptions> for RocksReadOptions {
    fn from(opts: engine_traits::IterOptions) -> Self {
        let r = opts.build_read_opts();
        RocksReadOptions(r)
    }
}
