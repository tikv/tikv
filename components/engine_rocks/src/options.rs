// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

use rocksdb::{
    ColumnFamilyOptions as RawCFOptions, ReadOptions as RawReadOptions,
    WriteOptions as RawWriteOptions,
};

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
        let mut r = RawReadOptions::default();
        r.fill_cache(opts.fill_cache());
        if opts.key_only() {
            r.set_titan_key_only(true);
        }
        if opts.total_order_seek_used() {
            r.set_total_order_seek(true);
        } else if opts.prefix_same_as_start() {
            r.set_prefix_same_as_start(true);
        }
        if let Some(builder) = opts.lower_bound {
            r.set_iterate_lower_bound(builder.build());
        }
        if let Some(builder) = opts.upper_bound {
            r.set_iterate_upper_bound(builder.build());
        }
        RocksReadOptions(r)
    }
}

impl From<&engine_traits::IterOptions> for RocksReadOptions {
    fn from(opts: &engine_traits::IterOptions) -> Self {
        opts.clone().into()
    }
}

pub struct RocksCFOptions(RawCFOptions);

impl RocksCFOptions {
    #[allow(dead_code)]
    pub fn into_raw(self) -> RawCFOptions {
        self.0
    }
}

impl From<engine_traits::CFOptions> for RocksCFOptions {
    fn from(_opts: engine_traits::CFOptions) -> Self {
        RocksCFOptions(RawCFOptions::default())
    }
}

impl From<&engine_traits::CFOptions> for RocksCFOptions {
    fn from(opts: &engine_traits::CFOptions) -> Self {
        opts.clone().into()
    }
}
