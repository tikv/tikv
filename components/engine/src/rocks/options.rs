// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

use crate::options::*;

pub use super::{RocksCFOptions, RocksReadOptions, RocksWriteOptions};
use crate::rocks::RocksIngestExternalFileOptions;

impl From<ReadOptions> for RocksReadOptions {
    fn from(_opts: ReadOptions) -> Self {
        let r = RocksReadOptions::default();
        r
    }
}

impl From<&ReadOptions> for RocksReadOptions {
    fn from(opts: &ReadOptions) -> Self {
        opts.clone().into()
    }
}

impl From<WriteOptions> for RocksWriteOptions {
    fn from(opts: WriteOptions) -> Self {
        let mut r = RocksWriteOptions::default();
        r.set_sync(opts.sync);
        r
    }
}

impl From<&WriteOptions> for RocksWriteOptions {
    fn from(opts: &WriteOptions) -> Self {
        opts.clone().into()
    }
}

impl From<IterOptionss> for RocksReadOptions {
    fn from(opts: IterOptionss) -> Self {
        let mut r = RocksReadOptions::default();
        r.fill_cache(opts.fill_cache);
        if opts.key_only {
            r.set_titan_key_only(true);
        }
        if opts.total_order_seek_used() {
            r.set_total_order_seek(true);
        } else if opts.prefix_same_as_start {
            r.set_prefix_same_as_start(true);
        }
        if let Some(builder) = opts.lower_bound {
            r.set_iterate_lower_bound(builder.build());
        }
        if let Some(builder) = opts.upper_bound {
            r.set_iterate_upper_bound(builder.build());
        }
        r
    }
}

impl From<&IterOptionss> for RocksReadOptions {
    fn from(opts: &IterOptionss) -> Self {
        opts.clone().into()
    }
}

impl From<CFOptions> for RocksCFOptions {
    fn from(_opts: CFOptions) -> Self {
        let r = RocksCFOptions::default();
        r
    }
}

impl From<&CFOptions> for RocksCFOptions {
    fn from(opts: &CFOptions) -> Self {
        opts.clone().into()
    }
}

impl From<IngestExternalFileOptions> for RocksIngestExternalFileOptions {
    fn from(opts: IngestExternalFileOptions) -> Self {
        let mut r = RocksIngestExternalFileOptions::new();
        r.move_files(opts.move_files);
        r
    }
}

impl From<&IngestExternalFileOptions> for RocksIngestExternalFileOptions {
    fn from(opts: &IngestExternalFileOptions) -> Self {
        opts.clone().into()
    }
}
