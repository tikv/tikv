// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

use crate::options::*;

pub use super::{RawCFOptions, RawReadOptions, RawWriteOptions};
use crate::rocks::RawIngestExternalFileOptions;

impl From<ReadOptions> for RawReadOptions {
    fn from(_opts: ReadOptions) -> Self {
        RawReadOptions::default()
    }
}

impl From<&ReadOptions> for RawReadOptions {
    fn from(opts: &ReadOptions) -> Self {
        opts.clone().into()
    }
}

impl From<WriteOptions> for RawWriteOptions {
    fn from(opts: WriteOptions) -> Self {
        let mut r = RawWriteOptions::default();
        r.set_sync(opts.sync);
        r
    }
}

impl From<&WriteOptions> for RawWriteOptions {
    fn from(opts: &WriteOptions) -> Self {
        opts.clone().into()
    }
}

impl From<IterOptions> for RawReadOptions {
    fn from(opts: IterOptions) -> Self {
        let mut r = RawReadOptions::default();
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

impl From<&IterOptions> for RawReadOptions {
    fn from(opts: &IterOptions) -> Self {
        opts.clone().into()
    }
}

impl From<CFOptions> for RawCFOptions {
    fn from(_opts: CFOptions) -> Self {
        RawCFOptions::default()
    }
}

impl From<&CFOptions> for RawCFOptions {
    fn from(opts: &CFOptions) -> Self {
        opts.clone().into()
    }
}

impl From<IngestExternalFileOptions> for RawIngestExternalFileOptions {
    fn from(opts: IngestExternalFileOptions) -> Self {
        let mut r = RawIngestExternalFileOptions::new();
        r.move_files(opts.move_files);
        r
    }
}

impl From<&IngestExternalFileOptions> for RawIngestExternalFileOptions {
    fn from(opts: &IngestExternalFileOptions) -> Self {
        opts.clone().into()
    }
}
