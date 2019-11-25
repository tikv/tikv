// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

use crate::cf_options::RocksColumnFamilyOptions;
use crate::db_options::RocksDBOptions;
use crate::engine::RocksEngine;
use engine::rocks::util::new_engine as new_engine_raw;
use engine::rocks::util::new_engine_opt as new_engine_opt_raw;
use engine::rocks::util::CFOptions;
use engine_traits::CF_DEFAULT;
use engine_traits::{Error, Result};
use engine_traits::Range;
use rocksdb::Range as RocksRange;
use rocksdb::{CFHandle, DB};
use std::sync::Arc;

pub fn get_cf_handle<'a>(db: &'a DB, cf: &str) -> Result<&'a CFHandle> {
    let handle = db
        .cf_handle(cf)
        .ok_or_else(|| Error::Engine(format!("cf {} not found", cf)))?;
    Ok(handle)
}

pub fn new_default_engine(path: &str) -> Result<RocksEngine> {
    let engine =
        new_engine_raw(path, None, &[CF_DEFAULT], None).map_err(|e| Error::Other(box_err!(e)))?;
    let engine = Arc::new(engine);
    let engine = RocksEngine::from_db(engine);
    Ok(engine)
}

pub struct RocksCFOptions<'a> {
    cf: &'a str,
    options: RocksColumnFamilyOptions,
}

impl<'a> RocksCFOptions<'a> {
    pub fn new(cf: &'a str, options: RocksColumnFamilyOptions) -> RocksCFOptions<'a> {
        RocksCFOptions { cf, options }
    }

    pub fn into_raw(self) -> CFOptions<'a> {
        CFOptions::new(self.cf, self.options.into_raw())
    }
}

pub fn new_engine(
    path: &str,
    db_opts: Option<RocksDBOptions>,
    cfs: &[&str],
    opts: Option<Vec<RocksCFOptions<'_>>>,
) -> Result<RocksEngine> {
    let db_opts = db_opts.map(RocksDBOptions::into_raw);
    let opts = opts.map(|o| o.into_iter().map(RocksCFOptions::into_raw).collect());
    let engine = new_engine_raw(path, db_opts, cfs, opts).map_err(|e| Error::Other(box_err!(e)))?;
    let engine = Arc::new(engine);
    let engine = RocksEngine::from_db(engine);
    Ok(engine)
}

pub fn new_engine_opt(
    path: &str,
    db_opt: RocksDBOptions,
    cfs_opts: Vec<RocksCFOptions<'_>>,
) -> Result<RocksEngine> {
    let db_opt = db_opt.into_raw();
    let cfs_opts = cfs_opts.into_iter().map(RocksCFOptions::into_raw).collect();
    let engine =
        new_engine_opt_raw(path, db_opt, cfs_opts).map_err(|e| Error::Other(box_err!(e)))?;
    let engine = Arc::new(engine);
    let engine = RocksEngine::from_db(engine);
    Ok(engine)
}

pub fn range_to_rocks_range<'a>(range: &Range<'a>) -> RocksRange<'a> {
    RocksRange::new(range.start_key, range.end_key)
}
