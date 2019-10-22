// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

use engine_traits::{Error, Result};
use rocksdb::{CFHandle, DB};

pub fn get_cf_handle<'a>(db: &'a DB, cf: &str) -> Result<&'a CFHandle> {
    let handle = db
        .cf_handle(cf)
        .ok_or_else(|| Error::Engine(format!("cf {} not found", cf)))?;
    Ok(handle)
}
