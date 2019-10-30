// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

use engine_traits::{Error, Result};
use engine::rocks::util::new_engine as new_engine_raw;
use crate::engine::RocksEngine;
use engine_traits::CF_DEFAULT;
use std::sync::Arc;

pub fn new_default_engine(path: &str) -> Result<RocksEngine> {
    let engine = new_engine_raw(path, None, &[CF_DEFAULT], None)
        .map_err(|e| Error::Other(box_err!(e)))?;
    let engine = Arc::new(engine);
    let engine = RocksEngine::from_db(engine);
    Ok(engine)
}
