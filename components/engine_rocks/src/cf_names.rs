// Copyright 2020 TiKV Project Authors. Licensed under Apache-2.0.

use crate::engine::RocksEngine;
use engine_traits::CFNamesExt;

impl CFNamesExt for RocksEngine {
    fn cf_names(&self) -> Vec<&str> {
        self.as_inner().cf_names()
    }
}
