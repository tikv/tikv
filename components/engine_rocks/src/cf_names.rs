// Copyright 2020 TiKV Project Authors. Licensed under Apache-2.0.

use engine_traits::CfNamesExt;

use crate::engine::RocksEngine;

impl CfNamesExt for RocksEngine {
    fn cf_names(&self) -> Vec<&str> {
        self.as_inner().cf_names()
    }
}
