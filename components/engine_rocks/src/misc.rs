// Copyright 2020 TiKV Project Authors. Licensed under Apache-2.0.

use crate::engine::RocksEngine;
use engine_traits::MiscExt;

impl MiscExt for RocksEngine {
    fn is_titan(&self) -> bool {
        self.as_inner().is_titan()
    }
}
