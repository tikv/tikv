// Copyright 2020 TiKV Project Authors. Licensed under Apache-2.0.

use crate::engine::SkiplistEngine;
use engine_traits::CFNamesExt;

impl CFNamesExt for SkiplistEngine {
    fn cf_names(&self) -> Vec<&str> {
        self.cf_handles.keys().copied().collect()
    }
}
