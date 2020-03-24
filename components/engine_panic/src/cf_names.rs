// Copyright 2020 TiKV Project Authors. Licensed under Apache-2.0.

use crate::engine::PanicEngine;
use engine_traits::CFNamesExt;

impl CFNamesExt for PanicEngine {
    fn cf_names(&self) -> Vec<&str> {
        panic!()
    }
}
