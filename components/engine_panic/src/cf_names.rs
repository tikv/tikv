// Copyright 2020 TiKV Project Authors. Licensed under Apache-2.0.

use engine_traits::CFNamesExt;

use crate::engine::PanicEngine;

impl CFNamesExt for PanicEngine {
    fn cf_names(&self) -> Vec<&str> {
        panic!()
    }
}
