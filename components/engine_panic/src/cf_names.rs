// Copyright 2020 TiKV Project Authors. Licensed under Apache-2.0.

use engine_traits::CfNamesExt;

use crate::engine::PanicEngine;

impl CfNamesExt for PanicEngine {
    fn cf_names(&self) -> Vec<&str> {
        panic!()
    }
}
