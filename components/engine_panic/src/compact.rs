// Copyright 2020 TiKV Project Authors. Licensed under Apache-2.0.

use engine_traits::{CompactExt, Result};
use crate::engine::PanicEngine;

impl CompactExt for PanicEngine {
    fn auto_compactions_is_disabled(&self) -> Result<bool> {
        panic!()
    }
}
