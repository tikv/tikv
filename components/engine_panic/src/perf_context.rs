// Copyright 2020 TiKV Project Authors. Licensed under Apache-2.0.

use crate::engine::PanicEngine;
use engine_traits::{PerfContextExt, PerfContext};

impl PerfContextExt for PanicEngine {
    type PerfContext = PanicPerfContext;

    fn get() -> Option<Self::PerfContext> {
        panic!()
    }
}

pub struct PanicPerfContext;

impl PerfContext for PanicPerfContext {
}
