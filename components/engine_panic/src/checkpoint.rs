// Copyright 2022 TiKV Project Authors. Licensed under Apache-2.0.

use engine_traits::{Checkpoint, Result};

use crate::PanicEngine;

pub struct PanicCheckpointer {}

impl Checkpoint for PanicEngine {
    type Checkpointer = PanicCheckpointer;

    fn new_checkpointer(&self) -> Result<Self::Checkpointer> {
        panic!()
    }
}
